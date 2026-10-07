package internal

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

var errInteractiveCancelled = errors.New("interactive deletion cancelled")

// PrefixChoice describes a top-level prefix presented for interactive selection.
type PrefixChoice struct {
	Prefix    string
	Protected bool
}

// ParsePrefixSelection parses numbered selections and rejects protected choices.
func ParsePrefixSelection(text string, choices []PrefixChoice) ([]string, error) {
	text = strings.TrimSpace(text)
	if text == "" || strings.EqualFold(text, "q") || strings.EqualFold(text, "quit") || strings.EqualFold(text, "cancel") {
		return nil, errInteractiveCancelled
	}

	selected := make([]string, 0)
	seen := make(map[int]struct{})
	for _, field := range strings.FieldsFunc(text, func(rune rune) bool { return rune == ',' || rune == ' ' || rune == '\t' }) {
		index, err := strconv.Atoi(field)
		if err != nil || index < 1 || index > len(choices) {
			return nil, fmt.Errorf("invalid prefix selection %q", field)
		}
		choice := choices[index-1]
		if choice.Protected {
			return nil, fmt.Errorf("prefix %q is protected and cannot be selected", choice.Prefix)
		}
		if _, ok := seen[index]; ok {
			continue
		}
		seen[index] = struct{}{}
		selected = append(selected, choice.Prefix)
	}
	if len(selected) == 0 {
		return nil, errInteractiveCancelled
	}

	return selected, nil
}

// ConfirmBucket requires the bucket name exactly before deletion starts.
func ConfirmBucket(text, bucket string) error {
	if text != bucket {
		return errInteractiveCancelled
	}
	return nil
}

func runInteractive(ctx context.Context, input io.Reader, output io.Writer, config Config, client S3API, bucketStats bool, bucketStatsOptions BucketStatsOptions) error {
	versioning, err := DetectVersioning(ctx, client, config.Bucket)
	if err != nil {
		return err
	}
	var scan *bucketStatsScan
	if bucketStats {
		scan = startBucketStatsScan(ctx, client, config.Bucket, versioning, bucketStatsOptions)
		defer scan.cancel()
	}
	// Discover without exclusions so protected prefixes stay visible, then mark
	// them unselectable before accepting input.
	prefixes, err := DiscoverPrefixes(ctx, client, config.Bucket, versioning, nil)
	if err != nil {
		return err
	}
	reader := bufio.NewReader(input)
	selected, err := selectPrefixesWithStats(reader, output, prefixChoices(prefixes, config.Exclude), scan)
	if err != nil {
		return err
	}
	reportBucketStats(output, scan)
	if err := confirmBucket(reader, output, config.Bucket); err != nil {
		return err
	}
	reportBucketStats(output, scan)
	progress := NewProgressRenderer(output, true)
	if _, err := DeleteSelectedPrefixes(ctx, client, config.Bucket, selected, versioning, config.Exclude, progress); err != nil {
		return fmt.Errorf("delete selected prefixes: %w", err)
	}

	return nil
}

func prefixChoices(prefixes, exclusions []string) []PrefixChoice {
	choices := make([]PrefixChoice, 0, len(prefixes))
	for _, prefix := range prefixes {
		choices = append(choices, PrefixChoice{Prefix: prefix, Protected: IsExcluded(prefix, exclusions)})
	}
	sort.Slice(choices, func(left, right int) bool { return choices[left].Prefix < choices[right].Prefix })
	return choices
}

func selectPrefixes(input *bufio.Reader, output io.Writer, choices []PrefixChoice) ([]string, error) {
	return selectPrefixesWithStats(input, output, choices, nil)
}

func selectPrefixesWithStats(input *bufio.Reader, output io.Writer, choices []PrefixChoice, scan *bucketStatsScan) ([]string, error) {
	if len(choices) == 0 {
		return nil, errInteractiveCancelled
	}
	if _, err := fmt.Fprintln(output, "Top-level prefixes:"); err != nil {
		return nil, fmt.Errorf("print prefixes: %w", err)
	}
	for index, choice := range choices {
		status := ""
		if choice.Protected {
			status = " (protected)"
		}
		if _, err := fmt.Fprintf(output, "%d. %s%s\n", index+1, choice.Prefix, status); err != nil {
			return nil, fmt.Errorf("print prefix: %w", err)
		}
	}
	for {
		reportBucketStats(output, scan)
		prompt := "Select prefixes by number (comma-separated; q cancels): "
		if scan != nil {
			prompt = "Select prefixes by number (comma-separated; s stops bucket scan; q cancels): "
		}
		if _, err := fmt.Fprint(output, prompt); err != nil {
			return nil, fmt.Errorf("print selection prompt: %w", err)
		}
		line, err := readLine(input)
		if err != nil {
			return nil, err
		}
		if strings.EqualFold(strings.TrimSpace(line), "s") && scan != nil {
			scan.cancel()
			if _, err := fmt.Fprintln(output, "Bucket statistics scan cancelled."); err != nil {
				return nil, fmt.Errorf("print scan cancellation: %w", err)
			}
			continue
		}
		return ParsePrefixSelection(line, choices)
	}
}

func confirmBucket(input *bufio.Reader, output io.Writer, bucket string) error {
	if _, err := fmt.Fprintf(output, "Type bucket name %q to confirm deletion: ", bucket); err != nil {
		return fmt.Errorf("print confirmation prompt: %w", err)
	}
	line, err := readLine(input)
	if err != nil {
		return err
	}
	return ConfirmBucket(line, bucket)
}

func readLine(input *bufio.Reader) (string, error) {
	line, err := input.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("read terminal input: %w", err)
	}
	if errors.Is(err, io.EOF) && line == "" {
		return "", errInteractiveCancelled
	}
	return strings.TrimSuffix(line, "\n"), nil
}

type bucketStatsResult struct {
	stats BucketStats
	err   error
}

type bucketStatsScan struct {
	cancel context.CancelFunc
	result <-chan bucketStatsResult
}

func startBucketStatsScan(ctx context.Context, client ObjectLister, bucket string, versioning Versioning, options BucketStatsOptions) *bucketStatsScan {
	scanContext, cancel := context.WithCancel(ctx)
	result := make(chan bucketStatsResult, 1)
	go func() {
		stats, err := ScanBucketStats(scanContext, client, bucket, versioning, options)
		result <- bucketStatsResult{stats: stats, err: err}
	}()
	return &bucketStatsScan{cancel: cancel, result: result}
}

func reportBucketStats(output io.Writer, scan *bucketStatsScan) {
	if scan == nil {
		return
	}
	select {
	case result := <-scan.result:
		if result.err != nil {
			_, _ = fmt.Fprintf(output, "Bucket statistics scan incomplete: %v\n", result.err)
			return
		}
		if result.stats.Complete {
			_, _ = fmt.Fprintf(output, "Bucket total: %d objects (%s)\n", result.stats.Objects, formatBytes(result.stats.Bytes))
			return
		}
		_, _ = fmt.Fprintf(output, "Bucket statistics scan incomplete (%s): %d objects (%s)\n", result.stats.Incomplete, result.stats.Objects, formatBytes(result.stats.Bytes))
	default:
	}
}
