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

func runInteractive(ctx context.Context, input io.Reader, output io.Writer, config Config, client S3API) error {
	versioning, err := DetectVersioning(ctx, client, config.Bucket)
	if err != nil {
		return err
	}
	// Discover without exclusions so protected prefixes stay visible, then mark
	// them unselectable before accepting input.
	prefixes, err := DiscoverPrefixes(ctx, client, config.Bucket, versioning, nil)
	if err != nil {
		return err
	}
	reader := bufio.NewReader(input)
	selected, err := selectPrefixes(reader, output, prefixChoices(prefixes, config.Exclude))
	if err != nil {
		return err
	}
	if err := confirmBucket(reader, output, config.Bucket); err != nil {
		return err
	}
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
	if _, err := fmt.Fprint(output, "Select prefixes by number (comma-separated; q cancels): "); err != nil {
		return nil, fmt.Errorf("print selection prompt: %w", err)
	}

	line, err := readLine(input)
	if err != nil {
		return nil, err
	}
	return ParsePrefixSelection(line, choices)
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
