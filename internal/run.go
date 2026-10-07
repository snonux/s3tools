package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/term"
)

// S3API is the S3 surface used by batch deletion.
type S3API interface {
	ObjectDeleter
	BucketVersioningAPI
}

type clientFactory func(context.Context, Config) (S3API, error)

// Run executes s3rm with command-line arguments.
func Run(ctx context.Context, args []string) error {
	return run(ctx, args, isTerminal(os.Stdin) && isTerminal(os.Stdout), isTerminal(os.Stdout), newClient)
}

func run(ctx context.Context, args []string, interactiveTerminal, outputTerminal bool, makeClient clientFactory) error {
	return runWithOutput(ctx, args, interactiveTerminal, outputTerminal, makeClient, os.Stdout)
}

func runWithOutput(ctx context.Context, args []string, interactiveTerminal, outputTerminal bool, makeClient clientFactory, output io.Writer) error {
	config, remaining, err := LoadConfig(args)
	if err != nil {
		return err
	}
	request, remaining, err := ParseBatchArgs(remaining)
	if err != nil {
		return err
	}
	if len(remaining) != 0 {
		return fmt.Errorf("unexpected arguments: %v", remaining)
	}
	if len(request.Prefixes) == 0 && request.ConfirmedBucket == "" {
		if !interactiveTerminal {
			return errors.New("non-interactive execution requires --prefix and --confirm-bucket")
		}
		client, err := makeClient(ctx, config)
		if err != nil {
			return err
		}
		return runInteractive(ctx, os.Stdin, output, config, client, request.BucketStats, request.BucketStatsOptions)
	}
	if err := validateBatchRequestHeader(request, config.Bucket); err != nil {
		return err
	}
	if err := validateBatchTargets(request.Prefixes, config.Exclude); err != nil {
		return err
	}
	client, err := makeClient(ctx, config)
	if err != nil {
		return err
	}
	versioning, err := DetectVersioning(ctx, client, config.Bucket)
	if err != nil {
		return err
	}
	available, err := DiscoverPrefixes(ctx, client, config.Bucket, versioning, config.Exclude)
	if err != nil {
		return err
	}
	if err := ValidateBatchRequest(request, config.Bucket, available, config.Exclude); err != nil {
		return err
	}
	stats := bucketStatsBaseline(ctx, client, config.Bucket, versioning, request.BucketStats, request.BucketStatsOptions)
	progress := NewProgressRendererWithBucketStats(output, outputTerminal, stats)
	if _, err := DeleteSelectedPrefixes(ctx, client, config.Bucket, request.Prefixes, versioning, config.Exclude, progress); err != nil {
		return fmt.Errorf("delete selected prefixes: %w", err)
	}

	return nil
}

func bucketStatsBaseline(ctx context.Context, client ObjectLister, bucket string, versioning Versioning, enabled bool, options BucketStatsOptions) *BucketStats {
	if !enabled {
		return nil
	}
	stats, err := ScanBucketStats(ctx, client, bucket, versioning, options)
	if err != nil {
		stats.Incomplete = "scan error"
	}
	return &stats
}

func validateBatchRequestHeader(request BatchRequest, bucket string) error {
	if len(request.Prefixes) == 0 {
		return errors.New("non-interactive mode requires at least one --prefix")
	}
	if request.ConfirmedBucket == "" {
		return errors.New("non-interactive mode requires --confirm-bucket")
	}
	if request.ConfirmedBucket != bucket {
		return fmt.Errorf("--confirm-bucket %q does not exactly match configured bucket %q", request.ConfirmedBucket, bucket)
	}
	return nil
}

func newClient(ctx context.Context, config Config) (S3API, error) {
	return NewS3Client(ctx, config)
}

func isTerminal(file *os.File) bool {
	return term.IsTerminal(int(file.Fd()))
}
