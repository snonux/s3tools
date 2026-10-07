package internal

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// BatchRequest identifies the explicit targets required for a non-interactive run.
type BatchRequest struct {
	Prefixes           []string
	ConfirmedBucket    string
	BucketStats        bool
	BucketStatsOptions BucketStatsOptions
}

// ParseBatchArgs extracts batch options and returns arguments for another mode.
func ParseBatchArgs(args []string) (BatchRequest, []string, error) {
	request := BatchRequest{Prefixes: make([]string, 0)}
	remaining := make([]string, 0, len(args))
	for index := 0; index < len(args); index++ {
		argument := args[index]
		if value, found, attached := batchOptionValue(argument, "prefix"); found {
			value, nextIndex, err := requiredBatchValue(value, attached, args, index, "--prefix")
			if err != nil {
				return BatchRequest{}, nil, err
			}
			request.Prefixes = append(request.Prefixes, value)
			index = nextIndex
			continue
		}
		if value, found, attached := batchOptionValue(argument, "confirm-bucket"); found {
			value, nextIndex, err := requiredBatchValue(value, attached, args, index, "--confirm-bucket")
			if err != nil {
				return BatchRequest{}, nil, err
			}
			if request.ConfirmedBucket != "" {
				return BatchRequest{}, nil, errors.New("--confirm-bucket may only be specified once")
			}
			request.ConfirmedBucket = value
			index = nextIndex
			continue
		}
		if argument == "--bucket-stats" {
			request.BucketStats = true
			continue
		}
		if value, found, attached := batchOptionValue(argument, "bucket-stats-pages"); found {
			value, nextIndex, err := requiredBatchValue(value, attached, args, index, "--bucket-stats-pages")
			if err != nil {
				return BatchRequest{}, nil, err
			}
			pages, err := strconv.Atoi(value)
			if err != nil || pages < 1 {
				return BatchRequest{}, nil, errors.New("--bucket-stats-pages must be a positive integer")
			}
			request.BucketStatsOptions.MaxPages = pages
			index = nextIndex
			continue
		}
		if value, found, attached := batchOptionValue(argument, "bucket-stats-timeout"); found {
			value, nextIndex, err := requiredBatchValue(value, attached, args, index, "--bucket-stats-timeout")
			if err != nil {
				return BatchRequest{}, nil, err
			}
			timeout, err := time.ParseDuration(value)
			if err != nil || timeout <= 0 {
				return BatchRequest{}, nil, errors.New("--bucket-stats-timeout must be a positive duration")
			}
			request.BucketStatsOptions.Timeout = timeout
			index = nextIndex
			continue
		}
		remaining = append(remaining, argument)
	}
	if !request.BucketStats && request.BucketStatsOptions != (BucketStatsOptions{}) {
		return BatchRequest{}, nil, errors.New("--bucket-stats-pages and --bucket-stats-timeout require --bucket-stats")
	}

	return request, remaining, nil
}

// ValidateBatchRequest verifies explicit targets before deletion begins.
func ValidateBatchRequest(request BatchRequest, bucket string, available []string, exclusions []string) error {
	if len(request.Prefixes) == 0 {
		return errors.New("non-interactive mode requires at least one --prefix")
	}
	if request.ConfirmedBucket == "" {
		return errors.New("non-interactive mode requires --confirm-bucket")
	}
	if request.ConfirmedBucket != bucket {
		return fmt.Errorf("--confirm-bucket %q does not exactly match configured bucket %q", request.ConfirmedBucket, bucket)
	}

	if err := validateBatchTargets(request.Prefixes, exclusions); err != nil {
		return err
	}

	availableSet := make(map[string]struct{}, len(available))
	for _, prefix := range available {
		availableSet[prefix] = struct{}{}
	}
	for _, prefix := range request.Prefixes {
		if _, exists := availableSet[prefix]; !exists {
			return fmt.Errorf("prefix %q does not exist in bucket %q", prefix, bucket)
		}
	}

	return nil
}

func validateBatchTargets(prefixes, exclusions []string) error {
	seen := make(map[string]struct{}, len(prefixes))
	for _, prefix := range prefixes {
		if err := validatePrefix(prefix); err != nil {
			return err
		}
		if IsExcluded(prefix, exclusions) {
			return fmt.Errorf("prefix %q is fully protected by an exclusion", prefix)
		}
		if _, duplicate := seen[prefix]; duplicate {
			return fmt.Errorf("prefix %q was selected more than once", prefix)
		}
		seen[prefix] = struct{}{}
	}
	return nil
}

func batchOptionValue(argument, name string) (string, bool, bool) {
	for _, prefix := range []string{"--", "-"} {
		option := prefix + name
		if argument == option {
			return "", true, false
		}
		if strings.HasPrefix(argument, option+"=") {
			return strings.TrimPrefix(argument, option+"="), true, true
		}
	}
	return "", false, false
}

func requiredBatchValue(value string, attached bool, args []string, index int, name string) (string, int, error) {
	if value != "" {
		return value, index, nil
	}
	if attached || index+1 == len(args) || strings.HasPrefix(args[index+1], "-") {
		return "", index, fmt.Errorf("%s requires a value", name)
	}
	return args[index+1], index + 1, nil
}
