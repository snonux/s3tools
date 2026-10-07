package internal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	defaultBucketStatsMaxPages = 1000
	defaultBucketStatsTimeout  = 2 * time.Minute
)

// BucketStatsOptions bounds a whole-bucket statistics scan.
type BucketStatsOptions struct {
	MaxPages int
	Timeout  time.Duration
}

// BucketStats reports the portion of a bucket examined by a scan.
type BucketStats struct {
	Objects    int64
	Bytes      int64
	Pages      int
	Complete   bool
	Incomplete string
}

// DefaultBucketStatsOptions returns the limits used for an opted-in scan.
func DefaultBucketStatsOptions() BucketStatsOptions {
	return BucketStatsOptions{MaxPages: defaultBucketStatsMaxPages, Timeout: defaultBucketStatsTimeout}
}

// ScanBucketStats counts bucket contents until the listing completes or a configured limit is reached.
func ScanBucketStats(ctx context.Context, client ObjectLister, bucket string, versioning Versioning, options BucketStatsOptions) (BucketStats, error) {
	options, err := normalizeBucketStatsOptions(options)
	if err != nil {
		return BucketStats{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()
	if versioning == Versioned {
		return scanVersionedBucketStats(ctx, client, bucket, options.MaxPages)
	}
	return scanUnversionedBucketStats(ctx, client, bucket, options.MaxPages)
}

func normalizeBucketStatsOptions(options BucketStatsOptions) (BucketStatsOptions, error) {
	defaults := DefaultBucketStatsOptions()
	if options.MaxPages == 0 {
		options.MaxPages = defaults.MaxPages
	}
	if options.Timeout == 0 {
		options.Timeout = defaults.Timeout
	}
	if options.MaxPages < 1 {
		return BucketStatsOptions{}, errors.New("bucket statistics page limit must be positive")
	}
	if options.Timeout < 1 {
		return BucketStatsOptions{}, errors.New("bucket statistics timeout must be positive")
	}
	return options, nil
}

func scanUnversionedBucketStats(ctx context.Context, client ObjectLister, bucket string, maxPages int) (BucketStats, error) {
	stats := BucketStats{}
	var continuationToken *string
	for {
		if err := ctx.Err(); err != nil {
			return incompleteBucketStats(stats, incompleteReason(err)), nil
		}
		if stats.Pages == maxPages {
			return incompleteBucketStats(stats, "page limit"), nil
		}
		result, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), EncodingType: types.EncodingTypeUrl, ContinuationToken: continuationToken})
		if err != nil {
			if ctx.Err() != nil {
				return incompleteBucketStats(stats, incompleteReason(ctx.Err())), nil
			}
			return stats, fmt.Errorf("scan bucket %q: %w", bucket, err)
		}
		if result == nil {
			return stats, fmt.Errorf("scan bucket %q: empty response", bucket)
		}
		if err := decodeListObjectsV2(result); err != nil {
			return stats, fmt.Errorf("scan bucket %q: decode URL-encoded keys: %w", bucket, err)
		}
		stats.Pages++
		for _, object := range result.Contents {
			stats.Objects++
			stats.Bytes += aws.ToInt64(object.Size)
		}
		if !aws.ToBool(result.IsTruncated) {
			stats.Complete = true
			return stats, nil
		}
		if result.NextContinuationToken == nil {
			return stats, fmt.Errorf("scan bucket %q: truncated response has no continuation token", bucket)
		}
		continuationToken = result.NextContinuationToken
	}
}

func scanVersionedBucketStats(ctx context.Context, client ObjectLister, bucket string, maxPages int) (BucketStats, error) {
	stats := BucketStats{}
	var keyMarker, versionMarker *string
	for {
		if err := ctx.Err(); err != nil {
			return incompleteBucketStats(stats, incompleteReason(err)), nil
		}
		if stats.Pages == maxPages {
			return incompleteBucketStats(stats, "page limit"), nil
		}
		result, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket), EncodingType: types.EncodingTypeUrl, KeyMarker: keyMarker, VersionIdMarker: versionMarker})
		if err != nil {
			if ctx.Err() != nil {
				return incompleteBucketStats(stats, incompleteReason(ctx.Err())), nil
			}
			return stats, fmt.Errorf("scan bucket %q versions: %w", bucket, err)
		}
		if result == nil {
			return stats, fmt.Errorf("scan bucket %q versions: empty response", bucket)
		}
		if err := decodeListObjectVersions(result); err != nil {
			return stats, fmt.Errorf("scan bucket %q versions: decode URL-encoded keys: %w", bucket, err)
		}
		stats.Pages++
		for _, object := range result.Versions {
			stats.Objects++
			stats.Bytes += aws.ToInt64(object.Size)
		}
		stats.Objects += int64(len(result.DeleteMarkers))
		if !aws.ToBool(result.IsTruncated) {
			stats.Complete = true
			return stats, nil
		}
		if result.NextKeyMarker == nil || result.NextVersionIdMarker == nil {
			return stats, fmt.Errorf("scan bucket %q versions: truncated response has no markers", bucket)
		}
		keyMarker, versionMarker = result.NextKeyMarker, result.NextVersionIdMarker
	}
}

func incompleteBucketStats(stats BucketStats, reason string) BucketStats {
	stats.Incomplete = reason
	return stats
}

func incompleteReason(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "time limit"
	}
	return "cancelled"
}
