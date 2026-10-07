package internal

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type streamResult struct {
	summary DeleteSummary
	pending []ObjectEntry
	err     error
}

type objectStreamPage struct {
	inventory Inventory
	after     string
	last      string
}

func deleteSelectedStreaming(ctx context.Context, client ObjectDeleter, bucket string, prefixes []string, versioning Versioning, exclusions []string, progress ProgressObserver) (DeleteSummary, error) {
	if err := ctx.Err(); err != nil {
		return DeleteSummary{}, &PartialDeleteError{Cause: err}
	}
	startStreamingProgress(ctx, progress)
	if progress != nil {
		defer progress.Finish()
	}

	summary := DeleteSummary{}
	for index, prefix := range prefixes {
		if err := validatePrefix(prefix); err != nil {
			setProgressPhase(progress, "deletion stopped")
			return summary, err
		}
		if IsExcluded(prefix, exclusions) {
			setProgressPhase(progress, "deletion stopped")
			return summary, fmt.Errorf("prefix %q is protected by an exclusion", prefix)
		}
		setProgressPhase(progress, fmt.Sprintf("listing and deleting prefix %q (%d/%d)", prefix, index+1, len(prefixes)))
		var result streamResult
		if versioning == Versioned {
			result = deleteVersionedStream(ctx, client, bucket, prefix, exclusions, progress)
		} else {
			result = deleteUnversionedStream(ctx, client, bucket, prefix, exclusions, progress)
		}
		mergeDeleteSummary(&summary, result.summary)
		if result.err != nil {
			setProgressPhase(progress, "deletion stopped")
			return summary, &PartialDeleteError{Summary: summary, Cause: result.err}
		}
		if err := ctx.Err(); err != nil {
			setProgressPhase(progress, "deletion stopped")
			return summary, &PartialDeleteError{Summary: summary, Cause: err}
		}
		if index == len(prefixes)-1 {
			completeStreamingDiscovery(progress)
		}
		setProgressPhase(progress, fmt.Sprintf("verifying prefix %q", prefix))
		remaining, err := firstRemainingEntry(ctx, client, bucket, prefix, versioning, exclusions, progress)
		if err != nil {
			setProgressPhase(progress, "deletion stopped")
			return summary, &PartialDeleteError{Summary: summary, Cause: fmt.Errorf("verify deletion: %w", err)}
		}
		if remaining != nil || len(result.summary.Failures) != 0 {
			if remaining != nil && !hasFailure(summary.Failures, remaining.Key, remaining.VersionID) {
				summary.Failures = append(summary.Failures, DeleteFailure{Key: remaining.Key, VersionID: remaining.VersionID, Code: "Unconfirmed", Message: "object remains after deletion"})
			}
			setProgressPhase(progress, "deletion stopped")
			var cause error
			if remaining != nil {
				cause = errors.New("eligible objects remain")
			}
			return summary, &PartialDeleteError{Summary: summary, Cause: cause}
		}
	}
	setProgressPhase(progress, "deletion complete")
	return summary, nil
}

func startStreamingProgress(ctx context.Context, progress ProgressObserver) {
	if streaming, ok := progress.(StreamingProgressObserver); ok {
		streaming.StartStreaming(ctx)
	} else if progress != nil {
		progress.Start(ctx, ProgressTotal{})
	}
}

func discoverStreaming(progress ProgressObserver, total ProgressTotal) {
	if streaming, ok := progress.(StreamingProgressObserver); ok {
		streaming.Discover(total)
	}
}

func completeStreamingDiscovery(progress ProgressObserver) {
	if streaming, ok := progress.(StreamingProgressObserver); ok {
		streaming.CompleteDiscovery()
	}
}

func mergeDeleteSummary(destination *DeleteSummary, source DeleteSummary) {
	destination.Deleted += source.Deleted
	destination.DeletedBytes += source.DeletedBytes
	destination.Failures = append(destination.Failures, source.Failures...)
}

// deleteUnversionedStream lists by the last observed key, which remains a
// stable lexical boundary when a deletion worker removes earlier keys.
func deleteUnversionedStream(ctx context.Context, client ObjectDeleter, bucket, prefix string, exclusions []string, progress ProgressObserver) streamResult {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	pages := make(chan objectStreamPage, 2)
	listed := make(chan error, 1)
	go func() {
		defer close(pages)
		listed <- listUnversionedStream(ctx, client, bucket, prefix, exclusions, progress, pages)
	}()

	result := streamResult{}
	for page := range pages {
		if result.err != nil {
			continue
		}
		deleted := deleteStreamPage(ctx, client, bucket, page.inventory, Unversioned, progress)
		mergeDeleteSummary(&result.summary, deleted.summary)
		if deleted.err != nil {
			result.err = deleted.err
			cancel()
			continue
		}
		if len(deleted.pending) != 0 {
			verified, failures, err := verifyUnacknowledgedPage(ctx, client, bucket, prefix, page, deleted.pending)
			result.summary.Deleted += verified.Objects
			result.summary.DeletedBytes += verified.Bytes
			result.summary.Failures = append(result.summary.Failures, failures...)
			if progress != nil && verified != (ProgressTotal{}) {
				progress.Advance(verified)
			}
			if err != nil {
				result.err = err
				cancel()
			}
		}
		if result.err == nil && len(result.summary.Failures) != 0 {
			result.err = errors.New("unversioned deletion had object failures")
			cancel()
		}
	}
	listErr := <-listed
	if result.err == nil {
		result.err = listErr
	}
	return result
}

func listUnversionedStream(ctx context.Context, client ObjectLister, bucket, prefix string, exclusions []string, progress ProgressObserver, pages chan<- objectStreamPage) error {
	var lastKey string
	pageCount := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		input := &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix), EncodingType: types.EncodingTypeUrl}
		if lastKey != "" {
			input.StartAfter = aws.String(lastKey)
		}
		response, err := client.ListObjectsV2(ctx, input)
		if err != nil {
			return fmt.Errorf("list objects below %q: %w", prefix, err)
		}
		if response == nil {
			return fmt.Errorf("list objects below %q: empty response", prefix)
		}
		if err := decodeListObjectsV2(response); err != nil {
			return fmt.Errorf("list objects below %q: decode URL-encoded keys: %w", prefix, err)
		}
		page := Inventory{}
		previousKey := lastKey
		for _, object := range response.Contents {
			key := aws.ToString(object.Key)
			if key <= lastKey || !strings.HasPrefix(key, prefix) {
				return fmt.Errorf("list objects below %q: non-increasing or out-of-prefix key %q", prefix, key)
			}
			lastKey = key
			if !IsExcluded(key, exclusions) {
				addEntry(&page, ObjectEntry{Key: key, Size: aws.ToInt64(object.Size)})
			}
		}
		if aws.ToBool(response.IsTruncated) && len(response.Contents) == 0 {
			return fmt.Errorf("list objects below %q: truncated page has no keys", prefix)
		}
		pageCount++
		setProgressPhaseQuiet(progress, fmt.Sprintf("listing and deleting prefix %q: %d pages listed", prefix, pageCount))
		if len(page.Entries) != 0 {
			discoverStreaming(progress, ProgressTotal{Objects: len(page.Entries), Bytes: page.Bytes})
			select {
			case pages <- objectStreamPage{inventory: page, after: previousKey, last: lastKey}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if !aws.ToBool(response.IsTruncated) {
			return nil
		}
	}
}

func deleteVersionedStream(ctx context.Context, client ObjectDeleter, bucket, prefix string, exclusions []string, progress ProgressObserver) streamResult {
	result := streamResult{}
	var marker string
	var lastPage string
	pages := 0
	for {
		if err := ctx.Err(); err != nil {
			result.err = err
			return result
		}
		input := &s3.ListObjectVersionsInput{Bucket: aws.String(bucket), Prefix: aws.String(prefix), EncodingType: types.EncodingTypeUrl}
		if marker != "" {
			input.KeyMarker = aws.String(marker)
		}
		response, err := client.ListObjectVersions(ctx, input)
		if err != nil {
			result.err = fmt.Errorf("list versions below %q: %w", prefix, err)
			return result
		}
		if response == nil {
			result.err = fmt.Errorf("list versions below %q: empty response", prefix)
			return result
		}
		if err := decodeListObjectVersions(response); err != nil {
			result.err = fmt.Errorf("list versions below %q: decode URL-encoded keys: %w", prefix, err)
			return result
		}
		page, finalKey, fingerprint, err := versionedStreamPage(response, prefix, marker, exclusions)
		if err != nil {
			result.err = err
			return result
		}
		pages++
		setProgressPhaseQuiet(progress, fmt.Sprintf("listing and deleting versions below %q: %d pages listed", prefix, pages))
		if len(page.Entries) == 0 {
			if !aws.ToBool(response.IsTruncated) {
				return result
			}
			if finalKey == "" {
				result.err = fmt.Errorf("list versions below %q: truncated page has no keys", prefix)
				return result
			}
			marker = finalKey
			lastPage = ""
			continue
		}
		if fingerprint == lastPage {
			result.err = fmt.Errorf("list versions below %q: deletion made no listing progress", prefix)
			return result
		}
		lastPage = fingerprint
		discoverStreaming(progress, ProgressTotal{Objects: len(page.Entries), Bytes: page.Bytes})
		deleted := deleteStreamPage(ctx, client, bucket, page, Versioned, progress)
		mergeDeleteSummary(&result.summary, deleted.summary)
		if deleted.err != nil {
			result.err = deleted.err
			return result
		}
		if len(deleted.summary.Failures) != 0 {
			result.err = errors.New("versioned deletion had object failures")
			return result
		}
		// Re-list at the same stable key marker. The page's version marker may
		// refer to a version that was just removed.
	}
}

func versionedStreamPage(response *s3.ListObjectVersionsOutput, prefix, marker string, exclusions []string) (Inventory, string, string, error) {
	page := Inventory{}
	lastKey := ""
	var fingerprint strings.Builder
	add := func(key, versionID string, size int64, deleteMarker bool) error {
		if key <= marker || !strings.HasPrefix(key, prefix) {
			return fmt.Errorf("list versions below %q: non-increasing or out-of-prefix key %q", prefix, key)
		}
		if key > lastKey {
			lastKey = key
		}
		if IsExcluded(key, exclusions) {
			return nil
		}
		addEntry(&page, ObjectEntry{Key: key, VersionID: versionID, Size: size, DeleteMarker: deleteMarker})
		_, _ = fmt.Fprintf(&fingerprint, "%d:%s%d:%s;", len(key), key, len(versionID), versionID)
		return nil
	}
	for _, object := range response.Versions {
		if err := add(aws.ToString(object.Key), aws.ToString(object.VersionId), aws.ToInt64(object.Size), false); err != nil {
			return Inventory{}, "", "", err
		}
	}
	for _, object := range response.DeleteMarkers {
		if err := add(aws.ToString(object.Key), aws.ToString(object.VersionId), 0, true); err != nil {
			return Inventory{}, "", "", err
		}
	}
	return page, lastKey, fingerprint.String(), nil
}

func deleteStreamPage(ctx context.Context, client ObjectDeleter, bucket string, page Inventory, versioning Versioning, progress ProgressObserver) streamResult {
	result := streamResult{}
	for start := 0; start < len(page.Entries); start += deleteBatchSize {
		if err := ctx.Err(); err != nil {
			result.err = err
			return result
		}
		end := min(start+deleteBatchSize, len(page.Entries))
		before := result.summary
		pending, err := deleteBatch(ctx, client, bucket, page.Entries[start:end], versioning, &result.summary)
		if progress != nil {
			progress.Advance(ProgressTotal{Objects: result.summary.Deleted - before.Deleted, Bytes: result.summary.DeletedBytes - before.DeletedBytes})
		}
		if err != nil {
			result.err = err
			return result
		}
		result.pending = append(result.pending, pending...)
	}
	return result
}

// verifyUnacknowledgedPage checks only the listed key range, so gateways that
// omit successful DeleteObjects entries do not require retaining every key in
// the prefix until the final scan.
func verifyUnacknowledgedPage(ctx context.Context, client ObjectLister, bucket, prefix string, page objectStreamPage, pending []ObjectEntry) (ProgressTotal, []DeleteFailure, error) {
	remains := make(map[string]bool, len(pending))
	for _, entry := range pending {
		remains[entry.Key] = false
	}
	cursor := page.after
	for {
		input := &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix), EncodingType: types.EncodingTypeUrl}
		if cursor != "" {
			input.StartAfter = aws.String(cursor)
		}
		response, err := client.ListObjectsV2(ctx, input)
		if err != nil {
			return ProgressTotal{}, nil, fmt.Errorf("verify unacknowledged deletes below %q: %w", prefix, err)
		}
		if response == nil {
			return ProgressTotal{}, nil, fmt.Errorf("verify unacknowledged deletes below %q: empty response", prefix)
		}
		if err := decodeListObjectsV2(response); err != nil {
			return ProgressTotal{}, nil, fmt.Errorf("verify unacknowledged deletes below %q: decode URL-encoded keys: %w", prefix, err)
		}
		pastPage := false
		for _, object := range response.Contents {
			key := aws.ToString(object.Key)
			if key <= cursor || !strings.HasPrefix(key, prefix) {
				return ProgressTotal{}, nil, fmt.Errorf("verify unacknowledged deletes below %q: non-increasing or out-of-prefix key %q", prefix, key)
			}
			cursor = key
			if key > page.last {
				pastPage = true
				break
			}
			if _, ok := remains[key]; ok {
				remains[key] = true
			}
		}
		if pastPage || !aws.ToBool(response.IsTruncated) {
			break
		}
		if len(response.Contents) == 0 {
			return ProgressTotal{}, nil, fmt.Errorf("verify unacknowledged deletes below %q: truncated page has no keys", prefix)
		}
	}
	verified := ProgressTotal{}
	var failures []DeleteFailure
	for _, entry := range pending {
		if remains[entry.Key] {
			failures = append(failures, DeleteFailure{Key: entry.Key, Code: "Unconfirmed", Message: "S3 did not acknowledge deletion and the object remains"})
			continue
		}
		verified.Objects++
		verified.Bytes += entry.Size
	}
	return verified, failures, nil
}

func firstRemainingEntry(ctx context.Context, client ObjectLister, bucket, prefix string, versioning Versioning, exclusions []string, progress ProgressObserver) (*ObjectEntry, error) {
	if versioning == Versioned {
		return firstRemainingVersion(ctx, client, bucket, prefix, exclusions, progress)
	}
	return firstRemainingObject(ctx, client, bucket, prefix, exclusions, progress)
}

func firstRemainingObject(ctx context.Context, client ObjectLister, bucket, prefix string, exclusions []string, progress ProgressObserver) (*ObjectEntry, error) {
	var token *string
	pages := 0
	for {
		response, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix), EncodingType: types.EncodingTypeUrl, ContinuationToken: token})
		if err != nil {
			return nil, fmt.Errorf("list objects below %q: %w", prefix, err)
		}
		if response == nil {
			return nil, fmt.Errorf("list objects below %q: empty response", prefix)
		}
		if err := decodeListObjectsV2(response); err != nil {
			return nil, fmt.Errorf("list objects below %q: decode URL-encoded keys: %w", prefix, err)
		}
		pages++
		setProgressPhaseQuiet(progress, fmt.Sprintf("verifying prefix %q: %d pages checked", prefix, pages))
		for _, object := range response.Contents {
			key := aws.ToString(object.Key)
			if !IsExcluded(key, exclusions) {
				return &ObjectEntry{Key: key, Size: aws.ToInt64(object.Size)}, nil
			}
		}
		if !aws.ToBool(response.IsTruncated) {
			return nil, nil
		}
		if response.NextContinuationToken == nil {
			return nil, fmt.Errorf("list objects below %q: truncated response has no continuation token", prefix)
		}
		token = response.NextContinuationToken
	}
}

func firstRemainingVersion(ctx context.Context, client ObjectLister, bucket, prefix string, exclusions []string, progress ProgressObserver) (*ObjectEntry, error) {
	var keyMarker, versionMarker *string
	pages := 0
	for {
		response, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket), Prefix: aws.String(prefix), EncodingType: types.EncodingTypeUrl, KeyMarker: keyMarker, VersionIdMarker: versionMarker})
		if err != nil {
			return nil, fmt.Errorf("list versions below %q: %w", prefix, err)
		}
		if response == nil {
			return nil, fmt.Errorf("list versions below %q: empty response", prefix)
		}
		if err := decodeListObjectVersions(response); err != nil {
			return nil, fmt.Errorf("list versions below %q: decode URL-encoded keys: %w", prefix, err)
		}
		pages++
		setProgressPhaseQuiet(progress, fmt.Sprintf("verifying prefix %q: %d pages checked", prefix, pages))
		for _, object := range response.Versions {
			key := aws.ToString(object.Key)
			if !IsExcluded(key, exclusions) {
				return &ObjectEntry{Key: key, VersionID: aws.ToString(object.VersionId), Size: aws.ToInt64(object.Size)}, nil
			}
		}
		for _, marker := range response.DeleteMarkers {
			key := aws.ToString(marker.Key)
			if !IsExcluded(key, exclusions) {
				return &ObjectEntry{Key: key, VersionID: aws.ToString(marker.VersionId), DeleteMarker: true}, nil
			}
		}
		if !aws.ToBool(response.IsTruncated) {
			return nil, nil
		}
		if response.NextKeyMarker == nil || response.NextVersionIdMarker == nil {
			return nil, fmt.Errorf("list versions below %q: truncated response has no markers", prefix)
		}
		keyMarker, versionMarker = response.NextKeyMarker, response.NextVersionIdMarker
	}
}
