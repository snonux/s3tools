package internal

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const deleteBatchSize = 1000

// ObjectDeleter provides the S3 APIs required to delete and verify a prefix.
type ObjectDeleter interface {
	ObjectLister
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

// DeleteFailure identifies an object S3 did not delete.
type DeleteFailure struct {
	Key       string
	VersionID string
	Code      string
	Message   string
}

// DeleteSummary reports confirmed deletions and failures.
type DeleteSummary struct {
	Deleted      int
	DeletedBytes int64
	Failures     []DeleteFailure
}

// PartialDeleteError reports a deletion that did not fully complete.
type PartialDeleteError struct {
	Summary DeleteSummary
	Cause   error
}

// Error implements error.
func (err *PartialDeleteError) Error() string {
	if err.Cause != nil {
		return fmt.Sprintf("partial deletion: %d object failures: %v", len(err.Summary.Failures), err.Cause)
	}
	return fmt.Sprintf("partial deletion: %d object failures", len(err.Summary.Failures))
}

// Unwrap returns the underlying request or verification error.
func (err *PartialDeleteError) Unwrap() error { return err.Cause }

// DeletePrefix deletes eligible entries under prefix and verifies none remain.
func DeletePrefix(ctx context.Context, client ObjectDeleter, bucket, prefix string, versioning Versioning, exclusions []string) (DeleteSummary, error) {
	return DeletePrefixWithProgress(ctx, client, bucket, prefix, versioning, exclusions, nil)
}

// DeletePrefixWithProgress deletes one prefix and reports its pre-deletion
// inventory and each batch S3 confirms.
func DeletePrefixWithProgress(ctx context.Context, client ObjectDeleter, bucket, prefix string, versioning Versioning, exclusions []string, progress ProgressObserver) (DeleteSummary, error) {
	if err := ctx.Err(); err != nil {
		return DeleteSummary{}, &PartialDeleteError{Cause: err}
	}
	inventory, err := InventoryPrefix(ctx, client, bucket, prefix, versioning, exclusions)
	if err != nil {
		return DeleteSummary{}, err
	}
	if progress != nil {
		progress.Start(ctx, ProgressTotal{Objects: len(inventory.Entries), Bytes: inventory.Bytes})
		defer progress.Finish()
	}

	return deleteInventory(ctx, client, bucket, prefix, inventory, versioning, exclusions, progress)
}

// DeleteSelectedPrefixes inventories all selected prefixes before deletion,
// then reports a single progress stream across the selected work.
func DeleteSelectedPrefixes(ctx context.Context, client ObjectDeleter, bucket string, prefixes []string, versioning Versioning, exclusions []string, progress ProgressObserver) (DeleteSummary, error) {
	if err := ctx.Err(); err != nil {
		return DeleteSummary{}, &PartialDeleteError{Cause: err}
	}
	inventories := make([]Inventory, len(prefixes))
	total := ProgressTotal{}
	for index, prefix := range prefixes {
		inventory, err := InventoryPrefix(ctx, client, bucket, prefix, versioning, exclusions)
		if err != nil {
			return DeleteSummary{}, err
		}
		inventories[index] = inventory
		total.Objects += len(inventory.Entries)
		total.Bytes += inventory.Bytes
	}
	if progress != nil {
		progress.Start(ctx, total)
		defer progress.Finish()
	}

	summary := DeleteSummary{}
	for index, prefix := range prefixes {
		deleted, err := deleteInventory(ctx, client, bucket, prefix, inventories[index], versioning, exclusions, progress)
		summary.Deleted += deleted.Deleted
		summary.DeletedBytes += deleted.DeletedBytes
		summary.Failures = append(summary.Failures, deleted.Failures...)
		if err != nil {
			return summary, err
		}
	}

	return summary, nil
}

func deleteInventory(ctx context.Context, client ObjectDeleter, bucket, prefix string, inventory Inventory, versioning Versioning, exclusions []string, progress ProgressObserver) (DeleteSummary, error) {

	summary := DeleteSummary{}
	for start := 0; start < len(inventory.Entries); start += deleteBatchSize {
		if err := ctx.Err(); err != nil {
			return summary, &PartialDeleteError{Summary: summary, Cause: err}
		}
		end := start + deleteBatchSize
		if end > len(inventory.Entries) {
			end = len(inventory.Entries)
		}
		before := summary
		if err := deleteBatch(ctx, client, bucket, inventory.Entries[start:end], versioning, &summary); err != nil {
			return summary, &PartialDeleteError{Summary: summary, Cause: err}
		}
		if progress != nil {
			progress.Advance(ProgressTotal{Objects: summary.Deleted - before.Deleted, Bytes: summary.DeletedBytes - before.DeletedBytes})
		}
	}

	if err := ctx.Err(); err != nil {
		return summary, &PartialDeleteError{Summary: summary, Cause: err}
	}
	remaining, err := InventoryPrefix(ctx, client, bucket, prefix, versioning, exclusions)
	if err != nil {
		return summary, &PartialDeleteError{Summary: summary, Cause: fmt.Errorf("verify deletion: %w", err)}
	}
	if len(summary.Failures) != 0 || len(remaining.Entries) != 0 {
		cause := error(nil)
		if len(remaining.Entries) != 0 {
			cause = fmt.Errorf("%d eligible objects remain", len(remaining.Entries))
		}
		return summary, &PartialDeleteError{Summary: summary, Cause: cause}
	}

	return summary, nil
}

func deleteBatch(ctx context.Context, client ObjectDeleter, bucket string, entries []ObjectEntry, versioning Versioning, summary *DeleteSummary) error {
	objects := make([]types.ObjectIdentifier, 0, len(entries))
	for _, entry := range entries {
		object := types.ObjectIdentifier{Key: aws.String(entry.Key)}
		if versioning == Versioned {
			object.VersionId = aws.String(entry.VersionID)
		}
		objects = append(objects, object)
	}

	result, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(false)}})
	if err != nil {
		return fmt.Errorf("delete batch: %w", err)
	}
	if result == nil {
		return fmt.Errorf("delete batch: empty response")
	}

	failed := make(map[string]struct{}, len(result.Errors))
	failedKeys := make(map[string]struct{}, len(result.Errors))
	unknownVersionFailures := make(map[string]DeleteFailure, len(result.Errors))
	confirmed := make(map[string]struct{}, len(result.Deleted))
	confirmedKeys := make(map[string]struct{}, len(result.Deleted))
	for _, deleted := range result.Deleted {
		key, versionID := aws.ToString(deleted.Key), aws.ToString(deleted.VersionId)
		confirmed[deletionKey(key, versionID)] = struct{}{}
		if versionID == "" {
			confirmedKeys[key] = struct{}{}
		}
	}
	for _, failure := range result.Errors {
		key, versionID := aws.ToString(failure.Key), aws.ToString(failure.VersionId)
		failureInfo := DeleteFailure{Key: key, VersionID: versionID, Code: aws.ToString(failure.Code), Message: aws.ToString(failure.Message)}
		if versionID == "" && versioning == Versioned {
			failedKeys[key] = struct{}{}
			unknownVersionFailures[key] = failureInfo
			continue
		}
		failed[deletionKey(key, versionID)] = struct{}{}
		summary.Failures = append(summary.Failures, failureInfo)
	}
	for _, entry := range entries {
		_, failedByID := failed[deletionKey(entry.Key, entry.VersionID)]
		_, failedByKey := failedKeys[entry.Key]
		_, confirmedByID := confirmed[deletionKey(entry.Key, entry.VersionID)]
		_, confirmedByKey := confirmedKeys[entry.Key]
		if failedByID {
			continue
		}
		if confirmedByID || (versioning == Unversioned && confirmedByKey) {
			summary.Deleted++
			summary.DeletedBytes += entry.Size
		} else if failedByKey {
			failure := unknownVersionFailures[entry.Key]
			failure.VersionID = entry.VersionID
			summary.Failures = append(summary.Failures, failure)
		} else {
			summary.Failures = append(summary.Failures, DeleteFailure{Key: entry.Key, VersionID: entry.VersionID, Code: "Unconfirmed", Message: "S3 did not acknowledge deletion"})
		}
	}

	return nil
}

func deletionKey(key, versionID string) string { return key + "\x00" + versionID }

var _ error = (*PartialDeleteError)(nil)

func hasFailure(failures []DeleteFailure, key, versionID string) bool {
	for _, failure := range failures {
		if failure.Key == key && failure.VersionID == versionID {
			return true
		}
	}

	return false
}

func failureText(failures []DeleteFailure) string {
	parts := make([]string, 0, len(failures))
	for _, failure := range failures {
		parts = append(parts, failure.Key+":"+failure.Code)
	}
	return strings.Join(parts, ", ")
}
