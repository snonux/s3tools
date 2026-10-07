package internal

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const deleteBatchSize = 1000

// ObjectDeleter provides the S3 APIs required to delete and verify a prefix.
type ObjectDeleter interface {
	ObjectLister
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
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
	failures := failureText(err.Summary.Failures)
	if err.Cause != nil {
		return fmt.Sprintf("partial deletion: %d object failures%s: cause=%s", len(err.Summary.Failures), failures, strconv.Quote(err.Cause.Error()))
	}
	return fmt.Sprintf("partial deletion: %d object failures%s", len(err.Summary.Failures), failures)
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
	setProgressPhase(progress, fmt.Sprintf("inventorying prefix %q (1/1)", prefix))
	inventory, err := InventoryPrefixWithProgress(ctx, client, bucket, prefix, versioning, exclusions, func(scanned InventoryProgress) {
		setProgressPhaseQuiet(progress, fmt.Sprintf("inventorying prefix %q (1/1): %d pages, %d objects (%s) scanned; total pending", prefix, scanned.Pages, scanned.Objects, formatBytes(scanned.Bytes)))
	})
	if err != nil {
		setProgressPhase(progress, "deletion stopped")
		return DeleteSummary{}, err
	}
	if progress != nil {
		progress.Start(ctx, ProgressTotal{Objects: len(inventory.Entries), Bytes: inventory.Bytes})
		defer progress.Finish()
	}

	summary, err := deleteInventory(ctx, client, bucket, prefix, inventory, versioning, exclusions, progress)
	if err != nil {
		setProgressPhase(progress, "deletion stopped")
	} else {
		setProgressPhase(progress, "deletion complete")
	}
	return summary, err
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
		setProgressPhase(progress, fmt.Sprintf("inventorying prefix %q (%d/%d); completed %d objects (%s)", prefix, index+1, len(prefixes), total.Objects, formatBytes(total.Bytes)))
		inventory, err := InventoryPrefixWithProgress(ctx, client, bucket, prefix, versioning, exclusions, func(scanned InventoryProgress) {
			setProgressPhaseQuiet(progress, fmt.Sprintf("inventorying prefix %q (%d/%d): %d pages, %d objects (%s) scanned here; %d objects (%s) in earlier prefixes; total pending", prefix, index+1, len(prefixes), scanned.Pages, scanned.Objects, formatBytes(scanned.Bytes), total.Objects, formatBytes(total.Bytes)))
		})
		if err != nil {
			setProgressPhase(progress, "deletion stopped")
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
			setProgressPhase(progress, "deletion stopped")
			var partial *PartialDeleteError
			if errors.As(err, &partial) && partial != nil {
				partial.Summary = summary
			}
			return summary, err
		}
	}
	setProgressPhase(progress, "deletion complete")

	return summary, nil
}

func deleteInventory(ctx context.Context, client ObjectDeleter, bucket, prefix string, inventory Inventory, versioning Versioning, exclusions []string, progress ProgressObserver) (DeleteSummary, error) {

	summary := DeleteSummary{}
	unacknowledged := make([]ObjectEntry, 0)
	for start := 0; start < len(inventory.Entries); start += deleteBatchSize {
		if err := ctx.Err(); err != nil {
			return summary, &PartialDeleteError{Summary: summary, Cause: err}
		}
		end := start + deleteBatchSize
		if end > len(inventory.Entries) {
			end = len(inventory.Entries)
		}
		setProgressPhase(progress, fmt.Sprintf("deleting prefix %q, batch %d/%d", prefix, start/deleteBatchSize+1, (len(inventory.Entries)+deleteBatchSize-1)/deleteBatchSize))
		before := summary
		pending, err := deleteBatch(ctx, client, bucket, inventory.Entries[start:end], versioning, &summary)
		// A mixed batch can confirm deletions before an individual request
		// fails. Publish that work before returning the partial failure.
		if progress != nil {
			progress.Advance(ProgressTotal{Objects: summary.Deleted - before.Deleted, Bytes: summary.DeletedBytes - before.DeletedBytes})
		}
		if err != nil {
			return summary, &PartialDeleteError{Summary: summary, Cause: err}
		}
		unacknowledged = append(unacknowledged, pending...)
	}

	if err := ctx.Err(); err != nil {
		return summary, &PartialDeleteError{Summary: summary, Cause: err}
	}
	setProgressPhase(progress, fmt.Sprintf("verifying prefix %q", prefix))
	remaining, err := InventoryPrefixWithProgress(ctx, client, bucket, prefix, versioning, exclusions, func(scanned InventoryProgress) {
		setProgressPhaseQuiet(progress, fmt.Sprintf("verifying prefix %q: %d pages, %d objects scanned", prefix, scanned.Pages, scanned.Objects))
	})
	if err != nil {
		return summary, &PartialDeleteError{Summary: summary, Cause: fmt.Errorf("verify deletion: %w", err)}
	}
	remainingKeys := make(map[string]struct{}, len(remaining.Entries))
	for _, entry := range remaining.Entries {
		remainingKeys[entry.Key] = struct{}{}
	}
	verified := ProgressTotal{}
	for _, entry := range unacknowledged {
		if _, remains := remainingKeys[entry.Key]; remains {
			summary.Failures = append(summary.Failures, DeleteFailure{Key: entry.Key, Code: "Unconfirmed", Message: "S3 did not acknowledge deletion and the object remains"})
			continue
		}
		summary.Deleted++
		summary.DeletedBytes += entry.Size
		verified.Objects++
		verified.Bytes += entry.Size
	}
	if progress != nil && verified != (ProgressTotal{}) {
		progress.Advance(verified)
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

func setProgressPhase(progress ProgressObserver, phase string) {
	if renderer, ok := progress.(interface{ SetPhase(string) }); ok {
		renderer.SetPhase(phase)
	}
}

func setProgressPhaseQuiet(progress ProgressObserver, phase string) {
	if renderer, ok := progress.(interface{ SetPhaseQuiet(string) }); ok {
		renderer.SetPhaseQuiet(phase)
	}
}

func deleteBatch(ctx context.Context, client ObjectDeleter, bucket string, entries []ObjectEntry, versioning Versioning, summary *DeleteSummary) ([]ObjectEntry, error) {
	objects := make([]types.ObjectIdentifier, 0, len(entries))
	bulkEntries := make([]ObjectEntry, 0, len(entries))
	individualEntries := make([]ObjectEntry, 0)
	for _, entry := range entries {
		if !xml10SafeValue(entry.Key) || (versioning == Versioned && !xml10SafeValue(entry.VersionID)) {
			individualEntries = append(individualEntries, entry)
			continue
		}
		object := types.ObjectIdentifier{Key: aws.String(entry.Key)}
		if versioning == Versioned {
			object.VersionId = aws.String(entry.VersionID)
		}
		objects = append(objects, object)
		bulkEntries = append(bulkEntries, entry)
	}

	unacknowledged := make([]ObjectEntry, 0)
	if len(objects) != 0 {
		pending, err := deleteObjectsBatch(ctx, client, bucket, objects, bulkEntries, versioning, summary)
		if err != nil {
			return nil, err
		}
		unacknowledged = append(unacknowledged, pending...)
	}

	for _, entry := range individualEntries {
		if err := deleteIndividualObject(ctx, client, bucket, entry, versioning); err != nil {
			return nil, err
		}
		summary.Deleted++
		summary.DeletedBytes += entry.Size
	}

	return unacknowledged, nil
}

func deleteObjectsBatch(ctx context.Context, client ObjectDeleter, bucket string, objects []types.ObjectIdentifier, entries []ObjectEntry, versioning Versioning, summary *DeleteSummary) ([]ObjectEntry, error) {
	result, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(false)}})
	if err != nil {
		return nil, fmt.Errorf("delete batch: %w", err)
	}
	if result == nil {
		return nil, fmt.Errorf("delete batch: empty response")
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
	unacknowledged := make([]ObjectEntry, 0)
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
		} else if versioning == Unversioned {
			// Some compatible S3 gateways (including Garage) omit successful
			// entries from the multi-delete response. The final inventory is the
			// authoritative success check for these objects.
			unacknowledged = append(unacknowledged, entry)
		} else {
			summary.Failures = append(summary.Failures, DeleteFailure{Key: entry.Key, VersionID: entry.VersionID, Code: "Unconfirmed", Message: "S3 did not acknowledge deletion"})
		}
	}
	return unacknowledged, nil
}

func deleteIndividualObject(ctx context.Context, client ObjectDeleter, bucket string, entry ObjectEntry, versioning Versioning) error {
	input := &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(entry.Key)}
	if versioning == Versioned {
		input.VersionId = aws.String(entry.VersionID)
	}
	result, err := client.DeleteObject(ctx, input)
	if err != nil {
		return fmt.Errorf("delete object %q: %w", entry.Key, err)
	}
	if result == nil {
		return fmt.Errorf("delete object %q: empty response", entry.Key)
	}
	return nil
}

// xml10SafeValue excludes carriage returns because XML normalizes them, as well
// as characters XML 1.0 cannot represent. Those values need a path-based delete.
func xml10SafeValue(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if character == '\r' || !(character == '\t' || character == '\n' ||
			character >= 0x20 && character <= 0xD7FF ||
			character >= 0xE000 && character <= 0xFFFD ||
			character >= 0x10000 && character <= 0x10FFFF) {
			return false
		}
	}
	return true
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
		part := " key=" + strconv.Quote(failure.Key)
		if failure.VersionID != "" {
			part += " version_id=" + strconv.Quote(failure.VersionID)
		}
		part += " code=" + strconv.Quote(failure.Code)
		part += " message=" + strconv.Quote(failure.Message)
		parts = append(parts, part)
	}
	if len(parts) == 0 {
		return ""
	}
	return ":" + strings.Join(parts, ";")
}
