package internal

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type fakeDeleter struct {
	mu                 sync.Mutex
	listVersions       func(context.Context, *s3.ListObjectVersionsInput) (*s3.ListObjectVersionsOutput, error)
	listObjects        func(context.Context, *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error)
	deleteObjects      func(context.Context, *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error)
	versionPages       []*s3.ListObjectVersionsOutput
	objectPages        []*s3.ListObjectsV2Output
	objectErrors       []error
	deleteOutputs      []*s3.DeleteObjectsOutput
	deleteErr          error
	deleteObjectErr    error
	deleteObjectErrors []error
	deleteInputs       []*s3.DeleteObjectsInput
	singleDeleteInputs []*s3.DeleteObjectInput
}

func (client *fakeDeleter) ListObjectVersions(ctx context.Context, input *s3.ListObjectVersionsInput, _ ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if client.listVersions != nil {
		return client.listVersions(ctx, input)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.versionPages) == 0 {
		return &s3.ListObjectVersionsOutput{}, nil
	}
	page := client.versionPages[0]
	client.versionPages = client.versionPages[1:]
	return page, nil
}

func (client *fakeDeleter) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if client.listObjects != nil {
		return client.listObjects(ctx, input)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.objectErrors) > 0 {
		err := client.objectErrors[0]
		client.objectErrors = client.objectErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	if len(client.objectPages) == 0 {
		return &s3.ListObjectsV2Output{}, nil
	}
	page := client.objectPages[0]
	client.objectPages = client.objectPages[1:]
	return page, nil
}

func (client *fakeDeleter) DeleteObjects(ctx context.Context, input *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if client.deleteObjects != nil {
		return client.deleteObjects(ctx, input)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	client.deleteInputs = append(client.deleteInputs, input)
	if client.deleteErr != nil {
		return nil, client.deleteErr
	}
	output := client.deleteOutputs[0]
	client.deleteOutputs = client.deleteOutputs[1:]
	return output, nil
}

func (client *fakeDeleter) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	client.singleDeleteInputs = append(client.singleDeleteInputs, input)
	if len(client.deleteObjectErrors) > 0 {
		err := client.deleteObjectErrors[0]
		client.deleteObjectErrors = client.deleteObjectErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	if client.deleteObjectErr != nil {
		return nil, client.deleteObjectErr
	}
	return &s3.DeleteObjectOutput{}, nil
}

var _ ObjectDeleter = (*fakeDeleter)(nil)

func TestDeletePrefixVersioned(t *testing.T) {
	client := &fakeDeleter{
		versionPages: []*s3.ListObjectVersionsOutput{
			{Versions: []types.ObjectVersion{{Key: aws.String("p/a"), VersionId: aws.String("v1"), Size: aws.Int64(4)}}, DeleteMarkers: []types.DeleteMarkerEntry{{Key: aws.String("p/dead"), VersionId: aws.String("v2")}}},
			{},
		},
		deleteOutputs: []*s3.DeleteObjectsOutput{{Deleted: []types.DeletedObject{{Key: aws.String("p/a"), VersionId: aws.String("v1")}, {Key: aws.String("p/dead"), VersionId: aws.String("v2")}}}},
	}
	summary, err := DeletePrefix(context.Background(), client, "bucket", "p/", Versioned, nil)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Deleted != 2 || summary.DeletedBytes != 4 {
		t.Fatalf("summary = %#v", summary)
	}
	objects := client.deleteInputs[0].Delete.Objects
	if len(objects) != 2 || aws.ToString(objects[0].VersionId) != "v1" || aws.ToString(objects[1].VersionId) != "v2" {
		t.Fatalf("delete objects = %#v", objects)
	}
}

func TestDeletePrefixPartialFailure(t *testing.T) {
	client := &fakeDeleter{
		objectPages: []*s3.ListObjectsV2Output{
			{Contents: []types.Object{{Key: aws.String("p/a"), Size: aws.Int64(3)}}},
			{Contents: []types.Object{{Key: aws.String("p/a"), Size: aws.Int64(3)}}},
		},
		deleteOutputs: []*s3.DeleteObjectsOutput{{Errors: []types.Error{{Key: aws.String("p/a"), Code: aws.String("AccessDenied")}}}},
	}
	summary, err := DeletePrefix(context.Background(), client, "bucket", "p/", Unversioned, nil)
	if err == nil || len(summary.Failures) != 1 || !hasFailure(summary.Failures, "p/a", "") {
		t.Fatalf("summary = %#v, error = %v", summary, err)
	}
}

func TestDeletePrefixReportsIndividualDeleteRequestFailure(t *testing.T) {
	requestErr := errors.New("offline")
	client := &fakeDeleter{
		objectPages:     []*s3.ListObjectsV2Output{{Contents: []types.Object{{Key: aws.String("p/control\x01.dat"), Size: aws.Int64(3)}}}},
		deleteObjectErr: requestErr,
	}

	summary, err := DeletePrefix(context.Background(), client, "bucket", "p/", Unversioned, nil)
	if !errors.Is(err, requestErr) {
		t.Fatalf("DeletePrefix() error = %v, want wrapped request error", err)
	}
	if summary.Deleted != 0 || summary.DeletedBytes != 0 || len(client.deleteInputs) != 0 || len(client.singleDeleteInputs) != 1 {
		t.Fatalf("summary = %#v, bulk deletes = %d, individual deletes = %d", summary, len(client.deleteInputs), len(client.singleDeleteInputs))
	}
}

func TestDeleteSelectedPrefixesPartialFailureKeepsAggregateSummary(t *testing.T) {
	client := &fakeDeleter{
		objectPages: []*s3.ListObjectsV2Output{
			{Contents: []types.Object{{Key: aws.String("a/one"), Size: aws.Int64(10)}}},
			{},
			{Contents: []types.Object{{Key: aws.String("b/two"), Size: aws.Int64(20)}}},
			{Contents: []types.Object{{Key: aws.String("b/two"), Size: aws.Int64(20)}}},
		},
		deleteOutputs: []*s3.DeleteObjectsOutput{
			{Deleted: []types.DeletedObject{{Key: aws.String("a/one")}}},
			{Errors: []types.Error{{Key: aws.String("b/two"), Code: aws.String("AccessDenied")}}},
		},
	}

	summary, err := DeleteSelectedPrefixes(context.Background(), client, "bucket", []string{"a/", "b/"}, Unversioned, nil, nil)
	if err == nil {
		t.Fatal("expected partial deletion error")
	}
	var partial *PartialDeleteError
	if !errors.As(err, &partial) {
		t.Fatalf("error %T does not contain PartialDeleteError", err)
	}
	want := DeleteSummary{
		Deleted:      1,
		DeletedBytes: 10,
		Failures:     []DeleteFailure{{Key: "b/two", Code: "AccessDenied"}},
	}
	if !reflect.DeepEqual(summary, want) {
		t.Fatalf("summary = %#v, want %#v", summary, want)
	}
	if !reflect.DeepEqual(partial.Summary, summary) {
		t.Fatalf("partial summary = %#v, returned summary = %#v", partial.Summary, summary)
	}
}

func TestDeleteSelectedPrefixesCancellationKeepsAggregateSummaryAndCause(t *testing.T) {
	client := &fakeDeleter{
		objectPages: []*s3.ListObjectsV2Output{
			{Contents: []types.Object{{Key: aws.String("a/one"), Size: aws.Int64(10)}}},
			{},
			{Contents: []types.Object{{Key: aws.String("b/two"), Size: aws.Int64(20)}}},
			{},
		},
		deleteOutputs: []*s3.DeleteObjectsOutput{
			{Deleted: []types.DeletedObject{{Key: aws.String("a/one")}}},
			{Deleted: []types.DeletedObject{{Key: aws.String("b/two")}}},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	advances := 0
	progress := &recordingProgress{onAdvance: func() {
		advances++
		if advances == 2 {
			cancel()
		}
	}}

	summary, err := DeleteSelectedPrefixes(ctx, client, "bucket", []string{"a/", "b/"}, Unversioned, nil, progress)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	var partial *PartialDeleteError
	if !errors.As(err, &partial) {
		t.Fatalf("error %T does not contain PartialDeleteError", err)
	}
	want := DeleteSummary{Deleted: 2, DeletedBytes: 30}
	if !reflect.DeepEqual(summary, want) {
		t.Fatalf("summary = %#v, want %#v", summary, want)
	}
	if !reflect.DeepEqual(partial.Summary, summary) {
		t.Fatalf("partial summary = %#v, returned summary = %#v", partial.Summary, summary)
	}
	if partial.Cause != context.Canceled {
		t.Fatalf("partial cause = %v, want %v", partial.Cause, context.Canceled)
	}
}

func TestDeletePrefixBatchesAndRequestFailure(t *testing.T) {
	entries := make([]types.ObjectVersion, deleteBatchSize+1)
	for index := range entries {
		entries[index] = types.ObjectVersion{Key: aws.String("p/" + strconv.Itoa(index)), VersionId: aws.String("v"), Size: aws.Int64(1)}
	}
	deleted := make([]types.DeletedObject, 0, len(entries))
	for _, entry := range entries {
		deleted = append(deleted, types.DeletedObject{Key: entry.Key, VersionId: entry.VersionId})
	}
	client := &fakeDeleter{versionPages: []*s3.ListObjectVersionsOutput{{Versions: entries}, {}}, deleteOutputs: []*s3.DeleteObjectsOutput{{Deleted: deleted[:deleteBatchSize]}, {Deleted: deleted[deleteBatchSize:]}}}
	if _, err := DeletePrefix(context.Background(), client, "bucket", "p/", Versioned, nil); err != nil {
		t.Fatal(err)
	}
	if len(client.deleteInputs) != 2 || len(client.deleteInputs[0].Delete.Objects) != deleteBatchSize || len(client.deleteInputs[1].Delete.Objects) != 1 {
		t.Fatal("batching failed")
	}

	broken := &fakeDeleter{objectPages: []*s3.ListObjectsV2Output{{Contents: []types.Object{{Key: aws.String("p/a")}}}}, deleteErr: errors.New("offline")}
	if _, err := DeletePrefix(context.Background(), broken, "bucket", "p/", Unversioned, nil); err == nil {
		t.Fatal("expected request failure")
	}
}

func TestDeletePrefixCancellationAndVersionlessFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cancelled := &fakeDeleter{}
	if _, err := DeletePrefix(ctx, cancelled, "bucket", "p/", Unversioned, nil); err == nil || len(cancelled.deleteInputs) != 0 {
		t.Fatalf("cancellation error = %v, delete calls = %d", err, len(cancelled.deleteInputs))
	}

	client := &fakeDeleter{
		versionPages: []*s3.ListObjectVersionsOutput{
			{Versions: []types.ObjectVersion{{Key: aws.String("p/a"), VersionId: aws.String("v1"), Size: aws.Int64(5)}}},
			{Versions: []types.ObjectVersion{{Key: aws.String("p/a"), VersionId: aws.String("v1"), Size: aws.Int64(5)}}},
		},
		deleteOutputs: []*s3.DeleteObjectsOutput{{Errors: []types.Error{{Key: aws.String("p/a"), Code: aws.String("AccessDenied")}}}},
	}
	summary, err := DeletePrefix(context.Background(), client, "bucket", "p/", Versioned, nil)
	if err == nil || summary.Deleted != 0 || summary.DeletedBytes != 0 {
		t.Fatalf("summary = %#v, error = %v", summary, err)
	}
}

func TestDeletePrefixAcceptsUnversionedGatewayWithoutDeleteAcknowledgement(t *testing.T) {
	client := &fakeDeleter{
		objectPages: []*s3.ListObjectsV2Output{
			{Contents: []types.Object{{Key: aws.String("p/a"), Size: aws.Int64(2)}}},
			{},
		},
		deleteOutputs: []*s3.DeleteObjectsOutput{{}},
	}
	summary, err := DeletePrefix(context.Background(), client, "bucket", "p/", Unversioned, nil)
	if err != nil || summary.Deleted != 1 || summary.DeletedBytes != 2 {
		t.Fatalf("summary = %#v, error = %v", summary, err)
	}
}

func TestDeletePrefixDoesNotCountUnacknowledgedDeleteWhenVerificationFails(t *testing.T) {
	client := &fakeDeleter{
		objectPages:   []*s3.ListObjectsV2Output{{Contents: []types.Object{{Key: aws.String("p/a"), Size: aws.Int64(2)}}}},
		objectErrors:  []error{nil, errors.New("listing unavailable")},
		deleteOutputs: []*s3.DeleteObjectsOutput{{}},
	}
	summary, err := DeletePrefix(context.Background(), client, "bucket", "p/", Unversioned, nil)
	if err == nil || summary.Deleted != 0 || summary.DeletedBytes != 0 {
		t.Fatalf("summary = %#v, error = %v", summary, err)
	}
}

func TestDeletePrefixCountsVerifiedUnacknowledgedDeleteWithOtherFailure(t *testing.T) {
	client := &fakeDeleter{
		objectPages: []*s3.ListObjectsV2Output{
			{Contents: []types.Object{{Key: aws.String("p/a"), Size: aws.Int64(2)}, {Key: aws.String("p/b"), Size: aws.Int64(3)}}},
			{Contents: []types.Object{{Key: aws.String("p/a"), Size: aws.Int64(2)}}},
		},
		deleteOutputs: []*s3.DeleteObjectsOutput{{Errors: []types.Error{{Key: aws.String("p/a"), Code: aws.String("AccessDenied")}}}},
	}
	summary, err := DeletePrefix(context.Background(), client, "bucket", "p/", Unversioned, nil)
	if err == nil || summary.Deleted != 1 || summary.DeletedBytes != 3 || !hasFailure(summary.Failures, "p/a", "") {
		t.Fatalf("summary = %#v, error = %v", summary, err)
	}
}

func TestDeletePrefixExpandsVersionlessErrors(t *testing.T) {
	client := &fakeDeleter{
		versionPages: []*s3.ListObjectVersionsOutput{
			{Versions: []types.ObjectVersion{{Key: aws.String("p/a"), VersionId: aws.String("v1")}, {Key: aws.String("p/a"), VersionId: aws.String("v2")}}},
			{Versions: []types.ObjectVersion{{Key: aws.String("p/a"), VersionId: aws.String("v1")}, {Key: aws.String("p/a"), VersionId: aws.String("v2")}}},
		},
		deleteOutputs: []*s3.DeleteObjectsOutput{{Errors: []types.Error{{Key: aws.String("p/a"), Code: aws.String("AccessDenied")}}}},
	}
	summary, err := DeletePrefix(context.Background(), client, "bucket", "p/", Versioned, nil)
	if err == nil || len(summary.Failures) != 2 || !hasFailure(summary.Failures, "p/a", "v1") || !hasFailure(summary.Failures, "p/a", "v2") {
		t.Fatalf("summary = %#v, error = %v", summary, err)
	}
}

func TestDeletePrefixDoesNotExpandFailureToConfirmedVersion(t *testing.T) {
	client := &fakeDeleter{
		versionPages: []*s3.ListObjectVersionsOutput{
			{Versions: []types.ObjectVersion{{Key: aws.String("p/a"), VersionId: aws.String("v1")}, {Key: aws.String("p/a"), VersionId: aws.String("v2")}}},
			{Versions: []types.ObjectVersion{{Key: aws.String("p/a"), VersionId: aws.String("v2")}}},
		},
		deleteOutputs: []*s3.DeleteObjectsOutput{{Deleted: []types.DeletedObject{{Key: aws.String("p/a"), VersionId: aws.String("v1")}}, Errors: []types.Error{{Key: aws.String("p/a"), Code: aws.String("AccessDenied")}}}},
	}
	summary, err := DeletePrefix(context.Background(), client, "bucket", "p/", Versioned, nil)
	if err == nil || summary.Deleted != 1 || len(summary.Failures) != 1 || !hasFailure(summary.Failures, "p/a", "v2") {
		t.Fatalf("summary = %#v, error = %v", summary, err)
	}
}

func TestDeletePrefixStartsDeletingBeforeListingFinishes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	listingRelease := make(chan struct{})
	deleteStarted := make(chan struct{})
	var listCalls atomic.Int32
	var deleteOnce sync.Once
	client := &fakeDeleter{
		listObjects: func(ctx context.Context, _ *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
			if listCalls.Add(1) == 1 {
				return &s3.ListObjectsV2Output{
					Contents:    []types.Object{{Key: aws.String("p/a"), Size: aws.Int64(4)}},
					IsTruncated: aws.Bool(true),
				}, nil
			}
			select {
			case <-listingRelease:
				return &s3.ListObjectsV2Output{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
		deleteObjects: func(_ context.Context, input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			deleteOnce.Do(func() { close(deleteStarted) })
			return &s3.DeleteObjectsOutput{Deleted: []types.DeletedObject{{Key: input.Delete.Objects[0].Key}}}, nil
		},
	}
	type deletionResult struct {
		summary DeleteSummary
		err     error
	}
	finished := make(chan deletionResult, 1)
	go func() {
		summary, err := DeletePrefix(ctx, client, "bucket", "p/", Unversioned, nil)
		finished <- deletionResult{summary, err}
	}()
	select {
	case <-deleteStarted:
		close(listingRelease)
	case result := <-finished:
		t.Fatalf("deletion returned before listing finished: %#v, %v", result.summary, result.err)
	case <-ctx.Done():
		t.Fatal("deletion did not start while listing was blocked")
	}
	select {
	case result := <-finished:
		if result.err != nil || result.summary.Deleted != 1 || result.summary.DeletedBytes != 4 {
			t.Fatalf("deletion = %#v, %v", result.summary, result.err)
		}
	case <-ctx.Done():
		t.Fatal("deletion did not complete")
	}
}

func TestDeletePrefixBoundsListedPagesWhileDeletionBlocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	firstDeleteRelease := make(chan struct{})
	fourthListed := make(chan struct{})
	var listCalls atomic.Int32
	var deleteCalls atomic.Int32
	client := &fakeDeleter{
		listObjects: func(_ context.Context, _ *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
			call := listCalls.Add(1)
			if call == 4 {
				close(fourthListed)
			}
			if call > 5 {
				return &s3.ListObjectsV2Output{}, nil
			}
			return &s3.ListObjectsV2Output{
				Contents:    []types.Object{{Key: aws.String("p/" + string(rune('a'+call-1))), Size: aws.Int64(1)}},
				IsTruncated: aws.Bool(call < 5),
			}, nil
		},
		deleteObjects: func(ctx context.Context, input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			if deleteCalls.Add(1) == 1 {
				select {
				case <-firstDeleteRelease:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return &s3.DeleteObjectsOutput{Deleted: []types.DeletedObject{{Key: input.Delete.Objects[0].Key}}}, nil
		},
	}
	finished := make(chan error, 1)
	go func() {
		_, err := DeletePrefix(ctx, client, "bucket", "p/", Unversioned, nil)
		finished <- err
	}()
	select {
	case <-fourthListed:
	case <-ctx.Done():
		t.Fatal("listing did not reach the bounded queue")
	}
	select {
	case <-time.After(50 * time.Millisecond):
		if got := listCalls.Load(); got != 4 {
			t.Fatalf("listing calls while deletion blocked = %d, want bounded at 4", got)
		}
	case err := <-finished:
		t.Fatalf("deletion finished while the first batch was blocked: %v", err)
	}
	close(firstDeleteRelease)
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("deletion did not finish after releasing the first batch")
	}
}

func TestDeletePrefixVersionedKeepsProtectedKeys(t *testing.T) {
	protected := types.ObjectVersion{Key: aws.String("p/protected/keep"), VersionId: aws.String("v1"), Size: aws.Int64(7)}
	client := &fakeDeleter{
		versionPages: []*s3.ListObjectVersionsOutput{
			{Versions: []types.ObjectVersion{{Key: aws.String("p/a"), VersionId: aws.String("v2"), Size: aws.Int64(3)}, protected}},
			{Versions: []types.ObjectVersion{protected}},
			{Versions: []types.ObjectVersion{protected}},
		},
		deleteOutputs: []*s3.DeleteObjectsOutput{{Deleted: []types.DeletedObject{{Key: aws.String("p/a"), VersionId: aws.String("v2")}}}},
	}
	summary, err := DeletePrefix(context.Background(), client, "bucket", "p/", Versioned, []string{"p/protected/"})
	if err != nil || summary.Deleted != 1 || summary.DeletedBytes != 3 {
		t.Fatalf("DeletePrefix() = %#v, %v", summary, err)
	}
	if len(client.deleteInputs) != 1 || len(client.deleteInputs[0].Delete.Objects) != 1 || aws.ToString(client.deleteInputs[0].Delete.Objects[0].Key) != "p/a" {
		t.Fatalf("delete inputs = %#v, protected key must remain", client.deleteInputs)
	}
}

func TestDeletePrefixVersionedRelistsSplitKeyAndAdvancesPastProtectedPages(t *testing.T) {
	versions := []types.ObjectVersion{
		{Key: aws.String("p/a"), VersionId: aws.String("v3"), Size: aws.Int64(1)},
		{Key: aws.String("p/a"), VersionId: aws.String("v2"), Size: aws.Int64(1)},
		{Key: aws.String("p/a"), VersionId: aws.String("v1"), Size: aws.Int64(1)},
		{Key: aws.String("p/protected/x"), VersionId: aws.String("v2"), Size: aws.Int64(7)},
		{Key: aws.String("p/protected/x"), VersionId: aws.String("v1"), Size: aws.Int64(7)},
		{Key: aws.String("p/protected/y"), VersionId: aws.String("v1"), Size: aws.Int64(7)},
		{Key: aws.String("p/z"), VersionId: aws.String("v1"), Size: aws.Int64(2)},
	}
	type markerRequest struct{ key, version string }
	var requests []markerRequest
	var deleted []markerRequest
	client := &fakeDeleter{
		listVersions: func(_ context.Context, input *s3.ListObjectVersionsInput) (*s3.ListObjectVersionsOutput, error) {
			marker := markerRequest{aws.ToString(input.KeyMarker), aws.ToString(input.VersionIdMarker)}
			requests = append(requests, marker)
			start := 0
			if marker.version != "" {
				start = len(versions)
				for index, version := range versions {
					if aws.ToString(version.Key) == marker.key && aws.ToString(version.VersionId) == marker.version {
						start = index + 1
						break
					}
				}
			} else if marker.key != "" {
				for start < len(versions) && aws.ToString(versions[start].Key) <= marker.key {
					start++
				}
			}
			end := min(start+2, len(versions))
			page := &s3.ListObjectVersionsOutput{Versions: slices.Clone(versions[start:end]), IsTruncated: aws.Bool(end < len(versions))}
			if aws.ToBool(page.IsTruncated) {
				last := page.Versions[len(page.Versions)-1]
				page.NextKeyMarker, page.NextVersionIdMarker = last.Key, last.VersionId
			}
			return page, nil
		},
		deleteObjects: func(_ context.Context, input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			output := &s3.DeleteObjectsOutput{}
			for _, object := range input.Delete.Objects {
				item := markerRequest{aws.ToString(object.Key), aws.ToString(object.VersionId)}
				deleted = append(deleted, item)
				for index, version := range versions {
					if aws.ToString(version.Key) == item.key && aws.ToString(version.VersionId) == item.version {
						versions = slices.Delete(versions, index, index+1)
						break
					}
				}
				output.Deleted = append(output.Deleted, types.DeletedObject{Key: object.Key, VersionId: object.VersionId})
			}
			return output, nil
		},
	}
	summary, err := DeletePrefix(context.Background(), client, "bucket", "p/", Versioned, []string{"p/protected/"})
	if err != nil || summary.Deleted != 4 || summary.DeletedBytes != 5 {
		t.Fatalf("DeletePrefix() = %#v, %v", summary, err)
	}
	for _, want := range []markerRequest{{"p/a", "v3"}, {"p/a", "v2"}, {"p/a", "v1"}, {"p/z", "v1"}} {
		if !slices.Contains(deleted, want) {
			t.Fatalf("deleted = %#v, missing %#v", deleted, want)
		}
	}
	if len(deleted) != 4 || len(versions) != 3 {
		t.Fatalf("deleted = %#v, remaining versions = %#v", deleted, versions)
	}
	if !slices.Contains(requests, markerRequest{"p/protected/x", ""}) || !slices.Contains(requests, markerRequest{"p/protected/x", "v1"}) {
		t.Fatalf("listing markers = %#v, want key-only stream marker and key/version verification marker", requests)
	}
}

func TestDeletePrefixVerifiesUnacknowledgedDeletesAcrossListedPages(t *testing.T) {
	objects := map[string]int64{"p/a": 1, "p/b": 2, "p/c": 3}
	var mu sync.Mutex
	var startAfters []string
	client := &fakeDeleter{
		listObjects: func(_ context.Context, input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
			mu.Lock()
			defer mu.Unlock()
			after := aws.ToString(input.StartAfter)
			if input.ContinuationToken == nil {
				startAfters = append(startAfters, after)
			}
			keys := make([]string, 0, len(objects))
			for key := range objects {
				if key > after {
					keys = append(keys, key)
				}
			}
			slices.Sort(keys)
			if len(keys) == 0 {
				return &s3.ListObjectsV2Output{}, nil
			}
			return &s3.ListObjectsV2Output{
				Contents:    []types.Object{{Key: aws.String(keys[0]), Size: aws.Int64(objects[keys[0]])}},
				IsTruncated: aws.Bool(len(keys) > 1),
			}, nil
		},
		deleteObjects: func(_ context.Context, input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			mu.Lock()
			defer mu.Unlock()
			for _, object := range input.Delete.Objects {
				delete(objects, aws.ToString(object.Key))
			}
			return &s3.DeleteObjectsOutput{}, nil // Compatible gateway omits success entries.
		},
	}
	summary, err := DeletePrefix(context.Background(), client, "bucket", "p/", Unversioned, nil)
	if err != nil || summary.Deleted != 3 || summary.DeletedBytes != 6 {
		t.Fatalf("DeletePrefix() = %#v, %v", summary, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(objects) != 0 {
		t.Fatalf("objects remaining = %#v", objects)
	}
	for _, cursor := range []string{"p/a", "p/b"} {
		if count := countString(startAfters, cursor); count < 2 {
			t.Fatalf("StartAfter requests = %#v, want streaming and page verification at %q", startAfters, cursor)
		}
	}
}

func TestDeletePrefixRejectsStaleUnversionedPage(t *testing.T) {
	var listCalls atomic.Int32
	var deleteCalls atomic.Int32
	client := &fakeDeleter{
		listObjects: func(_ context.Context, _ *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
			listCalls.Add(1)
			return &s3.ListObjectsV2Output{
				Contents:    []types.Object{{Key: aws.String("p/a"), Size: aws.Int64(1)}},
				IsTruncated: aws.Bool(true),
			}, nil
		},
		deleteObjects: func(_ context.Context, input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			deleteCalls.Add(1)
			return &s3.DeleteObjectsOutput{Deleted: []types.DeletedObject{{Key: input.Delete.Objects[0].Key}}}, nil
		},
	}
	_, err := DeletePrefix(context.Background(), client, "bucket", "p/", Unversioned, nil)
	if err == nil || !strings.Contains(err.Error(), "non-increasing") {
		t.Fatalf("DeletePrefix() error = %v, want stale page rejection", err)
	}
	if listCalls.Load() != 2 || deleteCalls.Load() > 1 {
		t.Fatalf("requests: lists=%d deletes=%d, want 2 lists and at most 1 delete", listCalls.Load(), deleteCalls.Load())
	}
}

func countString(values []string, want string) int {
	count := 0
	for _, value := range values {
		if value == want {
			count++
		}
	}
	return count
}
