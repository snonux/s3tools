package internal

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type fakeDeleter struct {
	versionPages  []*s3.ListObjectVersionsOutput
	objectPages   []*s3.ListObjectsV2Output
	objectErrors  []error
	deleteOutputs []*s3.DeleteObjectsOutput
	deleteErr     error
	deleteInputs  []*s3.DeleteObjectsInput
}

func (client *fakeDeleter) ListObjectVersions(_ context.Context, _ *s3.ListObjectVersionsInput, _ ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error) {
	page := client.versionPages[0]
	client.versionPages = client.versionPages[1:]
	return page, nil
}

func (client *fakeDeleter) ListObjectsV2(_ context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if len(client.objectErrors) > 0 {
		err := client.objectErrors[0]
		client.objectErrors = client.objectErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	page := client.objectPages[0]
	client.objectPages = client.objectPages[1:]
	return page, nil
}

func (client *fakeDeleter) DeleteObjects(_ context.Context, input *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	client.deleteInputs = append(client.deleteInputs, input)
	if client.deleteErr != nil {
		return nil, client.deleteErr
	}
	output := client.deleteOutputs[0]
	client.deleteOutputs = client.deleteOutputs[1:]
	return output, nil
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

func TestDeleteSelectedPrefixesPartialFailureKeepsAggregateSummary(t *testing.T) {
	client := &fakeDeleter{
		objectPages: []*s3.ListObjectsV2Output{
			{Contents: []types.Object{{Key: aws.String("a/one"), Size: aws.Int64(10)}}},
			{Contents: []types.Object{{Key: aws.String("b/two"), Size: aws.Int64(20)}}},
			{},
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
		entries[index] = types.ObjectVersion{Key: aws.String("p/" + string(rune(index+1))), VersionId: aws.String("v"), Size: aws.Int64(1)}
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
