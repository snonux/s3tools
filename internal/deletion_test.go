package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type fakeDeleter struct {
	versionPages  []*s3.ListObjectVersionsOutput
	objectPages   []*s3.ListObjectsV2Output
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

func TestDeletePrefixRequiresSuccessAcknowledgement(t *testing.T) {
	client := &fakeDeleter{
		objectPages: []*s3.ListObjectsV2Output{
			{Contents: []types.Object{{Key: aws.String("p/a"), Size: aws.Int64(2)}}},
			{},
		},
		deleteOutputs: []*s3.DeleteObjectsOutput{{}},
	}
	summary, err := DeletePrefix(context.Background(), client, "bucket", "p/", Unversioned, nil)
	if err == nil || len(summary.Failures) != 1 || summary.Failures[0].Code != "Unconfirmed" {
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
