package internal

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type diagnosticClient struct {
	fakeDeleter
}

func (client *diagnosticClient) GetBucketVersioning(context.Context, *s3.GetBucketVersioningInput, ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	return &s3.GetBucketVersioningOutput{Status: types.BucketVersioningStatusEnabled}, nil
}

var _ S3API = (*diagnosticClient)(nil)

func TestPartialDeleteErrorPreservesCauseAndEscapesFailureDetails(t *testing.T) {
	cause := errors.New("request\x1b[2J\r\ninterrupted")
	err := &PartialDeleteError{
		Summary: DeleteSummary{Failures: []DeleteFailure{{Key: "p/unsafe\x1b[2J", VersionID: "v\n1", Code: "AccessDenied", Message: "denied\r\ntry another credential"}}},
		Cause:   cause,
	}
	if !errors.Is(err, cause) {
		t.Fatal("PartialDeleteError no longer unwraps its cause")
	}
	var partial *PartialDeleteError
	if !errors.As(err, &partial) || partial != err {
		t.Fatalf("errors.As() = %v, want original PartialDeleteError", partial)
	}
	for _, want := range []string{
		`key="p/unsafe\x1b[2J"`,
		`version_id="v\n1"`,
		`code="AccessDenied"`,
		`message="denied\r\ntry another credential"`,
		`cause="request\x1b[2J\r\ninterrupted"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Error() = %q, missing %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "\x1b") || strings.Contains(err.Error(), "\r\n") {
		t.Fatalf("Error() contains unescaped control text: %q", err)
	}
}

func TestRunReportsEachVersionedS3Failure(t *testing.T) {
	t.Setenv("S3RM_BUCKET", "photos")
	t.Setenv("S3RM_REGION", "test-region")
	client := &diagnosticClient{fakeDeleter: fakeDeleter{
		versionPages: []*s3.ListObjectVersionsOutput{
			{CommonPrefixes: []types.CommonPrefix{{Prefix: aws.String("p/")}}},
			{Versions: []types.ObjectVersion{{Key: aws.String("p/one"), VersionId: aws.String("v1")}, {Key: aws.String("p/two"), VersionId: aws.String("v2")}}},
			{Versions: []types.ObjectVersion{{Key: aws.String("p/one"), VersionId: aws.String("v1")}, {Key: aws.String("p/two"), VersionId: aws.String("v2")}}},
		},
		deleteOutputs: []*s3.DeleteObjectsOutput{{Errors: []types.Error{
			{Key: aws.String("p/one"), VersionId: aws.String("v1"), Code: aws.String("AccessDenied"), Message: aws.String("policy denies one")},
			{Key: aws.String("p/two"), VersionId: aws.String("v2"), Code: aws.String("InvalidRequest"), Message: aws.String("policy denies two\x1b[2J")},
		}}},
	}}

	err := run(context.Background(), []string{"--prefix", "p/", "--confirm-bucket", "photos"}, false, false, func(context.Context, Config) (S3API, error) {
		return client, nil
	})
	if err == nil {
		t.Fatal("expected CLI error")
	}
	var partial *PartialDeleteError
	if !errors.As(err, &partial) {
		t.Fatalf("CLI error %T does not retain PartialDeleteError", err)
	}
	for _, want := range []string{
		`key="p/one" version_id="v1" code="AccessDenied" message="policy denies one"`,
		`key="p/two" version_id="v2" code="InvalidRequest" message="policy denies two\x1b[2J"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("CLI error = %q, missing %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "\x1b") {
		t.Fatalf("CLI error contains unescaped terminal control text: %q", err)
	}
}
