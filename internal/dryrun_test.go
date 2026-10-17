package internal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type dryRunClient struct {
	fakeDeleter
	versioning types.BucketVersioningStatus
}

func (client *dryRunClient) GetBucketVersioning(context.Context, *s3.GetBucketVersioningInput, ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	return &s3.GetBucketVersioningOutput{Status: client.versioning}, nil
}

func TestRunDryRunUnversioned(t *testing.T) {
	setTestConfig(t)
	client := &dryRunClient{fakeDeleter: fakeDeleter{objectPages: []*s3.ListObjectsV2Output{
		{CommonPrefixes: []types.CommonPrefix{{Prefix: aws.String("images/")}, {Prefix: aws.String("logs/")}}},
		{IsTruncated: aws.Bool(true), NextContinuationToken: aws.String("next"), Contents: []types.Object{
			{Key: aws.String("images/a"), Size: aws.Int64(4)},
			{Key: aws.String("images/keep/a"), Size: aws.Int64(90)},
		}},
		{Contents: []types.Object{
			{Key: aws.String("images/b"), Size: aws.Int64(6)},
			{Key: aws.String("images/keep.txt"), Size: aws.Int64(80)},
		}},
		{Contents: []types.Object{{Key: aws.String("logs/a"), Size: aws.Int64(2)}}},
	}}}
	var output bytes.Buffer
	err := runWithOutput(context.Background(), []string{
		"--dry-run", "--prefix=images/", "--prefix=logs/", "-exclude=images/keep/", "-exclude=images/keep.txt",
	}, false, false, func(context.Context, Config) (S3API, error) { return client, nil }, &output)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`Dry run for bucket "photos": nothing was deleted.`,
		`Would delete under "images/": 2 objects, 10 bytes`,
		`Would delete under "logs/": 1 objects, 2 bytes`,
		`Protected (excluded from totals): "images/keep/"`,
		`Protected (excluded from totals): "images/keep.txt"`,
		"Total eligible: 3 objects, 12 bytes",
		"status: dry run complete",
	} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("output missing %q: %s", want, output.String())
		}
	}
	assertDryRunReadOnly(t, client)
	if len(client.objectPages) != 0 {
		t.Fatal("dry run did not scan all pages and prefixes")
	}
}

func TestRunDryRunVersioned(t *testing.T) {
	for _, status := range []types.BucketVersioningStatus{types.BucketVersioningStatusEnabled, types.BucketVersioningStatusSuspended} {
		t.Run(string(status), func(t *testing.T) {
			setTestConfig(t)
			client := &dryRunClient{versioning: status, fakeDeleter: fakeDeleter{versionPages: []*s3.ListObjectVersionsOutput{
				{CommonPrefixes: []types.CommonPrefix{{Prefix: aws.String("images/")}}},
				{IsTruncated: aws.Bool(true), NextKeyMarker: aws.String("images/a"), NextVersionIdMarker: aws.String("v1"), Versions: []types.ObjectVersion{
					{Key: aws.String("images/a"), VersionId: aws.String("v2"), Size: aws.Int64(4)},
					{Key: aws.String("images/a"), VersionId: aws.String("v1"), Size: aws.Int64(6)},
					{Key: aws.String("images/keep"), VersionId: aws.String("v1"), Size: aws.Int64(90)},
				}},
				{DeleteMarkers: []types.DeleteMarkerEntry{
					{Key: aws.String("images/dead"), VersionId: aws.String("m1")},
					{Key: aws.String("images/keep"), VersionId: aws.String("m2")},
				}},
			}}}
			var output bytes.Buffer
			err := runWithOutput(context.Background(), []string{"--dry-run", "--prefix=images/", "-exclude=images/keep", "--confirm-bucket=photos"}, false, false,
				func(context.Context, Config) (S3API, error) { return client, nil }, &output)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(output.String(), "Total eligible: 2 object versions, 1 delete markers, 10 bytes") {
				t.Fatalf("output = %s", output.String())
			}
			assertDryRunReadOnly(t, client)
			if len(client.versionPages) != 0 {
				t.Fatal("dry run did not scan all version pages")
			}
		})
	}
}

func TestInteractiveDryRunNeedsOnlySelection(t *testing.T) {
	client := &dryRunClient{fakeDeleter: fakeDeleter{objectPages: []*s3.ListObjectsV2Output{
		{CommonPrefixes: []types.CommonPrefix{{Prefix: aws.String("images/")}, {Prefix: aws.String("keep/")}}},
		{Contents: []types.Object{{Key: aws.String("images/a"), Size: aws.Int64(4)}}},
	}}}
	var output bytes.Buffer
	status := NewProgressRenderer(&output, false)
	status.Begin(context.Background())
	defer status.Finish()
	err := runInteractive(context.Background(), strings.NewReader("1\n"), &output,
		Config{Bucket: "photos", Exclude: []string{"keep/"}}, client, false, BucketStatsOptions{}, true, status)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "confirm deletion") || !strings.Contains(output.String(), `2. "keep/" (protected)`) ||
		!strings.Contains(output.String(), "Total eligible: 1 objects, 4 bytes") {
		t.Fatalf("output = %s", output.String())
	}
	assertDryRunReadOnly(t, client)
}

func TestDryRunStopsOnIncompleteListing(t *testing.T) {
	setTestConfig(t)
	for _, listingError := range []error{errors.New("listing denied"), context.Canceled} {
		t.Run(listingError.Error(), func(t *testing.T) {
			client := &dryRunClient{fakeDeleter: fakeDeleter{
				objectPages: []*s3.ListObjectsV2Output{
					{CommonPrefixes: []types.CommonPrefix{{Prefix: aws.String("images/")}}},
					{IsTruncated: aws.Bool(true), NextContinuationToken: aws.String("next"), Contents: []types.Object{{Key: aws.String("images/a"), Size: aws.Int64(4)}}},
				},
				objectErrors: []error{nil, nil, listingError},
			}}
			var output bytes.Buffer
			err := runWithOutput(context.Background(), []string{"--dry-run", "--prefix=images/"}, false, false,
				func(context.Context, Config) (S3API, error) { return client, nil }, &output)
			if !errors.Is(err, listingError) {
				t.Fatalf("error = %v, want %v", err, listingError)
			}
			if strings.Contains(output.String(), "Total eligible:") || strings.Contains(output.String(), "dry run complete") {
				t.Fatalf("incomplete scan reported success: %s", output.String())
			}
			assertDryRunReadOnly(t, client)
		})
	}
}

func TestDryRunRequiresExplicitTargetInAutomation(t *testing.T) {
	setTestConfig(t)
	err := runWithOutput(context.Background(), []string{"--dry-run"}, false, false,
		func(context.Context, Config) (S3API, error) {
			t.Fatal("client must not be created without a target")
			return nil, nil
		}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "requires --prefix") {
		t.Fatalf("error = %v", err)
	}
}

func assertDryRunReadOnly(t *testing.T, client *dryRunClient) {
	t.Helper()
	if len(client.deleteInputs) != 0 || len(client.singleDeleteInputs) != 0 {
		t.Fatalf("dry run issued deletes: bulk=%d, individual=%d", len(client.deleteInputs), len(client.singleDeleteInputs))
	}
}
