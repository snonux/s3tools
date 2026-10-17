package internal

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type batchClient struct {
	fakeDeleter
}

type blockingDiscoveryClient struct {
	batchClient
	release chan struct{}
	calls   int
}

func (client *blockingDiscoveryClient) ListObjectsV2(ctx context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	client.calls++
	if client.calls == 1 {
		return &s3.ListObjectsV2Output{IsTruncated: aws.Bool(true), NextContinuationToken: aws.String("next"), CommonPrefixes: []types.CommonPrefix{{Prefix: aws.String("images/")}}}, nil
	}
	select {
	case <-client.release:
		return nil, errors.New("discovery stopped")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (client *batchClient) GetBucketVersioning(context.Context, *s3.GetBucketVersioningInput, ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	return &s3.GetBucketVersioningOutput{}, nil
}

var _ S3API = (*batchClient)(nil)

func TestParseBatchArgs(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		want       BatchRequest
		wantRemain []string
		wantError  string
	}{
		{name: "repeated prefixes", args: []string{"--prefix=a/", "-prefix", "b/", "--confirm-bucket=photos", "other"}, want: BatchRequest{Prefixes: []string{"a/", "b/"}, ConfirmedBucket: "photos"}, wantRemain: []string{"other"}},
		{name: "missing prefix value", args: []string{"--prefix", "--confirm-bucket", "photos"}, wantError: "--prefix requires a value"},
		{name: "missing confirmation value", args: []string{"--confirm-bucket="}, wantError: "--confirm-bucket requires a value"},
		{name: "duplicate confirmation", args: []string{"--confirm-bucket", "photos", "--confirm-bucket", "photos"}, wantError: "may only be specified once"},
		{name: "dry run", args: []string{"--dry-run", "--prefix=images/"}, want: BatchRequest{DryRun: true, Prefixes: []string{"images/"}}},
		{name: "single dash dry run", args: []string{"-dry-run"}, want: BatchRequest{DryRun: true}},
		{name: "bucket statistics options", args: []string{"--bucket-stats", "--bucket-stats-pages", "12", "--bucket-stats-timeout=3s"}, want: BatchRequest{BucketStats: true, BucketStatsOptions: BucketStatsOptions{MaxPages: 12, Timeout: 3 * time.Second}}},
		{name: "invalid bucket statistics page limit", args: []string{"--bucket-stats-pages=0"}, wantError: "positive integer"},
		{name: "bucket statistics bounds require opt in", args: []string{"--bucket-stats-pages=12"}, wantError: "require --bucket-stats"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, remaining, err := ParseBatchArgs(test.args)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("ParseBatchArgs() error = %v, want %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.ConfirmedBucket != test.want.ConfirmedBucket || got.DryRun != test.want.DryRun || got.BucketStats != test.want.BucketStats || got.BucketStatsOptions != test.want.BucketStatsOptions || strings.Join(got.Prefixes, ",") != strings.Join(test.want.Prefixes, ",") || strings.Join(remaining, ",") != strings.Join(test.wantRemain, ",") {
				t.Fatalf("ParseBatchArgs() = %#v, %#v; want %#v, %#v", got, remaining, test.want, test.wantRemain)
			}
		})
	}
}

func TestValidateBatchRequest(t *testing.T) {
	tests := []struct {
		name    string
		request BatchRequest
		want    string
	}{
		{name: "requires prefix", request: BatchRequest{ConfirmedBucket: "photos"}, want: "at least one --prefix"},
		{name: "requires confirmation", request: BatchRequest{Prefixes: []string{"images/"}}, want: "requires --confirm-bucket"},
		{name: "exact confirmation", request: BatchRequest{Prefixes: []string{"images/"}, ConfirmedBucket: "Photos"}, want: "does not exactly match"},
		{name: "root target", request: BatchRequest{Prefixes: []string{"/"}, ConfirmedBucket: "photos"}, want: "non-root directory"},
		{name: "object target", request: BatchRequest{Prefixes: []string{"root.txt"}, ConfirmedBucket: "photos"}, want: "non-root directory"},
		{name: "protected target", request: BatchRequest{Prefixes: []string{"private/"}, ConfirmedBucket: "photos"}, want: "fully protected"},
		{name: "nonexistent target", request: BatchRequest{Prefixes: []string{"missing/"}, ConfirmedBucket: "photos"}, want: "does not exist"},
		{name: "duplicate target", request: BatchRequest{Prefixes: []string{"images/", "images/"}, ConfirmedBucket: "photos"}, want: "selected more than once"},
		{name: "valid targets", request: BatchRequest{Prefixes: []string{"images/", "logs/"}, ConfirmedBucket: "photos"}},
		{name: "dry run needs no confirmation", request: BatchRequest{Prefixes: []string{"images/"}, DryRun: true}},
		{name: "dry run rejects wrong confirmation", request: BatchRequest{Prefixes: []string{"images/"}, DryRun: true, ConfirmedBucket: "other"}, want: "does not exactly match"},
		{name: "dry run requires prefix", request: BatchRequest{DryRun: true}, want: "at least one --prefix"},
		{name: "dry run rejects protected target", request: BatchRequest{Prefixes: []string{"private/"}, DryRun: true}, want: "fully protected"},
		{name: "dry run rejects missing target", request: BatchRequest{Prefixes: []string{"missing/"}, DryRun: true}, want: "does not exist"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateBatchRequest(test.request, "photos", []string{"images/", "logs/"}, []string{"private/"})
			if test.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateBatchRequest() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestRunBatch(t *testing.T) {
	setTestConfig(t)
	t.Run("non terminal requires explicit target and confirmation", func(t *testing.T) {
		err := run(context.Background(), nil, false, false, func(context.Context, Config) (S3API, error) {
			t.Fatal("client must not be created")
			return nil, nil
		})
		if err == nil || !strings.Contains(err.Error(), "requires --prefix and --confirm-bucket") {
			t.Fatalf("run() error = %v", err)
		}
	})
	t.Run("rejects invalid request before client creation", func(t *testing.T) {
		err := run(context.Background(), []string{"--prefix", "missing/", "--confirm-bucket", "other"}, false, false, func(context.Context, Config) (S3API, error) {
			t.Fatal("client must not be created")
			return nil, nil
		})
		if err == nil || !strings.Contains(err.Error(), "does not exactly match") {
			t.Fatalf("run() error = %v", err)
		}
	})
	t.Run("validates all targets before deleting", func(t *testing.T) {
		client := &batchClient{fakeDeleter: fakeDeleter{objectPages: []*s3.ListObjectsV2Output{{CommonPrefixes: []types.CommonPrefix{{Prefix: aws.String("images/")}}}}}}
		err := run(context.Background(), []string{"--prefix", "images/", "--prefix", "missing/", "--confirm-bucket", "photos"}, false, false, func(context.Context, Config) (S3API, error) { return client, nil })
		if err == nil || !strings.Contains(err.Error(), "does not exist") || len(client.deleteInputs) != 0 {
			t.Fatalf("run() error = %v, delete calls = %d", err, len(client.deleteInputs))
		}
	})
	t.Run("deletes validated target", func(t *testing.T) {
		client := &batchClient{fakeDeleter: fakeDeleter{
			objectPages: []*s3.ListObjectsV2Output{
				{CommonPrefixes: []types.CommonPrefix{{Prefix: aws.String("images/")}}},
				{Contents: []types.Object{{Key: aws.String("images/a"), Size: aws.Int64(4)}}},
				{},
			},
			deleteOutputs: []*s3.DeleteObjectsOutput{{Deleted: []types.DeletedObject{{Key: aws.String("images/a")}}}},
		}}
		err := run(context.Background(), []string{"--prefix", "images/", "--confirm-bucket", "photos"}, false, true, func(context.Context, Config) (S3API, error) { return client, nil })
		if err != nil {
			t.Fatal(err)
		}
		if len(client.deleteInputs) != 1 || aws.ToString(client.deleteInputs[0].Delete.Objects[0].Key) != "images/a" {
			t.Fatalf("delete requests = %#v", client.deleteInputs)
		}
	})
}

func TestRunBatchUsesCompleteBucketStatsBaseline(t *testing.T) {
	setTestConfig(t)
	client := &batchClient{fakeDeleter: fakeDeleter{
		objectPages: []*s3.ListObjectsV2Output{
			{CommonPrefixes: []types.CommonPrefix{{Prefix: aws.String("images/")}}},
			{Contents: []types.Object{{Key: aws.String("images/a"), Size: aws.Int64(4)}, {Key: aws.String("archive/b"), Size: aws.Int64(6)}, {Key: aws.String("root.txt"), Size: aws.Int64(2)}}},
			{Contents: []types.Object{{Key: aws.String("images/a"), Size: aws.Int64(4)}}},
			{},
		},
		deleteOutputs: []*s3.DeleteObjectsOutput{{Deleted: []types.DeletedObject{{Key: aws.String("images/a")}}}},
	}}
	output := &bytes.Buffer{}
	err := runWithOutput(context.Background(), []string{"--prefix", "images/", "--confirm-bucket", "photos", "--bucket-stats", "--bucket-stats-pages", "2"}, false, false, func(context.Context, Config) (S3API, error) {
		return client, nil
	}, output)
	if err != nil {
		t.Fatal(err)
	}
	if len(client.objectPages) != 0 || len(client.deleteInputs) != 1 {
		t.Fatalf("remaining list pages = %d, delete calls = %d", len(client.objectPages), len(client.deleteInputs))
	}
	if !strings.Contains(output.String(), "bucket remainder estimate from snapshot") || !strings.Contains(output.String(), "2 objects (8 B)") || !strings.Contains(output.String(), "status: deletion complete") {
		t.Fatalf("progress output = %q, want a decremented bucket remainder estimate", output.String())
	}
}

func TestRunBatchReportsStatusWhileClientSetupIsBlocked(t *testing.T) {
	setTestConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	output := channelProgressWriter{lines: make(chan string, 16)}
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- runWithOutput(ctx, []string{"--prefix=images/", "--confirm-bucket=photos"}, false, false, func(ctx context.Context, _ Config) (S3API, error) {
			select {
			case <-release:
				return nil, errors.New("connection failed")
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}, output)
	}()
	line := readProgressContaining(t, output.lines, "status: connecting to bucket; total pending")
	if !strings.Contains(line, "elapsed") {
		t.Fatalf("setup status = %q", line)
	}
	close(release)
	if err := <-done; err == nil || !strings.Contains(err.Error(), "connection failed") {
		t.Fatalf("run error = %v", err)
	}
	if line := readProgressContaining(t, output.lines, "status: operation stopped"); !strings.Contains(line, "total pending") {
		t.Fatalf("stopped status = %q", line)
	}
}

func TestRunBatchReportsDiscoveryPagesWhileNextPageIsBlocked(t *testing.T) {
	setTestConfig(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	output := channelProgressWriter{lines: make(chan string, 16)}
	client := &blockingDiscoveryClient{release: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- runWithOutput(ctx, []string{"--prefix=images/", "--confirm-bucket=photos"}, false, false, func(context.Context, Config) (S3API, error) {
			return client, nil
		}, output)
	}()
	var line string
	deadline := time.After(4 * time.Second)
	for line == "" {
		select {
		case reported := <-output.lines:
			if strings.Contains(reported, "discovering top-level prefixes: 1 pages, 1 prefixes found") {
				line = reported
			}
		case <-deadline:
			t.Fatal("timed out waiting for periodic discovery status")
		}
	}
	if !strings.Contains(line, "total pending") {
		t.Fatalf("discovery status = %q", line)
	}
	close(client.release)
	if err := <-done; err == nil || !strings.Contains(err.Error(), "discovery stopped") {
		t.Fatalf("run error = %v", err)
	}
	_ = readProgressContaining(t, output.lines, "status: operation stopped")
}

func setTestConfig(t *testing.T) {
	t.Helper()
	t.Setenv("S3RM_BUCKET", "photos")
	t.Setenv("S3RM_REGION", "test-region")
}
