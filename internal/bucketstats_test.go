package internal

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type blockingBucketStatsLister struct{}

func (blockingBucketStatsLister) ListObjectVersions(context.Context, *s3.ListObjectVersionsInput, ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error) {
	return nil, nil
}

func (blockingBucketStatsLister) ListObjectsV2(ctx context.Context, _ *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

var _ ObjectLister = blockingBucketStatsLister{}

func TestScanBucketStatsCompletesAndCountsVersionedContents(t *testing.T) {
	client := &fakeObjectLister{versionPages: []*s3.ListObjectVersionsOutput{{
		Versions:      []types.ObjectVersion{{Key: aws.String("a"), Size: aws.Int64(3)}},
		DeleteMarkers: []types.DeleteMarkerEntry{{Key: aws.String("deleted")}},
	}}}
	stats, err := ScanBucketStats(context.Background(), client, "bucket", Versioned, BucketStatsOptions{MaxPages: 1, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if !stats.Complete || stats.Objects != 2 || stats.Bytes != 3 || stats.Pages != 1 {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestScanBucketStatsStopsAtPageLimit(t *testing.T) {
	client := &fakeObjectLister{objectPages: []*s3.ListObjectsV2Output{
		{IsTruncated: aws.Bool(true), NextContinuationToken: aws.String("next"), Contents: []types.Object{{Key: aws.String("a"), Size: aws.Int64(3)}}},
		{Contents: []types.Object{{Key: aws.String("b"), Size: aws.Int64(5)}}},
	}}
	stats, err := ScanBucketStats(context.Background(), client, "bucket", Unversioned, BucketStatsOptions{MaxPages: 1, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Complete || stats.Incomplete != "page limit" || stats.Pages != 1 || stats.Objects != 1 || stats.Bytes != 3 || len(client.objectCalls) != 1 {
		t.Fatalf("stats = %#v, calls = %d", stats, len(client.objectCalls))
	}
}

func TestScanBucketStatsCancellationIsIncomplete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stats, err := ScanBucketStats(ctx, &fakeObjectLister{}, "bucket", Unversioned, BucketStatsOptions{MaxPages: 1, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Complete || stats.Incomplete != "cancelled" || stats.Pages != 0 {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestScanBucketStatsStopsAtTimeLimit(t *testing.T) {
	stats, err := ScanBucketStats(context.Background(), blockingBucketStatsLister{}, "bucket", Unversioned, BucketStatsOptions{MaxPages: 10, Timeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Complete || stats.Incomplete != "time limit" || stats.Pages != 0 {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestScanBucketStatsUsesDefaultsAndRejectsInvalidLimits(t *testing.T) {
	defaults := DefaultBucketStatsOptions()
	if defaults.MaxPages != 1000 || defaults.Timeout != 2*time.Minute {
		t.Fatalf("defaults = %#v", defaults)
	}
	if _, err := ScanBucketStats(context.Background(), &fakeObjectLister{}, "bucket", Unversioned, BucketStatsOptions{MaxPages: -1}); err == nil {
		t.Fatal("expected invalid page limit error")
	}
}
