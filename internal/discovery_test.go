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

type fakeObjectLister struct {
	versionPages []*s3.ListObjectVersionsOutput
	objectPages  []*s3.ListObjectsV2Output
	versionErr   error
	objectErr    error
	versionCalls []*s3.ListObjectVersionsInput
	objectCalls  []*s3.ListObjectsV2Input
}

func (client *fakeObjectLister) ListObjectVersions(_ context.Context, input *s3.ListObjectVersionsInput, _ ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error) {
	client.versionCalls = append(client.versionCalls, input)
	if client.versionErr != nil {
		return nil, client.versionErr
	}
	page := client.versionPages[0]
	client.versionPages = client.versionPages[1:]
	return page, nil
}

func (client *fakeObjectLister) ListObjectsV2(_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	client.objectCalls = append(client.objectCalls, input)
	if client.objectErr != nil {
		return nil, client.objectErr
	}
	page := client.objectPages[0]
	client.objectPages = client.objectPages[1:]
	return page, nil
}

var _ ObjectLister = (*fakeObjectLister)(nil)

func TestDiscoverPrefixes(t *testing.T) {
	versioned := &fakeObjectLister{versionPages: []*s3.ListObjectVersionsOutput{
		{IsTruncated: aws.Bool(true), NextKeyMarker: aws.String("b/"), NextVersionIdMarker: aws.String("v1"), CommonPrefixes: []types.CommonPrefix{{Prefix: aws.String("b/")}, {Prefix: aws.String("a/")}}},
		{CommonPrefixes: []types.CommonPrefix{{Prefix: aws.String("b/")}, {Prefix: aws.String("old/")}}, Versions: []types.ObjectVersion{{Key: aws.String("version-marker/")}}, DeleteMarkers: []types.DeleteMarkerEntry{{Key: aws.String("stale/")}}},
	}}
	got, err := DiscoverPrefixes(context.Background(), versioned, "bucket", Versioned, []string{"b/"})
	if err != nil || !reflect.DeepEqual(got, []string{"a/", "old/", "stale/", "version-marker/"}) {
		t.Fatalf("versioned = %#v, %v", got, err)
	}
	if aws.ToString(versioned.versionCalls[1].KeyMarker) != "b/" || aws.ToString(versioned.versionCalls[1].VersionIdMarker) != "v1" {
		t.Fatal("version pagination markers not forwarded")
	}

	unversioned := &fakeObjectLister{objectPages: []*s3.ListObjectsV2Output{{IsTruncated: aws.Bool(true), NextContinuationToken: aws.String("next"), CommonPrefixes: []types.CommonPrefix{{Prefix: aws.String("live/")}}}, {CommonPrefixes: []types.CommonPrefix{{Prefix: aws.String("later/")}}, Contents: []types.Object{{Key: aws.String("empty/")}}}}}
	got, err = DiscoverPrefixes(context.Background(), unversioned, "bucket", Unversioned, nil)
	if err != nil || !reflect.DeepEqual(got, []string{"empty/", "later/", "live/"}) {
		t.Fatalf("unversioned = %#v, %v", got, err)
	}
	if aws.ToString(unversioned.objectCalls[1].ContinuationToken) != "next" {
		t.Fatal("object continuation token not forwarded")
	}
}

func TestInventoryPrefix(t *testing.T) {
	client := &fakeObjectLister{versionPages: []*s3.ListObjectVersionsOutput{
		{IsTruncated: aws.Bool(true), NextKeyMarker: aws.String("p/a"), NextVersionIdMarker: aws.String("v1"), Versions: []types.ObjectVersion{{Key: aws.String("p/a"), VersionId: aws.String("1"), Size: aws.Int64(3)}, {Key: aws.String("p/keep/a"), VersionId: aws.String("2"), Size: aws.Int64(9)}}, DeleteMarkers: []types.DeleteMarkerEntry{{Key: aws.String("p/dead"), VersionId: aws.String("3")}}},
		{Versions: []types.ObjectVersion{{Key: aws.String("p/root.txt"), VersionId: aws.String("4"), Size: aws.Int64(8)}}},
	}}
	inventory, err := InventoryPrefix(context.Background(), client, "bucket", "p/", Versioned, []string{"p/keep/", "p/root.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if inventory.Bytes != 3 || len(inventory.Entries) != 2 || !inventory.Entries[1].DeleteMarker {
		t.Fatalf("inventory = %#v", inventory)
	}
}

func TestDiscoveryErrorsAndExclusions(t *testing.T) {
	client := &fakeObjectLister{objectErr: errors.New("denied")}
	if _, err := DiscoverPrefixes(context.Background(), client, "bucket", Unversioned, nil); err == nil {
		t.Fatal("expected list error")
	}
	if _, err := InventoryPrefix(context.Background(), client, "bucket", "", Unversioned, nil); err == nil {
		t.Fatal("expected root prefix rejection")
	}
	if _, err := InventoryPrefix(context.Background(), client, "bucket", "/", Unversioned, nil); err == nil {
		t.Fatal("expected slash root rejection")
	}
	if _, err := InventoryPrefix(context.Background(), client, "bucket", "keep/", Unversioned, []string{"keep/"}); err == nil {
		t.Fatal("expected protected prefix rejection")
	}
	if !IsExcluded("keep/a", []string{"keep/"}) || !IsExcluded("file", []string{"file"}) || IsExcluded("file2", []string{"file"}) {
		t.Fatal("exclusion matching failed")
	}
}
