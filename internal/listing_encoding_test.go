package internal

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestDiscoverPrefixesDecodesURLKeysAndKeepsContinuationTokensOpaque(t *testing.T) {
	const rawPrefix = "dir/literal%25+plus-雪\x01/"
	var requests int
	client := newListingRoundTripClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		if got := request.URL.Query().Get("encoding-type"); got != "url" {
			t.Errorf("encoding-type = %q, want url", got)
		}
		if requests == 1 {
			if got := request.URL.Query().Get("continuation-token"); got != "" {
				t.Errorf("first continuation-token = %q, want empty", got)
			}
			return listingResponse(`<ListBucketResult><EncodingType>url</EncodingType><IsTruncated>true</IsTruncated><NextContinuationToken>opaque%25%2Btoken</NextContinuationToken><CommonPrefixes><Prefix>dir/literal%2525%2Bplus-%E9%9B%AA%01/</Prefix></CommonPrefixes></ListBucketResult>`), nil
		}
		if requests != 2 {
			t.Fatalf("request count = %d, want 2", requests)
		}
		if got := request.URL.Query().Get("continuation-token"); got != "opaque%25%2Btoken" {
			t.Errorf("second continuation-token = %q, want opaque%%25%%2Btoken", got)
		}
		return listingResponse(`<ListBucketResult><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated></ListBucketResult>`), nil
	}))

	got, err := DiscoverPrefixes(context.Background(), client, "bucket", Unversioned, nil)
	if err != nil {
		t.Fatalf("DiscoverPrefixes() error = %v", err)
	}
	if len(got) != 1 || got[0] != rawPrefix {
		t.Fatalf("DiscoverPrefixes() = %#v, want exact key bytes %q", got, rawPrefix)
	}
}

func TestDiscoverVersionedPrefixesDecodesURLCommonPrefixes(t *testing.T) {
	const rawPrefix = "dir/literal%25+plus-雪\x01/"
	client := newListingRoundTripClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if got := request.URL.Query().Get("encoding-type"); got != "url" {
			t.Errorf("encoding-type = %q, want url", got)
		}
		return listingResponse(`<ListVersionsResult><EncodingType>url</EncodingType><CommonPrefixes><Prefix>dir/literal%2525%2Bplus-%E9%9B%AA%01/</Prefix></CommonPrefixes></ListVersionsResult>`), nil
	}))

	got, err := DiscoverPrefixes(context.Background(), client, "bucket", Versioned, nil)
	if err != nil {
		t.Fatalf("DiscoverPrefixes() error = %v", err)
	}
	if len(got) != 1 || got[0] != rawPrefix {
		t.Fatalf("DiscoverPrefixes() = %#v, want exact key bytes %q", got, rawPrefix)
	}
}

func TestInventoryPrefixDecodesKeysOnceBeforeExclusions(t *testing.T) {
	const (
		included = "p/literal%25+plus-雪\x01.dat"
		excluded = "p/exclude%25+雪\x01.dat"
	)
	var requests int
	client := newListingRoundTripClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		if got := request.URL.Query().Get("encoding-type"); got != "url" {
			t.Errorf("encoding-type = %q, want url", got)
		}
		if got := request.URL.Query().Get("prefix"); got != "p/" {
			t.Errorf("prefix = %q, want p/", got)
		}
		if requests == 1 {
			return listingResponse(`<ListBucketResult><EncodingType>url</EncodingType><IsTruncated>true</IsTruncated><NextContinuationToken>opaque%2525+token</NextContinuationToken><Contents><Key>p/literal%2525%2Bplus-%E9%9B%AA%01.dat</Key><Size>7</Size></Contents></ListBucketResult>`), nil
		}
		if requests != 2 {
			t.Fatalf("request count = %d, want 2", requests)
		}
		if got := request.URL.Query().Get("continuation-token"); got != "opaque%2525+token" {
			t.Errorf("continuation-token = %q, want opaque%%2525+token", got)
		}
		return listingResponse(`<ListBucketResult><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated><Contents><Key>p/exclude%2525%2B%E9%9B%AA%01.dat</Key><Size>3</Size></Contents></ListBucketResult>`), nil
	}))

	inventory, err := InventoryPrefix(context.Background(), client, "bucket", "p/", Unversioned, []string{excluded})
	if err != nil {
		t.Fatalf("InventoryPrefix() error = %v", err)
	}
	if len(inventory.Entries) != 1 || inventory.Entries[0].Key != included || inventory.Entries[0].Size != 7 {
		t.Fatalf("InventoryPrefix() entries = %#v, want exact included key %q", inventory.Entries, included)
	}
}

func TestInventoryVersionedDecodesKeyMarkersAndDeleteMarkerKeys(t *testing.T) {
	const rawKey = "p/literal%25+plus-雪\x01.dat"
	var requests int
	client := newListingRoundTripClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		query := request.URL.Query()
		if got := query.Get("encoding-type"); got != "url" {
			t.Errorf("encoding-type = %q, want url", got)
		}
		if requests == 1 {
			return listingResponse(`<ListVersionsResult><EncodingType>url</EncodingType><IsTruncated>true</IsTruncated><NextKeyMarker>p/literal%2525%2Bplus-%E9%9B%AA%01.dat</NextKeyMarker><NextVersionIdMarker>v2</NextVersionIdMarker></ListVersionsResult>`), nil
		}
		if requests != 2 {
			t.Fatalf("request count = %d, want 2", requests)
		}
		if got := query.Get("key-marker"); got != rawKey {
			t.Errorf("key-marker = %q, want exact original key %q", got, rawKey)
		}
		if got := query.Get("version-id-marker"); got != "v2" {
			t.Errorf("version-id-marker = %q, want v2", got)
		}
		return listingResponse(`<ListVersionsResult><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated><DeleteMarker><Key>p/literal%2525%2Bplus-%E9%9B%AA%01.dat</Key><VersionId>v2</VersionId></DeleteMarker></ListVersionsResult>`), nil
	}))

	inventory, err := InventoryPrefix(context.Background(), client, "bucket", "p/", Versioned, nil)
	if err != nil {
		t.Fatalf("InventoryPrefix() error = %v", err)
	}
	if len(inventory.Entries) != 1 || inventory.Entries[0].Key != rawKey || inventory.Entries[0].VersionID != "v2" || !inventory.Entries[0].DeleteMarker {
		t.Fatalf("InventoryPrefix() entries = %#v, want delete marker for %q", inventory.Entries, rawKey)
	}
}

func TestInventoryPrefixRejectsMalformedURLEncodedKeys(t *testing.T) {
	client := newListingRoundTripClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if got := request.URL.Query().Get("encoding-type"); got != "url" {
			t.Errorf("encoding-type = %q, want url", got)
		}
		return listingResponse(`<ListBucketResult><EncodingType>url</EncodingType><Contents><Key>p/bad%ZZkey</Key><Size>1</Size></Contents></ListBucketResult>`), nil
	}))

	if _, err := InventoryPrefix(context.Background(), client, "bucket", "p/", Unversioned, nil); err == nil || !strings.Contains(err.Error(), "decode URL-encoded keys") {
		t.Fatalf("InventoryPrefix() error = %v, want URL decoding error", err)
	}
}

func TestBucketStatsUsesURLEncodingAndDecodesVersionMarkers(t *testing.T) {
	const rawMarker = "p/literal%25+plus-雪\x01.dat"
	var versionRequests int
	client := newListingRoundTripClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		query := request.URL.Query()
		if got := query.Get("encoding-type"); got != "url" {
			t.Errorf("encoding-type = %q, want url", got)
		}
		if query.Has("list-type") {
			return listingResponse(`<ListBucketResult><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated><Contents><Key>p/literal%2525%2Bplus-%E9%9B%AA%01.dat</Key><Size>5</Size></Contents></ListBucketResult>`), nil
		}
		versionRequests++
		if versionRequests == 1 {
			return listingResponse(`<ListVersionsResult><EncodingType>url</EncodingType><IsTruncated>true</IsTruncated><NextKeyMarker>p/literal%2525%2Bplus-%E9%9B%AA%01.dat</NextKeyMarker><NextVersionIdMarker>v2</NextVersionIdMarker></ListVersionsResult>`), nil
		}
		if got := query.Get("key-marker"); got != rawMarker {
			t.Errorf("key-marker = %q, want exact original key %q", got, rawMarker)
		}
		if got := query.Get("version-id-marker"); got != "v2" {
			t.Errorf("version-id-marker = %q, want v2", got)
		}
		return listingResponse(`<ListVersionsResult><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated><Version><Key>p/literal%2525%2Bplus-%E9%9B%AA%01.dat</Key><VersionId>v2</VersionId><Size>5</Size></Version></ListVersionsResult>`), nil
	}))

	for _, versioning := range []Versioning{Unversioned, Versioned} {
		stats, err := ScanBucketStats(context.Background(), client, "bucket", versioning, BucketStatsOptions{MaxPages: 2, Timeout: time.Second})
		if err != nil {
			t.Fatalf("ScanBucketStats(%v) error = %v", versioning, err)
		}
		if !stats.Complete || stats.Objects != 1 || stats.Bytes != 5 {
			t.Errorf("ScanBucketStats(%v) = %#v, want one 5-byte object", versioning, stats)
		}
	}
	if versionRequests != 2 {
		t.Errorf("version listing requests = %d, want 2", versionRequests)
	}
}

func newListingRoundTripClient(t *testing.T, transport roundTripFunc) *s3.Client {
	t.Helper()
	t.Setenv("AWS_MAX_ATTEMPTS", "1")
	client, err := newS3Client(context.Background(), Config{
		Region:          "us-east-1",
		Endpoint:        "http://s3.example",
		AccessKeyID:     "test-key",
		SecretAccessKey: "test-secret",
	}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatalf("newS3Client() error = %v", err)
	}
	return client
}

func listingResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/xml"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

var _ ObjectLister = (*s3.Client)(nil)
