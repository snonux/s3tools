package internal

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestDeletePrefixPreservesListingKeysAcrossBulkAndIndividualDeletes(t *testing.T) {
	const (
		bulkKey        = "p/literal%25+plus-雪.dat"
		bulkKeyEncoded = "p/literal%2525%2Bplus-%E9%9B%AA.dat"
		controlKey     = "p/control\x01.dat"
	)
	var listRequests, bulkDeleteRequests, individualDeleteRequests int
	client := newListingRoundTripClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodGet && request.URL.Query().Has("list-type") {
			listRequests++
			if got := request.URL.Query().Get("encoding-type"); got != "url" {
				t.Errorf("encoding-type = %q, want url", got)
			}
			if listRequests == 1 {
				return listingResponse(`<ListBucketResult><EncodingType>url</EncodingType><Contents><Key>` + bulkKeyEncoded + `</Key><Size>4</Size></Contents><Contents><Key>p/control%01.dat</Key><Size>6</Size></Contents></ListBucketResult>`), nil
			}
			return listingResponse(`<ListBucketResult><EncodingType>url</EncodingType></ListBucketResult>`), nil
		}
		if request.Method == http.MethodPost && request.URL.Query().Has("delete") {
			bulkDeleteRequests++
			body, err := io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}
			want := `<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Object><Key>` + bulkKey + `</Key></Object><Quiet>false</Quiet></Delete>`
			if string(body) != want {
				t.Errorf("DeleteObjects body = %q, want exact key body %q", body, want)
			}
			return listingResponse(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Deleted><Key>` + bulkKey + `</Key></Deleted></DeleteResult>`), nil
		}
		if request.Method == http.MethodDelete {
			individualDeleteRequests++
			if got := request.URL.EscapedPath(); got != "/bucket/p/control%01.dat" {
				t.Errorf("individual delete path = %q, want escaped exact key path", got)
			}
			return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
		}
		t.Errorf("unexpected S3 request: %s %s", request.Method, request.URL)
		return listingResponse(`<Error><Code>UnexpectedRequest</Code></Error>`), nil
	}))

	summary, err := DeletePrefix(context.Background(), client, "bucket", "p/", Unversioned, nil)
	if err != nil {
		t.Fatalf("DeletePrefix() error = %v", err)
	}
	if summary.Deleted != 2 || summary.DeletedBytes != 10 {
		t.Errorf("DeletePrefix() summary = %#v, want 2 objects / 10 bytes", summary)
	}
	if listRequests != 2 || bulkDeleteRequests != 1 || individualDeleteRequests != 1 {
		t.Errorf("requests: lists=%d bulk deletes=%d individual deletes=%d, want 2/1/1", listRequests, bulkDeleteRequests, individualDeleteRequests)
	}
	if !xml10SafeValue("unicode-😀") {
		t.Error("xml10SafeValue() rejected valid supplementary Unicode")
	}
	if xml10SafeValue(controlKey) {
		t.Error("xml10SafeValue() accepted XML-incompatible control character")
	}
}

func TestDeletePrefixPreservesVersionIDForXMLIncompatibleKey(t *testing.T) {
	const (
		controlKey = "p/control\x01.dat"
		versionID  = "version%25+雪"
	)
	var listRequests, deleteRequests int
	client := newListingRoundTripClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodGet && request.URL.Query().Has("versions") {
			listRequests++
			if got := request.URL.Query().Get("encoding-type"); got != "url" {
				t.Errorf("encoding-type = %q, want url", got)
			}
			if listRequests == 1 {
				return listingResponse(`<ListVersionsResult><EncodingType>url</EncodingType><Version><Key>p/control%01.dat</Key><VersionId>` + versionID + `</VersionId><Size>8</Size></Version></ListVersionsResult>`), nil
			}
			return listingResponse(`<ListVersionsResult><EncodingType>url</EncodingType></ListVersionsResult>`), nil
		}
		if request.Method == http.MethodDelete {
			deleteRequests++
			if got := request.URL.EscapedPath(); got != "/bucket/p/control%01.dat" {
				t.Errorf("individual delete path = %q, want escaped exact key path", got)
			}
			if got := request.URL.Query().Get("versionId"); got != versionID {
				t.Errorf("versionId = %q, want exact version ID %q", got, versionID)
			}
			return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
		}
		t.Errorf("unexpected S3 request: %s %s", request.Method, request.URL)
		return listingResponse(`<Error><Code>UnexpectedRequest</Code></Error>`), nil
	}))

	summary, err := DeletePrefix(context.Background(), client, "bucket", "p/", Versioned, nil)
	if err != nil {
		t.Fatalf("DeletePrefix() error = %v", err)
	}
	if summary.Deleted != 1 || summary.DeletedBytes != 8 {
		t.Errorf("DeletePrefix() summary = %#v, want 1 object / 8 bytes", summary)
	}
	if listRequests != 2 || deleteRequests != 1 {
		t.Errorf("requests: lists=%d individual deletes=%d, want 2/1", listRequests, deleteRequests)
	}
	if xml10SafeValue(controlKey) {
		t.Fatal("test control key unexpectedly passed XML 1.0 validation")
	}
}

var _ ObjectDeleter = (*s3.Client)(nil)
