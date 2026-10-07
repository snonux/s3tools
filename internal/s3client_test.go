package internal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type fakeBucketVersioningClient struct {
	output *s3.GetBucketVersioningOutput
	err    error
	bucket string
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func (client *fakeBucketVersioningClient) GetBucketVersioning(_ context.Context, input *s3.GetBucketVersioningInput, _ ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	client.bucket = aws.ToString(input.Bucket)
	return client.output, client.err
}

var _ BucketVersioningAPI = (*fakeBucketVersioningClient)(nil)

func TestDetectVersioning(t *testing.T) {
	tests := []struct {
		name    string
		status  types.BucketVersioningStatus
		err     error
		want    Versioning
		wantErr string
	}{
		{name: "enabled", status: types.BucketVersioningStatusEnabled, want: Versioned},
		{name: "suspended", status: types.BucketVersioningStatusSuspended, want: Versioned},
		{name: "unversioned", want: Unversioned},
		{name: "request error", err: errors.New("denied"), wantErr: "get bucket versioning"},
		{name: "empty response", wantErr: "empty response"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output := &s3.GetBucketVersioningOutput{Status: test.status}
			if test.name == "empty response" {
				output = nil
			}
			client := &fakeBucketVersioningClient{output: output, err: test.err}
			got, err := DetectVersioning(context.Background(), client, "bucket")
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("DetectVersioning() error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("DetectVersioning() error = %v", err)
			}
			if got != test.want {
				t.Errorf("DetectVersioning() = %v, want %v", got, test.want)
			}
			if client.bucket != "bucket" {
				t.Errorf("bucket = %q, want bucket", client.bucket)
			}
		})
	}
}

func TestClientOptions(t *testing.T) {
	tests := []struct {
		name     string
		settings Config
		want     s3ClientOptions
	}{
		{name: "AWS endpoint", settings: Config{}, want: s3ClientOptions{}},
		{name: "compatible endpoint defaults to path style", settings: Config{Endpoint: "https://garage.example"}, want: s3ClientOptions{endpoint: "https://garage.example", usePathStyle: true}},
		{name: "virtual host override", settings: Config{Endpoint: "https://garage.example", UseVirtualHostStyle: true}, want: s3ClientOptions{endpoint: "https://garage.example"}},
		{name: "force path style", settings: Config{ForcePathStyle: true}, want: s3ClientOptions{usePathStyle: true}},
		{name: "force path style wins", settings: Config{Endpoint: "https://garage.example", ForcePathStyle: true, UseVirtualHostStyle: true}, want: s3ClientOptions{endpoint: "https://garage.example", usePathStyle: true}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := clientOptions(test.settings); got != test.want {
				t.Errorf("clientOptions() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestNewS3ClientUsesConfiguredEndpointAndCredentials(t *testing.T) {
	tests := []struct {
		name       string
		settings   Config
		wantHost   string
		wantPath   string
		wantSigned bool
	}{
		{
			name:       "path style custom endpoint",
			settings:   Config{Region: "garage", Endpoint: "http://garage.example", AccessKeyID: "key", SecretAccessKey: "secret"},
			wantHost:   "garage.example",
			wantPath:   "/bucket",
			wantSigned: true,
		},
		{
			name:       "virtual host custom endpoint",
			settings:   Config{Region: "garage", Endpoint: "http://garage.example", UseVirtualHostStyle: true, AccessKeyID: "key", SecretAccessKey: "secret"},
			wantHost:   "bucket.garage.example",
			wantPath:   "/",
			wantSigned: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
				if request.URL.Host != test.wantHost {
					t.Errorf("host = %q, want %q", request.URL.Host, test.wantHost)
				}
				if request.URL.Path != test.wantPath {
					t.Errorf("path = %q, want %q", request.URL.Path, test.wantPath)
				}
				if (request.Header.Get("Authorization") != "") != test.wantSigned {
					t.Errorf("Authorization = %q, want signed=%t", request.Header.Get("Authorization"), test.wantSigned)
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewBufferString("<?xml version=\"1.0\"?><ListBucketResult/>"))}, nil
			})
			client, err := newS3Client(context.Background(), test.settings, &http.Client{Transport: transport})
			if err != nil {
				t.Fatalf("newS3Client() error = %v", err)
			}
			if _, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("bucket")}); err != nil {
				t.Fatalf("ListObjectsV2() error = %v", err)
			}
		})
	}
}
