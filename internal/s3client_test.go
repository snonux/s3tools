package internal

import (
	"bytes"
	"context"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
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

func TestS3HTTPClientKeepsSDKBuildableClientWithDefaultTimeout(t *testing.T) {
	defaultClient := s3HTTPClient(nil)
	client, ok := defaultClient.(*awshttp.BuildableClient)
	if !ok {
		t.Fatalf("default HTTP client type = %T, want *awshttp.BuildableClient", defaultClient)
	}
	if got := client.GetTimeout(); got != defaultS3RequestTimeout {
		t.Errorf("default timeout = %s, want %s", got, defaultS3RequestTimeout)
	}
	if transport := client.GetTransport(); transport.TLSHandshakeTimeout <= 0 {
		t.Error("SDK buildable transport lost its TLS handshake timeout")
	}

	callerClient := &http.Client{}
	if got := s3HTTPClient(callerClient); got != callerClient {
		t.Errorf("injected HTTP client = %T, want original client %p", got, callerClient)
	}
}

func TestNewS3ClientSupportsCustomCABundle(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()

	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	certificatePath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(certificatePath, certificate, 0o600); err != nil {
		t.Fatalf("write certificate bundle: %v", err)
	}
	t.Setenv("AWS_CA_BUNDLE", certificatePath)
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "config"))

	client, err := NewS3Client(context.Background(), Config{Region: "us-east-1", AccessKeyID: "key", SecretAccessKey: "secret"})
	if err != nil {
		t.Fatalf("NewS3Client() with AWS_CA_BUNDLE error = %v", err)
	}
	if _, ok := client.Options().HTTPClient.(*awshttp.BuildableClient); !ok {
		t.Fatalf("HTTP client type = %T, want *awshttp.BuildableClient", client.Options().HTTPClient)
	}
}

func TestNewS3ClientDoesNotFollowRedirects(t *testing.T) {
	var redirectTargetHit atomic.Bool
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/target" {
			redirectTargetHit.Store(true)
			_, _ = io.WriteString(writer, `<?xml version="1.0"?><ListBucketResult/>`)
			return
		}
		http.Redirect(writer, request, server.URL+"/target", http.StatusMovedPermanently)
	}))
	defer server.Close()

	client, err := NewS3Client(context.Background(), Config{
		Region:          "us-east-1",
		Endpoint:        server.URL,
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
	})
	if err != nil {
		t.Fatalf("NewS3Client() error = %v", err)
	}
	if _, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("bucket")}); err == nil {
		t.Fatal("ListObjectsV2() error = nil after redirect, want S3 redirect response error")
	}
	if redirectTargetHit.Load() {
		t.Fatal("S3 client followed an HTTP redirect")
	}
}

func TestS3RequestTimeoutBoundsStalledResponses(t *testing.T) {
	tests := []struct {
		name      string
		stallBody bool
	}{
		{name: "headers"},
		{name: "body", stallBody: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("AWS_MAX_ATTEMPTS", "1")
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if test.stallBody {
					writer.Header().Set("Content-Type", "application/xml")
					writer.WriteHeader(http.StatusOK)
					writer.(http.Flusher).Flush()
				}
				<-request.Context().Done()
			}))
			defer server.Close()

			requestTimeout := 150 * time.Millisecond
			client, err := newS3Client(context.Background(), Config{
				Region:          "us-east-1",
				Endpoint:        server.URL,
				AccessKeyID:     "key",
				SecretAccessKey: "secret",
			}, awshttp.NewBuildableClient().WithTimeout(requestTimeout))
			if err != nil {
				t.Fatalf("newS3Client() error = %v", err)
			}

			err = listObjectsWithTestDeadline(t, client)
			if err == nil {
				t.Fatal("ListObjectsV2() error = nil, want timeout error")
			}
			if !strings.Contains(strings.ToLower(err.Error()), "timeout") && !strings.Contains(strings.ToLower(err.Error()), "deadline") {
				t.Errorf("ListObjectsV2() error = %v, want timeout or deadline context", err)
			}
		})
	}
}

func TestS3RequestPreservesCallerCancellation(t *testing.T) {
	t.Setenv("AWS_MAX_ATTEMPTS", "1")
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		<-request.Context().Done()
	}))
	defer server.Close()

	client, err := newS3Client(context.Background(), Config{
		Region:          "us-east-1",
		Endpoint:        server.URL,
		AccessKeyID:     "key",
		SecretAccessKey: "secret",
	}, awshttp.NewBuildableClient().WithTimeout(time.Second))
	if err != nil {
		t.Fatalf("newS3Client() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("bucket")})
	if err == nil {
		t.Fatal("ListObjectsV2() error = nil, want caller deadline error")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Errorf("ListObjectsV2() took %s, want caller cancellation within 500ms", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("ListObjectsV2() error = %v, want context.DeadlineExceeded", err)
	}
}

func listObjectsWithTestDeadline(t *testing.T, client *s3.Client) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("bucket")})
		result <- err
	}()
	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		cancel()
		select {
		case <-result:
		case <-time.After(time.Second):
			t.Fatal("ListObjectsV2() did not stop after test context cancellation")
		}
		t.Fatal("ListObjectsV2() exceeded the independent 2s test deadline")
		return nil
	}
}
