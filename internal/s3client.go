package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const defaultS3RequestTimeout = 2 * time.Minute

// Versioning describes the deletion API required by a bucket.
type Versioning uint8

const (
	// Unversioned uses ListObjectsV2 and deletes object keys.
	Unversioned Versioning = iota
	// Versioned uses ListObjectVersions and deletes exact version IDs.
	Versioned
)

// BucketVersioningAPI is the narrow API needed to discover a bucket's versioning state.
type BucketVersioningAPI interface {
	GetBucketVersioning(context.Context, *s3.GetBucketVersioningInput, ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error)
}

// NewS3Client constructs an S3-compatible client from Config.
func NewS3Client(ctx context.Context, settings Config) (*s3.Client, error) {
	return newS3Client(ctx, settings, nil)
}

func newS3Client(ctx context.Context, settings Config, httpClient aws.HTTPClient) (*s3.Client, error) {
	loadOptions := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(settings.Region)}
	loadOptions = append(loadOptions, awsconfig.WithHTTPClient(s3HTTPClient(httpClient)))
	if settings.AccessKeyID != "" {
		provider := credentials.NewStaticCredentialsProvider(settings.AccessKeyID, settings.SecretAccessKey, settings.SessionToken)
		loadOptions = append(loadOptions, awsconfig.WithCredentialsProvider(provider))
	}

	sdkConfig, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}

	options := clientOptions(settings)
	return s3.NewFromConfig(sdkConfig, func(client *s3.Options) {
		client.UsePathStyle = options.usePathStyle
		if options.endpoint != "" {
			client.BaseEndpoint = aws.String(options.endpoint)
		}
	}), nil
}

func s3HTTPClient(client aws.HTTPClient) aws.HTTPClient {
	if client == nil {
		return awshttp.NewBuildableClient().WithTimeout(defaultS3RequestTimeout)
	}
	return client
}

// DetectVersioning returns the listing and deletion mode for bucket.
func DetectVersioning(ctx context.Context, client BucketVersioningAPI, bucket string) (Versioning, error) {
	result, err := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	if err != nil {
		return Unversioned, fmt.Errorf("get bucket versioning for %q: %w", bucket, err)
	}
	if result == nil {
		return Unversioned, fmt.Errorf("get bucket versioning for %q: empty response", bucket)
	}
	if result.Status == types.BucketVersioningStatusEnabled || result.Status == types.BucketVersioningStatusSuspended {
		return Versioned, nil
	}

	return Unversioned, nil
}

type s3ClientOptions struct {
	endpoint     string
	usePathStyle bool
}

func clientOptions(settings Config) s3ClientOptions {
	return s3ClientOptions{
		endpoint:     settings.Endpoint,
		usePathStyle: settings.ForcePathStyle || (settings.Endpoint != "" && !settings.UseVirtualHostStyle),
	}
}
