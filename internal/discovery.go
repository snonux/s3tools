package internal

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ObjectLister provides the S3 listing calls used by discovery and inventory.
type ObjectLister interface {
	ListObjectVersions(context.Context, *s3.ListObjectVersionsInput, ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// ObjectEntry identifies one deletion unit and its stored bytes.
type ObjectEntry struct {
	Key          string
	VersionID    string
	Size         int64
	DeleteMarker bool
}

// Inventory is the eligible content found beneath a selected prefix.
type Inventory struct {
	Entries []ObjectEntry
	Bytes   int64
}

// InventoryProgress reports completed listing pages and eligible objects.
type InventoryProgress struct {
	Pages   int
	Objects int
	Bytes   int64
}

// DiscoveryProgress reports completed pages and unique top-level prefixes.
type DiscoveryProgress struct {
	Pages    int
	Prefixes int
}

// DiscoverPrefixes lists every top-level directory prefix in bucket.
func DiscoverPrefixes(ctx context.Context, client ObjectLister, bucket string, versioning Versioning, exclusions []string) ([]string, error) {
	return DiscoverPrefixesWithProgress(ctx, client, bucket, versioning, exclusions, nil)
}

// DiscoverPrefixesWithProgress lists prefixes and reports each completed page.
func DiscoverPrefixesWithProgress(ctx context.Context, client ObjectLister, bucket string, versioning Versioning, exclusions []string, onPage func(DiscoveryProgress)) ([]string, error) {
	if versioning == Versioned {
		return discoverVersionedPrefixes(ctx, client, bucket, exclusions, onPage)
	}

	return discoverUnversionedPrefixes(ctx, client, bucket, exclusions, onPage)
}

// InventoryPrefix lists deletion units under prefix, excluding protected keys.
func InventoryPrefix(ctx context.Context, client ObjectLister, bucket, prefix string, versioning Versioning, exclusions []string) (Inventory, error) {
	return InventoryPrefixWithProgress(ctx, client, bucket, prefix, versioning, exclusions, nil)
}

// InventoryPrefixWithProgress inventories a prefix and reports each completed page.
func InventoryPrefixWithProgress(ctx context.Context, client ObjectLister, bucket, prefix string, versioning Versioning, exclusions []string, onPage func(InventoryProgress)) (Inventory, error) {
	if err := validatePrefix(prefix); err != nil {
		return Inventory{}, err
	}
	if IsExcluded(prefix, exclusions) {
		return Inventory{}, fmt.Errorf("prefix %q is protected by an exclusion", prefix)
	}
	if versioning == Versioned {
		return inventoryVersions(ctx, client, bucket, prefix, exclusions, onPage)
	}

	return inventoryObjects(ctx, client, bucket, prefix, exclusions, onPage)
}

// IsExcluded reports whether key matches an exact-key or subtree protection rule.
func IsExcluded(key string, exclusions []string) bool {
	for _, exclusion := range exclusions {
		if strings.HasSuffix(exclusion, "/") && strings.HasPrefix(key, exclusion) {
			return true
		}
		if key == exclusion {
			return true
		}
	}

	return false
}

func discoverVersionedPrefixes(ctx context.Context, client ObjectLister, bucket string, exclusions []string, onPage func(DiscoveryProgress)) ([]string, error) {
	prefixes := make(map[string]struct{})
	pages := 0
	var keyMarker, versionMarker *string
	for {
		result, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket), Delimiter: aws.String("/"), EncodingType: types.EncodingTypeUrl, KeyMarker: keyMarker, VersionIdMarker: versionMarker})
		if err != nil {
			return nil, fmt.Errorf("list versioned prefixes in %q: %w", bucket, err)
		}
		if result == nil {
			return nil, fmt.Errorf("list versioned prefixes in %q: empty response", bucket)
		}
		if err := decodeListObjectVersions(result); err != nil {
			return nil, fmt.Errorf("list versioned prefixes in %q: decode URL-encoded keys: %w", bucket, err)
		}
		for _, item := range result.CommonPrefixes {
			prefix := aws.ToString(item.Prefix)
			if prefix != "" && !IsExcluded(prefix, exclusions) {
				prefixes[prefix] = struct{}{}
			}
		}
		for _, item := range result.Versions {
			addDirectoryMarker(prefixes, aws.ToString(item.Key), exclusions)
		}
		for _, item := range result.DeleteMarkers {
			addDirectoryMarker(prefixes, aws.ToString(item.Key), exclusions)
		}
		pages++
		if onPage != nil {
			onPage(DiscoveryProgress{Pages: pages, Prefixes: len(prefixes)})
		}
		if !aws.ToBool(result.IsTruncated) {
			return sortedPrefixes(prefixes), nil
		}
		if result.NextKeyMarker == nil || result.NextVersionIdMarker == nil {
			return nil, fmt.Errorf("list versioned prefixes in %q: truncated response has no markers", bucket)
		}
		keyMarker, versionMarker = result.NextKeyMarker, result.NextVersionIdMarker
	}
}

func discoverUnversionedPrefixes(ctx context.Context, client ObjectLister, bucket string, exclusions []string, onPage func(DiscoveryProgress)) ([]string, error) {
	prefixes := make(map[string]struct{})
	pages := 0
	var continuationToken *string
	for {
		result, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Delimiter: aws.String("/"), EncodingType: types.EncodingTypeUrl, ContinuationToken: continuationToken})
		if err != nil {
			return nil, fmt.Errorf("list prefixes in %q: %w", bucket, err)
		}
		if result == nil {
			return nil, fmt.Errorf("list prefixes in %q: empty response", bucket)
		}
		if err := decodeListObjectsV2(result); err != nil {
			return nil, fmt.Errorf("list prefixes in %q: decode URL-encoded keys: %w", bucket, err)
		}
		for _, item := range result.CommonPrefixes {
			prefix := aws.ToString(item.Prefix)
			if prefix != "" && !IsExcluded(prefix, exclusions) {
				prefixes[prefix] = struct{}{}
			}
		}
		for _, item := range result.Contents {
			addDirectoryMarker(prefixes, aws.ToString(item.Key), exclusions)
		}
		pages++
		if onPage != nil {
			onPage(DiscoveryProgress{Pages: pages, Prefixes: len(prefixes)})
		}
		if !aws.ToBool(result.IsTruncated) {
			return sortedPrefixes(prefixes), nil
		}
		if result.NextContinuationToken == nil {
			return nil, fmt.Errorf("list prefixes in %q: truncated response has no continuation token", bucket)
		}
		continuationToken = result.NextContinuationToken
	}
}

func inventoryVersions(ctx context.Context, client ObjectLister, bucket, prefix string, exclusions []string, onPage func(InventoryProgress)) (Inventory, error) {
	inventory := Inventory{}
	pages := 0
	var keyMarker, versionMarker *string
	for {
		result, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket), Prefix: aws.String(prefix), EncodingType: types.EncodingTypeUrl, KeyMarker: keyMarker, VersionIdMarker: versionMarker})
		if err != nil {
			return Inventory{}, fmt.Errorf("list versions below %q: %w", prefix, err)
		}
		if result == nil {
			return Inventory{}, fmt.Errorf("list versions below %q: empty response", prefix)
		}
		if err := decodeListObjectVersions(result); err != nil {
			return Inventory{}, fmt.Errorf("list versions below %q: decode URL-encoded keys: %w", prefix, err)
		}
		for _, object := range result.Versions {
			key := aws.ToString(object.Key)
			if !IsExcluded(key, exclusions) {
				addEntry(&inventory, ObjectEntry{Key: key, VersionID: aws.ToString(object.VersionId), Size: aws.ToInt64(object.Size)})
			}
		}
		for _, marker := range result.DeleteMarkers {
			key := aws.ToString(marker.Key)
			if !IsExcluded(key, exclusions) {
				addEntry(&inventory, ObjectEntry{Key: key, VersionID: aws.ToString(marker.VersionId), DeleteMarker: true})
			}
		}
		pages++
		if onPage != nil {
			onPage(InventoryProgress{Pages: pages, Objects: len(inventory.Entries), Bytes: inventory.Bytes})
		}
		if !aws.ToBool(result.IsTruncated) {
			return inventory, nil
		}
		if result.NextKeyMarker == nil || result.NextVersionIdMarker == nil {
			return Inventory{}, fmt.Errorf("list versions below %q: truncated response has no markers", prefix)
		}
		keyMarker, versionMarker = result.NextKeyMarker, result.NextVersionIdMarker
	}
}

func inventoryObjects(ctx context.Context, client ObjectLister, bucket, prefix string, exclusions []string, onPage func(InventoryProgress)) (Inventory, error) {
	inventory := Inventory{}
	pages := 0
	var continuationToken *string
	for {
		result, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix), EncodingType: types.EncodingTypeUrl, ContinuationToken: continuationToken})
		if err != nil {
			return Inventory{}, fmt.Errorf("list objects below %q: %w", prefix, err)
		}
		if result == nil {
			return Inventory{}, fmt.Errorf("list objects below %q: empty response", prefix)
		}
		if err := decodeListObjectsV2(result); err != nil {
			return Inventory{}, fmt.Errorf("list objects below %q: decode URL-encoded keys: %w", prefix, err)
		}
		for _, object := range result.Contents {
			key := aws.ToString(object.Key)
			if !IsExcluded(key, exclusions) {
				addEntry(&inventory, ObjectEntry{Key: key, Size: aws.ToInt64(object.Size)})
			}
		}
		pages++
		if onPage != nil {
			onPage(InventoryProgress{Pages: pages, Objects: len(inventory.Entries), Bytes: inventory.Bytes})
		}
		if !aws.ToBool(result.IsTruncated) {
			return inventory, nil
		}
		if result.NextContinuationToken == nil {
			return Inventory{}, fmt.Errorf("list objects below %q: truncated response has no continuation token", prefix)
		}
		continuationToken = result.NextContinuationToken
	}
}

func addEntry(inventory *Inventory, entry ObjectEntry) {
	inventory.Entries = append(inventory.Entries, entry)
	inventory.Bytes += entry.Size
}

func sortedPrefixes(prefixes map[string]struct{}) []string {
	result := make([]string, 0, len(prefixes))
	for prefix := range prefixes {
		result = append(result, prefix)
	}
	sort.Strings(result)
	return result
}

func addDirectoryMarker(prefixes map[string]struct{}, key string, exclusions []string) {
	if strings.HasSuffix(key, "/") && !IsExcluded(key, exclusions) {
		prefixes[key] = struct{}{}
	}
}

func validatePrefix(prefix string) error {
	if prefix == "" || prefix == "/" || !strings.HasSuffix(prefix, "/") {
		return fmt.Errorf("prefix %q must be a non-root directory ending in /", prefix)
	}
	return nil
}
