package internal

import (
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// The AWS SDK leaves URL-encoded listing fields untouched. Decode only when
// the service declares that encoding; PathUnescape preserves literal plus signs.
func decodeListObjectsV2(result *s3.ListObjectsV2Output) error {
	if result.EncodingType != types.EncodingTypeUrl {
		return nil
	}
	for index := range result.Contents {
		if err := decodeURLValue(result.Contents[index].Key); err != nil {
			return fmt.Errorf("contents key %d: %w", index, err)
		}
	}
	for index := range result.CommonPrefixes {
		if err := decodeURLValue(result.CommonPrefixes[index].Prefix); err != nil {
			return fmt.Errorf("common prefix %d: %w", index, err)
		}
	}
	return nil
}

func decodeListObjectVersions(result *s3.ListObjectVersionsOutput) error {
	if result.EncodingType != types.EncodingTypeUrl {
		return nil
	}
	for index := range result.Versions {
		if err := decodeURLValue(result.Versions[index].Key); err != nil {
			return fmt.Errorf("version key %d: %w", index, err)
		}
	}
	for index := range result.DeleteMarkers {
		if err := decodeURLValue(result.DeleteMarkers[index].Key); err != nil {
			return fmt.Errorf("delete marker key %d: %w", index, err)
		}
	}
	for index := range result.CommonPrefixes {
		if err := decodeURLValue(result.CommonPrefixes[index].Prefix); err != nil {
			return fmt.Errorf("common prefix %d: %w", index, err)
		}
	}
	if err := decodeURLValue(result.NextKeyMarker); err != nil {
		return fmt.Errorf("next key marker: %w", err)
	}
	return nil
}

func decodeURLValue(value *string) error {
	if value == nil {
		return nil
	}
	decoded, err := url.PathUnescape(*value)
	if err != nil {
		return err
	}
	*value = decoded
	return nil
}
