package internal

import (
	"context"
	"fmt"
	"io"
)

type dryRunTotal struct {
	objects int
	markers int
	bytes   int64
}

func (total dryRunTotal) text(versioning Versioning) string {
	if versioning == Versioned {
		return fmt.Sprintf("%d object versions, %d delete markers, %d bytes (%s)", total.objects, total.markers, total.bytes, formatBytes(total.bytes))
	}
	return fmt.Sprintf("%d objects, %d bytes (%s)", total.objects, total.bytes, formatBytes(total.bytes))
}

// dryRunSelectedPrefixes accepts only listing APIs so a preview cannot delete objects.
func dryRunSelectedPrefixes(ctx context.Context, client ObjectLister, bucket string, prefixes []string, versioning Versioning, exclusions []string, output io.Writer, progress *ProgressRenderer) error {
	totals := make([]dryRunTotal, 0, len(prefixes))
	total := dryRunTotal{}
	for _, prefix := range prefixes {
		current := dryRunTotal{}
		pages := 0
		progress.SetPhase(fmt.Sprintf("dry run: scanning prefix %q", prefix))
		err := walkPrefixInventory(ctx, client, bucket, prefix, versioning, exclusions, func(page Inventory) error {
			for _, entry := range page.Entries {
				if entry.DeleteMarker {
					current.markers++
				} else {
					current.objects++
				}
			}
			current.bytes += page.Bytes
			pages++
			progress.SetPhaseQuiet(fmt.Sprintf("dry run: scanning prefix %q: %d pages, %s", prefix, pages, current.text(versioning)))
			return ctx.Err()
		})
		if err != nil {
			progress.SetPhase("dry run stopped")
			return fmt.Errorf("dry run: %w", err)
		}
		totals = append(totals, current)
		total.objects += current.objects
		total.markers += current.markers
		total.bytes += current.bytes
	}
	progress.SetPhase("dry run complete")
	progress.Finish()
	if _, err := fmt.Fprintf(output, "Dry run for bucket %q: nothing was deleted.\n", bucket); err != nil {
		return fmt.Errorf("print dry-run report: %w", err)
	}
	for index, prefix := range prefixes {
		if _, err := fmt.Fprintf(output, "Would delete under %q: %s\n", prefix, totals[index].text(versioning)); err != nil {
			return fmt.Errorf("print dry-run prefix: %w", err)
		}
	}
	for _, exclusion := range exclusions {
		if _, err := fmt.Fprintf(output, "Protected (excluded from totals): %q\n", exclusion); err != nil {
			return fmt.Errorf("print dry-run exclusion: %w", err)
		}
	}
	if _, err := fmt.Fprintf(output, "Total eligible: %s\nPreview reflects listing time; concurrent writes can change the result.\n", total.text(versioning)); err != nil {
		return fmt.Errorf("print dry-run total: %w", err)
	}
	return nil
}
