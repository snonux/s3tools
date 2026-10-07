package internal

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type recordingProgress struct {
	started   []ProgressTotal
	advanced  []ProgressTotal
	finished  int
	onAdvance func()
}

func (progress *recordingProgress) Start(_ context.Context, total ProgressTotal) {
	progress.started = append(progress.started, total)
}

func (progress *recordingProgress) Advance(confirmed ProgressTotal) {
	progress.advanced = append(progress.advanced, confirmed)
	if progress.onAdvance != nil {
		progress.onAdvance()
	}
}

func (progress *recordingProgress) Finish() { progress.finished++ }

var _ ProgressObserver = (*recordingProgress)(nil)

func TestDeleteSelectedPrefixesAccountsAllInventoryBeforeDeleting(t *testing.T) {
	client := &fakeDeleter{
		objectPages: []*s3.ListObjectsV2Output{
			{Contents: []types.Object{{Key: aws.String("a/file"), Size: aws.Int64(3)}}},
			{Contents: []types.Object{{Key: aws.String("b/file"), Size: aws.Int64(5)}}},
			{},
			{},
		},
		deleteOutputs: []*s3.DeleteObjectsOutput{
			{Deleted: []types.DeletedObject{{Key: aws.String("a/file")}}},
			{Deleted: []types.DeletedObject{{Key: aws.String("b/file")}}},
		},
	}
	progress := &recordingProgress{}
	summary, err := DeleteSelectedPrefixes(context.Background(), client, "bucket", []string{"a/", "b/"}, Unversioned, nil, progress)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Deleted != 2 || summary.DeletedBytes != 8 {
		t.Fatalf("summary = %#v", summary)
	}
	if len(progress.started) != 1 || progress.started[0] != (ProgressTotal{Objects: 2, Bytes: 8}) {
		t.Fatalf("start = %#v", progress.started)
	}
	if len(progress.advanced) != 2 || progress.advanced[0] != (ProgressTotal{Objects: 1, Bytes: 3}) || progress.advanced[1] != (ProgressTotal{Objects: 1, Bytes: 5}) {
		t.Fatalf("advanced = %#v", progress.advanced)
	}
	if progress.finished != 1 {
		t.Fatalf("finished = %d", progress.finished)
	}
}

func TestDeleteSelectedPrefixesStopsOnCancellation(t *testing.T) {
	entries := make([]types.Object, deleteBatchSize+1)
	deleted := make([]types.DeletedObject, deleteBatchSize)
	for index := range entries {
		entries[index] = types.Object{Key: aws.String("p/" + string(rune(index+1))), Size: aws.Int64(1)}
		if index < len(deleted) {
			deleted[index] = types.DeletedObject{Key: entries[index].Key}
		}
	}
	client := &fakeDeleter{
		objectPages:   []*s3.ListObjectsV2Output{{Contents: entries}},
		deleteOutputs: []*s3.DeleteObjectsOutput{{Deleted: deleted}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	progress := &recordingProgress{onAdvance: cancel}
	summary, err := DeleteSelectedPrefixes(ctx, client, "bucket", []string{"p/"}, Unversioned, nil, progress)
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	if summary.Deleted != deleteBatchSize || len(client.deleteInputs) != 1 {
		t.Fatalf("summary = %#v, delete calls = %d", summary, len(client.deleteInputs))
	}
	if progress.finished != 1 {
		t.Fatalf("finished = %d", progress.finished)
	}
}

func TestFormatProgress(t *testing.T) {
	snapshot := ProgressSnapshot{
		Total:     ProgressTotal{Objects: 10, Bytes: 1024},
		Confirmed: ProgressTotal{Objects: 4, Bytes: 512},
		Elapsed:   4 * time.Second,
		Rate:      ProgressRate{Objects: 2, Bytes: 256},
	}
	got := formatProgress(snapshot)
	want := "deleted 4/10 objects (512 B/1.0 KiB); remaining 6 objects (512 B); rate 2 objects/s (256 B/s); elapsed 00:00:04; ETA 00:00:03"
	if got != want {
		t.Fatalf("formatProgress() = %q, want %q", got, want)
	}
}

func TestProgressSnapshotKeepsFractionalRateAndETA(t *testing.T) {
	snapshot := ProgressSnapshot{Total: ProgressTotal{Objects: 3}, Confirmed: ProgressTotal{Objects: 1}, Rate: ProgressRate{Objects: 0.5}}
	if snapshot.ETA() != 4*time.Second {
		t.Fatalf("ETA() = %s, want 4s", snapshot.ETA())
	}
	if !strings.Contains(formatProgress(snapshot), "rate 0.50 objects/s") {
		t.Fatalf("formatProgress() = %q", formatProgress(snapshot))
	}
}

func TestProgressRendererRateIncludesFirstBatchAfterIdle(t *testing.T) {
	start := time.Date(2026, time.October, 7, 9, 0, 0, 0, time.UTC)
	now := start
	renderer := newProgressRenderer(&bytes.Buffer{}, false, func() time.Time { return now })
	renderer.Start(context.Background(), ProgressTotal{Objects: 2000, Bytes: 20000})

	now = start.Add(11 * time.Second)
	renderer.Advance(ProgressTotal{Objects: 1000, Bytes: 10000})
	now = start.Add(12 * time.Second)
	snapshot := snapshotProgressAt(renderer, now)
	renderer.Finish()

	if snapshot.Rate.Objects != 1000.0/12.0 || snapshot.Rate.Bytes != 10000.0/12.0 {
		t.Fatalf("Rate = %#v, want first batch included across 12s", snapshot.Rate)
	}
	if snapshot.ETA() != 12*time.Second {
		t.Fatalf("ETA() = %s, want 12s", snapshot.ETA())
	}
}

func TestProgressRendererRateUsesBaselineAcrossWindowBoundary(t *testing.T) {
	start := time.Date(2026, time.October, 7, 9, 0, 0, 0, time.UTC)
	now := start
	renderer := newProgressRenderer(&bytes.Buffer{}, false, func() time.Time { return now })
	renderer.Start(context.Background(), ProgressTotal{Objects: 2000, Bytes: 20000})

	now = start.Add(time.Second)
	renderer.Advance(ProgressTotal{Objects: 100, Bytes: 1000})
	now = start.Add(6 * time.Second)
	renderer.Advance(ProgressTotal{Objects: 600, Bytes: 6000})
	now = start.Add(11 * time.Second)
	renderer.Advance(ProgressTotal{Objects: 400, Bytes: 4000})
	now = start.Add(12 * time.Second)
	snapshot := snapshotProgressAt(renderer, now)
	renderer.Finish()

	if snapshot.Rate.Objects != 1000.0/11.0 || snapshot.Rate.Bytes != 10000.0/11.0 {
		t.Fatalf("Rate = %#v, want batches after the retained baseline averaged over 11s", snapshot.Rate)
	}
	if got, want := snapshot.ETA(), time.Duration(float64(time.Second)*900.0/1000.0*11); got != want {
		t.Fatalf("ETA() = %s, want %s", got, want)
	}
}

func TestProgressRendererRateDecaysToZeroWhenIdle(t *testing.T) {
	start := time.Date(2026, time.October, 7, 9, 0, 0, 0, time.UTC)
	now := start
	renderer := newProgressRenderer(&bytes.Buffer{}, false, func() time.Time { return now })
	renderer.Start(context.Background(), ProgressTotal{Objects: 200})

	now = start.Add(11 * time.Second)
	renderer.Advance(ProgressTotal{Objects: 100})
	now = start.Add(12 * time.Second)
	active := snapshotProgressAt(renderer, now)
	now = start.Add(22 * time.Second)
	idle := snapshotProgressAt(renderer, now)
	renderer.Finish()

	if active.Rate.Objects <= 0 {
		t.Fatalf("active rate = %v, want positive rate", active.Rate.Objects)
	}
	if idle.Rate != (ProgressRate{}) {
		t.Fatalf("idle rate = %#v, want zero after the batch leaves the window", idle.Rate)
	}
	if idle.ETA() != 0 {
		t.Fatalf("idle ETA() = %s, want zero for unknown ETA", idle.ETA())
	}
}

func snapshotProgressAt(renderer *ProgressRenderer, now time.Time) ProgressSnapshot {
	renderer.mu.Lock()
	defer renderer.mu.Unlock()
	return renderer.snapshotLocked(now)
}

func TestProgressRendererTerminalAndLogOutput(t *testing.T) {
	now := time.Date(2026, time.October, 7, 9, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		terminal bool
		want     string
	}{
		{name: "terminal", terminal: true, want: "\rdeleted 1/1 objects (1 B/1 B); remaining 0 objects (0 B); rate 0 objects/s (0 B/s); elapsed 00:00:00; ETA 00:00:00\033[K\n"},
		{name: "log", want: "2026-10-07T09:00:00Z deleted 1/1 objects (1 B/1 B); remaining 0 objects (0 B); rate 0 objects/s (0 B/s); elapsed 00:00:00; ETA 00:00:00\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output := &bytes.Buffer{}
			renderer := newProgressRenderer(output, test.terminal, func() time.Time { return now })
			renderer.Start(context.Background(), ProgressTotal{Objects: 1, Bytes: 1})
			renderer.Advance(ProgressTotal{Objects: 1, Bytes: 1})
			renderer.Finish()
			if got := output.String(); got != test.want {
				t.Fatalf("output = %q, want %q", got, test.want)
			}
		})
	}
}

func TestProgressRendererCancellationDoesNotBlockFinish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	renderer := NewProgressRenderer(&bytes.Buffer{}, false)
	renderer.Start(ctx, ProgressTotal{Objects: 1})
	cancel()
	done := make(chan struct{})
	go func() {
		renderer.Finish()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Finish blocked after cancellation")
	}
}

func TestFormatDuration(t *testing.T) {
	if got := formatDuration(2*time.Hour + 3*time.Minute + 4*time.Second); got != "02:03:04" {
		t.Fatalf("formatDuration() = %q", got)
	}
	if strings.Contains(formatProgress(ProgressSnapshot{}), "NaN") {
		t.Fatal("zero progress contains NaN")
	}
}

func TestProgressRendererFinishBeforeStart(t *testing.T) {
	renderer := NewProgressRenderer(&bytes.Buffer{}, false)
	renderer.Finish()
	renderer.Start(context.Background(), ProgressTotal{Objects: 1})
	renderer.Finish()
}
