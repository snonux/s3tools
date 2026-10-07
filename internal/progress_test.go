package internal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
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

type recordingStreamingProgress struct {
	recordingProgress
	streamStarted int
	discovered    []ProgressTotal
	completed     int
}

func (progress *recordingStreamingProgress) StartStreaming(_ context.Context) {
	progress.streamStarted++
}

func (progress *recordingStreamingProgress) Discover(total ProgressTotal) {
	progress.discovered = append(progress.discovered, total)
}

func (progress *recordingStreamingProgress) CompleteDiscovery() {
	progress.completed++
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
var _ StreamingProgressObserver = (*recordingStreamingProgress)(nil)

func TestDeleteSelectedPrefixesReportsStreamingDiscovery(t *testing.T) {
	client := &fakeDeleter{
		objectPages: []*s3.ListObjectsV2Output{
			{Contents: []types.Object{{Key: aws.String("p/a"), Size: aws.Int64(3)}}},
			{},
		},
		deleteOutputs: []*s3.DeleteObjectsOutput{{Deleted: []types.DeletedObject{{Key: aws.String("p/a")}}}},
	}
	progress := &recordingStreamingProgress{}
	summary, err := DeleteSelectedPrefixes(context.Background(), client, "bucket", []string{"p/"}, Unversioned, nil, progress)
	if err != nil || summary.Deleted != 1 || summary.DeletedBytes != 3 {
		t.Fatalf("deletion = %#v, %v", summary, err)
	}
	if progress.streamStarted != 1 || progress.completed != 1 || progress.finished != 1 {
		t.Fatalf("lifecycle: start=%d complete=%d finish=%d", progress.streamStarted, progress.completed, progress.finished)
	}
	if len(progress.discovered) != 1 || progress.discovered[0] != (ProgressTotal{Objects: 1, Bytes: 3}) {
		t.Fatalf("discovered = %#v", progress.discovered)
	}
	if len(progress.advanced) != 1 || progress.advanced[0] != (ProgressTotal{Objects: 1, Bytes: 3}) {
		t.Fatalf("advanced = %#v", progress.advanced)
	}
}

func TestDeleteSelectedPrefixesStreamsProgressAsPrefixesFinish(t *testing.T) {
	client := &fakeDeleter{
		objectPages: []*s3.ListObjectsV2Output{
			{Contents: []types.Object{{Key: aws.String("a/file"), Size: aws.Int64(3)}}},
			{},
			{Contents: []types.Object{{Key: aws.String("b/file"), Size: aws.Int64(5)}}},
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
	if len(progress.started) != 1 || progress.started[0] != (ProgressTotal{}) {
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

func TestDeleteSelectedPrefixesReportsConfirmedProgressBeforeIndividualFailure(t *testing.T) {
	requestErr := errors.New("individual delete failed")
	tests := []struct {
		name       string
		objects    []types.Object
		bulkOutput *s3.DeleteObjectsOutput
		singleErrs []error
		want       ProgressTotal
	}{
		{
			name: "confirmed bulk delete before individual failure",
			objects: []types.Object{
				{Key: aws.String("p/control\x01"), Size: aws.Int64(20)},
				{Key: aws.String("p/ordinary"), Size: aws.Int64(10)},
			},
			bulkOutput: &s3.DeleteObjectsOutput{Deleted: []types.DeletedObject{{Key: aws.String("p/ordinary")}}},
			singleErrs: []error{requestErr},
			want:       ProgressTotal{Objects: 1, Bytes: 10},
		},
		{
			name: "confirmed individual delete before later failure",
			objects: []types.Object{
				{Key: aws.String("p/control\x01"), Size: aws.Int64(20)},
				{Key: aws.String("p/control\x02"), Size: aws.Int64(30)},
			},
			singleErrs: []error{nil, requestErr},
			want:       ProgressTotal{Objects: 1, Bytes: 20},
		},
		{
			name: "unacknowledged bulk delete is not counted without verification",
			objects: []types.Object{
				{Key: aws.String("p/control\x01"), Size: aws.Int64(20)},
				{Key: aws.String("p/ordinary"), Size: aws.Int64(10)},
			},
			bulkOutput: &s3.DeleteObjectsOutput{},
			singleErrs: []error{requestErr},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeDeleter{
				objectPages:        []*s3.ListObjectsV2Output{{Contents: test.objects}},
				deleteObjectErrors: test.singleErrs,
			}
			if test.bulkOutput != nil {
				client.deleteOutputs = []*s3.DeleteObjectsOutput{test.bulkOutput}
			}
			var output bytes.Buffer
			stats := &BucketStats{Complete: true, Objects: 5, Bytes: 100}
			progress := NewProgressRendererWithBucketStats(&output, false, stats)
			summary, err := DeleteSelectedPrefixes(context.Background(), client, "bucket", []string{"p/"}, Unversioned, nil, progress)
			if !errors.Is(err, requestErr) {
				t.Fatalf("error = %v, want original individual deletion error", err)
			}
			if summary.Deleted != test.want.Objects || summary.DeletedBytes != test.want.Bytes {
				t.Fatalf("summary = %#v, want confirmed work %#v", summary, test.want)
			}
			want := fmt.Sprintf("deleted %d objects", test.want.Objects)
			if !strings.Contains(output.String(), want) {
				t.Fatalf("final progress = %q, want %q", output.String(), want)
			}
			if !strings.Contains(output.String(), "total unknown (inventory in progress)") {
				t.Fatalf("final progress = %q, want unknown total after early failure", output.String())
			}
			want = fmt.Sprintf("%d objects (%s)", stats.Objects-int64(test.want.Objects), formatBytes(stats.Bytes-test.want.Bytes))
			if !strings.Contains(output.String(), "bucket remainder estimate from snapshot") || !strings.Contains(output.String(), want) {
				t.Fatalf("final progress = %q, want bucket remainder %q", output.String(), want)
			}
		})
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
	want := "deleted 4/10 objects (512 B/1.0 KiB); remaining 6 objects (512 B); rate 2 objects/s (256 B/s); elapsed 00:00:04; ETA 00:00:03; progress [####........] 40%"
	if got != want {
		t.Fatalf("formatProgress() = %q, want %q", got, want)
	}
}

func TestFormatProgressShowsSnapshotBucketRemainder(t *testing.T) {
	snapshotTime := time.Date(2026, time.October, 7, 10, 30, 0, 0, time.UTC)
	snapshot := ProgressSnapshot{
		Total:       ProgressTotal{Objects: 2, Bytes: 20},
		Confirmed:   ProgressTotal{Objects: 1, Bytes: 10},
		BucketStats: &BucketStats{Objects: 4, Bytes: 40, Complete: true, SnapshotAt: snapshotTime},
	}
	got := formatProgress(snapshot)
	want := "bucket remainder estimate from snapshot 2026-10-07T10:30:00Z: 3 objects (30 B)"
	if !strings.Contains(got, want) {
		t.Fatalf("formatProgress() = %q, want %q", got, want)
	}
}

func TestFormatProgressMarksIncompleteBucketSnapshotUnavailable(t *testing.T) {
	got := formatProgress(ProgressSnapshot{BucketStats: &BucketStats{Objects: 9, Bytes: 90, Incomplete: "page limit"}})
	want := "bucket remainder estimate unavailable (incomplete scan: page limit)"
	if !strings.Contains(got, want) {
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
		{name: "terminal", terminal: true, want: "\rdeleted 1/1 objects (1 B/1 B); remaining 0 objects (0 B); rate 0 objects/s (0 B/s); elapsed 00:00:00; ETA 00:00:00; progress [############] 100%\033[K\n"},
		{name: "log", want: "2026-10-07T09:00:00Z deleted 1/1 objects (1 B/1 B); remaining 0 objects (0 B); rate 0 objects/s (0 B/s); elapsed 00:00:00; ETA 00:00:00; progress [############] 100%\n"},
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

type channelProgressWriter struct {
	lines chan string
}

func (writer channelProgressWriter) Write(data []byte) (int, error) {
	writer.lines <- string(data)
	return len(data), nil
}

func TestProgressRendererReportsWhileTotalsPending(t *testing.T) {
	output := channelProgressWriter{lines: make(chan string, 256)}
	renderer := NewProgressRenderer(output, false)
	renderer.interval = 20 * time.Millisecond
	renderer.SetPhase("discovering prefixes")
	renderer.Begin(context.Background())
	renderer.Begin(context.Background())
	defer renderer.Finish()

	initial := readProgressLine(t, output.lines)
	if !strings.Contains(initial, "status: discovering prefixes; total pending") {
		t.Fatalf("initial status = %q", initial)
	}
	periodic := readProgressLine(t, output.lines)
	if !strings.Contains(periodic, "status: discovering prefixes; total pending") {
		t.Fatalf("periodic status = %q", periodic)
	}
	renderer.SetPhase("inventorying prefix 1/2")
	_ = readProgressContaining(t, output.lines, "status: inventorying prefix 1/2; total pending")
	renderer.SetPhaseQuiet("inventorying prefix 2/2")
	_ = readProgressContaining(t, output.lines, "status: inventorying prefix 2/2; total pending")
}

func TestProgressRendererTransitionsToDeletion(t *testing.T) {
	output := channelProgressWriter{lines: make(chan string, 256)}
	renderer := NewProgressRenderer(output, false)
	renderer.interval = 20 * time.Millisecond
	renderer.SetPhase("inventorying")
	renderer.Begin(context.Background())
	_ = readProgressLine(t, output.lines)
	renderer.Start(context.Background(), ProgressTotal{Objects: 2, Bytes: 20})
	renderer.SetPhase("deleting batch")
	line := readProgressContaining(t, output.lines, "status: deleting batch; deleted 0/2 objects")
	if !strings.Contains(line, "status: deleting batch; deleted 0/2 objects") || !strings.Contains(line, "progress [............] 0%") {
		t.Fatalf("deletion status = %q", line)
	}
	renderer.Advance(ProgressTotal{Objects: 1, Bytes: 10})
	renderer.Finish()
	final := readProgressContaining(t, output.lines, "deleted 1/2 objects")
	if !strings.Contains(final, "deleted 1/2 objects") || !strings.Contains(final, "progress [######......] 50%") {
		t.Fatalf("final status = %q", final)
	}
}

func TestProgressRendererStreamingTransitionsToExactTotal(t *testing.T) {
	output := channelProgressWriter{lines: make(chan string, 256)}
	renderer := NewProgressRenderer(output, false)
	renderer.SetPhase("inventorying and deleting")
	renderer.StartStreaming(context.Background())
	initial := readProgressLine(t, output.lines)
	if !strings.Contains(initial, "total unknown (inventory in progress)") {
		t.Fatalf("initial progress = %q", initial)
	}

	renderer.Discover(ProgressTotal{Objects: 3, Bytes: 30})
	renderer.Advance(ProgressTotal{Objects: 1, Bytes: 10})
	renderer.SetPhase("deleting while inventory continues")
	streaming := readProgressContaining(t, output.lines, "status: deleting while inventory continues")
	for _, want := range []string{"deleted 1 objects (10 B)", "discovered 3 objects (30 B)", "known pending 2 objects (20 B)", "ETA unknown"} {
		if !strings.Contains(streaming, want) {
			t.Fatalf("streaming progress = %q, missing %q", streaming, want)
		}
	}
	if strings.Contains(streaming, "progress [") || strings.Contains(streaming, "%") {
		t.Fatalf("streaming progress claims an exact percentage: %q", streaming)
	}

	renderer.CompleteDiscovery()
	exact := readProgressContaining(t, output.lines, "deleted 1/3 objects")
	if !strings.Contains(exact, "remaining 2 objects (20 B)") || !strings.Contains(exact, "progress [####........] 33%") {
		t.Fatalf("exact progress = %q", exact)
	}
	renderer.Discover(ProgressTotal{Objects: 1, Bytes: 10})
	renderer.Advance(ProgressTotal{Objects: 2, Bytes: 20})
	renderer.Finish()
	final := readProgressContaining(t, output.lines, "deleted 3/3 objects")
	if !strings.Contains(final, "progress [############] 100%") {
		t.Fatalf("final progress = %q", final)
	}
}

func TestProgressRendererConcurrentStreamingUpdates(t *testing.T) {
	var output bytes.Buffer
	renderer := NewProgressRenderer(&output, false)
	renderer.StartStreaming(context.Background())
	const workers = 10
	const updates = 100
	var wg sync.WaitGroup
	for range workers {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for range updates {
				renderer.Discover(ProgressTotal{Objects: 1, Bytes: 2})
			}
		}()
		go func() {
			defer wg.Done()
			for range updates {
				renderer.Advance(ProgressTotal{Objects: 1, Bytes: 2})
			}
		}()
	}
	wg.Wait()
	renderer.CompleteDiscovery()
	renderer.Finish()
	want := ProgressTotal{Objects: workers * updates, Bytes: workers * updates * 2}
	if renderer.total != want || renderer.confirmed != want {
		t.Fatalf("totals = %#v, confirmed = %#v, want %#v", renderer.total, renderer.confirmed, want)
	}
	if !strings.Contains(output.String(), "progress [############] 100%") {
		t.Fatalf("final progress = %q", output.String())
	}
}

func TestProgressRendererFinishBeforeTotals(t *testing.T) {
	output := channelProgressWriter{lines: make(chan string, 256)}
	renderer := NewProgressRenderer(output, false)
	renderer.interval = 20 * time.Millisecond
	renderer.SetPhase("checking versioning")
	renderer.Begin(context.Background())
	_ = readProgressLine(t, output.lines)
	renderer.Finish()
	final := readProgressLine(t, output.lines)
	if !strings.Contains(final, "status: checking versioning; total pending") {
		t.Fatalf("final status = %q", final)
	}
	select {
	case line := <-output.lines:
		t.Fatalf("status after Finish = %q", line)
	case <-time.After(50 * time.Millisecond):
	}
}

func readProgressLine(t *testing.T, lines <-chan string) string {
	t.Helper()
	select {
	case line := <-lines:
		return line
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for progress")
		return ""
	}
}

func readProgressContaining(t *testing.T, lines <-chan string, want string) string {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case line := <-lines:
			if strings.Contains(line, want) {
				return line
			}
		case <-deadline:
			t.Fatalf("timed out waiting for progress containing %q", want)
			return ""
		}
	}
}
