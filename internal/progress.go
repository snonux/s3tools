package internal

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

const (
	progressInterval   = 2 * time.Second
	progressRateWindow = 10 * time.Second
)

// ProgressTotal identifies the selected deletion work.
type ProgressTotal struct {
	Objects int
	Bytes   int64
}

// ProgressRate describes confirmed deletion throughput per second.
type ProgressRate struct {
	Objects float64
	Bytes   float64
}

// ProgressSnapshot describes deletion work at a point in time.
type ProgressSnapshot struct {
	Total       ProgressTotal
	Confirmed   ProgressTotal
	Elapsed     time.Duration
	Rate        ProgressRate
	BucketStats *BucketStats
}

// Remaining returns work which S3 has not confirmed as deleted.
func (snapshot ProgressSnapshot) Remaining() ProgressTotal {
	return ProgressTotal{
		Objects: max(snapshot.Total.Objects-snapshot.Confirmed.Objects, 0),
		Bytes:   max(snapshot.Total.Bytes-snapshot.Confirmed.Bytes, 0),
	}
}

// ETA estimates the time remaining at the current confirmed-object rate.
func (snapshot ProgressSnapshot) ETA() time.Duration {
	remaining := snapshot.Remaining()
	if remaining.Objects == 0 {
		return 0
	}
	if snapshot.Rate.Objects <= 0 {
		return 0
	}
	return time.Duration(float64(remaining.Objects) / float64(snapshot.Rate.Objects) * float64(time.Second))
}

// ProgressObserver receives confirmed deletion accounting.
type ProgressObserver interface {
	Start(context.Context, ProgressTotal)
	Advance(ProgressTotal)
	Finish()
}

type progressSample struct {
	at        time.Time
	confirmed ProgressTotal
}

// ProgressRenderer writes live deletion progress. Terminal output replaces a
// single line; non-terminal output writes timestamped lines suitable for logs.
type ProgressRenderer struct {
	output   io.Writer
	terminal bool
	now      func() time.Time
	interval time.Duration

	mu              sync.Mutex
	begun           bool
	started         bool
	startTime       time.Time
	phase           string
	total           ProgressTotal
	confirmed       ProgressTotal
	bucketStats     *BucketStats
	samples         []progressSample
	done            chan struct{}
	finished        sync.Once
	finishRequested bool
	wg              sync.WaitGroup
}

// NewProgressRenderer creates a renderer that reports every two seconds.
func NewProgressRenderer(output io.Writer, terminal bool) *ProgressRenderer {
	return NewProgressRendererWithBucketStats(output, terminal, nil)
}

// NewProgressRendererWithBucketStats creates a progress renderer with an optional whole-bucket snapshot.
func NewProgressRendererWithBucketStats(output io.Writer, terminal bool, bucketStats *BucketStats) *ProgressRenderer {
	return newProgressRendererWithBucketStats(output, terminal, time.Now, bucketStats)
}

func newProgressRenderer(output io.Writer, terminal bool, now func() time.Time) *ProgressRenderer {
	return newProgressRendererWithBucketStats(output, terminal, now, nil)
}

func newProgressRendererWithBucketStats(output io.Writer, terminal bool, now func() time.Time, bucketStats *BucketStats) *ProgressRenderer {
	return &ProgressRenderer{output: output, terminal: terminal, now: now, interval: progressInterval, bucketStats: bucketStats, done: make(chan struct{})}
}

// Begin starts reporting status before the amount of work is known.
func (renderer *ProgressRenderer) Begin(ctx context.Context) {
	renderer.mu.Lock()
	if renderer.begun || renderer.finishRequested {
		renderer.mu.Unlock()
		return
	}
	renderer.begun = true
	renderer.startTime = renderer.now()
	renderer.wg.Add(1)
	renderer.mu.Unlock()

	renderer.render(false)
	go renderer.renderLoop(ctx)
}

// SetPhase describes the current operation and reports a change immediately.
func (renderer *ProgressRenderer) SetPhase(phase string) {
	if !renderer.setPhase(phase) {
		return
	}
	renderer.render(false)
}

// SetPhaseQuiet describes the current operation for the next periodic report.
func (renderer *ProgressRenderer) SetPhaseQuiet(phase string) {
	_ = renderer.setPhase(phase)
}

func (renderer *ProgressRenderer) setPhase(phase string) bool {
	renderer.mu.Lock()
	defer renderer.mu.Unlock()
	if renderer.phase == phase {
		return false
	}
	renderer.phase = phase
	return renderer.begun && !renderer.finishRequested
}

// SetBucketStats updates the optional whole-bucket snapshot.
func (renderer *ProgressRenderer) SetBucketStats(stats *BucketStats) {
	renderer.mu.Lock()
	renderer.bucketStats = stats
	renderer.mu.Unlock()
}

// Start begins progress reporting for total work.
func (renderer *ProgressRenderer) Start(ctx context.Context, total ProgressTotal) {
	renderer.mu.Lock()
	if renderer.started || (renderer.begun && renderer.finishRequested) {
		renderer.mu.Unlock()
		return
	}
	renderer.started = true
	startLoop := !renderer.begun && !renderer.finishRequested
	if !renderer.begun {
		renderer.begun = true
		renderer.startTime = renderer.now()
	}
	renderer.total = total
	renderer.samples = []progressSample{{at: renderer.now()}}
	stopImmediately := renderer.finishRequested
	if startLoop {
		renderer.wg.Add(1)
	}
	renderer.mu.Unlock()

	if stopImmediately {
		renderer.finish()
		return
	}
	if startLoop {
		go renderer.renderLoop(ctx)
	}
}

// Advance adds deletion work which S3 confirmed.
func (renderer *ProgressRenderer) Advance(confirmed ProgressTotal) {
	renderer.mu.Lock()
	defer renderer.mu.Unlock()
	if !renderer.started {
		return
	}
	renderer.confirmed.Objects += confirmed.Objects
	renderer.confirmed.Bytes += confirmed.Bytes
	now := renderer.now()
	renderer.samples = append(renderer.samples, progressSample{at: now, confirmed: renderer.confirmed})
	renderer.samples = trimProgressSamples(renderer.samples, now)
}

// Finish stops progress reporting and renders its final state.
func (renderer *ProgressRenderer) Finish() {
	renderer.mu.Lock()
	begun := renderer.begun
	renderer.finishRequested = true
	renderer.mu.Unlock()
	if !begun {
		return
	}
	renderer.finish()
}

func (renderer *ProgressRenderer) finish() {
	renderer.finished.Do(func() {
		close(renderer.done)
		renderer.wg.Wait()
		renderer.render(true)
	})
}

func (renderer *ProgressRenderer) renderLoop(ctx context.Context) {
	defer renderer.wg.Done()
	ticker := time.NewTicker(renderer.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-renderer.done:
			return
		case <-ticker.C:
			renderer.render(false)
		}
	}
}

func (renderer *ProgressRenderer) render(final bool) {
	renderer.mu.Lock()
	defer renderer.mu.Unlock()
	if !renderer.begun || (!final && renderer.finishRequested) {
		return
	}
	now := renderer.now()
	line := formatPendingProgress(renderer.phase, now.Sub(renderer.startTime))
	if renderer.started {
		line = formatProgress(renderer.snapshotLocked(now))
		if renderer.phase != "" {
			line = "status: " + renderer.phase + "; " + line
		}
	}
	if renderer.terminal {
		if final {
			_, _ = fmt.Fprintf(renderer.output, "\r%s\033[K\n", line)
			return
		}
		_, _ = fmt.Fprintf(renderer.output, "\r%s\033[K", line)
		return
	}
	_, _ = fmt.Fprintf(renderer.output, "%s %s\n", now.Format(time.RFC3339), line)
}

func formatPendingProgress(phase string, elapsed time.Duration) string {
	if phase == "" {
		phase = "preparing"
	}
	return fmt.Sprintf("status: %s; total pending; elapsed %s", phase, formatDuration(elapsed))
}

func (renderer *ProgressRenderer) snapshotLocked(now time.Time) ProgressSnapshot {
	elapsed := now.Sub(renderer.startTime)
	rate := ProgressRate{}
	if len(renderer.samples) > 1 {
		last := renderer.samples[len(renderer.samples)-1]
		if now.Sub(last.at) <= progressRateWindow {
			baseline := renderer.samples[0]
			for index := 1; index < len(renderer.samples); index++ {
				if now.Sub(renderer.samples[index].at) <= progressRateWindow {
					baseline = renderer.samples[index-1]
					break
				}
			}
			period := now.Sub(baseline.at)
			if period > 0 {
				rate.Objects = float64(renderer.confirmed.Objects-baseline.confirmed.Objects) / period.Seconds()
				rate.Bytes = float64(renderer.confirmed.Bytes-baseline.confirmed.Bytes) / period.Seconds()
			}
		}
	}
	return ProgressSnapshot{Total: renderer.total, Confirmed: renderer.confirmed, Elapsed: elapsed, Rate: rate, BucketStats: renderer.bucketStats}
}

func formatProgress(snapshot ProgressSnapshot) string {
	remaining := snapshot.Remaining()
	eta := "unknown"
	if snapshot.ETA() > 0 || remaining.Objects == 0 {
		eta = formatDuration(snapshot.ETA())
	}
	line := fmt.Sprintf(
		"deleted %d/%d objects (%s/%s); remaining %d objects (%s); rate %s objects/s (%s/s); elapsed %s; ETA %s",
		snapshot.Confirmed.Objects,
		snapshot.Total.Objects,
		formatBytes(snapshot.Confirmed.Bytes),
		formatBytes(snapshot.Total.Bytes),
		remaining.Objects,
		formatBytes(remaining.Bytes),
		formatRate(snapshot.Rate.Objects),
		formatByteRate(snapshot.Rate.Bytes),
		formatDuration(snapshot.Elapsed),
		eta,
	)
	return line + "; " + formatProgressBar(snapshot) + formatBucketRemainder(snapshot.BucketStats, snapshot.Confirmed)
}

func formatProgressBar(snapshot ProgressSnapshot) string {
	const width = 12
	percent := 100.0
	if snapshot.Total.Objects > 0 {
		percent = 100 * float64(snapshot.Confirmed.Objects) / float64(snapshot.Total.Objects)
	}
	percent = min(max(percent, 0), 100)
	filled := int(percent * width / 100)
	return fmt.Sprintf("progress [%s%s] %d%%", strings.Repeat("#", filled), strings.Repeat(".", width-filled), int(percent))
}

func formatBucketRemainder(stats *BucketStats, confirmed ProgressTotal) string {
	if stats == nil {
		return ""
	}
	if !stats.Complete {
		reason := stats.Incomplete
		if reason == "" {
			reason = "scan did not complete"
		}
		return fmt.Sprintf("; bucket remainder estimate unavailable (incomplete scan: %s)", reason)
	}
	remainingObjects := max(stats.Objects-int64(confirmed.Objects), int64(0))
	remainingBytes := max(stats.Bytes-confirmed.Bytes, int64(0))
	return fmt.Sprintf("; bucket remainder estimate from snapshot %s: %d objects (%s)", stats.SnapshotAt.Format(time.RFC3339), remainingObjects, formatBytes(remainingBytes))
}

func formatRate(rate float64) string {
	if rate == float64(int(rate)) {
		return fmt.Sprintf("%d", int(rate))
	}
	return fmt.Sprintf("%.2f", rate)
}

func formatByteRate(rate float64) string {
	if rate < 1024 {
		return fmt.Sprintf("%.0f B", rate)
	}
	return formatBytes(int64(rate))
}

func formatBytes(bytes int64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	value := float64(bytes)
	unit := 0
	for value >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%d %s", bytes, units[unit])
	}
	return fmt.Sprintf("%.1f %s", value, units[unit])
}

func formatDuration(duration time.Duration) string {
	seconds := int(duration.Round(time.Second).Seconds())
	return fmt.Sprintf("%02d:%02d:%02d", seconds/3600, seconds/60%60, seconds%60)
}

func trimProgressSamples(samples []progressSample, now time.Time) []progressSample {
	start := 0
	for start < len(samples)-1 && now.Sub(samples[start+1].at) > progressRateWindow {
		start++
	}
	return samples[start:]
}

var _ ProgressObserver = (*ProgressRenderer)(nil)
