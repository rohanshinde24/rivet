package storage

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These benchmarks establish baselines. They are not optimization targets and
// they do not by themselves support any performance claim: a number here means
// something only alongside the manifest printed by BenchmarkEnvironment, and
// only when produced by more than one run.

var manifestOnce sync.Once

// logManifest prints the environment exactly once per process, so every
// result in a run can be attributed to a revision and a machine.
func logManifest(b *testing.B) {
	manifestOnce.Do(func() {
		for _, line := range environmentManifest(b.TempDir()) {
			b.Logf("%s", line)
		}
	})
}

func environmentManifest(dir string) []string {
	cfg := Config{Dir: dir}.withDefaults()
	return []string{
		"revision:    " + shellOutput("git", "rev-parse", "--short", "HEAD"),
		"tree state:  " + treeState(),
		"go version:  " + runtime.Version(),
		"platform:    " + runtime.GOOS + "/" + runtime.GOARCH,
		"cpu:         " + cpuModel(),
		"cpus:        " + fmt.Sprintf("%d logical, GOMAXPROCS=%d", runtime.NumCPU(), runtime.GOMAXPROCS(0)),
		"filesystem:  " + filesystemOf(dir),
		"sync policy: one file sync per write, then apply, then acknowledge",
		"limits:      " + fmt.Sprintf("key<=%dB value<=%dB sessions=%d queue=%d segment=%dB",
			cfg.MaxKeyBytes, cfg.MaxValueBytes, cfg.MaxSessions, cfg.QueueCapacity, cfg.SegmentTargetBytes),
		"variability: run with -count to see run-to-run spread; a single run is not a measurement",
	}
}

func shellOutput(name string, args ...string) string {
	out, err := exec.Command(name, args...).Output()
	if err != nil {
		return "unavailable"
	}
	return strings.TrimSpace(string(out))
}

func treeState() string {
	if out := shellOutput("git", "status", "--porcelain"); out == "" {
		return "clean"
	} else if out == "unavailable" {
		return "unavailable"
	}
	return "dirty (results are not attributable to the revision above)"
}

func cpuModel() string {
	switch runtime.GOOS {
	case "darwin":
		return shellOutput("sysctl", "-n", "machdep.cpu.brand_string")
	case "linux":
		out := shellOutput("sh", "-c", "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2")
		return strings.TrimSpace(out)
	}
	return "unavailable"
}

// filesystemOf reports the mounted filesystem backing dir, best effort. The
// sync cost this engine pays is a property of the filesystem, so a result
// without it cannot be compared against another machine.
func filesystemOf(dir string) string {
	df := shellOutput("df", "-P", dir)
	lines := strings.Split(df, "\n")
	if len(lines) < 2 {
		return "unavailable"
	}
	fields := strings.Fields(lines[1])
	if len(fields) == 0 {
		return "unavailable"
	}
	device := fields[0]

	for _, line := range strings.Split(shellOutput("mount"), "\n") {
		if strings.HasPrefix(line, device+" ") {
			return strings.TrimSpace(line)
		}
	}
	return device
}

// BenchmarkEnvironment records the manifest. It performs no engine work.
func BenchmarkEnvironment(b *testing.B) {
	logManifest(b)
	for b.Loop() {
	}
}

// --- latency bookkeeping ------------------------------------------------

// latencies collects per-operation durations so a benchmark can report the
// tail, which an average hides.
type latencies struct {
	mu      sync.Mutex
	samples []time.Duration
}

func (l *latencies) add(d time.Duration) {
	l.mu.Lock()
	l.samples = append(l.samples, d)
	l.mu.Unlock()
}

func (l *latencies) report(b *testing.B) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.samples) == 0 {
		return
	}
	sort.Slice(l.samples, func(i, j int) bool { return l.samples[i] < l.samples[j] })
	micros := func(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1000 }
	b.ReportMetric(micros(percentile(l.samples, 0.50)), "us/p50")
	b.ReportMetric(micros(percentile(l.samples, 0.95)), "us/p95")
	b.ReportMetric(micros(percentile(l.samples, 0.99)), "us/p99")
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func benchEngine(b *testing.B, tweak func(*Config)) *Engine {
	b.Helper()
	logManifest(b)

	cfg := Config{Dir: b.TempDir()}
	if tweak != nil {
		tweak(&cfg)
	}
	e, err := Open(cfg)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { e.Close(context.Background()) })
	return e
}

var valueSizes = []struct {
	name string
	size int
}{
	{"16B", 16},
	{"256B", 256},
	{"4KiB", 4 << 10},
	{"1MiB", 1 << 20},
}

// --- single client throughput -------------------------------------------

func BenchmarkPut(b *testing.B) {
	for _, vs := range valueSizes {
		b.Run(vs.name, func(b *testing.B) {
			e := benchEngine(b, nil)
			value := bytes.Repeat([]byte("v"), vs.size)
			ctx := context.Background()
			var lat latencies

			seq := uint64(0)
			b.SetBytes(int64(vs.size))
			b.ResetTimer()
			for b.Loop() {
				seq++
				start := time.Now()
				if _, err := e.Put(ctx, RequestIdentity{clientID(1), seq},
					[]byte(fmt.Sprintf("k%08d", seq)), value); err != nil {
					b.Fatalf("put: %v", err)
				}
				lat.add(time.Since(start))
			}
			b.StopTimer()
			lat.report(b)
		})
	}
}

func BenchmarkGet(b *testing.B) {
	for _, vs := range valueSizes {
		b.Run(vs.name, func(b *testing.B) {
			e := benchEngine(b, nil)
			value := bytes.Repeat([]byte("v"), vs.size)
			ctx := context.Background()

			const keys = 64
			for i := 0; i < keys; i++ {
				if _, err := e.Put(ctx, RequestIdentity{clientID(1), uint64(i + 1)},
					[]byte(fmt.Sprintf("k%08d", i)), value); err != nil {
					b.Fatalf("seed: %v", err)
				}
			}

			var lat latencies
			i := 0
			b.SetBytes(int64(vs.size))
			b.ResetTimer()
			for b.Loop() {
				start := time.Now()
				if _, found, _, err := e.Get(ctx, []byte(fmt.Sprintf("k%08d", i%keys))); err != nil || !found {
					b.Fatalf("get: found=%v err=%v", found, err)
				}
				lat.add(time.Since(start))
				i++
			}
			b.StopTimer()
			lat.report(b)
		})
	}
}

func BenchmarkDelete(b *testing.B) {
	e := benchEngine(b, nil)
	ctx := context.Background()
	var lat latencies

	seq := uint64(0)
	b.ResetTimer()
	for b.Loop() {
		seq++
		start := time.Now()
		// Deleting an absent key is a durable command too, so this measures
		// the same write path without needing a seeded key per iteration.
		if _, err := e.Delete(ctx, RequestIdentity{clientID(1), seq},
			[]byte(fmt.Sprintf("absent%08d", seq))); err != nil {
			b.Fatalf("delete: %v", err)
		}
		lat.add(time.Since(start))
	}
	b.StopTimer()
	lat.report(b)
}

// --- concurrency ---------------------------------------------------------

// BenchmarkConcurrentPut shows what more client goroutines buy against a
// single-owner event loop that synchronizes once per write.
func BenchmarkConcurrentPut(b *testing.B) {
	for _, clients := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("clients-%d", clients), func(b *testing.B) {
			e := benchEngine(b, func(c *Config) { c.QueueCapacity = HardMaxQueueCapacity })
			value := bytes.Repeat([]byte("v"), 256)
			var lat latencies
			var issued atomic.Int64

			b.ResetTimer()
			var wg sync.WaitGroup
			perClient := max(b.N/clients, 1)
			for c := 1; c <= clients; c++ {
				wg.Add(1)
				go func(c int) {
					defer wg.Done()
					ctx := context.Background()
					for i := 1; i <= perClient; i++ {
						start := time.Now()
						_, err := e.Put(ctx, RequestIdentity{clientID(byte(c)), uint64(i)},
							[]byte(fmt.Sprintf("c%02dk%08d", c, i)), value)
						if err != nil {
							b.Errorf("client %d: %v", c, err)
							return
						}
						lat.add(time.Since(start))
						issued.Add(1)
					}
				}(c)
			}
			wg.Wait()
			b.StopTimer()

			elapsed := b.Elapsed().Seconds()
			if elapsed > 0 {
				b.ReportMetric(float64(issued.Load())/elapsed, "writes/sec")
			}
			lat.report(b)
		})
	}
}

// BenchmarkMixed runs read/write ratios against a shared key space.
func BenchmarkMixed(b *testing.B) {
	mixes := []struct {
		name    string
		readPct int
		clients int
	}{
		{"95r5w", 95, 4},
		{"50r50w", 50, 4},
		{"20r80w", 20, 4},
	}

	for _, mix := range mixes {
		b.Run(mix.name, func(b *testing.B) {
			e := benchEngine(b, func(c *Config) { c.QueueCapacity = HardMaxQueueCapacity })
			value := bytes.Repeat([]byte("v"), 256)
			ctx := context.Background()

			const keys = 256
			for i := 0; i < keys; i++ {
				if _, err := e.Put(ctx, RequestIdentity{clientID(255), uint64(i + 1)},
					[]byte(fmt.Sprintf("k%08d", i)), value); err != nil {
					b.Fatalf("seed: %v", err)
				}
			}

			var lat latencies
			var reads, writes atomic.Int64

			b.ResetTimer()
			var wg sync.WaitGroup
			perClient := max(b.N/mix.clients, 1)
			for c := 1; c <= mix.clients; c++ {
				wg.Add(1)
				go func(c int) {
					defer wg.Done()
					rng := rand.New(rand.NewSource(int64(c) * 7919))
					seq := uint64(0)
					for i := 0; i < perClient; i++ {
						key := []byte(fmt.Sprintf("k%08d", rng.Intn(keys)))
						start := time.Now()
						if rng.Intn(100) < mix.readPct {
							if _, _, _, err := e.Get(ctx, key); err != nil {
								b.Errorf("get: %v", err)
								return
							}
							reads.Add(1)
						} else {
							seq++
							if _, err := e.Put(ctx, RequestIdentity{clientID(byte(c)), seq}, key, value); err != nil {
								b.Errorf("put: %v", err)
								return
							}
							writes.Add(1)
						}
						lat.add(time.Since(start))
					}
				}(c)
			}
			wg.Wait()
			b.StopTimer()

			if elapsed := b.Elapsed().Seconds(); elapsed > 0 {
				b.ReportMetric(float64(reads.Load()+writes.Load())/elapsed, "ops/sec")
			}
			lat.report(b)
		})
	}
}

// --- phase breakdown -----------------------------------------------------

// BenchmarkWritePhases reports where a write's time goes. Tracing is on here
// and off everywhere else, so these numbers carry its overhead and are a
// proportional breakdown rather than a throughput result.
func BenchmarkWritePhases(b *testing.B) {
	var (
		mu     sync.Mutex
		totals = map[string]time.Duration{}
		counts = map[string]int{}
		n      int
	)

	e := benchEngine(b, func(c *Config) {
		c.OnTrace = func(t Trace) {
			if t.Operation != TraceOpPut {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			n++
			for _, s := range t.Spans {
				totals[s.Name] += s.Duration
				counts[s.Name]++
			}
		}
	})

	value := bytes.Repeat([]byte("v"), 256)
	ctx := context.Background()
	seq := uint64(0)

	b.ResetTimer()
	for b.Loop() {
		seq++
		if _, err := e.Put(ctx, RequestIdentity{clientID(1), seq},
			[]byte(fmt.Sprintf("k%08d", seq)), value); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
	b.StopTimer()

	mu.Lock()
	defer mu.Unlock()
	if n == 0 {
		return
	}
	for _, name := range []string{SpanValidate, SpanQueueWait, SpanAdmit, SpanEncode,
		SpanAppend, SpanSync, SpanApply, SpanRespond} {
		if counts[name] == 0 {
			continue
		}
		mean := totals[name] / time.Duration(counts[name])
		b.ReportMetric(float64(mean.Nanoseconds())/1000, "us/"+name)
	}
}

// --- recovery ------------------------------------------------------------

// BenchmarkRecovery measures reopen time against the number of records that
// have to be replayed. No snapshot is taken, so every record is replayed.
func BenchmarkRecovery(b *testing.B) {
	for _, records := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("records-%d", records), func(b *testing.B) {
			logManifest(b)
			dir := b.TempDir()

			cfg := Config{Dir: dir}
			e, err := Open(cfg)
			if err != nil {
				b.Fatal(err)
			}
			value := bytes.Repeat([]byte("v"), 256)
			ctx := context.Background()
			for i := 1; i <= records; i++ {
				if _, err := e.Put(ctx, RequestIdentity{clientID(1), uint64(i)},
					[]byte(fmt.Sprintf("k%08d", i)), value); err != nil {
					b.Fatalf("seed: %v", err)
				}
			}
			if err := e.Close(ctx); err != nil {
				b.Fatal(err)
			}
			bytesToReplay := walBytes(b, dir)

			b.ResetTimer()
			for b.Loop() {
				reopened, err := Open(cfg)
				if err != nil {
					b.Fatalf("reopen: %v", err)
				}
				if got := reopened.Stats().ReplayedCommands; got != int64(records) {
					b.Fatalf("replayed %d records, want %d", got, records)
				}
				if err := reopened.Close(ctx); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()

			// ResetTimer clears reported metrics, so both are reported here.
			b.ReportMetric(float64(records), "records")
			b.ReportMetric(float64(bytesToReplay), "wal-bytes")
		})
	}
}

func walBytes(b *testing.B, dir string) int64 {
	b.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		b.Fatal(err)
	}
	var total int64
	for _, entry := range entries {
		if !hasSegmentPrefix(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			b.Fatal(err)
		}
		total += info.Size()
	}
	return total
}

// --- snapshot and compaction ---------------------------------------------

// BenchmarkSnapshot measures publication against the number of live keys, and
// separately reports the barrier copy, which is the part that runs on the
// event loop and therefore blocks foreground work.
func BenchmarkSnapshot(b *testing.B) {
	for _, keys := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("keys-%d", keys), func(b *testing.B) {
			var (
				mu      sync.Mutex
				copySum time.Duration
				copyN   int
				size    int64
			)

			e := benchEngine(b, func(c *Config) {
				c.SegmentTargetBytes = 1 << 20
				c.OnTrace = func(t Trace) {
					if t.Operation != TraceOpSnapshot {
						return
					}
					mu.Lock()
					defer mu.Unlock()
					for _, s := range t.Spans {
						switch s.Name {
						case SpanSnapshotCopy:
							copySum += s.Duration
							copyN++
						case SpanSnapshotWrite:
							size = s.Bytes
						}
					}
				}
			})

			value := bytes.Repeat([]byte("v"), 256)
			ctx := context.Background()
			seq := uint64(0)
			for i := 0; i < keys; i++ {
				seq++
				if _, err := e.Put(ctx, RequestIdentity{clientID(1), seq},
					[]byte(fmt.Sprintf("k%08d", i)), value); err != nil {
					b.Fatalf("seed: %v", err)
				}
			}

			b.ResetTimer()
			for b.Loop() {
				// Each snapshot needs a new applied sequence, otherwise the
				// engine correctly returns the existing one unchanged.
				b.StopTimer()
				seq++
				if _, err := e.Put(ctx, RequestIdentity{clientID(1), seq},
					[]byte(fmt.Sprintf("k%08d", seq)), value); err != nil {
					b.Fatalf("advance: %v", err)
				}
				b.StartTimer()

				if _, _, err := e.CreateSnapshot(ctx); err != nil {
					b.Fatalf("snapshot: %v", err)
				}
			}
			b.StopTimer()

			mu.Lock()
			defer mu.Unlock()
			if copyN > 0 {
				mean := copySum / time.Duration(copyN)
				b.ReportMetric(float64(mean.Nanoseconds())/1000, "us/barrier-copy")
			}
			b.ReportMetric(float64(size), "snapshot-bytes")
		})
	}
}

// BenchmarkCompaction measures the snapshot that reclaims a segment backlog,
// and reports how many bytes it freed.
//
// The backlog is built once and copied per iteration. Building it with the
// engine would cost one synchronized write per record and would scale with
// whatever iteration count the runner chooses, which would swamp the thing
// being measured even with the timer stopped.
func BenchmarkCompaction(b *testing.B) {
	const records = 2_000
	logManifest(b)

	template := b.TempDir()
	value := bytes.Repeat([]byte("v"), 256)
	ctx := context.Background()

	seed, err := Open(Config{Dir: template, SegmentTargetBytes: 4096})
	if err != nil {
		b.Fatal(err)
	}
	for i := 1; i <= records; i++ {
		if _, err := seed.Put(ctx, RequestIdentity{clientID(1), uint64(i)},
			[]byte(fmt.Sprintf("k%08d", i)), value); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}
	if err := seed.Close(ctx); err != nil {
		b.Fatal(err)
	}
	segmentsBefore := countSegmentFiles(b, template)

	var reclaimed int64
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		dir := b.TempDir()
		copyDirFiles(b, template, dir)
		e, err := Open(Config{Dir: dir, SegmentTargetBytes: 4096})
		if err != nil {
			b.Fatal(err)
		}
		before := walBytes(b, dir)
		b.StartTimer()

		if _, _, err := e.CreateSnapshot(ctx); err != nil {
			b.Fatalf("snapshot: %v", err)
		}

		b.StopTimer()
		reclaimed = before - walBytes(b, dir)
		if err := e.Close(ctx); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.StopTimer()

	b.ReportMetric(float64(reclaimed), "bytes-reclaimed")
	b.ReportMetric(float64(segmentsBefore), "segments-before")
}

func countSegmentFiles(b *testing.B, dir string) int {
	b.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		b.Fatal(err)
	}
	n := 0
	for _, entry := range entries {
		if hasSegmentPrefix(entry.Name()) {
			n++
		}
	}
	return n
}

// copyDirFiles copies a prepared data directory, leaving the lock file behind
// so the copy is opened cleanly.
func copyDirFiles(b *testing.B, src, dst string) {
	b.Helper()
	entries, err := os.ReadDir(src)
	if err != nil {
		b.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == lockFileName {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, entry.Name()))
		if err != nil {
			b.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, entry.Name()), data, 0o644); err != nil {
			b.Fatal(err)
		}
	}
}
