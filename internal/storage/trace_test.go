package storage

import (
	"context"
	"strings"
	"sync"
	"testing"
)

// traceCollector gathers traces from the goroutines that issue operations.
type traceCollector struct {
	mu     sync.Mutex
	traces []Trace
}

func (c *traceCollector) record(t Trace) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.traces = append(c.traces, t)
}

func (c *traceCollector) byOperation(op string) []Trace {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []Trace
	for _, t := range c.traces {
		if t.Operation == op {
			out = append(out, t)
		}
	}
	return out
}

func spanNames(t Trace) []string {
	names := make([]string, 0, len(t.Spans))
	for _, s := range t.Spans {
		names = append(names, s.Name)
	}
	return names
}

func expectSpans(t *testing.T, tr Trace, want ...string) {
	t.Helper()
	got := spanNames(tr)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("spans %v, want %v", got, want)
	}
	if tr.Total <= 0 {
		t.Fatalf("total duration is %v", tr.Total)
	}
	var sum int64
	for _, s := range tr.Spans {
		if s.Duration < 0 {
			t.Fatalf("span %s has negative duration %v", s.Name, s.Duration)
		}
		sum += int64(s.Duration)
	}
	if sum > int64(tr.Total) {
		t.Fatalf("spans sum to %v, longer than the total %v", sum, tr.Total)
	}
}

func openTraced(t *testing.T, dir string, c *traceCollector) *Engine {
	t.Helper()
	return open(t, dir, func(cfg *Config) { cfg.OnTrace = c.record })
}

func TestTraceWritePath(t *testing.T) {
	var c traceCollector
	e := openTraced(t, t.TempDir(), &c)
	defer closeEngine(t, e)

	put(t, e, 1, 1, "alpha", "one")

	traces := c.byOperation(TraceOpPut)
	if len(traces) != 1 {
		t.Fatalf("%d put traces, want 1", len(traces))
	}
	tr := traces[0]
	expectSpans(t, tr, SpanValidate, SpanQueueWait, SpanAdmit, SpanEncode,
		SpanAppend, SpanSync, SpanApply, SpanRespond)

	if tr.Sequence != 1 {
		t.Fatalf("sequence %d, want 1", tr.Sequence)
	}
	if tr.Outcome != CodeUnknown {
		t.Fatalf("outcome %v, want success", tr.Outcome)
	}
	if tr.Context == nil {
		t.Fatal("trace lost the caller context used for correlation")
	}
	for _, s := range tr.Spans {
		if s.Name == SpanAppend && s.Bytes <= 0 {
			t.Fatal("append span did not record the frame size")
		}
	}
}

func TestTraceReadPath(t *testing.T) {
	var c traceCollector
	e := openTraced(t, t.TempDir(), &c)
	defer closeEngine(t, e)

	put(t, e, 1, 1, "alpha", "one")
	expectGet(t, e, "alpha", "one")

	traces := c.byOperation(TraceOpGet)
	if len(traces) != 1 {
		t.Fatalf("%d get traces, want 1", len(traces))
	}
	expectSpans(t, traces[0], SpanValidate, SpanQueueWait, SpanRead, SpanRespond)
}

// A duplicate retry stops at the session check, so it must show no encode,
// append, or sync span: nothing was written.
func TestTraceDuplicateStopsAtAdmit(t *testing.T) {
	var c traceCollector
	e := openTraced(t, t.TempDir(), &c)
	defer closeEngine(t, e)

	put(t, e, 1, 1, "alpha", "one")
	put(t, e, 1, 1, "alpha", "one")

	traces := c.byOperation(TraceOpPut)
	if len(traces) != 2 {
		t.Fatalf("%d put traces, want 2", len(traces))
	}
	expectSpans(t, traces[1], SpanValidate, SpanQueueWait, SpanAdmit, SpanRespond)
}

func TestTraceSnapshotPath(t *testing.T) {
	var c traceCollector
	e := openTraced(t, t.TempDir(), &c)
	defer closeEngine(t, e)

	put(t, e, 1, 1, "alpha", "one")
	if _, _, err := e.CreateSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}

	traces := c.byOperation(TraceOpSnapshot)
	if len(traces) != 1 {
		t.Fatalf("%d snapshot traces, want 1", len(traces))
	}
	expectSpans(t, traces[0], SpanQueueWait, SpanSnapshotRotate, SpanSnapshotCopy,
		SpanSnapshotWrite, SpanSnapshotFileSync, SpanSnapshotValidate,
		SpanSnapshotRename, SpanSnapshotDirSync, SpanCompaction, SpanRespond)

	for _, s := range traces[0].Spans {
		if s.Name == SpanSnapshotWrite && s.Bytes <= 0 {
			t.Fatal("snapshot write span did not record the file size")
		}
	}
}

// An operation rejected before admission still reports an outcome, but carries
// no spans: the event loop may never have seen it.
func TestTraceRejectedOperation(t *testing.T) {
	var c traceCollector
	e := openTraced(t, t.TempDir(), &c)
	defer closeEngine(t, e)

	if _, err := e.Put(context.Background(), RequestIdentity{clientID(1), 1}, nil, []byte("v")); err == nil {
		t.Fatal("expected a validation error")
	}
	traces := c.byOperation(TraceOpPut)
	if len(traces) != 1 {
		t.Fatalf("%d traces, want 1", len(traces))
	}
	if traces[0].Outcome != CodeInvalidArgument {
		t.Fatalf("outcome %v, want INVALID_ARGUMENT", traces[0].Outcome)
	}
	if len(traces[0].Spans) != 0 {
		t.Fatalf("rejected operation carried spans %v", spanNames(traces[0]))
	}
}

// A failed operation reports the code that stopped it, and its spans end where
// the failure happened.
func TestTraceCarriesFailureOutcome(t *testing.T) {
	var c traceCollector
	e := openTraced(t, t.TempDir(), &c)
	defer closeEngine(t, e)

	if _, err := e.Put(context.Background(), RequestIdentity{clientID(1), 5},
		[]byte("k"), []byte("v")); CodeOf(err) != CodeSequenceGap {
		t.Fatalf("want SEQUENCE_GAP, got %v", err)
	}
	traces := c.byOperation(TraceOpPut)
	if traces[0].Outcome != CodeSequenceGap {
		t.Fatalf("outcome %v, want SEQUENCE_GAP", traces[0].Outcome)
	}
	expectSpans(t, traces[0], SpanValidate, SpanQueueWait, SpanAdmit, SpanRespond)
}

// Traces must never carry payload bytes. The struct has no field for them; this
// guards against a span name or operation name being built from user input.
func TestTraceCarriesNoPayload(t *testing.T) {
	var c traceCollector
	e := openTraced(t, t.TempDir(), &c)
	defer closeEngine(t, e)

	const secretKey, secretValue = "tracing-key-secret", "tracing-value-secret"
	put(t, e, 1, 1, secretKey, secretValue)
	expectGet(t, e, secretKey, secretValue)

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, tr := range c.traces {
		fields := append(spanNames(tr), tr.Operation)
		for _, f := range fields {
			if strings.Contains(f, secretKey) || strings.Contains(f, secretValue) {
				t.Fatalf("trace field %q leaked payload bytes", f)
			}
		}
	}
}

// Tracing is off unless a callback is set, and the engine then does no timing
// work at all.
func TestTracingDisabledByDefault(t *testing.T) {
	e := open(t, t.TempDir(), nil)
	defer closeEngine(t, e)

	if e.tracing {
		t.Fatal("tracing is enabled without a callback")
	}
	put(t, e, 1, 1, "k", "v")
	expectGet(t, e, "k", "v")
	if _, _, err := e.CreateSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// Traces are emitted from the calling goroutine, so a concurrent workload must
// stay clean under the race detector.
func TestTraceUnderConcurrency(t *testing.T) {
	var c traceCollector
	e := openTraced(t, t.TempDir(), &c)

	var wg sync.WaitGroup
	for client := 1; client <= 4; client++ {
		wg.Add(1)
		go func(client int) {
			defer wg.Done()
			for i := 1; i <= 15; i++ {
				put(t, e, byte(client), uint64(i), "k", "v")
				expectGet(t, e, "k", "v")
			}
		}(client)
	}
	wg.Wait()
	closeEngine(t, e)

	if got := len(c.byOperation(TraceOpPut)); got != 60 {
		t.Fatalf("%d put traces, want 60", got)
	}
	if got := len(c.byOperation(TraceOpGet)); got != 60 {
		t.Fatalf("%d get traces, want 60", got)
	}
}
