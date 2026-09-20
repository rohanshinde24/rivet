package storage

import (
	"context"
	"time"
)

// Span is one phase of a single operation. Phases are sequential and do not
// nest: each one ends where the next begins, so their durations sum to the
// operation total minus the time spent outside any named phase.
type Span struct {
	Name     string
	Duration time.Duration

	// Bytes carries the size a phase moved, where that is meaningful: the
	// frame an append wrote, or the snapshot a write produced. It is zero
	// elsewhere.
	Bytes int64
}

// Trace is the completed phase breakdown of one operation.
//
// It carries no identifier of its own. Correlation with a caller's own tracing
// is done through Context, which is the context the caller passed in; an
// adapter reads its span from there. Nothing in a Trace is ever written to
// disk, and no trace value reaches the durable command schema.
type Trace struct {
	Context   context.Context
	Operation string

	// Sequence is the global sequence the operation reached, or zero when it
	// never produced a command.
	Sequence uint64

	// Outcome is CodeUnknown for a successful operation.
	Outcome Code

	Total time.Duration
	Spans []Span
}

// Operation names used in Trace.Operation.
const (
	TraceOpGet      = "get"
	TraceOpPut      = "put"
	TraceOpDelete   = "delete"
	TraceOpSnapshot = "snapshot"
)

// Span names. The write path is validate, queue_wait, admit, encode, append,
// sync, apply, respond. A read is validate, queue_wait, read, respond. A
// snapshot is the barrier, the publication stages, and compaction.
const (
	SpanValidate  = "validate"
	SpanQueueWait = "queue_wait"
	SpanAdmit     = "admit"
	SpanEncode    = "encode"
	SpanAppend    = "append"
	SpanSync      = "sync"
	SpanApply     = "apply"
	SpanRead      = "read"
	SpanRespond   = "respond"

	SpanSnapshotRotate   = "snapshot_rotate"
	SpanSnapshotCopy     = "snapshot_copy"
	SpanSnapshotWrite    = "snapshot_write"
	SpanSnapshotFileSync = "snapshot_file_sync"
	SpanSnapshotValidate = "snapshot_validate"
	SpanSnapshotRename   = "snapshot_rename"
	SpanSnapshotDirSync  = "snapshot_dir_sync"
	SpanCompaction       = "compaction"
)

// traceRecorder accumulates the spans of one operation.
//
// Every method tolerates a nil receiver, so the engine calls them
// unconditionally and pays nothing when tracing is off.
//
// Ownership passes with the request: the caller creates the recorder, the
// event loop fills it, and the caller reads it only after the reply arrives.
// The snapshot worker holds it between the barrier and its completion
// message. Each handoff is through a channel, so no span is ever written and
// read concurrently.
type traceRecorder struct {
	ctx   context.Context
	op    string
	start time.Time
	last  time.Time
	spans []Span
}

func newTraceRecorder(ctx context.Context, op string) *traceRecorder {
	now := time.Now()
	return &traceRecorder{
		ctx:   ctx,
		op:    op,
		start: now,
		last:  now,
		spans: make([]Span, 0, 8),
	}
}

// mark closes the phase that began at the previous mark.
func (t *traceRecorder) mark(name string) {
	t.markBytes(name, 0)
}

func (t *traceRecorder) markBytes(name string, bytes int64) {
	if t == nil {
		return
	}
	now := time.Now()
	t.spans = append(t.spans, Span{Name: name, Duration: now.Sub(t.last), Bytes: bytes})
	t.last = now
}

// finish returns the completed trace. The caller must already hold the
// happens-before edge that makes the spans visible.
func (t *traceRecorder) finish(sequence uint64, outcome Code) Trace {
	return Trace{
		Context:   t.ctx,
		Operation: t.op,
		Sequence:  sequence,
		Outcome:   outcome,
		Total:     time.Since(t.start),
		Spans:     t.spans,
	}
}

// abandon returns a trace without spans, for an operation the caller stopped
// waiting on or that was rejected before admission. The event loop may still
// be writing spans, so they are deliberately not read.
func (t *traceRecorder) abandon(outcome Code) Trace {
	return Trace{
		Context:   t.ctx,
		Operation: t.op,
		Outcome:   outcome,
		Total:     time.Since(t.start),
	}
}

func (e *Engine) newTrace(ctx context.Context, op string) *traceRecorder {
	if !e.tracing {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return newTraceRecorder(ctx, op)
}

func (e *Engine) emitTrace(t *traceRecorder, sequence uint64, outcome Code) {
	if t == nil || e.cfg.OnTrace == nil {
		return
	}
	e.cfg.OnTrace(t.finish(sequence, outcome))
}

func (e *Engine) emitAbandonedTrace(t *traceRecorder, outcome Code) {
	if t == nil || e.cfg.OnTrace == nil {
		return
	}
	e.cfg.OnTrace(t.abandon(outcome))
}
