package storage

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Lifecycle states.
const (
	stateRecovering uint32 = iota
	stateServing
	stateClosing
	stateClosed
	stateFaulted
)

func stateName(s uint32) string {
	switch s {
	case stateRecovering:
		return "Recovering"
	case stateServing:
		return "Serving"
	case stateClosing:
		return "Closing"
	case stateClosed:
		return "Closed"
	case stateFaulted:
		return "Faulted"
	default:
		return "Unknown"
	}
}

// Stats is a bounded read-only view of operational state. It remains available
// after the engine has faulted or closed.
type Stats struct {
	State                string
	AppliedSequence      uint64
	Keys                 int64
	Sessions             int64
	SessionCapacity      int
	QueueDepth           int
	QueueCapacity        int
	LastSnapshotSequence uint64
	SnapshotActive       bool
	ReplayedCommands     int64
	TruncatedBytes       int64
	Gets                 uint64
	Puts                 uint64
	Deletes              uint64
	DuplicateHits        uint64
	EnqueueRejections    uint64
	StorageFailures      uint64
}

type reqKind uint8

const (
	reqGet reqKind = iota
	reqPut
	reqDelete
	reqSnapshot
)

type request struct {
	kind  reqKind
	ctx   context.Context
	trace *traceRecorder
	key   []byte
	value []byte
	id    RequestIdentity
	reply chan response
}

type response struct {
	value        []byte
	found        bool
	result       writeResult
	snapshotPath string
	err          error
}

type snapshotOutcome struct {
	sequence  uint64
	path      string
	ambiguous bool
	err       error
}

// phaseHook lets tests inject a failure at a named point on the write path.
// Like ioHooks it is unexported and reachable only from this package's tests.
type phaseHook func(phase string) error

// Write-path phases at which tests may inject a failure.
const (
	phaseBeforeAppend = "before_append"
	phaseAfterWrite   = "after_write"
	phaseBeforeSync   = "before_sync"
	phaseAfterSync    = "after_sync"
	phaseBeforeApply  = "before_apply"
	phaseAfterApply   = "after_apply"
	phaseBeforeReply  = "before_reply"
)

// Engine is a single-node durable key-value state machine over one data
// directory.
//
// All KV state, session state, the global sequence, the WAL writer, and
// lifecycle transitions are owned by one event loop goroutine.
// Exported methods only validate arguments, copy caller bytes, and hand a
// request to that loop.
type Engine struct {
	cfg     Config
	hooks   *ioHooks
	phase   phaseHook
	lock    *dirLock
	tracing bool

	reqCh        chan *request
	snapshotDone chan snapshotOutcome
	loopDone     chan struct{}

	// admitMu serializes admission against Close so that no request is ever
	// sent on a closed channel.
	admitMu  sync.RWMutex
	state    atomic.Uint32
	closeErr error
	closeOne sync.Once

	// Loop-owned. Nothing outside run() may touch these.
	sm             *stateMachine
	wal            *segmentWriter
	scratch        []byte
	snapshotActive bool
	snapshotReply  chan response
	snapshotTrace  *traceRecorder
	snapshotSeq    uint64

	// Mirrored for Stats, which must work when the loop is gone.
	appliedSeq   atomic.Uint64
	keyCount     atomic.Int64
	sessionCount atomic.Int64
	lastSnapSeq  atomic.Uint64
	snapActive   atomic.Bool
	replayed     atomic.Int64
	truncated    atomic.Int64
	gets         atomic.Uint64
	puts         atomic.Uint64
	deletes      atomic.Uint64
	dupHits      atomic.Uint64
	rejections   atomic.Uint64
	storageFails atomic.Uint64
}

// Open recovers the data directory and returns a serving engine.
//
// It fails rather than opening whenever durable state cannot be proven to be a
// valid prefix of acknowledged history, and it fails if another engine already
// owns the directory.
func Open(cfg Config) (*Engine, error) {
	return openEngine(cfg, defaultHooks(), nil)
}

func openEngine(cfg Config, hooks *ioHooks, phase phaseHook) (*Engine, error) {
	const op = "Open"

	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, wrapError(CodeStorage, op, "create data directory", err)
	}
	cfg.emit(Event{Name: EventOpenStart, Detail: cfg.Dir})

	lock, err := acquireDirLock(cfg.Dir)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		cfg:          cfg,
		hooks:        hooks,
		phase:        phase,
		lock:         lock,
		tracing:      cfg.OnTrace != nil,
		reqCh:        make(chan *request, cfg.QueueCapacity),
		snapshotDone: make(chan snapshotOutcome, 1),
		loopDone:     make(chan struct{}),
	}
	e.state.Store(stateRecovering)

	rec, err := runRecovery(cfg.Dir, cfg, hooks)
	if err != nil {
		cfg.emit(Event{Name: EventRecoveryFailed, Code: CodeOf(err), Err: err})
		lock.release()
		return nil, err
	}

	e.sm = rec.state
	e.snapshotSeq = rec.snapshotSequence

	// The active segment must be ready for appends, and durably
	// registered, before the engine serves anything.
	if rec.activeName != "" {
		e.wal, err = openSegmentForAppend(cfg.Dir, rec.activeName, rec.activeStart,
			rec.lastSequence, rec.appendOffset, hooks)
	} else {
		e.wal, err = createSegment(cfg.Dir, rec.lastSequence+1, hooks)
		if err == nil {
			cfg.emit(Event{Name: EventSegmentCreated, Segment: filepath.Base(e.wal.path),
				Sequence: rec.lastSequence + 1})
		}
	}
	if err != nil {
		lock.release()
		return nil, err
	}

	e.replayed.Store(rec.replayedCommands)
	e.truncated.Store(rec.truncatedBytes)
	e.lastSnapSeq.Store(rec.snapshotSequence)
	e.mirrorState()

	cfg.emit(Event{Name: EventRecoveryComplete, Sequence: e.sm.applied,
		Count: rec.replayedCommands, Bytes: rec.truncatedBytes})

	e.state.Store(stateServing)
	go e.run()
	return e, nil
}

func (e *Engine) mirrorState() {
	e.appliedSeq.Store(e.sm.applied)
	e.keyCount.Store(int64(len(e.sm.kv)))
	e.sessionCount.Store(int64(len(e.sm.sessions)))
}

// Stats returns a bounded operational view. It never blocks on the event loop,
// so it is still meaningful after a fault.
func (e *Engine) Stats() Stats {
	return Stats{
		State:                stateName(e.state.Load()),
		AppliedSequence:      e.appliedSeq.Load(),
		Keys:                 e.keyCount.Load(),
		Sessions:             e.sessionCount.Load(),
		SessionCapacity:      e.cfg.MaxSessions,
		QueueDepth:           len(e.reqCh),
		QueueCapacity:        e.cfg.QueueCapacity,
		LastSnapshotSequence: e.lastSnapSeq.Load(),
		SnapshotActive:       e.snapActive.Load(),
		ReplayedCommands:     e.replayed.Load(),
		TruncatedBytes:       e.truncated.Load(),
		Gets:                 e.gets.Load(),
		Puts:                 e.puts.Load(),
		Deletes:              e.deletes.Load(),
		DuplicateHits:        e.dupHits.Load(),
		EnqueueRejections:    e.rejections.Load(),
		StorageFailures:      e.storageFails.Load(),
	}
}

// Get returns a copy of the value for key.
//
// The read is ordered at the event loop between commands, which is its
// linearization point. It appends nothing to the WAL.
func (e *Engine) Get(ctx context.Context, key []byte) (value []byte, found bool, appliedSeq uint64, err error) {
	tr := e.newTrace(ctx, TraceOpGet)
	if err := e.validateKey("Get", key); err != nil {
		e.emitAbandonedTrace(tr, CodeOf(err))
		return nil, false, 0, err
	}
	tr.mark(SpanValidate)

	resp, err := e.submit(ctx, &request{kind: reqGet, trace: tr, key: append([]byte(nil), key...)})
	if err != nil {
		e.emitAbandonedTrace(tr, CodeOf(err))
		return nil, false, 0, err
	}
	e.emitTrace(tr, resp.result.AppliedSequence, CodeOf(resp.err))
	return resp.value, resp.found, resp.result.AppliedSequence, resp.err
}

// Put stores value under key.
//
// Success means the command is in a synchronized WAL and has been applied. Any
// error returned after the append began leaves the outcome unknown: the caller
// keeps the same request identity and retries.
func (e *Engine) Put(ctx context.Context, id RequestIdentity, key, value []byte) (writeResult, error) {
	tr := e.newTrace(ctx, TraceOpPut)
	fail := func(err error) (writeResult, error) {
		e.emitAbandonedTrace(tr, CodeOf(err))
		return writeResult{}, err
	}

	if err := e.validateKey("Put", key); err != nil {
		return fail(err)
	}
	if len(value) > e.cfg.MaxValueBytes {
		return fail(errorf(CodeInvalidArgument, "Put",
			"value is %d bytes, above the limit %d", len(value), e.cfg.MaxValueBytes))
	}
	if err := e.validateIdentity("Put", id); err != nil {
		return fail(err)
	}
	tr.mark(SpanValidate)

	resp, err := e.submit(ctx, &request{
		kind:  reqPut,
		id:    id,
		trace: tr,
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
	})
	if err != nil {
		return fail(err)
	}
	e.emitTrace(tr, resp.result.AppliedSequence, CodeOf(resp.err))
	return resp.result, resp.err
}

// Delete removes key. Deleting an absent key succeeds with Existed false; both
// outcomes are durable commands so a retry returns the original result.
func (e *Engine) Delete(ctx context.Context, id RequestIdentity, key []byte) (writeResult, error) {
	tr := e.newTrace(ctx, TraceOpDelete)
	fail := func(err error) (writeResult, error) {
		e.emitAbandonedTrace(tr, CodeOf(err))
		return writeResult{}, err
	}

	if err := e.validateKey("Delete", key); err != nil {
		return fail(err)
	}
	if err := e.validateIdentity("Delete", id); err != nil {
		return fail(err)
	}
	tr.mark(SpanValidate)

	resp, err := e.submit(ctx, &request{kind: reqDelete, id: id, trace: tr,
		key: append([]byte(nil), key...)})
	if err != nil {
		return fail(err)
	}
	e.emitTrace(tr, resp.result.AppliedSequence, CodeOf(resp.err))
	return resp.result, resp.err
}

// CreateSnapshot publishes a snapshot at the current applied sequence and then
// compacts the segments it covers. It returns when publication and compaction
// have finished.
func (e *Engine) CreateSnapshot(ctx context.Context) (sequence uint64, path string, err error) {
	tr := e.newTrace(ctx, TraceOpSnapshot)
	resp, err := e.submit(ctx, &request{kind: reqSnapshot, trace: tr})
	if err != nil {
		e.emitAbandonedTrace(tr, CodeOf(err))
		return 0, "", err
	}
	e.emitTrace(tr, resp.result.AppliedSequence, CodeOf(resp.err))
	if resp.err != nil {
		return 0, "", resp.err
	}
	return resp.result.AppliedSequence, resp.snapshotPath, nil
}

func (e *Engine) validateKey(op string, key []byte) error {
	if len(key) == 0 {
		return newError(CodeInvalidArgument, op, "key is empty")
	}
	if len(key) > e.cfg.MaxKeyBytes {
		return errorf(CodeInvalidArgument, op, "key is %d bytes, above the limit %d",
			len(key), e.cfg.MaxKeyBytes)
	}
	return nil
}

func (e *Engine) validateIdentity(op string, id RequestIdentity) error {
	if id.ClientID.IsZero() {
		return newError(CodeInvalidArgument, op, "client ID is zero")
	}
	if id.RequestSequence == 0 {
		return newError(CodeInvalidArgument, op, "request sequence must start at 1")
	}
	return nil
}

// submit admits a request to the bounded queue and waits for the loop.
//
// Admission is where overload and cancellation are reported as definitely not
// executed. Once the loop begins a write, caller cancellation stops the
// waiting, not the storage decision.
func (e *Engine) submit(ctx context.Context, r *request) (response, error) {
	const op = "submit"

	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return response{}, wrapError(CodeCanceled, op, "caller context ended before admission", err)
	}
	r.ctx = ctx
	r.reply = make(chan response, 1)

	e.admitMu.RLock()
	switch s := e.state.Load(); s {
	case stateFaulted:
		e.admitMu.RUnlock()
		return response{}, newError(CodeFaulted, op,
			"engine faulted on an earlier storage error and requires restart")
	case stateClosing, stateClosed:
		e.admitMu.RUnlock()
		return response{}, newError(CodeClosed, op, "engine is closed")
	}

	select {
	case e.reqCh <- r:
		e.admitMu.RUnlock()
	default:
		e.admitMu.RUnlock()
		e.rejections.Add(1)
		return response{}, errorf(CodeResourceExhausted, op,
			"request queue is full at capacity %d", e.cfg.QueueCapacity)
	}

	select {
	case resp := <-r.reply:
		return resp, nil
	case <-ctx.Done():
		// The request may already be executing. Reporting an unknown outcome
		// is the only honest answer.
		return response{}, wrapError(CodeCanceled, op,
			"caller stopped waiting; outcome unknown if the write had begun", ctx.Err())
	}
}

func (e *Engine) run() {
	defer close(e.loopDone)

	for {
		select {
		case r, ok := <-e.reqCh:
			if !ok {
				e.shutdown()
				return
			}
			e.handle(r)
		case out := <-e.snapshotDone:
			e.completeSnapshot(out)
		}
	}
}

// sendReply closes the respond span and hands the result to the caller. The
// channel send is the happens-before edge that publishes the spans, so the
// event loop must not touch the recorder after this returns.
func sendReply(r *request, resp response) {
	r.trace.mark(SpanRespond)
	r.reply <- resp
}

func (e *Engine) handle(r *request) {
	r.trace.mark(SpanQueueWait)

	// Requests queued before Close are resolved as definitely not executed.
	switch e.state.Load() {
	case stateFaulted:
		sendReply(r, response{err: newError(CodeFaulted, "handle", "engine is faulted")})
		return
	case stateClosing, stateClosed:
		sendReply(r, response{err: newError(CodeClosed, "handle", "engine closed before this request ran")})
		return
	}
	if err := r.ctx.Err(); err != nil {
		sendReply(r, response{err: wrapError(CodeCanceled, "handle",
			"caller context ended before processing", err)})
		return
	}

	switch r.kind {
	case reqGet:
		value, found := e.sm.get(r.key)
		r.trace.mark(SpanRead)
		e.gets.Add(1)
		sendReply(r, response{value: value, found: found,
			result: writeResult{AppliedSequence: e.sm.applied}})
	case reqPut:
		e.handleWrite(r, OpPut)
	case reqDelete:
		e.handleWrite(r, OpDelete)
	case reqSnapshot:
		e.handleSnapshot(r)
	}
}

func (e *Engine) handleWrite(r *request, kind OpKind) {
	cmd := &command{
		Kind:     kind,
		ClientID: r.id.ClientID,
		Sequence: r.id.RequestSequence,
		Key:      r.key,
	}
	if kind == OpPut {
		cmd.Value = r.value
	}

	decision, stored, err := e.sm.admit(cmd, e.cfg.MaxSessions)
	r.trace.mark(SpanAdmit)
	if err != nil {
		e.cfg.emit(Event{Name: EventSessionRejected, Code: CodeOf(err), Detail: err.Error()})
		sendReply(r, response{err: err})
		return
	}
	if decision == admitDuplicate {
		e.dupHits.Add(1)
		e.cfg.emit(Event{Name: EventDedupHit, Sequence: stored.AppliedSequence})
		sendReply(r, response{result: stored})
		return
	}

	// A new key beyond the configured maximum is rejected before anything is
	// appended, so the caller learns it definitely did not execute.
	if kind == OpPut {
		if _, exists := e.sm.kv[string(cmd.Key)]; !exists && len(e.sm.kv) >= e.cfg.MaxKeys {
			sendReply(r, response{err: errorf(CodeResourceExhausted, "handleWrite",
				"key count is at the configured maximum %d", e.cfg.MaxKeys)})
			return
		}
	}

	seq := e.sm.applied + 1
	payload := cmd.encode(nil)
	r.trace.mark(SpanEncode)

	if err := e.runPhase(phaseBeforeAppend); err != nil {
		sendReply(r, response{err: wrapError(CodeStorage, "append", "failpoint before append", err)})
		return
	}

	frame, err := e.wal.appendRecord(seq, payload, e.scratch)
	r.trace.markBytes(SpanAppend, int64(len(frame)))
	e.scratch = frame[:0]
	if err != nil {
		// Part of a frame may be on disk. Only recovery, which can see whether
		// it is the physical tail, may decide what it means.
		e.fault("append", err)
		sendReply(r, response{err: wrapError(CodeStorage, "append",
			"append failed; outcome unknown", err)})
		return
	}
	if err := e.runPhase(phaseAfterWrite); err != nil {
		e.fault("append", err)
		sendReply(r, response{err: wrapError(CodeStorage, "append", "failpoint after write", err)})
		return
	}

	if err := e.runPhase(phaseBeforeSync); err != nil {
		e.fault("sync", err)
		sendReply(r, response{err: wrapError(CodeStorage, "sync", "failpoint before sync", err)})
		return
	}
	err = e.wal.sync()
	r.trace.mark(SpanSync)
	if err != nil {
		e.fault("sync", err)
		sendReply(r, response{err: wrapError(CodeStorage, "sync",
			"sync failed; durability unknown", err)})
		return
	}
	if err := e.runPhase(phaseAfterSync); err != nil {
		e.fault("sync", err)
		sendReply(r, response{err: wrapError(CodeStorage, "sync", "failpoint after sync", err)})
		return
	}

	// The command is durable from here. Failing to apply it is an invariant
	// violation, not a rejected request.
	if err := e.runPhase(phaseBeforeApply); err != nil {
		e.fault("apply", err)
		sendReply(r, response{err: wrapError(CodeStorage, "apply", "failpoint before apply", err)})
		return
	}
	res, err := e.sm.apply(cmd, seq, e.cfg.MaxKeys)
	r.trace.mark(SpanApply)
	if err != nil {
		e.fault("apply", err)
		sendReply(r, response{err: wrapError(CodeCorruption, "apply",
			"durable command could not be applied", err)})
		return
	}
	e.mirrorState()
	if kind == OpPut {
		e.puts.Add(1)
	} else {
		e.deletes.Add(1)
	}
	if err := e.runPhase(phaseAfterApply); err != nil {
		e.fault("apply", err)
		sendReply(r, response{err: wrapError(CodeStorage, "apply", "failpoint after apply", err)})
		return
	}

	if err := e.maybeRotate(); err != nil {
		// The write itself succeeded and is durable, but the next append
		// target is uncertain, so the engine stops serving after answering.
		sendReply(r, response{result: res})
		e.fault("rotate", err)
		return
	}

	if err := e.runPhase(phaseBeforeReply); err != nil {
		e.fault("reply", err)
		sendReply(r, response{err: wrapError(CodeStorage, "reply", "failpoint before reply", err)})
		return
	}
	sendReply(r, response{result: res})
}

// maybeRotate seals the active segment once it passes the configured size.
// The threshold is operational: a segment may exceed it by one record.
func (e *Engine) maybeRotate() error {
	if e.wal.size < e.cfg.SegmentTargetBytes {
		return nil
	}
	return e.rotate()
}

// rotate seals the active segment and opens the next one. It is used both by
// the size threshold and by the snapshot barrier.
func (e *Engine) rotate() error {
	if e.wal.lastSeq < e.wal.startSeq {
		// Nothing has been written to this segment; there is nothing to seal.
		return nil
	}
	sealed, err := e.wal.seal()
	if err != nil {
		return err
	}
	e.cfg.emit(Event{Name: EventSegmentSealed, Segment: filepath.Base(sealed),
		Sequence: e.wal.lastSeq, Bytes: e.wal.size})

	next, err := createSegment(e.cfg.Dir, e.wal.lastSeq+1, e.hooks)
	if err != nil {
		return err
	}
	e.wal = next
	e.cfg.emit(Event{Name: EventSegmentCreated, Segment: filepath.Base(next.path),
		Sequence: next.startSeq})
	return nil
}

func (e *Engine) handleSnapshot(r *request) {
	if e.snapshotActive {
		sendReply(r, response{err: newError(CodeSnapshotInProgress, "CreateSnapshot",
			"another snapshot is already running")})
		return
	}
	seq := e.sm.applied
	if seq == e.snapshotSeq {
		// Nothing has been applied since the last snapshot; publishing again
		// would only rewrite identical bytes under a name that already exists.
		sendReply(r, response{
			result:       writeResult{AppliedSequence: seq},
			snapshotPath: filepath.Join(e.cfg.Dir, snapshotFinalName(seq)),
		})
		return
	}

	// The barrier runs on the event loop, so the captured copy and the
	// segment boundary describe exactly the same sequence.
	rotateErr := e.rotate()
	r.trace.mark(SpanSnapshotRotate)
	if err := rotateErr; err != nil {
		e.fault("snapshot.rotate", err)
		sendReply(r, response{err: wrapError(CodeStorage, "CreateSnapshot",
			"segment rotation failed at the snapshot barrier", err)})
		return
	}
	snap := e.sm.clone()
	r.trace.mark(SpanSnapshotCopy)

	e.snapshotActive = true
	e.snapActive.Store(true)
	e.snapshotReply = r.reply
	e.snapshotTrace = r.trace
	e.cfg.emit(Event{Name: EventSnapshotStart, Sequence: seq, Count: int64(len(snap.kv))})

	go func(snap *stateMachine, seq uint64, tr *traceRecorder) {
		path, ambiguous, err := publishSnapshot(e.cfg.Dir, snap, e.cfg, e.hooks, tr)
		e.snapshotDone <- snapshotOutcome{sequence: seq, path: path, ambiguous: ambiguous, err: err}
	}(snap, seq, r.trace)
}

func (e *Engine) completeSnapshot(out snapshotOutcome) {
	e.snapshotActive = false
	e.snapActive.Store(false)
	reply := e.snapshotReply
	trace := e.snapshotTrace
	e.snapshotReply, e.snapshotTrace = nil, nil

	if out.err != nil {
		e.cfg.emit(Event{Name: EventSnapshotFailed, Sequence: out.sequence,
			Code: CodeOf(out.err), Err: out.err})
		// A failure before publication leaves the active WAL path intact, so
		// the engine keeps serving. A failure at or after the rename can leave
		// the recovery path ambiguous, so it faults.
		if out.ambiguous {
			e.fault("snapshot.publish", out.err)
		}
		if reply != nil {
			trace.mark(SpanRespond)
			reply <- response{err: out.err}
		}
		return
	}

	e.snapshotSeq = out.sequence
	e.lastSnapSeq.Store(out.sequence)
	e.cfg.emit(Event{Name: EventSnapshotPublished, Sequence: out.sequence,
		Snapshot: filepath.Base(out.path)})

	// Only the owner compacts, and only after verified publication.
	e.compact(out.sequence)
	trace.mark(SpanCompaction)

	if reply != nil {
		trace.mark(SpanRespond)
		reply <- response{
			result:       writeResult{AppliedSequence: out.sequence},
			snapshotPath: out.path,
		}
	}
}

// compact deletes sealed segments the finalized snapshot fully covers, then
// prunes superseded snapshots while always retaining the newest prior one.
//
// Every step is best effort: a crash at any point leaves a valid recovery
// path, and a failure here never invalidates the snapshot that was published.
func (e *Engine) compact(through uint64) {
	entries, err := os.ReadDir(e.cfg.Dir)
	if err != nil {
		e.cfg.emit(Event{Name: EventStorageFailure, Detail: "compaction directory listing", Err: err})
		return
	}

	var (
		doomed    []segmentName
		snapshots []uint64
	)
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case hasSegmentPrefix(name):
			seg, perr := parseSegmentName(name)
			if perr == nil && seg.Sealed && seg.EndSeq <= through {
				doomed = append(doomed, seg)
			}
		case hasSnapshotPrefix(name):
			seq, final, perr := parseSnapshotName(name)
			if perr == nil && final {
				snapshots = append(snapshots, seq)
			}
		}
	}

	// Delete oldest first. Stopping partway then leaves a contiguous suffix
	// of segments rather than a hole, so an interrupted compaction still
	// presents a replayable chain on the next open.
	sort.Slice(doomed, func(i, j int) bool { return doomed[i].EndSeq < doomed[j].EndSeq })

	deleted := false
	for _, seg := range doomed {
		if rerr := e.hooks.Remove(filepath.Join(e.cfg.Dir, seg.Name)); rerr != nil {
			e.cfg.emit(Event{Name: EventStorageFailure, Segment: seg.Name,
				Detail: "segment deletion", Err: rerr})
			break
		}
		deleted = true
		e.cfg.emit(Event{Name: EventSegmentDeleted, Segment: seg.Name, Sequence: seg.EndSeq})
	}

	// Retain the newest snapshot and one prior; older ones are superseded.
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i] < snapshots[j] })
	if len(snapshots) > 2 {
		for _, seq := range snapshots[:len(snapshots)-2] {
			name := snapshotFinalName(seq)
			if rerr := e.hooks.Remove(filepath.Join(e.cfg.Dir, name)); rerr != nil {
				e.cfg.emit(Event{Name: EventStorageFailure, Snapshot: name,
					Detail: "snapshot deletion", Err: rerr})
				continue
			}
			deleted = true
		}
	}

	if deleted {
		if serr := e.hooks.SyncDir(e.cfg.Dir); serr != nil {
			e.cfg.emit(Event{Name: EventStorageFailure, Detail: "directory sync after compaction", Err: serr})
		}
	}
}

// fault moves the engine to Faulted. After this no operation is accepted, even
// though memory may still look readable: serving it would hide a required
// restart.
func (e *Engine) fault(op string, err error) {
	e.storageFails.Add(1)
	if e.state.CompareAndSwap(stateServing, stateFaulted) ||
		e.state.CompareAndSwap(stateRecovering, stateFaulted) {
		e.cfg.emit(Event{Name: EventFaulted, Detail: op, Code: CodeOf(err), Err: err})
	}
}

func (e *Engine) runPhase(phase string) error {
	if e.phase == nil {
		return nil
	}
	return e.phase(phase)
}

// shutdown runs on the event loop after admission has stopped and the queue
// has drained.
func (e *Engine) shutdown() {
	if e.snapshotActive {
		// Wait for the worker rather than abandoning a file it may still be
		// renaming. Finalized state is never deleted on the way out.
		out := <-e.snapshotDone
		e.completeSnapshot(out)
	}
	if e.wal != nil {
		if err := e.wal.sync(); err != nil {
			e.cfg.emit(Event{Name: EventStorageFailure, Detail: "sync on close", Err: err})
		}
		if err := e.wal.close(); err != nil {
			e.cfg.emit(Event{Name: EventStorageFailure, Detail: "close active segment", Err: err})
		}
	}
}

// Close stops admission, resolves queued requests as not executed, completes
// any write already in progress, waits for snapshot work, synchronizes and
// closes files, and releases the directory lock.
//
// A close deadline expiry is reported as an error. It never pretends durable
// cleanup happened.
func (e *Engine) Close(ctx context.Context) error {
	e.closeOne.Do(func() { e.closeErr = e.doClose(ctx) })
	return e.closeErr
}

func (e *Engine) doClose(ctx context.Context) error {
	const op = "Close"

	e.cfg.emit(Event{Name: EventCloseStart})

	e.admitMu.Lock()
	faulted := e.state.Load() == stateFaulted
	if !faulted {
		e.state.Store(stateClosing)
	}
	close(e.reqCh)
	e.admitMu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}
	timer := time.NewTimer(e.cfg.CloseTimeout)
	defer timer.Stop()

	select {
	case <-e.loopDone:
	case <-ctx.Done():
		e.cfg.emit(Event{Name: EventCloseDeadline, Err: ctx.Err()})
		return wrapError(CodeStorage, op, "close context ended before the event loop drained", ctx.Err())
	case <-timer.C:
		e.cfg.emit(Event{Name: EventCloseDeadline})
		return errorf(CodeStorage, op, "close timed out after %s", e.cfg.CloseTimeout)
	}

	err := e.lock.release()
	if !faulted {
		e.state.Store(stateClosed)
	}
	e.cfg.emit(Event{Name: EventCloseComplete, Sequence: e.appliedSeq.Load()})
	if err != nil {
		return wrapError(CodeStorage, op, "release directory lock", err)
	}
	return nil
}
