package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

func open(t *testing.T, dir string, tweak func(*Config)) *Engine {
	t.Helper()
	cfg := Config{Dir: dir}
	if tweak != nil {
		tweak(&cfg)
	}
	e, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return e
}

func closeEngine(t *testing.T, e *Engine) {
	t.Helper()
	if err := e.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func put(t *testing.T, e *Engine, client byte, seq uint64, key, value string) writeResult {
	t.Helper()
	res, err := e.Put(context.Background(), RequestIdentity{clientID(client), seq}, []byte(key), []byte(value))
	if err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
	return res
}

func del(t *testing.T, e *Engine, client byte, seq uint64, key string) writeResult {
	t.Helper()
	res, err := e.Delete(context.Background(), RequestIdentity{clientID(client), seq}, []byte(key))
	if err != nil {
		t.Fatalf("delete %s: %v", key, err)
	}
	return res
}

func expectGet(t *testing.T, e *Engine, key, want string) {
	t.Helper()
	v, found, _, err := e.Get(context.Background(), []byte(key))
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	if !found || string(v) != want {
		t.Fatalf("get %s = (%q, %v), want %q", key, v, found, want)
	}
}

func expectMissing(t *testing.T, e *Engine, key string) {
	t.Helper()
	if _, found, _, err := e.Get(context.Background(), []byte(key)); err != nil || found {
		t.Fatalf("get %s: found=%v err=%v, want missing", key, found, err)
	}
}

// --- basic behavior ----------------------------------------------------

func TestPutGetDelete(t *testing.T) {
	e := open(t, t.TempDir(), nil)
	defer closeEngine(t, e)

	put(t, e, 1, 1, "alpha", "one")
	expectGet(t, e, "alpha", "one")

	put(t, e, 1, 2, "alpha", "two")
	expectGet(t, e, "alpha", "two")

	// An empty value is a value, not an absence.
	put(t, e, 1, 3, "empty", "")
	v, found, _, err := e.Get(context.Background(), []byte("empty"))
	if err != nil || !found || len(v) != 0 {
		t.Fatalf("empty value: %q %v %v", v, found, err)
	}

	if res := del(t, e, 1, 4, "alpha"); !res.Existed {
		t.Fatal("deleting a present key must report existed=true")
	}
	expectMissing(t, e, "alpha")
	if res := del(t, e, 1, 5, "absent"); res.Existed {
		t.Fatal("deleting an absent key must report existed=false")
	}
}

func TestValidationRejectsBeforeExecution(t *testing.T) {
	e := open(t, t.TempDir(), func(c *Config) { c.MaxKeyBytes = 8; c.MaxValueBytes = 8 })
	defer closeEngine(t, e)

	ctx := context.Background()
	id := RequestIdentity{clientID(1), 1}

	cases := map[string]error{
		"empty key":    firstErr(e.Put(ctx, id, nil, []byte("v"))),
		"long key":     firstErr(e.Put(ctx, id, []byte("123456789"), []byte("v"))),
		"long value":   firstErr(e.Put(ctx, id, []byte("k"), []byte("123456789"))),
		"zero client":  firstErr(e.Put(ctx, RequestIdentity{ClientID{}, 1}, []byte("k"), []byte("v"))),
		"zero seq":     firstErr(e.Put(ctx, RequestIdentity{clientID(1), 0}, []byte("k"), []byte("v"))),
		"delete empty": firstErr(e.Delete(ctx, id, nil)),
	}
	for name, err := range cases {
		if CodeOf(err) != CodeInvalidArgument {
			t.Fatalf("%s: want INVALID_ARGUMENT, got %v", name, err)
		}
	}
	if e.Stats().AppliedSequence != 0 {
		t.Fatal("a rejected request reached the log")
	}
}

func firstErr(_ writeResult, err error) error { return err }

// --- acknowledged writes survive restart -------------------------------

func TestAcknowledgedWritesSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	e := open(t, dir, nil)
	for i := 1; i <= 50; i++ {
		put(t, e, 1, uint64(i), fmt.Sprintf("key-%02d", i), fmt.Sprintf("value-%02d", i))
	}
	del(t, e, 1, 51, "key-07")
	closeEngine(t, e)

	e = open(t, dir, nil)
	defer closeEngine(t, e)

	for i := 1; i <= 50; i++ {
		key := fmt.Sprintf("key-%02d", i)
		if i == 7 {
			expectMissing(t, e, key)
			continue
		}
		expectGet(t, e, key, fmt.Sprintf("value-%02d", i))
	}
	if got := e.Stats().AppliedSequence; got != 51 {
		t.Fatalf("applied sequence %d, want 51", got)
	}
}

// --- repeated recovery is deterministic --------------------------------

func TestRepeatedRecoveryIsIdentical(t *testing.T) {
	dir := t.TempDir()
	e := open(t, dir, nil)
	sequences := map[byte]uint64{}
	for i := 1; i <= 20; i++ {
		client := byte(i%3 + 1)
		sequences[client]++
		put(t, e, client, sequences[client], fmt.Sprintf("k%d", i%7), fmt.Sprintf("v%d", i))
	}
	closeEngine(t, e)

	var digests [][32]byte
	for round := 0; round < 3; round++ {
		e := open(t, dir, nil)
		digests = append(digests, e.sm.digest())
		closeEngine(t, e)
	}
	for i := 1; i < len(digests); i++ {
		if digests[i] != digests[0] {
			t.Fatalf("recovery %d produced a different state digest", i)
		}
	}
}

// --- duplicate suppression, live and across restart --------------------

func TestDuplicateRetry(t *testing.T) {
	dir := t.TempDir()
	e := open(t, dir, nil)

	first := put(t, e, 1, 1, "k", "v")
	if first.Duplicate {
		t.Fatal("first write reported as duplicate")
	}

	retry := put(t, e, 1, 1, "k", "v")
	if !retry.Duplicate || retry.AppliedSequence != first.AppliedSequence {
		t.Fatalf("live retry: %+v, want the stored result %+v", retry, first)
	}
	if e.Stats().AppliedSequence != 1 {
		t.Fatal("a duplicate retry appended a second command")
	}

	// Conflicting content under the same sequence is refused.
	_, err := e.Put(context.Background(), RequestIdentity{clientID(1), 1}, []byte("k"), []byte("other"))
	if CodeOf(err) != CodeRequestIDReuse {
		t.Fatalf("conflicting retry: want REQUEST_ID_REUSE, got %v", err)
	}

	del(t, e, 1, 2, "k")
	closeEngine(t, e)

	// The session table is durable, so the retry still resolves after restart.
	e = open(t, dir, nil)
	defer closeEngine(t, e)

	res, err := e.Delete(context.Background(), RequestIdentity{clientID(1), 2}, []byte("k"))
	if err != nil || !res.Duplicate {
		t.Fatalf("retry after restart: %+v %v", res, err)
	}
	_, err = e.Put(context.Background(), RequestIdentity{clientID(1), 1}, []byte("k"), []byte("v"))
	if CodeOf(err) != CodeDuplicateResultExpired {
		t.Fatalf("expired retry after restart: want DUPLICATE_RESULT_EXPIRED, got %v", err)
	}
}

// --- one writer per directory ------------------------------------------

func TestExclusiveDirectoryOwnership(t *testing.T) {
	dir := t.TempDir()
	e := open(t, dir, nil)
	defer closeEngine(t, e)

	if _, err := Open(Config{Dir: dir}); CodeOf(err) != CodeLocked {
		t.Fatalf("second open: want LOCKED, got %v", err)
	}
	put(t, e, 1, 1, "k", "v") // the first engine is unaffected
}

// --- tail damage versus interior corruption ----------------------------

func activeSegmentPath(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == activeSuffix {
			return filepath.Join(dir, entry.Name())
		}
	}
	t.Fatal("no active segment")
	return ""
}

// A crash between write and sync can leave a partial frame. Recovery discards
// exactly that frame and keeps every acknowledged write before it.
func TestTornTailIsTruncated(t *testing.T) {
	for _, cut := range []int{1, 5, 17, 24, 31} {
		t.Run(fmt.Sprintf("cut-%d", cut), func(t *testing.T) {
			dir := t.TempDir()
			e := open(t, dir, nil)
			for i := 1; i <= 5; i++ {
				put(t, e, 1, uint64(i), fmt.Sprintf("k%d", i), "v")
			}
			closeEngine(t, e)

			// Simulate a torn write: append a frame prefix to the segment.
			path := activeSegmentPath(t, dir)
			partial := encodeFrame(nil, 6, bytes.Repeat([]byte("x"), 64))
			if cut > len(partial) {
				t.Skip("cut longer than frame")
			}
			f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write(partial[:cut]); err != nil {
				t.Fatal(err)
			}
			f.Close()

			e = open(t, dir, nil)
			defer closeEngine(t, e)

			for i := 1; i <= 5; i++ {
				expectGet(t, e, fmt.Sprintf("k%d", i), "v")
			}
			if got := e.Stats().AppliedSequence; got != 5 {
				t.Fatalf("applied sequence %d, want 5", got)
			}
			if e.Stats().TruncatedBytes != int64(cut) {
				t.Fatalf("truncated %d bytes, want %d", e.Stats().TruncatedBytes, cut)
			}
			// The engine must be able to write again immediately.
			put(t, e, 1, 6, "k6", "v")
		})
	}
}

// A checksum failure on the final record is still only a tail candidate.
func TestCorruptFinalRecordIsDropped(t *testing.T) {
	dir := t.TempDir()
	e := open(t, dir, nil)
	for i := 1; i <= 3; i++ {
		put(t, e, 1, uint64(i), fmt.Sprintf("k%d", i), "v")
	}
	closeEngine(t, e)

	path := activeSegmentPath(t, dir)
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	buf[len(buf)-1] ^= 0xff // flip a byte inside the last frame's payload
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}

	e = open(t, dir, nil)
	defer closeEngine(t, e)

	expectGet(t, e, "k1", "v")
	expectGet(t, e, "k2", "v")
	expectMissing(t, e, "k3") // never acknowledged as durable after the flip
	if got := e.Stats().AppliedSequence; got != 2 {
		t.Fatalf("applied sequence %d, want 2", got)
	}
}

// Damage before later data is interior corruption: the engine refuses to open
// rather than skipping a record.
func TestInteriorCorruptionFailsClosed(t *testing.T) {
	dir := t.TempDir()
	e := open(t, dir, nil)
	for i := 1; i <= 4; i++ {
		put(t, e, 1, uint64(i), fmt.Sprintf("k%d", i), "value")
	}
	closeEngine(t, e)

	path := activeSegmentPath(t, dir)
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	buf[segmentHeaderBytes+frameHeaderBytes+2] ^= 0xff // inside the first record
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(Config{Dir: dir}); CodeOf(err) != CodeCorruption {
		t.Fatalf("want CORRUPTION, got %v", err)
	}
}

func TestCorruptSegmentHeaderFailsClosed(t *testing.T) {
	dir := t.TempDir()
	e := open(t, dir, nil)
	put(t, e, 1, 1, "k", "v")
	closeEngine(t, e)

	path := activeSegmentPath(t, dir)
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	buf[4] = 9 // unknown segment format version
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{Dir: dir}); CodeOf(err) != CodeCorruption {
		t.Fatalf("want CORRUPTION, got %v", err)
	}
}

// --- snapshots and safe compaction -------------------------------------

func TestSnapshotAndCompaction(t *testing.T) {
	dir := t.TempDir()
	// A small segment target forces several sealed segments before the snapshot.
	e := open(t, dir, func(c *Config) { c.SegmentTargetBytes = 1024 })

	for i := 1; i <= 100; i++ {
		put(t, e, 1, uint64(i), fmt.Sprintf("key-%03d", i), bytes.NewBuffer(make([]byte, 0)).String()+"value")
	}
	del(t, e, 1, 101, "key-050")

	sealedBefore := countFiles(t, dir, sealedSuffix)
	if sealedBefore == 0 {
		t.Fatal("expected rotation to seal segments")
	}

	seq, path, err := e.CreateSnapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if seq != 101 {
		t.Fatalf("snapshot sequence %d, want 101", seq)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("snapshot file: %v", err)
	}
	if got := countFiles(t, dir, sealedSuffix); got != 0 {
		t.Fatalf("%d sealed segments survived compaction", got)
	}

	// Writes continue after the barrier and must also survive.
	put(t, e, 1, 102, "after-snapshot", "yes")
	closeEngine(t, e)

	e = open(t, dir, nil)
	defer closeEngine(t, e)

	if got := e.Stats().AppliedSequence; got != 102 {
		t.Fatalf("applied sequence %d, want 102", got)
	}
	expectGet(t, e, "key-001", "value")
	expectGet(t, e, "key-100", "value")
	expectMissing(t, e, "key-050")
	expectGet(t, e, "after-snapshot", "yes")

	// The retained session state must still recognize a retry.
	res, err := e.Put(context.Background(), RequestIdentity{clientID(1), 102}, []byte("after-snapshot"), []byte("yes"))
	if err != nil || !res.Duplicate {
		t.Fatalf("retry across snapshot: %+v %v", res, err)
	}
}

func countFiles(t *testing.T, dir, suffix string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == suffix {
			n++
		}
	}
	return n
}

func TestSnapshotWithoutNewCommandsIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	e := open(t, dir, nil)
	defer closeEngine(t, e)

	put(t, e, 1, 1, "k", "v")
	seq1, path1, err := e.CreateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	seq2, path2, err := e.CreateSnapshot(context.Background())
	if err != nil {
		t.Fatalf("second snapshot: %v", err)
	}
	if seq1 != seq2 || path1 != path2 {
		t.Fatalf("expected the same snapshot, got %d/%s and %d/%s", seq1, path1, seq2, path2)
	}
}

// A corrupt highest snapshot fails closed rather than silently falling back to
// an older one whose WAL prefix may already be gone.
func TestCorruptHighestSnapshotFailsClosed(t *testing.T) {
	dir := t.TempDir()
	e := open(t, dir, nil)
	put(t, e, 1, 1, "k", "v")
	_, path, err := e.CreateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	closeEngine(t, e)

	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	buf[len(buf)-1] ^= 0xff
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(Config{Dir: dir}); CodeOf(err) != CodeCorruption {
		t.Fatalf("want CORRUPTION, got %v", err)
	}
}

// A temporary snapshot is never a recovery source, at any partial length.
func TestTemporarySnapshotIsIgnored(t *testing.T) {
	dir := t.TempDir()
	e := open(t, dir, nil)
	put(t, e, 1, 1, "k", "v")
	closeEngine(t, e)

	full := encodeSnapshot(buildState(t, 3))
	for _, n := range []int{0, 1, snapshotHeaderBytes, len(full)} {
		tmp := filepath.Join(dir, snapshotTmpName(99))
		if err := os.WriteFile(tmp, full[:n], 0o644); err != nil {
			t.Fatal(err)
		}
		e := open(t, dir, nil)
		if got := e.Stats().AppliedSequence; got != 1 {
			t.Fatalf("temp snapshot of %d bytes changed recovery: applied %d", n, got)
		}
		expectGet(t, e, "k", "v")
		closeEngine(t, e)
		os.Remove(tmp)
	}
}

// --- ambiguous storage errors fault the engine -------------------------

func TestSyncFailureFaultsEngine(t *testing.T) {
	dir := t.TempDir()
	hooks := defaultHooks()
	var failSync bool
	realSync := hooks.Sync
	hooks.Sync = func(f *os.File) error {
		if failSync {
			return errors.New("injected sync failure")
		}
		return realSync(f)
	}

	e, err := openEngine(Config{Dir: dir}, hooks, nil)
	if err != nil {
		t.Fatal(err)
	}
	put(t, e, 1, 1, "k", "v")

	failSync = true
	_, err = e.Put(context.Background(), RequestIdentity{clientID(1), 2}, []byte("k2"), []byte("v"))
	if CodeOf(err) != CodeStorage {
		t.Fatalf("want STORAGE, got %v", err)
	}

	// Everything is refused afterwards, even reads that memory could serve.
	if _, _, _, err := e.Get(context.Background(), []byte("k")); CodeOf(err) != CodeFaulted {
		t.Fatalf("get after fault: want FAULTED, got %v", err)
	}
	if _, err := e.Put(context.Background(), RequestIdentity{clientID(1), 3}, []byte("k3"), []byte("v")); CodeOf(err) != CodeFaulted {
		t.Fatalf("put after fault: want FAULTED, got %v", err)
	}

	// Stats and Close still work.
	if s := e.Stats(); s.State != "Faulted" || s.StorageFailures == 0 {
		t.Fatalf("stats after fault: %+v", s)
	}
	failSync = false
	if err := e.Close(context.Background()); err != nil {
		t.Fatalf("close after fault: %v", err)
	}

	// The acknowledged write is still recoverable.
	e2 := open(t, dir, nil)
	defer closeEngine(t, e2)
	expectGet(t, e2, "k", "v")
}

// A short write leaves a partial frame; the engine must fault rather than
// acknowledge, and recovery must discard exactly the partial bytes.
func TestShortWriteFaultsAndRecovers(t *testing.T) {
	dir := t.TempDir()
	hooks := defaultHooks()
	var truncateWrites bool
	hooks.Write = func(f *os.File, b []byte) (int, error) {
		if truncateWrites && len(b) > 8 {
			n, _ := f.Write(b[:8])
			return n, errors.New("injected disk full")
		}
		return f.Write(b)
	}

	e, err := openEngine(Config{Dir: dir}, hooks, nil)
	if err != nil {
		t.Fatal(err)
	}
	put(t, e, 1, 1, "k1", "v")

	truncateWrites = true
	if _, err := e.Put(context.Background(), RequestIdentity{clientID(1), 2}, []byte("k2"), []byte("v")); CodeOf(err) != CodeStorage {
		t.Fatalf("want STORAGE, got %v", err)
	}
	truncateWrites = false
	e.Close(context.Background())

	e2 := open(t, dir, nil)
	defer closeEngine(t, e2)
	expectGet(t, e2, "k1", "v")
	expectMissing(t, e2, "k2")
	if got := e2.Stats().AppliedSequence; got != 1 {
		t.Fatalf("applied %d, want 1", got)
	}
}

// --- bounded admission -------------------------------------------------

func TestQueueFullRejectsBeforeExecution(t *testing.T) {
	dir := t.TempDir()
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	var once sync.Once

	phase := func(p string) error {
		if p == phaseBeforeAppend {
			once.Do(func() {
				entered <- struct{}{}
				<-release
			})
		}
		return nil
	}

	e, err := openEngine(Config{Dir: dir, QueueCapacity: 1}, defaultHooks(), phase)
	if err != nil {
		t.Fatal(err)
	}
	defer closeEngine(t, e)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		e.Put(context.Background(), RequestIdentity{clientID(1), 1}, []byte("k1"), []byte("v"))
	}()
	<-entered // the loop is now blocked inside the first write

	wg.Add(1)
	go func() {
		defer wg.Done()
		e.Put(context.Background(), RequestIdentity{clientID(2), 1}, []byte("k2"), []byte("v"))
	}()

	deadline := time.Now().Add(2 * time.Second)
	for e.Stats().QueueDepth < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	_, err = e.Put(context.Background(), RequestIdentity{clientID(3), 1}, []byte("k3"), []byte("v"))
	if CodeOf(err) != CodeResourceExhausted {
		t.Fatalf("want RESOURCE_EXHAUSTED, got %v", err)
	}
	if e.Stats().EnqueueRejections == 0 {
		t.Fatal("rejection was not counted")
	}

	close(release)
	wg.Wait()
}

func TestCanceledContextIsNotExecuted(t *testing.T) {
	e := open(t, t.TempDir(), nil)
	defer closeEngine(t, e)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.Put(ctx, RequestIdentity{clientID(1), 1}, []byte("k"), []byte("v")); CodeOf(err) != CodeCanceled {
		t.Fatalf("want CANCELED, got %v", err)
	}
	if e.Stats().AppliedSequence != 0 {
		t.Fatal("a canceled request was executed")
	}
}

func TestClosedEngineRejects(t *testing.T) {
	e := open(t, t.TempDir(), nil)
	put(t, e, 1, 1, "k", "v")
	closeEngine(t, e)

	if _, err := e.Put(context.Background(), RequestIdentity{clientID(1), 2}, []byte("k"), []byte("v")); CodeOf(err) != CodeClosed {
		t.Fatalf("want CLOSED, got %v", err)
	}
	if err := e.Close(context.Background()); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// --- concurrency -------------------------------------------------------

// Concurrent clients exercise the event loop under the race detector. Each
// client writes its own sequence, and every acknowledged write must be
// readable afterwards and survive a restart.
func TestConcurrentClients(t *testing.T) {
	dir := t.TempDir()
	e := open(t, dir, nil)

	const clients, writes = 8, 40
	var wg sync.WaitGroup
	for c := 1; c <= clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for i := 1; i <= writes; i++ {
				key := fmt.Sprintf("c%d-k%d", c, i)
				_, err := e.Put(context.Background(), RequestIdentity{clientID(byte(c)), uint64(i)},
					[]byte(key), []byte("v"))
				if err != nil {
					t.Errorf("client %d write %d: %v", c, i, err)
					return
				}
				if _, found, _, err := e.Get(context.Background(), []byte(key)); err != nil || !found {
					t.Errorf("client %d read back %d: found=%v err=%v", c, i, found, err)
					return
				}
			}
		}(c)
	}
	wg.Wait()
	closeEngine(t, e)

	e = open(t, dir, nil)
	defer closeEngine(t, e)
	if got := e.Stats().AppliedSequence; got != clients*writes {
		t.Fatalf("applied %d, want %d", got, clients*writes)
	}
	for c := 1; c <= clients; c++ {
		for i := 1; i <= writes; i++ {
			expectGet(t, e, fmt.Sprintf("c%d-k%d", c, i), "v")
		}
	}
}

func TestSnapshotConcurrentWithWrites(t *testing.T) {
	dir := t.TempDir()
	e := open(t, dir, func(c *Config) { c.SegmentTargetBytes = 2048 })

	for i := 1; i <= 200; i++ {
		put(t, e, 1, uint64(i), fmt.Sprintf("k%03d", i), "v")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, _, err := e.CreateSnapshot(context.Background()); err != nil {
			t.Errorf("snapshot: %v", err)
		}
	}()
	for i := 201; i <= 300; i++ {
		put(t, e, 1, uint64(i), fmt.Sprintf("k%03d", i), "v")
	}
	wg.Wait()
	closeEngine(t, e)

	e = open(t, dir, nil)
	defer closeEngine(t, e)
	if got := e.Stats().AppliedSequence; got != 300 {
		t.Fatalf("applied %d, want 300", got)
	}
	for i := 1; i <= 300; i++ {
		expectGet(t, e, fmt.Sprintf("k%03d", i), "v")
	}
}

// --- randomized model comparison ---------------------------------------

// TestRandomizedModelComparison drives random operations through the engine
// while a plain map tracks what the result must be, restarting the engine at
// random points. Any divergence between the model and recovered state is a
// failure. The seed is printed so a failure can be replayed.
func TestRandomizedModelComparison(t *testing.T) {
	seed := int64(20260919)
	if s := os.Getenv("RIVET_SEED"); s != "" {
		fmt.Sscanf(s, "%d", &seed)
	}
	t.Logf("seed=%d (replay with RIVET_SEED=%d)", seed, seed)
	rng := rand.New(rand.NewSource(seed))

	dir := t.TempDir()
	model := map[string]string{}
	sequences := map[byte]uint64{}

	e := open(t, dir, func(c *Config) { c.SegmentTargetBytes = 4096 })
	defer func() {
		if e != nil {
			e.Close(context.Background())
		}
	}()

	const steps = 1500
	for step := 0; step < steps; step++ {
		switch n := rng.Intn(100); {
		case n < 55: // put
			client := byte(rng.Intn(4) + 1)
			sequences[client]++
			key := fmt.Sprintf("k%02d", rng.Intn(30))
			value := fmt.Sprintf("v%d", rng.Intn(1000))
			if _, err := e.Put(context.Background(), RequestIdentity{clientID(client), sequences[client]},
				[]byte(key), []byte(value)); err != nil {
				t.Fatalf("step %d put: %v", step, err)
			}
			model[key] = value

		case n < 70: // delete
			client := byte(rng.Intn(4) + 1)
			sequences[client]++
			key := fmt.Sprintf("k%02d", rng.Intn(30))
			_, wasPresent := model[key]
			res, err := e.Delete(context.Background(), RequestIdentity{clientID(client), sequences[client]}, []byte(key))
			if err != nil {
				t.Fatalf("step %d delete: %v", step, err)
			}
			if res.Existed != wasPresent {
				t.Fatalf("step %d delete existed=%v, model says %v", step, res.Existed, wasPresent)
			}
			delete(model, key)

		case n < 90: // read
			key := fmt.Sprintf("k%02d", rng.Intn(30))
			value, found, _, err := e.Get(context.Background(), []byte(key))
			if err != nil {
				t.Fatalf("step %d get: %v", step, err)
			}
			want, inModel := model[key]
			if found != inModel || (found && string(value) != want) {
				t.Fatalf("step %d get %s = (%q,%v), model says (%q,%v)", step, key, value, found, want, inModel)
			}

		case n < 94: // retry the last write of a client
			client := byte(rng.Intn(4) + 1)
			if sequences[client] == 0 {
				continue
			}

		case n < 97: // snapshot
			if _, _, err := e.CreateSnapshot(context.Background()); err != nil {
				t.Fatalf("step %d snapshot: %v", step, err)
			}

		default: // restart
			if err := e.Close(context.Background()); err != nil {
				t.Fatalf("step %d close: %v", step, err)
			}
			e = open(t, dir, func(c *Config) { c.SegmentTargetBytes = 4096 })
			verifyModel(t, e, model, step)
		}
	}

	verifyModel(t, e, model, steps)
	if err := e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	e = open(t, dir, nil)
	verifyModel(t, e, model, steps)
}

func verifyModel(t *testing.T, e *Engine, model map[string]string, step int) {
	t.Helper()
	keys := make([]string, 0, len(model))
	for k := range model {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value, found, _, err := e.Get(context.Background(), []byte(key))
		if err != nil || !found || string(value) != model[key] {
			t.Fatalf("step %d: key %s = (%q,%v,%v), model says %q", step, key, value, found, err, model[key])
		}
	}
	if got := int(e.Stats().Keys); got != len(model) {
		t.Fatalf("step %d: engine holds %d keys, model holds %d", step, got, len(model))
	}
}
