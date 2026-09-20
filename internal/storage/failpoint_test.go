package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

// These tests inject failures into the storage operations that a crash or a
// failing disk can interrupt: the rename that seals a segment, each directory
// synchronization, every stage of snapshot publication, each compaction
// deletion, and the truncation recovery performs on a torn tail.
//
// Two properties are checked every time. An acknowledged write is still
// readable after reopening, and the directory always reopens onto a valid
// state unless the injected failure is one that genuinely makes the recovery
// path unprovable.

var errInjected = errors.New("injected failure")

// failpointHooks returns default hooks plus the originals, so a test can wrap
// one operation and delegate the rest.
func failpointHooks() (hooks, real *ioHooks) {
	hooks = defaultHooks()
	original := *hooks
	return hooks, &original
}

const failpointValueBytes = 200

// writeUntilError writes records until one fails, and reports how many were
// acknowledged. Values are large enough that a small segment target forces
// rotation partway through.
func writeUntilError(e *Engine, client byte, attempts int) (acked int, err error) {
	value := bytes.Repeat([]byte("v"), failpointValueBytes)
	for i := 1; i <= attempts; i++ {
		_, err = e.Put(context.Background(), RequestIdentity{clientID(client), uint64(i)},
			[]byte(fmt.Sprintf("k%03d", i)), value)
		if err != nil {
			return acked, err
		}
		acked++
	}
	return acked, nil
}

// expectAckedSurvive reopens the directory with real hooks and checks that
// every acknowledged write is present and that nothing beyond them is.
func expectAckedSurvive(t *testing.T, dir string, acked int) *Engine {
	t.Helper()

	e := open(t, dir, nil)
	value := strings.Repeat("v", failpointValueBytes)
	for i := 1; i <= acked; i++ {
		expectGet(t, e, fmt.Sprintf("k%03d", i), value)
	}
	if got := e.Stats().AppliedSequence; got != uint64(acked) {
		t.Fatalf("applied sequence %d, want %d acknowledged writes", got, acked)
	}
	return e
}

func rotatingConfig(dir string) Config {
	return Config{Dir: dir, SegmentTargetBytes: 1024}
}

// --- segment seal and directory synchronization ------------------------

// The rename that seals a segment leaves the next append target uncertain when
// it fails, so the engine must stop rather than guess.
func TestSealRenameFailureFaults(t *testing.T) {
	dir := t.TempDir()
	hooks, real := failpointHooks()
	hooks.Rename = func(oldPath, newPath string) error {
		if strings.HasSuffix(newPath, sealedSuffix) {
			return errInjected
		}
		return real.Rename(oldPath, newPath)
	}

	e, err := openEngine(rotatingConfig(dir), hooks, nil)
	if err != nil {
		t.Fatal(err)
	}
	acked, err := writeUntilError(e, 1, 30)
	if CodeOf(err) != CodeFaulted {
		t.Fatalf("want FAULTED once rotation fails, got %v", err)
	}
	if acked == 0 {
		t.Fatal("no write was acknowledged before the failure")
	}
	if s := e.Stats(); s.State != "Faulted" {
		t.Fatalf("state %s, want Faulted", s.State)
	}
	e.Close(context.Background())

	// The rename never happened, so every record is still in the active
	// segment and must replay.
	closeEngine(t, expectAckedSurvive(t, dir, acked))
}

// The rename succeeds but the directory synchronization that makes it durable
// does not. The engine faults, and either name must recover.
func TestSealDirectorySyncFailureFaults(t *testing.T) {
	dir := t.TempDir()
	hooks, real := failpointHooks()
	var sealing atomic.Bool

	hooks.Rename = func(oldPath, newPath string) error {
		err := real.Rename(oldPath, newPath)
		if err == nil && strings.HasSuffix(newPath, sealedSuffix) {
			sealing.Store(true)
		}
		return err
	}
	hooks.SyncDir = func(d string) error {
		if sealing.Swap(false) {
			return errInjected
		}
		return real.SyncDir(d)
	}

	e, err := openEngine(rotatingConfig(dir), hooks, nil)
	if err != nil {
		t.Fatal(err)
	}
	acked, err := writeUntilError(e, 1, 30)
	if CodeOf(err) != CodeFaulted {
		t.Fatalf("want FAULTED, got %v", err)
	}
	e.Close(context.Background())

	if countFiles(t, dir, sealedSuffix) == 0 {
		t.Fatal("expected the sealed segment to exist after a successful rename")
	}
	closeEngine(t, expectAckedSurvive(t, dir, acked))
}

// The seal completes but creating the next active segment fails at its
// directory synchronization, leaving an empty active segment behind.
func TestSegmentCreateDirectorySyncFailureRecovers(t *testing.T) {
	dir := t.TempDir()
	hooks, real := failpointHooks()
	var stage atomic.Int32 // 0 idle, 1 seal renamed, 2 seal synced

	hooks.Rename = func(oldPath, newPath string) error {
		err := real.Rename(oldPath, newPath)
		if err == nil && strings.HasSuffix(newPath, sealedSuffix) {
			stage.Store(1)
		}
		return err
	}
	hooks.SyncDir = func(d string) error {
		switch stage.Load() {
		case 1: // the seal's own directory sync
			stage.Store(2)
			return real.SyncDir(d)
		case 2: // the new segment's directory sync
			stage.Store(0)
			return errInjected
		}
		return real.SyncDir(d)
	}

	e, err := openEngine(rotatingConfig(dir), hooks, nil)
	if err != nil {
		t.Fatal(err)
	}
	acked, err := writeUntilError(e, 1, 30)
	if CodeOf(err) != CodeFaulted {
		t.Fatalf("want FAULTED, got %v", err)
	}
	e.Close(context.Background())

	// A sealed segment plus an empty active segment is a valid chain.
	reopened := expectAckedSurvive(t, dir, acked)
	defer closeEngine(t, reopened)
	if _, err := reopened.Put(context.Background(), RequestIdentity{clientID(2), 1},
		[]byte("after"), []byte("ok")); err != nil {
		t.Fatalf("write after recovery: %v", err)
	}
}

// --- snapshot publication stages ---------------------------------------

// A snapshot that fails before its rename has touched nothing the active WAL
// path depends on, so the engine keeps serving.
func TestSnapshotPrepublicationFailureKeepsServing(t *testing.T) {
	cases := map[string]func(hooks, real *ioHooks){
		"temp write": func(hooks, real *ioHooks) {
			hooks.Write = func(f *os.File, b []byte) (int, error) {
				if strings.HasSuffix(f.Name(), snapshotTmpSuffix) {
					return 0, errInjected
				}
				return real.Write(f, b)
			}
		},
		"temp sync": func(hooks, real *ioHooks) {
			hooks.Sync = func(f *os.File) error {
				if strings.HasSuffix(f.Name(), snapshotTmpSuffix) {
					return errInjected
				}
				return real.Sync(f)
			}
		},
	}

	for name, inject := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			hooks, real := failpointHooks()
			inject(hooks, real)

			e, err := openEngine(rotatingConfig(dir), hooks, nil)
			if err != nil {
				t.Fatal(err)
			}
			acked, err := writeUntilError(e, 1, 12)
			if err != nil {
				t.Fatalf("writes before snapshot: %v", err)
			}

			if _, _, err := e.CreateSnapshot(context.Background()); err == nil {
				t.Fatal("snapshot reported success despite an injected failure")
			}
			if s := e.Stats(); s.State != "Serving" {
				t.Fatalf("state %s, want Serving: a snapshot-only failure must not fault", s.State)
			}

			// The engine must still accept writes on the untouched WAL path.
			if _, err := e.Put(context.Background(), RequestIdentity{clientID(1), uint64(acked + 1)},
				[]byte("still-writable"), []byte("yes")); err != nil {
				t.Fatalf("write after failed snapshot: %v", err)
			}
			closeEngine(t, e)

			reopened := open(t, dir, nil)
			defer closeEngine(t, reopened)
			value := strings.Repeat("v", failpointValueBytes)
			for i := 1; i <= acked; i++ {
				expectGet(t, reopened, fmt.Sprintf("k%03d", i), value)
			}
			expectGet(t, reopened, "still-writable", "yes")
			if got := reopened.Stats().AppliedSequence; got != uint64(acked+1) {
				t.Fatalf("applied sequence %d, want %d", got, acked+1)
			}
		})
	}
}

// A failure at or after the rename can leave the recovery path ambiguous, so
// the engine faults even though the snapshot was the only thing being written.
func TestSnapshotRenameFailureFaults(t *testing.T) {
	dir := t.TempDir()
	hooks, real := failpointHooks()
	hooks.Rename = func(oldPath, newPath string) error {
		if strings.HasSuffix(newPath, snapshotFinalSuffix) {
			return errInjected
		}
		return real.Rename(oldPath, newPath)
	}

	e, err := openEngine(rotatingConfig(dir), hooks, nil)
	if err != nil {
		t.Fatal(err)
	}
	acked, err := writeUntilError(e, 1, 12)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.CreateSnapshot(context.Background()); err == nil {
		t.Fatal("snapshot reported success despite a failed rename")
	}
	if s := e.Stats(); s.State != "Faulted" {
		t.Fatalf("state %s, want Faulted", s.State)
	}
	e.Close(context.Background())

	// The temporary file is never a recovery source; the WAL still holds
	// everything.
	if n := countFiles(t, dir, snapshotTmpSuffix); n == 0 {
		t.Fatal("expected the temporary snapshot to remain for diagnosis")
	}
	if n := countFiles(t, dir, snapshotFinalSuffix); n != 0 {
		t.Fatalf("%d finalized snapshots exist after a failed rename", n)
	}
	closeEngine(t, expectAckedSurvive(t, dir, acked))
}

// The rename lands but its directory synchronization fails. The engine faults,
// and the now-visible snapshot must still be a valid recovery source.
func TestSnapshotDirectorySyncFailureFaults(t *testing.T) {
	dir := t.TempDir()
	hooks, real := failpointHooks()
	var renamed atomic.Bool

	hooks.Rename = func(oldPath, newPath string) error {
		err := real.Rename(oldPath, newPath)
		if err == nil && strings.HasSuffix(newPath, snapshotFinalSuffix) {
			renamed.Store(true)
		}
		return err
	}
	hooks.SyncDir = func(d string) error {
		if renamed.Swap(false) {
			return errInjected
		}
		return real.SyncDir(d)
	}

	e, err := openEngine(rotatingConfig(dir), hooks, nil)
	if err != nil {
		t.Fatal(err)
	}
	acked, err := writeUntilError(e, 1, 12)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.CreateSnapshot(context.Background()); err == nil {
		t.Fatal("snapshot reported success despite a failed directory sync")
	}
	if s := e.Stats(); s.State != "Faulted" {
		t.Fatalf("state %s, want Faulted", s.State)
	}
	e.Close(context.Background())

	// No compaction ran, so both the snapshot and the full segment chain are
	// available and must agree.
	if n := countFiles(t, dir, snapshotFinalSuffix); n != 1 {
		t.Fatalf("%d finalized snapshots, want 1", n)
	}
	closeEngine(t, expectAckedSurvive(t, dir, acked))
}

// --- compaction --------------------------------------------------------

// Compaction runs after the snapshot is already durable, so a deletion
// failure is not a durability problem. The snapshot stands, the engine keeps
// serving, and the undeleted segments simply replay again.
func TestCompactionDeletionFailureKeepsServing(t *testing.T) {
	dir := t.TempDir()
	hooks, real := failpointHooks()
	hooks.Remove = func(name string) error {
		if strings.HasSuffix(name, sealedSuffix) {
			return errInjected
		}
		return real.Remove(name)
	}

	e, err := openEngine(rotatingConfig(dir), hooks, nil)
	if err != nil {
		t.Fatal(err)
	}
	acked, err := writeUntilError(e, 1, 40)
	if err != nil {
		t.Fatal(err)
	}
	before := countFiles(t, dir, sealedSuffix)
	if before == 0 {
		t.Fatal("expected sealed segments before the snapshot")
	}

	seq, _, err := e.CreateSnapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot must succeed even when compaction cannot: %v", err)
	}
	if seq != uint64(acked) {
		t.Fatalf("snapshot sequence %d, want %d", seq, acked)
	}
	if s := e.Stats(); s.State != "Serving" {
		t.Fatalf("state %s, want Serving", s.State)
	}
	if after := countFiles(t, dir, sealedSuffix); after != before {
		t.Fatalf("%d sealed segments remain, want all %d", after, before)
	}
	closeEngine(t, e)

	// Recovery must skip the records the snapshot already contains rather than
	// reapplying or rejecting them.
	closeEngine(t, expectAckedSurvive(t, dir, acked))
}

// Interrupting compaction partway must leave a contiguous suffix of segments,
// never a hole, so the remaining chain still replays.
func TestCompactionPartialDeletionRecovers(t *testing.T) {
	dir := t.TempDir()
	hooks, real := failpointHooks()
	var removals atomic.Int32

	hooks.Remove = func(name string) error {
		if strings.HasSuffix(name, sealedSuffix) && removals.Add(1) > 1 {
			return errInjected
		}
		return real.Remove(name)
	}

	e, err := openEngine(rotatingConfig(dir), hooks, nil)
	if err != nil {
		t.Fatal(err)
	}
	acked, err := writeUntilError(e, 1, 60)
	if err != nil {
		t.Fatal(err)
	}
	before := countFiles(t, dir, sealedSuffix)
	if before < 3 {
		t.Fatalf("need several sealed segments to interrupt, got %d", before)
	}

	if _, _, err := e.CreateSnapshot(context.Background()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	after := countFiles(t, dir, sealedSuffix)
	if after != before-1 {
		t.Fatalf("%d sealed segments remain, want %d after one deletion", after, before-1)
	}
	closeEngine(t, e)

	closeEngine(t, expectAckedSurvive(t, dir, acked))
}

// The directory synchronization that follows compaction is best effort: its
// failure must not undo a published snapshot or stop the engine.
func TestCompactionDirectorySyncFailureIsNonFatal(t *testing.T) {
	dir := t.TempDir()
	hooks, real := failpointHooks()
	var compacting atomic.Bool

	hooks.Remove = func(name string) error {
		err := real.Remove(name)
		if err == nil && strings.HasSuffix(name, sealedSuffix) {
			compacting.Store(true)
		}
		return err
	}
	hooks.SyncDir = func(d string) error {
		if compacting.Swap(false) {
			return errInjected
		}
		return real.SyncDir(d)
	}

	e, err := openEngine(rotatingConfig(dir), hooks, nil)
	if err != nil {
		t.Fatal(err)
	}
	acked, err := writeUntilError(e, 1, 40)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.CreateSnapshot(context.Background()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if s := e.Stats(); s.State != "Serving" {
		t.Fatalf("state %s, want Serving", s.State)
	}
	closeEngine(t, e)

	closeEngine(t, expectAckedSurvive(t, dir, acked))
}

// --- recovery truncation ------------------------------------------------

// If the torn tail cannot be removed, recovery must refuse to open. Serving
// with those bytes still present would let a later append build on a record
// the engine has already decided to discard.
func TestRecoveryTruncateFailureRefusesToOpen(t *testing.T) {
	dir := t.TempDir()
	e := open(t, dir, nil)
	for i := 1; i <= 3; i++ {
		put(t, e, 1, uint64(i), fmt.Sprintf("k%03d", i), "v")
	}
	closeEngine(t, e)

	path := activeSegmentPath(t, dir)
	partial := encodeFrame(nil, 4, bytes.Repeat([]byte("x"), 64))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(partial[:20]); err != nil {
		t.Fatal(err)
	}
	f.Close()

	hooks, _ := failpointHooks()
	hooks.Truncate = func(file *os.File, size int64) error {
		return errInjected
	}
	if _, err := openEngine(Config{Dir: dir}, hooks, nil); CodeOf(err) != CodeStorage {
		t.Fatalf("want STORAGE, got %v", err)
	}

	// The failed open must have released the directory, and a healthy open
	// must then recover normally.
	reopened := open(t, dir, nil)
	defer closeEngine(t, reopened)
	for i := 1; i <= 3; i++ {
		expectGet(t, reopened, fmt.Sprintf("k%03d", i), "v")
	}
	if got := reopened.Stats().TruncatedBytes; got != 20 {
		t.Fatalf("truncated %d bytes, want 20", got)
	}
}
