package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"testing"
	"time"
)

// Close is only complete when the goroutines the engine started are gone. A
// leaked event loop or snapshot worker would keep a file handle and the
// directory lock alive, so a process that opened many engines over its
// lifetime would eventually fail to open one at all.
//
// Both helpers below poll with a deadline rather than sleeping a fixed time:
// a sleep long enough to be reliable is long enough to hide a slow leak.

const goroutineSettleDeadline = 5 * time.Second

// settledGoroutines returns a goroutine count that has stopped moving, so a
// straggler from an earlier test cannot inflate the baseline.
func settledGoroutines(t *testing.T) int {
	t.Helper()

	deadline := time.Now().Add(goroutineSettleDeadline)
	last, stable := -1, 0
	for {
		runtime.Gosched()
		got := runtime.NumGoroutine()
		if got == last {
			if stable++; stable >= 3 {
				return got
			}
		} else {
			last, stable = got, 0
		}
		if time.Now().After(deadline) {
			return last
		}
		time.Sleep(time.Millisecond)
	}
}

// expectQuiescent waits for the goroutine count to fall back to the baseline,
// and dumps the surviving stacks if it does not.
func expectQuiescent(t *testing.T, baseline int) {
	t.Helper()

	deadline := time.Now().Add(goroutineSettleDeadline)
	for {
		runtime.Gosched()
		got := runtime.NumGoroutine()
		if got <= baseline {
			return
		}
		if time.Now().After(deadline) {
			var stacks bytes.Buffer
			if p := pprof.Lookup("goroutine"); p != nil {
				p.WriteTo(&stacks, 1)
			}
			t.Fatalf("%d goroutines still running %s after Close, baseline is %d\n\n%s",
				got, goroutineSettleDeadline, baseline, stacks.String())
		}
		time.Sleep(time.Millisecond)
	}
}

// A full workload, repeated, must leave nothing behind. Repetition is what
// catches a leak of one goroutine per open, which a single cycle would show
// as noise.
func TestNoGoroutineLeakAfterClose(t *testing.T) {
	baseline := settledGoroutines(t)

	for cycle := 0; cycle < 5; cycle++ {
		dir := t.TempDir()
		e := open(t, dir, func(c *Config) { c.SegmentTargetBytes = 2048 })

		var wg sync.WaitGroup
		for client := 1; client <= 4; client++ {
			wg.Add(1)
			go func(client int) {
				defer wg.Done()
				for i := 1; i <= 10; i++ {
					put(t, e, byte(client), uint64(i), "k", "v")
					expectGet(t, e, "k", "v")
				}
			}(client)
		}
		wg.Wait()

		if _, _, err := e.CreateSnapshot(context.Background()); err != nil {
			t.Fatalf("cycle %d snapshot: %v", cycle, err)
		}
		closeEngine(t, e)
		expectQuiescent(t, baseline)
	}
}

// A faulted engine still has to shut down cleanly. Its loop is refusing work
// rather than exiting, so Close is the only thing that stops it.
func TestNoGoroutineLeakAfterFault(t *testing.T) {
	baseline := settledGoroutines(t)

	hooks, real := failpointHooks()
	var failSync bool
	hooks.Sync = func(f *os.File) error {
		if failSync {
			return errors.New("injected sync failure")
		}
		return real.Sync(f)
	}

	e, err := openEngine(Config{Dir: t.TempDir()}, hooks, nil)
	if err != nil {
		t.Fatal(err)
	}
	put(t, e, 1, 1, "k", "v")

	failSync = true
	if _, err := e.Put(context.Background(), RequestIdentity{clientID(1), 2},
		[]byte("k2"), []byte("v")); CodeOf(err) != CodeStorage {
		t.Fatalf("want STORAGE, got %v", err)
	}
	if s := e.Stats(); s.State != "Faulted" {
		t.Fatalf("state %s, want Faulted", s.State)
	}

	failSync = false
	if err := e.Close(context.Background()); err != nil {
		t.Fatalf("close after fault: %v", err)
	}
	expectQuiescent(t, baseline)
}

// Closing while a snapshot is still publishing must wait for that worker, not
// abandon it partway through a rename.
func TestNoGoroutineLeakWithSnapshotInFlight(t *testing.T) {
	baseline := settledGoroutines(t)

	dir := t.TempDir()
	e := open(t, dir, func(c *Config) { c.SegmentTargetBytes = 2048 })
	for i := 1; i <= 200; i++ {
		put(t, e, 1, uint64(i), "k", "v")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Either outcome is fine: the snapshot completes, or admission has
		// already stopped. What must not happen is a surviving worker.
		if _, _, err := e.CreateSnapshot(context.Background()); err != nil && CodeOf(err) != CodeClosed {
			t.Errorf("snapshot: %v", err)
		}
	}()

	closeEngine(t, e)
	wg.Wait()
	expectQuiescent(t, baseline)

	// Whatever the race decided, the directory must still open.
	reopened := open(t, dir, nil)
	defer closeEngine(t, reopened)
	if got := reopened.Stats().AppliedSequence; got != 200 {
		t.Fatalf("applied sequence %d, want 200", got)
	}
}

// A failed Open must not leave the lock or any goroutine behind, or the next
// attempt would be refused for a reason that has nothing to do with the data.
func TestNoGoroutineLeakAfterFailedOpen(t *testing.T) {
	baseline := settledGoroutines(t)

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

	for i := 0; i < 3; i++ {
		if _, err := Open(Config{Dir: dir}); CodeOf(err) != CodeCorruption {
			t.Fatalf("attempt %d: want CORRUPTION, got %v", i, err)
		}
	}
	expectQuiescent(t, baseline)
}
