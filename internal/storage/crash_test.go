package storage

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
)

// Environment variables that turn this test binary into the crashing child.
const (
	crashEnvDir   = "RIVET_CRASH_DIR"
	crashEnvPhase = "RIVET_CRASH_PHASE"
	crashExitCode = 7
)

const crashDurableWrites = 5

// TestSubprocessCrashRecovery kills a real process at each write-path phase
// and checks what the surviving directory means.
//
// In-process error injection cannot cover this: only a killed process loses
// applied state that was never synchronized, which is exactly the boundary
// between a write that is durable and one that is not.
func TestSubprocessCrashRecovery(t *testing.T) {
	if dir := os.Getenv(crashEnvDir); dir != "" {
		runCrashChild(dir, os.Getenv(crashEnvPhase))
		return
	}

	cases := []struct {
		phase string
		// durable says whether the sixth write must be present after
		// recovery. When false either outcome is allowed, because the frame
		// had been written but not yet synchronized.
		durable bool
	}{
		{phaseBeforeAppend, false},
		{phaseAfterWrite, false},
		{phaseBeforeSync, false},
		{phaseAfterSync, true},
		{phaseBeforeApply, true},
		{phaseAfterApply, true},
		{phaseBeforeReply, true},
	}

	for _, tc := range cases {
		t.Run(tc.phase, func(t *testing.T) {
			dir := t.TempDir()

			cmd := exec.Command(os.Args[0], "-test.run=^TestSubprocessCrashRecovery$")
			cmd.Env = append(os.Environ(), crashEnvDir+"="+dir, crashEnvPhase+"="+tc.phase)
			var out bytes.Buffer
			cmd.Stdout, cmd.Stderr = &out, &out

			err := cmd.Run()
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != crashExitCode {
				t.Fatalf("child did not crash at %s: err=%v output=%s", tc.phase, err, out.String())
			}

			// The directory must reopen, and every acknowledged write must
			// still be there.
			e := open(t, dir, nil)
			defer closeEngine(t, e)

			for i := 1; i <= crashDurableWrites; i++ {
				expectGet(t, e, fmt.Sprintf("k%d", i), "v")
			}

			_, found, _, err := e.Get(context.Background(), []byte("k6"))
			if err != nil {
				t.Fatalf("read the crashed write: %v", err)
			}
			if tc.durable && !found {
				t.Fatalf("write was synchronized before the crash at %s but is missing", tc.phase)
			}
			if !tc.durable && found {
				t.Logf("write at %s survived: allowed, the frame reached the file before the crash", tc.phase)
			}

			// The recovered engine must accept new writes immediately.
			applied := e.Stats().AppliedSequence
			res := put(t, e, 9, 1, "after-crash", "ok")
			if res.AppliedSequence != applied+1 {
				t.Fatalf("post-recovery write got sequence %d, want %d", res.AppliedSequence, applied+1)
			}
		})
	}
}

// runCrashChild writes a few durable records and then dies inside the named
// phase of the next write, with no close, no flush, and no deferred cleanup.
func runCrashChild(dir, phase string) {
	armed := false
	hook := func(p string) error {
		if armed && p == phase {
			os.Exit(crashExitCode)
		}
		return nil
	}

	e, err := openEngine(Config{Dir: dir}, defaultHooks(), hook)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child open: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	for i := 1; i <= crashDurableWrites; i++ {
		if _, err := e.Put(ctx, RequestIdentity{clientID(1), uint64(i)},
			[]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			fmt.Fprintf(os.Stderr, "child write %d: %v\n", i, err)
			os.Exit(1)
		}
	}

	armed = true
	e.Put(ctx, RequestIdentity{clientID(1), crashDurableWrites + 1}, []byte("k6"), []byte("v"))

	fmt.Fprintf(os.Stderr, "child reached the end without crashing at %s\n", phase)
	os.Exit(1)
}
