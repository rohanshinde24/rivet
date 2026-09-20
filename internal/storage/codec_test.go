package storage

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func testConfig() Config {
	return Config{Dir: "unused"}.withDefaults()
}

func clientID(b byte) ClientID {
	var id ClientID
	id[0] = b
	return id
}

// --- command codec -----------------------------------------------------

func TestCommandRoundTrip(t *testing.T) {
	cfg := testConfig()
	cases := []struct {
		name string
		cmd  command
	}{
		{"put", command{Kind: OpPut, ClientID: clientID(7), Sequence: 3, Key: []byte("k"), Value: []byte("v")}},
		{"put empty value", command{Kind: OpPut, ClientID: clientID(1), Sequence: 1, Key: []byte("k"), Value: []byte{}}},
		{"put max key", command{Kind: OpPut, ClientID: clientID(2), Sequence: 9, Key: bytes.Repeat([]byte("k"), cfg.MaxKeyBytes), Value: []byte("v")}},
		{"delete", command{Kind: OpDelete, ClientID: clientID(3), Sequence: 4, Key: []byte("k")}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded := tc.cmd.encode(nil)
			if len(encoded) != tc.cmd.encodedLen() {
				t.Fatalf("encodedLen said %d, encode produced %d", tc.cmd.encodedLen(), len(encoded))
			}
			got, err := decodeCommand(encoded, cfg.MaxKeyBytes, cfg.MaxValueBytes)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got.Kind != tc.cmd.Kind || got.ClientID != tc.cmd.ClientID || got.Sequence != tc.cmd.Sequence {
				t.Fatalf("header mismatch: got %+v", got)
			}
			if !bytes.Equal(got.Key, tc.cmd.Key) {
				t.Fatalf("key mismatch")
			}
			if tc.cmd.Kind == OpPut && !bytes.Equal(got.Value, tc.cmd.Value) {
				t.Fatalf("value mismatch: got %q want %q", got.Value, tc.cmd.Value)
			}
			// Re-encoding an accepted command must be byte-identical.
			if !bytes.Equal(got.encode(nil), encoded) {
				t.Fatalf("re-encode is not stable")
			}
		})
	}
}

func TestCommandDecodeRejection(t *testing.T) {
	cfg := testConfig()
	valid := (&command{Kind: OpPut, ClientID: clientID(1), Sequence: 1, Key: []byte("k"), Value: []byte("v")}).encode(nil)

	mutate := func(f func([]byte) []byte) []byte { return f(append([]byte(nil), valid...)) }

	cases := map[string][]byte{
		"empty":              {},
		"short":              valid[:commandFixedBytes-1],
		"bad schema version": mutate(func(b []byte) []byte { b[0] = 2; return b }),
		"bad kind":           mutate(func(b []byte) []byte { b[1] = 9; return b }),
		"zero client":        mutate(func(b []byte) []byte { copy(b[2:18], make([]byte, 16)); return b }),
		"zero sequence":      mutate(func(b []byte) []byte { copy(b[18:26], make([]byte, 8)); return b }),
		"zero key length":    mutate(func(b []byte) []byte { copy(b[26:30], []byte{0, 0, 0, 0}); return b }),
		"huge key length":    mutate(func(b []byte) []byte { copy(b[26:30], []byte{0xff, 0xff, 0, 0}); return b }),
		"truncated value":    valid[:len(valid)-1],
		"trailing bytes":     append(append([]byte(nil), valid...), 0),
	}

	for name, buf := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeCommand(buf, cfg.MaxKeyBytes, cfg.MaxValueBytes); err == nil {
				t.Fatalf("decoder accepted %s", name)
			} else if CodeOf(err) != CodeCorruption {
				t.Fatalf("want CodeCorruption, got %v", CodeOf(err))
			}
		})
	}
}

// TestCommandDigestGolden pins the digest of a known command. A change here
// means retained test or benchmark data can no longer be interpreted, so it
// requires a format version bump rather than a test edit.
func TestCommandDigestGolden(t *testing.T) {
	cmd := command{Kind: OpPut, ClientID: clientID(1), Sequence: 1, Key: []byte("alpha"), Value: []byte("beta")}
	const want = "4a07e2d966168ca6bfaeb31a89bb56dbb5f749388e599a82f6b960359f062e11"
	if got := func() string { d := cmd.digest(); return hex.EncodeToString(d[:]) }(); got != want {
		t.Fatalf("digest changed: %s", got)
	}
}

// A PUT with an empty value and a DELETE of the same key must not collide:
// the operation kind is what separates them.
func TestCommandDigestSeparatesKinds(t *testing.T) {
	put := command{Kind: OpPut, ClientID: clientID(1), Sequence: 1, Key: []byte("k"), Value: []byte{}}
	del := command{Kind: OpDelete, ClientID: clientID(1), Sequence: 1, Key: []byte("k")}
	if put.digest() == del.digest() {
		t.Fatal("empty PUT and DELETE produce the same digest")
	}
	// Identity fields are deliberately excluded from the digest.
	other := put
	other.ClientID = clientID(2)
	other.Sequence = 99
	if put.digest() != other.digest() {
		t.Fatal("digest depends on client identity")
	}
}

// --- WAL codec ---------------------------------------------------------

func TestSegmentHeaderRoundTrip(t *testing.T) {
	for _, seq := range []uint64{1, 2, 1 << 40} {
		buf := encodeSegmentHeader(seq)
		if len(buf) != segmentHeaderBytes {
			t.Fatalf("header is %d bytes", len(buf))
		}
		got, err := decodeSegmentHeader(buf)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got != seq {
			t.Fatalf("start sequence %d, want %d", got, seq)
		}
	}
}

func TestSegmentHeaderRejection(t *testing.T) {
	base := encodeSegmentHeader(1)
	mutate := func(f func([]byte)) []byte {
		b := append([]byte(nil), base...)
		f(b)
		return b
	}

	cases := map[string][]byte{
		"short":            base[:segmentHeaderBytes-1],
		"bad magic":        mutate(func(b []byte) { b[0] = 'X' }),
		"bad version":      mutate(func(b []byte) { b[4] = 2 }),
		"bad header len":   mutate(func(b []byte) { b[5] = 99 }),
		"reserved nonzero": mutate(func(b []byte) { b[20] = 1 }),
		"bad checksum":     mutate(func(b []byte) { b[28] ^= 0xff }),
	}
	for name, buf := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeSegmentHeader(buf); err == nil {
				t.Fatalf("accepted %s", name)
			}
		})
	}
}

func TestFrameRoundTrip(t *testing.T) {
	payload := []byte("payload bytes")
	buf := encodeFrame(nil, 42, payload)

	seq, got, size, err := decodeFrame(buf, testConfig().maxFramePayload())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if seq != 42 || !bytes.Equal(got, payload) || size != len(buf) {
		t.Fatalf("round trip mismatch: seq=%d size=%d", seq, size)
	}
}

// Every truncation of a frame must report an incomplete frame rather than
// decoding a partial record.
func TestFrameTruncationIsIncomplete(t *testing.T) {
	buf := encodeFrame(nil, 1, []byte("payload"))
	max := testConfig().maxFramePayload()

	for n := 0; n < len(buf); n++ {
		_, _, _, err := decodeFrame(buf[:n], max)
		if !errors.Is(err, errIncompleteFrame) {
			t.Fatalf("truncation to %d bytes returned %v, want errIncompleteFrame", n, err)
		}
	}
}

func TestFrameCorruptionRejected(t *testing.T) {
	max := testConfig().maxFramePayload()
	base := encodeFrame(nil, 1, []byte("payload"))
	mutate := func(f func([]byte)) []byte {
		b := append([]byte(nil), base...)
		f(b)
		return b
	}

	cases := map[string][]byte{
		"bad magic":     mutate(func(b []byte) { b[0] = 'X' }),
		"bad version":   mutate(func(b []byte) { b[4] = 9 }),
		"bad type":      mutate(func(b []byte) { b[5] = 9 }),
		"flags set":     mutate(func(b []byte) { b[6] = 1 }),
		"payload flip":  mutate(func(b []byte) { b[frameHeaderBytes] ^= 0xff }),
		"checksum flip": mutate(func(b []byte) { b[20] ^= 0xff }),
	}
	for name, buf := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, _, err := decodeFrame(buf, max)
			if err == nil || errors.Is(err, errIncompleteFrame) {
				t.Fatalf("accepted %s: %v", name, err)
			}
		})
	}
}

// A declared length above the bound must be rejected before allocation.
func TestFrameDeclaredLengthBound(t *testing.T) {
	buf := encodeFrame(nil, 1, []byte("x"))
	buf[16], buf[17], buf[18], buf[19] = 0xff, 0xff, 0xff, 0x7f

	if _, _, _, err := decodeFrame(buf, 64); err == nil || errors.Is(err, errIncompleteFrame) {
		t.Fatalf("oversized declared payload accepted: %v", err)
	}
}

func TestSegmentNameRoundTrip(t *testing.T) {
	active := activeSegmentName(7)
	got, err := parseSegmentName(active)
	if err != nil || got.Sealed || got.StartSeq != 7 {
		t.Fatalf("active parse: %+v %v", got, err)
	}

	sealed := sealedSegmentName(7, 19)
	got, err = parseSegmentName(sealed)
	if err != nil || !got.Sealed || got.StartSeq != 7 || got.EndSeq != 19 {
		t.Fatalf("sealed parse: %+v %v", got, err)
	}

	for _, bad := range []string{
		"wal-1.active", "wal-x.sealed", "notawal", "wal-.active",
		sealedSegmentName(19, 7), strings.Replace(sealed, "-", "_", 1),
	} {
		if _, err := parseSegmentName(bad); err == nil {
			t.Fatalf("accepted impossible name %q", bad)
		}
	}
}

// --- snapshot codec ----------------------------------------------------

func buildState(t *testing.T, n int) *stateMachine {
	t.Helper()
	s := newStateMachine()
	for i := 0; i < n; i++ {
		cmd := &command{
			Kind:     OpPut,
			ClientID: clientID(byte(i%7 + 1)),
			Sequence: uint64(i/7 + 1),
			Key:      []byte{byte('a' + i%26), byte(i)},
			Value:    bytes.Repeat([]byte{byte(i)}, i%9),
		}
		if _, err := s.apply(cmd, uint64(i+1), DefaultMaxKeys); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
	return s
}

func TestSnapshotRoundTripIsDeterministic(t *testing.T) {
	cfg := testConfig()
	s := buildState(t, 40)

	a := encodeSnapshot(s)
	b := encodeSnapshot(s.clone())
	if !bytes.Equal(a, b) {
		t.Fatal("snapshot encoding is not deterministic")
	}

	got, err := decodeSnapshot(a, cfg)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.digest() != s.digest() {
		t.Fatal("decoded state digest differs")
	}
	if got.applied != s.applied {
		t.Fatalf("applied %d, want %d", got.applied, s.applied)
	}
}

func TestSnapshotRejection(t *testing.T) {
	cfg := testConfig()
	base := encodeSnapshot(buildState(t, 5))
	mutate := func(f func([]byte)) []byte {
		b := append([]byte(nil), base...)
		f(b)
		return b
	}

	cases := map[string][]byte{
		"short":            base[:snapshotHeaderBytes-1],
		"bad magic":        mutate(func(b []byte) { b[0] = 'X' }),
		"bad version":      mutate(func(b []byte) { b[4] = 9 }),
		"reserved nonzero": mutate(func(b []byte) { b[5] = 1 }),
		"payload flip":     mutate(func(b []byte) { b[snapshotHeaderBytes] ^= 0xff }),
		"checksum flip":    mutate(func(b []byte) { b[40] ^= 0xff }),
		"truncated":        base[:len(base)-1],
		"extended":         append(append([]byte(nil), base...), 0),
	}
	for name, buf := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeSnapshot(buf, cfg); err == nil {
				t.Fatalf("accepted %s", name)
			}
		})
	}
}

// --- state machine and session rules -----------------------------------

func TestApplySemantics(t *testing.T) {
	s := newStateMachine()
	c1 := clientID(1)

	put := &command{Kind: OpPut, ClientID: c1, Sequence: 1, Key: []byte("k"), Value: []byte("v1")}
	if _, err := s.apply(put, 1, DefaultMaxKeys); err != nil {
		t.Fatalf("put: %v", err)
	}
	if v, ok := s.get([]byte("k")); !ok || string(v) != "v1" {
		t.Fatalf("get after put: %q %v", v, ok)
	}

	replace := &command{Kind: OpPut, ClientID: c1, Sequence: 2, Key: []byte("k"), Value: []byte("v2")}
	if _, err := s.apply(replace, 2, DefaultMaxKeys); err != nil {
		t.Fatalf("replace: %v", err)
	}
	if v, _ := s.get([]byte("k")); string(v) != "v2" {
		t.Fatalf("replace did not take effect: %q", v)
	}

	del := &command{Kind: OpDelete, ClientID: c1, Sequence: 3, Key: []byte("k")}
	res, err := s.apply(del, 3, DefaultMaxKeys)
	if err != nil || !res.Existed {
		t.Fatalf("delete present: %+v %v", res, err)
	}
	delAgain := &command{Kind: OpDelete, ClientID: c1, Sequence: 4, Key: []byte("k")}
	res, err = s.apply(delAgain, 4, DefaultMaxKeys)
	if err != nil || res.Existed {
		t.Fatalf("delete absent must report existed=false: %+v %v", res, err)
	}

	// Out-of-order application is an invariant violation.
	if _, err := s.apply(&command{Kind: OpPut, ClientID: c1, Sequence: 5, Key: []byte("x")}, 99, DefaultMaxKeys); err == nil {
		t.Fatal("applied a command at the wrong global sequence")
	}
}

// Returned values must never alias engine-owned memory.
func TestGetCopiesValue(t *testing.T) {
	s := newStateMachine()
	value := []byte("original")
	cmd := &command{Kind: OpPut, ClientID: clientID(1), Sequence: 1, Key: []byte("k"), Value: value}
	if _, err := s.apply(cmd, 1, DefaultMaxKeys); err != nil {
		t.Fatal(err)
	}
	value[0] = 'X' // mutate the caller's buffer after the write

	got, _ := s.get([]byte("k"))
	if string(got) != "original" {
		t.Fatalf("engine state aliased the caller buffer: %q", got)
	}
	got[0] = 'Y'
	again, _ := s.get([]byte("k"))
	if string(again) != "original" {
		t.Fatalf("returned value aliased engine state: %q", again)
	}
}

// TestSessionRules covers every branch of the duplicate-write rules.
func TestSessionRules(t *testing.T) {
	s := newStateMachine()
	c1 := clientID(1)
	put := func(seq uint64, value string) *command {
		return &command{Kind: OpPut, ClientID: c1, Sequence: seq, Key: []byte("k"), Value: []byte(value)}
	}

	// A new client must start at 1.
	if _, _, err := s.admit(put(2, "v"), 10); CodeOf(err) != CodeSequenceGap {
		t.Fatalf("new client at sequence 2: want SEQUENCE_GAP, got %v", err)
	}

	first := put(1, "v")
	if d, _, err := s.admit(first, 10); err != nil || d != admitExecute {
		t.Fatalf("first write: %v %v", d, err)
	}
	if _, err := s.apply(first, 1, DefaultMaxKeys); err != nil {
		t.Fatal(err)
	}

	// Matching retry of the last sequence returns the stored result.
	d, stored, err := s.admit(put(1, "v"), 10)
	if err != nil || d != admitDuplicate || !stored.Duplicate || stored.AppliedSequence != 1 {
		t.Fatalf("matching retry: %v %+v %v", d, stored, err)
	}

	// Same sequence with different content is a reuse error.
	if _, _, err := s.admit(put(1, "different"), 10); CodeOf(err) != CodeRequestIDReuse {
		t.Fatalf("conflicting retry: want REQUEST_ID_REUSE, got %v", err)
	}

	// Next sequence executes.
	second := put(2, "v2")
	if d, _, err := s.admit(second, 10); err != nil || d != admitExecute {
		t.Fatalf("second write: %v %v", d, err)
	}
	if _, err := s.apply(second, 2, DefaultMaxKeys); err != nil {
		t.Fatal(err)
	}

	// An older sequence is never executed again.
	if _, _, err := s.admit(put(1, "v"), 10); CodeOf(err) != CodeDuplicateResultExpired {
		t.Fatalf("expired retry: want DUPLICATE_RESULT_EXPIRED, got %v", err)
	}
	// A gap is rejected.
	if _, _, err := s.admit(put(4, "v"), 10); CodeOf(err) != CodeSequenceGap {
		t.Fatalf("gap: want SEQUENCE_GAP, got %v", err)
	}
}

// A new client beyond capacity is rejected before execution; existing clients
// keep working.
func TestSessionCapacity(t *testing.T) {
	s := newStateMachine()
	const capacity = 3

	for i := 1; i <= capacity; i++ {
		cmd := &command{Kind: OpPut, ClientID: clientID(byte(i)), Sequence: 1, Key: []byte("k"), Value: []byte("v")}
		if _, _, err := s.admit(cmd, capacity); err != nil {
			t.Fatalf("client %d admit: %v", i, err)
		}
		if _, err := s.apply(cmd, uint64(i), DefaultMaxKeys); err != nil {
			t.Fatal(err)
		}
	}

	overflow := &command{Kind: OpPut, ClientID: clientID(99), Sequence: 1, Key: []byte("k"), Value: []byte("v")}
	if _, _, err := s.admit(overflow, capacity); CodeOf(err) != CodeResourceExhausted {
		t.Fatalf("overflow client: want RESOURCE_EXHAUSTED, got %v", err)
	}

	existing := &command{Kind: OpPut, ClientID: clientID(1), Sequence: 2, Key: []byte("k"), Value: []byte("v")}
	if _, _, err := s.admit(existing, capacity); err != nil {
		t.Fatalf("existing client rejected at capacity: %v", err)
	}
}

// Replay of a durable command that violates the session rules is corruption,
// never a normal rejection.
func TestReplaySessionViolationIsCorruption(t *testing.T) {
	s := newStateMachine()
	first := &command{Kind: OpPut, ClientID: clientID(1), Sequence: 1, Key: []byte("k"), Value: []byte("v")}
	if _, err := s.apply(first, 1, DefaultMaxKeys); err != nil {
		t.Fatal(err)
	}
	skipped := &command{Kind: OpPut, ClientID: clientID(1), Sequence: 5, Key: []byte("k"), Value: []byte("v")}
	if _, err := s.apply(skipped, 2, DefaultMaxKeys); CodeOf(err) != CodeCorruption {
		t.Fatalf("want CodeCorruption, got %v", err)
	}
}
