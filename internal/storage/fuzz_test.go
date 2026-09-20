package storage

import (
	"bytes"
	"testing"
)

// FuzzDecodeCommand asserts two properties over arbitrary bytes: the decoder
// never panics, and anything it accepts re-encodes to exactly the input. The
// second property is what keeps replay deterministic.
func FuzzDecodeCommand(f *testing.F) {
	cfg := testConfig()
	f.Add((&command{Kind: OpPut, ClientID: clientID(1), Sequence: 1, Key: []byte("k"), Value: []byte("v")}).encode(nil))
	f.Add((&command{Kind: OpDelete, ClientID: clientID(2), Sequence: 9, Key: []byte("key")}).encode(nil))
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xff}, 64))

	f.Fuzz(func(t *testing.T, data []byte) {
		cmd, err := decodeCommand(data, cfg.MaxKeyBytes, cfg.MaxValueBytes)
		if err != nil {
			return
		}
		if got := cmd.encode(nil); !bytes.Equal(got, data) {
			t.Fatalf("accepted payload is not canonical: re-encode produced %d bytes for %d", len(got), len(data))
		}
		if len(cmd.Key) == 0 || len(cmd.Key) > cfg.MaxKeyBytes {
			t.Fatalf("accepted key of length %d", len(cmd.Key))
		}
		if len(cmd.Value) > cfg.MaxValueBytes {
			t.Fatalf("accepted value of length %d", len(cmd.Value))
		}
	})
}

func FuzzDecodeFrame(f *testing.F) {
	max := testConfig().maxFramePayload()
	f.Add(encodeFrame(nil, 1, []byte("payload")))
	f.Add(encodeFrame(nil, 1<<40, nil))
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0}, frameHeaderBytes))

	f.Fuzz(func(t *testing.T, data []byte) {
		seq, payload, size, err := decodeFrame(data, max)
		if err != nil {
			return
		}
		if size > len(data) {
			t.Fatalf("reported size %d beyond the %d bytes given", size, len(data))
		}
		if len(payload) > max {
			t.Fatalf("accepted payload of %d bytes, bound is %d", len(payload), max)
		}
		if got := encodeFrame(nil, seq, payload); !bytes.Equal(got, data[:size]) {
			t.Fatal("accepted frame is not canonical")
		}
	})
}

func FuzzDecodeSnapshot(f *testing.F) {
	cfg := testConfig()
	f.Add(encodeSnapshot(newStateMachine()))
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xab}, snapshotHeaderBytes+8))

	f.Fuzz(func(t *testing.T, data []byte) {
		s, err := decodeSnapshot(data, cfg)
		if err != nil {
			return
		}
		if got := encodeSnapshot(s); !bytes.Equal(got, data) {
			t.Fatal("accepted snapshot is not canonical")
		}
		if len(s.kv) > cfg.MaxKeys || len(s.sessions) > cfg.MaxSessions {
			t.Fatal("accepted a snapshot beyond configured bounds")
		}
	})
}

// FuzzRecoveryTruncation checks that no prefix of a valid segment ever yields
// a partially applied command: recovery either rejects the file or applies a
// contiguous prefix.
func FuzzRecoveryTruncation(f *testing.F) {
	cfg := testConfig()

	segment := encodeSegmentHeader(1)
	for seq := uint64(1); seq <= 4; seq++ {
		cmd := &command{Kind: OpPut, ClientID: clientID(1), Sequence: seq,
			Key: []byte{byte('a' + seq)}, Value: []byte("value")}
		segment = encodeFrame(segment, seq, cmd.encode(nil))
	}
	for _, n := range []int{0, 10, segmentHeaderBytes, len(segment) / 2, len(segment) - 1, len(segment)} {
		f.Add(uint16(n))
	}

	f.Fuzz(func(t *testing.T, cut uint16) {
		buf := segment
		if int(cut) < len(buf) {
			buf = buf[:cut]
		}
		if len(buf) < segmentHeaderBytes {
			return
		}
		if _, err := decodeSegmentHeader(buf); err != nil {
			return
		}

		state := newStateMachine()
		offset := segmentHeaderBytes
		next := uint64(1)
		for offset < len(buf) {
			seq, payload, size, err := decodeFrame(buf[offset:], cfg.maxFramePayload())
			if err != nil {
				break // a torn tail stops replay; it never applies a partial command
			}
			if seq != next {
				t.Fatalf("sequence %d where %d was required", seq, next)
			}
			cmd, err := decodeCommand(payload, cfg.MaxKeyBytes, cfg.MaxValueBytes)
			if err != nil {
				t.Fatalf("checksum-valid frame held an undecodable command: %v", err)
			}
			if _, err := state.apply(cmd, seq, cfg.MaxKeys); err != nil {
				t.Fatalf("apply at %d: %v", seq, err)
			}
			next, offset = seq+1, offset+size
		}
		if state.applied != next-1 {
			t.Fatalf("applied %d but the accepted prefix ends at %d", state.applied, next-1)
		}
	})
}
