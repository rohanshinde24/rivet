package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	snapshotMagic         = "RVTS"
	snapshotFormatVersion = 1
	snapshotHeaderBytes   = 44

	snapshotTmpSuffix   = ".tmp"
	snapshotFinalSuffix = ".snap"
	snapshotPrefix      = "snapshot-"
)

func snapshotTmpName(seq uint64) string {
	return fmt.Sprintf("%s%0*d%s", snapshotPrefix, sequenceNameDigits, seq, snapshotTmpSuffix)
}

func snapshotFinalName(seq uint64) string {
	return fmt.Sprintf("%s%0*d%s", snapshotPrefix, sequenceNameDigits, seq, snapshotFinalSuffix)
}

// parseSnapshotName returns the applied sequence a snapshot file claims and
// whether it is finalized.
func parseSnapshotName(name string) (seq uint64, final bool, err error) {
	const op = "parseSnapshotName"

	if !strings.HasPrefix(name, snapshotPrefix) {
		return 0, false, errorf(CodeCorruption, op, "name %q lacks the snapshot prefix", name)
	}
	body := strings.TrimPrefix(name, snapshotPrefix)

	switch {
	case strings.HasSuffix(body, snapshotFinalSuffix):
		body, final = strings.TrimSuffix(body, snapshotFinalSuffix), true
	case strings.HasSuffix(body, snapshotTmpSuffix):
		body, final = strings.TrimSuffix(body, snapshotTmpSuffix), false
	default:
		return 0, false, errorf(CodeCorruption, op, "name %q has no known snapshot suffix", name)
	}

	if len(body) != sequenceNameDigits {
		return 0, false, errorf(CodeCorruption, op, "sequence field %q is not %d digits", body, sequenceNameDigits)
	}
	seq, perr := strconv.ParseUint(body, 10, 64)
	if perr != nil {
		return 0, false, wrapError(CodeCorruption, op, "bad snapshot sequence", perr)
	}
	return seq, final, nil
}

// encodeSnapshot serializes state into the version-1 container.
//
// Entry order is fully determined: keys lexicographically, sessions by raw
// client ID. Two snapshots of equal state are byte-identical, which is what
// lets tests compare files rather than parsed structures.
func encodeSnapshot(s *stateMachine) []byte {
	var scratch [8]byte
	payload := make([]byte, 0, 1024)

	keys := s.sortedKeys()
	for _, k := range keys {
		v := s.kv[k]
		binary.LittleEndian.PutUint32(scratch[:4], uint32(len(k)))
		payload = append(payload, scratch[:4]...)
		payload = append(payload, k...)
		binary.LittleEndian.PutUint32(scratch[:4], uint32(len(v)))
		payload = append(payload, scratch[:4]...)
		payload = append(payload, v...)
	}

	clients := s.sortedClients()
	for _, id := range clients {
		e := s.sessions[id]
		payload = append(payload, id[:]...)
		binary.LittleEndian.PutUint64(scratch[:], e.LastSequence)
		payload = append(payload, scratch[:]...)
		payload = append(payload, e.Digest[:]...)
		existed := byte(0)
		if e.Existed {
			existed = 1
		}
		payload = append(payload, byte(e.ResultKind), existed)
		binary.LittleEndian.PutUint64(scratch[:], e.AppliedSequence)
		payload = append(payload, scratch[:]...)
	}

	out := make([]byte, snapshotHeaderBytes, snapshotHeaderBytes+len(payload))
	copy(out[0:4], snapshotMagic)
	out[4] = snapshotFormatVersion
	// out[5:8] reserved, zero.
	binary.LittleEndian.PutUint64(out[8:16], s.applied)
	binary.LittleEndian.PutUint64(out[16:24], uint64(len(keys)))
	binary.LittleEndian.PutUint64(out[24:32], uint64(len(clients)))
	binary.LittleEndian.PutUint64(out[32:40], uint64(len(payload)))

	sum := crc32.Checksum(out[0:40], crc32cTable)
	sum = crc32.Update(sum, crc32cTable, payload)
	binary.LittleEndian.PutUint32(out[40:44], sum)

	return append(out, payload...)
}

// decodeSnapshot validates and rebuilds state from a container. Every declared
// count and length is checked against configuration before allocation, and the
// payload must be consumed exactly.
func decodeSnapshot(buf []byte, cfg Config) (*stateMachine, error) {
	const op = "decodeSnapshot"

	if len(buf) < snapshotHeaderBytes {
		return nil, errorf(CodeCorruption, op, "file is %d bytes, below header size %d", len(buf), snapshotHeaderBytes)
	}
	if string(buf[0:4]) != snapshotMagic {
		return nil, newError(CodeCorruption, op, "bad snapshot magic")
	}
	if buf[4] != snapshotFormatVersion {
		return nil, errorf(CodeCorruption, op, "unknown snapshot format version %d", buf[4])
	}
	for _, b := range buf[5:8] {
		if b != 0 {
			return nil, newError(CodeCorruption, op, "reserved header bytes are not zero")
		}
	}

	applied := binary.LittleEndian.Uint64(buf[8:16])
	kvCount := binary.LittleEndian.Uint64(buf[16:24])
	sessionCount := binary.LittleEndian.Uint64(buf[24:32])
	payloadLen := binary.LittleEndian.Uint64(buf[32:40])

	if kvCount > uint64(cfg.MaxKeys) {
		return nil, errorf(CodeCorruption, op, "snapshot holds %d keys, above the configured maximum %d", kvCount, cfg.MaxKeys)
	}
	if sessionCount > uint64(cfg.MaxSessions) {
		return nil, errorf(CodeCorruption, op, "snapshot holds %d sessions, above the configured maximum %d", sessionCount, cfg.MaxSessions)
	}
	if payloadLen > uint64(cfg.maxSnapshotPayload()) {
		return nil, errorf(CodeCorruption, op, "declared payload %d exceeds bound %d", payloadLen, cfg.maxSnapshotPayload())
	}
	if uint64(len(buf)-snapshotHeaderBytes) != payloadLen {
		return nil, errorf(CodeCorruption, op, "file holds %d payload bytes, header declares %d",
			len(buf)-snapshotHeaderBytes, payloadLen)
	}

	payload := buf[snapshotHeaderBytes:]
	want := binary.LittleEndian.Uint32(buf[40:44])
	sum := crc32.Checksum(buf[0:40], crc32cTable)
	sum = crc32.Update(sum, crc32cTable, payload)
	if sum != want {
		return nil, errorf(CodeCorruption, op, "snapshot checksum %08x does not match %08x", sum, want)
	}

	s := newStateMachine()
	s.applied = applied

	var prevKey []byte
	for i := uint64(0); i < kvCount; i++ {
		if len(payload) < 4 {
			return nil, errorf(CodeCorruption, op, "payload truncated at key %d", i)
		}
		keyLen := int(binary.LittleEndian.Uint32(payload[:4]))
		payload = payload[4:]
		if keyLen < 1 || keyLen > cfg.MaxKeyBytes || len(payload) < keyLen {
			return nil, errorf(CodeCorruption, op, "key %d has invalid length %d", i, keyLen)
		}
		key := payload[:keyLen]
		payload = payload[keyLen:]

		if prevKey != nil && string(key) <= string(prevKey) {
			return nil, errorf(CodeCorruption, op, "key %d breaks the required ordering", i)
		}
		prevKey = key

		if len(payload) < 4 {
			return nil, errorf(CodeCorruption, op, "payload truncated before value %d", i)
		}
		valueLen := int(binary.LittleEndian.Uint32(payload[:4]))
		payload = payload[4:]
		if valueLen < 0 || valueLen > cfg.MaxValueBytes || len(payload) < valueLen {
			return nil, errorf(CodeCorruption, op, "value %d has invalid length %d", i, valueLen)
		}
		s.kv[string(key)] = append([]byte(nil), payload[:valueLen]...)
		payload = payload[valueLen:]
	}

	var prevID ClientID
	for i := uint64(0); i < sessionCount; i++ {
		if len(payload) < sessionEntryBytes {
			return nil, errorf(CodeCorruption, op, "payload truncated at session %d", i)
		}
		var id ClientID
		copy(id[:], payload[0:16])
		if id.IsZero() {
			return nil, errorf(CodeCorruption, op, "session %d has a zero client ID", i)
		}
		if i > 0 && string(id[:]) <= string(prevID[:]) {
			return nil, errorf(CodeCorruption, op, "session %d breaks the required ordering", i)
		}
		prevID = id

		var e sessionEntry
		e.LastSequence = binary.LittleEndian.Uint64(payload[16:24])
		copy(e.Digest[:], payload[24:56])
		e.ResultKind = OpKind(payload[56])
		switch payload[57] {
		case 0:
			e.Existed = false
		case 1:
			e.Existed = true
		default:
			return nil, errorf(CodeCorruption, op, "session %d has a non-boolean existed byte", i)
		}
		e.AppliedSequence = binary.LittleEndian.Uint64(payload[58:66])

		if e.LastSequence == 0 {
			return nil, errorf(CodeCorruption, op, "session %d has a zero high-water sequence", i)
		}
		if e.ResultKind != OpPut && e.ResultKind != OpDelete {
			return nil, errorf(CodeCorruption, op, "session %d has unknown result kind %d", i, e.ResultKind)
		}
		if e.AppliedSequence > applied {
			return nil, errorf(CodeCorruption, op, "session %d was applied at %d, after the snapshot sequence %d",
				i, e.AppliedSequence, applied)
		}
		s.sessions[id] = e
		payload = payload[sessionEntryBytes:]
	}

	if len(payload) != 0 {
		return nil, errorf(CodeCorruption, op, "snapshot has %d trailing payload bytes", len(payload))
	}
	return s, nil
}

// publishSnapshot writes a temporary file, synchronizes it, validates it
// through the normal decode path, renames it atomically, and synchronizes the
// parent directory.
//
// Compaction is deliberately not done here. Only the event loop may delete
// segments, and only after this function has returned successfully.
func publishSnapshot(dir string, snap *stateMachine, cfg Config, hooks *ioHooks) (path string, ambiguous bool, err error) {
	const op = "publishSnapshot"

	seq := snap.applied
	tmpPath := filepath.Join(dir, snapshotTmpName(seq))
	finalPath := filepath.Join(dir, snapshotFinalName(seq))

	if _, err := os.Stat(finalPath); err == nil {
		return "", false, errorf(CodeCorruption, op, "snapshot for sequence %d already exists", seq)
	} else if !os.IsNotExist(err) {
		return "", false, wrapError(CodeStorage, op, "stat finalized snapshot", err)
	}

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return "", false, wrapError(CodeStorage, op, "create temporary snapshot", err)
	}

	encoded := encodeSnapshot(snap)
	if _, err := hooks.writeFull(f, encoded); err != nil {
		f.Close()
		return "", false, wrapError(CodeStorage, op, "write temporary snapshot", err)
	}
	if err := hooks.Sync(f); err != nil {
		f.Close()
		return "", false, wrapError(CodeStorage, op, "sync temporary snapshot", err)
	}
	if err := f.Close(); err != nil {
		return "", false, wrapError(CodeStorage, op, "close temporary snapshot", err)
	}

	// Validate what actually reached the disk, not the buffer we still hold.
	readBack, err := os.ReadFile(tmpPath)
	if err != nil {
		return "", false, wrapError(CodeStorage, op, "read back temporary snapshot", err)
	}
	decoded, err := decodeSnapshot(readBack, cfg)
	if err != nil {
		return "", false, wrapError(CodeCorruption, op, "temporary snapshot failed validation", err)
	}
	if decoded.applied != seq || decoded.digest() != snap.digest() {
		return "", false, errorf(CodeCorruption, op, "temporary snapshot does not round trip at sequence %d", seq)
	}

	if err := hooks.Rename(tmpPath, finalPath); err != nil {
		return "", true, wrapError(CodeStorage, op, "rename snapshot into place", err)
	}
	if err := hooks.SyncDir(dir); err != nil {
		return "", true, wrapError(CodeStorage, op, "sync directory after snapshot rename", err)
	}
	return finalPath, false, nil
}

// loadSnapshot reads and validates a finalized snapshot file.
func loadSnapshot(path string, cfg Config) (*stateMachine, error) {
	const op = "loadSnapshot"

	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, wrapError(CodeStorage, op, "read snapshot", err)
	}
	s, err := decodeSnapshot(buf, cfg)
	if err != nil {
		return nil, err
	}
	return s, nil
}
