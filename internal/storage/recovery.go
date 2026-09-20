package storage

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
)

// recoveryResult is everything Open needs to resume writing after rebuilding
// state from durable bytes.
type recoveryResult struct {
	state *stateMachine

	// activeName is the existing active segment to reopen, empty when a new
	// one must be created.
	activeName   string
	activeStart  uint64
	appendOffset int64
	lastSequence uint64

	snapshotSequence uint64
	replayedCommands int64
	truncatedBytes   int64
}

// runRecovery rebuilds engine state from the durable contents of dir.
//
// The whole function has one bias: it would rather refuse to open than open
// onto a state it cannot prove is a contiguous prefix of acknowledged
// history. The single exception is a torn record at the physical end of the
// newest active segment, which is the only damage a crash between write and
// sync can legitimately produce.
func runRecovery(dir string, cfg Config, hooks *ioHooks) (*recoveryResult, error) {
	const op = "recovery"

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, wrapError(CodeStorage, op, "read data directory", err)
	}

	var (
		finalSnapshots []uint64
		segments       []segmentName
	)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || name == lockFileName {
			continue
		}
		switch {
		case hasSnapshotPrefix(name):
			seq, final, perr := parseSnapshotName(name)
			if perr != nil {
				return nil, perr
			}
			if final {
				finalSnapshots = append(finalSnapshots, seq)
			} else {
				// Temporary snapshots are never recovery sources. They are
				// recorded and left in place for diagnosis; cleanup is best
				// effort and never precedes a safe recovery path.
				cfg.emit(Event{Name: EventSnapshotTempIgnore, Snapshot: name, Sequence: seq})
			}
		case hasSegmentPrefix(name):
			seg, perr := parseSegmentName(name)
			if perr != nil {
				return nil, perr
			}
			segments = append(segments, seg)
		default:
			cfg.emit(Event{Name: EventUnknownFile, Detail: name})
		}
	}

	// The highest finalized snapshot is the only candidate. An
	// older one cannot be silently substituted, because the WAL prefix it
	// needs may already have been compacted away.
	state := newStateMachine()
	var snapshotSeq uint64
	if len(finalSnapshots) > 0 {
		sort.Slice(finalSnapshots, func(i, j int) bool { return finalSnapshots[i] < finalSnapshots[j] })
		snapshotSeq = finalSnapshots[len(finalSnapshots)-1]
		name := snapshotFinalName(snapshotSeq)
		loaded, lerr := loadSnapshot(filepath.Join(dir, name), cfg)
		if lerr != nil {
			return nil, wrapError(CodeCorruption, op, "highest finalized snapshot is unusable", lerr)
		}
		if loaded.applied != snapshotSeq {
			return nil, errorf(CodeCorruption, op,
				"snapshot %s declares applied sequence %d", name, loaded.applied)
		}
		state = loaded
		cfg.emit(Event{Name: EventSnapshotSelected, Snapshot: name, Sequence: snapshotSeq,
			Count: int64(len(state.kv))})
	}

	// The segment set is validated before a single record is decoded.
	sort.Slice(segments, func(i, j int) bool { return segments[i].StartSeq < segments[j].StartSeq })
	activeCount := 0
	for i, seg := range segments {
		if !seg.Sealed {
			activeCount++
			if i != len(segments)-1 {
				return nil, errorf(CodeCorruption, op,
					"active segment %s is not the newest segment", seg.Name)
			}
		}
		if i > 0 {
			prev := segments[i-1]
			if !prev.Sealed {
				return nil, errorf(CodeCorruption, op, "segment %s follows active segment %s", seg.Name, prev.Name)
			}
			switch {
			case seg.StartSeq <= prev.EndSeq:
				return nil, errorf(CodeCorruption, op,
					"segment %s overlaps %s", seg.Name, prev.Name)
			case seg.StartSeq != prev.EndSeq+1:
				return nil, errorf(CodeCorruption, op,
					"sequence gap between %s and %s", prev.Name, seg.Name)
			}
		}
	}
	if activeCount > 1 {
		return nil, errorf(CodeCorruption, op, "%d active segments exist", activeCount)
	}
	if len(segments) > 0 && segments[0].StartSeq > snapshotSeq+1 {
		return nil, errorf(CodeCorruption, op,
			"oldest segment starts at %d, leaving a gap after snapshot sequence %d",
			segments[0].StartSeq, snapshotSeq)
	}

	res := &recoveryResult{state: state, snapshotSequence: snapshotSeq}
	nextSeq := snapshotSeq + 1
	if len(segments) > 0 {
		nextSeq = segments[0].StartSeq
	}

	// Decode and apply one contiguous increasing prefix.
	for i, seg := range segments {
		isNewest := i == len(segments)-1
		path := filepath.Join(dir, seg.Name)

		buf, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil, wrapError(CodeStorage, op, "read segment", rerr)
		}
		start, herr := decodeSegmentHeader(buf)
		if herr != nil {
			return nil, wrapError(CodeCorruption, op, "segment "+seg.Name, herr)
		}
		if start != seg.StartSeq {
			return nil, errorf(CodeCorruption, op,
				"segment %s header declares start %d", seg.Name, start)
		}

		offset := int64(segmentHeaderBytes)
		for offset < int64(len(buf)) {
			seq, payload, size, derr := decodeFrame(buf[offset:], cfg.maxFramePayload())
			if derr != nil {
				// Only a damaged record at the physical end of the newest
				// active segment may be discarded. Everything else means the
				// prefix cannot be proven, so recovery fails closed.
				tailCandidate := isNewest && !seg.Sealed &&
					(errors.Is(derr, errIncompleteFrame) || offset+int64(size) == int64(len(buf)))
				if !tailCandidate {
					cfg.emit(Event{Name: EventInteriorCorruption, Segment: seg.Name,
						Offset: offset, Sequence: seq, Code: CodeCorruption, Err: derr})
					return nil, wrapError(CodeCorruption, op,
						"interior corruption in "+seg.Name, derr)
				}
				dropped := int64(len(buf)) - offset
				if terr := truncateSegment(path, offset, hooks); terr != nil {
					return nil, terr
				}
				res.truncatedBytes = dropped
				cfg.emit(Event{Name: EventTailTruncated, Segment: seg.Name,
					Offset: offset, Bytes: dropped})
				buf = buf[:offset]
				break
			}

			if seq != nextSeq {
				return nil, errorf(CodeCorruption, op,
					"segment %s holds sequence %d where %d was required", seg.Name, seq, nextSeq)
			}

			// Records already contained in the snapshot are validated for
			// contiguity, then skipped rather than reapplied.
			if seq > state.applied {
				cmd, cerr := decodeCommand(payload, cfg.MaxKeyBytes, cfg.MaxValueBytes)
				if cerr != nil {
					return nil, wrapError(CodeCorruption, op,
						"undecodable command in "+seg.Name, cerr)
				}
				if _, aerr := state.apply(cmd, seq, cfg.MaxKeys); aerr != nil {
					// A durable command that will not apply is an
					// incompatibility, never a rejected live request.
					return nil, wrapError(CodeCorruption, op,
						"durable command rejected during replay", aerr)
				}
				res.replayedCommands++
			}

			nextSeq = seq + 1
			offset += int64(size)
		}

		if seg.Sealed && nextSeq-1 != seg.EndSeq {
			return nil, errorf(CodeCorruption, op,
				"sealed segment %s ends at sequence %d, not the %d in its name",
				seg.Name, nextSeq-1, seg.EndSeq)
		}
		if isNewest && !seg.Sealed {
			res.activeName = seg.Name
			res.activeStart = seg.StartSeq
			res.appendOffset = offset
		}
	}

	res.lastSequence = nextSeq - 1
	if res.lastSequence < state.applied {
		return nil, errorf(CodeCorruption, op,
			"WAL ends at sequence %d, before snapshot sequence %d", res.lastSequence, state.applied)
	}
	return res, nil
}

// truncateSegment removes a torn tail and synchronizes the file so that the
// discarded bytes cannot reappear after a later crash.
func truncateSegment(path string, size int64, hooks *ioHooks) error {
	const op = "truncateSegment"

	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return wrapError(CodeStorage, op, "open segment for truncation", err)
	}
	defer f.Close()

	if err := hooks.Truncate(f, size); err != nil {
		return wrapError(CodeStorage, op, "truncate torn tail", err)
	}
	if err := hooks.Sync(f); err != nil {
		return wrapError(CodeStorage, op, "sync after truncation", err)
	}
	return nil
}

func hasSnapshotPrefix(name string) bool {
	return len(name) > len(snapshotPrefix) && name[:len(snapshotPrefix)] == snapshotPrefix
}

func hasSegmentPrefix(name string) bool {
	return len(name) > len(segmentPrefix) && name[:len(segmentPrefix)] == segmentPrefix
}
