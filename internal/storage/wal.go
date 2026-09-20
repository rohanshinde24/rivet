package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Segment and frame container constants. The magic values identify Rivet WAL
// files on disk; the versions are independent of the command schema version.
const (
	segmentMagic         = "RVTL"
	segmentFormatVersion = 1
	segmentHeaderBytes   = 32

	frameMagic         = "RVTW"
	frameFormatVersion = 1
	frameHeaderBytes   = 24

	// recordTypeCommand is the only v1 record type: a state-machine command.
	recordTypeCommand = 1
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// errIncompleteFrame marks bytes that could be a torn write at the physical
// end of the newest active segment. Recovery may truncate there, and only
// there (ADR-0002). Any other decode failure is interior corruption.
var errIncompleteFrame = errors.New("incomplete frame")

// encodeSegmentHeader returns the 32-byte header for a segment beginning at
// startSeq.
func encodeSegmentHeader(startSeq uint64) []byte {
	buf := make([]byte, segmentHeaderBytes)
	copy(buf[0:4], segmentMagic)
	buf[4] = segmentFormatVersion
	binary.LittleEndian.PutUint16(buf[5:7], segmentHeaderBytes)
	binary.LittleEndian.PutUint64(buf[7:15], startSeq)
	// buf[15:28] stays zero: reserved, and required to be zero on read.
	binary.LittleEndian.PutUint32(buf[28:32], crc32.Checksum(buf[0:28], crc32cTable))
	return buf
}

// decodeSegmentHeader validates a segment header and returns its start
// sequence. Unknown versions, bad lengths, nonzero reserved bytes, and
// checksum mismatches all fail startup rather than being repaired.
func decodeSegmentHeader(buf []byte) (uint64, error) {
	const op = "decodeSegmentHeader"

	if len(buf) < segmentHeaderBytes {
		return 0, errorf(CodeCorruption, op, "header is %d bytes, need %d", len(buf), segmentHeaderBytes)
	}
	if string(buf[0:4]) != segmentMagic {
		return 0, newError(CodeCorruption, op, "bad segment magic")
	}
	if buf[4] != segmentFormatVersion {
		return 0, errorf(CodeCorruption, op, "unknown segment format version %d", buf[4])
	}
	if n := binary.LittleEndian.Uint16(buf[5:7]); n != segmentHeaderBytes {
		return 0, errorf(CodeCorruption, op, "header length %d is not %d", n, segmentHeaderBytes)
	}
	for _, b := range buf[15:28] {
		if b != 0 {
			return 0, newError(CodeCorruption, op, "reserved header bytes are not zero")
		}
	}
	want := binary.LittleEndian.Uint32(buf[28:32])
	if got := crc32.Checksum(buf[0:28], crc32cTable); got != want {
		return 0, errorf(CodeCorruption, op, "header checksum %08x does not match %08x", got, want)
	}
	return binary.LittleEndian.Uint64(buf[7:15]), nil
}

// encodeFrame appends one complete record frame to dst.
func encodeFrame(dst []byte, seq uint64, payload []byte) []byte {
	var head [frameHeaderBytes]byte
	copy(head[0:4], frameMagic)
	head[4] = frameFormatVersion
	head[5] = recordTypeCommand
	// head[6:8] flags stay zero in v1.
	binary.LittleEndian.PutUint64(head[8:16], seq)
	binary.LittleEndian.PutUint32(head[16:20], uint32(len(payload)))

	sum := crc32.Checksum(head[0:20], crc32cTable)
	sum = crc32.Update(sum, crc32cTable, payload)
	binary.LittleEndian.PutUint32(head[20:24], sum)

	dst = append(dst, head[:]...)
	return append(dst, payload...)
}

// decodeFrame decodes the frame at the start of buf.
//
// It returns errIncompleteFrame when buf simply ends early, which the caller
// distinguishes from corruption: only a torn tail may be truncated away. The
// declared payload length is checked against maxPayload before any allocation
// (INV-003-4, INV-003-11).
func decodeFrame(buf []byte, maxPayload int) (seq uint64, payload []byte, size int, err error) {
	const op = "decodeFrame"

	if len(buf) < frameHeaderBytes {
		return 0, nil, 0, errIncompleteFrame
	}
	if string(buf[0:4]) != frameMagic {
		return 0, nil, 0, newError(CodeCorruption, op, "bad frame magic")
	}
	if buf[4] != frameFormatVersion {
		return 0, nil, 0, errorf(CodeCorruption, op, "unknown frame version %d", buf[4])
	}
	if buf[5] != recordTypeCommand {
		return 0, nil, 0, errorf(CodeCorruption, op, "unknown record type %d", buf[5])
	}
	if f := binary.LittleEndian.Uint16(buf[6:8]); f != 0 {
		return 0, nil, 0, errorf(CodeCorruption, op, "frame flags %04x are not zero", f)
	}

	seq = binary.LittleEndian.Uint64(buf[8:16])
	declared := binary.LittleEndian.Uint32(buf[16:20])
	if declared > uint32(maxPayload) || declared > HardMaxFramePayloadBytes {
		return 0, nil, 0, errorf(CodeCorruption, op,
			"declared payload %d exceeds maximum %d", declared, maxPayload)
	}

	size = frameHeaderBytes + int(declared)
	if len(buf) < size {
		return 0, nil, 0, errIncompleteFrame
	}

	body := buf[frameHeaderBytes:size]
	want := binary.LittleEndian.Uint32(buf[20:24])
	sum := crc32.Checksum(buf[0:20], crc32cTable)
	sum = crc32.Update(sum, crc32cTable, body)
	if sum != want {
		// A checksum mismatch on a fully present frame is still only a tail
		// candidate; the caller decides using position, not content.
		return 0, nil, size, errorf(CodeCorruption, op,
			"frame checksum %08x does not match %08x", sum, want)
	}
	return seq, body, size, nil
}

// Segment file naming. Both forms use fixed-width sequence numbers so that
// lexicographic and numeric order agree.
const (
	segmentPrefix      = "wal-"
	activeSuffix       = ".active"
	sealedSuffix       = ".sealed"
	sequenceNameDigits = 20
)

func activeSegmentName(startSeq uint64) string {
	return fmt.Sprintf("%s%0*d%s", segmentPrefix, sequenceNameDigits, startSeq, activeSuffix)
}

func sealedSegmentName(startSeq, endSeq uint64) string {
	return fmt.Sprintf("%s%0*d-%0*d%s", segmentPrefix,
		sequenceNameDigits, startSeq, sequenceNameDigits, endSeq, sealedSuffix)
}

// segmentName is a parsed WAL file name.
type segmentName struct {
	Name     string
	StartSeq uint64
	EndSeq   uint64 // meaningful only when Sealed
	Sealed   bool
}

// parseSegmentName parses a WAL file name. An impossible name is an error
// rather than a file to ignore: unexplained files in the data directory could
// mean a different writer or a partial upgrade.
func parseSegmentName(name string) (segmentName, error) {
	const op = "parseSegmentName"

	if !strings.HasPrefix(name, segmentPrefix) {
		return segmentName{}, errorf(CodeCorruption, op, "name %q lacks the segment prefix", name)
	}
	body := strings.TrimPrefix(name, segmentPrefix)

	parseSeq := func(s string) (uint64, error) {
		if len(s) != sequenceNameDigits {
			return 0, errorf(CodeCorruption, op, "sequence field %q is not %d digits", s, sequenceNameDigits)
		}
		return strconv.ParseUint(s, 10, 64)
	}

	switch {
	case strings.HasSuffix(body, activeSuffix):
		start, err := parseSeq(strings.TrimSuffix(body, activeSuffix))
		if err != nil {
			return segmentName{}, wrapError(CodeCorruption, op, "bad active segment name", err)
		}
		return segmentName{Name: name, StartSeq: start}, nil

	case strings.HasSuffix(body, sealedSuffix):
		parts := strings.Split(strings.TrimSuffix(body, sealedSuffix), "-")
		if len(parts) != 2 {
			return segmentName{}, errorf(CodeCorruption, op, "sealed segment name %q is malformed", name)
		}
		start, err := parseSeq(parts[0])
		if err != nil {
			return segmentName{}, wrapError(CodeCorruption, op, "bad sealed start sequence", err)
		}
		end, err := parseSeq(parts[1])
		if err != nil {
			return segmentName{}, wrapError(CodeCorruption, op, "bad sealed end sequence", err)
		}
		if end < start {
			return segmentName{}, errorf(CodeCorruption, op, "sealed segment ends at %d before start %d", end, start)
		}
		return segmentName{Name: name, StartSeq: start, EndSeq: end, Sealed: true}, nil

	default:
		return segmentName{}, errorf(CodeCorruption, op, "name %q has no known segment suffix", name)
	}
}

// segmentWriter owns the single active WAL segment. Only the event loop uses
// it (ADR-0005).
type segmentWriter struct {
	dir      string
	file     *os.File
	path     string
	startSeq uint64
	lastSeq  uint64
	size     int64
	hooks    *ioHooks
}

// createSegment creates and durably registers a new active segment beginning
// at startSeq. The header is written and synchronized, then the parent
// directory is synchronized, before any record may be appended to it.
func createSegment(dir string, startSeq uint64, hooks *ioHooks) (*segmentWriter, error) {
	const op = "createSegment"

	path := filepath.Join(dir, activeSegmentName(startSeq))
	if _, err := os.Stat(path); err == nil {
		return nil, errorf(CodeCorruption, op, "active segment for sequence %d already exists", startSeq)
	} else if !os.IsNotExist(err) {
		return nil, wrapError(CodeStorage, op, "stat active segment", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, wrapError(CodeStorage, op, "create active segment", err)
	}

	w := &segmentWriter{dir: dir, file: f, path: path, startSeq: startSeq, lastSeq: startSeq - 1, hooks: hooks}

	header := encodeSegmentHeader(startSeq)
	n, err := hooks.writeFull(f, header)
	w.size = int64(n)
	if err != nil {
		f.Close()
		return nil, wrapError(CodeStorage, op, "write segment header", err)
	}
	if err := hooks.Sync(f); err != nil {
		f.Close()
		return nil, wrapError(CodeStorage, op, "sync segment header", err)
	}
	if err := hooks.SyncDir(dir); err != nil {
		f.Close()
		return nil, wrapError(CodeStorage, op, "sync directory after segment create", err)
	}
	return w, nil
}

// openSegmentForAppend reopens an existing active segment positioned at
// endOffset, which recovery has already proven to be the end of a valid
// record prefix.
func openSegmentForAppend(dir, name string, startSeq, lastSeq uint64, endOffset int64, hooks *ioHooks) (*segmentWriter, error) {
	const op = "openSegmentForAppend"

	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return nil, wrapError(CodeStorage, op, "open active segment", err)
	}
	if _, err := f.Seek(endOffset, 0); err != nil {
		f.Close()
		return nil, wrapError(CodeStorage, op, "seek to append offset", err)
	}
	return &segmentWriter{
		dir: dir, file: f, path: path,
		startSeq: startSeq, lastSeq: lastSeq, size: endOffset, hooks: hooks,
	}, nil
}

// appendRecord writes one complete frame and does not return until every byte
// has been handed to the file or an error occurred. It does not synchronize;
// the caller decides the durability point.
func (w *segmentWriter) appendRecord(seq uint64, payload []byte, scratch []byte) ([]byte, error) {
	const op = "appendRecord"

	if seq != w.lastSeq+1 {
		return scratch, errorf(CodeCorruption, op, "record sequence %d is not %d", seq, w.lastSeq+1)
	}
	frame := encodeFrame(scratch[:0], seq, payload)
	n, err := w.hooks.writeFull(w.file, frame)
	w.size += int64(n)
	if err != nil {
		// A partial frame is on disk. The caller must fault: only recovery,
		// which can see whether this is the physical tail, may decide.
		return frame, wrapError(CodeStorage, op, "write record frame", err)
	}
	w.lastSeq = seq
	return frame, nil
}

func (w *segmentWriter) sync() error {
	if err := w.hooks.Sync(w.file); err != nil {
		return wrapError(CodeStorage, "sync", "sync active segment", err)
	}
	return nil
}

// seal synchronizes the segment, renames it to its sealed name, and
// synchronizes the directory. After it returns the segment is immutable.
func (w *segmentWriter) seal() (string, error) {
	const op = "sealSegment"

	if err := w.hooks.Sync(w.file); err != nil {
		return "", wrapError(CodeStorage, op, "sync before seal", err)
	}
	if err := w.file.Close(); err != nil {
		return "", wrapError(CodeStorage, op, "close before seal", err)
	}
	sealed := filepath.Join(w.dir, sealedSegmentName(w.startSeq, w.lastSeq))
	if err := w.hooks.Rename(w.path, sealed); err != nil {
		return "", wrapError(CodeStorage, op, "rename to sealed name", err)
	}
	if err := w.hooks.SyncDir(w.dir); err != nil {
		return "", wrapError(CodeStorage, op, "sync directory after seal", err)
	}
	return sealed, nil
}

func (w *segmentWriter) close() error {
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}
