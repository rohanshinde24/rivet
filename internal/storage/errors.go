package storage

import (
	"errors"
	"fmt"
)

// Code is a bounded classification of every failure the engine can report.
// Codes are part of the engine contract: callers branch on them to decide
// whether an operation definitely did not execute, definitely executed, or
// has an unknown outcome (SPEC-003 "Write acknowledgement").
type Code uint8

const (
	CodeUnknown Code = iota

	// CodeInvalidArgument marks a pre-execution validation failure: limits,
	// malformed identity, or a closed/absent argument. Definitely not executed.
	CodeInvalidArgument

	// CodeRequestIDReuse marks a reused client sequence carrying different
	// command content. Definitely not executed for the new content.
	CodeRequestIDReuse

	// CodeDuplicateResultExpired marks a client sequence below the retained
	// high-water mark. Never executed again.
	CodeDuplicateResultExpired

	// CodeSequenceGap marks a client sequence that is not exactly
	// last_sequence + 1 (or not 1 for a new client). Definitely not executed.
	CodeSequenceGap

	// CodeResourceExhausted marks a bounded admission rejection: the request
	// queue is full or the session table is at capacity. Definitely not executed.
	CodeResourceExhausted

	// CodeCanceled marks a caller context that ended before the request was
	// admitted or before the event loop began processing it. Definitely not
	// executed.
	CodeCanceled

	// CodeSnapshotInProgress marks a snapshot request made while another
	// snapshot job is active.
	CodeSnapshotInProgress

	// CodeFaulted marks a request refused because an earlier ambiguous storage
	// error moved the engine to Faulted (INV-003-12).
	CodeFaulted

	// CodeClosed marks a request refused because the engine is closing or closed.
	CodeClosed

	// CodeCorruption marks durable state that cannot be proven to be a valid
	// recovery prefix. Recovery fails closed (INV-003-3, INV-003-4).
	CodeCorruption

	// CodeStorage marks an I/O error. When it can make the active recovery path
	// ambiguous the engine also transitions to Faulted, and the caller's outcome
	// is unknown rather than failed.
	CodeStorage

	// CodeLocked marks a data directory already owned by another engine
	// instance (INV-003-10).
	CodeLocked
)

func (c Code) String() string {
	switch c {
	case CodeInvalidArgument:
		return "INVALID_ARGUMENT"
	case CodeRequestIDReuse:
		return "REQUEST_ID_REUSE"
	case CodeDuplicateResultExpired:
		return "DUPLICATE_RESULT_EXPIRED"
	case CodeSequenceGap:
		return "SEQUENCE_GAP"
	case CodeResourceExhausted:
		return "RESOURCE_EXHAUSTED"
	case CodeCanceled:
		return "CANCELED"
	case CodeSnapshotInProgress:
		return "SNAPSHOT_IN_PROGRESS"
	case CodeFaulted:
		return "FAULTED"
	case CodeClosed:
		return "CLOSED"
	case CodeCorruption:
		return "CORRUPTION"
	case CodeStorage:
		return "STORAGE"
	case CodeLocked:
		return "LOCKED"
	default:
		return "UNKNOWN"
	}
}

// Error is the engine's error type. Detail never contains raw key or value
// bytes (SPEC-003 "Observability").
type Error struct {
	Code   Code
	Op     string
	Detail string
	Err    error
}

func (e *Error) Error() string {
	var b string
	if e.Op != "" {
		b = e.Op + ": " + e.Code.String()
	} else {
		b = e.Code.String()
	}
	if e.Detail != "" {
		b += ": " + e.Detail
	}
	if e.Err != nil {
		b += ": " + e.Err.Error()
	}
	return b
}

func (e *Error) Unwrap() error { return e.Err }

func newError(code Code, op, detail string) *Error {
	return &Error{Code: code, Op: op, Detail: detail}
}

func wrapError(code Code, op, detail string, err error) *Error {
	return &Error{Code: code, Op: op, Detail: detail, Err: err}
}

func errorf(code Code, op, format string, args ...any) *Error {
	return &Error{Code: code, Op: op, Detail: fmt.Sprintf(format, args...)}
}

// CodeOf reports the engine code carried by err, or CodeUnknown.
func CodeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return CodeUnknown
}
