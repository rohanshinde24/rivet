package storage

// Event is one structured engine event. Its fields are deliberately a fixed,
// bounded set: no raw key or value bytes are ever carried, and a client is
// identified only by a short diagnostic prefix of its ID.
type Event struct {
	Name     string
	Segment  string
	Snapshot string
	Sequence uint64
	Offset   int64
	Bytes    int64
	Count    int64
	Code     Code
	Detail   string
	Err      error
}

// Event names. Every name that can appear is listed here so that consumers can
// enumerate them without reading the implementation.
const (
	EventOpenStart          = "engine.open.start"
	EventRecoveryComplete   = "engine.recovery.complete"
	EventRecoveryFailed     = "engine.recovery.failed"
	EventSnapshotSelected   = "snapshot.selected"
	EventSnapshotTempIgnore = "snapshot.temp.ignored"
	EventSnapshotStart      = "snapshot.start"
	EventSnapshotPublished  = "snapshot.published"
	EventSnapshotFailed     = "snapshot.failed"
	EventSegmentCreated     = "wal.segment.created"
	EventSegmentSealed      = "wal.segment.sealed"
	EventSegmentDeleted     = "wal.segment.deleted"
	EventTailTruncated      = "wal.tail.truncated"
	EventInteriorCorruption = "wal.interior.corruption"
	EventStorageFailure     = "storage.failure"
	EventFaulted            = "engine.faulted"
	EventSessionRejected    = "session.rejected"
	EventDedupHit           = "session.dedup.hit"
	EventCloseStart         = "engine.close.start"
	EventCloseComplete      = "engine.close.complete"
	EventCloseDeadline      = "engine.close.deadline"
	EventUnknownFile        = "directory.unknown.file"
)

func (c Config) emit(e Event) {
	if c.OnEvent != nil {
		c.OnEvent(e)
	}
}
