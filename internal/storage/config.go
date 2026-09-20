package storage

import "time"

// Hard ceilings. Configured limits may only move downward from these values;
// raising one is an on-disk and API migration, not a configuration change.
const (
	HardMaxKeyBytes      = 1024
	HardMaxValueBytes    = 1 << 20 // 1 MiB
	HardMaxSessions      = 1 << 20
	HardMaxQueueCapacity = 1 << 16
	HardMaxKeys          = 1 << 24

	// HardMaxFramePayloadBytes bounds any single WAL frame payload before
	// allocation, independent of configuration. It is the largest possible
	// v1 command payload at the hard key/value ceilings, rounded up.
	HardMaxFramePayloadBytes = commandFixedBytes + 4 + HardMaxKeyBytes + HardMaxValueBytes
)

// Default limits. Each is a deliberate choice rather than a tuned number:
// changing one changes behavior a caller can observe.
const (
	DefaultMaxKeyBytes        = HardMaxKeyBytes
	DefaultMaxValueBytes      = HardMaxValueBytes
	DefaultMaxSessions        = 10_000
	DefaultQueueCapacity      = 1_024
	DefaultMaxKeys            = 1_000_000
	DefaultSegmentTargetBytes = 64 << 20
	DefaultCloseTimeout       = 30 * time.Second
)

// Config configures one engine over one data directory.
//
// A zero value for any limit selects the default. Limits are validated at Open
// and never change for the lifetime of the engine.
type Config struct {
	// Dir is the data directory. The engine takes exclusive ownership of it
	// for its lifetime.
	Dir string

	// MaxKeyBytes bounds a key. Keys are non-empty.
	MaxKeyBytes int

	// MaxValueBytes bounds a value. Values may be empty.
	MaxValueBytes int

	// MaxKeys bounds the number of live keys, and with the byte limits bounds
	// snapshot size.
	MaxKeys int

	// MaxSessions bounds the client session table. A new client beyond
	// capacity is rejected before execution; sessions are not silently
	// evicted.
	MaxSessions int

	// QueueCapacity bounds the request queue feeding the event loop. A request
	// that finds the queue full is rejected before execution.
	QueueCapacity int

	// SegmentTargetBytes is the size after which the active WAL segment is
	// rotated. It is operational, not a correctness bound: a segment may
	// exceed it by one bounded record.
	SegmentTargetBytes int64

	// CloseTimeout bounds Close. Expiry is reported as an error; it never
	// pretends durable cleanup occurred.
	CloseTimeout time.Duration

	// OnEvent receives structured engine events. It is called from the event
	// loop and from recovery, so it must not block or call back into the
	// engine. Nil disables event reporting.
	OnEvent func(Event)
}

func (c Config) withDefaults() Config {
	if c.MaxKeyBytes == 0 {
		c.MaxKeyBytes = DefaultMaxKeyBytes
	}
	if c.MaxValueBytes == 0 {
		c.MaxValueBytes = DefaultMaxValueBytes
	}
	if c.MaxKeys == 0 {
		c.MaxKeys = DefaultMaxKeys
	}
	if c.MaxSessions == 0 {
		c.MaxSessions = DefaultMaxSessions
	}
	if c.QueueCapacity == 0 {
		c.QueueCapacity = DefaultQueueCapacity
	}
	if c.SegmentTargetBytes == 0 {
		c.SegmentTargetBytes = DefaultSegmentTargetBytes
	}
	if c.CloseTimeout == 0 {
		c.CloseTimeout = DefaultCloseTimeout
	}
	return c
}

func (c Config) validate() error {
	const op = "config"
	if c.Dir == "" {
		return newError(CodeInvalidArgument, op, "Dir is required")
	}
	if c.MaxKeyBytes < 1 || c.MaxKeyBytes > HardMaxKeyBytes {
		return errorf(CodeInvalidArgument, op, "MaxKeyBytes %d outside [1,%d]", c.MaxKeyBytes, HardMaxKeyBytes)
	}
	if c.MaxValueBytes < 0 || c.MaxValueBytes > HardMaxValueBytes {
		return errorf(CodeInvalidArgument, op, "MaxValueBytes %d outside [0,%d]", c.MaxValueBytes, HardMaxValueBytes)
	}
	if c.MaxKeys < 1 || c.MaxKeys > HardMaxKeys {
		return errorf(CodeInvalidArgument, op, "MaxKeys %d outside [1,%d]", c.MaxKeys, HardMaxKeys)
	}
	if c.MaxSessions < 1 || c.MaxSessions > HardMaxSessions {
		return errorf(CodeInvalidArgument, op, "MaxSessions %d outside [1,%d]", c.MaxSessions, HardMaxSessions)
	}
	if c.QueueCapacity < 1 || c.QueueCapacity > HardMaxQueueCapacity {
		return errorf(CodeInvalidArgument, op, "QueueCapacity %d outside [1,%d]", c.QueueCapacity, HardMaxQueueCapacity)
	}
	if c.SegmentTargetBytes < 1024 {
		return errorf(CodeInvalidArgument, op, "SegmentTargetBytes %d below 1024", c.SegmentTargetBytes)
	}
	if c.CloseTimeout < 0 {
		return newError(CodeInvalidArgument, op, "CloseTimeout is negative")
	}
	return nil
}

// maxFramePayload is the largest frame payload this configuration can produce.
// Decoders reject a declared length above it before allocating.
func (c Config) maxFramePayload() int {
	return commandFixedBytes + 4 + c.MaxKeyBytes + c.MaxValueBytes
}

// maxSnapshotPayload bounds a snapshot payload before allocation.
func (c Config) maxSnapshotPayload() int64 {
	kv := int64(c.MaxKeys) * int64(4+c.MaxKeyBytes+4+c.MaxValueBytes)
	sessions := int64(c.MaxSessions) * sessionEntryBytes
	return kv + sessions
}
