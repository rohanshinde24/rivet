package storage

import (
	"crypto/sha256"
	"encoding/binary"
)

// OpKind is the durable operation kind of a command.
type OpKind uint8

const (
	OpInvalid OpKind = 0
	OpPut     OpKind = 1
	OpDelete  OpKind = 2
)

func (k OpKind) String() string {
	switch k {
	case OpPut:
		return "PUT"
	case OpDelete:
		return "DELETE"
	default:
		return "INVALID"
	}
}

// commandSchemaVersion is the version of the durable command payload schema.
// It is independent of the WAL frame version and of any transport message.
const commandSchemaVersion uint8 = 1

// commandFixedBytes counts the fields present in every command payload:
// schema version, operation kind, client ID, request sequence, and key length.
const commandFixedBytes = 1 + 1 + 16 + 8 + 4

// ClientID identifies a write session. It must be nonzero.
type ClientID [16]byte

// IsZero reports whether the identifier is the reserved all-zero value.
func (c ClientID) IsZero() bool {
	for _, b := range c {
		if b != 0 {
			return false
		}
	}
	return true
}

// RequestIdentity is the durable identity of one write. Sequences begin at 1
// and increase by exactly one per client.
type RequestIdentity struct {
	ClientID        ClientID
	RequestSequence uint64
}

// command is the deterministic durable form of a write. It holds no deadline,
// clock reading, trace identifier, address, or response text.
type command struct {
	Kind     OpKind
	ClientID ClientID
	Sequence uint64
	Key      []byte
	Value    []byte // nil for OpDelete
}

// encodedLen returns the exact payload length for c.
func (c *command) encodedLen() int {
	n := commandFixedBytes + len(c.Key)
	if c.Kind == OpPut {
		n += 4 + len(c.Value)
	}
	return n
}

// encode appends the canonical payload encoding of c to dst.
//
// The encoding is deterministic: the same command always produces identical
// bytes, which is what makes replay and snapshot digests comparable.
func (c *command) encode(dst []byte) []byte {
	var scratch [8]byte

	dst = append(dst, commandSchemaVersion, byte(c.Kind))
	dst = append(dst, c.ClientID[:]...)
	binary.LittleEndian.PutUint64(scratch[:8], c.Sequence)
	dst = append(dst, scratch[:8]...)
	binary.LittleEndian.PutUint32(scratch[:4], uint32(len(c.Key)))
	dst = append(dst, scratch[:4]...)
	dst = append(dst, c.Key...)
	if c.Kind == OpPut {
		binary.LittleEndian.PutUint32(scratch[:4], uint32(len(c.Value)))
		dst = append(dst, scratch[:4]...)
		dst = append(dst, c.Value...)
	}
	return dst
}

// decodeCommand decodes a payload produced by encode. It copies key and value
// out of buf so the caller may reuse buf, and rejects any length that is
// inconsistent with the payload or with the configured limits before
// allocating.
func decodeCommand(buf []byte, maxKey, maxValue int) (*command, error) {
	const op = "decodeCommand"

	if len(buf) < commandFixedBytes {
		return nil, errorf(CodeCorruption, op, "payload %d bytes below minimum %d", len(buf), commandFixedBytes)
	}
	if v := buf[0]; v != commandSchemaVersion {
		return nil, errorf(CodeCorruption, op, "unknown command schema version %d", v)
	}

	kind := OpKind(buf[1])
	if kind != OpPut && kind != OpDelete {
		return nil, errorf(CodeCorruption, op, "unknown operation kind %d", buf[1])
	}

	var c command
	c.Kind = kind
	copy(c.ClientID[:], buf[2:18])
	if c.ClientID.IsZero() {
		return nil, newError(CodeCorruption, op, "zero client ID")
	}
	c.Sequence = binary.LittleEndian.Uint64(buf[18:26])
	if c.Sequence == 0 {
		return nil, newError(CodeCorruption, op, "zero request sequence")
	}

	keyLen := int(binary.LittleEndian.Uint32(buf[26:30]))
	if keyLen < 1 || keyLen > maxKey {
		return nil, errorf(CodeCorruption, op, "key length %d outside [1,%d]", keyLen, maxKey)
	}
	rest := buf[commandFixedBytes:]
	if len(rest) < keyLen {
		return nil, errorf(CodeCorruption, op, "payload holds %d bytes for a %d byte key", len(rest), keyLen)
	}
	c.Key = append([]byte(nil), rest[:keyLen]...)
	rest = rest[keyLen:]

	if kind == OpDelete {
		if len(rest) != 0 {
			return nil, errorf(CodeCorruption, op, "delete payload has %d trailing bytes", len(rest))
		}
		return &c, nil
	}

	if len(rest) < 4 {
		return nil, errorf(CodeCorruption, op, "payload truncated before value length")
	}
	valueLen := int(binary.LittleEndian.Uint32(rest[:4]))
	if valueLen < 0 || valueLen > maxValue {
		return nil, errorf(CodeCorruption, op, "value length %d outside [0,%d]", valueLen, maxValue)
	}
	rest = rest[4:]
	if len(rest) != valueLen {
		return nil, errorf(CodeCorruption, op, "payload holds %d bytes for a %d byte value", len(rest), valueLen)
	}
	c.Value = append([]byte(nil), rest...)
	return &c, nil
}

// digestDomain separates command digests from every other SHA-256 use in the
// system, so a digest can never be confused with a hash of other bytes.
const digestDomain = "rivet.storage.command.v1\x00"

// digest is the canonical content digest of a command: operation kind, key,
// and value. It deliberately excludes client ID and sequence, because its
// purpose is to decide whether a retry of the same sequence carries the same
// content.
func (c *command) digest() [32]byte {
	var scratch [8]byte
	h := sha256.New()
	h.Write([]byte(digestDomain))
	h.Write([]byte{byte(c.Kind)})

	binary.LittleEndian.PutUint64(scratch[:], uint64(len(c.Key)))
	h.Write(scratch[:])
	h.Write(c.Key)

	// A PUT with an empty value and a DELETE both write a zero length here;
	// the kind byte above is what distinguishes them.
	binary.LittleEndian.PutUint64(scratch[:], uint64(len(c.Value)))
	h.Write(scratch[:])
	h.Write(c.Value)

	var out [32]byte
	h.Sum(out[:0])
	return out
}
