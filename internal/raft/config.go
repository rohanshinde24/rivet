package raft

import "fmt"

// Defaults for a group's timing and message limits.
//
// Timing is expressed only in ticks. A tick is whatever the driver decides it
// is; the core never converts one into a duration, because a duration invites
// comparison against a real clock and the core is meant to have no sense of
// real time at all.
const (
	// DefaultHeartbeatTicks is how often a leader sends a heartbeat.
	DefaultHeartbeatTicks = 1

	// A follower campaigns after a timeout drawn uniformly from
	// [DefaultElectionTickMin, DefaultElectionTickMax). The ten-to-one ratio
	// against the heartbeat lets a follower miss roughly ten heartbeats before
	// campaigning, which absorbs transient delay without causing needless
	// elections, and a randomization window as wide as the base value keeps
	// split votes rare.
	DefaultElectionTickMin = 10
	DefaultElectionTickMax = 20

	// DefaultMaxEntriesPerMsg and DefaultMaxBytesPerMsg bound one append.
	// Neither can make a single large entry unsendable; see LogStore.Entries.
	DefaultMaxEntriesPerMsg = 64
	DefaultMaxBytesPerMsg   = 1 << 20

	// DefaultMaxInflightPerPeer is one: pipelining is not part of this
	// milestone, and one outstanding append per peer keeps flow control
	// trivial enough to reason about without a measurement.
	DefaultMaxInflightPerPeer = 1
)

// Config describes one member of one group.
type Config struct {
	// ID is this member. It must appear in Peers.
	ID NodeID

	// Peers is every voting member of the group, including this one.
	// Membership is fixed for the lifetime of the group.
	Peers []NodeID

	HeartbeatTicks   int
	ElectionTickMin  int
	ElectionTickMax  int
	MaxEntriesPerMsg int
	MaxBytesPerMsg   uint64

	// Seed drives every random choice the core makes, so that a schedule
	// replays exactly. A core never reads a global or cryptographic source.
	Seed int64

	// OnObserve receives structured facts about this member's progress. It is
	// called from inside a step, so it must not block or call back into the
	// node. Nil disables observation entirely.
	OnObserve func(Observation)
}

func (c Config) withDefaults() Config {
	if c.HeartbeatTicks == 0 {
		c.HeartbeatTicks = DefaultHeartbeatTicks
	}
	if c.ElectionTickMin == 0 {
		c.ElectionTickMin = DefaultElectionTickMin
	}
	if c.ElectionTickMax == 0 {
		c.ElectionTickMax = DefaultElectionTickMax
	}
	if c.MaxEntriesPerMsg == 0 {
		c.MaxEntriesPerMsg = DefaultMaxEntriesPerMsg
	}
	if c.MaxBytesPerMsg == 0 {
		c.MaxBytesPerMsg = DefaultMaxBytesPerMsg
	}
	return c
}

func (c Config) validate() error {
	if c.ID == None {
		return fmt.Errorf("raft: config ID must not be %d", None)
	}

	seen := make(map[NodeID]bool, len(c.Peers))
	self := false
	for _, p := range c.Peers {
		if p == None {
			return fmt.Errorf("raft: peer list contains the reserved id %d", None)
		}
		if seen[p] {
			return fmt.Errorf("raft: peer %d appears twice", p)
		}
		seen[p] = true
		if p == c.ID {
			self = true
		}
	}
	if len(c.Peers) == 0 {
		return fmt.Errorf("raft: peer list is empty")
	}
	if !self {
		return fmt.Errorf("raft: peer list does not contain this member %d", c.ID)
	}

	if c.HeartbeatTicks < 1 {
		return fmt.Errorf("raft: HeartbeatTicks %d is below 1", c.HeartbeatTicks)
	}
	if c.ElectionTickMin <= c.HeartbeatTicks {
		return fmt.Errorf("raft: ElectionTickMin %d must exceed HeartbeatTicks %d",
			c.ElectionTickMin, c.HeartbeatTicks)
	}
	if c.ElectionTickMax <= c.ElectionTickMin {
		return fmt.Errorf("raft: ElectionTickMax %d must exceed ElectionTickMin %d",
			c.ElectionTickMax, c.ElectionTickMin)
	}
	if c.MaxEntriesPerMsg < 1 {
		return fmt.Errorf("raft: MaxEntriesPerMsg %d is below 1", c.MaxEntriesPerMsg)
	}
	if c.MaxBytesPerMsg < 1 {
		return fmt.Errorf("raft: MaxBytesPerMsg %d is below 1", c.MaxBytesPerMsg)
	}
	return nil
}

// quorum is the number of members that must agree.
func (c Config) quorum() int { return len(c.Peers)/2 + 1 }
