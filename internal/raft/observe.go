package raft

// Observability for the core.
//
// The core emits facts, never timings. It cannot read a clock, so a span with
// a duration is something only the driver can produce, and the driver is
// where tracing belongs. What the core can say is what it decided and why,
// which is the part that is hard to reconstruct afterwards from timings
// alone.
//
// No entry payload ever appears in an observation. A reason is one of the
// fixed strings below, never text built from data.

// ObservationKind names what happened.
type ObservationKind uint8

const (
	ObsRoleChange ObservationKind = iota
	ObsTermAdvanced
	ObsElectionStarted
	ObsElectionWon
	ObsElectionLost
	ObsVoteGranted
	ObsVoteDenied
	ObsAppendRejected
	ObsLogTruncated
	ObsCommitAdvanced
	ObsProgressReset
)

func (k ObservationKind) String() string {
	switch k {
	case ObsRoleChange:
		return "role_change"
	case ObsTermAdvanced:
		return "term_advanced"
	case ObsElectionStarted:
		return "election_started"
	case ObsElectionWon:
		return "election_won"
	case ObsElectionLost:
		return "election_lost"
	case ObsVoteGranted:
		return "vote_granted"
	case ObsVoteDenied:
		return "vote_denied"
	case ObsAppendRejected:
		return "append_rejected"
	case ObsLogTruncated:
		return "log_truncated"
	case ObsCommitAdvanced:
		return "commit_advanced"
	case ObsProgressReset:
		return "progress_reset"
	default:
		return "unknown"
	}
}

// Reasons carried by an observation. They are a closed set so that a consumer
// can group by them without parsing.
const (
	ReasonTimeout        = "election_timeout"
	ReasonForced         = "forced"
	ReasonHigherTerm     = "higher_term"
	ReasonLeaderAppend   = "leader_append"
	ReasonQuorumRejected = "quorum_rejected"
	ReasonAlreadyVoted   = "already_voted"
	ReasonLogBehind      = "candidate_log_behind"
	ReasonLogMismatch    = "previous_entry_mismatch"
	ReasonStaleTerm      = "stale_term"
)

// Observation is one structured fact about a member's progress.
type Observation struct {
	Kind ObservationKind
	Node NodeID
	Term Term
	Role Role

	// Peer is the other member involved, where there is one.
	Peer NodeID

	// Index carries a log position: a truncation point, a commit index, or an
	// append rejection hint.
	Index Index

	Reason string
}

// Metrics is a snapshot of a member's counters.
//
// Counts are cumulative for the life of this member; a restart resets them,
// which is correct, because a restarted member is a new process and pretending
// otherwise would hide exactly the restart a reader is looking for.
type Metrics struct {
	Term      Term
	Role      Role
	Commit    Index
	Applied   Index
	LastIndex Index

	LogEntries int
	LogBytes   uint64

	ElectionsStarted uint64
	ElectionsWon     uint64
	ElectionsLost    uint64

	MessagesSent     map[MessageType]uint64
	MessagesReceived map[MessageType]uint64

	AppendsRejected uint64
	EntriesAppended uint64
	CommitAdvances  uint64

	// TicksSinceHeartbeat is how long a follower has gone without hearing
	// from a leader, counted in ticks rather than time because the core has
	// no other unit.
	TicksSinceHeartbeat int

	// FollowerLag is how far behind each peer is, on a leader only.
	FollowerLag map[NodeID]Index
}

type counters struct {
	electionsStarted uint64
	electionsWon     uint64
	electionsLost    uint64
	sent             map[MessageType]uint64
	received         map[MessageType]uint64
	appendsRejected  uint64
	entriesAppended  uint64
	commitAdvances   uint64
}

func newCounters() counters {
	return counters{
		sent:     map[MessageType]uint64{},
		received: map[MessageType]uint64{},
	}
}

func (n *Node) observe(o Observation) {
	if n.cfg.OnObserve == nil {
		return
	}
	o.Node = n.cfg.ID
	if o.Term == 0 {
		o.Term = n.term
	}
	o.Role = n.role
	n.cfg.OnObserve(o)
}

// Metrics returns a snapshot. It allocates, so it belongs on a reporting path
// rather than inside a step.
func (n *Node) Metrics() Metrics {
	m := Metrics{
		Term:                n.term,
		Role:                n.role,
		Commit:              n.log.committed,
		Applied:             n.log.applied,
		LastIndex:           n.log.lastIndex(),
		ElectionsStarted:    n.counters.electionsStarted,
		ElectionsWon:        n.counters.electionsWon,
		ElectionsLost:       n.counters.electionsLost,
		AppendsRejected:     n.counters.appendsRejected,
		EntriesAppended:     n.counters.entriesAppended,
		CommitAdvances:      n.counters.commitAdvances,
		TicksSinceHeartbeat: n.electionElapsed,
		MessagesSent:        map[MessageType]uint64{},
		MessagesReceived:    map[MessageType]uint64{},
	}
	for k, v := range n.counters.sent {
		m.MessagesSent[k] = v
	}
	for k, v := range n.counters.received {
		m.MessagesReceived[k] = v
	}

	if ents := n.log.store.LastIndex(); ents > 0 {
		if all, err := n.log.store.Entries(1, ents+1, ^uint64(0)); err == nil {
			m.LogEntries = len(all)
			for _, e := range all {
				m.LogBytes += entrySize(e)
			}
		}
	}

	if n.role == Leader {
		m.FollowerLag = map[NodeID]Index{}
		last := n.log.lastIndex()
		for peer, pr := range n.progress {
			if peer == n.cfg.ID {
				continue
			}
			m.FollowerLag[peer] = last - pr.Match
		}
	}
	return m
}
