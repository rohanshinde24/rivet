package raft

import (
	"errors"
	"math/rand"
)

var (
	// ErrNotLeader means a proposal reached a member that cannot accept it.
	ErrNotLeader = errors.New("raft: this member is not the leader")

	// ErrReadyOutstanding means the driver stepped the core again before
	// performing the work it was already handed. Allowing that would let a
	// message be sent describing state the driver has not yet made durable.
	ErrReadyOutstanding = errors.New("raft: a Ready batch is outstanding; call Advance first")

	// ErrReplicationPending marks a path that arrives with log replication.
	// A leader can be elected and can hold leadership without it.
	ErrReplicationPending = errors.New("raft: log replication is not implemented yet")
)

// Status is a read-only view of a member, for tests and observability.
type Status struct {
	ID        NodeID
	Term      Term
	Vote      NodeID
	Role      Role
	Leader    NodeID
	Commit    Index
	LastIndex Index
}

// Node is one member's Raft state.
//
// It is a value driven by a single owner, not a service. Every method here
// runs to completion without blocking, touching a clock, or performing I/O.
type Node struct {
	cfg Config
	log LogStore
	rng *rand.Rand

	// Durable once the driver has written the HardState in a Ready batch.
	term Term
	vote NodeID

	// Rebuilt on restart rather than persisted.
	role   Role
	leader NodeID
	commit Index

	electionElapsed  int
	heartbeatElapsed int
	electionTimeout  int

	// votes records responses to this member's own candidacy, true for a
	// grant. It is discarded whenever the term changes.
	votes map[NodeID]bool

	// Staged output, drained by Ready and cleared by Advance.
	msgs       []Message
	hardDirty  bool
	stateDirty bool

	readyOutstanding bool
}

// NewNode creates a member from its durable state. A member that has never
// run passes the zero HardState.
func NewNode(cfg Config, log LogStore, hs HardState) (*Node, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if log == nil {
		return nil, errors.New("raft: log store is nil")
	}

	n := &Node{
		cfg:    cfg,
		log:    log,
		rng:    rand.New(rand.NewSource(cfg.Seed)),
		term:   hs.Term,
		vote:   hs.Vote,
		role:   Follower,
		leader: None,
	}
	// Restored state is already durable, so it is not staged for writing.
	n.resetElectionTimer()
	return n, nil
}

func (n *Node) Status() Status {
	return Status{
		ID:        n.cfg.ID,
		Term:      n.term,
		Vote:      n.vote,
		Role:      n.role,
		Leader:    n.leader,
		Commit:    n.commit,
		LastIndex: n.log.LastIndex(),
	}
}

// Step applies one event.
//
// It compares the durable and reportable state before and after, so a change
// is staged because it happened rather than because some branch remembered to
// flag it.
func (n *Node) Step(ev Event) error {
	if n.readyOutstanding {
		return ErrReadyOutstanding
	}

	beforeHard := HardState{Term: n.term, Vote: n.vote}
	beforeState := StateChange{Role: n.role, Term: n.term, Leader: n.leader}

	err := n.step(ev)

	if (HardState{Term: n.term, Vote: n.vote}) != beforeHard {
		n.hardDirty = true
	}
	if (StateChange{Role: n.role, Term: n.term, Leader: n.leader}) != beforeState {
		n.stateDirty = true
	}
	return err
}

func (n *Node) step(ev Event) error {
	switch ev.Type {
	case EventTick:
		n.tick()
		return nil
	case EventCampaign:
		n.campaign()
		return nil
	case EventPropose:
		if n.role != Leader {
			return ErrNotLeader
		}
		return ErrReplicationPending
	case EventMessage:
		n.handleMessage(ev.Message)
		return nil
	default:
		return errors.New("raft: unknown event type")
	}
}

// Ready returns the work the driver must perform, and blocks further stepping
// until Advance reports it done.
func (n *Node) Ready() Ready {
	var r Ready
	if n.hardDirty {
		hs := HardState{Term: n.term, Vote: n.vote}
		r.HardState = &hs
	}
	if n.stateDirty {
		sc := StateChange{Role: n.role, Term: n.term, Leader: n.leader}
		r.StateChange = &sc
	}
	r.Messages = n.msgs

	if !r.IsEmpty() {
		n.readyOutstanding = true
	}
	return r
}

// Advance reports that the batch has been persisted, sent, and applied, in
// that order. The batch is accepted as an argument because it becomes
// meaningful once entries flow: the core then learns which of them reached
// the log store.
func (n *Node) Advance(Ready) {
	n.hardDirty = false
	n.stateDirty = false
	n.msgs = nil
	n.readyOutstanding = false
}

// --- timing -------------------------------------------------------------

func (n *Node) tick() {
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTicks {
			n.heartbeatElapsed = 0
			n.broadcastHeartbeat()
		}
		return
	}

	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		n.campaign()
	}
}

// resetElectionTimer draws a fresh timeout. The draw comes from the core's
// own seeded source, never a global one, so that a schedule replays exactly.
func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	spread := n.cfg.ElectionTickMax - n.cfg.ElectionTickMin
	n.electionTimeout = n.cfg.ElectionTickMin + n.rng.Intn(spread)
}

// --- role transitions ---------------------------------------------------

func (n *Node) becomeFollower(term Term, leader NodeID) {
	if term != n.term {
		// A new term carries no vote. Keeping one would let a member vote
		// twice across terms on the strength of a stale record.
		n.term = term
		n.vote = None
	}
	n.role = Follower
	n.leader = leader
	n.votes = nil
	n.resetElectionTimer()
}

func (n *Node) becomeCandidate() {
	n.term++
	n.vote = n.cfg.ID
	n.role = Candidate
	n.leader = None
	n.votes = map[NodeID]bool{n.cfg.ID: true}
	n.resetElectionTimer()
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leader = n.cfg.ID
	n.votes = nil
	n.heartbeatElapsed = 0

	// Assert leadership immediately rather than waiting a tick, so that other
	// members stop counting down toward an election they would lose.
	n.broadcastHeartbeat()
}

func (n *Node) campaign() {
	n.becomeCandidate()

	// A single-member group is its own quorum and wins without asking.
	if n.cfg.quorum() == 1 {
		n.becomeLeader()
		return
	}

	lastIndex := n.log.LastIndex()
	lastTerm, _ := n.log.Term(lastIndex)
	for _, peer := range n.cfg.Peers {
		if peer == n.cfg.ID {
			continue
		}
		n.send(Message{
			Type:         MsgRequestVote,
			To:           peer,
			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
		})
	}
}

func (n *Node) broadcastHeartbeat() {
	prevIndex := n.log.LastIndex()
	prevTerm, _ := n.log.Term(prevIndex)
	for _, peer := range n.cfg.Peers {
		if peer == n.cfg.ID {
			continue
		}
		n.send(Message{
			Type:         MsgAppend,
			To:           peer,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			LeaderCommit: n.commit,
		})
	}
}

func (n *Node) send(m Message) {
	m.From = n.cfg.ID
	m.Term = n.term
	n.msgs = append(n.msgs, m)
}

// --- message handling ---------------------------------------------------

func (n *Node) handleMessage(m Message) {
	switch {
	case m.Term > n.term:
		// A higher term is handled before anything else the message says. An
		// append in a higher term also identifies its sender as the leader.
		lead := None
		if m.Type == MsgAppend {
			lead = m.From
		}
		n.becomeFollower(m.Term, lead)

	case m.Term < n.term:
		// A stale request is answered so the sender learns the current term.
		// A stale response is discarded: it describes a term that is over.
		switch m.Type {
		case MsgRequestVote:
			n.send(Message{Type: MsgRequestVoteResp, To: m.From, Reject: true})
		case MsgAppend:
			n.send(Message{Type: MsgAppendResp, To: m.From, Reject: true})
		}
		return
	}

	switch m.Type {
	case MsgRequestVote:
		n.handleVoteRequest(m)
	case MsgRequestVoteResp:
		n.handleVoteResponse(m)
	case MsgAppend:
		n.handleAppend(m)
	case MsgAppendResp:
		// The leader's use of this arrives with replication.
	}
}

func (n *Node) handleVoteRequest(m Message) {
	// A member may repeat its vote to the same candidate, which makes a
	// duplicated or retried request harmless.
	canVote := n.vote == None || n.vote == m.From

	if canVote && n.candidateIsUpToDate(m) {
		n.vote = m.From
		// Granting a vote means a leader may be forming, so the voter stops
		// counting down toward its own candidacy.
		n.resetElectionTimer()
		n.send(Message{Type: MsgRequestVoteResp, To: m.From})
		return
	}
	n.send(Message{Type: MsgRequestVoteResp, To: m.From, Reject: true})
}

// candidateIsUpToDate compares logs by last term first, then by length. A
// longer log does not win against a log that has seen a later term, which is
// what keeps a committed entry from being lost to a stale but lengthy peer.
func (n *Node) candidateIsUpToDate(m Message) bool {
	lastIndex := n.log.LastIndex()
	lastTerm, _ := n.log.Term(lastIndex)

	if m.LastLogTerm != lastTerm {
		return m.LastLogTerm > lastTerm
	}
	return m.LastLogIndex >= lastIndex
}

func (n *Node) handleVoteResponse(m Message) {
	if n.role != Candidate {
		return
	}
	if _, seen := n.votes[m.From]; !seen {
		n.votes[m.From] = !m.Reject
	}

	granted, rejected := 0, 0
	for _, ok := range n.votes {
		if ok {
			granted++
		} else {
			rejected++
		}
	}

	switch {
	case granted >= n.cfg.quorum():
		n.becomeLeader()
	case rejected >= n.cfg.quorum():
		// This term cannot be won. Waiting out the timeout would work too,
		// but stepping down now frees the member to vote in the next term.
		n.becomeFollower(n.term, None)
	}
}

func (n *Node) handleAppend(m Message) {
	// An append in the current term settles who leads it, including for a
	// candidate that has not yet lost.
	n.becomeFollower(m.Term, m.From)

	prevTerm, err := n.log.Term(m.PrevLogIndex)
	if err != nil || prevTerm != m.PrevLogTerm {
		// The hint tells the leader where this member's log actually ends, so
		// it can back up by more than one index per round trip. The leader's
		// use of it arrives with replication.
		n.send(Message{
			Type:       MsgAppendResp,
			To:         m.From,
			Reject:     true,
			RejectHint: n.log.LastIndex() + 1,
		})
		return
	}

	n.send(Message{
		Type:       MsgAppendResp,
		To:         m.From,
		MatchIndex: m.PrevLogIndex + Index(len(m.Entries)),
	})
}
