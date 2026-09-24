package raft

import (
	"errors"
	"math/rand"
	"sort"
)

var (
	// ErrNotLeader means a proposal reached a member that cannot accept it.
	ErrNotLeader = errors.New("raft: this member is not the leader")

	// ErrReadyOutstanding means the driver stepped the core again before
	// performing the work it was already handed. Allowing that would let a
	// message be sent describing state the driver has not yet made durable.
	ErrReadyOutstanding = errors.New("raft: a Ready batch is outstanding; call Advance first")
)

// progress is a leader's view of one member's log.
type progress struct {
	// Next is the index of the next entry to send.
	Next Index

	// Match is the highest index known to be held by that member. It only
	// ever moves forward, because a stale response must not retract what a
	// later one already confirmed.
	Match Index

	// inflight marks an unanswered append. Exactly one may be outstanding per
	// peer: pipelining is not part of this milestone, and one at a time keeps
	// flow control simple enough to reason about without measurement. A
	// heartbeat clears it, so a lost response cannot wedge a peer.
	inflight bool
}

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
	log *raftLog
	rng *rand.Rand

	// Durable once the driver has written the HardState in a Ready batch.
	term Term
	vote NodeID

	// Rebuilt on restart rather than persisted.
	role   Role
	leader NodeID

	electionElapsed  int
	heartbeatElapsed int
	electionTimeout  int

	// votes records responses to this member's own candidacy, true for a
	// grant. It is discarded whenever the term changes.
	votes map[NodeID]bool

	// progress is the leader's view of every member, nil otherwise.
	progress map[NodeID]*progress

	// Staged output, drained by Ready and cleared by Advance.
	msgs       []Message
	hardDirty  bool
	stateDirty bool

	readyOutstanding bool

	counters counters
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
		cfg:      cfg,
		log:      newRaftLog(log),
		rng:      rand.New(rand.NewSource(cfg.Seed)),
		term:     hs.Term,
		vote:     hs.Vote,
		role:     Follower,
		leader:   None,
		counters: newCounters(),
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
		Commit:    n.log.committed,
		LastIndex: n.log.lastIndex(),
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
		return n.propose(ev.Data)
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
	if staged := n.log.unstable; len(staged) > 0 {
		r.Entries = append([]Entry(nil), staged...)
	}
	r.Messages = n.msgs
	r.CommittedEntries = n.log.nextCommitted(n.cfg.MaxBytesPerMsg)

	if !r.IsEmpty() {
		n.readyOutstanding = true
	}
	return r
}

// Advance reports that the batch has been persisted, sent, and applied, in
// that order. The batch is accepted as an argument because it becomes
// meaningful once entries flow: the core then learns which of them reached
// the log store.
func (n *Node) Advance(r Ready) {
	if k := len(r.Entries); k > 0 {
		n.log.stableTo(r.Entries[k-1].Index)
	}
	if k := len(r.CommittedEntries); k > 0 {
		n.log.appliedTo(r.CommittedEntries[k-1].Index)
	}
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
			// Clearing the outstanding flag makes the heartbeat double as a
			// retry, so a lost response cannot silence a peer indefinitely.
			for _, peer := range n.cfg.Peers {
				if peer != n.cfg.ID {
					n.progress[peer].inflight = false
				}
			}
			n.broadcastAppend(true)
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

func (n *Node) becomeFollower(term Term, leader NodeID, reason string) {
	n.progress = nil
	prevRole, prevLeader := n.role, n.leader
	if term != n.term {
		n.observe(Observation{Kind: ObsTermAdvanced, Term: term, Reason: reason})
		// A new term carries no vote. Keeping one would let a member vote
		// twice across terms on the strength of a stale record.
		n.term = term
		n.vote = None
	}
	n.role = Follower
	n.leader = leader
	n.votes = nil
	n.resetElectionTimer()

	if prevRole != Follower || prevLeader != leader {
		n.observe(Observation{Kind: ObsRoleChange, Peer: leader, Reason: reason})
	}
}

func (n *Node) becomeCandidate(reason string) {
	n.term++
	n.vote = n.cfg.ID
	n.role = Candidate
	n.leader = None
	n.progress = nil
	n.votes = map[NodeID]bool{n.cfg.ID: true}
	n.resetElectionTimer()

	n.counters.electionsStarted++
	n.observe(Observation{Kind: ObsElectionStarted, Reason: reason})
	n.observe(Observation{Kind: ObsRoleChange})
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leader = n.cfg.ID
	n.votes = nil
	n.heartbeatElapsed = 0

	last := n.log.lastIndex()
	n.progress = make(map[NodeID]*progress, len(n.cfg.Peers))
	for _, peer := range n.cfg.Peers {
		n.progress[peer] = &progress{Next: last + 1}
	}

	// A new leader appends one empty entry in its own term.
	//
	// Without it a leader that inherited only entries from earlier terms
	// could never commit them: the commit rule refuses to count replicas of
	// an earlier term's entry on their own, so the log would sit
	// committed-short until an unrelated proposal happened to arrive.
	n.log.append(Entry{Term: n.term, Index: last + 1})
	self := n.progress[n.cfg.ID]
	self.Match = n.log.lastIndex()
	self.Next = self.Match + 1

	n.counters.electionsWon++
	n.counters.entriesAppended++
	n.observe(Observation{Kind: ObsElectionWon, Index: last + 1})
	n.observe(Observation{Kind: ObsRoleChange})
	n.observe(Observation{Kind: ObsProgressReset, Index: last + 1})

	// Assert leadership immediately rather than waiting a tick, so that other
	// members stop counting down toward an election they would lose.
	n.broadcastAppend(true)
}

func (n *Node) campaign() {
	// A campaign that starts before the timeout elapsed was forced, which is
	// worth distinguishing in a trace from one the clock produced.
	reason := ReasonTimeout
	if n.electionElapsed < n.electionTimeout {
		reason = ReasonForced
	}
	n.becomeCandidate(reason)

	// A single-member group is its own quorum and wins without asking.
	if n.cfg.quorum() == 1 {
		n.becomeLeader()
		return
	}

	lastIndex := n.log.lastIndex()
	lastTerm := n.log.lastTerm()
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

// propose appends a command to the leader's own log and starts replicating it.
func (n *Node) propose(data []byte) error {
	if n.role != Leader {
		return ErrNotLeader
	}

	e := Entry{Term: n.term, Index: n.log.lastIndex() + 1, Data: append([]byte(nil), data...)}
	n.log.append(e)
	n.counters.entriesAppended++

	self := n.progress[n.cfg.ID]
	self.Match = e.Index
	self.Next = e.Index + 1

	// A lone member is its own quorum, so its proposal is committed here.
	n.maybeAdvanceCommit()
	n.broadcastAppend(false)
	return nil
}

// broadcastAppend offers every peer whatever it is missing. When force is
// set an empty append is sent to a peer that needs nothing, which is what
// keeps a quiet leader from being deposed.
func (n *Node) broadcastAppend(force bool) {
	for _, peer := range n.cfg.Peers {
		if peer == n.cfg.ID {
			continue
		}
		n.maybeSendAppend(peer, force)
	}
}

func (n *Node) maybeSendAppend(to NodeID, force bool) {
	pr := n.progress[to]
	if pr == nil || (pr.inflight && !force) {
		return
	}

	prevIndex := pr.Next - 1
	prevTerm, err := n.log.term(prevIndex)
	if err != nil {
		// The entry this member needs is no longer held. Catching it up needs
		// a snapshot, which arrives with the replicated shard.
		return
	}

	ents, err := n.log.entries(pr.Next, n.log.lastIndex()+1, n.cfg.MaxBytesPerMsg)
	if err != nil {
		ents = nil
	}
	if len(ents) > n.cfg.MaxEntriesPerMsg {
		ents = ents[:n.cfg.MaxEntriesPerMsg]
	}
	if len(ents) == 0 && !force {
		return
	}

	n.send(Message{
		Type:         MsgAppend,
		To:           to,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      ents,
		LeaderCommit: n.log.committed,
	})
	pr.inflight = true
}

// maybeAdvanceCommit moves the commit index to the highest position a quorum
// holds, but only when that position carries an entry from the current term.
//
// Counting replicas of an earlier term's entry is the classic way to commit
// something that a later leader can still overwrite, so the term check is not
// an optimization and cannot be relaxed.
func (n *Node) maybeAdvanceCommit() bool {
	matches := make([]Index, 0, len(n.cfg.Peers))
	for _, peer := range n.cfg.Peers {
		if peer == n.cfg.ID {
			matches = append(matches, n.log.lastIndex())
			continue
		}
		matches = append(matches, n.progress[peer].Match)
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })

	candidate := matches[n.cfg.quorum()-1]
	if candidate <= n.log.committed {
		return false
	}
	if t, err := n.log.term(candidate); err == nil && t == n.term {
		n.log.commitTo(candidate)
		n.counters.commitAdvances++
		n.observe(Observation{Kind: ObsCommitAdvanced, Index: n.log.committed})
		return true
	}
	return false
}

// lastIndexOfTerm finds this member's last entry in a given term, which lets
// a leader skip a whole conflicting term in one round trip.
func (n *Node) lastIndexOfTerm(t Term) (Index, bool) {
	for i := n.log.lastIndex(); i > 0; i-- {
		term, err := n.log.term(i)
		if err != nil {
			return 0, false
		}
		switch {
		case term == t:
			return i, true
		case term < t:
			return 0, false
		}
	}
	return 0, false
}

func (n *Node) send(m Message) {
	m.From = n.cfg.ID
	m.Term = n.term
	n.counters.sent[m.Type]++
	n.msgs = append(n.msgs, m)
}

// --- message handling ---------------------------------------------------

func (n *Node) handleMessage(m Message) {
	n.counters.received[m.Type]++

	switch {
	case m.Term > n.term:
		// A higher term is handled before anything else the message says. An
		// append in a higher term also identifies its sender as the leader.
		lead := None
		if m.Type == MsgAppend {
			lead = m.From
		}
		n.becomeFollower(m.Term, lead, ReasonHigherTerm)

	case m.Term < n.term:
		// A stale request is answered so the sender learns the current term.
		// A stale response is discarded: it describes a term that is over.
		switch m.Type {
		case MsgRequestVote:
			n.observe(Observation{Kind: ObsVoteDenied, Peer: m.From, Reason: ReasonStaleTerm})
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
		n.handleAppendResp(m)
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
		n.observe(Observation{Kind: ObsVoteGranted, Peer: m.From})
		n.send(Message{Type: MsgRequestVoteResp, To: m.From})
		return
	}

	reason := ReasonLogBehind
	if !canVote {
		reason = ReasonAlreadyVoted
	}
	n.observe(Observation{Kind: ObsVoteDenied, Peer: m.From, Reason: reason})
	n.send(Message{Type: MsgRequestVoteResp, To: m.From, Reject: true})
}

// candidateIsUpToDate compares logs by last term first, then by length. A
// longer log does not win against a log that has seen a later term, which is
// what keeps a committed entry from being lost to a stale but lengthy peer.
func (n *Node) candidateIsUpToDate(m Message) bool {
	lastIndex := n.log.lastIndex()
	lastTerm := n.log.lastTerm()

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
		n.counters.electionsLost++
		n.observe(Observation{Kind: ObsElectionLost, Reason: ReasonQuorumRejected})
		n.becomeFollower(n.term, None, ReasonQuorumRejected)
	}
}

func (n *Node) handleAppend(m Message) {
	// An append in the current term settles who leads it, including for a
	// candidate that has not yet lost.
	n.becomeFollower(m.Term, m.From, ReasonLeaderAppend)

	prevTerm, err := n.log.term(m.PrevLogIndex)
	if err != nil || prevTerm != m.PrevLogTerm {
		hint, hintTerm := n.conflictHint(m.PrevLogIndex, prevTerm, err)
		n.counters.appendsRejected++
		n.observe(Observation{Kind: ObsAppendRejected, Peer: m.From, Index: hint, Reason: ReasonLogMismatch})
		n.send(Message{
			Type:       MsgAppendResp,
			To:         m.From,
			Reject:     true,
			RejectHint: hint,
			RejectTerm: hintTerm,
		})
		return
	}

	replacedFrom, appended := n.log.truncateAndAppend(m.Entries)
	n.counters.entriesAppended += uint64(appended)
	if replacedFrom > 0 {
		n.observe(Observation{Kind: ObsLogTruncated, Peer: m.From, Index: replacedFrom})
	}

	lastNew := m.PrevLogIndex + Index(len(m.Entries))
	if m.LeaderCommit > n.log.committed {
		// A follower may only commit as far as it can actually see. Trusting
		// the leader's index past its own log would report entries applied
		// that it does not hold.
		before := n.log.committed
		n.log.commitTo(min(m.LeaderCommit, lastNew))
		if n.log.committed > before {
			n.counters.commitAdvances++
			n.observe(Observation{Kind: ObsCommitAdvanced, Index: n.log.committed})
		}
	}

	n.send(Message{
		Type:       MsgAppendResp,
		To:         m.From,
		MatchIndex: lastNew,
	})
}

// conflictHint tells the leader where to resume after a rejected append.
//
// Reporting the first index of the conflicting term rather than one index
// back is what lets a follower that is a whole term behind be caught up in
// one round trip instead of one per entry.
func (n *Node) conflictHint(prevIndex Index, prevTerm Term, lookupErr error) (Index, Term) {
	if lookupErr != nil {
		return n.log.lastIndex() + 1, 0
	}
	first := prevIndex
	for first > 1 {
		t, err := n.log.term(first - 1)
		if err != nil || t != prevTerm {
			break
		}
		first--
	}
	return first, prevTerm
}

func (n *Node) handleAppendResp(m Message) {
	if n.role != Leader {
		return
	}
	pr := n.progress[m.From]
	if pr == nil {
		return
	}
	pr.inflight = false

	if m.Reject {
		next := m.RejectHint
		if m.RejectTerm > 0 {
			if idx, ok := n.lastIndexOfTerm(m.RejectTerm); ok {
				next = idx + 1
			}
		}
		if next < 1 {
			next = 1
		}
		// Only ever back up. A reordered rejection must not undo progress a
		// later acceptance already established.
		if next < pr.Next {
			pr.Next = next
		}
		n.maybeSendAppend(m.From, true)
		return
	}

	if m.MatchIndex > pr.Match {
		pr.Match = m.MatchIndex
		pr.Next = pr.Match + 1
		if n.maybeAdvanceCommit() {
			// Tell everyone at once. A follower otherwise learns of a commit
			// only on the next heartbeat, which delays application by a tick
			// for no reason.
			n.broadcastAppend(true)
			return
		}
	}
	n.maybeSendAppend(m.From, false)
}
