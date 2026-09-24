package raft

import (
	"errors"
	"runtime"
	"testing"
	"time"
)

// settledGoroutines returns a goroutine count that has stopped moving, so a
// straggler from an earlier test cannot hide a new one.
func settledGoroutines(t testing.TB) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	last, stable := -1, 0
	for {
		runtime.Gosched()
		got := runtime.NumGoroutine()
		if got == last {
			if stable++; stable >= 3 {
				return got
			}
		} else {
			last, stable = got, 0
		}
		if time.Now().After(deadline) {
			return last
		}
		time.Sleep(time.Millisecond)
	}
}

// testPeer is a member plus the store a driver would write on its behalf.
// The two travel together because completing a Ready cycle without writing
// the batch is not a shortcut, it is a different protocol.
type testPeer struct {
	*Node
	store *MemoryLog
}

func newTestNode(t testing.TB, id NodeID, peers ...NodeID) *testPeer {
	t.Helper()
	store := NewMemoryLog()
	n, err := NewNode(Config{ID: id, Peers: peers, Seed: 1}, store, HardState{})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return &testPeer{Node: n, store: store}
}

// drive performs the work in a batch in the order the contract requires, then
// reports it done.
func drive(t testing.TB, p *testPeer) Ready {
	t.Helper()
	r := p.Ready()
	if len(r.Entries) > 0 {
		if err := ApplyEntries(p.store, r.Entries); err != nil {
			t.Fatalf("persisting staged entries: %v", err)
		}
	}
	p.Advance(r)
	return r
}

// step applies an event and completes the Ready cycle, which is what a driver
// does. Tests that care about the cycle itself drive it by hand.
func step(t testing.TB, p *testPeer, ev Event) Ready {
	t.Helper()
	if err := p.Step(ev); err != nil {
		t.Fatalf("step %v: %v", ev.Type, err)
	}
	return drive(t, p)
}

func recv(t testing.TB, p *testPeer, m Message) Ready {
	t.Helper()
	return step(t, p, Event{Type: EventMessage, Message: m})
}

func messagesOfType(r Ready, typ MessageType) []Message {
	var out []Message
	for _, m := range r.Messages {
		if m.Type == typ {
			out = append(out, m)
		}
	}
	return out
}

// --- starting state -----------------------------------------------------

func TestNewNodeStartsAsFollower(t *testing.T) {
	n := newTestNode(t, 1, 1, 2, 3)
	s := n.Status()
	if s.Role != Follower || s.Term != 0 || s.Vote != None || s.Leader != None {
		t.Fatalf("fresh node: %+v", s)
	}
}

func TestNewNodeRestoresDurableState(t *testing.T) {
	n, err := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}}, NewMemoryLog(), HardState{Term: 7, Vote: 2})
	if err != nil {
		t.Fatal(err)
	}
	if s := n.Status(); s.Term != 7 || s.Vote != 2 {
		t.Fatalf("restored state: %+v", s)
	}
	// Restored state is already durable and must not be staged for writing.
	if r := n.Ready(); !r.IsEmpty() {
		t.Fatalf("restore staged work: %+v", r)
	}
}

// --- elections ----------------------------------------------------------

func TestElectionTimeoutStartsCampaign(t *testing.T) {
	n := newTestNode(t, 1, 1, 2, 3)

	var last Ready
	for i := 0; i < DefaultElectionTickMax; i++ {
		last = step(t, n, Event{Type: EventTick})
		if n.Status().Role == Candidate {
			break
		}
	}

	s := n.Status()
	if s.Role != Candidate {
		t.Fatalf("no campaign after %d ticks: %+v", DefaultElectionTickMax, s)
	}
	if s.Term != 1 || s.Vote != 1 {
		t.Fatalf("candidate must advance its term and vote for itself: %+v", s)
	}
	if last.HardState == nil || last.HardState.Term != 1 || last.HardState.Vote != 1 {
		t.Fatalf("term and vote must be staged for durability: %+v", last.HardState)
	}
	votes := messagesOfType(last, MsgRequestVote)
	if len(votes) != 2 {
		t.Fatalf("sent %d vote requests, want one per peer", len(votes))
	}
}

func TestSingleMemberGroupElectsItself(t *testing.T) {
	n := newTestNode(t, 1, 1)
	step(t, n, Event{Type: EventCampaign})

	if s := n.Status(); s.Role != Leader || s.Leader != 1 {
		t.Fatalf("a lone member is its own quorum: %+v", s)
	}
}

func TestCandidateWinsOnQuorum(t *testing.T) {
	n := newTestNode(t, 1, 1, 2, 3)
	step(t, n, Event{Type: EventCampaign})

	r := recv(t, n, Message{Type: MsgRequestVoteResp, From: 2, To: 1, Term: 1})

	s := n.Status()
	if s.Role != Leader {
		t.Fatalf("two of three votes must win: %+v", s)
	}
	if len(messagesOfType(r, MsgAppend)) != 2 {
		t.Fatal("a new leader must assert itself immediately rather than waiting a tick")
	}
	if r.StateChange == nil || r.StateChange.Role != Leader {
		t.Fatalf("leadership must be reported: %+v", r.StateChange)
	}
}

func TestCandidateStepsDownWhenItCannotWin(t *testing.T) {
	n := newTestNode(t, 1, 1, 2, 3)
	step(t, n, Event{Type: EventCampaign})

	recv(t, n, Message{Type: MsgRequestVoteResp, From: 2, To: 1, Term: 1, Reject: true})
	recv(t, n, Message{Type: MsgRequestVoteResp, From: 3, To: 1, Term: 1, Reject: true})

	if s := n.Status(); s.Role != Follower {
		t.Fatalf("a candidate rejected by a quorum cannot win: %+v", s)
	}
}

// A duplicated response must not be counted twice, or one peer could elect a
// leader on its own.
func TestDuplicateVoteResponseCountsOnce(t *testing.T) {
	n := newTestNode(t, 1, 1, 2, 3, 4, 5)
	step(t, n, Event{Type: EventCampaign})

	for i := 0; i < 3; i++ {
		recv(t, n, Message{Type: MsgRequestVoteResp, From: 2, To: 1, Term: 1})
	}
	if s := n.Status(); s.Role != Candidate {
		t.Fatalf("one peer repeated three times must not form a quorum of five: %+v", s)
	}
}

// --- terms --------------------------------------------------------------

func TestHigherTermForcesFollowerAndClearsVote(t *testing.T) {
	n := newTestNode(t, 1, 1, 2, 3)
	step(t, n, Event{Type: EventCampaign})
	if s := n.Status(); s.Vote != 1 {
		t.Fatalf("expected a self vote: %+v", s)
	}

	recv(t, n, Message{Type: MsgRequestVoteResp, From: 2, To: 1, Term: 9})

	s := n.Status()
	if s.Role != Follower || s.Term != 9 {
		t.Fatalf("a higher term must depose: %+v", s)
	}
	if s.Vote != None {
		t.Fatal("a new term carries no vote; keeping one would allow voting twice on a stale record")
	}
}

func TestStaleRequestIsAnsweredStaleResponseIsIgnored(t *testing.T) {
	n := newTestNode(t, 1, 1, 2, 3)
	step(t, n, Event{Type: EventCampaign}) // term 1

	r := recv(t, n, Message{Type: MsgRequestVote, From: 2, To: 1, Term: 0})
	replies := messagesOfType(r, MsgRequestVoteResp)
	if len(replies) != 1 || !replies[0].Reject || replies[0].Term != 1 {
		t.Fatalf("a stale request must be told the current term: %+v", replies)
	}

	r = recv(t, n, Message{Type: MsgRequestVoteResp, From: 3, To: 1, Term: 0})
	if len(r.Messages) != 0 {
		t.Fatalf("a stale response must be discarded: %+v", r.Messages)
	}
}

// --- voting -------------------------------------------------------------

func TestVoteGrantedOncePerTerm(t *testing.T) {
	n := newTestNode(t, 1, 1, 2, 3)

	r := recv(t, n, Message{Type: MsgRequestVote, From: 2, To: 1, Term: 1})
	if replies := messagesOfType(r, MsgRequestVoteResp); len(replies) != 1 || replies[0].Reject {
		t.Fatalf("first request in a term must be granted: %+v", replies)
	}

	// The same candidate may repeat itself; a retry is not a second vote.
	r = recv(t, n, Message{Type: MsgRequestVote, From: 2, To: 1, Term: 1})
	if replies := messagesOfType(r, MsgRequestVoteResp); replies[0].Reject {
		t.Fatal("a repeated request from the same candidate must be granted again")
	}

	r = recv(t, n, Message{Type: MsgRequestVote, From: 3, To: 1, Term: 1})
	if replies := messagesOfType(r, MsgRequestVoteResp); !replies[0].Reject {
		t.Fatal("a second candidate in the same term must be refused")
	}
}

// Logs are compared by last term first and only then by length. A longer log
// must lose to one that has seen a later term, which is what stops a
// committed entry being lost to a stale but lengthy peer.
func TestVoteComparesLastTermBeforeLength(t *testing.T) {
	voterLog := NewMemoryLogWith(
		Entry{Index: 1, Term: 1}, Entry{Index: 2, Term: 1}, Entry{Index: 3, Term: 1})

	cases := map[string]struct {
		lastIndex Index
		lastTerm  Term
		grant     bool
	}{
		"shorter log, later term": {lastIndex: 1, lastTerm: 2, grant: true},
		"same term, longer":       {lastIndex: 4, lastTerm: 1, grant: true},
		"same term, equal":        {lastIndex: 3, lastTerm: 1, grant: true},
		"same term, shorter":      {lastIndex: 2, lastTerm: 1, grant: false},
		"longer log, older term":  {lastIndex: 9, lastTerm: 0, grant: false},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			log := NewMemoryLogWith(voterLog.ents[1:]...)
			node, err := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}}, log, HardState{})
			if err != nil {
				t.Fatal(err)
			}
			n := &testPeer{Node: node, store: log}
			r := recv(t, n, Message{
				Type: MsgRequestVote, From: 2, To: 1, Term: 5,
				LastLogIndex: tc.lastIndex, LastLogTerm: tc.lastTerm,
			})
			granted := !messagesOfType(r, MsgRequestVoteResp)[0].Reject
			if granted != tc.grant {
				t.Fatalf("granted=%v, want %v", granted, tc.grant)
			}
		})
	}
}

// --- leadership maintenance ---------------------------------------------

func electLeader(t *testing.T, n *testPeer) {
	t.Helper()
	step(t, n, Event{Type: EventCampaign})
	recv(t, n, Message{Type: MsgRequestVoteResp, From: 2, To: 1, Term: n.Status().Term})
	if n.Status().Role != Leader {
		t.Fatalf("failed to elect: %+v", n.Status())
	}
}

func TestLeaderHeartbeatsOnSchedule(t *testing.T) {
	n := newTestNode(t, 1, 1, 2, 3)
	electLeader(t, n)

	for i := 0; i < 3; i++ {
		r := step(t, n, Event{Type: EventTick})
		if got := len(messagesOfType(r, MsgAppend)); got != 2 {
			t.Fatalf("tick %d produced %d heartbeats, want one per peer", i, got)
		}
	}
}

func TestHeartbeatSuppressesElection(t *testing.T) {
	n := newTestNode(t, 1, 1, 2, 3)

	// Far more ticks than an election timeout, with a heartbeat interleaved
	// often enough to keep resetting it.
	for i := 0; i < DefaultElectionTickMax*3; i++ {
		step(t, n, Event{Type: EventTick})
		if i%(DefaultElectionTickMin-1) == 0 {
			recv(t, n, Message{Type: MsgAppend, From: 2, To: 1, Term: 1})
		}
		if n.Status().Role != Follower {
			t.Fatalf("campaigned at tick %d despite heartbeats", i)
		}
	}
	if s := n.Status(); s.Leader != 2 {
		t.Fatalf("heartbeats must identify the leader: %+v", s)
	}
}

func TestAppendWithMismatchedPrevIsRejectedWithHint(t *testing.T) {
	n := newTestNode(t, 1, 1, 2, 3)

	r := recv(t, n, Message{
		Type: MsgAppend, From: 2, To: 1, Term: 1,
		PrevLogIndex: 5, PrevLogTerm: 3,
	})
	replies := messagesOfType(r, MsgAppendResp)
	if len(replies) != 1 || !replies[0].Reject {
		t.Fatalf("an append beyond this log must be rejected: %+v", replies)
	}
	if replies[0].RejectHint != 1 {
		t.Fatalf("hint %d, want the first index this member lacks", replies[0].RejectHint)
	}
}

// --- the Ready cycle ----------------------------------------------------

func TestReadyBlocksSteppingUntilAdvance(t *testing.T) {
	n := newTestNode(t, 1, 1, 2, 3)
	if err := n.Step(Event{Type: EventCampaign}); err != nil {
		t.Fatal(err)
	}

	r := n.Ready()
	if r.IsEmpty() {
		t.Fatal("campaigning must produce work")
	}
	if err := n.Step(Event{Type: EventTick}); !errors.Is(err, ErrReadyOutstanding) {
		t.Fatalf("stepping with work outstanding = %v, want ErrReadyOutstanding", err)
	}

	n.Advance(r)
	if err := n.Step(Event{Type: EventTick}); err != nil {
		t.Fatalf("stepping after Advance: %v", err)
	}
}

// An empty batch must not block the driver, or a tick that changes nothing
// would wedge the cycle.
func TestEmptyReadyDoesNotBlock(t *testing.T) {
	n := newTestNode(t, 1, 1, 2, 3)
	if r := n.Ready(); !r.IsEmpty() {
		t.Fatalf("nothing has happened yet: %+v", r)
	}
	if err := n.Step(Event{Type: EventTick}); err != nil {
		t.Fatalf("an empty Ready must not block: %v", err)
	}
}

// State is staged because it changed, not because a branch remembered to flag
// it: a tick that changes nothing must stage nothing.
func TestQuietTickStagesNothing(t *testing.T) {
	n := newTestNode(t, 1, 1, 2, 3)
	r := step(t, n, Event{Type: EventTick})
	if r.HardState != nil || r.StateChange != nil || len(r.Messages) != 0 {
		t.Fatalf("a quiet tick staged work: %+v", r)
	}
}

func TestProposalRequiresLeadership(t *testing.T) {
	n := newTestNode(t, 1, 1, 2, 3)
	if err := n.Step(Event{Type: EventPropose, Data: []byte("x")}); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("propose to a follower = %v, want ErrNotLeader", err)
	}
	n.Advance(n.Ready())

	electLeader(t, n)
	before := n.Status().LastIndex
	r := step(t, n, Event{Type: EventPropose, Data: []byte("x")})
	if n.Status().LastIndex != before+1 {
		t.Fatalf("proposal did not extend the log: %d then %d", before, n.Status().LastIndex)
	}
	if len(r.Entries) == 0 {
		t.Fatal("a proposal must be staged for the driver to persist")
	}
}

// Randomness comes from the core's own seeded source, so two members with the
// same seed must behave identically and two with different seeds must not.
func TestElectionTimeoutIsSeeded(t *testing.T) {
	ticksToCampaign := func(seed int64) int {
		n, err := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}, Seed: seed}, NewMemoryLog(), HardState{})
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; ; i++ {
			n.Step(Event{Type: EventTick})
			n.Advance(n.Ready())
			if n.Status().Role == Candidate {
				return i
			}
		}
	}

	if a, b := ticksToCampaign(42), ticksToCampaign(42); a != b {
		t.Fatalf("same seed produced %d and %d ticks", a, b)
	}
	var seen = map[int]bool{}
	for seed := int64(0); seed < 25; seed++ {
		seen[ticksToCampaign(seed)] = true
	}
	if len(seen) < 2 {
		t.Fatal("every seed produced the same timeout; the draw is not random")
	}
}

// --- bounds -------------------------------------------------------------

// An append must respect both the entry count and the byte budget, and must
// still carry one entry when a single command exceeds the budget on its own.
func TestAppendRespectsMessageLimits(t *testing.T) {
	store := NewMemoryLog()
	node, err := NewNode(Config{
		ID: 1, Peers: []NodeID{1, 2, 3},
		MaxEntriesPerMsg: 4,
		MaxBytesPerMsg:   512,
	}, store, HardState{})
	if err != nil {
		t.Fatal(err)
	}
	p := &testPeer{Node: node, store: store}

	step(t, p, Event{Type: EventCampaign})
	recv(t, p, Message{Type: MsgRequestVoteResp, From: 2, To: 1, Term: 1})

	for i := 0; i < 20; i++ {
		step(t, p, Event{Type: EventPropose, Data: []byte("small")})
	}

	// Force a peer all the way back so the next append has plenty to carry.
	p.progress[2].Next = 1
	p.progress[2].inflight = false
	r := step(t, p, Event{Type: EventTick})

	var sent *Message
	for i := range r.Messages {
		if r.Messages[i].To == 2 && r.Messages[i].Type == MsgAppend {
			sent = &r.Messages[i]
		}
	}
	if sent == nil {
		t.Fatal("no append was sent to the lagging peer")
	}
	if len(sent.Entries) > 4 {
		t.Fatalf("append carried %d entries, above the limit of 4", len(sent.Entries))
	}
	var bytes uint64
	for _, e := range sent.Entries {
		bytes += entrySize(e)
	}
	if len(sent.Entries) > 1 && bytes > 512 {
		t.Fatalf("append carried %d bytes, above the budget of 512", bytes)
	}
}

// The core must start nothing. A goroutine here would have no owner, no way
// to be stopped, and no place in a value that is supposed to be steppable
// from a single thread.
func TestCoreStartsNoGoroutines(t *testing.T) {
	baseline := settledGoroutines(t)

	p := newTestNode(t, 1, 1, 2, 3)
	step(t, p, Event{Type: EventCampaign})
	recv(t, p, Message{Type: MsgRequestVoteResp, From: 2, To: 1, Term: 1})
	for i := 0; i < 50; i++ {
		step(t, p, Event{Type: EventPropose, Data: []byte("x")})
		step(t, p, Event{Type: EventTick})
	}

	if got := settledGoroutines(t); got > baseline {
		t.Fatalf("%d goroutines after stepping a core, baseline is %d", got, baseline)
	}
}
