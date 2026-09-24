package raft

import (
	"fmt"
	"testing"
)

// cluster is the smallest thing that can exercise replication: several members
// and a deterministic message pump. It has no faults, no delay and no
// reordering; those belong to the simulator, and this exists only so that
// replication can be tested before the simulator does.
type cluster struct {
	t       testing.TB
	ids     []NodeID
	peers   map[NodeID]*testPeer
	inbox   []Message
	applied map[NodeID][]Entry
	sent    int
}

func newCluster(t testing.TB, ids ...NodeID) *cluster {
	t.Helper()
	c := &cluster{t: t, ids: ids, peers: map[NodeID]*testPeer{}, applied: map[NodeID][]Entry{}}
	for _, id := range ids {
		c.peers[id] = c.newPeer(id, NewMemoryLog())
	}
	return c
}

func (c *cluster) newPeer(id NodeID, store *MemoryLog) *testPeer {
	c.t.Helper()
	n, err := NewNode(Config{ID: id, Peers: c.ids, Seed: int64(id)}, store, HardState{})
	if err != nil {
		c.t.Fatalf("NewNode %d: %v", id, err)
	}
	return &testPeer{Node: n, store: store}
}

// preloadAt replaces a member's log and durable term, so a test can start it
// from a history that would take many steps to produce.
//
// The term matters as much as the entries. A member restored with entries
// from a term above its own current term is a state no restart can produce,
// and building one makes a test prove something about a situation that cannot
// occur.
func (c *cluster) preloadAt(id NodeID, term Term, ents ...Entry) {
	c.t.Helper()
	store := NewMemoryLogWith(ents...)
	n, err := NewNode(Config{ID: id, Peers: c.ids, Seed: int64(id)}, store, HardState{Term: term})
	if err != nil {
		c.t.Fatalf("NewNode %d: %v", id, err)
	}
	c.peers[id] = &testPeer{Node: n, store: store}
}

func (c *cluster) collect(p *testPeer) {
	c.t.Helper()
	r := drive(c.t, p)
	c.inbox = append(c.inbox, r.Messages...)
	c.sent += len(r.Messages)
	if len(r.CommittedEntries) > 0 {
		c.applied[p.Status().ID] = append(c.applied[p.Status().ID], r.CommittedEntries...)
	}
}

// pump delivers messages until the cluster goes quiet.
func (c *cluster) pump() {
	c.t.Helper()
	for round := 0; len(c.inbox) > 0; round++ {
		if round > 500 {
			c.t.Fatal("cluster never settled")
		}
		batch := c.inbox
		c.inbox = nil
		for _, m := range batch {
			p := c.peers[m.To]
			if p == nil {
				continue
			}
			if err := p.Step(Event{Type: EventMessage, Message: m}); err != nil {
				c.t.Fatalf("deliver to %d: %v", m.To, err)
			}
			c.collect(p)
		}
	}
}

func (c *cluster) campaign(id NodeID) {
	c.t.Helper()
	p := c.peers[id]
	if err := p.Step(Event{Type: EventCampaign}); err != nil {
		c.t.Fatalf("campaign: %v", err)
	}
	c.collect(p)
	c.pump()
}

func (c *cluster) propose(id NodeID, data string) {
	c.t.Helper()
	p := c.peers[id]
	if err := p.Step(Event{Type: EventPropose, Data: []byte(data)}); err != nil {
		c.t.Fatalf("propose: %v", err)
	}
	c.collect(p)
	c.pump()
}

func (c *cluster) logOf(id NodeID) []Entry {
	c.t.Helper()
	p := c.peers[id]
	last := p.store.LastIndex()
	if last == 0 {
		return nil
	}
	ents, err := p.store.Entries(1, last+1, 1<<30)
	if err != nil {
		c.t.Fatalf("reading log of %d: %v", id, err)
	}
	return ents
}

// expectLogsAgree checks that no two members hold different entries at the
// same index, which is the property that makes a replicated log a log.
func (c *cluster) expectLogsAgree() {
	c.t.Helper()
	for _, a := range c.ids {
		for _, b := range c.ids {
			la, lb := c.logOf(a), c.logOf(b)
			for i := 0; i < len(la) && i < len(lb); i++ {
				if la[i].Term != lb[i].Term || string(la[i].Data) != string(lb[i].Data) {
					c.t.Fatalf("members %d and %d disagree at index %d: %+v vs %+v",
						a, b, la[i].Index, la[i], lb[i])
				}
			}
		}
	}
}

// --- basic replication --------------------------------------------------

func TestLeaderAppendsAnEntryInItsOwnTerm(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	c.campaign(1)

	if s := c.peers[1].Status(); s.Role != Leader {
		t.Fatalf("expected leadership: %+v", s)
	}
	log := c.logOf(1)
	if len(log) != 1 || log[0].Term != c.peers[1].Status().Term {
		t.Fatalf("a new leader must append an entry in its own term: %+v", log)
	}
}

func TestProposalReplicatesAndCommits(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	c.campaign(1)
	c.propose(1, "alpha")
	c.propose(1, "beta")

	c.expectLogsAgree()

	for _, id := range c.ids {
		if got := len(c.logOf(id)); got != 3 {
			t.Fatalf("member %d holds %d entries, want the no-op plus two proposals", id, got)
		}
	}
	if got := c.peers[1].Status().Commit; got != 3 {
		t.Fatalf("leader commit %d, want 3", got)
	}

	// Every member must apply the same commands in the same order.
	var reference []string
	for _, id := range c.ids {
		var got []string
		for _, e := range c.applied[id] {
			if len(e.Data) > 0 {
				got = append(got, string(e.Data))
			}
		}
		if reference == nil {
			reference = got
			continue
		}
		if fmt.Sprint(got) != fmt.Sprint(reference) {
			t.Fatalf("member %d applied %v, member %d applied %v", c.ids[0], reference, id, got)
		}
	}
	if fmt.Sprint(reference) != "[alpha beta]" {
		t.Fatalf("applied %v, want [alpha beta]", reference)
	}
}

func TestSingleMemberCommitsImmediately(t *testing.T) {
	c := newCluster(t, 1)
	c.campaign(1)
	c.propose(1, "alpha")

	if got := c.peers[1].Status().Commit; got != 2 {
		t.Fatalf("commit %d, want the no-op and the proposal", got)
	}
}

// --- the current-term commit rule ---------------------------------------

// An entry from an earlier term must not be committed by counting replicas.
// A later leader can still overwrite it, so committing it here would be a lie
// that no amount of replication makes true.
func TestEarlierTermEntryIsNotCommittedByCounting(t *testing.T) {
	store := NewMemoryLogWith(Entry{Index: 1, Term: 1})
	node, err := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}}, store, HardState{Term: 2})
	if err != nil {
		t.Fatal(err)
	}

	// Stand the member up as a leader in term 2 holding only an entry from
	// term 1, and tell it a quorum already holds that entry.
	node.role = Leader
	node.leader = 1
	node.progress = map[NodeID]*progress{
		1: {Match: 1, Next: 2},
		2: {Match: 1, Next: 2},
		3: {Match: 0, Next: 2},
	}

	node.maybeAdvanceCommit()
	if got := node.log.committed; got != 0 {
		t.Fatalf("committed %d: an earlier term's entry was committed by replica count alone", got)
	}

	// Once an entry from the current term commits, everything before it does
	// too, which is the only route by which the earlier entry becomes safe.
	node.log.append(Entry{Index: 2, Term: 2})
	node.progress[1] = &progress{Match: 2, Next: 3}
	node.progress[2] = &progress{Match: 2, Next: 3}
	node.maybeAdvanceCommit()

	if got := node.log.committed; got != 2 {
		t.Fatalf("committed %d, want 2 once a current-term entry reached a quorum", got)
	}
}

// --- divergence ---------------------------------------------------------

func TestFollowerDivergentSuffixIsReplaced(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	// Member 2 holds entries from an old term that never committed. Member 1
	// starts in a later term, so anything it writes outranks them.
	c.preloadAt(2, 1,
		Entry{Index: 1, Term: 1, Data: []byte("stale-a")},
		Entry{Index: 2, Term: 1, Data: []byte("stale-b")})
	c.preloadAt(1, 5)

	c.campaign(1)
	c.propose(1, "real")
	c.pump()

	c.expectLogsAgree()
	for _, e := range c.logOf(2) {
		if string(e.Data) == "stale-a" || string(e.Data) == "stale-b" {
			t.Fatalf("a divergent entry survived: %+v", e)
		}
	}
}

// A repeated append is ordinary traffic. It must not rewrite the log, or a
// duplicate would churn the store and could truncate entries a leader still
// believes are held.
func TestRepeatedAppendIsIdempotent(t *testing.T) {
	n := newTestNode(t, 1, 1, 2, 3)
	msg := Message{
		Type: MsgAppend, From: 2, To: 1, Term: 1,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []Entry{{Index: 1, Term: 1, Data: []byte("x")}, {Index: 2, Term: 1, Data: []byte("y")}},
	}

	recv(t, n, msg)
	first := n.store.LastIndex()
	recv(t, n, msg)
	recv(t, n, msg)

	if got := n.store.LastIndex(); got != first {
		t.Fatalf("log grew from %d to %d on repeated appends", first, got)
	}
	if term, _ := n.store.Term(2); term != 1 {
		t.Fatalf("entry rewritten by a duplicate: term %d", term)
	}
}

// A follower may only commit as far as it can see. Trusting a leader's commit
// index past its own log would report entries applied that it does not hold.
func TestFollowerCommitIsBoundedByItsOwnLog(t *testing.T) {
	n := newTestNode(t, 1, 1, 2, 3)

	recv(t, n, Message{
		Type: MsgAppend, From: 2, To: 1, Term: 1,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      []Entry{{Index: 1, Term: 1}},
		LeaderCommit: 99,
	})

	if got := n.Status().Commit; got != 1 {
		t.Fatalf("commit %d, want 1: the follower holds only one entry", got)
	}
}

// --- catch-up -----------------------------------------------------------

// A follower a whole term behind must be caught up in a couple of round trips,
// not one per missing entry. Backing up one index at a time is correct and
// unusably slow, so the hint is load-bearing.
func TestConflictHintSkipsAWholeTerm(t *testing.T) {
	c := newCluster(t, 1, 2, 3)

	// Both members hold twenty entries, disagreeing from index 1 onward, so
	// the leader's first probe lands at index 20 and must walk back.
	var stale, real []Entry
	for i := 1; i <= 20; i++ {
		stale = append(stale, Entry{Index: Index(i), Term: 2, Data: []byte("stale")})
		real = append(real, Entry{Index: Index(i), Term: 5, Data: []byte("real")})
	}
	c.preloadAt(2, 2, stale...)
	c.preloadAt(1, 5, real...)

	c.campaign(1)

	c.sent = 0
	c.propose(1, "next")
	c.expectLogsAgree()

	for _, e := range c.logOf(2) {
		if string(e.Data) == "stale" {
			t.Fatalf("a divergent entry survived at index %d", e.Index)
		}
	}
	// Twenty divergent entries backed up one index per round trip would cost
	// far more than this.
	if c.sent > 30 {
		t.Fatalf("catching up one follower cost %d messages; the hint is not skipping terms", c.sent)
	}
	t.Logf("caught up a twenty-entry divergence in %d messages", c.sent)
}

// A stale acceptance must not retract progress a later one established, or a
// leader could resend entries a follower already holds and, worse, lower a
// match index that a commit decision already depended on.
func TestMatchIndexNeverRetreats(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	c.campaign(1)
	c.propose(1, "a")
	c.propose(1, "b")

	leader := c.peers[1]
	before := leader.progress[2].Match

	// Replay an older acceptance.
	if err := leader.Step(Event{Type: EventMessage, Message: Message{
		Type: MsgAppendResp, From: 2, To: 1, Term: leader.Status().Term, MatchIndex: 1,
	}}); err != nil {
		t.Fatal(err)
	}
	drive(t, leader)

	if got := leader.progress[2].Match; got != before {
		t.Fatalf("match retreated from %d to %d on a stale acceptance", before, got)
	}
}

// --- leader change ------------------------------------------------------

func TestLogsConvergeAcrossLeaderChange(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	c.campaign(1)
	c.propose(1, "from-one")

	c.campaign(2)
	if s := c.peers[2].Status(); s.Role != Leader {
		t.Fatalf("member 2 failed to take over: %+v", s)
	}
	c.propose(2, "from-two")

	c.expectLogsAgree()

	log := c.logOf(3)
	var commands []string
	for _, e := range log {
		if len(e.Data) > 0 {
			commands = append(commands, string(e.Data))
		}
	}
	if fmt.Sprint(commands) != "[from-one from-two]" {
		t.Fatalf("member 3 holds %v, want both commands in order", commands)
	}
}
