package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
)

// The simulator is the driver for every safety test in this package.
//
// It lives in a test file on purpose. Nothing outside this package needs it
// yet, and a harness that ships in the binary is weight the binary does not
// carry for a reason. If a later milestone needs it, moving it is a rename.
//
// It holds every member, a virtual clock, and a network it can delay, drop,
// duplicate, reorder and partition. Nothing here sleeps, reads a real clock,
// or opens a socket, so thousands of schedules run in seconds and any one of
// them replays exactly from its seed.

// simMember is one member plus the durable state its driver keeps for it.
type simMember struct {
	id   NodeID
	node *Node

	// store is the underlying log, read directly by the invariant checks.
	store *MemoryLog

	// write is what the member and its driver actually go through. It is the
	// store unless a test has wrapped it to inject failures.
	write LogStore

	// hard is what the driver has actually written. A crash keeps this and
	// the store, and discards everything else, which is the whole point.
	hard HardState

	down bool
}

type inflight struct {
	msg     Message
	deliver uint64
}

type sim struct {
	t    *testing.T
	seed int64
	rng  *rand.Rand

	ids     []NodeID
	members map[NodeID]*simMember
	net     []inflight
	now     uint64

	dropRate float64
	dupRate  float64
	maxDelay int

	// partition maps a member to a group. Messages only flow within a group.
	partition map[NodeID]int

	// blocked cuts a single direction, which is how an asymmetric partition
	// is expressed: a leader that can still send but never hears back.
	blocked map[[2]NodeID]bool

	// persistMayFail says a store failure is the point of the test rather
	// than a bug in it. A member that cannot persist stops participating.
	persistMayFail bool

	// Invariant bookkeeping.
	leaders   map[Term]NodeID
	committed map[Index]Entry
	prevLog   map[NodeID][]Entry
	converged map[NodeID]map[Index]bool
	prevRole  map[NodeID]Role
	prevTerm  map[NodeID]Term

	trace []string

	// capture turns a violation into a recoverable panic instead of a test
	// failure, for the tests that provoke violations on purpose.
	capture bool
}

// simViolation unwinds a check that has found a violation.
//
// Every check is written assuming fail does not return, which is true of
// t.Fatalf. A capture mode that merely recorded the message and returned
// would let the check carry on reading state it has already rejected, which
// is how it first crashed rather than reported.
type simViolation struct{ msg string }

func newSim(t *testing.T, seed int64, ids ...NodeID) *sim {
	t.Helper()
	s := &sim{
		t:         t,
		seed:      seed,
		rng:       rand.New(rand.NewSource(seed)),
		ids:       ids,
		members:   map[NodeID]*simMember{},
		partition: map[NodeID]int{},
		blocked:   map[[2]NodeID]bool{},
		leaders:   map[Term]NodeID{},
		committed: map[Index]Entry{},
		prevLog:   map[NodeID][]Entry{},
		converged: map[NodeID]map[Index]bool{},
		prevRole:  map[NodeID]Role{},
		prevTerm:  map[NodeID]Term{},
	}
	for _, id := range ids {
		store := NewMemoryLog()
		s.members[id] = &simMember{id: id, store: store, write: store}
		s.start(id)
	}
	return s
}

func (s *sim) logf(format string, args ...any) {
	s.trace = append(s.trace, fmt.Sprintf("t=%d "+format, append([]any{s.now}, args...)...))
	// A schedule that runs long enough to matter produces more trace than is
	// useful; the tail is what explains a failure.
	if len(s.trace) > 4000 {
		s.trace = s.trace[len(s.trace)-2000:]
	}
}

// fail reports a violation with everything needed to reproduce it.
func (s *sim) fail(format string, args ...any) {
	if s.capture {
		panic(simViolation{msg: fmt.Sprintf(format, args...)})
	}
	tail := s.trace
	if len(tail) > 60 {
		tail = tail[len(tail)-60:]
	}
	s.t.Fatalf("seed=%d tick=%d: %s\n\nlast events:\n%s",
		s.seed, s.now, fmt.Sprintf(format, args...), strings.Join(tail, "\n"))
}

// start creates a member from whatever its driver has made durable. It is how
// a member both boots and recovers, because those are the same thing.
func (s *sim) start(id NodeID) {
	m := s.members[id]
	node, err := NewNode(Config{
		ID: id, Peers: s.ids,
		// A distinct seed per member keeps them from campaigning in lockstep.
		Seed: s.seed*1000 + int64(id),
	}, m.write, m.hard)
	if err != nil {
		s.t.Fatalf("starting member %d: %v", id, err)
	}
	m.node = node
	m.down = false
	s.logf("member %d started at term %d", id, m.hard.Term)
}

// crash discards everything the driver had not written. The store and the
// hard state survive; the node, its role, its commit index and its timers do
// not.
func (s *sim) crash(id NodeID) {
	m := s.members[id]
	m.node = nil
	m.down = true
	delete(s.prevLog, id)
	delete(s.prevRole, id)
	delete(s.prevTerm, id)
	s.logf("member %d crashed", id)
}

func (s *sim) restart(id NodeID) {
	if s.members[id].down {
		s.start(id)
	}
}

// --- the driver ---------------------------------------------------------

// drive performs a Ready batch in the order the contract requires and checks
// that the batch could honestly be performed in that order.
func (s *sim) drive(m *simMember) {
	r := m.node.Ready()
	if r.IsEmpty() {
		return
	}

	// Persist first.
	if r.HardState != nil {
		m.hard = *r.HardState
	}
	if len(r.Entries) > 0 {
		if err := ApplyEntries(m.write, r.Entries); err != nil {
			if !s.persistMayFail {
				s.fail("member %d could not persist staged entries: %v", m.id, err)
			}
			// A member that cannot write its log cannot honestly answer for
			// it, so it stops rather than replying on memory alone.
			s.logf("member %d failed to persist and is stopping: %v", m.id, err)
			s.crash(m.id)
			return
		}
	}

	// Only now may anything be sent, and only things the durable state
	// supports. This is the assertion that the ordering contract exists for.
	s.checkSendable(m, r)

	for _, msg := range r.Messages {
		s.enqueue(msg)
	}

	for _, e := range r.CommittedEntries {
		s.observeCommitted(m, e)
	}
	m.node.Advance(r)
}

// checkSendable verifies that no message in a batch claims something the
// durable state does not support.
func (s *sim) checkSendable(m *simMember, r Ready) {
	for _, msg := range r.Messages {
		if msg.Term != m.hard.Term {
			s.fail("member %d sent %s in term %d while its durable term is %d",
				m.id, msg.Type, msg.Term, m.hard.Term)
		}
		switch {
		case msg.Type == MsgRequestVoteResp && !msg.Reject:
			if m.hard.Vote != msg.To {
				s.fail("member %d granted a vote to %d while its durable vote is %d",
					m.id, msg.To, m.hard.Vote)
			}
		case msg.Type == MsgAppendResp && !msg.Reject:
			if msg.MatchIndex > m.store.LastIndex() {
				s.fail("member %d claimed to hold index %d while its store ends at %d",
					m.id, msg.MatchIndex, m.store.LastIndex())
			}
		}
	}
}

func (s *sim) linkUp(from, to NodeID) bool {
	return s.partition[from] == s.partition[to] && !s.blocked[[2]NodeID{from, to}]
}

func (s *sim) enqueue(msg Message) {
	if !s.linkUp(msg.From, msg.To) {
		s.logf("dropped %s %d->%d: link down", msg.Type, msg.From, msg.To)
		return
	}
	if s.rng.Float64() < s.dropRate {
		s.logf("dropped %s %d->%d", msg.Type, msg.From, msg.To)
		return
	}

	copies := 1
	if s.rng.Float64() < s.dupRate {
		copies = 2
	}
	for i := 0; i < copies; i++ {
		delay := uint64(1)
		if s.maxDelay > 0 {
			delay += uint64(s.rng.Intn(s.maxDelay))
		}
		s.net = append(s.net, inflight{msg: msg, deliver: s.now + delay})
	}
}

// deliverDue hands over every message whose time has come. Messages are
// delivered in an order the seed chooses, so reordering is the norm rather
// than a special case.
func (s *sim) deliverDue() {
	var due, rest []inflight
	for _, f := range s.net {
		if f.deliver <= s.now {
			due = append(due, f)
		} else {
			rest = append(rest, f)
		}
	}
	s.net = rest

	s.rng.Shuffle(len(due), func(i, j int) { due[i], due[j] = due[j], due[i] })
	for _, f := range due {
		m := s.members[f.msg.To]
		if m == nil || m.down {
			continue
		}
		if !s.linkUp(f.msg.From, f.msg.To) {
			continue
		}
		s.stepMember(m, Event{Type: EventMessage, Message: f.msg})
	}
}

func (s *sim) stepMember(m *simMember, ev Event) {
	if m.down {
		return
	}
	if err := m.node.Step(ev); err != nil && err != ErrNotLeader {
		s.fail("member %d step: %v", m.id, err)
	}
	s.drive(m)
}

// tick advances the virtual clock by one unit.
func (s *sim) tick() {
	s.now++
	s.deliverDue()
	for _, id := range s.ids {
		m := s.members[id]
		if !m.down {
			s.stepMember(m, Event{Type: EventTick})
		}
	}
	s.checkInvariants()
}

func (s *sim) run(ticks int) {
	for i := 0; i < ticks; i++ {
		s.tick()
	}
}

func (s *sim) propose(data string) bool {
	for _, id := range s.ids {
		m := s.members[id]
		if m.down || m.node.Status().Role != Leader {
			continue
		}
		s.logf("proposing %q to leader %d", data, id)
		s.stepMember(m, Event{Type: EventPropose, Data: []byte(data)})
		return true
	}
	return false
}

// proposeTo offers a command to one named member, which is how a test aims at
// an isolated leader rather than at whichever member happens to lead.
func (s *sim) proposeTo(id NodeID, data string) {
	m := s.members[id]
	if m.down {
		return
	}
	s.logf("proposing %q to member %d", data, id)
	s.stepMember(m, Event{Type: EventPropose, Data: []byte(data)})
}

func (s *sim) leader() (NodeID, bool) {
	for _, id := range s.ids {
		m := s.members[id]
		if !m.down && m.node.Status().Role == Leader {
			return id, true
		}
	}
	return None, false
}

// logEntries reads a member's stored entries without copying. Invariant
// checks run after every tick of every schedule, so a copy here would cost
// more than the protocol being tested. Callers must only read.
func (s *sim) logEntries(id NodeID) []Entry {
	return s.members[id].store.all()
}

// --- invariants ---------------------------------------------------------

func (s *sim) observeCommitted(m *simMember, e Entry) {
	// No two members may report different commands applied at the same index.
	if prev, seen := s.committed[e.Index]; seen {
		if prev.Term != e.Term || string(prev.Data) != string(e.Data) {
			s.fail("index %d applied as term %d %q by one member and term %d %q by member %d",
				e.Index, prev.Term, prev.Data, e.Term, e.Data, m.id)
		}
		return
	}
	s.committed[e.Index] = e
}

func (s *sim) checkInvariants() {
	s.checkElectionSafety()
	s.checkLeaderAppendOnly()
	s.checkLogMatching()
	s.checkCommittedNeverReplaced()
}

// Once a member holds the entry that was committed at an index, nothing may
// ever replace it there.
//
// This is deliberately stronger than checking what members apply. An entry
// wrongly committed and then overwritten by a later leader may never be
// applied twice, so a checker that only watches applications can miss the
// loss entirely. Watching the logs catches the overwrite itself.
//
// A member that has not yet converged on a committed index is not in
// violation: a lagging minority legitimately holds a stale suffix until the
// leader reaches it.
func (s *sim) checkCommittedNeverReplaced() {
	for _, id := range s.ids {
		log := s.logEntries(id)
		conv := s.converged[id]
		if conv == nil {
			conv = map[Index]bool{}
			s.converged[id] = conv
		}
		for idx, want := range s.committed {
			if int(idx) > len(log) {
				if conv[idx] {
					s.fail("member %d dropped committed index %d", id, idx)
				}
				continue
			}
			got := log[idx-1]
			matches := got.Term == want.Term && bytes.Equal(got.Data, want.Data)
			if conv[idx] && !matches {
				s.fail("member %d replaced committed index %d: term %d %q became term %d %q",
					id, idx, want.Term, want.Data, got.Term, got.Data)
			}
			if matches {
				conv[idx] = true
			}
		}
	}
}

// At most one leader may exist in a term.
func (s *sim) checkElectionSafety() {
	for _, id := range s.ids {
		m := s.members[id]
		if m.down {
			continue
		}
		st := m.node.Status()
		if st.Role != Leader {
			continue
		}
		if prev, seen := s.leaders[st.Term]; seen && prev != id {
			s.fail("members %d and %d were both leader in term %d", prev, id, st.Term)
		}
		if _, seen := s.leaders[st.Term]; !seen {
			s.leaders[st.Term] = id
			s.logf("member %d became leader of term %d", id, st.Term)
			s.checkLeaderCompleteness(id, st.Term)
		}
	}
}

// A leader that is committed to an entry must hold every entry already
// committed anywhere, or an acknowledged write could be lost.
func (s *sim) checkLeaderCompleteness(id NodeID, term Term) {
	log := s.logEntries(id)
	held := make(map[Index]Entry, len(log))
	for _, e := range log {
		held[e.Index] = e
	}
	for idx, want := range s.committed {
		got, ok := held[idx]
		if !ok {
			s.fail("member %d became leader of term %d without committed index %d", id, term, idx)
		}
		if got.Term != want.Term || string(got.Data) != string(want.Data) {
			s.fail("member %d became leader of term %d holding a different entry at committed index %d",
				id, term, idx)
		}
	}
}

// A leader never rewrites or drops an entry in its own log.
func (s *sim) checkLeaderAppendOnly() {
	for _, id := range s.ids {
		m := s.members[id]
		if m.down {
			continue
		}
		st := m.node.Status()
		log := s.logEntries(id)

		if s.prevRole[id] == Leader && st.Role == Leader && s.prevTerm[id] == st.Term {
			prev := s.prevLog[id]
			if len(log) < len(prev) {
				s.fail("leader %d lost entries: %d then %d", id, len(prev), len(log))
			}
			for i := range prev {
				if prev[i].Term != log[i].Term || string(prev[i].Data) != string(log[i].Data) {
					s.fail("leader %d rewrote index %d", id, prev[i].Index)
				}
			}
		}
		s.prevRole[id], s.prevTerm[id] = st.Role, st.Term
		s.prevLog[id] = append(s.prevLog[id][:0], log...)
	}
}

// If two logs hold the same term at the same index, everything before it must
// be identical.
func (s *sim) checkLogMatching() {
	for _, a := range s.ids {
		for _, b := range s.ids {
			if a >= b {
				continue
			}
			la, lb := s.logEntries(a), s.logEntries(b)
			prefixEqual := true
			for i := 0; i < len(la) && i < len(lb); i++ {
				same := la[i].Term == lb[i].Term && bytes.Equal(la[i].Data, lb[i].Data)
				if la[i].Term == lb[i].Term {
					if !same || !prefixEqual {
						s.fail("members %d and %d share term %d at index %d but disagree before it",
							a, b, la[i].Term, la[i].Index)
					}
				}
				prefixEqual = prefixEqual && same
			}
		}
	}
}

// --- simulator self-tests -----------------------------------------------

func TestSimElectsALeaderOnAQuietNetwork(t *testing.T) {
	s := newSim(t, 1, 1, 2, 3)
	s.run(60)

	id, ok := s.leader()
	if !ok {
		s.fail("no leader after 60 quiet ticks")
	}
	t.Logf("member %d leads term %d", id, s.members[id].node.Status().Term)
}

func TestSimReplicatesUnderDelayAndDuplication(t *testing.T) {
	s := newSim(t, 7, 1, 2, 3)
	s.maxDelay = 3
	s.dupRate = 0.3
	s.run(40)

	for i := 0; i < 10; i++ {
		s.propose(fmt.Sprintf("cmd-%d", i))
		s.run(10)
	}
	s.run(60)

	if len(s.committed) < 5 {
		s.fail("only %d entries committed under delay and duplication", len(s.committed))
	}
}

// Everything the simulator does must come from its seed. Two runs of the same
// schedule that diverge would make every failure unreproducible, which is the
// property the whole design exists to buy.
func TestSimIsDeterministic(t *testing.T) {
	fingerprint := func(seed int64) string {
		s := newSim(t, seed, 1, 2, 3)
		s.maxDelay = 2
		s.dropRate = 0.1
		s.dupRate = 0.1
		s.run(50)
		for i := 0; i < 5; i++ {
			s.propose(fmt.Sprintf("cmd-%d", i))
			s.run(15)
		}

		var b strings.Builder
		ids := append([]NodeID(nil), s.ids...)
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, id := range ids {
			st := s.members[id].node.Status()
			fmt.Fprintf(&b, "%d:%d:%v:%d:%d|", id, st.Term, st.Role, st.Commit, st.LastIndex)
			for _, e := range s.logEntries(id) {
				fmt.Fprintf(&b, "%d/%d/%s,", e.Index, e.Term, e.Data)
			}
			b.WriteString(";")
		}
		return b.String()
	}

	for seed := int64(1); seed <= 5; seed++ {
		a, b := fingerprint(seed), fingerprint(seed)
		if a != b {
			t.Fatalf("seed %d produced two different outcomes:\n%s\n%s", seed, a, b)
		}
	}
}

// A crash keeps only what the driver wrote. If the core were allowed to treat
// anything else as durable, this is where it would show.
func TestSimSurvivesLeaderCrash(t *testing.T) {
	s := newSim(t, 11, 1, 2, 3)
	s.run(40)

	before, ok := s.leader()
	if !ok {
		s.fail("no leader to crash")
	}
	for i := 0; i < 5; i++ {
		s.propose(fmt.Sprintf("before-%d", i))
		s.run(5)
	}
	committedBefore := len(s.committed)

	s.crash(before)
	s.run(80)

	after, ok := s.leader()
	if !ok {
		s.fail("no leader elected after member %d crashed", before)
	}
	if after == before {
		s.fail("the crashed member is still reported as leader")
	}

	s.restart(before)
	s.run(60)

	if len(s.committed) < committedBefore {
		s.fail("committed entries went from %d to %d across a crash", committedBefore, len(s.committed))
	}
}

// A minority cannot commit. When the partition heals the two sides must
// converge on the majority's history.
func TestSimPartitionedMinorityCannotCommit(t *testing.T) {
	s := newSim(t, 23, 1, 2, 3, 4, 5)
	s.run(60)

	leader, ok := s.leader()
	if !ok {
		s.fail("no leader before partitioning")
	}

	// Isolate the leader with one follower: two members cannot form a quorum
	// of five.
	var companion NodeID
	for _, id := range s.ids {
		if id != leader {
			companion = id
			break
		}
	}
	for _, id := range s.ids {
		s.partition[id] = 0
	}
	s.partition[leader] = 1
	s.partition[companion] = 1
	s.logf("partitioned %d and %d away from the rest", leader, companion)

	for i := 0; i < 5; i++ {
		s.proposeTo(leader, fmt.Sprintf("minority-%d", i))
		s.run(10)
	}

	// The majority side legitimately elects and commits while this goes on,
	// so the count of committed entries is not the thing to check. What must
	// never appear is anything the isolated leader accepted.
	for idx, e := range s.committed {
		if strings.HasPrefix(string(e.Data), "minority-") {
			s.fail("the isolated leader committed %q at index %d", e.Data, idx)
		}
	}

	s.run(100)
	for _, id := range s.ids {
		s.partition[id] = 0
	}
	s.logf("partition healed")
	s.run(200)

	if _, ok := s.leader(); !ok {
		s.fail("no leader after the partition healed")
	}
}

// --- checking the checkers ----------------------------------------------

// A harness whose assertions cannot fail proves nothing, and these run after
// every tick of every schedule, so a silent one would make the whole campaign
// decorative. Each violation below is introduced deliberately.
func TestSimCheckersDetectViolations(t *testing.T) {
	violation := func(build func(*sim)) (msg string) {
		s := newSim(t, 77, 1, 2, 3)
		s.run(60)
		if len(s.committed) == 0 {
			t.Fatal("setup committed nothing to violate")
		}

		s.capture = true
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			v, ok := r.(simViolation)
			if !ok {
				panic(r)
			}
			msg = v.msg
		}()

		build(s)
		s.checkInvariants()
		return ""
	}

	t.Run("committed entry replaced", func(t *testing.T) {
		got := violation(func(s *sim) {
			// Rewrite an entry that a member has already converged on.
			for _, id := range s.ids {
				m := s.members[id]
				if m.store.LastIndex() >= 1 {
					m.store.ents[1].Term = 99
					m.store.ents[1].Data = []byte("forged")
				}
			}
		})
		if got == "" {
			t.Fatal("rewriting a committed entry went unnoticed")
		}
		t.Logf("caught: %s", got)
	})

	t.Run("two leaders in one term", func(t *testing.T) {
		got := violation(func(s *sim) {
			for _, id := range s.ids {
				m := s.members[id]
				if m.down {
					continue
				}
				m.node.role = Leader
				m.node.term = 500
			}
		})
		if got == "" {
			t.Fatal("two leaders in one term went unnoticed")
		}
		t.Logf("caught: %s", got)
	})

	t.Run("leader rewrites its own log", func(t *testing.T) {
		got := violation(func(s *sim) {
			for _, id := range s.ids {
				m := s.members[id]
				if m.down || m.node.Status().Role != Leader {
					continue
				}
				// Drop the tail of a leader's own log, which it may never do.
				_ = m.store.TruncateSuffix(1)
			}
		})
		if got == "" {
			t.Fatal("a leader losing entries went unnoticed")
		}
		t.Logf("caught: %s", got)
	})
}
