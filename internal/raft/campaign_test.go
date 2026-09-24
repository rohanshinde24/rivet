package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
)

// The named scenarios and the randomized campaign. Together they are the
// evidence that the protocol holds, and they are only as good as the
// invariant checks the simulator runs after every tick.

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// --- injectable log store -----------------------------------------------

var errInjectedStore = errors.New("injected log store failure")

// faultyLog fails writes once a member has done enough of them to be a
// participating part of the group.
type faultyLog struct {
	*MemoryLog
	failAfter int
	appends   int
}

func (f *faultyLog) Append(ents []Entry) error {
	f.appends++
	if f.appends > f.failAfter {
		return errInjectedStore
	}
	return f.MemoryLog.Append(ents)
}

func (s *sim) failStoreOf(id NodeID, afterAppends int) {
	m := s.members[id]
	m.write = &faultyLog{MemoryLog: m.store, failAfter: afterAppends}
	s.persistMayFail = true
	s.start(id)
	s.logf("member %d will fail writes after %d appends", id, afterAppends)
}

// --- named scenarios ----------------------------------------------------

// R01: a leader crashes and returns. It must come back with everything it
// promised, and must not contradict anything decided while it was gone.
func TestR01LeaderCrashAndRestart(t *testing.T) {
	s := newSim(t, 101, 1, 2, 3)
	s.run(40)

	leader, ok := s.leader()
	if !ok {
		s.fail("no leader to crash")
	}
	for i := 0; i < 4; i++ {
		s.proposeTo(leader, fmt.Sprintf("before-%d", i))
		s.run(6)
	}
	committedBefore := len(s.committed)
	termBefore := s.members[leader].hard.Term

	s.crash(leader)
	s.run(120)
	if _, ok := s.leader(); !ok {
		s.fail("the group did not elect a replacement")
	}

	s.restart(leader)
	s.run(120)

	if got := s.members[leader].hard.Term; got < termBefore {
		s.fail("member %d came back in term %d, below the %d it had promised", leader, got, termBefore)
	}
	if len(s.committed) < committedBefore {
		s.fail("committed entries fell from %d to %d", committedBefore, len(s.committed))
	}
}

// R02: a follower misses a stretch of history and must be caught up rather
// than left behind.
func TestR02FollowerRejoinsBehind(t *testing.T) {
	s := newSim(t, 102, 1, 2, 3)
	s.run(40)

	leader, ok := s.leader()
	if !ok {
		s.fail("no leader")
	}
	var follower NodeID
	for _, id := range s.ids {
		if id != leader {
			follower = id
			break
		}
	}

	s.crash(follower)
	for i := 0; i < 8; i++ {
		s.proposeTo(leader, fmt.Sprintf("missed-%d", i))
		s.run(6)
	}
	leaderLen := len(s.logEntries(leader))

	s.restart(follower)
	s.run(150)

	if got := len(s.logEntries(follower)); got != leaderLen {
		s.fail("member %d holds %d entries after rejoining, leader holds %d", follower, got, leaderLen)
	}
}

// R03: a leader cut off from a quorum cannot commit, and the majority side
// carries on without it.
func TestR03SymmetricPartitionIsolatesLeader(t *testing.T) {
	s := newSim(t, 103, 1, 2, 3)
	s.run(40)

	leader, ok := s.leader()
	if !ok {
		s.fail("no leader")
	}
	for _, id := range s.ids {
		s.partition[id] = 0
	}
	s.partition[leader] = 1

	for i := 0; i < 5; i++ {
		s.proposeTo(leader, fmt.Sprintf("isolated-%d", i))
		s.run(10)
	}
	for idx, e := range s.committed {
		if len(e.Data) > 0 && string(e.Data)[:9] == "isolated-" {
			s.fail("an isolated leader committed %q at index %d", e.Data, idx)
		}
	}

	for _, id := range s.ids {
		s.partition[id] = 0
	}
	s.run(200)
	if _, ok := s.leader(); !ok {
		s.fail("no leader after the partition healed")
	}
}

// R04: the leader can still send but never hears back. It keeps followers
// quiet while being unable to commit anything, which is exactly the case that
// leader step-down is meant to address later.
func TestR04AsymmetricPartition(t *testing.T) {
	s := newSim(t, 104, 1, 2, 3)
	s.run(40)

	leader, ok := s.leader()
	if !ok {
		s.fail("no leader")
	}
	for _, id := range s.ids {
		if id != leader {
			s.blocked[[2]NodeID{id, leader}] = true
		}
	}
	s.logf("member %d can send but not receive", leader)

	before := len(s.committed)
	for i := 0; i < 5; i++ {
		s.proposeTo(leader, fmt.Sprintf("deaf-%d", i))
		s.run(10)
	}
	if len(s.committed) != before {
		s.fail("a leader that hears nothing committed %d entries", len(s.committed)-before)
	}

	s.blocked = map[[2]NodeID]bool{}
	s.run(200)
	if _, ok := s.leader(); !ok {
		s.fail("no leader after the link was restored")
	}
}

// R05: duplication and reordering are ordinary conditions, not edge cases.
func TestR05DuplicationAndReorderUnderLoad(t *testing.T) {
	s := newSim(t, 105, 1, 2, 3)
	s.dupRate = 0.4
	s.maxDelay = 4
	s.run(60)

	for i := 0; i < 15; i++ {
		if id, ok := s.leader(); ok {
			s.proposeTo(id, fmt.Sprintf("load-%d", i))
		}
		s.run(8)
	}
	s.dupRate, s.maxDelay = 0, 0
	s.run(200)

	if len(s.committed) < 8 {
		s.fail("only %d entries committed under duplication and reordering", len(s.committed))
	}
}

// R06: a member that was away returns with an inflated term and deposes a
// healthy leader. That is disruptive and it is correct: without pre-vote this
// is the defined behavior, so the assertion is that safety holds, not that
// the disruption is absent.
func TestR06ReturningMemberDisruptsHealthyLeader(t *testing.T) {
	s := newSim(t, 106, 1, 2, 3)
	s.run(40)

	leader, ok := s.leader()
	if !ok {
		s.fail("no leader")
	}
	var away NodeID
	for _, id := range s.ids {
		if id != leader {
			away = id
			break
		}
	}

	// Isolate one member so it campaigns repeatedly and inflates its term.
	for _, id := range s.ids {
		s.partition[id] = 0
	}
	s.partition[away] = 1
	s.run(200)

	awayTerm := s.members[away].node.Status().Term
	leaderTerm := s.members[leader].node.Status().Term
	if awayTerm <= leaderTerm {
		s.fail("the isolated member reached term %d, not above the leader's %d", awayTerm, leaderTerm)
	}

	for _, id := range s.ids {
		s.partition[id] = 0
	}
	s.run(200)

	// Disruption is expected. What must hold is that a leader exists and no
	// invariant was broken getting there, which the simulator checked on
	// every tick along the way.
	if _, ok := s.leader(); !ok {
		s.fail("no leader after the member returned")
	}
}

// R07: entries accepted by a minority but never committed must be replaced by
// the history the majority agreed on.
func TestR07DivergentSuffixIsOverwritten(t *testing.T) {
	s := newSim(t, 107, 1, 2, 3, 4, 5)
	s.run(60)

	leader, ok := s.leader()
	if !ok {
		s.fail("no leader")
	}
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

	for i := 0; i < 4; i++ {
		s.proposeTo(leader, fmt.Sprintf("orphan-%d", i))
		s.run(8)
	}
	if got := len(s.logEntries(leader)); got == 0 {
		s.fail("the isolated leader accepted nothing to orphan")
	}

	// The majority elects and commits without them.
	s.run(200)
	for i := 0; i < 3; i++ {
		if id, ok := s.leader(); ok && s.partition[id] == 0 {
			s.proposeTo(id, fmt.Sprintf("real-%d", i))
		}
		s.run(10)
	}

	for _, id := range s.ids {
		s.partition[id] = 0
	}
	s.run(300)

	for _, e := range s.logEntries(leader) {
		if len(e.Data) >= 7 && string(e.Data)[:7] == "orphan-" {
			s.fail("an orphaned entry survived on member %d at index %d", leader, e.Index)
		}
	}
}

// R08: a member whose log store fails stops participating. The group must
// carry on without it while a quorum remains.
func TestR08LogStoreFailureStopsAMember(t *testing.T) {
	s := newSim(t, 108, 1, 2, 3, 4, 5)
	s.run(60)

	leader, ok := s.leader()
	if !ok {
		s.fail("no leader")
	}
	var victim NodeID
	for _, id := range s.ids {
		if id != leader {
			victim = id
			break
		}
	}
	s.failStoreOf(victim, 1)

	for i := 0; i < 6; i++ {
		if id, ok := s.leader(); ok {
			s.proposeTo(id, fmt.Sprintf("after-fault-%d", i))
		}
		s.run(10)
	}
	s.run(150)

	if !s.members[victim].down {
		s.fail("member %d kept participating after its log store failed", victim)
	}
	if len(s.committed) < 3 {
		s.fail("the remaining quorum committed only %d entries", len(s.committed))
	}
}

// --- randomized campaign ------------------------------------------------

// TestRandomizedSchedules runs seeded schedules of random faults, checking
// every invariant after every tick, then requires the group to recover once
// the faults stop.
//
// The seed count is fixed in advance rather than chosen from what passes. It
// is not a statistical guarantee; it is a budget, raised on the nightly run.
func TestRandomizedSchedules(t *testing.T) {
	seeds := envInt("RIVET_RAFT_SEEDS", 500)
	base := int64(envInt("RIVET_RAFT_BASE_SEED", 1))
	if testing.Short() {
		seeds = 25
	}

	for i := 0; i < seeds; i++ {
		runRandomSchedule(t, base+int64(i))
	}
	t.Logf("%d schedules clean from base seed %d", seeds, base)
}

func runRandomSchedule(t *testing.T, seed int64) {
	rng := rand.New(rand.NewSource(seed))

	ids := []NodeID{1, 2, 3}
	if rng.Intn(4) == 0 {
		ids = []NodeID{1, 2, 3, 4, 5}
	}

	s := newSim(t, seed, ids...)
	s.dropRate = rng.Float64() * 0.25
	s.dupRate = rng.Float64() * 0.2
	s.maxDelay = rng.Intn(4)

	// The schedule is deliberately biased toward leadership churn while
	// entries are in flight. A uniformly random fault schedule spends almost
	// all of its time in states where nothing interesting can go wrong: the
	// dangerous region is an entry accepted by a minority, then a leader
	// change, then the original leader returning to power still holding it.
	proposals := 0
	churn := func() {
		if id, ok := s.leader(); ok {
			if rng.Intn(2) == 0 {
				s.crash(id)
				return
			}
			for _, p := range ids {
				s.partition[p] = 0
			}
			s.partition[id] = 1
			return
		}
		s.restart(ids[rng.Intn(len(ids))])
	}

	for stepNo := 0; stepNo < 250; stepNo++ {
		switch n := rng.Intn(100); {
		case n < 50:
			// Plain time passing.
		case n < 72:
			if id, ok := s.leader(); ok {
				s.proposeTo(id, fmt.Sprintf("p%d", proposals))
				proposals++
			}
		case n < 86:
			churn()
		case n < 93:
			s.restart(ids[rng.Intn(len(ids))])
		case n < 98:
			for _, id := range ids {
				s.partition[id] = rng.Intn(2)
			}
		default:
			for _, id := range ids {
				s.partition[id] = 0
			}
		}
		s.tick()
	}

	// Stop every fault. What follows is the liveness the group owes once a
	// quorum can communicate again.
	for _, id := range ids {
		s.partition[id] = 0
		s.restart(id)
	}
	s.blocked = map[[2]NodeID]bool{}
	s.dropRate, s.dupRate, s.maxDelay = 0, 0, 0
	s.run(300)

	leader, ok := s.leader()
	if !ok {
		s.fail("no leader emerged after every fault stopped")
	}

	before := len(s.committed)
	s.proposeTo(leader, fmt.Sprintf("final-%d", seed))
	s.run(200)
	if len(s.committed) <= before {
		s.fail("a proposal made after recovery never committed")
	}
}

// R09: a crash at each durability boundary inside a batch.
//
// Between batches a crash is easy to survive. The interesting crashes are
// inside one: after the term and vote are written but before the entries,
// after the entries but before anything is sent, and after sending but before
// applying. Each leaves a different mixture of written and lost state, and
// none of them may let a member contradict something another member already
// acted on.
func TestR09CrashAtEveryDurabilityBoundary(t *testing.T) {
	points := []persistPoint{
		pointBeforeHardState,
		pointAfterHardState,
		pointAfterEntries,
		pointBeforeSend,
		pointAfterSend,
	}

	for _, point := range points {
		t.Run(point.String(), func(t *testing.T) {
			s := newSim(t, 900+int64(point), 1, 2, 3, 4, 5)

			// Crash a handful of times at this boundary, then let the group
			// settle. Crashing forever would only prove that a dead group
			// makes no progress.
			budget := 6
			s.crashAt = func(id NodeID, p persistPoint) bool {
				if p != point || budget == 0 {
					return false
				}
				budget--
				return true
			}

			proposals := 0
			for step := 0; step < 200; step++ {
				if step%7 == 0 {
					if id, ok := s.leader(); ok {
						s.proposeTo(id, fmt.Sprintf("v%d", proposals))
						proposals++
					}
				}
				if step%5 == 0 {
					for _, id := range s.ids {
						s.restart(id)
					}
				}
				s.tick()
			}

			s.crashAt = nil
			for _, id := range s.ids {
				s.restart(id)
			}
			s.run(300)

			if budget > 0 {
				t.Skipf("the boundary %s was reached only %d times", point, 6-budget)
			}
			leader, ok := s.leader()
			if !ok {
				s.fail("no leader after crashing %s", point)
			}

			before := len(s.committed)
			s.proposeTo(leader, "after-recovery")
			s.run(200)
			if len(s.committed) <= before {
				s.fail("nothing committed after recovering from crashes %s", point)
			}
		})
	}
}
