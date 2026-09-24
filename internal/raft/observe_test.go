package raft

import (
	"bytes"
	"strings"
	"testing"
)

// collectObservations wires a recorder into a member.
func collectObservations(t *testing.T, id NodeID, peers []NodeID) (*testPeer, *[]Observation) {
	t.Helper()
	var seen []Observation
	store := NewMemoryLog()
	n, err := NewNode(Config{
		ID: id, Peers: peers, Seed: 1,
		OnObserve: func(o Observation) { seen = append(seen, o) },
	}, store, HardState{})
	if err != nil {
		t.Fatal(err)
	}
	return &testPeer{Node: n, store: store}, &seen
}

func kinds(obs []Observation) []string {
	var out []string
	for _, o := range obs {
		out = append(out, o.Kind.String())
	}
	return out
}

func containsKind(obs []Observation, k ObservationKind) *Observation {
	for i := range obs {
		if obs[i].Kind == k {
			return &obs[i]
		}
	}
	return nil
}

func TestObservationsFollowAnElection(t *testing.T) {
	p, seen := collectObservations(t, 1, []NodeID{1, 2, 3})

	step(t, p, Event{Type: EventCampaign})
	if containsKind(*seen, ObsElectionStarted) == nil {
		t.Fatalf("no election start in %v", kinds(*seen))
	}
	if o := containsKind(*seen, ObsRoleChange); o == nil || o.Role != Candidate {
		t.Fatalf("no candidate role change in %v", kinds(*seen))
	}

	recv(t, p, Message{Type: MsgRequestVoteResp, From: 2, To: 1, Term: 1})
	if o := containsKind(*seen, ObsElectionWon); o == nil {
		t.Fatalf("no election won in %v", kinds(*seen))
	}
	if containsKind(*seen, ObsProgressReset) == nil {
		t.Fatalf("no progress reset in %v", kinds(*seen))
	}
}

func TestObservationsExplainVoteDecisions(t *testing.T) {
	p, seen := collectObservations(t, 1, []NodeID{1, 2, 3})

	recv(t, p, Message{Type: MsgRequestVote, From: 2, To: 1, Term: 1})
	if o := containsKind(*seen, ObsVoteGranted); o == nil || o.Peer != 2 {
		t.Fatalf("no grant to member 2 in %v", kinds(*seen))
	}

	*seen = nil
	recv(t, p, Message{Type: MsgRequestVote, From: 3, To: 1, Term: 1})
	o := containsKind(*seen, ObsVoteDenied)
	if o == nil || o.Reason != ReasonAlreadyVoted {
		t.Fatalf("denial reason %+v, want %s", o, ReasonAlreadyVoted)
	}

	*seen = nil
	recv(t, p, Message{Type: MsgRequestVote, From: 3, To: 1, Term: 0})
	o = containsKind(*seen, ObsVoteDenied)
	if o == nil || o.Reason != ReasonStaleTerm {
		t.Fatalf("stale denial reason %+v, want %s", o, ReasonStaleTerm)
	}
}

func TestObservationsReportTruncationAndCommit(t *testing.T) {
	p, seen := collectObservations(t, 1, []NodeID{1, 2, 3})

	recv(t, p, Message{
		Type: MsgAppend, From: 2, To: 1, Term: 1,
		Entries: []Entry{{Index: 1, Term: 1, Data: []byte("a")}, {Index: 2, Term: 1, Data: []byte("b")}},
	})
	*seen = nil

	// A different leader replaces the tail.
	recv(t, p, Message{
		Type: MsgAppend, From: 3, To: 1, Term: 2,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries:      []Entry{{Index: 2, Term: 2, Data: []byte("c")}},
		LeaderCommit: 2,
	})

	if o := containsKind(*seen, ObsLogTruncated); o == nil || o.Index != 2 {
		t.Fatalf("no truncation at index 2 in %v", kinds(*seen))
	}
	if o := containsKind(*seen, ObsCommitAdvanced); o == nil || o.Index != 2 {
		t.Fatalf("no commit advance to 2 in %v", kinds(*seen))
	}
}

func TestObservationsReportRejection(t *testing.T) {
	p, seen := collectObservations(t, 1, []NodeID{1, 2, 3})

	recv(t, p, Message{Type: MsgAppend, From: 2, To: 1, Term: 1, PrevLogIndex: 9, PrevLogTerm: 3})
	o := containsKind(*seen, ObsAppendRejected)
	if o == nil || o.Reason != ReasonLogMismatch || o.Index != 1 {
		t.Fatalf("rejection observation %+v", o)
	}
}

// An observation must never carry entry payload bytes, because the whole
// point of a structured event is that it can be logged everywhere.
func TestObservationsCarryNoPayload(t *testing.T) {
	const secret = "observation-payload-secret"
	p, seen := collectObservations(t, 1, []NodeID{1, 2, 3})

	recv(t, p, Message{
		Type: MsgAppend, From: 2, To: 1, Term: 1,
		Entries:      []Entry{{Index: 1, Term: 1, Data: []byte(secret)}},
		LeaderCommit: 1,
	})
	step(t, p, Event{Type: EventCampaign})

	for _, o := range *seen {
		if strings.Contains(o.Reason, secret) || strings.Contains(o.Kind.String(), secret) {
			t.Fatalf("observation leaked payload: %+v", o)
		}
	}
	if len(*seen) == 0 {
		t.Fatal("nothing was observed")
	}
}

func TestObservationsDisabledByDefault(t *testing.T) {
	p := newTestNode(t, 1, 1, 2, 3)
	if p.cfg.OnObserve != nil {
		t.Fatal("observation is on without a callback")
	}
	step(t, p, Event{Type: EventCampaign})
}

func TestMetricsTrackProgress(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	c.campaign(1)
	c.propose(1, "alpha")
	c.propose(1, "beta")

	m := c.peers[1].Metrics()
	if m.Role != Leader {
		t.Fatalf("role %v", m.Role)
	}
	if m.ElectionsStarted != 1 || m.ElectionsWon != 1 {
		t.Fatalf("election counters: started %d won %d", m.ElectionsStarted, m.ElectionsWon)
	}
	if m.Commit != 3 || m.LastIndex != 3 {
		t.Fatalf("commit %d last %d, want 3 and 3", m.Commit, m.LastIndex)
	}
	if m.LogEntries != 3 || m.LogBytes == 0 {
		t.Fatalf("log accounting: %d entries, %d bytes", m.LogEntries, m.LogBytes)
	}
	if m.MessagesSent[MsgAppend] == 0 || m.MessagesReceived[MsgAppendResp] == 0 {
		t.Fatalf("message counters: %+v / %+v", m.MessagesSent, m.MessagesReceived)
	}
	for peer, lag := range m.FollowerLag {
		if lag != 0 {
			t.Fatalf("member %d lags by %d after a settled round", peer, lag)
		}
	}

	follower := c.peers[2].Metrics()
	if follower.FollowerLag != nil {
		t.Fatal("a follower reported follower lag")
	}
	if follower.TicksSinceHeartbeat < 0 {
		t.Fatal("negative tick count")
	}
}

// Metrics must describe the member without disturbing it.
func TestMetricsDoNotMutate(t *testing.T) {
	c := newCluster(t, 1, 2, 3)
	c.campaign(1)
	c.propose(1, "alpha")

	before := c.peers[1].Status()
	beforeLog := append([]Entry(nil), c.logOf(1)...)
	_ = c.peers[1].Metrics()
	after := c.peers[1].Status()

	if before != after {
		t.Fatalf("Metrics changed status: %+v then %+v", before, after)
	}
	afterLog := c.logOf(1)
	if len(beforeLog) != len(afterLog) {
		t.Fatal("Metrics changed the log")
	}
	for i := range beforeLog {
		if !bytes.Equal(beforeLog[i].Data, afterLog[i].Data) {
			t.Fatal("Metrics changed log contents")
		}
	}
}
