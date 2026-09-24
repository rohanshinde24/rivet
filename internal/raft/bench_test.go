package raft

import (
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// Baselines for the core. Every one of these runs against a simulated network
// with no disk and no sockets, so they measure the protocol and nothing else.
// A number here says what the core costs, never what a deployment would do.

var raftManifestOnce sync.Once

func raftManifest(b *testing.B) {
	raftManifestOnce.Do(func() {
		out := func(name string, args ...string) string {
			o, err := exec.Command(name, args...).Output()
			if err != nil {
				return "unavailable"
			}
			return strings.TrimSpace(string(o))
		}
		load := "unavailable"
		if u := out("uptime"); u != "unavailable" {
			if i := strings.Index(u, "load average"); i >= 0 {
				if j := strings.Index(u[i:], ":"); j >= 0 {
					load = strings.TrimSpace(u[i+j+1:])
				}
			}
		}
		b.Logf("revision:  %s", out("git", "rev-parse", "--short", "HEAD"))
		b.Logf("go:        %s on %s/%s, %d cpus", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU())
		b.Logf("load:      %s (a contended host makes these incomparable)", load)
		b.Logf("transport: simulated, in process, no disk and no sockets")
	})
}

// BenchmarkStep measures the cost of advancing a member by one event, which
// bounds how many groups a node could host before the protocol itself is the
// limit.
func BenchmarkStep(b *testing.B) {
	raftManifest(b)
	p := newTestNode(b, 1, 1, 2, 3)
	step(b, p, Event{Type: EventCampaign})
	recv(b, p, Message{Type: MsgRequestVoteResp, From: 2, To: 1, Term: 1})

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := p.Step(Event{Type: EventTick}); err != nil {
			b.Fatal(err)
		}
		r := p.Ready()
		p.Advance(r)
	}
}

// BenchmarkProposeAndCommit measures how fast a three-member group carries a
// command from proposal to commit with no delay in the network.
func BenchmarkProposeAndCommit(b *testing.B) {
	raftManifest(b)
	s := newSim(b, 1, 1, 2, 3)
	s.run(40)
	leader, ok := s.leader()
	if !ok {
		b.Fatal("no leader")
	}

	committed := 0
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		before := len(s.committed)
		s.proposeTo(leader, fmt.Sprintf("c%d", committed))
		for i := 0; i < 20 && len(s.committed) == before; i++ {
			s.tick()
		}
		if len(s.committed) == before {
			b.Fatalf("a proposal did not commit within twenty ticks")
		}
		committed++
	}
	b.StopTimer()
	b.ReportMetric(float64(committed)/b.Elapsed().Seconds(), "commits/sec")
}

// BenchmarkElection measures how many ticks pass between losing a leader and
// having another, which is the quantity a failover budget is built from. It
// is reported in ticks because the core has no other unit; a driver choosing
// 100ms per tick can multiply.
func BenchmarkElection(b *testing.B) {
	raftManifest(b)

	total, rounds := 0, 0
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		s := newSim(b, int64(rounds)+1, 1, 2, 3)
		s.run(60)
		leader, ok := s.leader()
		if !ok {
			b.Fatal("no leader to remove")
		}
		b.StartTimer()

		s.crash(leader)
		ticks := 0
		for ; ticks < 400; ticks++ {
			s.tick()
			if id, ok := s.leader(); ok && id != leader {
				break
			}
		}
		if ticks == 400 {
			b.Fatal("no replacement leader within four hundred ticks")
		}
		total += ticks
		rounds++
	}
	b.StopTimer()
	if rounds > 0 {
		b.ReportMetric(float64(total)/float64(rounds), "ticks/election")
	}
}

// BenchmarkCatchUp measures the ticks a follower needs to be brought level
// after missing a stretch of history, which is what bounds how long a
// restarted member is useless for.
func BenchmarkCatchUp(b *testing.B) {
	for _, behind := range []int{10, 100, 500} {
		b.Run(fmt.Sprintf("behind-%d", behind), func(b *testing.B) {
			raftManifest(b)

			total, rounds := 0, 0
			for b.Loop() {
				b.StopTimer()
				s := newSim(b, 5, 1, 2, 3)
				s.run(40)
				leader, ok := s.leader()
				if !ok {
					b.Fatal("no leader")
				}
				var follower NodeID
				for _, id := range s.ids {
					if id != leader {
						follower = id
						break
					}
				}
				s.crash(follower)
				for i := 0; i < behind; i++ {
					s.proposeTo(leader, fmt.Sprintf("e%d", i))
					s.tick()
				}
				target := len(s.logEntries(leader))
				b.StartTimer()

				s.restart(follower)
				ticks := 0
				for ; ticks < 2000; ticks++ {
					s.tick()
					if len(s.logEntries(follower)) >= target {
						break
					}
				}
				if ticks == 2000 {
					b.Fatalf("follower %d never caught up", follower)
				}
				total += ticks
				rounds++
			}
			if rounds > 0 {
				b.ReportMetric(float64(total)/float64(rounds), "ticks/catchup")
				b.ReportMetric(float64(behind), "entries-behind")
			}
		})
	}
}

// BenchmarkLogMemory measures what a log entry costs in memory, which bounds
// how many groups and how much history one node can hold.
func BenchmarkLogMemory(b *testing.B) {
	raftManifest(b)
	payload := make([]byte, 256)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		log := NewMemoryLog()
		for i := 1; i <= 1000; i++ {
			if err := log.Append([]Entry{{Index: Index(i), Term: 1, Data: payload}}); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	b.ReportMetric(1000, "entries/op")
}
