package raft

import "testing"

func TestConfigDefaults(t *testing.T) {
	c := Config{ID: 1, Peers: []NodeID{1, 2, 3}}.withDefaults()

	if c.HeartbeatTicks != DefaultHeartbeatTicks ||
		c.ElectionTickMin != DefaultElectionTickMin ||
		c.ElectionTickMax != DefaultElectionTickMax ||
		c.MaxEntriesPerMsg != DefaultMaxEntriesPerMsg ||
		c.MaxBytesPerMsg != DefaultMaxBytesPerMsg {
		t.Fatalf("defaults not applied: %+v", c)
	}
	if err := c.validate(); err != nil {
		t.Fatalf("default config is invalid: %v", err)
	}
}

// The election timeout must leave room for several heartbeats, or a healthy
// leader would be deposed by ordinary jitter.
func TestConfigValidation(t *testing.T) {
	valid := Config{ID: 1, Peers: []NodeID{1, 2, 3}}.withDefaults()

	cases := map[string]func(*Config){
		"zero id":            func(c *Config) { c.ID = None },
		"empty peers":        func(c *Config) { c.Peers = nil },
		"self not in peers":  func(c *Config) { c.Peers = []NodeID{2, 3} },
		"duplicate peer":     func(c *Config) { c.Peers = []NodeID{1, 2, 2} },
		"reserved peer id":   func(c *Config) { c.Peers = []NodeID{1, None} },
		"no heartbeat":       func(c *Config) { c.HeartbeatTicks = 0 },
		"election too short": func(c *Config) { c.ElectionTickMin = c.HeartbeatTicks },
		"empty jitter":       func(c *Config) { c.ElectionTickMax = c.ElectionTickMin },
		"no entries allowed": func(c *Config) { c.MaxEntriesPerMsg = 0 },
		"no bytes allowed":   func(c *Config) { c.MaxBytesPerMsg = 0 },
	}
	for name, break_ := range cases {
		t.Run(name, func(t *testing.T) {
			c := valid
			c.Peers = append([]NodeID(nil), valid.Peers...)
			break_(&c)
			if err := c.validate(); err == nil {
				t.Fatalf("validate accepted %s", name)
			}
		})
	}
}

func TestQuorum(t *testing.T) {
	for _, tc := range []struct {
		peers int
		want  int
	}{{1, 1}, {2, 2}, {3, 2}, {4, 3}, {5, 3}} {
		c := Config{}
		for i := 1; i <= tc.peers; i++ {
			c.Peers = append(c.Peers, NodeID(i))
		}
		if got := c.quorum(); got != tc.want {
			t.Fatalf("quorum of %d peers = %d, want %d", tc.peers, got, tc.want)
		}
	}
}
