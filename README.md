# Rivet

Rivet is a sharded, fault-tolerant distributed key-value storage engine built to study and demonstrate explicit distributed-systems guarantees. The implementation target is Go with gRPC/Protobuf, independently implemented Raft groups, a custom WAL and snapshots, and a replicated metadata control plane.

## What is here

`internal/storage` holds the single-node durable storage engine: a deterministic key-value state machine over a versioned write-ahead log, with atomic snapshots, strict crash recovery, and bounded client sessions for retry recognition. It has no external dependencies.

Nothing else is built yet. Replication, sharding, routing, and the metadata control plane described below are design work, not code.

Design documents are maintained outside this repository.

## Scope boundary

- P0 builds a legitimate sharded, replicated, durable KV system with explicit linearizable per-shard operations, recovery, routing, failover, correctness checking, and observability.
- P1 adds safe online shard reconfiguration, explainable load-aware rebalancing, overload protection, Kafka-backed at-least-once change streams, Kubernetes deployment, and deeper chaos/benchmark evidence.
- P2/P3 work is out of scope unless explicitly approved.
- Rivet assumes non-Byzantine participants; it does not defend against malicious nodes, corrupted operators, or adversarial clients.

## Working rule

Every significant subsystem follows:

```text
specification -> invariants -> alternatives -> design -> failure analysis
              -> test plan -> implementation -> fault testing -> measurement
              -> documentation -> postmortem when warranted
```

No correctness or performance claim is made without a defined model and reproducible evidence.
