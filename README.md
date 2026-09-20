# Rivet

Rivet is a sharded, fault-tolerant distributed key-value storage engine built to study and demonstrate explicit distributed-systems guarantees. The implementation target is Go with gRPC/Protobuf, independently implemented Raft groups, a custom WAL and snapshots, and a replicated metadata control plane.

The P0.0 foundation was approved on 2026-09-06. The project is now at the **P0.1 storage-engine design gate**. There is deliberately no storage-engine or distributed-systems implementation yet; implementation begins only after SPEC-003 is reviewed and approved.

## Start here

1. [Project charter](specs/000-project-charter.md)
2. [Failure model](specs/001-failure-model.md)
3. [Client API and consistency contract](specs/002-client-api.md)
4. [P0.1 storage-engine specification](specs/003-storage-engine.md)
5. [Initial architecture](docs/architecture.md)
6. [P0/P1 milestone dependencies](docs/milestones.md)
7. [Approved design questions](docs/open-design-questions.md)
8. [Architecture decisions](adr/README.md)
9. [Repository bootstrap proposal](docs/repository-bootstrap.md)
10. [CI plan](docs/ci-plan.md)
11. [P0.0 Definition of Done](docs/p0-0-definition-of-done.md)

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

## Current gate

Review [SPEC-003](specs/003-storage-engine.md), especially its seven remaining review questions, failure table, test plan, and P0.1 Definition of Done. Do not implement P0.1 until SPEC-003 is approved and the remaining bootstrap choices needed by code are settled.
