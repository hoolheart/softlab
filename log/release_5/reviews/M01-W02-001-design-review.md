# M01-W02-001 Architect detailed-design review

Reviewer: sw-jerry (Architect). Date: 2026-10-22 Asia/Shanghai.
Subject: design commit `2bec5a3`, branch codex/m01-w02-controlled-execution.
Verdict: **APPROVED — Tester feature red tests may proceed**.

Reviewed [detailed design](../design/M01-W02-001-design.md) against
[PRD](../prd.md), [architecture envelope](../architecture-plan.md),
[test plan](../test/M01-W02-001-plan.md), Developer approval `d7036c4`,
Tester characterization `d7b73a6`, current example/W01 callers and relevant
Process, SimpleProcess, SweepProcess, AtomJob and Coordinator source contracts.
The plan's historical pending header is superseded by Developer-owned approval.
This is architecture/design review, not implementation, code review or test
execution. No review finding or test failure owned by another role is closed.

| Concern | Review finding |
| --- | --- |
| Scope and compatibility | Changes remain example/guide/tests only. Existing five-element/library exports, dependency/support and storage contracts are preserved. run(config, output_dir) retains successful verified DataGroup return; failure carrier subclasses RuntimeError. Existing helper seams and W01 fixtures remain regression constraints. No library change is proposed or authorized. |
| Process orchestration | Source characterization supports the local commit/join boundary: run_process resets/runs without a stop seam, SimpleProcess reset cannot preempt, Sweep records retain rows. One fresh count/record per acquisition and explicit commit/join failure handling avoid claiming universal reuse or retry. Stopping after N complete rows omits only count's bookkeeping completion commit, with actual work drainage still required. |
| Request/acknowledgment | Executor acknowledges only before another cycle; N-row completion has priority over late requests, current synchronous cycle may finish, raised failure cannot be erased by a stop request. Request and acknowledgment remain separate observables through cleanup. No thread safety, hardware stop or synchronous preemption claim. |
| Truthful rows and boundaries | Group allocation precedes prepare, complete rows survive cancellation/failure, actual committed tick/time are independent snapshots. Failed model tick leaves K rows/ticks; failed sample can leave K rows/tick K+1. No partial save/plot, extra teardown step, rollback or retry. Empty partial inspection is explicit. |
| Stage outcomes | Acquisition, persistence, plot and cleanup distinguish completed sampling from later failure. Verified storage requires disconnect/copy reload/round-trip verification; plot failure retains its separately verified result. Existing text metadata is unchanged, without W03 manifest creep or false no-byte/atomic storage claims. |
| Ownership and termination | Independent stop, task cancellation/drain and loop-close attempts preserve combined primary/cleanup diagnostics. Facts work_drained and loop_closed reflect actual operations, not stop requests; other backend/figure/scratch cleanup remains independent. Borrowed/default resources remain outside ownership. BaseException cleanup/re-raise behavior is retained. |
| Reset and repetition | Explicit reset helper rejects active/undrained/unclosed state, retains terminal snapshots and uses existing Coordinator staged reset semantics. No reset in cleanup; no process restart from reset. Fresh repeats create independent resources, topology, data and stop state. Reset callback external side effects are not covered by Coordinator atomicity. |
| Acceptance traceability | Design maps AC01–12 to unchanged W01 assertions, characterized legacy behavior and planned deterministic feature tests/guide journey. Test plan requires actual operation/fault injection rather than mocked acquisition or prose matching. No runtime or acceptance gate is inferred from this review. |

No blocking design findings raised. Tester must now implement and execute the
approved feature contracts against committed preimplementation source and record
expected red results; Developer implementation follows that evidence. Red tests
must independently observe actual committed boundaries, row retention and request/
acknowledgment through cleanup, including in-flight/late stop and failure
precedence. Reviewer/Tester later own their respective issue/failure closures.
These are existing approved test obligations, not new design requirements.

R4-BASE-SCHED-01, R4-BASE-META-01, R4-BASE-HDF5-01 and historical
OBS-004/005/006/DEFECT-2 remain open with the established bounded measures.
No prerequisite justifies a library modification from current evidence. Any
newly demonstrated prerequisite blocks affected implementation for Architect/PO
scope review. Feature runtime tests, implementation, independent code review,
full verification, principle completion, exact candidate CI, dev integration,
PO acceptance and main promotion remain pending. UI/Figma and hardware N/A.

This review and board phase handoff require immediate docs(log) commit/push;
coordinator verifies actual file/commit/upstream evidence before progression.
