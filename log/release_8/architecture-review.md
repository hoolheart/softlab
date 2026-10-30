# M02-W01 architecture and requirements review

Reviewer: Architect (sw-jerry role) | Date: 2026-10-30 Asia/Shanghai

Verdict: **APPROVED for predevelopment planning**, subject to the evidence and
future gates below. This is a source and document review; no runtime outcome,
extraction decision, executable test, implementation or release acceptance is
certified. The user's pause before development remains in force.

## Inputs and scope

Reviewed [PRD](prd.md), [tasks](tasks.md), [board](sprint-board.md), the
[M02 plan](../../plan/milestone-02.md), its [architecture review](../../plan/m02-architecture-review.md),
[M01 closure](../release_7/closure.md), [follow-on backlog](../release_7/follow-on-backlog.md),
[principles](../../principles.md), and the [release checklist](../templates/release-checklist.md).
Inspected `Process`, `run_process`, `SimpleProcess`, `SweepProcess`, `AtomJob`,
count and scan, the scheduler interface and implementation, the thermal
executor, and existing integration and compatibility callers. No new runtime
experiment was run.

## Feasibility and decomposition

The proposed signal-monitoring scenario is a plausible distinct second count
client: two potentially stateful readings and a derived difference differ from
the thermal policy, and a failed second read can expose row atomicity. A
contract double can expose extra reads, but actual characterization must still
show whether a useful amount of orchestration is shared. The proposal does not
establish a new public API or mandate extraction.

The phases in [tasks](tasks.md) follow the approved M02 order and retain one
active W01 task: written tests and developer review, design and architectural
review, the requested pause, then characterization, an evidence-based decision,
independent review/testing and release closure. W02 cannot start before W01 is
accepted and closed. UI review does not apply. W02 must include cleanup and
truthful failures needed for its supported scope; W03 may extend coverage but
cannot repair a knowingly incomplete W02 release.

## Source assessment for the Designer

| Boundary | Source facts and questions for characterization |
| --- | --- |
| Existing runner | `run_process` chooses the default scheduler for a non-scheduler argument, resets the process and returns `(bool, elapsed)`; its loop ignores the return of `commit` and checks `join`. Preserve its established contract and investigate invalid and failed transitions without recasting defects as guarantees. |
| Process lifecycle | `SimpleProcess` commits an action at a scheduler point and waits for it in `join`; reset while running sets an abort flag. `SweepProcess` drives and resets its body and sweeper. Determine actual outcomes on failure and repeated use with assertions. |
| Acquisition | `AtomJob.body` invokes hooks around setters and getters, collects values, then appends a row. Its ordering suggests a second getter failure can leave no complete row, but only runtime evidence can establish observed effects. Count and scan share this machinery; both need compatibility coverage. |
| Thermal ownership | The example owns a private serial scheduler and event loop, configures the model, advances via `hook_before_get`, checks stop between rows, and records terminal observations. Persistence and plotting are caller responsibilities. Confirm cleanup and exception paths, including the loop and backend, rather than inferring them from the count abstraction. |
| Default scheduler | The concrete scheduler's completion callback refers to the module singleton. A separately constructed instance is not automatically a safe private scheduler; retain R4-BASE-SCHED-01 and compare the example's bounded alternative without assuming a broad repair. |
| Callers | Existing tests use `run_process`, `count` and `scan` through real and simulated station integrations; the thermal example supplies its own loop. Characterization must cover actual callers as well as a distinct client. |

Compare three alternatives after the test evidence exists: retain local
execution, reuse or compatibly extend existing helpers, and introduce a small
additive seam in `huo`. State supported and rejected modes, ownership of each
loop, scheduler, action, callback and record, stop request and acknowledgment,
boundary versus committed rows, primary versus cleanup error, and reset
eligibility. Avoid promising arbitrary chained actions, threads, borrowed
resources or nested running loops without evidence and explicit tests. Reject
unsupported modes before side effects. Keep model policy and storage outside
generic execution; do not add reads or assume callbacks are idempotent.

The planned future boundary is a hypothesis. Current `run_process` and
`SimpleProcess` must not be described as already offering the richer terminal,
stop-acknowledgment or cleanup guarantees. A failed sample after an advance may
leave the boundary beyond the completed-row count; do not infer rollback.

## Findings and disposition

No blocking requirements or decomposition finding was identified. The Product
Owner was asked to retain the distinction between current behavior and the
provisional future contract; the PRD already states that the candidate is
subject to design and no implemented contract is claimed. This clarification
does not close any future runtime or review issue.

Known debt and historical warnings remain tracked in the PRD; their current
impact awaits execution in the actual environment. A blocker requires a
separately reviewed scope disposition. No fix or waiver is implied here.
Preparation document consistency, commit and push are coordinator checks;
candidate CI, product acceptance, promotion and clean-worktree release closure
remain future gates. Detailed design receives a separate architect review once
the Designer submits it.
