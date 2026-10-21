# PRD — M01-W02: controlled interruption and repetition

Owner: sw-camille (Product Owner). Status: **ACTIVATED; execution gates pending**.
Date: 2026-10-21 Asia/Shanghai. Source baseline: dev `815c282`.

## Activation, goal and scope

The user requested starting and finishing the second release of the first
milestone, activating **M01-W02** from [the milestone plan](../../plan/milestone-01.md).
This is process namespace **release_5**, unrelated to historical Release 2 or
package versions. W01 has [PO acceptance](../release_4/acceptance.md); its closure
is integrated at dev `815c282` and promoted to main `b03092e`. Historical W01
records that describe promotion as pending predate that actual promotion.

The researcher must be able to interrupt the synthetic thermal reference,
inspect what actually completed, understand whether data was saved, and perform
a clean repeat. Extend the bounded [reference example](../../examples/thermal_experiment.py)
and [guide](../../docs/user-guide/thermal-experiment.md), characterize existing
Process/Scheduler cancellation and reset, and demonstrate deliberate reset and
fresh repetition. Example-local behavior is preferred. A minimal compatible
`huo` extension is permitted only after source characterization demonstrates a
concrete need and the Architect and PO review its scope before implementation.
This PRD specifies observable behavior, not the implementation surface.

Work proceeds serially on `codex/m01-w02-setup` from dev; the Architect owns
task decomposition. [AGENTS.md](../../AGENTS.md) and [principles.md](../../principles.md)
override generic skill branch rules: task candidate CI before dev integration,
release acceptance before main promotion. No implementation or unrun gate is
certified by this requirements activation.

Excluded: durable partial-run recovery or resume, automatic retry, arbitrary
process preemption, concurrent reset, physical termination, real instruments,
hardware/firmware, W03 rich/versioned manifest, W04 adapters, public universal
run framework, backend storage-format repair, new dependencies, supported-Python
changes, UI/Figma, package version/tag and publication. Partial rows must remain
inspectable in memory; automatically persisting interrupted runs is not required.

## Acceptance criteria

All criteria are Must. The Tester owns executable assertions/results, the
Developer reviews test implementability, the Designer owns detailed design and
code review, and the Architect reviews design. Technical issue closure remains
with its reviewer or Tester.

| ID | User need / behavior | Verifiable acceptance condition |
| --- | --- | --- |
| AC-01 | Preserve the successful reference experiment. | Default uninterrupted invocation still produces the W01 eight-row delayed trace, schema/units, fresh UUID, verified reopened HDF5 and headless PNG. Existing public imports, hooks and return behavior remain compatible. |
| AC-02 | Know the existing cancellation/reset limits before changes. | Committed source/caller characterization and assertions establish the relevant Process/Scheduler cancel, completion, reset and repeated-run behavior, including failure/status returns and owned-resource handling. Reset is never treated as evidence of stopped execution. |
| AC-03 | Stop cooperatively at a documented acquisition boundary. | A supported request is observed before the next advance/sample cycle. Acknowledgment occurs only when the example has accepted the request and will issue no further advance/sample cycles. The guide distinguishes request from acknowledgment and synchronous work already in progress. |
| AC-04 | Obtain a deterministic stopped result. | Inject a stop request immediately after completed row K, with 0 < K < N: exactly K rows and K committed ticks remain, outcome is cancelled, and no coordinator tick occurs after acknowledgment, including during persistence decisions or cleanup. Stop before the first cycle yields zero rows/ticks. A late request after acquisition completion does not retroactively cancel completed acquisition. |
| AC-05 | Retain truthful partial acquisition after model failure. | Inject a model failure on the next attempted tick after K completed rows: exactly K rows remain, the failed tick publishes no new coordinator boundary, acquisition reports failed with cause and final committed boundary, no retry or fabricated row occurs, and cleanup issues no extra step. |
| AC-06 | Retain truthful partial acquisition after sampling failure. | Fail sampling after a successful next tick following K completed rows: exactly K complete rows remain, the final committed boundary may be K+1, failed acquisition exposes that difference and cause, and no partial/fabricated row, rollback claim or retry occurs. |
| AC-07 | Distinguish acquisition, persistence and overall outcomes. | A caller can inspect terminal acquisition outcome (completed/cancelled/failed), requested/completed row counts, final committed boundary, diagnostics and persistence disposition. Completed acquisition followed by save/reload failure remains completed acquisition with failed persistence and unsuccessful overall execution; plotting failure identifies any verified stored result separately. |
| AC-08 | Make partial retention and command failure visible. | Cancelled/failed acquisition returns or otherwise exposes retained complete rows through the documented supported inspection path. Its persistence disposition is explicitly not attempted; it does not save/plot the partial record or announce a completed result. The command exits unsuccessfully for cancellation, acquisition/persistence/plot failure and cleanup obstruction. Empty partial results are inspectable without pretending that a valid completed file exists. |
| AC-09 | Reset deliberately after termination. | An example demonstrates reset only after execution has terminated and owned asynchronous work has drained; reset is a separate explicit model operation, never cleanup. Successful reset restores declared initial boundary/state. Injected reset failure is visible and preserves the prior committed boundary/state according to the Coordinator contract. No active execution is resumed implicitly. |
| AC-10 | Repeat without contamination after interruption/failure/reset. | Two fresh full executions after exercised cancellation/failure/reset paths produce equal finite numeric traces within W01 tolerance, different UUIDs, exactly N rows each and reopenable fresh files. No previous rows, stop state or failed state leaks into either repeat; wall timestamps/UUIDs are excluded from numeric comparison. |
| AC-11 | Release owned resources and preserve borrowed state. | Success, pre-first-cycle cancellation, mid-run cancellation, model/sample failure, persistence/plot failure and reset failure have asserted cleanup outcomes. Owned scheduler/tasks/loop/backend/figures are released when their operations succeed; cleanup obstruction is reported alongside the primary outcome. Borrowed/global scheduler, event loops and figures remain unchanged. No teardown step/reset occurs. |
| AC-12 | Reproduce and understand the supported journey. | The guide provides repository-root headless commands or executable examples for normal run, deterministic cooperative stop, partial inspection, deliberate reset and fresh repeat. It documents boundary timing, all outcome/persistence meanings, software-only cancellation limits, no-resume policy, owned resources and failure diagnostics. An independent Tester follows the documented path using temporary synthetic outputs. |

## Constraints and edge cases

The W01 numerical fixture remains `T_initial = T_ambient = 20 degC`, C=10 J/K,
G=1 W/K, dt=1 s, P=10 W, N=8. Its delivered trace is
`20, 21, 21.9, 22.71, 23.439, 24.0951, 24.68559, 25.217031`;
absolute comparison tolerance is `1e-10`. Observation source boundary is tick-1;
wall time remains separate. No hidden warmup, rollback or extra reset tick is
allowed. Existing configuration and collision refusals still precede acquisition.

Acknowledgment is an execution boundary promise, not synchronous preemption or a
physical stop claim. A request during synchronous advance/sample work can be
acknowledged only after that work returns; completed rows from that in-flight
cycle may therefore remain. Tests must observe both request and acknowledgment,
not infer acknowledgment from a request flag. Requests already acknowledged
cannot permit another cycle. Stops before preparation/acquisition must not
fabricate a committed sample. A failure already raised is not erased by a
subsequent stop request. Detailed design must specify observable precedence and
terminal reporting consistently with these requirements.

Inspection may use an example-local result or documented failure carrier; no
public library API is implied. Exact fields/encoding, invocation surface and
test seams belong to reviewed detailed design. Outcome metadata for an otherwise
successful stored reference may use existing explicit text conventions; rich
provenance/schema evolution remains W03. Never claim partial persistence is
atomic or that a failed save produced no bytes. Refuse existing output locations
and preserve experiment data. Tests use fresh temporary paths and synthetic data.

Reset demonstration must preserve the pre-reset terminal result for inspection.
A successful reset starts no acquisition by itself. Fresh repeats use fresh
model/station/process/record/resources; reset does not imply general reuse of a
cancelled legacy Process or promise universal retryability.

Carry forward explicitly tracked R4-BASE-SCHED-01, R4-BASE-META-01,
R4-BASE-HDF5-01 and historical OBS-004/005/006/DEFECT-2. Existing bounded measures
remain subject to regression checks; this PRD does not close those defects or
authorize broad repairs. Newly discovered prerequisites require reviewed scope
disposition, never silent fixes or waivers.

## Delivery and release gates

Require approved test plan and Developer review before approved detailed design
and Architect review; implementation follows those gates. Independent review
must close every scoped issue, and Tester must close every test failure. Run
unittest discovery, compileall and import smoke checks for Python changes,
record actual Python/dependency versions, warnings/skips and limitations, and
check configured Python 3.9/3.13 candidate CI before dev integration. Named
zero-warning claims need executed evidence; unconfigured lint is not implied.

Every completed process artifact is committed/pushed immediately with
`docs(log):` before handoff. Inspect all eleven principles at release start,
task completion and release completion. Update actual architecture and user
documentation; perform scope/diff/status checks; obtain explicit PO acceptance
against AC-01–12 before main promotion. M01-W02 acceptance is not acceptance of
the whole first milestone. No fixed calendar deadline is promised.
