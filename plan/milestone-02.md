# M02 — Proposed reusable execution milestone

Status: **PROPOSED — planning only**. M01 is [CLOSED](../log/release_7/closure.md);
this plan does not implement or accept M02. Four serial, week-sized increments
are planning envelopes, not calendar promises or package releases. Assign an
unused `log/release_x/` namespace at kickoff. The objective is to reduce
repeated execution glue for a bounded count acquisition while preserving the
accepted thermal workflow and existing `huo` contracts. No real equipment is
needed. Agree an implementation PRD and acceptance criteria at kickoff; if
capacity or evidence is insufficient, narrow scope or move the exit date.

## Evidence and decisions at kickoff

- The [M01 follow-on backlog](../log/release_7/follow-on-backlog.md) identifies
  example-specific execution and timing, the direct adapter's extra observation
  callback, and existing debt. M01 provides two adapter policies for one procedure,
  not proof that a general-purpose framework or multiple domains need the same API.
- [`thermal_experiment.py`](../examples/thermal_experiment.py) owns a private,
  serial, unchained count scheduler and event loop, stop request, terminal result,
  acquisition loop and cleanup. Its persistence, plotting and manifest validation
  are example-specific. Evaluate each seam against actual callers; do not simply
  publish or copy the entire example. Compare existing `Process`, `Scheduler`,
  `count`, `scan` and their callers before choosing an extension.
- Establish a second **small synthetic count client** with a distinct acquisition
  policy; use an independent contract double additionally where useful. Demonstrate which orchestration is
  shared and which remains caller-owned. If no common seam is supported by this
  evidence, document the decision, retain example-local execution and revise the
  M02 scope/exit through product review rather than inventing an abstraction.
- Record the impact and existing dispositions of R4-BASE-SCHED-01,
  R4-BASE-META-01, R4-BASE-HDF5-01, OBS-004/005/006 and DEFECT-2. A defect
  blocking an agreed gate needs a separately scoped disposition; unrelated debt
  is not silently fixed or waived. Capture baseline warnings, including the M01
  pandas concatenation and GitHub Actions runtime warnings, in the environments
  where they actually occur.

## Product acceptance and planning authority

The product benefit is that a maintainer can write a second bounded count client
by supplying its acquisition policy while reusing the reviewed execution and
lifecycle behavior. W01 records the before/after responsibility map and the
specific repeated orchestration targeted for removal. W04 demonstrates that both
clients use that seam without copied execution loops or caller-side cleanup
workarounds for behavior the seam claims to own. This is a maintainability claim;
no line-count reduction, performance improvement or cross-domain suitability is
accepted without separate evidence. The second client must differ in acquisition
policy; renaming the thermal example is insufficient. A contract double can
characterize semantics but, by itself, cannot establish a useful second client.

Each increment below is a proposed sub-release with its own bounded PRD,
acceptance evidence and unused log namespace assigned at kickoff. Product Owner
owns scope and acceptance; Architect owns feasibility/decomposition; Tester owns
tests and test-failure closure; independent reviewer owns review closure.
Developer and Designer participate at the applicable test/design/implementation
handoffs. W01 includes executable characterization work and therefore retains
behavioral test/review gates. Every sub-release closes using the project release
checklist (candidate CI before `dev`, acceptance before `main`, promoted-state
verification and clean worktree) before the next starts. W04 additionally closes
the milestone. This planning document does not start any sub-release.

## Proposed increments

| Increment | Outcome and bounded work | Acceptance and dependency |
| --- | --- | --- |
| M02-W01 — Characterize and select seam | Characterize current count/scan callers, scheduler and process lifecycle, default/global versus owned resources, callback order, cancellation, failure and repeated use. Compare both thermal adapters and a second synthetic count use case; document duplication, non-shared behavior and a minimal candidate with explicit ownership and API compatibility. | Assertion-based baseline tests preserve existing behavior, including scan, failure and cancellation; independent review accepts evidence and a scoped design decision. A blocker or lack of evidence triggers a recorded scope decision before implementation. Depends on M01 closure. |
| M02-W02 — Bounded execution | Implement only the reviewed shared count-execution seam in the appropriate existing `huo` abstraction, with explicit advance/sample ordering, terminal acquisition state, cooperative request/acknowledgment and ownership. Keep configuration, persistence, plotting and model-specific policies with callers. | The synthetic client and reference integration fixture use the same supported seam without changing existing count/scan semantics. Tests cover ordered operations, success, stop before and after a row, failure before/after advance, no fabricated row or post-acknowledgment work, and independent cleanup. Depends on W01 accepted design. |
| M02-W03 — Lifecycle and repetition | Complete the approved lifecycle boundary: observable primary and cleanup failures, partial in-memory rows and final boundary, owned versus borrowed resource cleanup, fresh repeated runs and explicit reset eligibility. Define retry behavior; default to no automatic retry unless a specific safe retry is independently justified and tested. | Fault injection verifies primary cause and cleanup diagnostics, terminal state, no unsafe implicit reset/retry, repeated fresh runs and no borrowed/global resource disposal. If an advance succeeds but sampling fails, final boundary may exceed row count; no rollback is claimed. Depends on W02. |
| M02-W04 — Integration and acceptance | Migrate both M01 adapter modes to the accepted seam, retain the documented end-to-end command, update API/docstrings and guide, complete independent review, regression, principle inspection and milestone acceptance. | Coordinated and direct modes retain numerical traces, timing policies, record schema and v1/v2 stored-context behavior. Existing callers pass characterization and regression checks. A fresh documented walkthrough and repeat pass; candidate CI, promotion and closure follow the project checklist. Depends on W03. |

The boundaries are an ordering proposal, not permission to overlap tasks. If
implementation cannot be separated safely, combine related work into one serial
task and scale its gates to the resulting change. Avoid a separate release or
commit for each administrative status update.

## Release details

### M02-W01 — Characterization and seam decision

**Deliverables:** a caller and ownership map; an assertion-based compatibility
suite; an inventory of existing failures/warnings; a comparison of the two M01
adapter timings and a second synthetic count scenario; and a short decision
record with proposed interfaces, ownership, limitations and rejected options.
Inspect `Process.commit/join/reset`, scheduler point and loop behavior, count and
scan hooks and their actual callers. Cover normal use, invalid operations,
cancellation, errors and repeat behavior as currently observed; document gaps
without treating current defects as desirable guarantees. Record whether the
second client really duplicates orchestration and why the chosen seam belongs in
`huo`. Review the test plan for implementability and design independently.

**Acceptance:** baseline assertions run against unchanged behavior; operation
order and lifecycle are documented with evidence; product and architecture
review agree whether an extension is justified and define its supported scope.
If the evidence does not justify extraction, return for a reviewed milestone
scope change. Do not start W02 on an assumed API or an unresolved prerequisite. Assign all
lifecycle work required to release W02 safely to W02, even if that reduces other
W02 scope or moves its date; do not defer inseparable cleanup or truthful failure
reporting to W03.

### M02-W02 — Shared count execution

**Deliverables:** reviewed tests and detailed design; the smallest justified
execution seam; a synthetic client exercising it; and a bounded reference integration fixture exercising both adapter policies
while preserving existing public entry points. The shipped thermal CLI migrates
in W04; W02 proves integration through tests without maintaining a second public
runner. Define precisely
whether the caller or executor owns each scheduler, loop, action, callback and
record. Explicitly represent advance and measurement policy without moving
thermal model logic into generic execution. Preserve the existing supported
scope of synchronous/async work; any new capability requires separate design
and tests.

**Acceptance:** ordered call traces prove command/advance/read/row-commit
behavior for both reference adapters and the second client. Independent tests
cover completed count, stop at zero and after a known row, late stop, advance
failure and read failure after advance. Incomplete rows are not added, no
additional cycle starts after acknowledgment, and existing count/scan tests
remain unchanged. Independent code review and tester-owned issue closure precede
candidate CI and integration. Before W02 release, basic owned-resource cleanup
must pass on success, cancellation and failure, and terminal state must preserve
the primary failure and any cleanup failure. W03 extends fault coverage and
lifecycle refinement; it is not a prerequisite deferred past an incomplete W02
release.

### M02-W03 — Terminal lifecycle and repeatability

**Deliverables:** a reviewed lifecycle and failure contract, implementation and
fault-injection tests for paths actually supported by W02, and usage guidance.
Document terminal status, error precedence, cleanup diagnostics, reset
preconditions and behavior on reset failure. Define ownership transfer and
borrowed-resource handling. Evaluate retry as a decision, not a requirement to
perform one: automatic retry stays absent unless an idempotent boundary and its
observable effects are demonstrated.

**Acceptance:** injected scheduler, acquisition and cleanup failures retain
primary and cleanup evidence; completed rows and last boundary are truthful;
owned work is drained and closed while borrowed resources remain usable. If borrowed
execution resources are unsupported, reject them before side effects and test
that rejection rather than implying borrowing support. Test
interruption, failed reset and two fresh repeats. Terminal evidence survives
reset, UUIDs differ, and deterministic traces match after excluding timestamps.
A sample failure after advance is represented honestly even when the final
boundary exceeds the completed-row count. Tests do not claim that blocking or
physical work was stopped by software cancellation.

### M02-W04 — Reference integration and milestone acceptance

**Deliverables:** final reference migration for both adapters; updated example
and public API guidance; compatibility and fresh-environment walkthrough
evidence; independent code and product reviews; principle inspection; and one
closure summary linking all increments, debt and limits. Retain v1 coordinated
and v2 direct manifest semantics, numerical results, schema, reload validation,
CLI behavior and example ownership. The complete guide command must still save,
reopen, verify and plot using a fresh synthetic output directory.

**Acceptance:** compare both modes to independent numerical/timing oracles and
existing M01 evidence; test corrupt/unsupported manifest behavior and fresh
process inspection. Run the full relevant regression, compile and import checks,
record warnings, and independently exercise a fresh-environment walkthrough.
Product acceptance maps the increments to actual evidence and states remaining
limitations. Freeze the candidate, pass exact-candidate CI before `dev`, promote
accepted content to `main`, verify required promoted-state checks, reconcile
closure facts and verify a clean worktree. If a required gate fails or cannot be
run, record the limitation and leave the milestone open.

## Required execution contract and test matrix

The PRD and reviewed design must define the seam's supported inputs, returns,
exceptions, ordering, ownership and state transitions before code changes.
The following are acceptance obligations, not a prescribed class hierarchy:

1. **Ordering and timing:** assert one intended advance per completed cycle;
   an attempted acquisition can advance and then fail before row commit, and must
   retain the last known boundary without fabricating a completed row. Assert
   reads and writes in the declared order, row commit only after all required
   reads succeed, and separate acquisition boundary from measurement source.
   Test both previous-boundary and current-boundary policies. Direct observation
   calls a pure model callback again in M01; do not assume repeated observation
   is safe for stateful or nondeterministic sensors. Test a counting/stateful
   double to expose unintended extra reads; retain policy with the adapter.
2. **Terminal outcomes:** distinguish completed, failed and cooperatively
   cancelled acquisition; record completed rows, last known boundary, cause,
   request and acknowledgment. A request is not an acknowledgment, and a late
   request after completion does not retroactively cancel success. Preflight
   errors and interruption/cleanup behavior must be specified and tested.
3. **Failure and ownership:** inject advance, control write, sample read,
   scheduling and cleanup failures; retain completed rows without manufacturing
   data. Preserve the primary error alongside cleanup failures. Drain/close only
   owned work and resources, including event loops; never stop a borrowed/global
   scheduler. Specify unsupported threaded/chained actions and running-loop use
   rather than implying support. Cooperative cancellation cannot preempt blocking
   work or establish physical termination.
4. **Reset and repeat:** test reset only after the documented terminal and drain
   conditions, including a failed reset. Preserve terminal evidence. Fresh runs
   have distinct identities and matching deterministic numeric traces; exclude
   wall timestamps and UUIDs from numerical comparison. No durable resume,
   rollback or automatic retry is implied.
5. **Compatibility:** characterize existing public `Process`, `Scheduler`,
   `count` and `scan` behavior and imports before modification. Confirm unchanged
   legacy callers and both M01 manifest versions, including separate-process
   reload and incompatible/corrupt metadata rejection. Do not make M03's general
   provenance schema or storage repair a prerequisite for execution extraction.

## Scope boundaries and follow-on handoff

M03 owns a separately reviewed bounded provenance design; M02 preserves the
existing example envelope and does not promise environment locking, arbitrary
serialization, migration or persisted partial results. M04 owns the named
transport, simulated adapter contract, physical settling/readback/timeouts and
access decision. No physical or VISA qualification follows from M02. Scan/grid
extensions belong to M05; M02 characterizes existing scan behavior without
promising new scan semantics. M07 evaluates durable partial results and recovery.
No new dependency, global scheduler replacement, general run service, async
framework, universal adapter contract or hard real-time claim is implicit.

## Finish line

Before implementation, record required independent review, tests, product
acceptance, exact integration-candidate CI, promotion and promoted-state checks,
plus clean worktree, as defined by the [release checklist](../log/templates/release-checklist.md).
For Python changes run unittest discovery, compile and import checks in the
actual environment; candidate CI supplies its own Python-version evidence.
Use synthetic data and temporary output paths, and record warnings and limits.
Freeze required code and documents before candidate CI; reuse valid evidence for
unchanged content. Product acceptance maps every increment to evidence and
records any approved scope changes. Mark M02 **CLOSED** only after the applicable
checks and promotion are verified and the worktree is clean. A plan or a passing
local test alone is not acceptance.
