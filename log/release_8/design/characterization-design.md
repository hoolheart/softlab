# M02-W01 detailed design — characterization and seam decision

Owner: Software Designer (sw-celeste role) | 2026-10-30 Asia/Shanghai
Status: **APPROVED for predevelopment preparation** by the
[Architect review](../reviews/design-review.md); characterization remains pending.

Inputs: [requirements](../prd.md), [architecture review](../architecture-review.md),
[written test plan](../test/characterization-plan.md), its
[Developer approval](../reviews/test-plan-review.md), and the
[M02 plan](../../../plan/milestone-02.md). Source inspection informs the design;
no runtime result or extraction decision is claimed. User-requested pause
prohibits executable test creation, production changes and new experiments.

## Structure and observations

After resume, the Tester creates `tests/test_m02_w01_characterization.py`, using
`unittest` and small test-local fixtures. Organize it into caller/lifecycle,
count/scan, thermal policy, signal monitor, failure/stop/repeat and isolated
scheduler cases for T02–T10. T01 is a source and caller inventory; T11 is the
evidence-based decision and T12 covers review and release gates. These are
documentation and gate activities, not executable fixture groups. Keep the
evidence inventory and comparison in release documentation, not in production
modules. Reuse existing
thermal tests and helpers only where their ownership and observations are clear;
retain their effects when wrapping them. Test actual public imports and callers,
including `run_process`, rather than a stand-in for their behavior. Establish
baseline revision, environment and warnings before adding tests.

Independent event sinks append immutable tuples at source configuration,
synthetic boundary advancement, each getter invocation, derived computation,
record append and lifecycle operations. Instrument wrappers must call the
original operation exactly once, log the attempt and completion separately, and
avoid extra inspection reads. Capture rows as values at append, boundary index,
return/exception/diagnostic, resource state and the associated event index.
Use explicit expected events, not values reconstructed from the observed trace.
Ignore timestamp and UUID only in numeric repeat comparison; separately verify
fresh identity when the relevant client exposes it. Characterize actual legacy
return and exception paths without declaring them new guarantees. Assertions for
future candidates remain distinguishable from observed baseline assertions.

Source inspection shows `run_process` resets the process, selects the singleton
for a non-`Scheduler` argument, ignores the return of `commit`, checks `join`,
and returns a boolean and elapsed time. `AtomJob` has hooks and accumulates values
for a record; its precise runtime failure behavior still requires observation.
Thermal `_CountScheduler` owns a private loop, accepts unchained nonthreaded
actions and is a useful controlled fixture, not evidence that the generic
concrete scheduler safely supports private ownership.

## Distinct monitoring client and oracle

The test-local client configures a synthetic fixed source once, advances an
integer clock for each attempt, reads a consumptive signal then reference once,
computes signal minus reference and appends one paired row only after both reads
succeed. It supplies its own acquisition/derived-value policy; any use of the
existing count machinery must be demonstrably through its public interfaces.
If the existing record shape cannot directly hold the difference, perform a
caller-owned transformation only after both readings succeed, recording separately
which row is owned by count and which by the client. Do not change the library
or assume an append hook exists. Define and inspect both row sets so a partial
underlying row cannot be hidden by the derived record.

Use predeclared sequences signal `[10, 13, 17]` and reference `[1, 2, 4]`,
one consumption per call. Configure source to a fixed synthetic value `5` once.
For three successful boundaries the policy trace, excluding separately tagged
framework hook/lifecycle events, is:

```text
configure(5)
advance(1), signal(10), reference(1), difference(9), append(1,10,1,9)
advance(2), signal(13), reference(2), difference(11), append(2,13,2,11)
advance(3), signal(17), reference(4), difference(13), append(3,17,4,13)
```

Check exactly three reads per source, three advances, and three complete derived
rows. A separate counting double checks invocation count; it supplements the
actual monitoring client. At boundary 2 inject first-read and second-read
failures separately. Check one completed derived row, boundary 2, an attempt and
exception event, exact consumed counts and the actual underlying record/error
path; no derived row for boundary 2, no rollback claim. Failure at reference
consumes the second signal value even though its row is incomplete. Reject a
fixture whose callback observes a stateful input again to derive the difference.

## Thermal, compatibility and failure fixtures

Use deterministic existing thermal configurations with a precomputed numeric
oracle independent of the implementation, and tag command, advance, observation,
source boundary and append. Compare coordinated previous-boundary against direct
current-boundary timing; count all pure model callback observations, including
the known direct extra observation. Treat the expected extra call as a hypothesis
until tested, not permission to repeat a stateful sensor read. Use the existing
schema and v1/v2 context tests without modifying or reinterpreting stored data.

For count and scan, declare explicit hooks, setter and getter traces and expected
rows before execution. Trace dry-run events separately so terminal dry-run is
not mistaken for a sample. Capture invalid inputs and commit/join/reset results
as observed including false, exception or diagnostics. Fault at scheduling,
advance, each read and cleanup independently, then combine primary and cleanup
faults; record both actual diagnostics and every attempted cleanup. Track stop
request and acknowledgment only where exposed, late stop separately, and rows
and last boundary independently. No assertion implies preemption, rollback or
physical termination. Repeat tests record reset eligibility and state, fresh run
identities where available, and whether legacy records append or clear.

## Isolation and ownership

Each fixture owns its synthetic inputs, temporary files, actions and record.
Thermal serial fixtures own their private scheduler and loop and close them in
`finally`; assert borrowed/global resources remain usable and do not close them.
Never concurrently mutate singleton state. Concrete scheduler singleton and
private-instance completion probes each run in a separate child process, with
parent deadline and captured partial trace. Parent terminates and reaps a timed
out child; timeout is an observed failure and child cleanup is unverifiable.
Normal child paths drain/close only owned resources in `finally`. Do not patch
completion routing and call that an unmodified pass. Correlate recorded point and
callback activity before attributing an observed failure to R4-BASE-SCHED-01.
T08 may inspect private `_CtrlPoint.cancel` only as an explicitly labeled private
characterization; it is not a public scheduler cancellation API. If HDF5 reads
are involved, use temporary disposable copies and retain R4-BASE-HDF5-01.

## Evidence and decision after execution

Map every T01–T12 result to command, revision, environment, observed outcome,
warnings and issue ownership; the Tester closes test failures and the independent
reviewer closes review findings. Inventory repository callers and classify each
loop, scheduler, action, callback, record and storage owner using source facts,
observations and open hypotheses as separate columns. Assess all PRD debt items
against the actual path; do not silently waive a blocker.

Compare (1) retained local execution, (2) reuse or compatible extension of
existing helpers and (3) a minimal additive seam. For each, cite the two thermal
policies and the distinct monitor traces; list duplicated orchestration and its
owner, policy that stays with callers, required lifecycle/failure behavior,
compatibility risks, and supported/rejected modes. A useful seam needs concrete
shared behavior across real clients and a defensible ownership boundary, not a
matching vocabulary or a double alone. Unsupported borrowed resources, running
loops, chains or threads must be rejected before side effects unless explicitly
justified and tested. Preserve primary and cleanup evidence, actual boundary and
row counts, stop request/acknowledgment, and reset eligibility. If reuse is not
proven, request Product Owner/Architect scope revision. W02 must include any
cleanup and truthful failure handling necessary for its own safe supported scope;
W03 may extend it. The decision remains pending until runtime evidence and the
appropriate reviews exist.

## Sequence and gates

Written test-plan approval precedes this design and an independent Architect
review follows it. The requested pause occurs after reviewed preparation; no
characterization execution is authorized yet. After user resumption, Tester
implements and runs the suite against unchanged production behavior, documents
actual findings and closes test failures; Designer independently reviews the
result, Architect and Product Owner review the evidence-based decision, and the
project's scoped release gates apply. Exact-candidate CI, integration, promotion
and acceptance are future work; preparation approval is not release acceptance.
