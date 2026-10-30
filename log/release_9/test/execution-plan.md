# M02-W02 bounded count execution test plan

Owner: Tester | 2026-10-30 Asia/Shanghai  
Status: **PLANNED — requires developer review and execution against the frozen implementation.**

The finish line is independent code review and test-failure closure, product
acceptance, exact integration-candidate CI before `dev`, verified promotion and
required promoted-state checks, and a clean worktree. This plan asserts no gate
has passed. Inputs: [M02 plan](../../../plan/milestone-02.md),
[accepted W01 decision](../../release_8/design/evidence-decision.md), its
[architect review](../../release_8/reviews/evidence-decision-review.md),
[scope agreement](../../release_8/reviews/scope-agreement.md), and
[baseline results](../../release_8/test/characterization-results.md). API names
are resolved by the reviewed W02 design; assertions below are behavioral, not
an alternative implementation specification.

## Characterization and independent oracles

Existing `count` and `scan` use a supplied scheduler and preserve hook order;
legacy reset during execution is cooperative, and repeat appends to the record.
W01 observed a generic private scheduler completion-routing defect and broken
generic timeout behavior; neither is a new executor oracle. The thermal serial
prototype rejects unknown points and unsupported actions with `False`, and uses
`None` for timeout. Its direct mode performs one extra caller-owned pure model
observation per row; coordinated mode reads previous-boundary values. A paired
signal/reference monitor differs in acquisition policy and a failed second read
can consume the first value without committing a row. Existing W01 tests supply
assertion-based legacy characterization; do not modify their expected results to
make a new implementation pass.

Use fixed, predeclared event lists and numerical inputs, independent of the
implementation. Instrument the actual acquisition record, not a copied
`DataGroup.record` result or persistence replay. Use consumptive counters to
catch extra reads. Tests must use synthetic sources and temporary resources; no
hardware, network, or existing experiment data. Check actual terminal results,
not only exceptions or console text. Treat scheduler/loop ownership separately
from caller-owned process, records, source callbacks and model. Awaitable action
coverage uses a real coroutine; do not infer preemption of blocking work.

## Test matrix

| ID | Stimulus | Required independent assertions |
| --- | --- | --- |
| E01 | Started serial scheduler: synchronous and awaitable action, then reuse for another point. | Each action runs exactly once; wait is `True`, repeated wait does not rerun; start/running/stop state is truthful. Public Scheduler interface is satisfied. |
| E02 | Never-started and stopped scheduler; started scheduler with chained, threaded, unknown-endpoint and already-used endpoint actions. | Each unsupported commit returns `False`, with action-body counter zero for rejected actions. Unknown/missing wait returns `False`. Recommit cannot replace an accepted action. |
| E03 | Async action with deterministic timeout, unknown point, and deliberate action exception. | Timeout returns `None`, unknown/failure returns `False`, success returns `True`; exception identity and diagnostic remain observable; no timed-out action is rerun implicitly. Cleanup pending work. |
| E04 | Attempt synchronous entry from an already running event loop; unsupported borrowed/global resource input if API exposes it. | Reject before creating owned resources or invoking acquisition callbacks; caller loop and singleton remain usable and unchanged. If borrowing is not an input, document that no borrowed-resource mode exists. |
| E05 | Synthetic monitor: configure once, advance, read signal, read reference, derive difference, commit row, for three cycles. | Compare exact trace and independent expected values. Exactly one of each required read per cycle, three committed rows, boundary 3; no executor-supplied extra observation. Same public seam as reference fixture. |
| E06 | Reference integration fixture with previous-boundary and current-boundary adapter policies. | Independently assert command/advance/read/row order and numeric source boundaries; count all direct extra observations, which remain adapter policy. Both fixtures use the same seam without importing an example-private executor. |
| E07 | Stop requested before first cycle; request after row 2 of a longer run; request after final row. | Status and request/acknowledgment distinguish all cases; first has zero rows/advances, second has exactly two and no post-ack cycle, late request leaves completed status and is not acknowledged. No physical termination claim. |
| E08 | Advance fails before changing boundary; first and second reads fail after advance. A separate reference fixture tests a caller-owned control-write failure before runner invocation; also inject a control-write failure inside a scheduled action. | For runner failures preserve primary exception identity and truthful boundary independently of completed rows; no incomplete row, rollback or implicit retry; assert consumptive read counts. For caller setup failure assert its exception and caller cleanup without claiming a runner result; in-action write failure is retained as the runner primary. |
| E09 | Inject scheduling/commit failure and cleanup failure independently and together with acquisition failure. | Terminal failure retains primary cause and separately reports cleanup diagnostics; all independent cleanup steps attempted. On success, cancellation and failure owned work is drained and loop closed; any obstruction is reported, never represented as successful cleanup. |
| E10 | Borrowed thread event loop and module-global singleton exist before each run. | Identity and usability persist after success, cancellation and failure; only owned resources stopped or closed. |
| E11 | Run unchanged existing count/scan characterization and public imports; run count with new scheduler and existing `run_process`. | Existing ordered hooks, imports, results, reset/abort and append-on-repeat behavior stay unchanged. Scan receives no new behavior claim. |

Timeout and cleanup injection must be bounded, and test cleanup must run even
when assertions fail. If a fixture prevents cleanup, report exactly which step
was obstructed; do not assert that a forcibly terminated process cleaned up.
Repeat/reset refinement and expanded fault coverage belong to W03, but basic
W02 terminal cleanup and preservation of primary plus cleanup failure are not
postponed. Running-loop rejection must not leak an unawaited coroutine.

## Commands and evidence

Execute from the repository root in the actual installed environment; record
revision, interpreter, dependencies, counts, exits, warnings, skips and
limitations. Adapt the new suite filename to the delivered files.

```sh
.venv/bin/python --version
.venv/bin/python -W all -m unittest discover -s tests -p 'test_m02_w02*.py'
.venv/bin/python -W all -m unittest discover -s tests -p 'test_m02_w01_characterization.py'
.venv/bin/python -W all -m unittest discover -s tests -p 'test_huo_execution_contracts.py'
.venv/bin/python -W all -m unittest discover -s tests -p 'test_*.py'
.venv/bin/python -m compileall -q softlab
.venv/bin/python -c 'import softlab; print(softlab.__version__)'
git diff --check
```

Tester records actual outcomes and closes test failures after verification;
independent reviewer owns review findings. Python 3.9/3.13 CI is credited only
for an actual passing exact candidate. Historical pandas and Actions warnings
are recorded where observed, not assumed absent or waived. Baseline scheduler,
metadata, HDF5, OBS-004/005/006 and DEFECT-2 dispositions remain as in W01;
no unrelated repair is implied. Product acceptance and release closure must
reference real evidence, with no fabricated CI, promotion or clean-tree claim.

## Design-specific validation and acceptance mapping

The proposed design additionally requires tests for invalid total (including
`bool`), invalid callbacks/stop/process, nonzero initial rows, pending process,
and invalid row count, each rejected before acquisition, control, or action side effects; zero total
completes without acquisition. After a successful action, a row-count progress
violation fails instead of spinning. An `after_row` exception is retained as the
primary failure. Boundary/count inspection errors after an existing acquisition
failure must be reported separately and must not replace that primary failure.
Inject `BaseException` interruption and verify all cleanup attempts and re-raise
of the same interruption with the terminal `CountResult` attached as
`count_result`. Also test an exception refusing attribute assignment: diagnostics
are emitted to stderr and the original interruption is re-raised. Resource construction failure must release anything already created.
A completed acquisition with cleanup failure must remain acquisition-completed
while the overall result reports failure. Verify the final design's concrete API
and diagnostics representation before implementation; this plan does not invent
those names.

Traceability: AC01 = plan and design review; AC02 = E01, E11; AC03 = E05–E06;
AC04 = E07; AC05 = E08; AC06 = E09–E10 and interruption checks; AC07 = E02–E04;
AC08 = tester execution and independent review; AC09–AC10 = acceptance and
release checklist. The developer review must confirm that failure injection is
feasible without exposing or mutating production internals as a public contract.

Preflight validates ordinary argument types and running-loop state before invoking
the row-count inspection callback; the initial count check necessarily calls
that callback before owned execution resources are created. Instrument it
separately from acquisition callbacks. Its own side effects are caller-owned.
Use narrowly scoped patching or test doubles for internal failure injection; no
private implementation field is thereby promised as public API. The revised design names `SerialScheduler`, `StopRequest`, `CountResult`, and
`run_count`; executable tests must use those public imports and the documented
Python 3.9-safe interruption diagnostics mechanism, subject to architect review.

Developer review responses: TP-01 resolved by the two control-write cases in
E08; TP-02 resolved by the distinguished preflight inspection and validation
order above; TP-03 resolved by the named public API, same-exception attachment
and stderr fallback, and narrowly scoped failure injection. These are proposed
resolutions pending developer re-review and architect approval of the design.
