# M02-W01 Architect evidence-decision review

Reviewer: Architect (sw-jerry role) | Date: 2026-10-30 Asia/Shanghai

Verdict: **APPROVED** (final re-review: 2026-10-30 Asia/Shanghai).
AR-ED-01, AR-ED-02 and AR-ED-03 are **CONFIRMED CLOSED**. The minimal additive
executor seam is architecturally sound, and the corrected decision record stays
within the recorded evidence. This grants Architect boundary approval only; it
does not authorize W02 implementation, change the PRD, or close Product Owner
scope agreement, independent review, testing, acceptance, CI, integration or
promotion gates.

## Inputs and review scope

Reviewed the [evidence decision](../design/evidence-decision.md),
[characterization results](../test/characterization-results.md), approved
[characterization design](../design/characterization-design.md), [PRD](../prd.md),
[M02 plan](../../../plan/milestone-02.md), [architecture](../../../arch.md), the
`huo` scheduler/process implementations, the thermal executor, and the recorded
characterization assertions. The review checked evidence attribution, boundary
minimality and ownership, compatibility and lifecycle policies, five-element
placement, and baseline-debt disposition. No new runtime experiment was performed
and no production code or test was changed.

## Review issues requiring correction

### AR-ED-01 — Separate the incompatible meanings of reset

**Owner:** Software Designer (sw-celeste role).

Section 3.5 says that "Reset restores run eligibility only under documented
terminal/drained conditions" and cites T10, while section 3.1 also includes
unchanged `Process.commit/join/reset` compatibility. These are not the same
contract:

- Legacy `Process.reset()` restores process eligibility and, while running,
  signals cooperative abort; T08 explicitly characterizes reset during a running
  body without preemption. It has no terminal/drained/closed precondition.
- Legacy count/scan repeat uses `run_process`/`Process.reset()` and appends rows;
  T03/T10 do not establish a terminal/drained reset guard.
- Thermal `reset_after_run()` has terminal/drained/closed preconditions and
  preserves terminal evidence, but resets the retained coordinator/model; it is
  not a `Scheduler.reset()` API and does not restore executor eligibility.

**Required correction:** distinguish legacy process reset/abort and append-on-
repeat behavior from caller-owned post-run model reset and fresh-run construction.
Do not imply a new scheduler reset API or a new guard on unchanged legacy
`Process.reset()`. State which reset behavior, if any, is a W02 obligation and
which remains W03 lifecycle refinement. Closure requires the corrected boundary
to be internally consistent with T03, T08, T10 and F-06.

### AR-ED-02 — Narrow the second-client lifecycle claim to the evidence

**Owner:** Software Designer (sw-celeste role).

The candidate-1 rationale says both the thermal and monitoring clients demonstrate
"truthful terminal state, owned cleanup." T06/T07 do demonstrate a distinct
acquisition policy reusing the same serial `Scheduler` shape through public
`count`/`run_process`, including truthful action failure and partial consumption.
The monitoring fixture does not implement a terminal result, stop acknowledgment,
owned-work drain, or cleanup diagnostics; its test cleanup only stops and closes
the fixture scheduler. The recorded terminal/cleanup evidence belongs to the
thermal T08/T09 fixtures, not to a second full lifecycle client.

**Required correction:** limit the two-client reuse claim to the behavior actually
demonstrated: serial point/action dispatch, public process compatibility,
acquisition-policy ownership, and action-failure propagation. Keep terminal state,
cooperative stop, and owned cleanup as thermal-derived W02 contract obligations,
without attributing their second-client demonstration to T06/T07. This correction
does not remove the concrete evidence for the minimal executor seam.

### AR-ED-03 — Repair the unsupported-action and re-commit evidence citations

**Owner:** Software Designer (sw-celeste role).

Section 3.2 cites T02 for chained/threaded rejection and F-03 for re-commit
rejection. In the recorded T02 fixture, the chained and threaded commits are made
against a scheduler that was never started, so their `False` results do not isolate
those rejection modes. F-03 in the results report records unknown/missing-point
and after-stop behavior, not re-commit behavior.

The full recorded regression includes
`CountSchedulerContractTests.test_serial_action_success_failure_and_unsupported_actions`
in `tests/test_thermal_example.py`, which does exercise re-commit, chained, and
threaded rejection on a started thermal scheduler.

**Required correction:** cite that precise recorded regression evidence, or label
these rows as source-derived candidate requirements requiring W02 assertions
rather than as outcomes established by F-03/T02. Retain the requirement that W02
prove rejection before action side effects; do not claim that the current T02
fixture independently proves it.

## Architecture and boundary assessment

Subject to the corrections above, candidate 3 remains the appropriate boundary:

- A bounded serial executor implementing the existing public `Scheduler`
  interface is additive and uses dependency inversion rather than replacing the
  global scheduler or creating a framework. Reuse of public `Scheduler`, `Process`,
  `count`, `scan`, and `run_process` contracts is distinct from the refuted private
  reuse of generic `_SchedulerImpl`.
- The proposed home is `huo`, with acquisition/model policy and records remaining
  caller-owned and persistence remaining in `shui`. This preserves the five-element
  responsibilities. Exact module placement and public naming remain W02 detailed
  design decisions.
- No new dependency, public framework, scan extension, storage redesign, or
  supported-Python change is implied. W01 ships no executor or production API.
- The supported serial, unchained, nonthreaded action model and private-loop
  ownership are consistent with T02–T04 and the F-01/F-02 findings. The failure,
  partial-row/final-boundary, primary-versus-cleanup-error, cooperative-stop, and
  owned-resource policies correctly carry forward the thermal evidence. Timeout
  `None` is a candidate contract distinct from the broken generic behavior, not a
  claim that generic `_CtrlPoint` already satisfies it.
- Leaving acquisition-loop extraction depth to W02 detailed design is acceptable
  only if the selected design still satisfies W02's required ordering, terminal,
  stop, failure and cleanup acceptance behavior. This review does not pre-approve
  an implementation shape.

## Baseline debt and limitations

The debt disposition is honest and within scope. R4-BASE-SCHED-01 is confirmed by
F-01 and avoided rather than silently repaired; generic singleton repair remains
outside scope. R4-BASE-META-01 is untouched, R4-BASE-HDF5-01 retains disposable-
copy reads, OBS-004/005/006 and DEFECT-2 retain their existing dispositions, and
historical warnings are recorded as not observed rather than waived. No unrelated
repair is authorized by this review.

The decision correctly retains the recorded limitations: Python 3.9 and CI were
not exercised, forced-process cleanup is unverified, and no hardware, VISA or real
storage-backend qualification is claimed. W02 candidate CI must supply its own
version evidence. Product Owner scope agreement, independent characterization
review/testing, release acceptance, exact-candidate CI, integration, promotion and
closure remain pending.

## Re-review condition

Designer should close AR-ED-01 through AR-ED-03 in the evidence-decision record
with documentation-only corrections and request Architect re-review. No new
production implementation or broadened characterization scope is authorized by
these findings.

## Issue closure records (Designer, 2026-10-30 Asia/Shanghai)

Closure owner: Software Designer (sw-celeste role). All corrections are
documentation-only edits to
[evidence-decision.md](../design/evidence-decision.md); no production code,
test, or PRD change was made. Final acceptance rests with the Architect on
reinspection.

### AR-ED-01 — CLOSED by owner (pending Architect reinspection)

**What changed:** Section 3.5 was rewritten to separate the three behaviors
previously conflated under "reset": (1) legacy `Process.reset()` restores run
eligibility and, while running, signals cooperative abort without preemption
(T08), with no terminal/drained precondition and no record clearing — legacy
count/scan repeat appends rows (F-06, T03, T10), and the seam preserves this
verbatim; (2) thermal `reset_after_run()` is caller-owned post-run model reset
with terminal/drained/closed preconditions, is not a `Scheduler.reset()` API,
and does not restore executor eligibility; (3) repeat is fresh-run
construction with fresh identities (T10). Section 3.5 now states the W02
obligation explicitly: no scheduler reset API is defined; legacy
`Process.reset()`/abort and append-on-repeat semantics are a W02 carry-forward
obligation; post-run model reset stays caller-owned; scheduler-level
reset/lifecycle refinement is W03 scope. Section 3.1's Interface row was
qualified to reference the unchanged legacy reset semantics.

**Where:** `log/release_8/design/evidence-decision.md`, sections 3.1 and 3.5.

**Closure statement:** The corrected boundary is internally consistent with
T03 (reset/repeat-append), T08 (reset-during-running-body signals abort
without preemption; no terminal/drained guard on legacy reset), T10 (fresh
runs, append-not-clear), and F-06 (legacy append semantics). No new scheduler
reset API or new guard on unchanged legacy `Process.reset()` is implied.

### AR-ED-02 — CLOSED by owner (pending Architect reinspection)

**What changed:** The candidate-1 rationale in section 2 now limits the
two-client shared shape to what T06/T07 actually demonstrate: caller-owned
serial executor with private loop, one unchained nonthreaded `Action` per
point, commit/join per boundary through public `count`/`run_process`, and
truthful action-failure propagation with caller-visible partial consumption.
The claims of truthful terminal state, cooperative stop acknowledgment, and
owned cleanup are now attributed solely to the thermal client (T08/T09); the
text explicitly records that the monitoring fixture implements no terminal
result, stop acknowledgment, owned-work drain, or cleanup diagnostics, and
that its test cleanup only stops and closes the fixture scheduler. The
section-1 T06 evidence row was likewise narrowed: "execution/lifecycle shape"
became the demonstrated serial execution shape (executor reuse through public
`count`/`run_process`), with an explicit note that T06/T07 do not demonstrate
terminal-state or owned-cleanup behavior.

**Where:** `log/release_8/design/evidence-decision.md`, section 1 (T06 row)
and section 2 (candidate-1 rationale).

**Closure statement:** Terminal state, cooperative stop, and owned cleanup
remain thermal-derived W02 contract obligations (sections 3.3/3.4/3.5
unchanged in substance). The concrete evidence for the minimal executor seam —
executor reuse and acquisition-policy separation across two real clients — is
retained; the core verdict is unchanged.

### AR-ED-03 — CLOSED by owner (pending Architect reinspection)

**What changed:** Section 3.2 now cites the recorded regression
`CountSchedulerContractTests.test_serial_action_success_failure_and_unsupported_actions`
(`tests/test_thermal_example.py`, green within the recorded 306-test full
discovery at baseline `35e6e9b`) as the isolating evidence for chained-action,
threaded-action, and re-commit rejection, each asserted on a **started**
thermal scheduler. The F-03 citation for re-commit was removed (F-03 records
unknown/missing-point and after-stop behavior only). The T02 citation on the
chained/threaded rows was removed from those rows and T02 is retained only on
the not-started row, with an explicit isolation caveat: the T02 fixture makes
its chained/threaded commits against a never-started scheduler, so those
`False` results do not isolate the rejection modes. The caveat also records
that W02 must prove rejection before action side effects in its own suite and
that no side-effect-ordering claim is made for the current T02 fixture.

**Where:** `log/release_8/design/evidence-decision.md`, section 3.2 (table and
new isolation-caveat paragraph).

**Closure statement:** Each rejection mode now references evidence that
actually isolates it, or (for side-effect ordering) states the residual W02
obligation instead of citing non-isolating references. The unknown/missing-
point and after-stop rows retain their valid F-03/T02/T10 citations.

## Architect final re-review (2026-10-30 Asia/Shanghai)

Reinspection covered the corrected evidence decision, the Designer closure
records above, the recorded characterization results, and the cited regression
assertions. This was a documentation and recorded-evidence review; no new
runtime test was executed and no production code, test or PRD was changed.

### AR-ED-01 — CONFIRMED CLOSED

Sections 3.1 and 3.5 now distinguish unchanged legacy `Process.reset()` run
eligibility, cooperative abort and append-on-repeat behavior from the thermal
caller-owned `reset_after_run()` policy and fresh-run construction. The legacy
contract has no new terminal/drained guard and introduces no scheduler reset
API. These statements trace to T03, T08, T10 and F-06 without conflating their
scopes. W02 carries forward the legacy behavior and caller-owned model reset;
scheduler-level reset/lifecycle refinement remains W03 scope.

### AR-ED-02 — CONFIRMED CLOSED

The section 1 T06 row and section 2 candidate-1 rationale now limit the
second-client demonstration to serial point/action dispatch through public
`count`/`run_process`, acquisition-policy separation, action-failure
propagation and caller-visible partial consumption. Terminal state,
cooperative stop acknowledgment and owned cleanup are attributed only to the
thermal T08/T09 evidence. The monitoring fixture is no longer presented as a
second full lifecycle client, while the demonstrated basis for the minimal
executor seam remains intact.

### AR-ED-03 — CONFIRMED CLOSED

Section 3.2 now cites
`CountSchedulerContractTests.test_serial_action_success_failure_and_unsupported_actions`
for chained, threaded and re-commit rejection on a started thermal scheduler.
That regression is within the recorded 306-test full discovery that passed at
baseline `35e6e9b`; its assertions match the cited rejection outcomes. F-03 is
no longer cited for re-commit, T02 is retained only as not-started evidence,
and the isolation caveat explicitly rejects interpreting T02's never-started
chained/threaded results as mode-isolating proof. Rejection before action side
effects remains a required W02 assertion rather than an overclaimed existing
result.

### Final verdict

**APPROVED — 2026-10-30 Asia/Shanghai.** All three Architect review issues are
closed and the evidence decision receives Architect boundary approval. Product
Owner scope agreement and all independent review, testing, acceptance,
exact-candidate CI, integration, promotion and release-closure gates remain
separate and pending. This verdict does not authorize W02 implementation.
