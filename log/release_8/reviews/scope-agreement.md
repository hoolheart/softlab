# M02-W01 Product Owner scope agreement

Reviewer: Product Owner (sw-camille) | Date: 2026-10-30 Asia/Shanghai

Verdict: **AGREED** with the supported scope of candidate 3, the minimal
additive bounded serial executor seam defined in the
[evidence decision](../design/evidence-decision.md). This is scope agreement for
the next release's boundary, not W01 release acceptance or closure. It does not
authorize W02 implementation; W02 may start only after W01 completes its
remaining independent checks, product acceptance, exact-candidate CI,
integration/promotion and closure gates.

## Inputs and review perspective

Reviewed the [PRD](../prd.md), [characterization results](../test/characterization-results.md),
[evidence decision](../design/evidence-decision.md), and
[Architect boundary review](evidence-decision-review.md), with the approved
[characterization design](../design/characterization-design.md),
[test plan](../test/characterization-plan.md), and
[requirements architecture review](../architecture-review.md) as supporting
context. This review judges user value, requirement coverage, supported scope,
and recorded limitations. It is not a code review or a new runtime test.

## Product and user-value judgment

The selected seam addresses the demonstrated user need without creating a broad
execution framework. The thermal and signal-monitoring clients share bounded
serial point execution while retaining materially different acquisition
policies. Reusing that execution shape can remove duplicated lifecycle and
failure handling without hiding caller-specific reads, derived values, timing,
or record policy.

The decision makes the right ownership distinction for users:

- Reuse the existing public `Scheduler`, `count`, `scan`, `run_process`, and
  `Process` contracts, not the refuted private ownership path of generic
  `_SchedulerImpl`.
- Keep the executor serial and bounded, with one unchained nonthreaded action
  per acquired point and a privately owned loop.
- Keep acquisition and derived-value policy with the caller. In particular, the
  direct thermal adapter's extra pure observation must not become a hidden
  executor read or an assumption that stateful inputs can be observed again.
- Preserve truthful partial results, final boundary, primary cause, and cleanup
  diagnostics. Do not fabricate a row, roll back consumed inputs, or hide a
  primary failure behind cleanup failure.
- Preserve cooperative stop, legacy reset/abort, append-on-repeat, and fresh-run
  behavior without implying preemption, record clearing, or a new scheduler
  reset API.

The second-client claim is appropriately limited to serial execution,
acquisition-policy separation, action-failure propagation, and visible partial
consumption. Terminal state, stop acknowledgment, and owned cleanup remain
thermal-derived obligations that W02 must prove for its own implementation.

## Acceptance-criterion status at scope agreement

| ID | Status | Product Owner assessment and remaining limitation |
| --- | --- | --- |
| W01-AC01 | **PARTIALLY MET** | The source assessment, T04 runtime evidence, and decision sections 3.1–3.5 provide decision-level caller categories and ownership for scheduler, loop, action, callback, record, cancellation/stop, and cleanup, while separating observed defects from proposed guarantees. However, the reviewed artifacts do not consolidate the planned repository-search-backed, file/function caller inventory or show its completeness check. Complete or consolidate that T01 inventory and have its completeness independently checked before W01 acceptance. This evidence gap does not justify changing the additive boundary, which leaves existing callers unchanged. |
| W01-AC02 | **MET for the characterization scope, with recorded environment limits** | The assertion-based suite ran against unchanged production baseline `35e6e9b`: 25 characterization tests and 306-test full discovery passed, with commands, Python 3.13.15 environment, warnings, results, and ownership findings recorded. Count/scan compatibility, ordering, normal completion, invalid operations, stop/cancellation, failures, cleanup, and repeated use are covered. Python 3.9/CI and forced-process cleanup are not claimed; baseline defects are findings, not desirable contracts. |
| W01-AC03 | **MET** | T05–T07 compare both thermal timing policies with the distinct paired signal/reference monitor. The evidence separates shared serial orchestration from caller-owned acquisition/derived-value policy, and F-05 detects the direct adapter's extra pure observation without confusing it with a measurement read. Failure traces also expose consumptive-source effects and multi-read row atomicity. |
| W01-AC04 | **MET** | The decision compares retained local execution, reuse/extension of existing helpers, and a minimal additive seam. Runtime evidence refutes private reuse of the generic concrete scheduler and demonstrates useful reuse of public contracts across two operational contexts. Candidate 3 is therefore justified; the fallback milestone scope revision is not triggered. |
| W01-AC05 | **MET as a candidate boundary; no executor is claimed in W01** | Sections 3.1–3.5 define supported/rejected modes, ownership, rejection, failure, cleanup, stop/reset/repeat, and compatibility obligations. Legacy imports and count/scan/run_process behavior remain unchanged; command/advance/read/row order, partial-row/final-boundary truthfulness, and primary/cleanup error independence are explicit. Inseparable failure and cleanup safety belongs to W02; scheduler-level reset/lifecycle refinement remains W03 scope. |
| W01-AC06 | **MET for the selected path** | Every named baseline item retains its tracked disposition. R4-BASE-SCHED-01 is confirmed and avoided rather than repaired; metadata and HDF5 repairs remain separately scoped; OBS-004/005/006 and DEFECT-2 are untouched. Historical warnings are recorded as not observed in this environment, not waived. No baseline item is silently converted into a PASS or an unauthorized repair. |
| W01-AC07 | **PARTIALLY MET — remaining phase gates** | Tester-owned runtime evidence and Architect boundary approval exist, and this record supplies Product Owner scope agreement. Independent characterization review/testing, reviewer/tester-owned issue closure, final product acceptance, and task-completion principle/document checks remain pending. Preparation approvals are not release acceptance. |
| W01-AC08 | **NOT YET MET — expected closure work remains** | Candidate freeze, exact-candidate CI before `dev`, product acceptance before `main`, promoted-state verification, final hashes/CI links, scope hygiene, clean worktree, and CLOSED status remain future gates. No integration, promotion, or release closure is inferred from this agreement. |

## Baseline-debt and warning dispositions

| Item | Confirmed disposition |
| --- | --- |
| R4-BASE-SCHED-01 | F-01 confirms the private concrete scheduler completion-routing defect. Candidate 3 owns dispatch and avoids that path. Generic singleton repair remains outside M02 scope; W02 must not depend on it. |
| R4-BASE-META-01 | Characterization did not touch metadata construction/hydration. The existing metadata behavior and separately scoped repair remain unchanged. |
| R4-BASE-HDF5-01 | F-04/F-07 separate persistence replay and copied-record inspection from acquisition evidence. Disposable-copy reads and temporary synthetic storage remain required; no storage repair is authorized. |
| OBS-004 / OBS-005 / OBS-006 | The selected path does not alter the affected initialization, fallback/strict-evaluation, or lookup/cycle behavior. Existing dispositions and limitations stand. |
| DEFECT-2 | The deliberate programmer-error warning is not silenced or reclassified. |
| Historical pandas/Actions warnings | Not observed under the recorded Python 3.13.15 `-W all` runs. This is an environment-specific observation, not a waiver or a claim for Python 3.9/other environments. |

No selected-path baseline blocker requires expanding W01 into repair work.
F-02 likewise remains observed generic `_CtrlPoint` debt: the candidate's
three-valued timeout contract is a W02 obligation, not a claim that the generic
implementation already works.

## Agreed W02 boundary and scope conditions

The following conditions preserve the agreed user value and prevent the boundary
from becoming an overpromise:

1. **Close the AC01 evidence gap before W01 acceptance.** Consolidate the actual
   repository caller inventory at file/function granularity for Process
   commit/join/reset, Scheduler, count, scan, and the thermal executor; map the
   required ownership categories and distinguish source facts, runtime
   observations, known defects, and proposed guarantees. Independent review
   must check completeness against repository search. This is evidence
   completion, not authorization for new production behavior or runtime scope.
2. **Keep W02 minimal and additively placed.** W02 detailed design and Architect
   review must select the `huo` module placement/public name and decide how much
   of the acquisition loop to extract. The examples prototype must not become a
   production import. Any loop extraction must preserve demonstrated ordering,
   terminal, stop, failure, and cleanup behavior.
3. **Prove rejection and compatibility, including side-effect ordering.** W02
   tests must show that chained, threaded, re-commit, not-started/after-stop,
   borrowed/global, and other unsupported modes are rejected as specified before
   action side effects where that requirement applies. Existing public imports,
   count/scan/run_process behavior, legacy reset/abort, and append-on-repeat must
   remain unchanged. No scan extension or scheduler reset API is implied.
4. **Carry all inseparable safety behavior into W02.** Prove private-loop and
   borrowed/global-resource isolation; truthful failure causes, completed rows,
   and final boundary; independent primary/cleanup diagnostics; cleanup attempts
   on every supported terminal path; and cooperative stop request/acknowledgment
   without preemption. The monitor fixture is not evidence that a second client
   already demonstrates these lifecycle behaviors.
5. **Keep user-visible acquisition consequences explicit.** A failed paired read
   may consume stateful input without producing a complete derived row; no
   rollback is promised. Legacy repeats append rather than clear records, while
   fresh-run construction supplies fresh identities. A stop request is not an
   acknowledgment or a physical-termination guarantee.
6. **Retain evidence limits and exclusions.** W02 must obtain its own applicable
   Python 3.9 and candidate-CI evidence. No hardware, VISA, real-storage,
   forced-process-cleanup, generic-scheduler-repair, metadata-repair, or storage-
   repair claim follows from W01. Thermal-example migration remains W04 scope;
   scheduler-level reset and further lifecycle refinement remain W03 scope.

There is no unresolved user-facing objection to the selected boundary. The open
items above are conditions for evidence completion and faithful W02 design and
verification, not permission to broaden this release.

## Conclusion

**AGREED:** candidate 3 supplies a safe, minimal, evidence-supported W02 seam
and satisfies the product intent of W01 without shipping an executor in W01.
AC01 evidence completion and all AC07/AC08 gates remain explicitly open; this
agreement neither accepts nor closes the release.

Signed: sw-camille, Product Owner
