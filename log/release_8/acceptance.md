# M02-W01 product acceptance

Reviewer: sw-camille, Product Owner | Date: 2026-10-30 Asia/Shanghai  
Release: 8 (M02-W01) | Sprint: W01 | Task: M02-W01-001

## Verdict

**PASS — the bounded W01 product outcome, characterization evidence, and
selected minimal additive seam are accepted.**

This accepts the W01 requirements/evidence and candidate-3 boundary. It does not
mark the release CLOSED, claim exact-candidate CI, authorize integration or
promotion, or start W02. The remaining AC07 task/release checks and AC08 closure
gates follow this acceptance as scheduled.

## Evidence reviewed

- [PRD and acceptance conditions](prd.md).
- [Characterization results](test/characterization-results.md): 25
  characterization tests and the full 306-test discovery run passed against
  unchanged production baseline `35e6e9b`; compile/import checks passed;
  findings F-01 through F-07, commands, environment, warnings, and limitations
  are recorded.
- [Evidence decision](design/evidence-decision.md): candidate 3, the minimal
  additive bounded serial executor seam, selected from runtime evidence.
- [Architect evidence-decision review](reviews/evidence-decision-review.md):
  APPROVED, with AR-ED-01 through AR-ED-03 confirmed closed.
- [Product Owner scope agreement](reviews/scope-agreement.md) and its final open
  condition, now closed by the independent
  [caller-inventory completeness check](reviews/caller-inventory-check.md).
- [Sprint board](sprint-board.md), including the user-authorized reduction of
  the independent full characterization-suite review round.

This is a Product Owner evidence and requirements review. No new runtime test,
code review, production change, or test change is claimed by this acceptance.

## Final acceptance-criterion status

| ID | Final status at W01 acceptance | Product Owner assessment |
| --- | --- | --- |
| W01-AC01 | **MET** | The independent repository-search-backed check is **COMPLETE**, with file/usage inventory across production, tests, examples, and notebooks. All actual callers fit the ownership categories in the evidence decision; scheduler, loop, action, callback, record, cancellation/stop, and cleanup ownership are mapped without confusing observed defects with proposed guarantees. The `grid_scan` observation is a documented non-gap covered by legacy compatibility, with no boundary impact. |
| W01-AC02 | **MET for the characterization scope, with recorded environment limits** | The assertion-based suite ran against unchanged production behavior: 25 characterization tests and 306 full-discovery tests passed, with compile/import checks, commands, Python 3.13.15 environment, warnings, and results recorded. Count/scan compatibility, ordering, completion, invalid operations, stop/cancellation, failures, cleanup, and repeated use are covered. Python 3.9/CI and forced-process cleanup are not claimed; baseline defects remain findings rather than desirable contracts. |
| W01-AC03 | **MET** | T05–T07 compare both thermal timing policies with the distinct paired signal/reference monitor. Shared serial orchestration is separated from caller-owned acquisition and derived-value policy. F-05 establishes the direct adapter's extra pure observation, while failure evidence exposes consumptive-source effects and multi-read row atomicity. |
| W01-AC04 | **MET** | The decision compares retained local execution, reuse/extension of existing helpers, and a minimal additive seam. Runtime evidence refutes private reuse of generic `_SchedulerImpl` and demonstrates useful reuse of public contracts across two operational contexts. Candidate 3 is justified; the milestone scope-revision fallback is not triggered. |
| W01-AC05 | **MET as the W02 candidate boundary; no executor is claimed in W01** | The accepted record defines supported/rejected modes, ownership, ordering, compatibility, failure truthfulness, cleanup, stop/reset/repeat behavior, and explicit limitations. Legacy imports and count/scan/run_process behavior remain unchanged. All inseparable failure and cleanup safety remains mandatory for W02; scheduler-level reset and further lifecycle refinement remain W03 scope. |
| W01-AC06 | **MET for the selected path** | Every named baseline item retains its tracked disposition. R4-BASE-SCHED-01 is confirmed and avoided, not silently repaired; metadata and HDF5 repairs remain separately scoped; OBS-004/005/006 and DEFECT-2 are untouched. Historical warnings are recorded as not observed in the named environment, not waived. No selected-path baseline blocker requires unauthorized W01 repair work. |
| W01-AC07 | **MET for the product-acceptance gate; final task/release checks remain pending** | Tester-owned runtime evidence exists with no open test failure; Architect design/boundary approvals and reviewer-owned closures are recorded; Product Owner scope agreement and this acceptance are complete. The independent full characterization-suite review was **not performed and is not marked passed**; its user-authorized cut is recorded below. Task-completion principle/document checks and release-completion inspection remain coordinator-owned post-acceptance gates and are not claimed here. |
| W01-AC08 | **PENDING — required post-acceptance closure work** | Candidate freeze, exact-candidate CI before `dev`, promoted-state verification, final hashes/CI links, scope hygiene, clean worktree, one authoritative closure summary, and CLOSED status remain required. This PASS supplies the acceptance prerequisite for the later `main` promotion sequence; none of the unrun CI, integration, promotion, or verification gates is represented as passed. |

## Cut-check disposition

The user authorized cutting **only the independent full review round of the
characterization suite** for this process-only W01 release. The recorded basis
is zero production-code changes, Developer review of the suite at test-plan
level, and reusable green evidence from the 25-test suite and full 306-test
regression. This disposition is accepted for W01 and recorded on the sprint
board.

This is a scope reduction, not a retroactive independent-review PASS and not a
waiver of any failed check. Tester-owned execution/results, Developer plan
review, Architect design and evidence-decision reviews, the independent AC01
inventory completeness check, Product Owner acceptance, and all remaining
principle, CI, integration, promotion, and closure gates retain their required
status. No performed review has an open blocking finding, and no test failure is
silently closed. The reduction sets no precedent for W02–W04.

## Remaining debt and follow-on scope conditions

### W02 — required before its own acceptance

1. Select the executor's `huo` module placement/public name and acquisition-loop
   extraction depth through detailed design and Architect review. The
   `examples/` prototype must not become a production import.
2. Prove all specified rejection and compatibility behavior, including chained,
   threaded, re-commit, not-started/after-stop, borrowed/global, and other
   unsupported modes before action side effects where required. Preserve public
   imports, count/scan/run_process behavior, legacy reset/abort, and
   append-on-repeat. No scan extension or scheduler reset API is implied.
3. Prove private-loop and borrowed/global-resource isolation; truthful primary
   causes, completed rows, and final boundary; independent primary/cleanup
   diagnostics; cleanup attempts on every supported terminal path; and
   cooperative stop request/acknowledgment without preemption. These safety
   behaviors cannot be deferred to W03.
4. Keep acquisition consequences visible: a failed paired read may consume
   stateful input without a complete derived row; no rollback is promised;
   legacy repeats append; fresh-run construction supplies fresh identities; a
   stop request is neither acknowledgment nor physical termination.
5. Obtain W02's own applicable Python 3.9 and candidate-CI evidence. Do not
   depend on the refuted private generic `_SchedulerImpl` path; implement and
   test the candidate three-valued timeout contract rather than treating F-02
   as already fixed.

### W03 — retained refinement scope

- Scheduler-level reset and further lifecycle refinement remain W03 scope.
- Any extended fault coverage or retry/lifecycle decision must not be used to
  defer W02's inseparable cleanup, compatibility, or truthful-failure duties.

### W04 — retained integration scope

- Migrate the thermal example to the accepted seam and prove the existing
  reference workflow and both acquisition policies without broadening the W01
  boundary. W01 acceptance does not perform or authorize that migration.

### Continuing baseline debt and evidence limits

R4-BASE-SCHED-01 generic scheduler repair remains outside M02; F-01/F-02 remain
tracked baseline debt that the selected path must avoid. R4-BASE-META-01 and
R4-BASE-HDF5-01 remain separately scoped. OBS-004/005/006 and DEFECT-2 retain
their existing dispositions. No hardware, VISA, real-storage,
forced-process-cleanup, metadata-repair, storage-repair, or generic-scheduler-
repair claim follows from W01. Python 3.9 and CI evidence remain unclaimed until
actual applicable candidate runs succeed.

## Acceptance conclusion

All W01 evidence and decision conditions required before product acceptance are
met, including the formerly open AC01 inventory condition. There is no unresolved
user-facing objection or scoped acceptance blocker. The pending AC07/AC08 items
are explicitly post-acceptance process and closure gates; they keep release 8
OPEN and W02 unauthorized until completed.

**Overall verdict: PASS.**

Signed: sw-camille, Product Owner
