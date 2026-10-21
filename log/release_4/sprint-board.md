# Sprint board — M01-W01 (process release_4)

Updated: 2026-10-21. Integration branch: dev.

| ID | Scope | Owner / next handoff | Phase | Started | Expected completion | Remaining gates | Merge commit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| M01-W01-001 | Thermal reference vertical slice, guide and evidence | Coordinator / final closure | Done: integrated into dev | 2026-10-18 | Completed task integration 2026-10-21 | PO PASS; final principles PASS; closure CI/docs integration pending; no main promotion | c89ffe6689484f1ea1eaf7088e8e2592fa21908d |

Exactly one active task on `codex/m01-w01-thermal-example`, created from dev
`e034825`. The task is Done after the dev integration recorded below. No new task has started.
User resumed execution on 2026-10-21; prior evidence dates remain historical.

## Completed gates and evidence

- Requirements activated at `c776fe8`; see [PRD](prd.md),
  [architecture plan](architecture-plan.md), [tasks](tasks.md) and
  [start principle inspection](principle_compliance_report.md).
- Tester baseline and assertion plan approved by Developer; see
  [baseline](test/baseline.md), [plan](test/m01-w01-001-test-plan.md) and
  [review](reviews/m01-w01-001-test-plan-review.md).
- [Detailed design](design/m01-w01-001-detailed-design.md) revised at `5fdb984`;
  Architect closed DR-01 and approved artifact-preserving reload, metadata glue
  and bounded scheduler in [design review](reviews/m01-w01-001-design-review.md).
- Executable red tests and Tester contract corrections are recorded in
  [red evidence](test/m01-w01-001-red-tests.md). Implementation `6f8b26d`, guide
  `864891a` and cleanup fix `5c5f6c4` are committed; see
  [implementation evidence](implementation.md).
- Independent code review **APPROVED** at `c722b9d`, CR-01 closed by Reviewer;
  see [code review](reviews/m01-w01-001-code-review.md).
- Independent Testing **PASS** on `a7953e3`: 229 tests under -W error, compile/
  import, two documented CLI runs, HDF5/PNG/failure/ownership assertions and guide
  walkthrough. [Results](test/m01-w01-001-test-results.md) committed at `23ae605`;
  AC01–12/NFR01–05 traceability and baseline-debt disposition are recorded there.
  Candidate a7953e3 CI passes both Python 3.9/3.13 jobs. On 2026-10-21 Tester
  also verified exact evidence candidate `23ae6056ee0d37f4afeea3f35bdc99b80b940aba`
  [CI run 39142118165](https://github.com/hoolheart/softlab/actions/runs/39142118165):
  completed/success for both compatibility (3.9) and compatibility (3.13).
  No source changed since runtime testing; full suite was not repeated.

## Current handoff and limits

Actual architecture updated at `af1dc74`; architecture plan reconciled.
Architect task-completion/release-preacceptance principle inspection passes
readiness for all eleven principles; see [inspection](principle_compliance_report.md).
Current release acceptance is **PASS** by sw-camille on 2026-10-21;
see [acceptance](acceptance.md) and [product summary](sprint_summary.md).
Task implementation is already integrated as recorded below. Remaining work is
exact closure-document candidate CI and
follow-up dev integration before any main promotion. Earlier phase verdicts are
historical evidence; this board does not certify future commit CI.

Existing R4-BASE-SCHED-01/META-01/HDF5-01 and OBS-004/005/006/DEFECT-2 remain
tracked unfixed debt, with approved bounded compatibility measures tested for this
example. No new runtime failure or required dependency is open in the tested
scope. No library source/dependency/storage-format/Python-range change, hardware,
UI/Figma or publication is part of this task. Process artifacts are committed and
pushed before role handoff; historical phase detail remains in linked reports
and Git history rather than contradictory current-state statements.


## Actual dev integration — 2026-10-21

Approved task candidate `6bdff20e99f79cb24e150393225c72a34dd3860c` was merged
with --no-ff into freshly fetched dev `e034825b637910f353a4a3f5929d1993fe617457`,
producing/pushing `c89ffe6689484f1ea1eaf7088e8e2592fa21908d`.
[Integration evidence](integration.md) records exact run/commit checks and the
candidate per-job API inconsistency. Dev merge-head CI explicitly confirms both
Python 3.9/3.13 success. Review/testing/principle task gates are complete;
no task issue remains open. Earlier phase text is historical.

Task integration is Done; release acceptance is PASS by sw-camille on 2026-10-21. This
reconciliation record is committed on the preserved task branch; it is not yet
in dev. PO reviewed integrated dev scope and recorded acceptance on this branch;
final closure evidence/CI must be integrated in one documented follow-up.
No main promotion, package publication or next task is authorized here.


Final Architect release-state principle inspection on 2026-10-21: **PASS** for
all eleven principles, including actual dev integration and PO acceptance.
See [completion report](completion_report.md). Runtime/code remains unchanged
from the tested candidate. Next owner: Developer/integrator after coordinator
formal handoff and exact closure-candidate CI, for follow-up dev reconciliation;
any main promotion follows the required gates. Current main is unchanged; no
future merge/run hash or package publication is claimed here.
