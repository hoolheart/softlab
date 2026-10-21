# Sprint board — M01-W01 (process release_4)

Updated: 2026-10-21. Integration branch: dev.

| ID | Scope | Owner / next handoff | Phase | Started | Expected completion | Remaining gates | Merge commit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| M01-W01-001 | Thermal reference vertical slice, guide and evidence | Coordinator final-candidate CI, then sw-tom integration | Principles ready; final-candidate CI pending | 2026-10-18 | Evidence-driven; one working-week capacity forecast | Exact final-candidate CI, dev integration, PO release acceptance | Pending |

Exactly one active task on `codex/m01-w01-thermal-example`, created from dev
`e034825`. Nothing is marked Done or integrated. No new task has started.
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
Next: coordinator verifies committed/pushed handoff and exact-final-SHA CI
before Developer dev integration. Release acceptance remains required
before any main promotion. Every later commit creates a new candidate requiring
its own CI check; earlier green CI does not certify future hashes.

Existing R4-BASE-SCHED-01/META-01/HDF5-01 and OBS-004/005/006/DEFECT-2 remain
tracked unfixed debt, with approved bounded compatibility measures tested for this
example. No new runtime failure or required dependency is open in the tested
scope. No library source/dependency/storage-format/Python-range change, hardware,
UI/Figma or publication is part of this task. Process artifacts are committed and
pushed before role handoff; historical phase detail remains in linked reports
and Git history rather than contradictory current-state statements.
