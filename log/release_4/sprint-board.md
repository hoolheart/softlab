# Sprint board — M01-W01 (process release_4)

Updated: 2026-10-18. Integration branch: dev.

| ID | Scope | Owner | Phase | Started | Expected completion | Blocker | Merge commit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| M01-W01-001 | Thermal reference vertical slice, guide and evidence | sw-jerry next (Architect reinspection) | Design | 2026-10-18 | Evidence-driven; one working-week forecast | DR-01 revised in design; Architect reinspection/closure required | Pending |

Exactly one active task on `codex/m01-w01-thermal-example` from dev `e034825`.
Branch setup is complete; PO activation `c776fe8` is committed/pushed.
[Architecture plan](architecture-plan.md), [task sequence](tasks.md), and
[start principle inspection](principle_compliance_report.md) accompany this setup.
Tester baseline/independent oracle and assertion test plan are complete: 210 baseline
tests pass locally on Python 3.13.15, including a named warning-as-error rerun.
[Test plan](test/m01-w01-001-test-plan.md) at `3590c0a` is Developer-approved;
[baseline evidence](test/baseline.md) records cache diagnostics and their temporary
environment resolution. No future example test or CI pass is claimed.
Developer implementability review is APPROVED; see
[review](reviews/m01-w01-001-test-plan-review.md). No executable tests or production
code were changed. Detailed design is complete at `fce0957`; see
[design](design/m01-w01-001-detailed-design.md). Next phase is Architect review,
including bounded public Scheduler adapter and disk-derived metadata hydration
decisions. Design is proposed, not approved; no executable code/tests were created.
Detailed design/architect review, Tester executable red tests,
implementation, independent
code review, testing, completion principles, candidate CI and dev integration are
pending. Done requires gates and merge evidence; no runtime pass is claimed.
UI/Figma, hardware and publication are N/A. Release acceptance and any main
promotion remain later gates. Completed process updates must be committed/pushed
before each role handoff; commit identifiers are recorded by the next phase to
avoid circular self-hashes.

Architect review: **CHANGES_REQUESTED** at the current design gate; see
[review](reviews/m01-w01-001-design-review.md). Public bounded Scheduler adapter
and disk-derived metadata hydration approved in principle; DR-01 remains OPEN.
Designer revision is committed at `5fdb984`: verified reload uses a fresh owned
temporary HDF5 copy, leaving delivered artifact intact; successive independent
reload/identity/integrity and failure-cleanup assertions are specified.
Architect reinspection is next; Designer has not closed DR-01. No executable red tests
or implementation may start yet. New baseline defects R4-BASE-SCHED-01,
R4-BASE-META-01 and R4-BASE-HDF5-01 are tracked in that review, unfixed and
excluded from incidental production repair.


Architect reinspection of revised design `5fdb984` (handoff `2b6fc91`) is
**APPROVED**; DR-01 is CLOSED by Architect in the design review. Earlier
CHANGES_REQUESTED text above records historical sequence only. Artifact-preserving
copy reload, disk metadata hydration and the bounded public Scheduler adapter are
approved design decisions. Tester executable red tests are next; implementation
and all downstream gates remain pending. No baseline defect is claimed fixed.
