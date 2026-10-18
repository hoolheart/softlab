# Sprint board — M01-W01 (process release_4)

Updated: 2026-10-18. Integration branch: dev.

| ID | Scope | Owner | Phase | Started | Expected completion | Blocker | Merge commit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| M01-W01-001 | Thermal reference vertical slice, guide and evidence | sw-tom next (Developer review) | Design | 2026-10-18 | Evidence-driven; one working-week forecast | Developer test-plan review and design/architect review required before implementation | Pending |

Exactly one active task on `codex/m01-w01-thermal-example` from dev `e034825`.
Branch setup is complete; PO activation `c776fe8` is committed/pushed.
[Architecture plan](architecture-plan.md), [task sequence](tasks.md), and
[start principle inspection](principle_compliance_report.md) accompany this setup.
Tester baseline/independent oracle and assertion test plan are complete: 210 baseline
tests pass locally on Python 3.13.15, including a named warning-as-error rerun.
[Test plan](test/m01-w01-001-test-plan.md) at `3590c0a` awaits Developer approval;
[baseline evidence](test/baseline.md) records cache diagnostics and their temporary
environment resolution. No future example test or CI pass is claimed.
Next phase is Developer implementability review of the test plan.
Developer review, detailed design/architect review, Tester executable red tests,
implementation, independent
code review, testing, completion principles, candidate CI and dev integration are
pending. Done requires gates and merge evidence; no runtime pass is claimed.
UI/Figma, hardware and publication are N/A. Release acceptance and any main
promotion remain later gates. Completed process updates must be committed/pushed
before each role handoff; commit identifiers are recorded by the next phase to
avoid circular self-hashes.
