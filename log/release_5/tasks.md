# M01-W02 serial tasks

Owner: sw-jerry (Architect). Date: 2026-10-21 Asia/Shanghai.
Inputs: [PRD](prd.md), [milestone](../../plan/milestone-01.md),
[planned architecture](architecture-plan.md). Baseline dev `815c282`.

| Order / ID | Deliverable | Branch / dependency | State |
| --- | --- | --- | --- |
| 0 / M01-W02-SETUP | Activated PRD, feasibility, architecture envelope, serial backlog, start principle inspection; independent document review, Tester document validation, candidate CI | codex/m01-w02-setup from dev 815c282 | Done; dev caaa57c; closure-record reconciliation pending |
| 1 / M01-W02-001 | Characterize legacy cancellation/reset; bounded example stop/outcomes/partial retention/reset/repeat; tests, guide and actual architecture | codex/m01-w02-controlled-execution from integrated setup dev; not created yet | Backlog |

The implementation is one coherent vertical task covering AC-01–12: boundary
accounting, outcome reporting and cleanup must agree before a user can trust a
repeat. Roles are serial phases, not separate overlapping tasks. No next task
begins until setup has passed its own documentation gates and integrated to dev.
No fixed calendar deadline; no UI/Figma or hardware gate applies.

## Setup completion gate

Architect PRD/feasibility review and planned decomposition → Designer independent
document review and issue closure → Tester document/path/scope/diff validation
and principle evidence → coordinator formal gate → exact candidate CI (both
configured Python versions) → Developer dev integration. Documentation-only
setup does not require fabricated production tests/design or runtime claims.
Log updates commit/push immediately with docs(log): before each handoff.

## Implementation gate

Tester source/caller characterization, executable assertions and test plan →
Developer implementability review → Designer detailed design → Architect review
→ Tester red tests against approved contract → Developer implementation/self-check
→ Designer independent review/closure (at most three loops) → Tester full checks
and failure closure (at most three loops) → task principles → exact candidate CI
→ Developer dev integration. Update board at every phase. All code review/tests
identify committed subjects; an unrun or failed gate stays pending/failed.

Before code, characterize Process/Scheduler reset/cancel/failure/status/ownership
and existing example success/failure behavior. If a library change is required,
stop implementation for concrete evidence plus Architect and PO scope review;
no general huo repair is authorized by this backlog. Existing tests and W01
numeric fixture/imports/hooks/return behavior constrain the design.

After implementation: Architect updates actual arch.md and release principle
inspection, Tester final integration/environment/warning evidence and guide
walkthrough, PO AC-01–12 acceptance, closure-record candidate CI/dev reconciliation,
then accepted release promotion to main. Do not confuse setup completion, task
integration, package publication or entire M01 acceptance with W02 acceptance.

Required evidence: test plan/developer review, characterization and red results,
detailed design/Architect review, implementation record, Designer review/closure,
Tester results/closure, principle inspections, exact candidate/dev CI and merge
hashes, release acceptance. Use log/release_5/test/, design/ and reviews/ with
M01-W02-001-prefixed files; unresolved references are future required artifacts,
not assertions those files already exist. No dependency/version/storage-format
change, durable resume, retry, W03 manifest or W04 adapter work is included.
