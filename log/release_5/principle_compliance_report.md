# M01-W02 release-start principle inspection

Delegated inspector: sw-jerry (Architect); coordinator owns formal gate review.
Date: 2026-10-21. Subject PRD 0691bfc, baseline dev815c282 and local setup
artifacts. **Start readiness assessment complete; transport resolved 2026-10-22; formal
setup gate awaits independent document review, Tester validation and CI.** Task/release completion
remain pending. No executed test, CI or acceptance verdict is asserted here.

| Principle | Start evidence / disposition | Assessment |
| --- | --- | --- |
| 1 Architecture | architecture-plan.md labels future local coordination [PLANNED]; actual arch unchanged; five-element ownership preserved | Ready |
| 2 Characterize first | Source/callers read; legacy reset/stop limits and AtomJob row boundary identified; executable characterization required before code | Ready; tests pending |
| 3 TDD gates | tasks.md separates document-only setup validation from serial implementation test/design/review/testing gates | Ready |
| 4 One task | Current branch codex/m01-w02-setup from dev815c282; only setup active, implementation backlog | Ready |
| 5 Ownership | PO PRD, Architect decomposition/feasibility only; Designer review, Tester tests and closures preserved; UI N/A | Ready |
| 6 Evidence transport | PRD0691bfc and architecture records08d1299 committed/pushed to origin/codex/m01-w02-setup; user explicitly confirmed the exact origin trusted on 2026-10-22; this update requires immediate commit/push verification | Transport resolved; update verification pending |
| 7 Environment | No runtime execution here; Tester owns actual versions, dependencies, warnings/skips and local/matrix evidence; missing requirements block affected gates | Pending relevant execution |
| 8 Baseline debt | Three R4-BASE issues and OBS-004/005/006/DEFECT-2 explicitly retained; bounded local measures regress; no automatic closure or broad repair | Ready; regression pending |
| 9 Data/instruments | Synthetic fresh temporary outputs, explicit private resources; physical stop/resume excluded; reset separate after terminal drainage | Ready; failure checks pending |
| 10 Minimality | Local example extension preferred; no dependency/framework/Python/storage change; library prerequisite needs evidence and Architect/PO scope review | Ready |
| 11 Reproducibility | Exact candidate CI before dev, PO acceptance before main; independent setup validation and task/release inspections required; no unrun gate certified | Ready; gates pending |

Method: read-only architecture/source/document assessment and git status/branch
inspection; wrote only architecture/process records. No production/test changes,
execution or technical code review. Documentation review/validation must inspect
saved paths/links/scope and resolve issues under the appropriate owner's authority.
Coordinator must verify actual commits/pushes, independent setup gates and CI
before integration. No source-inspection prerequisite currently forces a library
change; runtime findings may change that assessment. Setup completion cannot
certify W02 release acceptance.

## Transport resolution — 2026-10-22 Asia/Shanghai

The user explicitly confirmed git@github.com:hoolheart/softlab.git trusted in
direct response to the destination question ("I confirm it’s trustable"). The
previous automatic approval rejection is resolved for this authorized push.
A narrow git push origin HEAD succeeded, advancing codex/m01-w02-setup from
0691bfc to08d1299. Clean worktree and correct branch/remote were observed before
that push. All four architecture/process files exist in08d1299. This status
update is committed/pushed immediately; coordinator verifies its resulting SHA
and upstream equality without circular self-hashes. No implementation or tests
were performed. Independent documentation review, Tester document validation,
exact candidate CI and dev integration remain pending; no technical gate is
certified by transport resolution.

## Setup document validation — 2026-10-22 Asia/Shanghai

Tester sw-mike validated committed e5d87dd with **PASS for setup documents**.
All eleven principles have explicit applicability/evidence in
[test/M01-W02-SETUP-validation.md](test/M01-W02-SETUP-validation.md).
Six Markdown files, 18 local links, AC-01–12, whitespace, scope, dev base and
subject/upstream equality passed actual checks. Independent Designer approval
is recorded in reviews/M01-W02-SETUP-review.md. No runtime checks or release
acceptance were performed. This supersedes pending independent document review
and Tester validation in the historical start assessment; coordinator formal
gate, exact candidate CI and dev integration remain pending. Setup is still the
only active task. This update requires immediate docs(log) commit/push and
coordinator verification before handoff; it does not mark setup Done.

## M01-W02-001 independent testing evidence — 2026-10-22

Tester sw-mike validated committed2095c08 (Designer-approved repair27d8a17) with
**PASS for local task testing**. The complete eleven-principle task evidence and
limits are recorded in [test/M01-W02-001-results.md](test/M01-W02-001-results.md).
Actual -W error full257/focused21 tests, compile softlab/examples/tests, import,
root CLI normal/early/mid/late, documented stored inspection and verbatim
partial/reset/two-repeat journey pass. All Tester-owned feature/regression red
failures are closed by green execution. Designer CR-W02-01 closure is separately
recorded in the code-review report; Tester does not close review findings.

Principles1–10 have scoped task evidence in that report, with actual arch.md
release-end update still assigned to Architect and historical debt explicitly
open. Principle11 remains pending the coordinator principle/formal verdict,
exact candidate CI on both configured versions and dev integration. No local
3.9/Ubuntu, UI/hardware or unconfigured lint evidence is claimed. Baseline bounded
scheduler/metadata/HDF5 measures pass regression; OBS/DEFECT debt remains under
its existing disposition. No production changes by Tester. These records require
immediate docs(log) commit/push before handoff. Setup historical pending wording
above is superseded by its integrated closure on the board; W02 task/release
completion and PO acceptance are not inferred from this local testing PASS.
