# M01-W02 closure final integration and document validation

Tester: sw-mike. Date: 2026-10-22 Asia/Shanghai.
Subject: committed e1c0c6335c589ee0cfaec64d94806d7790ff2217 on
codex/m01-w02-release-closure, based on integrated dev10b2d7d;
actual architecture3ad83ed, release inspection5bc1514, Designer closure review
e1c0c63 (APPROVED for subject5bc1514).
Verdict: **PASS for final local release integration/document validation**.
No new Tester failures found. PO release acceptance, coordinator formal gate,
exact closure candidate CI/dev reconciliation and main promotion remain pending.

## Actual executed checks

Repository root, existing .venv/bin/python, macOS Python3.13.15 conda-forge
(Clang19.1.7). Environment: MPLBACKEND=Agg,
MPLCONFIGDIR=/tmp/softlab-w02-mpl, XDG_CACHE_HOME=/tmp/softlab-w02-cache.
Dependency configuration/runtime bytes unchanged from independent results on
2095c08; version inventory is recorded in M01-W02-001-results.md.

| Command / check | Result |
| --- | --- |
| `.venv/bin/python -W error -m unittest discover -s tests -p 'test_*.py'` | Exit0; **257 tests,15.586s,OK**, no skips. Includes feature21 methods, six nested-cleanup subcases, W01 failure matrix, legacy characterization and reset/failure integration. |
| `.venv/bin/python -W error -m compileall -q softlab examples tests` | Exit0, no diagnostics. |
| `.venv/bin/python -W error -c 'import softlab,sys; print(sys.version); print(softlab.__version__)'` | Exit0; Python3.13.15, softlab0.3.0, no warning diagnostics. |
| Python pathlib/re inspection of arch.md and log/release_5 Markdown | 22 files/94 explicit local links resolve; final-newline/no-trailing-whitespace checks pass before this report. |
| `git diff --check 10b2d7d HEAD` | Exit0, no whitespace diagnostics. |
| `git diff --name-only 2095c08 HEAD -- softlab examples tests docs pyproject.toml` and same from10b2d7d | Both empty: actual runtime/tests/guide/config unchanged from prior tested subject and integrated dev. |
| Python scope assertion over `git diff --name-only 10b2d7d HEAD` | Seven files, all arch.md or log/release_5 records. |
| `git rev-parse HEAD '@{upstream}'` / `git status --short` | HEAD/upstream both e1c0c6335c589ee0cfaec64d94806d7790ff2217; clean before final records. |

No Python warnings, compilation errors, skipped tests or new unexpected logging
warnings reported in named checks. Expected legacy failure warnings are captured
by assertions; HDF5 connection prints are informational. This is named-check
evidence, not a claim about unconfigured linters/static analysis or all platforms.
No production or test source changed during this closure validation.

## Document and guide applicability

Read actual W02 architecture section, planned-envelope disposition, release
architecture record, all eleven principle readiness entries, tasks/board,
integration evidence and Designer closure review. Current W02 architecture
explicitly supersedes historical W01 future-W02 statements and matches the local
execution/result/cleanup/reset boundaries. W03/W04 remain future. Existing
library/dependency/Python/storage limits and historical debt remain explicit.

The original task-board pending paragraphs and tasks closure Design cell are
historical handoff states: completed architecture3ad83ed, Designer approval and
this Tester PASS supersede them through the latest board/inspection entries.
Setup and implementation remain Done; closure alone is active. No unrun closure
CI or PO acceptance is marked passed. Implementation candidate/dev CI references
are distinguished from this closure candidate's still-pending CI.

Independent guide CLI normal/early/mid/late, stored inspection and verbatim
partial/reset/two-repeat journey passed on2095c08 as recorded in
M01-W02-001-results.md. Actual git comparisons above confirm guide and executable
bytes identical; no repeated unchanged guide run was needed for this doc-only
closure. The final full regression was actually rerun, rather than inferred.
No new production tests/design were fabricated for closure documents.

## Eleven-principle final Tester readiness

| Principle | Current evidence and applicability |
| --- | --- |
| 1 | Actual architecture3ad83ed documents delivered local W02 composition; planned W03/W04 and historical wording explicitly separated/superseded. |
| 2 | Preimplementation characterizationd7b73a6 and preserved callers/contracts; current full257 passes unchanged compatibility tests. |
| 3 | Owned test-plan/design/red/implementation/review/fix/test/principle/CI/dev chain is committed; closure uses approved document review and actual relevant validation. |
| 4 | Setup and implementation integrated serially; closure is sole active task on codex/m01-w02-release-closure. |
| 5 | Designer approved documents and closed own review issue; Tester validates/executes and retains prior own-failure closure; PO acceptance remains PO-owned. UI N/A. |
| 6 | Subject/upstream equality and clean baseline verified; this completed evidence/board/inspection requires immediate docs(log) commit/push before handoff. |
| 7 | Actual final257 warning-as-error/compile/import checks pass in named environment; exact closure candidate Python3.9/3.13 CI remains pending. |
| 8 | R4-BASE-SCHED/META/HDF5 and OBS/DEFECT debt remain explicitly open under bounded disposition; no new observed scoped failure/warning. |
| 9 | Synthetic temporary fixtures, owned-resource/failure assertions and copy-only reading; no hardware operation or physical-stop/resume claim. |
| 10 | Closure changes architecture/process records only; no library/dependency/Python/storage/version change or framework expansion. |
| 11 | Final local checks/diff/paths/scope and applicable unchanged guide evidence complete; coordinator formal verdict, PO acceptance and exact closure CI/dev/main gates remain required. |

These entries support formal/PO readiness; they do not replace coordinator or PO
authority. Designer CR-W02-01 remains closed by its owner; Tester-owned red and
regression failures remain closed with repeated green execution. No new failure
is left open and no technical issue is closed by this documents-only inspection.

## Baseline disposition and limits

R4-BASE-SCHED-01 remains unfixed with the bounded private executor and legacy
status/cancellation characterization. R4-BASE-META-01 retains explicit text
hydration. R4-BASE-HDF5-01 retains copy-only loading and source integrity checks.
These scoped local measures regress green; no library repair/waiver is introduced.
OBS-004/005/006/DEFECT-2 remain historical out-of-scope debt under existing
tracked disposition, with no new observed named-check failure. Prior CI Node20/
forced24 and Ubuntu26 platform notices remain disclosed in release inspection,
not reclassified as absent or Python warnings. No current closure CI was run by
Tester and no whole-system zero-warning assertion is made.

Only local macOS Python3.13 executed here. The exact closure integration candidate
requires configured Ubuntu Python3.9/3.13 CI; previous implementation CI cannot
substitute for it. PO must accept AC01–12 before release promotion. Hardware/UI,
durable partial recovery/resume, W03/W04, publication and whole-M01 acceptance
are outside this gate. Immediate docs(log) commit/push and final clean/upstream
verification accompany handoff without circular self-hashes.
