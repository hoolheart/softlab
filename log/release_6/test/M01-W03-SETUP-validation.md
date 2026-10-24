# M01-W03-SETUP document validation

Tester: sw-mike. Date: 2026-10-22 Asia/Shanghai.
Subject: committed a6f134833d6e5348300866b6175e322328d0c926 on
codex/m01-w03-setup from dev410bc16a9778ada5063281ec0d4db2f21af76ac4.
Designer APPROVED setup subject2ccd22d; no review issues raised.
Verdict: **PASS for setup documentation validation only**.
Coordinator formal/start-principle check, exact candidate CI and dev integration
remain pending. No runtime/implementation/W03 acceptance is certified.

## Actual checks

- `git status --short`: clean before validation; branch codex/m01-w03-setup.
- `git rev-parse HEAD '@{upstream}'`: both a6f134833d6e5348300866b6175e322328d0c926.
- `git merge-base dev HEAD`: 410bc16a9778ada5063281ec0d4db2f21af76ac4.
- `git remote get-url origin`: git@github.com:hoolheart/softlab.git,
  user-confirmed trusted destination.
- `git diff --check 410bc16 HEAD`: exit0, no whitespace diagnostics.
- Python pathlib/re/subprocess assertions: six release_6 Markdown files have
  final newlines/no trailing whitespace; all15 explicit local Markdown links
  resolve; PRD table AC-01–12 appear exactly once in order; principles.md has
  eleven numbered principles; all six committed changes from baseline are
  release_6 Markdown only. Structural interpreter: Python3.13.9.
- Read PRD, architecture envelope, serial tasks, start principles, board and
  Designer review against plan/milestone-01.md and supplied AGENTS/principles
  constraints. W03 meaning matches stored model/procedure/configuration/units,
  source/software/clock/outcome/run identity and fresh-process typed inspection.
  W04 and whole-M01 acceptance remain outside scope.

## Envelope consistency and eleven-principle applicability

The planned single JSON-text metadata envelope is supported by the documented
static backend review, without promising a backend format repair. Closed schema
identifier/version, declared JSON-compatible values and strict bool/count and
finite-number distinctions are explicit; exact nesting/interfaces remain detailed
design work. Missing legacy context, corrupt present content and unsupported
schema/version differ. Reload must validate stored identity/count/order/units/
timing against actual samples, preserve producer identity and avoid reader-default
hydration. Source clean/dirty/unavailable cases and digest byte scope are required.
Snapshot follows completed acquisition before save/reload/plot/cleanup, with later
outcomes unknown; W02 partial outcomes remain in memory. No atomic durability or
arbitrary serialization claim. These are reviewed requirements/plans, not executed
backend/type/fresh-process assertions.

| Principle | Applicable setup evidence / disposition |
| --- | --- |
| 1 | PASS for documents: future envelope labeled PLANNED; actual architecture unchanged; existing five-element owners retained. |
| 2 | PASS for setup: source/caller findings recorded; executable characterization required before public/example behavior changes. |
| 3 | PASS: document-only setup validation and future serial test/review/design/code/testing gates distinguished; no invented production tests. |
| 4 | PASS: setup alone active from dev410bc16; implementation/closure backlog and branches not created. |
| 5 | PASS: PO requirements/Architect envelope/Designer review/Tester validation separate; reviewer/tester closure retained; UI N/A. |
| 6 | PASS for subject: committed/upstream equality verified; this report/board/inspection require immediate docs(log) commit/push. |
| 7 | PASS for applicable scope: structural interpreter reported; runtime/dependency checks irrelevant to pure documents, candidate CI still pending. |
| 8 | PASS: R4-BASE and OBS/DEFECT debt explicitly open; local measures require regression later; no silent repair/waiver. |
| 9 | PASS for setup: no instrument/data access; future synthetic temporary/copy-only/owned-resource checks specified. |
| 10 | PASS: scope only release_6 docs; no library/backend/dependency/Python/framework/storage contract change. |
| 11 | PASS for local document readiness: paths/scope/diff evidence; coordinator formal/exact CI/dev integration and later PO release acceptance pending. |

## Limits and handoff

No production tests, compileall/import, runtime encoding/backend/source identity,
fresh-process inspection, instrument operations or CI were run or inferred.
No missing required component, test skip or runtime warning is fabricated.
Historical W02 acceptance is dependency evidence; its257 tests are not current
W03 runtime evidence. No review issue or historical defect closed by Tester.
Implementation test-plan/characterization/design/code/testing follows only after
setup integration. Board advances to Formal/CI pending, never Done. Completed
records immediately commit/push as docs(log); final hash/upstream/clean status
are reported separately without circular self-hashes.

## Targeted English-preference validation — 2026-10-22

Tester: sw-mike. Subject committed5eb3c878ce4ba7503ac084a76ca2ac610e365dc3,
including AGENTS.md preferencee46c914 and Designer targeted APPROVED review.
Verdict: **PASS for targeted document update**; no issues found.

The exact six-line Project language addition requires English for communication
and newly created/updated deliverables in current and future softlab sessions.
Preserving existing Chinese instructions avoids unrelated translation and leaves
the new English requirement intact. This matches the direct user request.

Actual Python3.13.9 pathlib/subprocess/re assertions verified that removing the
exact added section reproduces the complete prior AGENTS.md byte content:
all existing engineering/workflow policies are preserved. The e46c914 commit
changes AGENTS.md only; changes since prior validated6b30e0c are that file and
the targeted Designer review. Eight Markdown files/21 explicit local links resolve;
all final newlines and added-section whitespace pass. `git diff --check 6b30e0c
HEAD` exits0 with no diagnostics. `git rev-parse HEAD '@{upstream}'` returns
5eb3c878ce4ba7503ac084a76ca2ac610e365dc3 for both; worktree clean before this
report. Original setup documents and their validation remain applicable.

All11 prior setup principle dispositions remain valid: this narrow language
policy changes no runtime scope, ownership, serial gate, data, dependency or
compatibility rule. No runtime/code/test/design checks were fabricated or run.
Coordinator formal/start-principle and exact candidate CI/dev integration remain
pending; board already correctly says Formal/CI pending and needs no phase change.
This completed update commits/pushes immediately with docs(log) before handoff;
no implementation or release acceptance is asserted.
