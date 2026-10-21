# M01-W02-SETUP document validation

Tester: sw-mike. Date: 2026-10-22 Asia/Shanghai.
Subject: committed `e5d87dd8cfac23f8acb6f0f8be7f24c8c6be0202` on
`codex/m01-w02-setup`; dev baseline `815c282`.
Verdict: **PASS for setup documentation validation only**. Test failures: none.
Formal coordinator review, exact candidate CI and integration remain pending.

## Executed checks and results

- `git status --short`: clean before validation; branch is codex/m01-w02-setup.
- `git rev-parse HEAD '@{upstream}'`: both were e5d87dd8cfac23f8acb6f0f8be7f24c8c6be0202.
- `git remote get-url origin`: git@github.com:hoolheart/softlab.git, explicitly
  confirmed trusted by the user. `git merge-base dev HEAD`: 815c2825cce038313322fc66bb96ccc7a7847207.
- `git diff --check 815c282 e5d87dd`: exit 0, no whitespace diagnostics.
- Python pathlib/re/subprocess assertions: all six setup Markdown documents
  have final newlines and no trailing whitespace; all 18 explicit local Markdown
  links resolve; PRD table contains AC-01 through AC-12 exactly once in order;
  principles.md contains eleven numbered principles; all six changed paths from
  baseline are Markdown under log/release_5. Python used: 3.13.9.
- Read PRD, tasks, planned architecture, independent review, principle report
  and board against plan/milestone-01.md, plan/validation.md and principles.md.
  W02 scope matches controlled interruption, partial retention, truthful outcomes,
  reset and fresh repetition. W03/W04, hardware, dependencies and publication
  remain excluded. Planned behavior and pending executable evidence are explicit.
  Review verdict is APPROVED; no review issue was raised or closed by Tester.

## Principle applicability

| Principle | Setup assessment / evidence |
| --- | --- |
| 1 | PASS: future envelope explicitly PLANNED; actual architecture unchanged. |
| 2 | PASS for setup: characterization is a prerequisite before behavior changes; executable characterization remains pending. |
| 3 | PASS: document validation applies here; implementation test/developer/design/architect/review/test gates are ordered. |
| 4 | PASS: setup is sole active task from dev; implementation stays backlog. |
| 5 | PASS: Tester validates documents only; Designer owns review; no UI change. |
| 6 | PASS for subject: exact upstream equality verified; this update must commit/push before handoff. |
| 7 | PASS for applicable scope: Python 3.13.9 runs structural checks; no runtime/dependency or supported-version compatibility claim. Candidate CI pending. |
| 8 | PASS: baseline issues explicitly retained with regression requirements; no waiver or closure. |
| 9 | PASS: no instrument or experiment-data access; future checks require synthetic temporary storage and owned resources. |
| 10 | PASS: six documentation files only; no dependency, framework, Python or storage change. |
| 11 | PASS for local document validation: diff/scope/path evidence above; formal/CI/integration and release acceptance remain pending. |

## Limits and handoff

No production tests, compileall/import checks, runtime failure injections,
scientific-library dependency validation, hardware operations or CI were run.
They cannot establish additional evidence for this documentation-only subject.
No implementation or W02 release acceptance is certified. No skipped configured
check, runtime warning or baseline test result is fabricated. This report,
principle inspection and board handoff require immediate docs(log) commit/push;
resulting hash/upstream/clean status are reported separately to avoid self-hashes.
