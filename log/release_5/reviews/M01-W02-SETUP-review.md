# M01-W02-SETUP independent document review

Reviewer: sw-celeste (Software Designer). Date: 2026-10-22 Asia/Shanghai.
Subject: committed `f83375f` on `codex/m01-w02-setup`, based on dev
`815c282`. Verdict: **APPROVED for documentation setup**.

## Reviewed scope and evidence

Reviewed [PRD](../prd.md), [tasks](../tasks.md),
[planned architecture](../architecture-plan.md), [board](../sprint_board.md)
and [start inspection](../principle_compliance_report.md) against
[the milestone](../../../plan/milestone-01.md),
[AGENTS](../../../AGENTS.md), [principles](../../../principles.md),
[current architecture](../../../arch.md) and the existing thermal example.
Read the relevant Process/SimpleProcess definitions and checked source locations
for SweepProcess reset, AtomJob ordering, scheduler stop and example acquisition.
This is a source-supported document review, not runtime characterization or
implementation code review. No production code, tests or detailed design changed.

| Check | Finding |
| --- | --- |
| Scope and acceptance | AC-01–12 refine W02 cancellation/failure/reset/repetition and preserve W01 numeric/storage behavior. W03 manifests, W04 adapters, real instruments, universal APIs and publication remain excluded. |
| Accounting | Request and acknowledgment differ; complete rows and committed ticks differ after sample failure. Failed step publication, zero-row stop, late request and failure precedence are explicit. No fabricated row, rollback, retry or extra teardown tick is promised. |
| Retention and reset | Partial rows remain inspectable in memory; persistence is explicitly not attempted for failed/cancelled acquisition. Reset follows terminal/drained work, preserves prior terminal evidence and never implies reusable cancelled Process state. Fresh repeats own fresh resources. |
| Responsibility and debt | Example-local composition is preferred; no library change is currently justified. Any required minimal huo change needs evidence and Architect/PO review first. Existing scheduler/metadata/HDF5 debt remains open with bounded regression requirements; historical accepted W01 is not reopened. |
| Current versus planned | Architecture envelope is marked PLANNED and does not alter actual arch.md. Existing source limits are distinguished from pending executable assertions; future required artifact paths are expressly not asserted to exist. |
| Gates and ownership | One active setup task; implementation remains backlog until setup dev integration. Test/developer review precedes design/architect review and implementation. Reviewer/tester closure authority, exact candidate CI and release acceptance before main promotion are preserved. UI and production TDD are truthfully inapplicable to document-only setup. |
| Formal evidence | Subject changes exactly five release_5 documents; merge-base with dev is 815c282. Subject is already at origin/codex/m01-w02-setup. Conventional docs(log) messages and trusted origin match the authorized workflow. |

## Issues and disposition

No review issues raised; issue IDs: none. No technical issue or historical debt
is closed by this verdict. Reviewer sw-celeste retains review-issue closure
authority. Tester document/path/scope/diff validation, coordinator formal gate,
exact candidate CI, dev integration and every implementation/release gate remain
pending. This approval does not certify W02 release completion or whole M01.

Validation performed: read the saved subject documents and governing sources;
inspect git log, branch/upstream, merge-base and subject diff scope. No runtime
checks were executed or inferred. This report and the board handoff are committed
and pushed immediately with docs(log); resulting hash and upstream equality are
reported separately to avoid circular self-hashes.
