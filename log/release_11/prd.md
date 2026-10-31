# PRD — M02-W04: reference integration and milestone acceptance

Owner: Product Owner | Date: 2026-10-30 Asia/Shanghai
Status: **REVIEWED — PRD preparation only; implementation and acceptance pending**
Process namespace: `release_11` (not a package version).

## Product outcome and prerequisite

A maintainer can run the existing synthetic thermal example in both coordinated
and direct adapter modes through the accepted shared bounded count seam while
preserving the established workflow and observed results. Alongside the distinct
signal-monitor client, this demonstrates shared execution and lifecycle without
copied execution loops or caller-side cleanup workarounds for responsibilities
the seam owns. Acquisition policy, timing, model, persistence, plotting,
manifest and example-specific resource management remain caller-owned. This is a
maintainability outcome; it makes no unmeasured line-count, performance or
cross-domain claim.

The [M02 plan](../../plan/milestone-02.md) defines W04 and milestone exit.
W01's [acceptance](../release_8/acceptance.md) selected the seam, W02's
[acceptance](../release_9/acceptance.md) accepted bounded execution, and W03's
[acceptance](../release_10/acceptance.md) accepted lifecycle refinement. W03's
authoritative closure is recorded on local `main` at commit `80701da`, readable
with `git show 80701da:log/release_10/closure.md`; it is not present in this
`dev`-based preparation tree and its local-only status must not be confused with
a pushed remote record. W03 closure records candidate and promoted CI evidence;
its separately requested push is outside this PRD preparation. W04's own
integration requires the prerequisite release state and its applicable evidence
to remain verifiable.

## Requirements and acceptance

| ID | Acceptance criterion |
| --- | --- |
| W04-AC01 | Before behavior changes, independently reviewed tests and detailed design map the current example and both adapter policies to the shared `run_count` lifecycle. They specify ownership and compatibility for the CLI, results, stop, failure, partial data, reset and example-specific cleanup; Architect reviews feasibility and Developer reviews the test plan. Map `execute`, `run`, `RunResult`, `StopRequest`, `reset_after_run` and CLI behavior, and reconcile the bounded run UUID with the existing persisted run identity. Preserve W01–W03 evidence where unchanged; explicitly disposition compatibility tests that use private example helpers instead of silently dropping them. |
| W04-AC02 | Both shipped thermal adapter modes call the accepted shared `run_count` seam. Demonstrate that the thermal example and distinct signal-monitor client reuse its execution/lifecycle without copied execution loops or caller-side workarounds for behavior it owns. Keep configuration, timing/observation, model logic, storage, plotting and example-owned resources with the caller. |
| W04-AC03 | Independent numerical and timing oracles establish coordinated previous-boundary and direct current-boundary traces, command/advance/read/row order, one intended advance per completed row, source ticks and existing schema, including the default eight-row traces. Preserve direct mode's existing extra pure observation without introducing hidden reads or assuming stateful sensors are idempotent. Compare with relevant M01 evidence. |
| W04-AC04 | Preserve the public CLI and accepted v1 coordinated/v2 direct manifest and stored-context semantics. Save and reopen synthetic output for both modes; verify independent inspection in a fresh process and rejection of corrupt, unsupported, mixed or incompatible metadata, while preserving valid older files and output-collision and exit behavior. No schema migration or general provenance redesign is implied. |
| W04-AC05 | Regression and fault tests establish completion, stop before and after a row, late stop, acquisition failure before/after advance, incomplete-row handling, truthful boundary and primary/cleanup evidence mapped into the example result, separate persistence/plotting failures and owned-resource cleanup and global/borrowed isolation. Fresh repeat runs preserve terminal evidence and deterministic numerical traces excluding timestamps and UUIDs; legacy count/scan and existing public contracts remain compatible. Cooperative stop does not promise physical termination or preemption. |
| W04-AC06 | The documented complete headless walkthrough runs in a fresh Python environment using a new synthetic output directory: save, reopen, verify and plot. An independent repeat uses another fresh directory. Record exact setup and commands, version, warnings, output and any environment limitations; do not treat a preexisting environment or example printout alone as the fresh walkthrough. |
| W04-AC07 | Update actual architecture, public API/docstrings and user guide for the migrated behavior and unchanged ownership/limits. An independent code reviewer closes review findings and the Tester owns test execution and failure closure. Run relevant full unittest discovery, compile and import checks, and record actual environments, warnings, skips and failures. UI/Figma gates do not apply. |
| W04-AC08 | Product Owner independently accepts W04 and a milestone acceptance matrix maps W01–W04 and the M02 maintainability outcome to actual evidence, remaining debt, scope and limitations. Principle inspection passes for applicable scope; any material deviation or unsupported reuse claim requires reviewed scope decision rather than an invented pass. |
| W04-AC09 | Freeze and push the exact candidate with required records; pass its CI before `dev` integration; obtain product acceptance before `main` promotion; verify promoted revision/tree and required promoted-state checks; record final merge revisions, CI URLs, debt and scope in one authoritative closure summary; verify a clean worktree. Only then mark W04 and M02 **CLOSED**. A failed or unavailable required gate leaves the milestone open. |

## Scope, debt and limitations

Synthetic use and temporary output directories only; no real instrument access,
VISA or physical settling qualification, calibration, hard real-time claim,
preemption or forced-process cleanup guarantee. No new dependency, generic
scheduler repair, storage repair, global scheduler replacement, generalized
provenance, new scan behavior, automatic retry, rollback or durable recovery is
implied. M03 owns broader provenance, M04 physical contracts, M05 scan
extensions and M07 durable recovery. W04 does not expand the supported bounded
runner or silently change legacy `Process.reset()`. The Product Owner approved
the explicit [pre-run setup-failure compatibility change](reviews/setup-failure-product-decision.md):
when no runner was created, cleanup flags are false and thermal reset is
ineligible; other behavior remains subject to the acceptance criteria above.

R4-BASE-SCHED-01/F-01 and generic timeout F-02 remain tracked; use the accepted
serial seam rather than the defective private generic scheduler. R4-BASE-META-01,
R4-BASE-HDF5-01, OBS-004/005/006 and DEFECT-2 retain existing dispositions;
use disposable-copy reads where required, without incidental repairs or waivers.
Record actual warnings in the environments observed, including any Matplotlib or
fontconfig cache, historical pandas concatenation and GitHub Actions Node
runtime notices. A baseline issue blocking an agreed gate requires a separately
reviewed scope/disposition rather than a silent pass.

## Preparation and eventual release gates

The current request authorizes **PRD preparation only**. No executable tests,
production changes, migration, release acceptance, CI, promotion or milestone
closure is claimed or authorized by this document. The Product Owner defines
requirements; an Architect may review feasibility. Any later implementation
requires authorization and the applicable serial workflow: reviewed test plan
and detailed design, implementation, independent review and testing, principle
inspection, product acceptance, exact-candidate CI, promotion verification and
clean-worktree closure under [principles](../../principles.md) and the
[release checklist](../templates/release-checklist.md). Evidence for unchanged
content may be reused with applicability stated. One authoritative closure
summary links release and milestone evidence without recursive administrative
gates.
