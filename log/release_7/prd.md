# PRD — M01-W04: adaptation rehearsal and milestone acceptance

Owner: Product Owner. Date: 2026-10-30 Asia/Shanghai.
Status: **ACTIVATED — requirements defined; implementation and acceptance pending**.
Process namespace: `release_7`; this is not a package version.

The user has authorized the fourth release of the first milestone. The plan
identifies W04 as the final M01 weekly increment. Its prerequisite, W03, is
[CLOSED](../release_6/closure.md). Deliver a credible rehearsal showing that the
existing thermal reference procedure can work through alternate model-backed
control and measurement adapters and a separately expressed timing/settling
policy. This is an example-level demonstration, not a promise of a universal
adapter framework or hardware readiness.

## Required outcomes and acceptance

| ID | Acceptance condition |
| --- | --- |
| AC-01 | The same reference procedure runs with both the existing setup and an alternate model-backed control/measurement implementation. Adapter substitution and timing policy are explicit; the procedure's acquisition sequence, column order, units and record shape remain consistent. Contract test doubles exercise control writes and measurement reads, including ordering and visible failures. |
| AC-02 | Tests establish each adapter's observation source and timing with an independent expected result. The guide distinguishes coordinator previous-boundary measurement delay from any alternative delay or settling behavior; it does not equate wall-clock waiting with simulated advancement or imply physical equivalence. Timing differences are represented truthfully wherever recorded context claims them. |
| AC-03 | The delivered guide gives a single root-level, headless command that creates the complete reference result without hardware: execute, save, reload and plot. A tester executes it in a fresh environment or clearly documents the actual environment and limits, and verifies resulting data, manifest and plot. The walkthrough explains adapter selection, execution and inspection. |
| AC-04 | A physical-instrument mapping explains which control and measurement operations an eventual real setup would need, units, timing/settling and lifecycle considerations. It separates physical settling from model stepping and labels unverified physical behavior explicitly. No physical operation or qualification is claimed. |
| AC-05 | Existing W01 numerical and record contracts, W02 interruption/repetition/outcome behavior, and W03 typed manifest and legacy loading semantics remain valid. Tests cover meaningful alternate-path behavior, failure visibility and owned-resource cleanup; no fabricated observations or false completion. Any intentionally changed public behavior needs explicit scope review and compatibility characterization. |
| AC-06 | The follow-on backlog records observed gaps and their proposed disposition without implying M02 or later scope has already been approved. The milestone acceptance record maps all four weekly increments to actual accepted evidence and assesses the M01 exit gate. |
| AC-07 | Independent design/code review and tester checks are completed with owners closing their own issues. Applicable principle inspection, full regression, compile and import checks, guide/architecture consistency, scope/diff hygiene and actual environment, warning and skip reporting are recorded. Product Owner accepts the evidence before promotion. |
| AC-08 | Freeze and push the final candidate; exact configured candidate CI must pass before integration into `dev`. Promote accepted content to `main`, verify the promoted revision/tree and required promoted-state checks, and ensure a clean worktree. Record merge revisions and CI links in one closure summary and mark CLOSED only after all applicable gates pass. |

## Scope and boundaries

The W04 VISA simulation spike is **excluded** from this bounded release; the
required demonstration uses model-backed adapters and test doubles. Neither
real-device operation nor simulated-VISA validation is authorized or claimed.
No package publication, version/tag, new dependencies, universal framework,
backend-wide metadata migration, physical calibration or hardware qualification
is included. UI gates do not apply unless actual UI work is proposed. A necessary
prerequisite or a behavior change beyond this scope requires explicit review.

Preserve the accepted W03 bounded manifest and distinguish unsupported timing
and missing/versioned metadata rather than silently recording misleading values.
Carry forward R4-BASE-SCHED-01, R4-BASE-META-01 and R4-BASE-HDF5-01 and
OBS-004/005/006 and DEFECT-2 with existing dispositions; assess their actual
impact without silently waiving new failures. W03 recorded parser/pandas and CI
platform notices: report observed warnings and skips without asserting universal
warning-free or full-environment reproducibility. Use synthetic data and temporary
or fresh output paths; protect existing data and release owned resources.

## Finish line and evidence

Apply proportionate project gates in [principles](../../principles.md) and the
[release checklist](../templates/release-checklist.md): reviewed requirements,
test plan and design; implementation; independent review and testing; principle
inspection; acceptance; exact candidate CI; integration and promotion verification;
and clean worktree. Reuse evidence for unchanged content and repeat checks only
when affected. Actual CI outcomes and revisions belong in the final closure
summary, without creating recursive evidence commits. This PRD does not claim any
pending gate has passed.
