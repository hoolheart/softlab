# M02-W04 and M02 milestone closure

Status: **CLOSED — W04 and M02**
Date: 2026-10-31 Asia/Shanghai. This is the authoritative closure summary for
release 11 and M02. The coordinator verifies final worktree cleanliness after
committing and pushing this factual closure record.

Both thermal adapter modes now use shared bounded count execution alongside the
distinct synthetic signal monitor. The example retains caller-owned model,
timing, storage and plotting behavior. The documented, narrowly scoped
[pre-run setup-failure compatibility change](reviews/setup-failure-product-decision.md)
reports false runner cleanup flags and ineligible reset when no runner existed.
No line-count, performance or cross-domain claim follows from this work.

| Final fact | Evidence |
| --- | --- |
| W01–W04 and acceptance | The [milestone matrix](milestone-acceptance.md) links the accepted increments. W04 [product acceptance](acceptance.md) and [principle inspection](principle_compliance_report.md) PASS for applicable local evidence; [design review](reviews/thermal-design-review.md) and [code review](reviews/code-review.md) APPROVED with findings closed. W03 closure is published on `origin/main` at `80701da`. |
| Independent testing | [Tester results](test/execution-results.md) report 358 full and 13 targeted local tests, compile/import, and a fresh isolated Python 3.13.15 walkthrough and independent repeats for both adapter modes, with separate-process inspection, eight-row numerical/timing oracles, distinct IDs and PNGs. Earlier failed/stalled installations and the font-cache notice are recorded there; no fresh Python 3.9 walkthrough is claimed. |
| Integrated candidate and CI | Candidate `4f60cccdd27d6137470963642801f8ce08197b34`; [candidate CI](https://github.com/hoolheart/softlab/actions/runs/41210134451) passed Python 3.9 and 3.13 before integration. `dev` merge `47a65424a45ad6db238d474ee5fb26498a486b67`. |
| Promoted revision and verification | `main` promotion `244cc359a09111550c68375a712747e4da5eddba` matches `origin/main` and contains the candidate; remaining branch differences are earlier closure records. [Promoted-state CI](https://github.com/hoolheart/softlab/actions/runs/41210307315) passed both Python jobs. |
| Remaining debt and limits | R4-BASE-SCHED-01/F-01, generic timeout F-02, R4-BASE-META-01, R4-BASE-HDF5-01, OBS-004/005/006 and DEFECT-2 retain tracked dispositions. GitHub Actions Node runtime deprecation annotation is a baseline warning; prior pandas and local cache notices retain recorded dispositions. No real-hardware qualification, physical termination, forced-process cleanup, rollback, automatic retry or durable recovery is claimed. M03 provenance, M04 physical contracts, M05 scan extensions and M07 recovery remain follow-on scope. |
| Worktree and scope | Final clean-worktree verification follows the closure record commit and push. |

The [release checklist](../templates/release-checklist.md) governs final
verification and closure.
