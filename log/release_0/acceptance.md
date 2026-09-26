# BOOT-001 preparation acceptance

Owner: sw-camille (product owner). Date: 2026-09-26.
Reviewed baseline: `6d77369` on `codex/workflow-preparation`, plus the requirements
formalization in this same acceptance update.

Verdict: **PASS — preparation artifacts only**.

The product-owner review inspected the actual principles, architecture, guidance,
backlog, record layout/templates, CI configuration, branch tracking and role
evidence. It assessed delivery against [prd.md](prd.md), not implementation of
future `tu` APIs. The PRD formalization timing is explicit; initial requirements
were the approved user conversation.

| Criteria | Acceptance evidence |
| --- | --- |
| B1 Rules | Root principles and AGENTS agree on compatibility, serial gates, role ownership and `dev` integration. |
| B2 Architecture | `arch.md` separates current implementation from proposed evolution; independent reviewer verified source claims. |
| B3 Branch preparation | `git branch -vv` shows `dev` tracking `origin/dev` and the task branch tracking its remote at `6d77369`; `main` remains at `f288b2f`. |
| B4 Bounded backlog | `tasks.md` contains BOOT-001 and eight ordered, explicitly unauthorized future `tu` tasks with compatibility criteria. |
| B5 Process records | Layout, seven templates, design/validation-plan review, independent review, testing, board and principle records exist. Pending integration gates remain marked pending. |
| B6 Verification | Tester reports 5 local tests with no failures/errors/skips, successful compile/import and successful remote Python 3.9/3.13 jobs for `44cb265`. Limitations and the closed initial CI failure are recorded. |
| B7 Scope | `git diff --stat main..HEAD` contains documentation and CI only. No production implementation, dependency or support-range changes appear. |

Supporting evidence: [independent review](review/bootstrap_review.md),
[tester results](tests/bootstrap_validation.md), and
[successful CI run](https://github.com/hoolheart/softlab/actions/runs/36252706086).
The reviewer issued APPROVED; the tester issued PASS for applicable bootstrap
validation and closed the CI configuration failure. This product acceptance does
not close issues on behalf of either role or claim unexecuted checks passed.

The baseline cache diagnostics and writable-cache disposition are documented.
No real-hardware, packaging, notebook, lint/static-analysis or general zero-warning
claim is made. UI acceptance is inapplicable because no UI changed.

## Integration and release limits

This verdict accepts the preparation deliverables. It does not assert integration
has occurred. Before integration the coordinator must complete principle/formal
inspection, confirm this record is committed/pushed, and obtain successful CI for
the final candidate, including subsequent documentation commits. Do not mark the
board Done until actual integration evidence exists.

No release to `main` and no future `tu` implementation are authorized by this
verdict. Those require their own approved scope and acceptance gates.
