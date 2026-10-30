# Tasks — M02-W01 (process release_8)

Owner: Product Owner; Architect reviews feasibility/decomposition.
Date: 2026-10-30 Asia/Shanghai. Status: **PAUSED BEFORE DEVELOPMENT** at the
user-requested boundary. W01 release remains OPEN.

One task, **M02-W01-001 — characterize and decide the bounded count seam**,
uses serial phases below. These are handoffs within one task, not independent
releases. Requirements are in the [PRD](prd.md); the
[board](sprint-board.md) records actual state.

| Phase | Owner / required reviewer | Deliverable and exit condition |
| --- | --- | --- |
| Requirements | Product Owner / Architect feasibility review | W01 scope, acceptance IDs, distinct monitoring client, baseline dispositions and finish line; no W02 implementation included. |
| Test planning | Tester / Developer | Written cases trace W01-AC01–08 to assertions, fixtures, environment and evidence expectations. Explain current behavior versus desired contract and unrun characterization. |
| Detailed design | Designer / Architect | Source/caller and ownership assessment, candidate comparison and provisional boundary; explicit compatibility, failure, cleanup and rejection policy. Extraction approval remains conditional on runtime evidence. |
| Requested pause | Coordinator | Verify required document approvals and preparation principle readiness; batch commit/push artifacts and hand off PAUSED BEFORE DEVELOPMENT. No executable tests/code/runtime experiments. |
| Characterization after resume | Tester; Developer reviews implementability | Add assertion-based tests against unchanged production baseline; execute in recorded environment; report warnings/failures/ownership/order and truthful results. Pending user resume. |
| Decision after evidence | Designer / Architect; Product Owner scope agreement | Reconcile candidate against both thermal policies and distinct client; accept minimal justified seam or return milestone scope for review. No shared executor implementation in W01. |
| Independent checks | Independent reviewer; Tester | Review new executable characterization and evidence; reviewer/tester close owned issues; relevant regression/compile/import checks and document/principle consistency. |
| W01 release closure | Product Owner; Coordinator/integrator | Acceptance, frozen candidate exact CI before dev, main promotion and required verification, one closure summary and clean worktree. Only then W02 may begin. |

Requirements received [Architect approval](architecture-review.md); the written
test plan received [Developer approval](reviews/test-plan-review.md), with
DR-TP-01 closed by its reviewer; detailed design received
[Architect approval](reviews/design-review.md), with AR-DD-01 closed by its
reviewer. These approvals cover source/document preparation only. The
[predevelopment handoff](predevelopment-handoff.md) records the pause and next
steps; executable characterization and all later phases remain pending.

W01-AC02/03/04 require runtime evidence after resume; desk inspection cannot close
them. W01-AC05 supplies the next release's safe boundary, not a newly shipped
executor. Missing reuse evidence triggers product/architecture revision instead
of starting W02. New blockers retain owner, evidence and disposition; existing
debt is not automatically repaired or waived.
