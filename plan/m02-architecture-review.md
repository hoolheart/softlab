# M02 Architect planning review

Date: 2026-10-30. Reviewer: independent Architect (sw-jerry role).
Verdict: **APPROVED — planning feasibility and decomposition only**, following
reinspection of the corrections below. The public API and extraction decision
remain W01 deliverables. This review does not certify implementation, tests,
release acceptance, integration or promotion.

## Evidence inspected

Reviewed [M02](milestone-02.md), the [Product Owner review](m02-product-review.md),
[overview](README.md), [roadmap](roadmap.md), [principles](../principles.md),
[release checklist](../log/templates/release-checklist.md), [M01 closure](../log/release_7/closure.md)
and [follow-on backlog](../log/release_7/follow-on-backlog.md). Source inspection
covered `huo` process interfaces and `run_process`, `SimpleProcess`,
`SweepProcess`, `AtomJob`, count/scan construction, the scheduler interface and
concrete scheduler, and the thermal acquisition, adapter, execution and reset
callers. Existing compatibility and thermal adapter assertions were inspected;
they were not executed for this document review. Source baseline is the unchanged
M01 implementation in the current checkout.

## Findings and owner closure

| ID | Finding | Architect disposition |
| --- | --- | --- |
| AR-M02-01 | Independently releasing W02 while leaving all lifecycle completion to W03 could permit incomplete cleanup or failure reporting. | CLOSED after reinspection: W01 explicitly assigns inseparable lifecycle work to W02; W02 requires owned cleanup on success/cancel/failure and truthful primary plus cleanup evidence before release. W03 adds coverage and refinement. Scope or dates move if needed. |
| AR-M02-02 | The ordering matrix needed to distinguish a completed row from an attempted acquisition that advanced before sampling failed. | CLOSED after reinspection: the matrix now explicitly retains the last boundary without fabricating a row after an advance/read failure. This agrees with M01 direct-adapter assertions showing three rows at boundary four. |

No unresolved architectural planning finding remains. These closures concern
the reviewed plan wording; later implementations require their own review.

## Feasibility and decomposition

A bounded count executor in `huo` is plausible because the thermal caller
already drives the public `Process.commit/join/reset` and `Scheduler` interfaces.
`AtomJob.body` performs getter hooks and reads before adding a row, providing an
existing point for policy ordering. This does not prove a new abstraction is
necessary. W01 must demonstrate shared orchestration with a distinct synthetic
client and compare extending existing helpers against an additive bounded seam.
The two thermal timing policies alone are insufficient evidence of reuse.

The current concrete scheduler has a completion callback referencing the module
singleton. Constructing a private concrete scheduler is therefore not an
established ownership solution. The thermal example instead supplies its own
serial public-interface scheduler, excludes chained/threaded actions and owns a
private loop. W01 must explicitly account for R4-BASE-SCHED-01 before choosing a
seam; a broad singleton scheduler repair is not implicitly authorized. Similarly,
legacy `run_process` resets automatically and returns a Boolean/time tuple, while
the reference retains richer terminal evidence. Preserve legacy behavior and
select an additive contract unless a compatible extension is demonstrated.

W02's reference fixture can exercise the existing private simulation/adapter
helpers through the new seam while the shipped CLI remains unchanged until W04.
It must not become a second maintained public runner. W02 must already be safe
within its supported scope; W03 can then expand fault coverage, clarify reset
eligibility and verify fresh repetitions. W04 performs the final migration and
compares both numerical/timing policies and stored v1/v2 context. Each release
retains its own closure gates and finishes before its successor begins.

Resource ownership and observation policy must remain explicit. Borrowed or
running-loop modes may be rejected before effects instead of being implemented.
Cleanup cannot dispose of global/borrowed resources, and reset cannot rewrite
terminal evidence. Direct model observation re-invokes a pure callback today;
generic execution must not add reads or assume sensor idempotence. The planned
stateful read double and boundary/cause assertions address these risks. Persisted
partial recovery, generalized provenance, new scan behavior and hardware
settling remain with their named later milestones.

## Principle inspection and limits

Planning inspection: **PASS for reviewed scope**. Principles 1, 2 and 10 are
served by layer ownership, caller characterization, compatibility and the
evidence-dependent minimal seam. Principle 3 retains future TDD and independent
design/review/testing; no production test is fabricated for this document task.
Principles 4 and 5 apply to the single planning-review task and separate Product
Owner/Architect authority; no UI design gate applies. Principles 6 and 7 require
the coordinator's final batched commit/push and document/link/diff validation;
those actions are pending and not certified here. Principle 8 retains baseline
debt and warnings for explicit kickoff impact assessment. Principle 9 requires
synthetic data, temporary outputs and explicit resource ownership. Principle 11
is reflected in the per-release candidate CI, acceptance, promotion verification
and clean-worktree finish line; all execution gates remain future obligations.

No production source or `arch.md` was changed, no dependency was added, and no
runtime, CI or hardware result is claimed by this planning review.
