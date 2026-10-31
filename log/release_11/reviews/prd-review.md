# M02-W04 PRD architecture and feasibility review

Reviewer: Architect | 2026-10-30 Asia/Shanghai
Verdict: **APPROVED for PRD preparation**.

Reviewed the [draft PRD](../prd.md) against the [M02 plan](../../../plan/milestone-02.md), W03 bounded lifecycle and acceptance, current thermal example, its adapter tests, user guide and tracked debt. The criteria are feasible and testable as a scoped migration. The requirements retain both adapter timing policies and manifest versions, observable public entry points, independently checked lifecycle evidence and caller ownership of model, storage and plotting. They require a fresh-environment walkthrough, explicit evidence limitations and release gates rather than assuming migration or CI has passed. No code change, implementation approval or milestone acceptance follows from this review.

Design and test-plan reviews should resolve these implementation choices without expanding scope:

1. `execute` currently creates its scheduler before preparing the model and owns writer, loop and teardown separately; `run_count` creates and closes its own scheduler. Specify how setup failure and post-acquisition persistence/plot failure map to `RunResult` flags and existing cleanup diagnostics without double-closing or falsely claiming a scheduler was created. Preserve control-write ordering and validate the observable behavior against existing callers.
2. Example `StopRequest` and `RunResult` are distinct types, and the example's stored run identity is `DataGroup.id`, while W03's `CountResult.run_id` is an in-memory UUID. Design an explicit mapping, especially callbacks, interruption attachment and persisted identity, with no assumption that the UUIDs are interchangeable.
3. Preserve the W01 characterization tests' useful behavior even where they directly use `_make_scheduler` and `_acquire`; assess these private fixtures against the actual migration instead of treating their signatures as public contracts or silently removing behavioral assertions. Verify existing direct model observation is pure only for this fixture and count reads explicitly.
4. `run_count` only detects prior bounded claims, requires a fresh empty record and cannot preempt synchronous work. Tests should use fresh process/model/record per run and retain caller responsibility for legacy-used processes and physical termination limits.
5. W03 closure is locally visible through its recorded commit rather than in this `dev` tree. Its remote publication and applicable prerequisite evidence must be verified before downstream integration; do not present the local record as already pushed. Fresh-environment setup may fail due to dependency/network availability; record an actual blocker and leave the milestone open if the required walkthrough cannot run.

The PRD's tracked debt and synthetic-only scope are consistent with the plan. These points are review inputs for subsequent approved design and tests, not unclosed PRD defects. No runtime checks were performed for this document review.
