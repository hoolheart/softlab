# M02-W03 PRD architecture review

Reviewer: independent architecture reviewer | Date: 2026-10-30 Asia/Shanghai
Verdict: **APPROVED**

Reviewed [draft PRD](../prd.md) against the [milestone plan](../../../plan/milestone-02.md), W02 acceptance, the W02 closure at `main:log/release_9/closure.md` (commit `4cbe0c4`), the W01 evidence decision and the current bounded runner and serial scheduler. This is requirements and feasibility review, not an implementation or test verdict.

## Findings

- **AR-PRD-01 — Broken prerequisite reference.** The PRD links `../release_9/closure.md`, which does not exist in this dev-based candidate tree. The authoritative closure exists on `main` at `4cbe0c4`. Use an accessible stable reference to that revision or another verifiable reference without copying or reopening closure. **CLOSED:** The PRD links the immutable `4cbe0c4` revision; the referenced closure exists at that revision.
- **AR-PRD-02 — Reset scope is ambiguous.** AC06 states reset is permitted only under documented terminal and drain conditions, while AC09 preserves legacy `Process` behavior. State explicitly that any new eligibility restriction applies to the bounded execution path and does not silently change legacy `Process.reset()` semantics; document supported behavior through the reviewed design and tests. **CLOSED:** AC06 explicitly limits the requirement to the bounded-run path and preserves legacy `Process.reset()` behavior.

## Scope assessment

The criteria otherwise cover the W03 plan's scheduler, acquisition and cleanup failure evidence, truthful partial rows and boundary, interruption, owned and unsupported borrowed resources, failed reset and independent fresh repeats with distinct identities. They retain W02's already-required cleanup and reporting, explicitly avoid retry and rollback by default, and leave thermal migration to W04. The listed generic scheduler, timeout, metadata, HDF5, OBS and DEFECT-2 dispositions are consistent with the W01 decision and W02 closure. CI/runtime warnings remain observations rather than claimed fixes. The design and test reviews must resolve precise failure injection points, observable reset eligibility, ownership and terminal evidence without treating this PRD review as proof of behavior.

No runtime checks, candidate CI, product acceptance or release closure are claimed by this review. Both findings were corrected by the Product Owner and independently re-reviewed. The referenced closure was verified in the local Git object at the linked revision; no network availability claim is made.
