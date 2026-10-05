# Technical requirements review — SIM-001

Owner: sw-jerry (Architect) | Candidate revision: `8ba0106`
Verdict: APPROVED | Date: 2026-10-05
Requirements: [PRD](../prd.md) | Scope: feasibility and architectural placement

## Assessment and evidence

The proposed object/instrument distinction is sound and feasible. Preserve a
separate simulated object so independent mock devices can share one physical
model. Dynamic evolution requires previous state and explicit time:
`x_next = F(x, u, t, dt)` and `y = G(x_next)`. Inputs alone would not describe
memory, accumulation or relaxation. A memoryless function remains a special
case. The release intentionally omits direct feedthrough, solvers and clocks
that advance on real-time sleeps.

`softlab/tu/theory/model.py` represents model configuration, feature calculation
and mapping selection. Its legacy exception fallback and shallow configuration
values are unsuitable as the new transactional simulation contract. Likewise
`theory/mapping.py` deliberately maps one fixed-shape ndarray to another; it
should remain usable inside callbacks without being generalized. A separate
simulation abstraction under `tu` is the compatible extension.

`station/parameter.py` supplies before-set and before-get hooks; base `Device`
needs no hardware acquisition. These permit controls to assign model inputs
and measurements to observe outputs without changing station interfaces.
`huo/process/common.py:250` executes setters, an optional after-set hook, then
an optional before-get hook and getters. `count` and `scan` forward these hooks.
An explicit model step in the process's before-get hook can therefore advance
once per acquisition, independent of wall-clock delays. No production change
outside `tu` is justified. Tests and documentation outside `tu` are needed to
prove and explain this existing integration seam.

## Findings

| ID | Severity | Evidence and required correction | State | Reviewer closure |
| --- | --- | --- | --- | --- |
| — | — | No blocking PRD defect found in source inspection. | — | — |

## Verdict and design constraints

APPROVED for the bounded single task, not implementation or release acceptance.
Detailed design must specify supported scalar/ndarray values, declarations and
validators, defensive ownership, callback reentrancy, commit-on-success
semantics, time overflow/nonpositive-step rejection and reset behavior. Copies
must protect committed state even if callbacks mutate their arguments before
raising. Output errors must not leave a committed new state/time. Do not claim
rollback of user callbacks' external effects. Prefer a documented hook bridge
over a general mock-driver hierarchy; any adapter must justify its need.

OBS-004, OBS-005, OBS-006 and DEFECT-2 remain existing recorded limitations.
This review neither fixes nor closes them. Fresh baseline execution, test-plan
review, detailed design, code review, testing and CI are all pending.
