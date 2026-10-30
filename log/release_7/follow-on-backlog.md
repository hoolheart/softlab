# M01 follow-on backlog

Status: proposed observations, not authorized implementation tasks. This list
preserves limitations observed in the thermal rehearsal; prioritization and scope
remain subject to a later milestone decision.

| Observation | Proposed disposition |
| --- | --- |
| Execution and timing remain example-specific. | Evaluate extraction only from demonstrated duplication in proposed M02; preserve existing count/scan behavior and characterize cancellation and resource ownership. |
| The direct model adapter re-invokes a pure observation callback. | Do not generalize its semantics to stateful or nondeterministic sensors; evaluate an explicit acquisition boundary against another use case. |
| Physical settling, timeout and control readback have only a conceptual mapping. | Address a named transport, simulated contracts and explicit access decision in proposed M04; real operations and hardware qualification require separate authorization and evidence. |
| The bounded v1/v2 manifest is reference-specific and does not lock the environment. | Evaluate a compatible, bounded record/provenance design in proposed M03; do not infer arbitrary object serialization or migration support. |
| Tracked R4-BASE-SCHED-01, R4-BASE-META-01, R4-BASE-HDF5-01 and OBS-004/005/006 and DEFECT-2 remain. | Retain their existing dispositions and assess impact during later kickoff; do not silently change unrelated contracts. |
