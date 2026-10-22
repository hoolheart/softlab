# M01-W03-001 Developer test-plan review

Reviewer: sw-tom (Developer). 2026-10-22 Asia/Shanghai.
Subject45ee0bbfc12b276ba24b87d306b61bfa3c9c3982; characterization tests35d68f2.
Verdict: **APPROVED for implementability; detailed design may proceed**.

Read [test plan](../test/M01-W03-001-plan.md),
[characterization](../test/M01-W03-001-characterization.md),
[PRD](../prd.md), [architecture envelope](../architecture-plan.md), committed
metadata characterization tests and actual example/backend hydration contracts.
This is Developer implementability review only: no detailed interfaces/schema,
production/test edits or test reruns.

| Concern | Implementability finding |
| --- | --- |
| Representation | Explicit JSON text survives current str(value) HDF5 metadata writes. Capture/hydrate the optional envelope before destructive copy-loader attribute consumption. Keep legacy three-key hydration and no-envelope interpretation; no backend repair is needed. |
| Strict values/schema | Closed field/type validation and finite-number checks can precede encoding/persistence. Bool-as-count, malformed/present-invalid and unsupported schema/version rejection are explicit; exact keys/nesting/seams belong to Designer/Architect approval, not this plan. |
| Stored consistency | Real h5py tampering of owned copies, actual row/schema/tick/time/delay validation and independent changed-config Euler oracle are concrete assertions. Distinct run UUID and exact declared scalar/container types constrain decoding without requiring arbitrary constructors. |
| Independent inspection | Subprocess production/inspection with no original objects can assert typed envelope plus actual samples. Producer identity must remain captured disk evidence; absent legacy context cannot be repaired from reader globals. |
| Source identity | Source-relative Git query scope, declared digest coverage and explicit unavailable reasons are implementable without using unrelated current-directory provenance or making Git installation mandatory. Controlled query seams/temporary fixtures avoid changing the checkout. |
| Lifecycle/resources | Attach only after completed acquisition before save; never predict later verification/plot/cleanup success. Existing RunResult/RunFailure and structured helper-cleanup contracts remain constraints. Retained W01/W02 fault matrix plus manifest encoding/write/read faults provide bounded regression evidence. |
| Sequence/scope | Characterization is committed before behavior change; detailed design/Architect review precedes concrete feature red tests and implementation. No source prerequisite justifies library changes; broader provenance/framework/backend work remains excluded. |

No implementability issues raised; no review/Test failure or historical debt is
closed. Three focused/260 full warning-as-error results are Tester-owned evidence,
not Developer reruns. Local Python3.13 evidence does not replace exact later
Ubuntu3.9/3.13 candidate CI. R4-BASE-SCHED-01/META-01/HDF5-01 and OBS/DEFECT debt
remain open under characterized bounded measures. No future feature correctness,
independent code-review or release acceptance gate is certified by this approval.

Handoff: Designer detailed design, then Architect review, then Tester executable
feature red contracts against committed preimplementation source. Board remains
Design with Designer phase owner. This report/board update commits/pushes
immediately as docs(log), resulting hash supplied separately.
