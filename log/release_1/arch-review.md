# TU-001 architectural review

Reviewer: sw-jerry (architect). Candidate: `9c84b35` on
`codex/tu-001-characterization`. Verdict: **APPROVED** for the TU-001
characterization architecture gate. This is an architecture/feasibility review,
not a code review, test result, detailed-design approval or release acceptance.

The compatibility matrix, PRD and characterization cases cover the interfaces
that later opt-in contracts must preserve: parameter call/hook behavior, device
composition and builders, station identity, VISA construction and command side
effects, model fallback, ndarray mapping and public exports. The cases keep
production unchanged and fit the existing five-module boundary: `tu` defines
device/model behavior; `huo` executes, `shui` persists, `mu` coordinates uses,
and `jin` supplies utilities. No new framework or dependency is needed for
TU-001. Direct source checks support the matrix's six observations.

| Observation | Architectural disposition |
| --- | --- |
| OBS-001 timeout units | Open TU-006 decision; preserve existing forwarding until the compatibility impact is assessed. |
| OBS-002 raw write | Open TU-006 defect candidate; require targeted regression and explicit behavior decision. |
| OBS-003 failed init/ownership | Open TU-004 lifecycle and TU-006 VISA decisions; no cleanup guarantee inferred from current code. |
| OBS-004 subclass initialization | Open TU-005/TU-006 defect candidate; reproduce before correction. |
| OBS-005 model fallback | Existing `features` fallback is characterized; TU-007 must expose failures through a new strict path. |
| OBS-006 delegation/cycles | Open TU-004 policy decision; explicit lookup remains available today. |

Approval means TU-001 can proceed to its remaining independent review, test,
principle and CI gates. It does not close OBS-001–006, approve a later detailed
design, or authorize integration. The matrix's claimed local 20-test pass is
tester evidence; this review did not rerun it. Hardware and concurrency remain
unverified.
