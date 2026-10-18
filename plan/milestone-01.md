# M01 — Proposed reference experiment milestone

Status: **PROPOSED**. Start TBD; four working-week increments including review
and integration. Purpose: prove the complete simulation → procedure → record →
persistence → plot path before extracting a framework. No hardware is needed.

Use one deterministic thermal step-response example. A lumped thermal body is
the object under study; a heater control and thermometer observation represent
instruments. The model is educational, with declared parameters and validity
limits. It does not claim physical calibration or universal thermal accuracy.

| Release | Purpose | Bounded deliverables | Acceptance and dependency |
| --- | --- | --- | --- |
| M01-W01 — Executable vertical slice | Demonstrate a complete useful experiment with existing primitives. | Headless reference script/guide; explicit coordinated stepping via `huo`; numeric `DataRecord` in `DataGroup`; fresh-file HDF5 save/reload; temperature-vs-simulation-time PNG; deterministic assertions and owned-resource cleanup. Detailed [PRD](release-01-prd.md). | Independent numerical/delay oracle, exact point count, save/reload identity and headless plot checks pass. Existing contracts preserved. Depends on accepted Release 3 source, no new public framework. |
| M01-W02 — Controlled interruption and repetition | Turn the demonstration into a trustworthy execution example. | Characterize existing Process/Scheduler cancel/reset semantics; cooperative stop at documented boundaries; explicit acquisition/persistence outcomes; retain completed partial rows; reset and repeated-run examples; targeted minimal compatible `huo` extension only if evidence requires it. | Inject a model failure, sample failure and cancellation after a known point; assert no fabricated rows, no extra post-acknowledgment ticks, visible outcomes and cleanup; two fresh repeats match. Depends on W01 acceptance; cancellation is software-only. |
| M01-W03 — Explainable stored result | Preserve enough context to understand and repeat the reference run. | Bounded reference manifest: model/procedure configuration, units, source/software identity, clock/delay interpretation, outcome and run ID. Choose an explicit JSON-text metadata envelope or separate artifact after backend review; no arbitrary object serializer. Document missing/version mismatch behavior. | Fresh-process reload recovers every required field with types restored through the chosen explicit envelope; changed configuration is distinguishable; unsupported metadata rejected visibly; old data round trips remain unchanged. Depends on W02 outcome semantics. |
| M01-W04 — Adaptation rehearsal and acceptance | Prove a credible adapter boundary and finish M01. | Run the same reference procedure with alternate model-backed control/measurement adapters; document a physical-instrument mapping and separate settling policy; contract test doubles, walkthrough, month acceptance and follow-on backlog. A VISA simulation spike is included only if it fits the reviewed weekly scope. | Procedure acquisition/order/record shape stays the same while adapter/timing policy changes; describe measurement-delay differences honestly; one documented command reproduces the complete reference result; independent review and month acceptance pass. Depends on W03; no real-device or simulated-VISA claim without executed evidence. |

## Weekly execution discipline

Each week contains requirements confirmation, test-plan review, design review,
implementation, independent review, testing and integration. Do not overlap
active tasks to recover schedule. W01 should be an example/integration change
using documented hooks; if it reveals a prerequisite production defect, record
it, assess its scope and adjust the release before implementation. W02–W04 are
planning envelopes; refine their PRDs after the preceding week's evidence.

For weekly acceptance use regression/compile/import checks for Python changes,
configured candidate CI, link/diff verification and a principle inspection.
Record warnings and baseline limitations. The M01 acceptance artifact must map
all four releases to committed evidence; this document alone is not acceptance.
The M01 goal is a dependable reference workflow, not a stable universal run API.

## Capacity and scope order

Keep numeric acquisition, timing correctness, deterministic repeat, storage
round trip and visible failure behavior. If capacity is insufficient, remove
optional notebook presentation and the W04 VISA spike first; move unmet required
gates to the next increment with revised dates. Do not omit reviews or redefine
partial output as success. Publication/version tagging is a separate decision.
