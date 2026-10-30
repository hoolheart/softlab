# PRD — M02-W01: characterize and select an execution seam

Owner: Product Owner (sw-camille role). Date: 2026-10-30 Asia/Shanghai.
Status: **PAUSED BEFORE DEVELOPMENT — requirements approved for preparation;
release acceptance pending**. Architect feasibility approval is recorded in
[requirements review](architecture-review.md).
Process namespace: `release_8`; this is not a package version.

The user authorizes starting M02's first sub-release and explicitly requests a
pause before actual development. Prepare reviewed requirements, a written test
plan with Developer review, and detailed design with Architect review. Do not
write executable characterization tests or production code, run new runtime
experiments, implement a shared executor, or begin W02 before that pause.
Source/document inspection can inform hypotheses; it cannot certify observed
runtime behavior or justify a final extraction decision by itself.

## Product outcome

W01 establishes whether a bounded count client can reuse execution/lifecycle
orchestration currently embedded in the thermal example while retaining its own
acquisition policy. Its completed outcome is compatibility evidence and a
reviewed decision, not a shipped shared executor. W02 implementation requires
accepted W01 evidence and design. The [M02 plan](../../plan/milestone-02.md) and
its independent [product](../../plan/m02-product-review.md) and
[architecture](../../plan/m02-architecture-review.md) reviews supply planning
context; they are not W01 runtime or acceptance evidence. M01 is
[CLOSED](../release_7/closure.md).

### Distinct second client

Use a synthetic signal-monitoring count acquisition: configure a fixed signal
source once, then acquire a bounded number of paired signal/reference readings
at successive synthetic clock boundaries, committing a row only when both reads
succeed. The caller computes and records a derived difference. Unlike the
thermal case, it has no heater feedback/model-temperature policy and needs no
pure model callback re-observation. A consumptive/stateful measurement source
makes extra reads visible. This is useful as a small repeatable monitoring or
background-subtraction experiment and exposes multi-read row atomicity. It is
synthetic, deterministic and requires no instrument, new dependency or storage
format. The design may narrow its mechanics while preserving that product
distinction. A contract double supplements this client; it does not replace it.

## Requirements and acceptance

| ID | W01 acceptance condition | Evidence status at preparation |
| --- | --- | --- |
| W01-AC01 | Inventory actual callers of Process commit/join/reset, Scheduler, count, scan and the thermal executor; map scheduler, loop, action, callback, record, cancellation and cleanup ownership. Separate existing behavior, known defects and proposed guarantees. | Source assessment may be prepared; runtime confirmation pending. |
| W01-AC02 | An assertion-based characterization suite runs against unchanged production behavior and covers count/scan compatibility, callback order, normal completion, invalid operations, cancellation request/acknowledgment, failures, cleanup and repeated use. Record commands, environment, warnings and results without treating defects as desirable contracts. | Written plan only before pause; executable tests and execution pending. |
| W01-AC03 | Compare coordinated previous-boundary and direct current-boundary thermal policies with the signal-monitoring client. Identify repeated orchestration, caller-owned policy and non-shared work. Detect unexpected extra reads using stateful observations. | Scenario defined; executable comparison pending. |
| W01-AC04 | A decision record compares reuse of existing helpers, an additive minimal seam and retained local execution. Runtime evidence supports the selected boundary and compatibility claims. If useful reuse is unproven, obtain Product Owner/Architect scope revision rather than invent an API. | Candidate assessment/design only; final extraction decision pending. |
| W01-AC05 | The candidate contract states supported/rejected modes and ownership. It preserves legacy imports/count/scan/run_process behavior; command/advance/read/row order, truthful partial-row/final-boundary evidence and primary/cleanup failures are explicit. W02 includes all inseparable cleanup and failure safety; W03 adds refinement. | Subject to detailed design and Architect review; no implemented contract claimed. |
| W01-AC06 | Assess each baseline item below against the chosen path, preserve its tracked disposition, and identify any gate blocker needing separately reviewed scope. Record actual warnings only in the environments where observed. | Desk assessment allowed; current runtime impact remains pending. |
| W01-AC07 | Tester owns test evidence/failure closure; independent reviewer owns review findings; Architect reviews design and feasibility; Product Owner accepts requirements/evidence. Relevant principle and document checks pass. | Preparation approvals cover documents only. |
| W01-AC08 | Freeze completed W01 code/tests and required documents; pass exact-candidate CI before dev integration; obtain product acceptance before main promotion; verify promoted state and required checks, scope hygiene and clean worktree; record final hashes/CI links once and mark CLOSED. | Future release finish line; no integration/promotion authorized by preparation readiness. |

## Baseline debt disposition requirements

The [M01 follow-on backlog](../release_7/follow-on-backlog.md) and
[architecture debt register](../../arch.md) retain ownership of existing debt.

| Item | Required W01 disposition; no fix or waiver claimed |
| --- | --- |
| R4-BASE-SCHED-01 | Assess concrete scheduler singleton completion callback against private ownership; compare the thermal serial scheduler workaround. A generic singleton repair is outside implicit scope. |
| R4-BASE-META-01 | Preserve explicit metadata creation/disk hydration; keep metadata-constructor repair outside execution extraction. Assess whether characterization touches it. |
| R4-BASE-HDF5-01 | Preserve disposable-copy reads for the destructive legacy loader; use temporary synthetic files only. Storage repair is separately scoped. |
| OBS-004 | Retain avoidance of non-None settable init_value on affected Quantized/VISA subclasses; assess any selected client dependency. |
| OBS-005 | Preserve legacy feature fallback and opt-in strict evaluation; do not silently change error behavior. |
| OBS-006 | Retain explicit lookup escape hatch and existing cycle limitation; assess actual selected callers. |
| DEFECT-2 | Preserve the deliberate programmer-error warning for neither-readable-nor-writable Parameter construction; do not silence it. |
| Historical warnings | M01 fresh Python 3.9 walkthrough observed pandas concatenation FutureWarnings; Actions reported runtime deprecation notices. These are historical evidence, not fresh W01 observations or blanket waivers. |

If a known issue blocks an agreed gate, stop that affected gate for explicit
scope/disposition review. No unrelated fix, warning suppression or evidence-free
PASS follows from listing it here.

## Scope, roles and pause

One active task, M02-W01-001, covers this sub-release through closure; phases
are serial. Product Owner owns requirements and acceptance; Architect owns
feasibility, decomposition and design review; Tester owns written test plan and
eventual tests; Developer reviews implementability and later writes code;
Designer owns detailed design and later independent code review; Coordinator
enforces gates. UI/Figma is inapplicable. No new dependency, public framework,
scan extension, provenance redesign, durable recovery or hardware operation is
included. W02-W04 remain future releases.

The immediate finish line is an approved written test plan and detailed design,
reviewed requirements, recorded start-principle readiness and committed/pushed
preparation artifacts on `codex/m02-w01-preparation`, followed by an explicit
**PAUSED BEFORE DEVELOPMENT** handoff. No new runtime tests are required for
these documentation-only gates. Preparation readiness is not W01 acceptance.
Resume requires the user to authorize crossing this requested pause.

The eventual release finish line follows the
[release checklist](../templates/release-checklist.md) and
[principles](../../principles.md), including independent behavioral review and
testing, product acceptance, exact candidate CI, integration/promotion checks and
clean status. All these execution/closure gates remain pending at preparation.
