# R3-003 test plan — cohesive coordinated simulation

Tester: sw-mike | Date: 2026-10-11
Branch: `codex/r3-003-coordination` (from `dev` @ `0bd97d5`, head `72379f4`)
Requirements: `log/release_3/prd.md` | Task: `log/release_3/tasks.md` R3-003
Characterization baseline: see `r3-003-baseline.md` (165 tests OK, zero
warnings; coordinated-simulation capability confirmed ABSENT with
verbatim probe evidence; seven no-regression anchors recorded).

## Status of this document

This is a **test plan only**. Every case is marked:

- **[BASELINE-READY]** — executable against the current baseline without
  new production code (compatibility re-runs of existing suites and
  characterization assertions of behavior that must not change). These
  pass today and must keep passing.
- **[PLAN]** — requires the R3-003 implementation. Against the current
  baseline every [PLAN] case fails by `AttributeError`/absent-API error
  (baseline §2); per repo AGENTS.md these are the assertion-pinned
  minimal reproductions: they must vacuously fail before implementation
  and pass after.

No production API names are hard-coded where the mechanism is design
work: cases pin **observable behavior** (a shared clock advances all
participants one interval; an edge at tick t delivers the source value
committed at t−d; a failed tick commits nothing; a connected participant
cannot be standalone-evolved or removed) and accept whichever
class/method names the approved detailed design documents. Placeholder
spellings (`connect`, `sim_dt`, `coordinator`, `step`, `prepare`,
`mock_control`, `mock_measurement`) denote capabilities, not contracts.

## Acceptance criteria under test (verbatim from `log/release_3/prd.md`)

> **R3-AC-01** | Uniform delayed network | A deterministic chain,
> fan-out and feedback network produce the specified previous-boundary
> results regardless of object/device registration or iteration order.
> No edge uses a newly computed same-tick source.

> **R3-AC-02** | Defined initial state and device timing | Tick-zero
> object outputs and mock initial samples are documented; the first
> transition has defined results. Writes stage controls, control
> readback shows pending values, measurements show delivered samples,
> and reads/writes never implicitly tick.

> **R3-AC-03** | Shared clock | Valid explicit `sim_dt` controls all
> coordinated participants; invalid/nonfinite/zero/negative/bool values
> are rejected. Successful ticks advance one interval; failed ticks do
> not. Mid-run interval changes fail until reset. Models using dt have
> a documented consistent binding.

> **R3-AC-04** | Station membership and builders | Existing device use
> remains compatible. Object membership, explicit lookup, naming,
> builder discovery/construction, failed-build safety and independent
> instances work; connected removal and shared-active-ownership
> conflicts are rejected.

> **R3-AC-05** | Valid safe connections | Endpoint/direction/value
> compatibility and single-driver rules are enforced. Fan-out copies
> values safely; ndarray mutation cannot alter another participant or
> committed snapshot. Unconnected inputs retain their values; invalid
> graph changes are atomic.

> **R3-AC-06** | Atomic tick/reset | An evolution, observation,
> validation or factory failure preserves the prior committed network
> and clock with original callback exception identity. A successful
> reset rebuilds initial participants/buffers/clock; accepted pending
> writes survive failed ticks. External side-effect limits are explicit.

> **R3-AC-08** | Compatibility and usable delivery | Existing standalone
> object/device/theory behavior and imports remain compatible except the
> explicitly corrected gaps. A worked connected example demonstrates the
> clock, delayed feedback, mock interaction and reset. Regression,
> compile/import, warning and candidate CI evidence precede integration;
> architecture describes implemented capability only after delivery.

**Scope split.** This plan covers the R3-003 task row exactly:
"Cohesive coordinated simulation: opt-in mock endpoints, connections,
ownership, shared sim_dt, tick-zero preparation, delayed stepping and
atomic reset" — R3-AC-01–06 **in full** plus the **compatibility
portion** of R3-AC-08 (existing standalone behavior/imports compatible;
regression, compile/import, warning and candidate CI evidence). The
worked connected example and user guide are **R3-004 scope** and excluded
here. From R3-AC-04 only the *remaining* clause is new work in this task
(*connected removal and shared-active-ownership conflicts are rejected*);
the membership/builder portion was closed by R3-002 and is re-run here
only as regression.

PRD behavioral statements pinned by this plan (lines 33–66 — clock,
initialization, delayed signals; lines 68–106 — composition, ownership,
failures), and the architecture-plan mechanisms (lines 13–94: opt-in
coordinator, mock pending/delivered storage, one boundary snapshot per
transition, tick-zero preparation, staged coordinated reset,
station-owned fixed interval with explicit model-dt binding, ownership
guards, safe detach, directional single-driver connections with
fan-out and permitted cycles).

## Planned test files

| File | Covers |
| --- | --- |
| `tests/test_tu_coordination.py` (new) | R3-TC-01a–01e, 02a–02e, 03a–03f, 04a–04c, 05a–05g, 06a–06e: coordinated network, tick-zero, clock, ownership, connections, atomicity (automated `unittest`; synthetic fixtures only) |
| (no new file) | R3-TC-08a–08f: full-suite regression, compile/import/warning gates, dependency/scope checks |
| (manual record checks) | CHK-03-1: reviewer/design-gate scope checklist, including the five tasks.md design-gate proofs |

All synthetic fixtures; stdlib + NumPy only; temp dirs only; no real
instruments; no `huo` process; UI/hardware gates N/A. Naming note:
`tests/test_tu_coordination.py` is a placeholder file name; the tester
may adjust it to the approved design's module layout and record the
adjustment in the results document.

Fixture convention (deterministic models, shared by cases): a scalar
first-order accumulator `x_next = x + u, y = x` and an ndarray gain
model, constructed via `SimulatedObject` exactly as the existing
`tests/test_tu_simulation.py` fixtures do — no new fixture machinery.

---

## R3-AC-01 — uniform delayed network

**Purpose:** every edge delivers the source value committed at the
previous boundary; no same-tick propagation; results independent of
registration/iteration order. Baseline: no connection or stepping API
exists (baseline §2/§3) — [PLAN] cases fail by absent API today.

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| R3-TC-01a **[PLAN]** | Chain A→B→C advances at most one edge per transition | Prepared network of three accumulator objects connected A→B→C; distinct known initial states/inputs | Prepare at tick 0; perform transition 0→1; record B's input, C's input, all states | B consumes A's tick-0 output; C consumes B's **tick-0** output (NOT B's newly computed one); A's new output reaches C no earlier than transition 1→2. Numeric expectations computed in-test from the fixture's declared initial values |
| R3-TC-01b **[PLAN]** | Fan-out one source to two destinations | Prepared network; source S connected to D1 and D2 | Transition 0→1; record D1 and D2 inputs | Both destinations received the identical S tick-0 value; one source feeding multiple destinations is legal |
| R3-TC-01c **[PLAN]** | Feedback edge uses initialized previous value on first transition | Prepared network with feedback edge C→A (cycle); documented initial edge value | Transition 0→1; record A's consumed feedback input | A consumed the initialized/declared boundary value, not a same-tick C output; cycles are permitted (no cycle rejection, no solver) |
| R3-TC-01d **[PLAN]** | Determinism independent of registration/iteration order | Same logical network registered in two different orders (build B before A vs A before B; dict-traversal-hostile orders), plus a run after listing/inspecting members | Prepare and run both networks for ≥3 transitions with identical fixture callbacks and initial values | Bit-identical committed states, delivered samples and clock on every tick across both orders and across the inspection interleaving; insertion/traversal order cannot change deterministic results (PRD lines 19–21) |
| R3-TC-01e **[PLAN]** | No edge uses a newly computed same-tick source — negative assertion | Prepared network; per-tick trace recording each object's computed outputs vs delivered edge values | Run 3 transitions | For every tick k→k+1 and every edge, the delivered value equals the source's committed output at tick k (asserted against an in-test golden trace); any same-tick value fails the case |

## R3-AC-02 — defined initial state and device timing

**Purpose:** tick-zero outputs are computed from initial states during
explicit preparation; mock initial samples documented; control writes
stage, readback shows pending; measurements show delivered samples;
reads/writes never tick. Baseline: no mock endpoint or preparation API
(baseline §3, GAP rows 2–3).

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| R3-TC-02a **[PLAN]** | Tick-zero outputs from initial states at explicit preparation | Station with objects but **no** preparation called | Inspect members (snapshot/description/lookup) BEFORE any preparation; then call the preparation entry | Inspection alone never invokes callbacks, prepares, evolves or ticks (object states/callback counters unchanged); the explicit preparation computes tick-zero outputs from initial states and publishes tick 0/sim-time 0 as one operation |
| R3-TC-02b **[PLAN]** | Failed tick-zero leaves simulation unstarted | Prepared fixture whose object initial-state factory or tick-zero output evaluation is armed to raise on the first preparation attempt | Attempt preparation; catch; inspect clock/network/participants | Original exception identity (`assertIs`); no partial clock/network/ownership commit — no tick, no sim-time, no activation; a later successful preparation with a healthy factory works from untouched initial declarations |
| R3-TC-02c **[PLAN]** | Control write stages pending; readback shows pending; no implicit tick | Prepared connected network with a mock control source driving an object input | Write value v1 (control readback → v1); verify object input/state and clock unchanged and tick count unchanged; perform transition; verify the pending value was delivered across the outgoing edge at the next boundary | Write is observable as pending control readback immediately; it does NOT change downstream object state nor advance time; the next explicit transition samples the pending value and delivers it across its edge (PRD lines 48–51) |
| R3-TC-02d **[PLAN]** | Measurement read returns last delivered sample; tick-zero value explicitly initialized and documented | Prepared network with a mock measurement sink on an object output | Read measurement before any transition; read after each of two transitions; compare | Initial read returns the explicitly initialized/documented tick-zero sample, distinct-by-construction from the first delivered source value (fixture pins different values); subsequent reads return the most recent delivered sample; a measurement read never fetches the current source directly, never bypasses the edge delay, never advances time (clock and tick count unchanged by reads) |
| R3-TC-02e **[PLAN]** | Multiple writes before a tick use the final pending value | Prepared network; mock control | Write v1 then v2 before any transition; transition; inspect delivered value | The delivered value is v2 (last write wins); intermediate write v1 never reaches any destination |
| R3-TC-02f **[PLAN]** | Reads and writes never implicitly tick — comprehensive | Prepared network with counters on every callback and every mock read/write path | Perform: control read, control write, measurement read, member inspection, `describe`/snapshot, unconnected-input write; after each, assert tick count, sim-time and object callback counters | None of these operations advances the clock, invokes `evolve`/`observe`, or mutates any other participant (PRD lines 62–63: inspection/read/write does not advance the clock) |

## R3-AC-03 — shared clock

**Purpose:** one explicit, finite, positive, non-boolean station-owned
interval governs all participants; rejection of invalid values; exact
per-tick advance; no advance on failure; no mid-run change; documented
model-dt binding. Baseline: `Station.sim_dt` absent (baseline §2).

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| R3-TC-03a **[PLAN]** | Valid explicit sim_dt accepted and shared | Station; ordinary `Station(name)` constructed with no simulation argument | Set a valid interval (e.g. `0.5`); prepare a multi-participant network; run 3 transitions; query each participant's view of simulated time and tick | Ordinary station/device use requires no new argument or simulation initialization (baseline §4 anchor 7 unchanged); all participants observe the same tick count and the same accumulated simulated time (k × sim_dt within documented float policy); interval applies uniformly |
| R3-TC-03b **[PLAN]** (data-driven) | Invalid sim_dt values rejected | Station not yet started | Attempt to set each of: `0`, `-1.0`, `float('nan')`, `float('inf')`, `float('-inf')`, `True`, `False`, `'0.5'`, `None`, `0.5+0j`, a `Decimal`/NumPy-scalar value | Each is explicitly rejected before any run state changes (exact error classes per approved design; message names the invalid value category); after every rejection the station remains usable with a valid interval (PRD line 57: strictly positive finite real duration, booleans rejected) |
| R3-TC-03c **[PLAN]** | Failed ticks do not advance the clock | Prepared network; evolve callback armed to raise at a chosen transition | Run transitions up to and including the failing one; record tick/time before and after the failure | Tick count and simulated time identical before/after the failed attempt (PRD line 61: failed ticks do not advance); original exception identity preserved (cross-ref R3-TC-06a) |
| R3-TC-03d **[PLAN]** | Mid-run interval change rejected until coordinated reset | Prepared and stepped network | Attempt to change the interval mid-run (each documented setter path); catch; run one more successful transition; then coordinated-reset and change the interval | Every mid-run change attempt is rejected explicitly with clock/network untouched; after reset the change succeeds and the new interval governs subsequent ticks (architecture-plan lines 73–76) |
| R3-TC-03e **[PLAN]** | Model-dt binding is explicit, validated, never inferred by name | Model declaring a dt input (e.g. input named `dt`); prepared network | Bind the model's declared dt input to the station interval per the documented binding mechanism; run transitions and check the model received k × sim_dt; then attempt a contradictory second driver/write onto the same dt input | Bound model receives values consistent with the shared interval at every tick (no silent disagreement — PRD line 66); a second connection driver or contradictory write onto the bound input is rejected; the binding is by explicit declaration, never by name inference (architecture-plan lines 76–77); binding mechanism and its semantics are documented (CHK-03-1 cites the doc location) |
| R3-TC-03f **[PLAN]** (boundary) | Numeric clock limits: representability and precision | Station | (a) attempt the smallest positive float `5e-324` and the largest finite float as intervals; (b) run many transitions with a small interval (e.g. `1e-9` × 10⁶ ticks — kept fast, or the design's documented limit) and compare accumulated time against the documented roundoff policy; (c) attempt values that overflow the internal tick×dt representation per the design's documented limit | (a)/(c) acceptance/rejection behavior matches the design's documented representability rule — no silent `inf`/`nan` clock; (b) accumulated simulated time matches the documented floating-point policy (exact assertion pinned to the policy as documented, not to an invented precision); design-gate proof of numeric clock limits cited by CHK-03-1 is exercised by these cases |

## R3-AC-04 — remaining clause: connected removal and shared-active-ownership

(The membership/builder portion of AC-04 was closed by R3-002; its 17
test methods re-run as regression in R3-TC-08a/08c.)

**Purpose:** a participant connected/enrolled in an active coordinated
simulation cannot be removed, standalone-evolved or standalone-reset;
one mutable participant belongs to at most one active coordinated
simulation. Baseline: only a forward-looking `rm_object` docstring
(station.py lines 205–208); no guard exists — [PLAN] cases fail by
absent API today.

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| R3-TC-04a **[PLAN]** | Connected removal rejected | Station holding a connected/enrolled object | Attempt `rm_object` on the connected participant (each documented removal path) | Explicit rejection naming the active coordination/edges; the object remains a member, connected and committed-state untouched; no hardware cleanup is attempted (PRD lines 83–84; architecture-plan lines 91–92) |
| R3-TC-04b **[PLAN]** | Removal succeeds after safe detach / stopped boundary | Same network after the documented detach/stop path removes edges and deactivates | Remove the participant; look it up | Removal succeeds once disconnected/inactive; membership bookkeeping only; the detached participant's standalone evolve/reset is available again |
| R3-TC-04c **[PLAN]** | Shared-active-ownership conflict rejected | One mutable object; two stations (or two coordinators) | Enroll the object in station S1's active simulation; attempt enrollment/activation in S2 | Explicit rejection of the second activation; S1's simulation remains coherent and fully functional; multiple mock controllers/readers *within* S1's simulation are permitted (PRD line 84–85: at most one active coordinated simulation per mutable participant) |
| R3-TC-04d **[PLAN]** | Standalone evolve/reset prevented while coordinated | Prepared connected network | Attempt `obj.evolve_once()` and `obj.reset()` directly on a coordinated participant; also attempt a write onto a *connected* object input via `set_input` | Each attempt is explicitly prevented/rejected rather than silently invalidating snapshots (PRD lines 86–87; architecture-plan line 81); unconnected-input writes may still stage pending values through the controlled boundary (positive sub-assertion); read access remains safe; after reset/detach the standalone operations work again |
| R3-TC-04e **[PLAN]** | Reentrancy guard | Prepared network with a callback that attempts a control write / second step from inside a tick | Run a transition with the reentrant callback | The nested operation is rejected or deferred per the documented reentrancy rule (architecture-plan line 82: mid-tick/reentrant control writes guarded); no corruption of the in-flight snapshot; original user exception behavior documented |

## R3-AC-05 — valid safe connections

**Purpose:** endpoint/direction/value compatibility and single-driver
rules; safe fan-out copies; ndarray isolation; unconnected inputs retain
values; invalid graph edits are atomic. Baseline: no connection API
(baseline §3, GAP row 8).

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| R3-TC-05a **[PLAN]** (data-driven) | Endpoint/direction validation | Station with objects and mock endpoints | Attempt connections with: unknown source/destination names, non-endpoint names (an input offered as source, an output offered as destination), wrong-direction mock endpoints (measurement as source, control as destination) | Each invalid endpoint/direction fails explicitly before any graph change; after each rejection the graph is bit-identical to before (atomic — PRD lines 71–73) |
| R3-TC-05b **[PLAN]** | Single-driver per input | Prepared network; input `u` of object B already driven by A | Attempt a second connection from C to the same input `u` | Explicit rejection (no implicit summation, coercion or conflict resolution — PRD line 71); the original A→B edge remains intact and functional |
| R3-TC-05c **[PLAN]** | Value compatibility validated per destination before commit | Network where a source output's category/dtype/shape does not match a connected destination input's pinned spec (scalar vs ndarray; ndarray dtype or shape mismatch) — Release 2 output shapes are not pinned, so this must be checked at connection/tick time per design | Attempt the connection; where the design validates at connection time, also construct a case whose incompatibility only appears at tick time (e.g. output shape changes between ticks) | Explicit rejection naming source/destination and the mismatch; when detected at tick time, the tick fails atomically with the prior network committed (cross-ref R3-TC-06b); no partial graph changes in either path (PRD lines 74–76) |
| R3-TC-05d **[PLAN]** | Fan-out copies values safely — ndarray mutation isolation | Prepared fan-out from an ndarray-valued source to two destinations | After a transition, mutate the ndarray obtained from one destination's delivered input (via its getter) | The other destination's delivered value and every committed snapshot/source buffer are unchanged (no aliasing — PRD line 74; baseline §4 anchor 3 semantics extended across participants); ndarray mutation cannot alter another participant or committed snapshot |
| R3-TC-05e **[PLAN]** | Unconnected inputs retain pending/declared values | Prepared network where object D has a declared-but-unconnected input with an initial value | Write a pending value to the unconnected input; run transitions | The unconnected input keeps its staged/declared value across transitions and is never overwritten by the network; unconnected input writes stage pending values through the controlled boundary (PRD line 76; architecture-plan line 83) |
| R3-TC-05f **[PLAN]** | Topology changes require a stopped/reset boundary | Running (started, mid-run) network | Attempt to add/remove an edge mid-run (each documented mutation path) | Explicit rejection; mid-tick mutation is not permitted (PRD line 88); after a stopped/reset boundary the same topology change succeeds |
| R3-TC-05g **[PLAN]** | Invalid graph changes are atomic — combined scenario | Prepared network with several valid edges | In one attempt, perform an invalid multi-edit (e.g. batch containing one invalid edge and one valid edge, per the design's batch API if any); catch | Either the whole batch is rejected with the graph identical to before, or the design has no batch API and each edit is individually atomic; no partial graph state is observable in either case |

## R3-AC-06 — atomic tick/reset

**Purpose:** failures preserve the prior committed network and clock
with original exception identity; pending writes survive failed ticks;
successful reset rebuilds everything; failed reset leaves the prior
network usable. Baseline: only per-object standalone reset exists
(object.py lines 649–704); no coordinated transaction — [PLAN] cases
fail by absent API today.

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| R3-TC-06a **[PLAN]** | Evolve failure preserves committed network and clock; original exception identity | Prepared network; one object's evolve callback raises a unique sentinel exception object at a chosen transition; a control write accepted before the failing attempt | Run transitions to the failure; catch; inspect every participant's committed states, all delivered samples, edge buffers, tick count and sim-time; retry after disarming | `assertIs` on the identical exception object (never caught/wrapped/replaced — PRD line 101); all committed participant states, received samples and station clock are exactly the prior committed values; the pending control write accepted before the attempt **remains pending** and is delivered by the successful retry (PRD lines 99–101) |
| R3-TC-06b **[PLAN]** | Observation/validation failure at tick time is atomic | Network whose object's observe callback (or output validation against a connected destination) fails at a chosen transition | Run to the failure | Identical exception identity; prior committed network and clock preserved; no partial publication of any candidate (architecture-plan lines 44–48: publication invokes no user code and happens only on full success) |
| R3-TC-06c **[PLAN]** | Factory failure during coordinated reset leaves prior committed network usable | Running network; one participant's initial-state factory armed to raise on the next reset attempt | Record committed states/samples/clock; attempt coordinated reset; catch; then run one more transition | The identical factory exception propagates; the prior committed network, samples and clock are intact and **usable** (a subsequent transition behaves exactly as if the reset had never been attempted); external factory side effects are outside the guarantee — the documented limitation is cited (PRD lines 103–105) |
| R3-TC-06d **[PLAN]** | Successful reset rebuilds initial participants, buffers, controls, measurement samples, and clock as one operation | Prepared and stepped network with: staged control writes, delivered measurement samples, evolved states, written-unconnected pending inputs, a feedback edge buffer | Mutate several things from initial; then coordinated reset; inspect everything | Tick returns to 0 and sim-time to 0; participant inputs/states restored to declared initial values (fresh factory invocations where declared); control sources restored to initial values (accepted pending writes do not leak across reset); edge buffers restored to documented initial values; measurement sinks restored to explicitly initialized tick-zero samples; all visible together (architecture-plan lines 65–67); a subsequent re-run reproduces the original trace under deterministic callbacks/factories |
| R3-TC-06e **[PLAN]** | Reset failure atomicity under multi-participant staging | Network with ≥3 participants; factory of a *later-declared* participant armed to raise mid-staging | Attempt reset; inspect earlier-staged participants and clock | No partial reset is visible: every participant remains at its pre-reset committed values; clock unchanged; network fully usable (staging is all-or-nothing) |

## R3-AC-08 (this task's portion) — compatibility and evidence

The worked connected example is **excluded** (R3-004 scope).

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| R3-TC-08a **[BASELINE-READY]** | Pre-implementation regression | Current branch | `python -m unittest discover -s tests -p 'test_*.py'` | 165 tests, OK, exit 0 — matches `r3-003-baseline.md` §1; recorded pre-implementation |
| R3-TC-08b **[PLAN]** | Post-implementation regression + gates | R3-003 implementation on this branch | Re-run unittest; `python -m compileall -q softlab`; import smoke; `-W error::Warning` full-suite gate | All pre-existing 165 tests pass unmodified; new R3-003 cases pass; zero warnings; compile exit 0; import clean; no OBS-004/005/006 or DEFECT-2 warning fires (any firing recorded verbatim and dispositioned) |
| R3-TC-08c **[PLAN]** | Standalone compatibility beyond new capability | Implementation present | Existing `test_tu_simulation*.py`, `test_tu_station_objects.py`, `test_tu_contracts.py`, `test_tu_descriptions.py`, `test_tu_lifecycle.py` pass unmodified; standalone probes of baseline §4 anchors 1–7 (evolve/observe contract, exception identity, copy boundaries, membership, delegation precedence) | Standalone object/device/theory behavior and imports compatible except the R3-001-corrected gaps; no coordinated machinery fires without explicit opt-in — ordinary `Station(name)` + device use needs no simulation argument (baseline §4 anchors verified live again post-implementation) |
| R3-TC-08d **[PLAN]** | Opt-in property — no automatic device execution | Implementation present | Register a real (non-mock) device with parameters; assemble an ordinary station; attempt ordinary operations; verify no simulation code executes arbitrary device parameter hooks; verify mock participation is opt-in only | Device signal participation is opt-in for mock signals; no automatic execution of arbitrary device parameter hooks; ordinary device snapshot/describe contracts unchanged (PRD line 160; architecture-plan lines 22–27) |
| R3-TC-08e **[PLAN]** | No new required dependencies / scope | Implementation present | `git diff dev...HEAD --stat -- softlab/huo softlab/jin softlab/shui softlab/mu`; `git diff dev...HEAD -- pyproject.toml setup.py` | No production runtime changes outside `softlab/tu/`; no new required dependencies; Python-support range unchanged |
| R3-TC-08f **[PLAN]** | Candidate CI evidence | Candidate commit pushed | CI run on the task branch (Python 3.9 and 3.13 matrix jobs) green | Recorded run ID, branch, head SHA and job conclusions in the results document — regression, compile/import and warning evidence precede integration (R3-001 REC-1 lesson: cite the pre-merge task-branch run) |

## Traceability summary

| AC (R3-003 scope) | Test cases |
| --- | --- |
| R3-AC-01 — uniform delayed network | R3-TC-01a–01e (automated) |
| R3-AC-02 — initial state and device timing | R3-TC-02a–02f (automated) |
| R3-AC-03 — shared clock | R3-TC-03a–03f (automated); CHK-03-1 (binding/numeric-limit documentation citations) |
| R3-AC-04 — connected removal & shared-active-ownership (remaining clause) | R3-TC-04a–04e (automated); R3-AC-04 regression portion: R3-TC-08a/08c |
| R3-AC-05 — valid safe connections | R3-TC-05a–05g (automated) |
| R3-AC-06 — atomic tick/reset | R3-TC-06a–06e (automated) |
| R3-AC-08 — compatibility portion | R3-TC-08a–08f (automated); CHK-03-1 (reviewer checklist) |
| (out of scope, R3-004) | worked connected example; user guide; release-wide acceptance — **not planned here** |

Manual vs automated, explicitly: automated `unittest` cases are
R3-TC-01a–06e and R3-TC-08a–08f; manual record checks are CHK-03-1
(reviewer scope/design-gate checklist, same convention as Release 2
CHK-06-1 / R3-001 CHK-08-1 / R3-002 CHK-04-1):

1. `git diff dev...HEAD --stat -- softlab/huo softlab/jin softlab/shui
   softlab/mu` must be empty; production runtime changes confined to
   `softlab/tu/`; no concrete `huo` process or automatic scheduler code.
2. The five tasks.md design gates (snapshot publication,
   ownership/reentrancy, safe detach, model-dt binding, numeric clock
   limits) must be **proven in the approved detailed design before
   implementation** — CHK-03-1 records the file/line citation of each
   proof; without a proof the corresponding [PLAN] cases above cannot
   pass by construction and the gate is failed, not waived.
3. Documentation citations: model-dt binding semantics and numeric
   clock/roundoff policy doc locations (for R3-TC-03e/03f); tick-zero
   mock initial-sample documentation location (for R3-TC-02d);
   reentrancy-rule and safe-detach documentation locations (for
   R3-TC-04b/04e) — each verified by file/line citation recorded in the
   results document.
4. Reset external-side-effect limitation explicitly documented (for
   R3-TC-06c, PRD line 105).

## Failure-mode coverage (explicit)

| Failure mode | Covering cases |
| --- | --- |
| Conflicting/invalid sim_dt (zero, negative, NaN, ±inf, bool, non-real) | R3-TC-03b |
| Mid-run sim_dt change | R3-TC-03d |
| Clock overflow/precision boundaries | R3-TC-03f |
| Connection cycles (positive: permitted with delay) | R3-TC-01c |
| Invalid endpoints/directions; duplicate drivers | R3-TC-05a, 05b |
| Value incompatibility at connection and at tick time | R3-TC-05c |
| Reset failure atomicity (mid-staging, factory raise) | R3-TC-06c, 06e |
| Tick failure atomicity (evolve, observe/validation) | R3-TC-06a, 06b; clock aspect R3-TC-03c |
| Ownership conflicts (connected removal, dual activation, standalone evolve/reset while coordinated, connected-input write, reentrancy) | R3-TC-04a–04e |
| Pending-write loss across failed tick | R3-TC-06a (survival asserted) |
| ndarray aliasing across fan-out/snapshots | R3-TC-05d |
| Failed tick-zero preparation | R3-TC-02b |
| Non-atomic invalid graph edits | R3-TC-05a, 05g |

## Risks and notes for the design/implementation review

1. **API names are design work.** Placeholder spellings denote
   capabilities. The tester rebinds fixtures to the approved detailed
   design and records the rebind in the results document.
2. **Delay is one tick for every edge** (PRD lines 16–22), including
   feedback; configurable multi-tick delays and solvers are out of
   scope. Tests assert exactly one-boundary delay, never zero.
3. **Shared external side effects cannot gain order-independence**
   (architecture-plan line 52): R3-TC-01d asserts determinism only for
   pure deterministic callbacks, matching the plan's guarantee boundary.
4. **Numeric clock policy is design work** (architecture-plan lines
   77–78): R3-TC-03f asserts against the *documented* representability
   and roundoff policy, whatever the design chooses — the test pins
   "documented and enforced", not a specific representation.
5. **Mock participation is opt-in only** (PRD line 160): R3-TC-08d
   guards against automatic execution of arbitrary device parameter
   hooks; connected controllers read pending sources and connected
   measurements read buffered deliveries, distinct from standalone
   bridge readback (R3-001 semantics — architecture-plan lines 24–27).
6. **Ownership is this task's scope** (tasks.md R3-003 row; PRD
   R3-AC-04 remaining clause): R3-TC-04a–04e are new-work cases, not
   regression; the `rm_object` forward-looking docstring (station.py
   lines 205–208) will be reconciled with the delivered behavior and
   the citation updated in the results document.
7. **Global registry/module state between tests:** coordinator and
   registry fixtures must isolate with the same `patch.dict`/fresh-
   instance discipline as the R3-002 device/object-registry tests; the
   module-global `_default_station` (station.py line 257) must not leak
   coordinated state across tests.
8. **No concrete `huo` process, scheduler or automatic advancement**
   in this task: any test requiring invocation timing policy is out of
   scope; CHK-03-1 enforces this at review.

## Revision history

- **2026-10-11 — Revision 1** (tester: sw-mike): initial plan, written
  against the characterization baseline recorded the same day in
  `r3-003-baseline.md` (branch `codex/r3-003-coordination`, head
  `72379f4`).
