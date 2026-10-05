# Requirements — Release 3: connected simulation preparation

Owner: sw-camille (Product Owner) | Date: 2026-10-05
Status: requirements defined; architect review pending; execution not authorized
Baseline: `dev` at `603ba85`; preparation branch `codex/release-3-preparation`.

## Goal and confirmed direction

An experiment author can assemble simulated objects and mock devices into a
station, connect their signals, and evolve the assembled simulation with one
shared discrete clock. Objects retain inputs, internal states and outputs:
`next_states = evolve(inputs, previous_states)` and `outputs = output(states)`.
The existing callback keyword `observe` need not be renamed merely to express
this conceptual output function.

The user explicitly requires **every connection to carry previous-step values**.
No edge propagates a source's newly computed value within the same tick. The
rule applies equally to object-to-object, mock-device-to-object and
object-to-mock-device connections, including feedback. A shared tick uses one
consistent snapshot, so insertion/traversal order cannot change deterministic
results. Cycles are allowed with this delay; instantaneous feedback solving is
not required.

Station manages simulated objects alongside its existing devices and owns a
shared `sim_dt`. Release 3 supplies coordinated explicit stepping in `tu`.
A future concrete experiment process in `huo` will decide when to advance; that
process, scheduling policy and automatic advancement are **not Release 3 work**.

## Behavioral requirements and bounded decisions

### Clock, initialization and delayed signals

A simulation starts at tick zero, simulated time zero, with declared initial
object inputs/states and initial mock signal values. Initial object outputs
are computed from initial states for the first edge snapshot; this is
initialization/explicit simulation preparation, never an implicit side effect
of station inspection. Failure to produce a valid initial snapshot leaves the
simulation unstarted without a partial clock/network commit.

For a transition from tick `k` to `k+1`, all destinations consume source values
from the same boundary snapshot at `k`; objects evolve from their states at
`k`; the new states, delivered mock observations and clock become visible
together only on success. Newly derived object outputs are available to edges
at the next transition. For example, `A -> B -> C` cannot transmit A's newly
computed output through both edges during one transition. A feedback edge uses
its initialized previous value on the first transition.

A mock control write between ticks updates a pending source value, observable
as control readback immediately; it does not change downstream object state.
The next explicit transition samples that pending source value and delivers
it across its outgoing edge. A mock measurement read returns its most recently
delivered sample: it does not fetch the current source directly, bypass an
edge's delay or advance time. Its tick-zero value must be explicitly initialized
and documented; it must not be confused with the first delivered source value.
Multiple writes before a tick use the final pending value.

`sim_dt` is a strictly positive finite real duration (booleans rejected), shared
by all participants. Simulation use requires an explicit interval; ordinary
station/device use requires no new argument or simulation initialization.
Each successful tick advances time by exactly that interval conceptually
(with documented floating-point limits). Changing the interval after a run
starts is rejected until coordinated reset; inspection/read/write does not
advance the clock. Callbacks keep their input/previous-state contract: time or
dt needed by a model is supplied through explicitly declared model input or
configuration, not an imposed positional callback argument. Binding a model's
declared interval to the station interval must avoid silent disagreement.

### Composition, ownership and failures

Connections target declared compatible signal endpoints. One source may feed
multiple destinations, but an input has at most one connection driver; no
implicit summation, coercion or conflict resolution is introduced. Invalid
endpoints, duplicate drivers and incompatible values fail explicitly without
partial graph changes. Preserve Release 2's scalar/ndarray value categories,
copy boundaries, fixed input/state dtype and shape. Output values must be
validated for each connected destination before commit, since Release 2 does
not pin output shapes. Unconnected inputs keep their pending/declared values.

A station can add, look up, list, build and remove simulated objects, while
existing device operations retain their behavior. New object names cannot
silently shadow devices or another object. Explicit lookup remains available
for names colliding with station methods. Removing a connected participant is
rejected until its edges are removed; removal performs no hardware cleanup.
A mutable participant belongs to at most one active coordinated simulation;
multiple mock controllers/readers may refer to it within that simulation.
Direct standalone evolution/reset of a participant while coordinated must be
prevented or explicitly rejected, rather than silently invalidating snapshots.
Topology changes require a stopped/reset boundary, not mid-tick mutation.

Provide object builders following the **actual** device convention: a builder
has a model identity and `build(name, **kwargs)` constructs an object; a
model-keyed registry supports discovery and station construction. Keep object
and device registries distinct and preserve existing DeviceBuilder behavior
(including its existing registration semantics); do not refactor it into an
unrelated fluent builder. Invalid/failed object builds leave station membership
unchanged. Built instances must have independent mutable state. The detailed
class names, methods and representation remain design work.

A failed tick must preserve all previously committed participant states,
received samples and station clock; pending control writes already accepted
before that attempt remain pending. Original user callback exceptions propagate
unchanged. A coordinated reset restores initial participant values, pending
controls, edge buffers and tick/time zero as one operation; a failed reset
leaves the prior committed network usable and unchanged. Atomicity covers
library-owned simulation state, not external callback/factory side effects.
No real device I/O is part of a tick/reset or automatic station management.

### Remaining Release 2 gaps included in this release

1. **Bridge readback:** correct the documented Device/Parameter bridge so that
   control reads reflect authoritative object inputs after reset, direct input
   changes and writes through another controller. Characterize standalone
   behavior and distinguish it from pending-source readback in a connected run.
   Measurement reads in the new connected mode must obey the edge-delay rule.
2. **Malformed callback keys:** wrong/missing/extra keys, including mixed string
   and non-string keys, must produce the documented `ValueError` rather than an
   incidental sorting `TypeError`; non-mapping results remain `TypeError`.
3. **Reset claims:** remove claims that any successful reset is behaviorally
   identical to fresh construction, or that failed reset guarantees identical
   future observations, when user factories/callbacks have external state.
   Promise preservation/restoration of owned inputs/states only; reproducible
   observations require deterministic callbacks and reproducible factories.
4. **Closure evidence:** reconcile stale Release 2 board statements with its
   committed acceptance/completion records. This is documentation reconciliation,
   not retroactive repair of past evidence or an invented `main` promotion.
   Record verified commit references and distinguish acceptance from promotion.

Existing Release 1 OBS-004/005/006 and DEFECT-2 remain tracked debt. Only the
specific Release 2 gaps above are included for correction; unrelated fixes,
warning waivers or historical reclassification require separate authorization.

## Acceptance criteria

All criteria are Must. These are observable outcomes, not a test plan or API
design; concrete tests and design await task authorization.

| ID | Need | Acceptance condition |
| --- | --- | --- |
| R3-AC-01 | Uniform delayed network | A deterministic chain, fan-out and feedback network produce the specified previous-boundary results regardless of object/device registration or iteration order. No edge uses a newly computed same-tick source. |
| R3-AC-02 | Defined initial state and device timing | Tick-zero object outputs and mock initial samples are documented; the first transition has defined results. Writes stage controls, control readback shows pending values, measurements show delivered samples, and reads/writes never implicitly tick. |
| R3-AC-03 | Shared clock | Valid explicit `sim_dt` controls all coordinated participants; invalid/nonfinite/zero/negative/bool values are rejected. Successful ticks advance one interval; failed ticks do not. Mid-run interval changes fail until reset. Models using dt have a documented consistent binding. |
| R3-AC-04 | Station membership and builders | Existing device use remains compatible. Object membership, explicit lookup, naming, builder discovery/construction, failed-build safety and independent instances work; connected removal and shared-active-ownership conflicts are rejected. |
| R3-AC-05 | Valid safe connections | Endpoint/direction/value compatibility and single-driver rules are enforced. Fan-out copies values safely; ndarray mutation cannot alter another participant or committed snapshot. Unconnected inputs retain their values; invalid graph changes are atomic. |
| R3-AC-06 | Atomic tick/reset | An evolution, observation, validation or factory failure preserves the prior committed network and clock with original callback exception identity. A successful reset rebuilds initial participants/buffers/clock; accepted pending writes survive failed ticks. External side-effect limits are explicit. |
| R3-AC-07 | Close Release 2 gaps | Bridge readback follows reset, direct changes and shared controllers; mixed callback keys give the documented error; reset documentation states deterministic/factory limitations accurately; Release 2 board agrees with verified closure evidence. |
| R3-AC-08 | Compatibility and usable delivery | Existing standalone object/device/theory behavior and imports remain compatible except the explicitly corrected gaps. A worked connected example demonstrates the clock, delayed feedback, mock interaction and reset. Regression, compile/import, warning and candidate CI evidence precede integration; architecture describes implemented capability only after delivery. |

## Scope, evidence and pause

Runtime production changes are restricted to `softlab/tu/`. Tests, user guide,
architecture and process records outside it are necessary to verify and explain
these contracts. No production edits to `huo`, `shui`, `jin` or `mu` are presently
needed; if design discovers a concrete need, report why and the proposed scope
before expansion. No new dependencies or Python-support changes are planned.

Excluded: a concrete `huo` simulation experiment process, scheduler/background
workers, real-time pacing, thread safety, solvers, instant edges, configurable
multi-tick delays, graph persistence, noise framework, physical fidelity claims,
real instrument access and UI. Device signal participation is opt-in for mock
signals, not automatic execution of arbitrary device parameter hooks.

Historical Release 2 acceptance records 135 tests; this preparation does not
claim a fresh run or certify the newly identified gaps. Baseline evidence and
compatibility characterization belong to the later authorized execution phase.
Use the repository's `codex/<task>` -> `dev` -> `main` gates and serial workflow.
UI and hardware gates are not applicable; normal TDD/review/test/principle/CI
and release acceptance gates will apply when execution is authorized.

**Stop after release preparation, before the first task.** All execution tasks
remain Backlog. Do not create first-task tests, test plans, detailed designs or
production changes during this preparation. The architect may review feasibility,
decompose the backlog and document planned architecture. Detailed API decisions
remain pending; no unresolved product choice currently blocks preparation.
