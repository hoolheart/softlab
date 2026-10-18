# Simulated objects (softlab.tu.simulation)

`SimulatedObject` is the deterministic simulated-object foundation of
the `tu` layer. This guide covers construction, the evolve/observation
contract, reset, the error model, ownership/aliasing rules, the
supported value contract, optional time context, the distinction
between a simulated object and a mock device, the deterministic-callback
contract, the `Device`/`Parameter` bridge pattern, known limitations, a
fully executed example, coordinated multi-participant simulation (shared
clock, delayed connections and feedback, mock endpoints, coordinated
reset), and a fully executed connected example.

Public API: `SimulatedObject`, `ObjectBuilder`, `register_object_builder`,
`get_object_builder`, `MockControl`, `MockMeasurement`, `Coordinator`,
`Endpoint` — all exported from `softlab.tu.simulation`.

## 1. Simulated object vs. mock device

The simulated object represents *the thing being controlled or
observed* — a physical quantity, a circuit, a thermal load. A mock
instrument represents *how* an experiment controls or observes that
thing — the driver-level surface of a VISA-like device. The two are
deliberately separate: the object owns model state and knows nothing
about instruments; mock devices close over the object through
parameters (see §9).

One object may be shared by several devices, so object state never
implicitly belongs to any instrument.

## 2. Construction

```python
SimulatedObject(
    name,      # non-empty str
    inputs,    # {name: initial value or zero-argument factory}
    states,    # {name: initial value or zero-argument factory}
    outputs,   # sequence of output names the observe callback produces
    evolve,    # (inputs, previous_states) -> next_states
    observe,   # (states) -> outputs
)
```

The object owns three declared variable namespaces:

- **inputs** — externally driven variables, including time/`dt` when a
  model needs them (there is no dedicated clock API, §8),
- **states** — internal variables carried between evolutions,
- **outputs** — names of values derived from state by `observe`.

Rules enforced at construction:

- every variable name (all three roles) must be a non-empty `str`;
- names must be unique **across all three roles** — a name may not
  appear in two roles (roles remain distinct namespaces for access);
- `inputs`/`states` must be mappings, `outputs` a sequence of `str`,
  `evolve`/`observe` callable;
- every initial value must satisfy the value contract (§6).

An initial value may also be a **zero-argument factory** (any callable
is unambiguously a factory, since the value contract admits only exact
`int`, exact `float` and exact numeric `np.ndarray`). Factories are
invoked once at construction — their first result fixes the variable's
value specification — and again at every `reset()`. This is the
designed failure-injection point for reset atomicity (§5).

Declaration is inspectable without side effects (never invokes
callbacks): `name`, `input_names`, `state_names`, `output_names`
(properties, declaration order).

## 3. The evolve contract

Evolution happens **only** on explicit `evolve_once()` calls:

```python
object.evolve_once()   # invokes evolve exactly once
```

`evolve_once()` copies the current input store and the current state
store, invokes `evolve(u, x_previous)` exactly once with those copies,
validates the returned next-state mapping, and commits by swapping the
state store (build-then-swap, §5). It never invokes `observe`, never
invoked by construction, `set_input`, inspection, `observe_outputs` or
`reset`. It returns `None`; read results through `get_state()` /
`observe_outputs()`.

The `evolve` callback must return a mapping whose keys are **exactly**
the declared state names; each value must satisfy that state's pinned
specification (§6). `evolve` receives fresh defensive copies: it may
mutate or retain its arguments without any effect on committed state.

**Pending inputs.** `set_input(name, value)` validates and stores a new
input but never evolves. There is exactly one input store; "pending"
means "stored but not yet consumed by an evolution". The new input
takes effect at the next `evolve_once()`. Inputs persist across
evolutions — they represent a drive level, not a one-shot message.

**Memoryless idiom.** Outputs derive from state only; direct
input→output feedthrough is out of scope. For input-dependent outputs,
declare a holding state and copy the input in `evolve`:

```python
SimulatedObject(
    'gain', {'u': 0.0}, {'held': 0.0}, ['y'],
    evolve=lambda u, x: {'held': u['u']},   # ignores previous_states
    observe=lambda x: {'y': 10.0 * x['held']},
)
```

## 4. Observation

```python
object.get_state(name)        # copy of one committed state, never evolves
object.observe_outputs()      # fresh dict of all outputs
```

`observe_outputs()` invokes `observe` (G) exactly once per call with a
copy of the committed state, validates that the result keys are exactly
the declared output names and that each value satisfies the value
contract, and returns a new `dict` with fresh values on every call. It
never touches the input or state stores, so repeated calls are stable
under a deterministic `G` (no caching anywhere — `G` is recomputed per
call). Observation is **all-outputs-at-once** by design; there is no
per-output observation method.

## 5. Reset and failure atomicity

```python
object.reset()
```

`reset()` rebuilds the complete initial condition: fresh copies of
pristine construction-time values for literal-declared variables,
fresh factory invocations for factory-declared variables (declaration
order). It never invokes `evolve` or `observe`. On success the owned
input and state stores are restored from their declared sources. This
is not an equivalence to fresh construction: a factory with external
state may return a different value on each reset, and reproducible
observations require deterministic callbacks and reproducible
factories.

`reset()` executes in two phases — **build** (produce and validate
every candidate value into fresh local dicts, committed stores
untouched) then **swap** (rebind the input and state stores). Any
exception during the build phase — a raising factory or an invalid
factory result — propagates as the **identical original exception
object** and leaves the object in its complete, unmodified pre-reset
committed state; the object remains fully usable, and no rollback path
is needed because no mutation precedes the swap. Subsequent
observations are unchanged under a deterministic `observe`; a stateful
`observe` follows its own external state, which is outside this
guarantee.

Every mutating operation is build-then-swap: a failed `set_input`
leaves the input store, state and outputs exactly as before; a raising
`evolve` or an invalid next-state mapping leaves committed state and
the input store untouched; `observe_outputs` performs no mutation at
all.

## 6. Supported value contract and ownership

Supported value categories — a **closed list**, enforced identically
for initial values, `set_input` values, `evolve` results and `observe`
results:

| Category | Accepted types | Copy rule |
| --- | --- | --- |
| Integer scalar | exact `int` (`type(v) is int`) | immutable — passed as-is |
| Float scalar | exact `float` (`type(v) is float`) | immutable — passed as-is |
| Numeric array | exact `np.ndarray` (`type(v) is np.ndarray`) with a numeric dtype | `np.array(v, copy=True)` on every boundary crossing |

Deliberately rejected, each with a documented error naming the
variable:

- `bool` (subclasses `int` but is not an exact `int`) — `TypeError`;
- NumPy scalar types (`np.float64`, `np.int64`, …) — `TypeError`; the
  message directs model authors to `float(v)`, `int(v)` or `.item()`;
- non-exact `np.ndarray` subclasses, `list`/`tuple`/other sequences,
  `str`, `None`, dicts, arbitrary objects — `TypeError`;
- object-dtype or other non-numeric ndarrays — `ValueError` (ragged
  sequences cannot exist as exact numeric ndarrays).

**Per-variable pinning.** The declaration fixes a value specification
per variable. Scalar variables accept only their declared scalar
category — an `int`-declared input rejects floats and vice versa; no
implicit conversion anywhere (declare `0.0` if float semantics are
wanted). ndarray variables pin **dtype and shape** from the initial
value; later values must be exact ndarrays with identical dtype and
shape (category mismatch `TypeError`, dtype/shape mismatch
`ValueError` naming the variable and the expected/actual dtype/shape).

**Ownership/aliasing.** Copy-in/copy-out is one uniform mechanism at
every boundary:

- *in* (construction, `set_input`, `evolve` result, `observe` result):
  every ndarray is copied before it can touch committed storage;
- *out* (`get_input`, `get_state`, `observe_outputs`): every ndarray is
  a fresh copy; no returned object aliases committed storage;
- *callback arguments*: `evolve`/`observe` receive fresh copies that
  are discarded after the call; the object never retains aliases to
  objects it handed to callbacks;
- scalars (`int`/`float`) are immutable and need no copying.

Consequence: two independently constructed objects share no mutable
container; all state is per-instance, and no module-level mutable state
exists in the package.

### Error table

| Failure site | Error contract |
| --- | --- |
| Invalid declaration (construction) | `TypeError` (wrong argument category) / `ValueError` (empty/duplicate names, unsupported initial value) — no partially constructed object escapes |
| Unknown variable name (`set_input`/`get_input`/`get_state`) | `KeyError` naming the variable |
| Incompatible value (`set_input`, `evolve`/`observe` results) | `TypeError` (wrong category) / `ValueError` (ndarray dtype/shape mismatch, non-numeric dtype) |
| `evolve` result with missing/extra state keys | `ValueError` naming the discrepancy; `TypeError` if not a mapping |
| `observe` result with missing/extra output keys | `ValueError` naming the discrepancy; `TypeError` if not a mapping |
| Exception raised by the user's `evolve`/`observe` callback or initial-value factory | the **identical original exception object propagates** — never caught, wrapped or replaced (including `BaseException` subclasses) |

## 7. Deterministic-callback contract

Determinism is the model author's contract: callbacks should be pure
functions of their arguments. Callbacks with external mutable state,
random generators or I/O are permitted but caller-managed. **External
side effects are outside every rollback guarantee** — a raising
factory's file write or network call is not undone by the build-then-
swap atomicity.

## 8. Optional time context

There is no clock API and no wall-clock access anywhere in the object.
Models that need time declare it as an ordinary input (e.g. `dt`)
and/or a state (e.g. `t`, advanced by `evolve` as
`t_next = t + dt`). `reset()` restores the declared initial `t`.
In a coordinated simulation the shared clock is owned by the station
(`sim_dt`) and the coordinator (`tick`/`sim_time`), and a model's
declared interval input can be bound explicitly with
`Coordinator.bind_interval` — see §12.

## 9. Bridging to Device and Parameter (mock devices)

The bridge is a closure pattern — no adapter class, no `Process`
subclass, no `huo` edits. Mock devices are plain `Device` instances
with plain `Parameter` instances (base class only; do not use
`QuantizedParameter`/`VisaParameter`), constructed **without**
`init_value` and wired via hooks. Choose parameter names that do not
collide with `Device` attributes/methods.

**Input parameter (control):**

```python
ctrl = Parameter('drive', ... , validator=ValNumber(),
                 before_set=lambda old, new: sim.set_input('u', new),
                 before_get=lambda stored: sim.get_input('u'))
```

`Parameter.set` order is permission → validate → decode → `before_set`
→ store → `after_set`; `Parameter.get` order is permission →
`before_get` → encode → return. Exactly one `set_input` per parameter
set; no evolution occurs on set or read. Parameter read-back via
`get()` returns the object's **authoritative input store**
(`before_get`'s return replaces the stored value), so the read reflects
the object's current input after direct `set_input` changes, `reset()`
and writes through another controller sharing the same object.

**Standalone vs connected readback.** This section documents the
**standalone** pattern: every control read follows the object's
authoritative input store (this is the R3-001 correction; the read
reflects the object's current input after direct `set_input` changes,
`reset()` and writes through another controller sharing the same
object). In a **connected** (coordinated) run the readback surfaces
differ by design: a `MockControl` read returns the staged **pending**
source value and a `MockMeasurement` read returns the most recently
**delivered** sample obeying the edge-delay rule — see §12. Both
patterns are implemented and both are stated here.

**Two-gate validation asymmetry.** The parameter's own validator and
the object's per-variable value specification are **two separate,
independent gates**, applied in sequence: the parameter validator runs
first (the validate step of `Parameter.set`), and the object may still
reject a value the parameter accepted (e.g. a `float` passed to an
`int`-pinned input). The sim specification is **authoritative**: a
sim-spec rejection surfaces as the object's `TypeError`/`ValueError`
propagating out of `Parameter.set`, and because the wiring is in
`before_set` it propagates *before* the parameter's store changes, so
the parameter and the object stay consistent and can never disagree.

**Measurement parameter (observation):**

```python
meas = Parameter('signal', ...,
                 before_get=lambda stored: sim.observe_outputs()['y'])
```

`before_get`'s return replaces the stored value, so the getter returns
`G(x_committed)`. The read never advances state.

**Sharing.** A control `Device` and an observation `Device` close over
the **same** `SimulatedObject` instance; object state has no per-device
copies by construction.

**Explicit advancement in huo count/scan.** `count()` and `scan()`
accept `hook_before_set` / `hook_after_set` / `hook_before_get` /
`hook_after_get`, invoked synchronously exactly once per non-dry-run
point; the terminal dry-run sweep invokes no hooks. Bind the bound
method directly:

```python
proc = count('acquire', None, None, meas_param,
             times=N, hook_before_get=sim.evolve_once)
proc = scan('sweep', [meas_param], None, None,
            ctrl_param, values, hook_after_set=sim.evolve_once)
```

**Hook binding constraint:** step on `hook_before_get` (count and
scan) or `hook_after_set` (scan only) — **never `hook_before_set`**,
which fires before the point value is committed and would evolve with
the previous input. Evolve count equals point count exactly; no
wall-clock sleep is interpreted as a simulation step (delays default
to zero); identical executions are deterministic. The contract is
single-threaded, matching the existing `Device` lifecycle contract;
callbacks and factories must not re-enter the same object.

## 10. Known limitations

- Strict scalar category pinning: an `int`-declared input rejects
  floats (declare `0.0` if float semantics are wanted).
- ndarray shape/dtype pinning forbids dynamically resizing state
  arrays (fixed-size state is the MVP contract).
- All-outputs observation means one failing output makes the whole
  observation call fail; `reset` restores observability.
- Direct input→output feedthrough is out of scope (use the memoryless
  holding-state idiom, §3).
- Out of scope by design: ODE/PDE solvers, event simulation,
  automatic wall-clock advancement and automatic/background
  multi-object scheduling (explicit coordinated stepping with a shared
  `sim_dt` is covered in §12), background workers,
  stochastic/noise frameworks, calibration/fitting, model persistence,
  hardware access, UI.

## 11. Executed example

The following program was executed end-to-end with the project
installed; its printed output is recorded verbatim and is byte-for-byte
reproducible across runs.

```python
import numpy as np
from softlab.tu.simulation import SimulatedObject

# Accumulating scalar model: x' = u, advanced by explicit Euler.
scalar = SimulatedObject(
    'integrator',
    inputs={'u': 0.0, 'dt': 0.0},
    states={'t': 0.0, 'x': 0.0},
    outputs=['time', 'position'],
    evolve=lambda u, x: {
        't': x['t'] + u['dt'],
        'x': x['x'] + u['dt'] * u['u'],
    },
    observe=lambda x: {'time': x['t'], 'position': x['x']},
)

scalar.set_input('u', 2.0)
scalar.set_input('dt', 0.5)
print('scalar model, u=2.0, dt=0.5:')
for _ in range(4):
    scalar.evolve_once()
    out = scalar.observe_outputs()
    print(f"  t={out['time']:.1f}  x={out['position']:.1f}")

scalar.reset()
print('after reset:', scalar.observe_outputs())

# ndarray vector integrator: v' = A v, explicit Euler.
a = np.array([[0.0, 1.0],
              [-1.0, 0.0]])

def evolve_vec(u, x):
    dt = u['dt']
    return {'v': x['v'] + dt * (a @ x['v'])}

vector = SimulatedObject(
    'oscillator',
    inputs={'dt': 0.0},
    states={'v': np.array([1.0, 0.0])},
    outputs=['vec'],
    evolve=evolve_vec,
    observe=lambda x: {'vec': x['v']},
)

print('vector model, harmonic oscillator, dt=0.1:')
vector.set_input('dt', 0.1)
for step in range(5):
    vector.evolve_once()
    v = vector.get_state('v')
    print(f'  step {step + 1}: v = [{v[0]:.6f}, {v[1]:.6f}]')
```

Actual output:

```text
scalar model, u=2.0, dt=0.5:
  t=0.5  x=1.0
  t=1.0  x=2.0
  t=1.5  x=3.0
  t=2.0  x=4.0
after reset: {'time': 0.0, 'position': 0.0}
vector model, harmonic oscillator, dt=0.1:
  step 1: v = [1.000000, -0.100000]
  step 2: v = [0.990000, -0.200000]
  step 3: v = [0.970000, -0.299000]
  step 4: v = [0.940100, -0.396000]
  step 5: v = [0.900500, -0.490010]
```

## 12. Coordinated simulation

**Composition and ownership.** A coordinated simulation is owned by one
`Coordinator(station)` — exactly one per station, permanent (no
retirement API), strictly opt-in; ordinary station or device use never
constructs one. Simulated objects participate as station members,
enrolled by member name; mock control/measurement endpoints are
coordinator-owned ports, **not** devices and not station members, so no
`Parameter` hook ever fires for them. The lifecycle has two states:
STOPPED (the connection graph is mutable) and RUNNING (after a
successful `prepare()`); a coordinated `reset()` lands back in STOPPED.

**Shared explicit clock.** The shared interval is station-owned:
`station.sim_dt` validates its value (`bool` → `TypeError`, non-real →
`TypeError`, non-finite or non-positive → `ValueError`), coerces through
`float()` (`Decimal` is rejected), and a change while a coordinator is
prepared or mid-operation is rejected with `RuntimeError` until a
coordinated reset. The coordinator's `tick` is an exact, unbounded
`int`; `sim_time == float(tick) * sim_dt` is computed at publication,
never accumulated. The clock is readable only after the first successful
`prepare()`, and a non-finite candidate time fails the tick atomically
with `OverflowError`.

**Delayed edges and feedback.** Connections are `Endpoint` pairs
resolved and validated at `connect` time; each destination has a single
driver, and graph edits are STOPPED-only, validated fully before one
mutation. At every transition k→k+1 each destination consumes the
committed boundary snapshot `S_k` — the source's previous-boundary value
— so no edge propagates a newly computed same-tick value by construction
(publication is the only writer). `S_0` is computed from the initial
states at the explicit `prepare()`; a feedback edge therefore consumes
its initialized previous value on the first transition. Fan-out
delivers an independent copy per destination.

**Mock interaction.** `MockControl.write(value)` stages a pending value
(last write wins); the readback shows the pending value immediately, the
next transition samples and delivers it, the pending persists across
ticks and survives failed ticks, and writes issued while a tick or reset
is in flight are rejected with `RuntimeError`. `MockMeasurement.read()`
returns the most recently delivered sample obeying the edge-delay rule;
the tick-zero sample is the constructor initial, because `prepare()`
delivers nothing to measurements. Reads and writes never tick and never
fetch around the edge delay.

**Interval binding.** `Coordinator.bind_interval(object_name,
input_name)` explicitly binds a model's declared float input to the
shared interval — nothing is inferred from names. Each transition
overlays `float(sim_dt)`; the single-driver rule extends to bindings;
reset restores the declared initial.

**Coordinated reset.** One operation rebuilds every participant's
inputs and states (declared factories re-invoked), republishes `S_0`,
restores control pendings and measurement samples to constructor
initials, zeroes the clock and lands STOPPED. Topology (edges, bindings,
enrollment) is retained; a failure leaves the prior committed network
fully usable; and a re-run (`prepare()` + `step()`) reproduces the trace
under deterministic callbacks and factories. Atomicity covers
library-owned state only — external side effects of user callbacks are
outside every rollback guarantee — and reset is not claimed to equal
fresh construction.

**Connected vs standalone.** §9's bridge remains the standalone
pattern: every control read follows the object's authoritative input
store. Connected participants use the endpoints above instead; while an
object is enrolled, direct `evolve_once()`/`reset()` calls and
`set_input` on driven inputs are rejected until the object is withdrawn.

## 13. Executed connected example

The following program was executed end-to-end with the project
installed; its printed output is recorded verbatim and is byte-for-byte
reproducible across runs.

```python
from softlab.tu.simulation import (
    Coordinator,
    Endpoint,
    MockControl,
    MockMeasurement,
    SimulatedObject,
)
from softlab.tu.station import Station


def make_object(name, initial_x):
    return SimulatedObject(
        name,
        inputs={'u': 0.0},
        states={'x': initial_x},
        outputs=['y'],
        evolve=lambda u, x: {'x': x['x'] + u['u']},
        observe=lambda x: {'y': x['x']},
    )


station = Station('connected-demo')
station.add_object(make_object('A', 1.0))
station.add_object(make_object('B', 10.0))
station.add_object(make_object('C', 100.0))
station.add_object(make_object('D', 0.0))
station.sim_dt = 0.5

drive = MockControl('drive', 2.0)
probe = MockMeasurement('probe', -1.0)

coordinator = Coordinator(station)
for name in ['A', 'B', 'C', 'D']:
    coordinator.enroll(name)
coordinator.add_control(drive)
coordinator.add_measurement(probe)
coordinator.connect(Endpoint('A', 'y'), Endpoint('B', 'u'))
coordinator.connect(Endpoint('B', 'y'), Endpoint('C', 'u'))
coordinator.connect(Endpoint('C', 'y'), Endpoint('A', 'u'))
coordinator.connect(Endpoint('drive'), Endpoint('D', 'u'))
coordinator.connect(Endpoint('C', 'y'), Endpoint('probe'))


def state_line():
    return (
        '  states: '
        f'A.x={station.object("A").get_state("x"):.1f} '
        f'B.x={station.object("B").get_state("x"):.1f} '
        f'C.x={station.object("C").get_state("x"):.1f} '
        f'D.x={station.object("D").get_state("x"):.1f}'
    )


def delivered_line():
    return (
        '  delivered: '
        f'A.u={station.object("A").get_input("u"):.1f} '
        f'B.u={station.object("B").get_input("u"):.1f} '
        f'C.u={station.object("C").get_input("u"):.1f} '
        f'D.u={station.object("D").get_input("u"):.1f}'
    )


print('connected example: chain A -> B -> C, feedback C -> A, '
      'drive -> D, C.y -> probe')
print(f'sim_dt = {station.sim_dt:.1f}')

coordinator.prepare()
print(f'after prepare: tick={coordinator.tick} '
      f'sim_time={coordinator.sim_time:.1f}')
print(state_line())
print(f'  drive pending={drive.read():.1f}  probe={probe.read():.1f}')

coordinator.step()
print(f'step to tick={coordinator.tick} '
      f'sim_time={coordinator.sim_time:.1f}')
print(delivered_line())
print(state_line())
print(f'  probe={probe.read():.1f}')

drive.write(3.0)
print(f'write drive=3.0 between ticks: readback={drive.read():.1f}, '
      f'tick still {coordinator.tick}, '
      f'D.x still {station.object("D").get_state("x"):.1f}')

coordinator.step()
print(f'step to tick={coordinator.tick} '
      f'sim_time={coordinator.sim_time:.1f}')
print(delivered_line())
print(state_line())
print(f'  probe={probe.read():.1f}')

coordinator.reset()
print(f'after reset: tick={coordinator.tick} '
      f'sim_time={coordinator.sim_time:.1f} '
      f'prepared={coordinator.prepared}')
print(state_line())
print(f'  drive pending={drive.read():.1f}  probe={probe.read():.1f}')

coordinator.prepare()
coordinator.step()
print(f're-run after prepare + one step: tick={coordinator.tick} '
      f'sim_time={coordinator.sim_time:.1f}')
print(state_line())
print(f'  probe={probe.read():.1f}')
```

Actual output:

```text
connected example: chain A -> B -> C, feedback C -> A, drive -> D, C.y -> probe
sim_dt = 0.5
after prepare: tick=0 sim_time=0.0
  states: A.x=1.0 B.x=10.0 C.x=100.0 D.x=0.0
  drive pending=2.0  probe=-1.0
step to tick=1 sim_time=0.5
  delivered: A.u=100.0 B.u=1.0 C.u=10.0 D.u=2.0
  states: A.x=101.0 B.x=11.0 C.x=110.0 D.x=2.0
  probe=100.0
write drive=3.0 between ticks: readback=3.0, tick still 1, D.x still 2.0
step to tick=2 sim_time=1.0
  delivered: A.u=110.0 B.u=101.0 C.u=11.0 D.u=3.0
  states: A.x=211.0 B.x=112.0 C.x=121.0 D.x=5.0
  probe=110.0
after reset: tick=0 sim_time=0.0 prepared=False
  states: A.x=1.0 B.x=10.0 C.x=100.0 D.x=0.0
  drive pending=2.0  probe=-1.0
re-run after prepare + one step: tick=1 sim_time=0.5
  states: A.x=101.0 B.x=11.0 C.x=110.0 D.x=2.0
  probe=100.0
```

For a complete headless run connecting coordinated simulation, `huo` count hooks,
HDF5 save/reopen verification and a PNG, follow the
[simulated thermal experiment](thermal-experiment.md). It documents explicit
measurement latency, fresh output paths and the legacy HDF5 reader limitations.
