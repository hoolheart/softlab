# Design — SIM-001: deterministic simulated-object foundation

Owner: sw-celeste | Status: APPROVED (sw-jerry, commit `3d6cb13`; review
minors 1–2 closed in this revision — see §11) | Date: 2026-10-05
Source revision: `4fbe97e` on `codex/sim-001-simulation-foundation`
Requirements: [prd.md](../prd.md) (SIM-AC-01–08) |
Approved test plan: [sim-001-test-plan.md](../test/sim-001-test-plan.md) |
Test-plan review: [sim-001-test-plan-review.md](../reviews/sim-001-test-plan-review.md)
(APPROVED; sw-tom's four design-input recommendations adopted — see §9) |
Architecture: [arch.md](../../../arch.md) | Conventions: [AGENTS.md](../../../AGENTS.md)

Scope guard: production changes are restricted to `softlab/tu/` (new
`softlab/tu/simulation/` package plus the `softlab/tu/__init__.py` export line).
Tests, guide and `arch.md` evidence live outside `tu` per the PRD. No `huo`,
`jin`, `shui` or `mu` production changes. Zero new dependencies (stdlib +
NumPy only; NumPy is already a required dependency).

## 1. Component overview

The simulated object represents *the thing being controlled or observed* —
distinct from a mock instrument, which represents *how* an experiment controls
or observes it (PRD goal). One object may be shared by several devices, so
object state never implicitly belongs to any instrument.

A `SimulatedObject` owns three declared variable namespaces:

- **inputs** — externally driven variables (including time/`dt` when a model
  needs them; there is no dedicated clock API),
- **states** — internal variables carried between evolutions,
- **outputs** — names of values derived from state by the observation
  function.

The model author supplies two deterministic callbacks:

- `evolve(inputs, previous_states) -> next_states` — the transition function
  (`x_next = evolve(u, x_previous)`),
- `observe(states) -> outputs` — the output function `G` (`y = G(x_next)`).

Evolution happens **only** on explicit `evolve_once()` calls. Observation
never evolves. `reset()` restores the documented initial condition. All
committed values are defended by copy-in/copy-out (SIM-AC-05).

### Placement and exports

New subpackage `softlab/tu/simulation/` (per the planned architecture in
[tasks.md](../tasks.md)), exported by adding `simulation` to the subpackage
import tuple in `softlab/tu/__init__.py` — matching the existing convention
where `tu/__init__.py` imports subpackages and class names live in the
subpackage initializers. Canonical public path:
`softlab.tu.simulation.SimulatedObject`.

### Static structure

```mermaid
classDiagram
    class SimulatedObject {
        +str name
        +Tuple~str,...~ input_names
        +Tuple~str,...~ state_names
        +Tuple~str,...~ output_names
        +set_input(name: str, value: Any) None
        +get_input(name: str) Any
        +get_state(name: str) Any
        +evolve_once() None
        +observe_outputs() Dict~str, Any~
        +reset() None
        -Dict~str, Any~ _inputs
        -Dict~str, Any~ _states
        -Mapping _input_specs
        -Mapping _state_specs
        -Mapping _input_sources
        -Mapping _state_sources
        -Callable _evolve
        -Callable _observe
    }
    class EvolveCallable {
        <<callback>>
        (inputs, previous_states) -> next_states
    }
    class ObserveCallable {
        <<callback G>>
        (states) -> outputs
    }
    class Device {
        +add_parameter()
    }
    class Parameter {
        +before_set(old, new)
        +before_get(stored) value
    }
    SimulatedObject ..> EvolveCallable : invokes once per evolve_once()
    SimulatedObject ..> ObserveCallable : invokes per observe_outputs()
    Device o-- Parameter
    Parameter ..> SimulatedObject : bridge closures (test/example side,\nno inheritance, no adapter class)
    note for SimulatedObject "No dependency on Device / TheoryModel /\nscheduling / storage. NumPy + stdlib only."
```

`SimulatedObject` is a plain class (no base classes, no mixins). It does not
import anything from `softlab`; the dependency direction is strictly
one-way (bridge closures in tests/examples reference the object).

## 2. Interface definitions (DIP — contract before implementation)

### 2.1 Construction

```python
class SimulatedObject:
    def __init__(
        self,
        name: str,
        inputs: Mapping[str, Union[Any, Callable[[], Any]]],
        states: Mapping[str, Union[Any, Callable[[], Any]]],
        outputs: Sequence[str],
        evolve: Callable[
            [Dict[str, Any], Dict[str, Any]], Mapping[str, Any]
        ],
        observe: Callable[[Dict[str, Any]], Mapping[str, Any]],
    ) -> None: ...
```

- `name` — non-empty `str`; else `ValueError` (mirrors `Parameter`).
- `inputs` / `states` — mappings from variable name to an **initial value
  or a zero-argument factory** producing one. Because the value contract
  (§2.4) admits only exact `int`, exact `float` and exact numeric
  `np.ndarray`, any `Callable` value is unambiguously a factory. Factories
  are invoked once at construction (their first result fixes the variable's
  value specification) and again at every `reset()` — this is the designed,
  documented failure-injection point for the reset atomicity contract
  (§2.5, §6 SIM-TC-05i).
- `outputs` — non-repeating sequence of output names; the names `observe`
  must produce.
- `evolve` / `observe` — the deterministic model callbacks (§2.2).

Declaration validation at construction (SIM-TC-01a/01c):

- `inputs`/`states` must be `Mapping`s, `outputs` a `Sequence` of `str`,
  `evolve`/`observe` callable — else `TypeError`.
- Every variable name (all three roles) must be a non-empty `str` —
  else `ValueError`.
- Names must be unique **across all three roles** (roles remain distinct
  namespaces for access, but a name may not appear in two roles) —
  else `ValueError` naming the duplicate.
- Every initial value (literal or first factory result) must satisfy the
  value contract (§2.4) — else `TypeError` (unsupported category) or
  `ValueError` (non-numeric ndarray dtype).
- Any exception escapes `__init__`; **no partially constructed object
  exists** (standard Python constructor semantics).

The declaration fixes, per variable, a private **value specification**
(`int` | `float` | `(ndarray, dtype, shape)`) recorded from the initial
value. For a **literal-declared** initial value, construction makes **two**
independent copies: a pristine copy retained for `reset()`, and the working
committed value (SIM-TC-05c: later mutation of the caller's container
changes neither). Pristine copies are retained **only** for literal-declared
initial values: for a **factory-declared** variable nothing is retained to
copy — the initial value is rebuilt by re-invoking the factory at each
`reset()` (§2.5, §3.2).

### 2.2 Callback contracts

```python
EvolveCallback = Callable[
    [Dict[str, Any], Dict[str, Any]],  # inputs, previous_states
    Mapping[str, Any],                 # next_states
]
ObserveCallback = Callable[
    [Dict[str, Any]],                  # states
    Mapping[str, Any],                 # outputs
]
```

- Both callbacks receive **fresh defensive copies** of the current stores;
  they may mutate or retain their arguments without any effect on committed
  state (SIM-TC-05e).
- `evolve` must return a mapping whose keys are **exactly** the declared
  state names (missing/extra keys: `ValueError` naming the discrepancy; a
  non-mapping return: `TypeError`). Each returned value is validated against
  the state variable's specification (§2.4) and **copied in** before commit.
- `observe` must return a mapping whose keys are exactly the declared output
  names (same error rules); values are validated against the value contract
  (no per-output fixed spec — outputs are derived data; category and ndarray
  numeric-dtype checks apply) and copied out.
- **Memoryless idiom**: a memoryless model declares a holding state whose
  `evolve` copies the current input (`{'held': u['u']}`), ignoring
  `previous_states`; `observe` computes the output from `held`. This is the
  documented way to obtain input-dependent outputs with a state-derived `G`
  (SIM-TC-02c); direct input→output feedthrough is out of scope (§7).
- **Time/dt**: models that need time declare it as an ordinary input (e.g.
  `dt`) and/or a state (e.g. `t`, advanced by `evolve` as
  `t_next = t + dt`). No clock API, no wall-clock access anywhere in the
  object (SIM-TC-02d, SIM-TC-03d).
- **Determinism** is the model author's contract (PRD): callbacks with
  external mutable state, random generators or I/O are permitted but
  caller-managed; external side effects are outside every rollback
  guarantee (SIM-TC-05g — user guide must state this).

### 2.3 Public API

Declaration inspection (read-only, never invokes callbacks):

```python
    @property
    def name(self) -> str: ...
    @property
    def input_names(self) -> Tuple[str, ...]: ...   # declaration order
    @property
    def state_names(self) -> Tuple[str, ...]: ...
    @property
    def output_names(self) -> Tuple[str, ...]: ...
```

Input API:

```python
    def set_input(self, name: str, value: Any) -> None: ...
    def get_input(self, name: str) -> Any: ...
```

- `set_input` — validates `value` against the variable's specification and
  copies it in, then **atomically replaces** that one entry of the input
  store (validate-and-copy fully precede the store mutation). Never invokes
  `evolve` and never changes state or outputs (SIM-TC-03c). The new input
  takes effect at the next `evolve_once()` — **pending-input publication
  decision (open question 1): inputs are applied at the evolve boundary**;
  there is exactly one input store, and "pending" means "stored but not yet
  consumed by an evolution". Inputs persist across evolutions (they
  represent a drive level, not a one-shot message).
- `get_input` — returns a **copy** of the current stored input. After a
  failed `set_input`, returns the previous value (SIM-TC-05a).
- Unknown `name`: `KeyError` naming the variable (SIM-TC-01d).

Evolution API:

```python
    def evolve_once(self) -> None: ...
```

- Invokes the `evolve` callback **exactly once** per call, with copies of
  the current input store and current state store
  (`x_next = evolve(u, x_previous)`), validates and copies in the returned
  next-state mapping, then commits by swapping the state store
  (build-then-swap, §3). Never invokes `observe`. Returns `None`; read
  results through `get_state`/`observe_outputs`.

Observation/inspection API (read-only, **never** evolves):

```python
    def get_state(self, name: str) -> Any: ...
    def observe_outputs(self) -> Dict[str, Any]: ...
```

- `get_state` — returns a copy of the named committed state. Pure: no
  callback invocation, no mutation (SIM-TC-03a). Unknown name: `KeyError`.
- `observe_outputs` — invokes `observe` (G) **once per call** with a copy
  of the committed state, validates and copies out the results, and returns
  them in a fresh `dict` (a new dict object with fresh values on every
  call; SIM-TC-05d). Never touches the input or state stores, so repeated
  calls are stable under deterministic `G` (SIM-TC-03b). Observation is
  **all-outputs-at-once** by design (single `G` per PRD wording "an output
  function"); there is no per-output observation method (see §6,
  SIM-TC-05h binding).

Reset API:

```python
    def reset(self) -> None: ...
```

- Rebuilds the complete initial condition (inputs and states) by
  build-then-swap (§3.3): fresh copies of pristine literal initial values,
  fresh factory invocations for factory-declared variables. Never invokes
  `evolve` or `observe`. On success the object is behaviorally identical to
  a newly constructed one (SIM-TC-04a), including any time inputs/states.

### 2.4 Value contract (SIM-AC-01, SIM-AC-05)

Supported value categories — **closed list**, enforced identically for
initial values, `set_input`, `evolve` results and `observe` results:

| Category | Accepted types | Copy rule |
| --- | --- | --- |
| Integer scalar | exact `int` (`type(v) is int`) | immutable — passed as-is |
| Float scalar | exact `float` (`type(v) is float`) | immutable — passed as-is |
| Numeric array | exact `np.ndarray` (`type(v) is np.ndarray`) with `np.issubdtype(v.dtype, np.number)` | `np.array(v, copy=True)` on every boundary crossing |

Deliberately **rejected** categories (SIM-TC-01e, SIM-TC-05f), each with a
documented error and message naming the variable:

- `bool` (subclasses `int` but is not an exact `int`) — `TypeError`;
- NumPy scalar types (`np.float64`, `np.int64`, …) — `TypeError`; the
  message directs model authors to `float(v)`/`int(v)`/`.item()`;
- non-exact `np.ndarray` subclasses, `list`/`tuple`/other sequences,
  `str`, `None`, dicts, arbitrary objects — `TypeError`;
- object-dtype or other non-numeric ndarrays (this covers ragged sequences,
  which cannot exist as exact numeric ndarrays) — `ValueError`.

Per-variable specification pinning:

- Scalar variables accept only their declared scalar category (an `int`
  input cannot later receive a `float` and vice versa) — mismatch:
  `TypeError`. No implicit conversion anywhere.
- ndarray variables pin **dtype and shape** from the initial value; later
  values must be exact ndarrays with identical dtype and shape — category
  mismatch `TypeError`, dtype/shape mismatch `ValueError` naming the
  variable and the expected/actual dtype/shape.

**Copy-in/copy-out (ownership/aliasing, SIM-AC-05)** — one uniform
mechanism (adopting sw-tom recommendation 2):

- *In* (construction, `set_input`, `evolve` result, `observe` result):
  every ndarray value is copied before it can touch committed storage or
  escape as a result.
- *Out* (`get_input`, `get_state`, `observe_outputs` return): every
  ndarray value is a fresh copy; no returned object aliases committed
  storage.
- *Callback arguments*: `evolve`/`observe` receive fresh copies that are
  discarded after the call; the object never retains aliases to objects it
  handed to callbacks (SIM-TC-05e).
- Scalars (`int`/`float`) are immutable and need no copying.
- Consequence for SIM-TC-04c: two independently constructed objects share
  no mutable container; all state is per-instance (no module-level mutable
  state exists in the package).

### 2.5 Error model

| Failure site | Error contract |
| --- | --- |
| Invalid declaration (construction) | `TypeError` (wrong argument category: non-mapping, non-callable, bad output sequence) / `ValueError` (empty/duplicate names, unsupported initial value) — no partially constructed object escapes |
| Unknown variable name (`set_input`/`get_input`/`get_state`) | `KeyError` naming the variable |
| Incompatible value (`set_input`, `evolve`/`observe` results) | `TypeError` (wrong category) / `ValueError` (ndarray dtype/shape mismatch, non-numeric dtype) |
| `evolve` result with missing/extra state keys | `ValueError` naming the discrepancy; `TypeError` if not a mapping |
| `observe` result with missing/extra output keys | `ValueError` naming the discrepancy; `TypeError` if not a mapping |
| Exception raised **by the user's `evolve`/`observe` callback or initial-value factory** | The **identical original exception object propagates** — never caught, wrapped or replaced (Release 1 error-identity policy, cf. `Reading.error`). This includes `BaseException` subclasses. |

**Failure atomicity (SIM-AC-05).** Every mutating operation is
build-then-swap (§3): all validation, copying and user-callback execution
complete *before* any committed storage is replaced.

- **Input update**: validate-and-copy precede the single-entry replacement;
  a failed `set_input` leaves the input store, state and outputs exactly as
  before (SIM-TC-05a).
- **Evolution**: a raised callback or an invalid next-state mapping leaves
  committed state and the input store untouched; nothing is partially
  published (SIM-TC-05b).
- **Observation**: `observe_outputs` performs no mutation at all; a raising
  `G` cannot corrupt anything, prior observations are unaffected, and a
  later observation against a valid state works (SIM-TC-05h).
- **Reset (full contract — resolves SIM-TC-05i-PENDING)**: `reset()`
  executes in two phases:
  1. **Build**: for every input and state variable, in declaration order,
     produce the candidate initial value — re-invoke the factory for
     factory-declared variables, copy the pristine construction-time value
     for literal-declared ones — and validate it against the variable's
     specification. Candidate values accumulate in fresh local dicts.
  2. **Swap**: only after *all* candidates are built and validated, rebind
     the internal input store and state store to the new dicts (two plain
     attribute rebinds; atomic under the single-threaded contract, §7).

  Any exception during the build phase — a raising factory (the documented
  injection point), or a factory result that violates the specification —
  propagates as the **identical original exception object** and leaves the
  object in its **complete, unmodified pre-reset committed state**: inputs,
  states and therefore all subsequent observations are exactly as before
  the call, and the object remains fully usable (`set_input`,
  `evolve_once`, `observe_outputs` and a later `reset` all behave
  normally). No rollback path exists because no mutation precedes the
  swap. External side effects of user factories are outside this guarantee
  (SIM-TC-05g). This is sw-tom recommendation 4 (build-then-swap),
  generalized by the factory mechanism so that the failure branch is
  executable in a test without monkeypatching internals.

### 2.6 Optional time/dt

Ordinary inputs/states only (§2.2). Example: `inputs={'dt': 0.1}`,
`states={'t': 0.0, 'x': 0.0}`,
`evolve = lambda u, x: {'t': x['t'] + u['dt'], 'x': x['x'] + u['dt'] * f(x)}`.
`reset()` restores the declared initial `t`. No dedicated clock API is
added now or reserved; wall-clock time is never read (SIM-TC-03d).

## 3. Algorithms and sequences

### 3.1 set-input → evolve → observe

```mermaid
sequenceDiagram
    participant U as User / mock Parameter
    participant S as SimulatedObject
    participant E as evolve callback
    participant G as observe callback (G)
    U->>S: set_input(name, value)
    S->>S: KeyError if unknown; validate against spec;\ncopy ndarray in
    S->>S: replace single input-store entry
    Note over S: NO evolve, NO state/output change
    U->>S: evolve_once()
    S->>S: u = copies of inputs; x = copies of states
    S->>E: evolve(u, x)  [exactly once]
    E-->>S: next_states mapping
    S->>S: validate keys == state_names;\nvalidate+copy each value
    S->>S: swap state store (commit)
    U->>S: observe_outputs()
    S->>S: x = copies of committed states
    S->>G: observe(x)
    G-->>S: outputs mapping
    S->>S: validate keys == output_names; copy values out
    S-->>U: fresh dict of output copies
```

**evolve-once semantics**: the `evolve` callback is invoked exactly once
per `evolve_once()` call and from nowhere else — not by construction, not
by `set_input`, not by inspection, not by `observe_outputs`, not by
`reset`. Testable by a counter-wrapped callback (SIM-TC-02a/02b/03a/03c).

### 3.2 reset (build-then-swap)

```mermaid
sequenceDiagram
    participant U as Caller
    participant S as SimulatedObject
    participant F as initial-value factory (if declared)
    U->>S: reset()
    loop each input, then each state (declaration order)
        alt factory-declared
            S->>F: invoke factory
            F-->>S: fresh value (or raise)
        else literal-declared
            S->>S: copy pristine construction-time value
        end
        S->>S: validate against variable spec; copy in
        Note over S: candidates accumulate in LOCAL dicts;\ncommitted stores untouched
    end
    S->>S: rebind input store and state store (swap)
    Note over S: never invokes evolve or observe
```

### 3.3 Failure paths

```mermaid
sequenceDiagram
    participant U as Caller
    participant S as SimulatedObject
    participant C as callback / factory
    Note over U,S: (a) failed input update
    U->>S: set_input(name, bad_value)
    S->>S: validation raises before any store mutation
    S-->>U: TypeError/ValueError; input store keeps previous value
    Note over U,S: (b) failed evolution
    U->>S: evolve_once()
    S->>C: evolve(u_copy, x_copy)
    C--xS: raises E
    S-->>U: identical E propagates; state store never swapped
    Note over U,S: (c) failed observation
    U->>S: observe_outputs()
    S->>C: observe(x_copy)
    C--xS: raises E
    S-->>U: identical E propagates; nothing was mutated
    Note over U,S: (d) failed reset
    U->>S: reset()
    S->>C: factory() during build phase
    C--xS: raises E
    S-->>U: identical E propagates; pre-reset committed state intact;\nobject fully usable; later reset may succeed
```

### 3.4 Complexity

All operations are O(V · C) where V is the number of variables touched and
C the cost of copying the largest value; no algorithmic subtlety. The
commit/swap steps are O(1) rebinds. No caching anywhere (observations
recompute `G` per call — deterministic `G` makes this stable, SIM-TC-03b).

## 4. Bridge design — existing Device/Parameter and huo integration

The bridge is a **documented closure pattern**, not new production code
(adopting sw-tom recommendation 3: no adapter class, no `Process`
subclass, zero `huo` edits). It lives in `tests/test_tu_simulation_integration.py`
and the user-guide example.

### 4.1 Mock device wiring

Mock devices are plain `Device` instances (the base device is already
virtual and performs zero I/O — arch.md §Devices). Parameters are plain
`Parameter` instances (not `QuantizedParameter`/`VisaParameter`, avoiding
the OBS-004 subclass-init hazard), constructed **without** `init_value`
and wired via hooks:

- **Input parameter** (control): validator chosen by the example (e.g.
  `ValNumber()`); wire with
  `before_set=lambda old, new: sim.set_input('u', new)`.
  Rationale for `before_set` over `after_set`: `Parameter.set` order is
  permission → validate → decode → `before_set` → store → `after_set`
  (arch.md §Parameters). **Two-gate validation asymmetry** (must be stated
  in the user guide): the parameter's own validator and the object's
  per-variable value specification are **two separate, independent gates**,
  applied in sequence — the parameter validator runs first (the validate
  step of `Parameter.set`), and the object may still reject a value the
  parameter accepted (e.g. a `float` passed to an `int`-pinned input). The
  sim specification is **authoritative**: a sim-spec rejection surfaces as
  the object's `TypeError`/`ValueError` propagating out of
  `Parameter.set`, and because the wiring is in `before_set` it propagates
  *before* the parameter's store changes, so the parameter and the object
  stay consistent and can never disagree. Exactly one `set_input` per
  parameter set; no evolution occurs (SIM-TC-06a, cross-check SIM-TC-03c).
  Parameter read-back via `get()` returns the parameter's stored value,
  which equals the object's current (pending) input — the documented
  contract.
- **Measurement parameter** (observation): wire with
  `before_get=lambda old: sim.observe_outputs()['y']`
  (`before_get`'s return replaces the stored value; the getter therefore
  returns `G(x_committed)`). The read never advances state (SIM-TC-06b).
  This follows the existing precedent in `tests/test_tu_integration.py`
  (`before_get`-based getter wiring).
- **Sharing (SIM-TC-06c)**: a control `Device` and an observation `Device`
  close over the **same** `SimulatedObject` instance. Object state has no
  per-device copies by construction.

Naming discipline for example parameters: choose names that do not collide
with `Device` methods/attributes (OBS-006 avoidance, §7).

### 4.2 Explicit advancement in huo count/scan (SIM-TC-06d/06e)

`count()` and `scan()` already accept `hook_before_set` / `hook_after_set`
/ `hook_before_get` / `hook_after_get` keyword arguments, passed through to
`AtomJob`, invoked synchronously **exactly once per non-dry-run point**;
the terminal dry-run sweep execution invokes **no** hooks. Therefore:

```python
proc = count('acquire', None, None, meas_param,
             times=N, hook_before_get=sim.evolve_once)
proc = scan('sweep', [meas_param], None, None,
            ctrl_param, values, hook_after_set=sim.evolve_once)
success, _ = run_process(proc, get_scheduler(), verbose=False)
```

- `sim.evolve_once` is itself a no-arg synchronous callable — the hook is a
  direct bound method, not even a lambda.
- **Hook binding constraint (from the approved plan):** stepping binds to
  `hook_before_get` (count and scan) or `hook_after_set` (scan only) —
  **never `hook_before_set`**, which fires before the point value is
  committed and would evolve with the previous input (off-by-one against
  the SIM-TC-06e analytic sequence).
- Consequence: evolve count == point count exactly; no wall-clock sleep is
  interpreted as a simulation step (delays default to zero); the run is
  deterministic across identical executions.
- Scheduler discipline per AGENTS.md and the existing test pattern:
  `get_scheduler()` started in `setUp`, stopped in `tearDown`,
  `verbose=False`.

## 5. Module structure

```
softlab/tu/simulation/
    __init__.py   # re-exports SimulatedObject (only public name)
    object.py     # SimulatedObject + private helpers
```

- `object.py` contents: `SimulatedObject` (§2) and module-private helpers
  `_classify_value(value) -> spec`, `_validate_against_spec(name, value,
  spec)`, `_copy_value(value)`. Imports: `typing` names and
  `numpy as np` only. **No `softlab` imports** — the package sits at a
  leaf of the import graph, so a circular import is impossible by
  construction.
- `__init__.py`:

  ```python
  """Deterministic simulated-object foundation"""

  from softlab.tu.simulation.object import SimulatedObject
  ```

- `softlab/tu/__init__.py` gains `simulation` in its subpackage import
  tuple (order: `station`, `simulation`, `theory` — alphabetical, matching
  existing style). No class names are added to `tu/__init__.py`, per the
  existing convention.

**Circular-import check strategy**: (1) structural — `object.py` imports
nothing from `softlab`; (2) executable — SIM-TC-07c imports
`softlab.tu.simulation` directly, imports via `softlab.tu`, and asserts
class identity
(`softlab.tu.simulation.SimulatedObject is softlab.tu.simulation.object.SimulatedObject`),
mirroring the Release 1 `public_exports_retain_class_identity` pattern.

**Dependencies**: none added. NumPy (already required) + stdlib `typing`.
`pyproject.toml` untouched (SIM-TC-07d).

**Public exports** (complete list — nothing else is public):
`softlab.tu.simulation.SimulatedObject`.

## 6. Design-to-test traceability

| Test case | Design element that enables it |
| --- | --- |
| SIM-TC-01a | §2.1 declaration validation; §2.3 `input_names`/`state_names`/`output_names` (declaration order); distinct role namespaces; cross-role duplicate prohibition |
| SIM-TC-01b | §2.4 ndarray category: exact `np.ndarray`, numeric dtype, dtype/shape pinning |
| SIM-TC-01c | §2.1 construction-time `TypeError`/`ValueError` rules; no partially constructed object |
| SIM-TC-01d | §2.5 `KeyError` naming the unknown variable on `set_input`/`get_input`/`get_state` |
| SIM-TC-01e | §2.4 rejected categories (`TypeError`) and dtype/shape mismatch (`ValueError`); committed state unchanged per §2.5 input-update atomicity |
| SIM-TC-02a | §3.1 evolve-once semantics; copies of committed input + previous state passed to `evolve`; commit by swap; `y == G(x_next)` via `observe_outputs` |
| SIM-TC-02b | §2.3 inputs persist across evolutions (drive level); exactly one callback invocation per `evolve_once` |
| SIM-TC-02c | §2.2 memoryless idiom (holding state; `evolve` ignores `previous_states`) |
| SIM-TC-02d | §2.6 dt/time as ordinary inputs/states; no wall-clock access anywhere |
| SIM-TC-02e | §2.2/§3.1: observation derives from committed (post-evolve) state only |
| SIM-TC-03a | §2.3: `get_state`/`observe_outputs`/name properties never invoke `evolve` (counter-checkable) |
| SIM-TC-03b | §3.4: no caching; deterministic `G` on unchanged committed state gives identical results |
| SIM-TC-03c | §2.3 pending-input publication decision: `set_input` stores but never evolves; new input visible via `get_input`; state/outputs reflect last committed evolution |
| SIM-TC-03d | §4.1 getter wiring (`before_get` closure); object performs no I/O and reads no clock (§2.6) |
| SIM-TC-04a | §2.3/§3.2 reset build-then-swap restores declared initial inputs/states (incl. time values) |
| SIM-TC-04b | Deterministic callbacks + copies + explicit evolution ⇒ replay equality (§2.2 determinism contract) |
| SIM-TC-04c | §2.4: construction double-copy; per-instance storage only; no module-level mutable state |
| SIM-TC-04d | §2.5 evolution atomicity (failure leaves no residue) + reset restores initial condition |
| SIM-TC-05a | §2.5 input-update atomicity: validate/copy before single-entry replacement |
| SIM-TC-05b | §2.5 evolution atomicity: identical exception object (never caught); swap never reached; state bit-identical |
| SIM-TC-05c | §2.4 copy-in: caller-supplied and construction-time containers never alias committed storage |
| SIM-TC-05d | §2.4 copy-out: `get_state`/`observe_outputs` return fresh copies/dicts |
| SIM-TC-05e | §2.2/§2.4: callbacks receive discarded copies; returned values copied in before commit |
| SIM-TC-05f | §2.4 closed supported-category list with documented `TypeError`/`ValueError` rejections |
| SIM-TC-05g | §2.2 external-side-effect caveat — user-guide requirement (§6, SIM-TC-08a) |
| SIM-TC-05h | §2.3 all-outputs observation; §2.5 observation atomicity. **Binding decision:** because a raising `G` fails the whole observation call, the "subsequent valid observation" leg binds to the plan's documented alternative — *reset, then observe the initial state* — which §2.3/§3.2 reset supports directly |
| SIM-TC-05i-PENDING | **Resolved — see concrete spec below** (§2.5 reset atomicity contract) |
| SIM-TC-06a | §4.1 `before_set` wiring; exactly one `set_input` per set; no evolution; read-back = pending input |
| SIM-TC-06b | §4.1 `before_get` wiring; read returns `G(x_committed)`; no advancement |
| SIM-TC-06c | §4.1 two devices closing over one object |
| SIM-TC-06d | §4.2 `hook_before_get=sim.evolve_once` on `count`; evolve count == N; no sleeps |
| SIM-TC-06e | §4.2 `hook_after_set` (or `hook_before_get`) on `scan`; never `hook_before_set`; deterministic repeat runs |
| CHK-06-1/2 | §4: zero `huo` production changes by construction (pattern uses only existing `count`/`scan` kwargs and `run_process`) |
| SIM-TC-07a/07b | No changes to existing modules; additive package only |
| SIM-TC-07c | §5 circular-import strategy (structural + identity check) |
| SIM-TC-07d | §5: NumPy + stdlib only; `pyproject.toml` untouched |
| SIM-TC-07e | §7 debt-avoidance rules |
| SIM-TC-08a | Guide topics checklist (below) |
| SIM-TC-08b | §2.2/§2.6 support the accumulating scalar model and ndarray vector-integrator model with exact, reproducible printed output |
| SIM-TC-08c | §2 signatures carry full type annotations; docstring topics enumerated below |
| SIM-TC-08d | Process evidence; not a design element |

### SIM-TC-05i resolution (concrete executable case — replaces the placeholder)

sw-mike shall reclassify **SIM-TC-05i-PENDING → SIM-TC-05i [PLAN]** with
this exact contract:

| Field | Content |
| --- | --- |
| **Case** | Failure atomicity — reset |
| **Preconditions** | Object constructed with at least one factory-declared initial value (e.g. `states={'x': factory}`) where the factory is instrumented to raise `RuntimeError('reset boom')` on a chosen invocation count while returning the valid initial value otherwise. The object has been evolved away from its initial condition. |
| **Steps** | (1) Snapshot committed inputs/states/observations. (2) Arm the factory to raise on its next invocation. (3) Call `reset()`; catch the exception. (4) Read inputs, states and outputs. (5) Disarm the factory; call `reset()` again; observe. |
| **Expected result** | (3) The propagated exception is the **identical** `RuntimeError` object raised by the factory (identity preserved, never wrapped). (4) Inputs, states and observations are exactly the pre-reset committed snapshot — no partially committed reset; `set_input`/`evolve_once`/`observe_outputs` all remain usable. (5) The second `reset()` succeeds and restores the documented initial condition; subsequent observation matches a pristine object. |

This is implementable entirely through the public construction API (the
factory is user-supplied), requires no monkeypatching of internals, and
pins every element of the §2.5 reset atomicity contract.

### User-guide / docstring requirements handed to sw-tom

Guide (`docs/`, name at sw-tom's discretion, e.g. `docs/simulation.md`):
construction, `evolve` contract, observation, reset, error table (§2.5),
ownership/aliasing rules (§2.4), optional time context (§2.6), mock-device
vs simulated-object distinction (§1), supported value contract (§2.4),
deterministic-callback contract incl. the external-side-effects caveat
(SIM-TC-05g), and the §4 bridge pattern with the hook-binding constraint —
including the **two-gate validation asymmetry** (the parameter validator
and the object's specification are two separate gates; the sim
specification is authoritative, and a sim-spec rejection propagates out of
`Parameter.set` *before* the parameter store changes, so parameter and
object stay consistent — §4.1);
executed example = accumulating scalar model + ndarray vector integrator
(SIM-TC-08b). Docstrings: constructor, every public method/property, with
Args/Returns/Errors/Side-effects per repo convention.

## 7. Risks, limitations, deliberate exclusions

**Deliberately out of scope** (PRD exclusions, restated as design
non-goals): direct input→output feedthrough (outputs derive from state
only; §2.2 memoryless idiom covers the practical need); ODE/PDE solvers,
event simulation, automatic wall-clock advancement, background workers,
multi-object scheduling, stochastic/noise frameworks, calibration/fitting,
model persistence, hardware access, UI. Adding any of these requires a new
approved change.

**Threading/reentrancy policy**: the contract is **single-threaded**,
matching the existing `Device` lifecycle contract (arch.md). No locking,
no cross-thread atomicity claims. Callbacks (and factories) **must not**
re-enter the same object (no `set_input`/`evolve_once`/`reset` from inside
`evolve`/`observe`/a factory); reentrancy is unsupported and undocumented
behavior, deliberately not defended — the callbacks receive copies, so
reentrant mutation could not corrupt committed state, but ordering
guarantees would be void. The scheduler integration (§4.2) invokes hooks
synchronously on the scheduler thread, which satisfies the contract for
the single-scheduler test pattern.

**Release 1 debt avoidance** (PRD compatibility section):

- **OBS-004** (subclass init/set-hook hazard): the bridge uses only base
  `Parameter`, constructed without `init_value`; initial inputs are applied
  by a normal `set` after wiring (§4.1).
- **OBS-005** (legacy `TheoryModel.features` fallback): the design does
  not touch `TheoryModel`; no simulation semantics are attached to theory
  models (PRD: do not reinterpret).
- **OBS-006** (delegated-name collisions): example/test parameter names
  must not collide with `Device` attributes/methods; the guide states this.
- **DEFECT-2** (both-permissions-False warning): every bridge parameter is
  settable, gettable, or both — never neither; the suite must remain
  zero-warning.

**Known design limitations to state honestly in the guide**: strict scalar
category pinning means an `int`-declared input rejects floats (declare
`0.0` if float semantics are wanted); ndarray shape/dtype pinning forbids
dynamically resizing state arrays (fixed-size state is the MVP contract);
all-outputs observation means one failing output makes the whole
observation call fail (reset restores observability — SIM-TC-05h binding).

## 8. Implementation notes for sw-tom

- Build order: `object.py` helpers → `SimulatedObject` (construction +
  validation first) → `__init__.py` exports → `tu/__init__.py` line →
  run SIM-TC-07a-style import smoke → bridge example/tests.
- Keep the module free of `softlab` imports; if a reuse temptation appears
  (e.g. `jin.validator`), resist it — the §2.4 contract is a closed
  three-category check, simpler than adapting general validators.
- Black line width 80, target py39; full type annotations; English
  docstrings with Args/Returns/Errors/Side-effects.
- `bool` must be rejected *before* the `int` check can accept it — use
  exact-type comparisons (`type(v) is int`), never `isinstance`.
- The pristine construction-time copies and the working copies must be two
  independent copy operations, not one copy shared by reference.
- Do not add convenience aliases (`step`, `run`, per-output getters,
  setters via `__call__`): §2.3 is the complete public surface.

## 9. Disposition of sw-tom's four design-input recommendations

| # | Recommendation | Disposition |
| --- | --- | --- |
| 1 | Pending until explicit evolution | **Adopted** (§2.3): one input store; assignment stores, `evolve_once` consumes copies; new inputs affect state only at the evolve boundary |
| 2 | Copies in/out uniformly | **Adopted** (§2.4): single copy mechanism at every boundary |
| 3 | Hook binding, zero `huo` edits | **Adopted** (§4.2): bound method `sim.evolve_once` as `hook_before_get`/`hook_after_set`; no adapter, no `Process` subclass |
| 4 | Build-then-swap reset | **Adopted and generalized** (§2.5, §3.2): factory-declared initial values make the reset-failure branch executable without monkeypatching, resolving SIM-TC-05i-PENDING |

## 10. Architect review

sw-jerry: **APPROVED** — review at
`log/release_2/reviews/sim-001-design-review.md` (commit `3d6cb13`), with
three minor items. Items 1 and 2 are closed in this revision (§11); item 3
is a coordinator/sw-mike follow-up (test plan revision 2 reclassifying
SIM-TC-05i-PENDING → SIM-TC-05i [PLAN]) and requires no design change.

## 11. Review closure

sw-jerry's design review
(`log/release_2/reviews/sim-001-design-review.md`, commit `3d6cb13`)
approved this design with three minor items. Closure status:

- **Item 1 [minor] §4.1 — two-gate validation asymmetry — CLOSED.** §4.1
  now states explicitly that the parameter-level validator and the
  simulation-object value specification are two separate, independent
  gates; that the parameter validator runs first (the validate step of
  `Parameter.set`); that the sim specification is authoritative; and that
  a sim-spec rejection propagates as the object's `TypeError`/`ValueError`
  out of `Parameter.set` *before* the parameter store changes (because the
  wiring is in `before_set`), keeping parameter and object consistent. The
  same statement is added to the §6 user-guide topic checklist handed to
  sw-tom. No API change, per the review.
- **Item 2 [minor] §2.1/§2.5 — pristine copies for factory-declared
  variables — CLOSED.** §2.1 now states explicitly that pristine copies are
  retained **only** for literal-declared initial values; factory-declared
  variables retain nothing to copy and are rebuilt by re-invoking the
  factory at each `reset()` (consistent with §2.5 and §3.2, unchanged).
  Editorial only, per the review.
- **Item 3 [minor] §6 SIM-TC-05i handoff — tracked, no design change.**
  Per the review, this is a coordinator/sw-mike scheduling item (test plan
  revision 2 reclassifying SIM-TC-05i-PENDING → SIM-TC-05i [PLAN]); no
  design-document change was requested or made.
