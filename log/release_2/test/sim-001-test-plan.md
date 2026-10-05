# SIM-001 test plan — deterministic simulated-object foundation

Tester: sw-mike | Date: 2026-10-05
Branch: `codex/sim-001-simulation-foundation`
Requirements: `log/release_2/prd.md` (SIM-AC-01 through SIM-AC-08)
Baseline: see `sim-001-baseline.md` (99 tests OK, exit 0, no warnings).

## Status of this document

This is a **test plan only**. Every case below is marked:

- **[PLAN]** — design input for sw-celeste/sw-tom; requires implementation to
  exist before it can be executed (deferred to the testing phase).
- **[BASELINE-READY]** — can be executed against the current baseline without
  any new production code (SIM-AC-07 compatibility re-run and the
  pre-implementation import sanity parts of SIM-AC-07).

The exact public API names are intentionally not hard-coded: the detailed
design is not yet written. Test steps refer to *roles* (declared inputs,
internal state, outputs, `evolve`, observation, reset) per the PRD contract
`x_next = evolve(u, x_previous)`, `y = G(x_next)`. When the design lands,
these steps will be bound to the approved names. All cases are implementable
as `unittest`-compatible tests under `tests/`.

## Planned test files

| File | Covers |
| --- | --- |
| `tests/test_tu_simulation.py` | SIM-AC-01, SIM-AC-02, SIM-AC-03, SIM-AC-04, SIM-AC-05 (object-level unit tests) |
| `tests/test_tu_simulation_integration.py` | SIM-AC-06 (Device/Parameter bridge + huo count/scan integration) |
| `tests/test_sim_user_guide.py` | SIM-AC-08 (executed deterministic user-guide example) |
| (no new file) | SIM-AC-07 re-runs the existing suite `python -m unittest discover -s tests -p 'test_*.py'` plus an import check for the new public API |

Fixtures/common helpers (e.g., a deterministic accumulating scalar model, a
memoryless model, an ndarray-valued model) will be defined inside these test
files; no changes to existing test files are planned.

---

## SIM-AC-01 — distinct input/state/output roles, scalar and ndarray value contract

**Purpose:** verify the object exposes declared input, state and output
variables; documented scalar and ndarray examples work; invalid declarations,
unknown variables and incompatible values raise documented errors.

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| SIM-TC-01a **[PLAN]** | Scalar declaration contract | Deterministic scalar accumulating model defined (e.g., `x_next = x + u`) | Construct object with declared input `u`, state `x`, output `y = G(x)`; inspect declarations | Declarations enumerate exactly the declared variables per role; roles are distinct namespaces; initial values per documented defaults |
| SIM-TC-01b **[PLAN]** | ndarray value contract | ndarray-valued model (e.g., vector integrator `x_next = x + u*dt` with `u`, `x` as float ndarrays) | Assign ndarray inputs; evolve; observe outputs | ndarray values accepted per documented value contract; output dtype/shape match documented behavior (numeric ndarray, no object arrays unless documented) |
| SIM-TC-01c **[PLAN]** | Invalid declaration errors | — | Attempt declarations that violate the documented rules (e.g., undeclared-type variable, duplicate name across roles if prohibited, non-numeric value where numeric required) | Each invalid declaration raises the documented error type with a useful message; no partially constructed object escapes |
| SIM-TC-01d **[PLAN]** | Unknown-variable errors | Constructed object | Attempt to read/write a name that was never declared, in each role | Documented error (e.g., `KeyError`-family) naming the unknown variable |
| SIM-TC-01e **[PLAN]** | Incompatible-value errors | Constructed object with declared typed inputs | Assign values of an incompatible category per the documented supported-value rules (e.g., string to a numeric input, ragged/object array if unsupported) | Documented validation error; committed state unchanged (see SIM-TC-05a) |

## SIM-AC-02 — dynamic and memoryless evaluation; evolve-once per explicit evolution

**Purpose:** deterministic accumulating model evolves via `evolve` once per
explicit evolution; outputs derive from resulting state; memoryless model
works; time/dt optionally supplied as input/context.

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| SIM-TC-02a **[PLAN]** | Accumulating evolve semantics | Scalar accumulator, `x=0`, callback counter on `evolve` | Assign `u=1`; request one explicit evolution | `evolve` invoked exactly once with the committed input and previous state; `x` becomes `1`; output reflects resulting state (`y == G(1)`) |
| SIM-TC-02b **[PLAN]** | Sequence accumulates deterministically | Same model | Evolve 3 more times with `u=1` | After each evolution `x` increments by 1; after 4 evolutions `x == 4`; `evolve` called exactly 4 times total |
| SIM-TC-02c **[PLAN]** | Memoryless model | Model whose outputs depend only on current inputs (no previous state; `evolve` may ignore `x_previous` or return documented identity) | Assign input; evolve; observe; repeat with different input | Outputs depend only on current input, not history; repeated same input reproduces same output |
| SIM-TC-02d **[PLAN]** | dt as input/context | Model documented to consume a dt input or optional context | Supply dt explicitly; evolve twice with same dt | Result matches analytic expectation; no hidden wall-clock dt is used; results are byte-for-byte reproducible across runs |
| SIM-TC-02e **[PLAN]** | Output from resulting state | Model where `y` differs from `x` (non-identity observation) | Assign input; evolve | Observed output equals `G(x_next)`, not `G(x_previous)` |

## SIM-AC-03 — read without unintended evolution

**Purpose:** inspecting state/outputs/declarations never invokes `evolve`,
never changes committed state, never touches hardware; repeated observation
is stable; input assignment alone does not evolve.

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| SIM-TC-03a **[PLAN]** | Inspection does not invoke evolve | Object with counter-wrapped `evolve` callback | Read state, read each output, read declarations | `evolve` call count unchanged (zero); committed state unchanged |
| SIM-TC-03b **[PLAN]** | Repeated observation stability | Object after some evolutions | Observe outputs twice without any evolution in between | Two observations identical and both equal to `G(x_committed)` |
| SIM-TC-03c **[PLAN]** | Input assignment alone does not evolve | Counter-wrapped `evolve` | Assign a new input value; do NOT request evolution; read state and outputs | `evolve` count unchanged; state and outputs still reflect the last committed evolution; the new input is visible as a pending input only (per documented contract) |
| SIM-TC-03d **[PLAN]** | No hardware/scheduler access on read | Object wired to mock device (SIM-AC-06 fixture) | Read via parameter getters | No I/O, no scheduler involvement, no wall-clock dependency; reads succeed with no event-loop work |

## SIM-AC-04 — repeatable preparation: reset and replay reproducibility

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| SIM-TC-04a **[PLAN]** | Reset restores initial condition | Accumulator evolved several times from documented initial inputs/state | Call reset | Inputs, state and outputs return to documented initial values (including any time values represented there); subsequent observation matches the pristine initial object |
| SIM-TC-04b **[PLAN]** | Replay reproduces observations | Reset model; deterministic input/evolve sequence recorded | Replay the identical input assignment + explicit evolution sequence | Every observation in the replay equals the corresponding observation in the original run (exact equality for deterministic callbacks) |
| SIM-TC-04c **[PLAN]** | Independent objects do not share mutable state | Two objects constructed independently from the same declaration/model (each with its own initial mutable containers, e.g., ndarray state) | Evolve object A only; read state/outputs of B | B's committed state/outputs unchanged; mutating an inspection value returned from A (SIM-TC-05c) does not affect B; no shared mutable container between A and B (verified by identity checks where inspection allows, plus behavioral checks) |
| SIM-TC-04d **[PLAN]** | Reset after failed evolution | Object where `evolve` raises for a specific input (see SIM-TC-05b) | Trigger failed evolution; then reset | Reset succeeds and restores the documented initial condition; the failure leaves no residue (SIM-TC-05b) |

## SIM-AC-05 — predictable failures, atomicity, and ownership (mutable-alias protection)

**Purpose:** failures are visible with original exceptions preserved; failed
input update / evolve / reset leaves no partially committed state; mutable
values supplied by callers, returned by inspection, or passed to callbacks
cannot silently mutate committed state; unsupported value categories are
clearly rejected.

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| SIM-TC-05a **[PLAN]** | Failure atomicity — input update | Object with valid committed input/state; a validation rule that rejects value `v_bad` | Attempt to assign `v_bad`; catch exception; read state, outputs, and the previously valid input | The original exception (or its documented wrapper chaining it via `__cause__`) propagates; committed state and outputs unchanged; the input retains its previous committed value |
| SIM-TC-05b **[PLAN]** | Failure atomicity — evolve | Model whose `evolve` raises `RuntimeError("boom")` for a specific input; snapshot committed state before | Assign triggering input; request evolution; catch; compare state/outputs to snapshot | Exactly the original exception object (identity preserved, not swallowed/replaced) propagates; committed state and outputs bit-identical to snapshot; the failed next-state is not partially published |
| SIM-TC-05c **[PLAN]** | Mutable-alias protection — caller-supplied values | Input/state declared with ndarray values | Build ndarray `u`; assign it; mutate `u` in place; read the object's committed input; also mutate an initial-condition container passed at construction | Committed values are copies (or immutable equivalents per design): in-place mutation of caller containers never changes committed state |
| SIM-TC-05d **[PLAN]** | Mutable-alias protection — returned inspection values | Object with ndarray state committed | `r = inspect_state()`; mutate `r` in place (e.g., `r[:] = 999`); re-inspect state; also `o = observe()`; mutate `o`; re-observe | Re-inspection shows the committed value unchanged; no inspection return value is an alias of committed storage |
| SIM-TC-05e **[PLAN]** | Mutable-alias protection — callback arguments | Model whose `evolve` mutates its arguments in place (adversarial callback) or retains a reference and mutates later | Evolve once with ndarray input/state; after evolution completes, mutate the retained argument via the callback's reference | Committed state is unaffected: the object must not retain aliases to arguments it handed to callbacks (copies out before publish; exact mechanism per design) |
| SIM-TC-05f **[PLAN]** | Unsupported value category documented | — | Attempt an explicitly unsupported category (e.g., object arrays, ragged sequences, or non-numeric scalars, per the design's documented supported-value rules) | Clear documented rejection (error type documented in API docs); behavior for that category is stated in the user guide |
| SIM-TC-05g **[PLAN]** | External side effects out of rollback scope — documentation check | — | Review user guide/API docs | Guide states that external side effects of user callbacks are outside rollback guarantees (honest-bounds documentation per SIM-AC-08) |

## SIM-AC-06 — existing experiment interfaces (Device/Parameter bridge + huo integration)

**Purpose:** a synthetic worked example wires declared inputs/measurements to
existing `softlab/tu` `Device`/`Parameter` interfaces; one object can be
shared between controls and observations; explicit advancement occurs in a
defined hook; an existing `huo` count or scan path runs **without editing
`huo`**; no real sleeps are interpreted as simulation steps.

Design dependency: the detailed design must define where explicit
advancement occurs. **Planned assertion (design-time input):** advancement
happens in a `huo` process hook that is invoked per measurement point —
candidates are a per-point action on the existing count/scan process (e.g.,
a hook/extension point already offered by the process, or a small custom
`Process` subclass in the *test/example* that wraps stepping) — and the
simulation object's `evolve` is called exactly once per count/scan point.
If the existing count/scan offers no hook, the design must specify the
minimal adapter; the test below then pins that contract. This is a planned
assertion, not yet bound to API names.

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| SIM-TC-06a **[PLAN]** | Parameter bridge for inputs | Mock `Device` built from existing `Device`/`Parameter` interfaces; input parameter setter wired to the simulation object's input assignment | Set input parameter via the parameter's normal set path (validation/codecs/hooks intact) | Object's committed input updates exactly once; no evolution occurs (SIM-TC-03c cross-check); parameter read-back reflects the documented contract (pending vs committed per design) |
| SIM-TC-06b **[PLAN]** | Parameter bridge for observations | Measurement parameter getter wired to object observation | Advance explicitly once; read measurement parameter | Getter returns `G(x_next)`; the read itself does not advance state (counter check) |
| SIM-TC-06c **[PLAN]** | One object shared between controls/observations | Two mock devices (a "control" device writing inputs, an "observation" device reading outputs) bound to the SAME simulation object | Set input via device A; advance via the defined hook; read via device B | Device B observes the state produced by A's input: shared object, no per-device state copies; consistent observations |
| SIM-TC-06d **[PLAN]** | huo count integration with explicit stepping | Existing `huo` count flow (e.g., `count` process) run with the measurement parameter(s) of SIM-TC-06b; advancement placed in the design-defined hook | Run count for N points; count `evolve` invocations; record the observed values | Exactly one `evolve` per count point (the planned assertion above); recorded values equal the deterministic sequence produced by manual replay of the same steps; **no wall-clock sleep is used as a simulation step** (test runs without sleeping; stepping is synchronous in the hook) |
| SIM-TC-06e **[PLAN]** | huo scan integration with explicit stepping | Existing `huo` scan flow over a settable parameter driving a model input | Run scan over k points; count evolutions | One evolution per scan point; recorded outputs match the analytic sequence; run is deterministic across two identical executions |
| SIM-TC-06f **[PLAN]** | No huo edits | — | `git diff` check in test documentation / review of integration path | Integration uses only existing `huo` public API; zero production changes under `softlab/huo/` |

## SIM-AC-07 — compatibility and imports

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| SIM-TC-07a **[BASELINE-READY]** | Fresh baseline regression | Current branch (recorded in `sim-001-baseline.md`) | `python -m unittest discover -s tests -p 'test_*.py'` | Exit code 0; **99 tests, OK**; zero warnings — matching the baseline record; executed pre-implementation and recorded |
| SIM-TC-07b **[PLAN]** | Post-implementation regression | Implementation merged on this branch | Re-run the identical command plus `python -m compileall -q softlab` and `python -c "import softlab; print(softlab.__version__)"` | All pre-existing tests still pass (99 + any newly added SIM-001 tests); zero warnings; compile exit 0; import smoke clean |
| SIM-TC-07c **[PLAN]** | New public imports without circularity | Implementation present | Import new public API from `softlab.tu` (and top level if exported); import each new submodule directly (e.g., `import softlab.tu.simulation`) | All succeed with no circular-import error; public class identity check (same pattern as Release 1 `public_exports_retain_class_identity`) |
| SIM-TC-07d **[PLAN]** | No new required dependencies | — | Inspect `pyproject.toml` diff and run import smoke in the current env | No new required dependencies added; optional extras untouched; imports work with the existing environment |
| SIM-TC-07e **[PLAN]** | Release 1 debt unchanged | — | If the suite emits any warning/failure involving OBS-004/005/006 or DEFECT-2, record verbatim and obtain disposition through the workflow | No silent waivers; no unrelated repairs |

## SIM-AC-08 — usable, honestly bounded delivery (docs + executed example)

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| SIM-TC-08a **[PLAN]** | User guide exists and covers required topics | Guide delivered under `docs/` | Checklist review of the guide: construction, `evolve`, observation, reset, errors, ownership/aliasing rules, optional time context, mock-device vs simulated-object distinction, supported value contract, deterministic-callback contract (including external side effects caveat) | Every topic present, in English, consistent with the implemented API |
| SIM-TC-08b **[PLAN]** | Executed deterministic example | Guide contains a runnable example (accumulating model + ndarray model per planned architecture) | Execute the example code as an automated test (`tests/test_sim_user_guide.py`) capturing stdout | Runs deterministically; actual output equals the guide's stated expected output; repeated execution byte-identical |
| SIM-TC-08c **[PLAN]** | API docstrings | Implementation present | Sample the public API docstrings (constructor, evolve, observation, reset, error cases) | English docstrings with parameters/returns/exceptions per repo convention |
| SIM-TC-08d **[PLAN]** | Full evidence before integration | All above complete | Assemble evidence: unittest results, compile/import results, warning check, candidate CI reference | Recorded in the test results document; nothing marked passed that was not run |

## Traceability summary

| AC | Test cases |
| --- | --- |
| SIM-AC-01 | SIM-TC-01a–01e |
| SIM-AC-02 | SIM-TC-02a–02e |
| SIM-AC-03 | SIM-TC-03a–03d |
| SIM-AC-04 | SIM-TC-04a–04d |
| SIM-AC-05 | SIM-TC-05a–05g |
| SIM-AC-06 | SIM-TC-06a–06f |
| SIM-AC-07 | SIM-TC-07a–07e |
| SIM-AC-08 | SIM-TC-08a–08d |

## Risks and open questions for the design review

1. Exact publication boundary for pending inputs (SIM-TC-03c) — the plan
   accepts either "pending until evolve" or "committed on assignment" per
   documented design, but the design must state which.
2. Whether inspection returns copies or read-only views (SIM-TC-05d): plan
   requires functional protection either way; the design picks the mechanism.
3. The SIM-AC-06 stepping hook (SIM-TC-06d planned assertion): the design
   must name the concrete hook/extension point; the test then pins
   evolve-once-per-point.
4. Failure atomicity for reset (SIM-AC-05 covers input update/evolve; reset
   atomicity rides on SIM-TC-04d/05b and will be asserted explicitly once the
   reset error contract is designed).
