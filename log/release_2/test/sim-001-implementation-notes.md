# SIM-001 implementation notes

Owner: sw-tom | Branch: `codex/sim-001-simulation-foundation` |
Design: `log/release_2/design/sim-001-detailed-design.md` (APPROVED) |
Date: 2026-10-05

## What was built

- `softlab/tu/simulation/object.py` — `SimulatedObject` plus private
  helpers `_classify_value`, `_validate_against_spec`, `_copy_value`
  per design §5. Implements the full approved design: declared
  input/state/output namespaces with cross-role name uniqueness;
  literal and factory-declared initial values (factories invoked once
  at construction, re-invoked per `reset()`); pinned per-variable value
  specifications over the closed three-category contract (exact `int`,
  exact `float`, exact numeric `np.ndarray`); copy-in/copy-out at every
  boundary including callback arguments; pending-input publication at
  the evolve boundary; build-then-swap atomicity for input updates,
  evolution, observation and reset; identical original-exception
  propagation for user callbacks and factories (never caught, wrapped
  or replaced); strict evolve-once semantics (callback invoked exactly
  once per `evolve_once()` and from nowhere else).
- `softlab/tu/simulation/__init__.py` — re-exports `SimulatedObject`
  (only public name).
- `softlab/tu/__init__.py` — `simulation` added to the subpackage
  import tuple (order: `station`, `simulation`, `theory`).
- `docs/user-guide/simulation.md` — user guide per design §4/§6:
  construction, evolve contract, observation, reset, error table,
  ownership/aliasing rules, supported value contract, deterministic-
  callback contract with external-side-effects caveat, optional time
  context, mock-device vs simulated-object distinction, the
  `Device`/`Parameter` bridge pattern with the hook-binding constraint
  and the two-gate validation asymmetry, known limitations, and a
  deterministic executed example with recorded actual output.

No deviations from the approved design. No changes outside the
authorized scope (no `huo`/`jin`/`shui`/`mu` production edits, no
`pyproject.toml` change, no new dependencies). Per task assignment, the
unittest test files (`tests/test_tu_simulation*.py`,
`tests/test_sim_user_guide.py`) are NOT part of this delivery — they
are sw-mike's deliverable in the testing phase; self-testing used
throwaway scripts only (not committed).

## Self-test commands and results

Throwaway self-test scripts (in `/tmp`-style scratch dir, not
committed) covering construction validation, value-contract rejections,
spec pinning (scalar category, ndarray dtype/shape), pending inputs,
evolve-once counting, callback copy isolation, key validation, original
exception identity from evolve/observe/factory, reset build-then-swap
atomicity (armed-factory failure leaves committed state intact; object
remains usable; later reset succeeds), factory-result validation
failure, per-instance isolation, and the memoryless holding-state idiom:

```text
$ python sim_selftest.py
import ok, version: 0.3.0
part 1 ok
$ python sim_selftest2.py
part 2 ok
```

Executed example (guide §11) run twice, byte-identical output:

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

Repo verification (Python 3.13 via project `.venv`):

```text
$ python -m compileall -q softlab        # exit 0
$ python -c "import softlab; print(softlab.__version__)"
0.3.0
$ python -m unittest discover -s tests -p 'test_*.py'
Ran 99 tests in 0.441s

OK
$ git diff --check                          # clean
```

## Commits

- `4eab94d` `feat(tu): add simulated-object foundation package`
- `0364080` `docs(user-guide): add simulation guide with executed
  example`
- (this note) `docs(log): add SIM-001 implementation notes`
