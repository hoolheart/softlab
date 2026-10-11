# R3-003 characterization baseline — coordinated simulation capability

Tester: sw-mike | Date: 2026-10-11
Branch: `codex/r3-003-coordination` (head `72379f4`, from `dev` @ `0bd97d5`,
synced with origin at characterization time)
Recorded BEFORE any R3-003 test files or production changes exist on this
branch (`git status --short` clean). No production code was modified;
characterization snippets were run read-only from stdin against the
installed package.

## Environment

| Item | Value |
| --- | --- |
| Python | 3.13.15 (`$PWD/.venv`, conda env; invoked as `$PWD/.venv/bin/python` — `conda activate` unavailable in this non-interactive shell, an environment limit, not a project defect; same interpreter as CI's 3.13 leg) |
| softlab version | `0.3.0` |
| Platform | macOS (darwin) |
| Real instruments | none (N/A per task); all fixtures synthetic |
| Python 3.9 leg | NOT run locally (environment limit); CI matrix evidence recorded at R3-TC-08f time |

## 1. Full regression suite (fresh baseline)

Command:

```
$PWD/.venv/bin/python -m unittest discover -s tests -p 'test_*.py'
```

Output (verbatim, tail):

```
...................................................................................................................................................
----------------------------------------------------------------------
Ran 165 tests in 0.483s

OK
Connected to sqlite3 backend, data is stored in /var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/tmp54ka6l6t/readings.db.
Connected to HDF5 backend, data is stored in /var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/tmp54ka6l6t/readings.hdf5.
```

- Test count: **165** (147 pre-R3-002 anchor + 18 R3-002 cases on
  `tests/test_tu_station_objects.py`); supersedes the 147 in
  `r3-002-baseline.md` as the R3-003 pre-implementation anchor.
- Result: **OK**, exit code **0**.
- Warnings: **zero** — `-W default` re-run, `grep -ci warning` on full
  output: **0** (command recorded above; the two "Connected to …" lines
  are informational prints, not warnings).
- Compile: `python -m compileall -q softlab` → exit 0, no output.
- Import smoke: `python -c "import softlab; print(softlab.__version__)"`
  → `0.3.0`, clean.
- `-W error::Warning` full gate NOT run for this baseline (unverified-here;
  executed as R3-TC-08b at implementation time — pending, not waived).

## 2. Coordinated-simulation capability — ABSENT (gap, expected)

Direct inspection and runtime probe. `softlab/tu/simulation/` contains
exactly `__init__.py`, `object.py` (R3-001/R2, 804 lines),
`builder.py` (R3-002, 101 lines). `grep` over `softlab/` finds **no**
coordinator/connection/edge/clock/pending/delivered artifact:
`coordinator|coordinated|connection|sim_dt|edge|snapshot|tick|pending|
delivered` matches in `softlab/tu/simulation/` only the unrelated
set_input docstring word "pending publication"
(object.py line 481), and in `station.py` only the forward-looking
docstring at lines 205–208: *"removal of an object enrolled in an active
coordinated simulation will be rejected by the future ownership layer
(R3-003 scope), not by this method."*

Runtime probe (verbatim):

```
== simulation package attrs ==
  sim.Coordinator: False
  sim.coordinator: False
  sim.Connection: False
  sim.connect: False
  sim.Edge: False
  sim.MockEndpoint: False
  sim.MockSignal: False
  sim.register_connection: False
  sim.sim_dt: False
  sim.tick: False
  sim.step: False
  sim.advance: False
  sim.prepare: False
  sim.reset_all: False
  sim.SimulationClock: False
  sim.OwnershipError: False
  sim.SharedSimClock: False
== Station attrs ==
  Station.sim_dt: False
  Station.set_sim_dt: False
  Station.objects: True          (R3-002 membership — present)
  Station.object: True           (R3-002 membership — present)
  Station.connect: False
  Station.coordinator: False
  Station.tick: False
  Station.prepare: False
  Station.reset_simulation: False
```

Station-level probe: `getattr(st, 'connect', 'ABSENT')` → `ABSENT`.

**Finding GAP-1:** every R3-003 capability — opt-in mock endpoints,
connection/edge model, shared `sim_dt` clock, tick-zero preparation,
delayed stepping, atomic coordinated reset, ownership/reentrancy guards —
is absent. This is the new capability under test; all [PLAN] cases in
`r3-003-test-plan.md` fail by `AttributeError` today (vacuous-fail per
AGENTS.md assertion-pinned reproduction convention).

## 3. Gap inventory vs architecture plan (evidence-based)

| Plan element (architecture-plan.md) | State | Evidence |
| --- | --- | --- |
| Opt-in coordinator owning enrollment, connections, snapshots, delivered samples, clock, lifecycle (lines 13–18) | ABSENT | probe §2; no class/module in `softlab/tu/simulation/` beyond object/builder |
| Explicit opt-in mock signal storage/bridges: control source owns pending values, measurement sink owns delivered samples (lines 22–27) | ABSENT | no mock endpoint artifacts; `Parameter`/`Device` have no simulation-mode hooks (parameter.py classes: `Parameter`, `QuantizedParameter`, `ProxyParameter` only) |
| One boundary snapshot per transition; freeze pending sources at k; detached candidates; publish only on full success (lines 36–55) | ABSENT | no tick/step API exists to snapshot |
| Tick-zero preparation builds outputs from initial states, stages initial mock samples, publishes nothing on failure (lines 59–63) | ABSENT | no prepare/initialize entry point |
| Coordinated reset staging inputs/states/controls/buffers/samples then publishing tick/time zero together; failed reset leaves committed data intact (lines 65–71) | ABSENT | `SimulatedObject.reset` is per-object standalone only (object.py lines 649–704) |
| Station-owned explicit interval; fixed during run; changeable only at reset boundary; model-dt binding validated, never inferred by name (lines 73–78) | ABSENT | `Station.sim_dt` absent (probe §2); object.py lines 10–12 document "no dedicated clock API", time/dt is a declared model input |
| Ownership guards: standalone evolve/reset prevented while coordinated; connected-input writes rejected; reentrancy; safe detach at stopped boundary (lines 81–86) | ABSENT | only the forward-looking `rm_object` docstring (station.py lines 205–208); no guard exists on `evolve_once`/`reset` |
| Directional single-driver connections, fan-out, cycles permitted; atomic rejection of invalid graph edits (lines 88–94) | ABSENT | no connection API |

Design-gate status (tasks.md lines 41–44): snapshot publication,
ownership/reentrancy, safe detach, model-dt binding and numeric clock
limits must be **proven by the detailed design before implementation**.
As of this baseline no R3-003 detailed design exists under
`log/release_3/design/` for this task — these are required design gates,
recorded as **unproven/pending**, not waived.

## 4. No-regression anchors (existing behavior that must NOT change)

All verified live on this branch, verbatim output from the read-only
characterization snippet run during this step:

1. **Standalone evolve/observe contract** (R2/R3-001): an
   `evolve_once` advances state exactly once; `observe_outputs` does not
   evolve (`standalone: x=0.0 y=0.0` — y reflects the state *after* the
   single evolution, not a second one).
2. **Original exception identity** (object.py lines 557–561, 686–687):
   a raising evolve callback propagates the identical exception object
   (`exception identity: same object -> True` via `assertIs`-style
   probe).
3. **Copy boundaries** (object.py lines 197–219): mutating an ndarray
   returned by `get_state` does not affect committed storage
   (`committed v[0]=0.0` after setting the getter result's `[0]=99.0`).
4. **R3-002 station object membership** (station.py lines 175–255):
   `add_object` inserts under `obj.name` with device-collision and
   duplicate rejection; `objects`/`object`/`rm_object`/`build_object`
   present and exercised (`objects=('acc',) object('acc')='acc'`).
   R3-002 suite: 17 test methods in `tests/test_tu_station_objects.py`,
   all passing within the 165.
5. **R3-001 corrected gaps** (mixed callback-key `ValueError`, bridge
   readback, narrowed reset claims): covered by the 12 R3-001 cases in
   the 165-test suite; must keep passing unmodified.
6. **Device-side contracts**: delegation precedence (real members win),
   device builder registry semantics, snapshot/describe device-only
   legacy shape — pinned by existing `tests/test_tu_contracts.py`,
   `test_tu_descriptions.py`, `test_tu_lifecycle.py`.
7. **No-clock-object contract**: `SimulatedObject` exposes no time/dt
   state of its own (object.py lines 10–12, 233–236); any coordinated
   clock must be station/coordinator-owned and must not inject a
   positional callback argument (prd.md lines 62–66).

## 5. Environment limits

- Python 3.9 leg unverified locally; CI matrix evidence is recorded at
  candidate time (R3-TC-08f).
- `-W error::Warning` full gate deferred to implementation-time evidence
  (R3-TC-08b), matching the R3-002 convention.
- No real instruments (N/A); UI/hardware gates N/A.
- Notebook-based exploratory validation (`tests/test_*.ipynb`) not run;
  automated unittest evidence is the authoritative gate.

## 6. Zero-coverage list (explicitly NOT covered by this plan)

- The worked connected example and user guide (R3-AC-08 remainder —
  R3-004 scope).
- R3-AC-07 gap corrections (closed by R3-001; only regression re-runs here).
- Concrete `huo` simulation process, scheduling policy, automatic
  advancement, real-time pacing, thread safety (excluded by PRD).
- Real instrument access; VISA simulation backends (Release 2 contract,
  orthogonal to coordinated simulation).
- Release 1 OBS-004/005/006 and DEFECT-2 debt (tracked separately, no
  authorization to fix here).
- Multi-tick configurable delays, solvers, graph persistence, noise
  framework (explicitly excluded by PRD/architecture plan).
