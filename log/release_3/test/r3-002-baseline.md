# R3-002 characterization baseline — station membership and object builders

Tester: sw-mike | Date: 2026-10-07
Branch: `codex/r3-002-builders` (from `dev` @ `6dc4d36`, tip `f279958`,
synced with origin at characterization time)
Recorded BEFORE any R3-002 test files or production changes exist on
this branch (`git status --short` clean except the two
`log/release_3/test/` records of this step). No production code was
modified; characterization scripts were run from a temp directory
outside the repository.

## Environment

| Item | Value |
| --- | --- |
| Python | 3.13.15 (`$PWD/.venv`, conda env; invoked as `$PWD/.venv/bin/python` — `conda activate` is unavailable in this non-interactive shell, an environment limit, not a project defect; same interpreter) |
| softlab version | `0.3.0` |
| Platform | macOS (darwin) |
| Real instruments | none (N/A per task); all fixtures synthetic |

## 1. Full regression suite (fresh baseline)

Command:

```
$PWD/.venv/bin/python -m unittest discover -s tests -p 'test_*.py'
```

Output (verbatim, tail):

```
...................................................................................................................................................
----------------------------------------------------------------------
Ran 147 tests in 0.515s

OK
Connected to sqlite3 backend, data is stored in /var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/tmp4zfekkle/readings.db.
Connected to HDF5 backend, data is stored in /var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/tmp4zfekkle/readings.hdf5.
```

- Test count: **147** (135 pre-R3 + 12 R3-001 cases added on this
  branch's parent line; the R3-001 record's 135 is superseded — this
  is the R3-002 pre-implementation anchor).
- Result: **OK**, exit code **0**.
- Warnings: **zero** — re-run under `-W default`, grep count of
  "warning" in full output: **0** (verbatim command and count recorded;
  output file `/tmp/r3-002-warnrun.txt`, `Ran 147 tests … OK`).
- `-W error::Warning` full gate was NOT run for this baseline
  (unverified-here; executed as R3-TC-08b at implementation time —
  recorded as pending, not waived).
- Import smoke: `python -c "import softlab; print(softlab.__version__)"`
  → `0.3.0`, clean.
- Compile: `python -m compileall -q softlab` → exit 0, no output.

The two backend lines are informational prints from data-backend
tests, not warnings.

## 2. Object membership on Station — ABSENT (gap, expected)

Direct inspection of `softlab/tu/station/station.py` and runtime
probe. Station's only membership store is `self._devices`
(station.py line 45), delegated at line 46; membership methods are
`add_device` (118–124), `rm_device` (126–128), `device` (130–132),
`build_device` (134–145); `snapshot` (67–76) and `describe` (78–116)
emit only `devices`.

Runtime probe (verbatim):

```
== A: object membership APIs present on Station? ==
  Station.add_object: False
  Station.rm_object: False
  Station.object: False
  Station.objects: False
  Station.build_object: False
  Station.obj: False
  Station.simulated_objects: False
```

Attempting to register a `SimulatedObject` through the only existing
membership path:

```
add_device(SimulatedObject) -> TypeError Invalid device type <class 'softlab.tu.simulation.object.SimulatedObject'>
```

(source: station.py lines 120–121 `isinstance` guard; snapshot keys
confirmed device-only: `['created_at', 'devices', 'name']`.)

**Finding GAP-1:** none of the PRD R3-AC-04 membership capabilities
(add / look up / list / remove / build objects on a station) exists.
This is the new capability under test — all R3-TC-04a–04e/04m fail by
`AttributeError` today.

## 3. Object builder / model-keyed registry — ABSENT (gap, expected)

`softlab/tu/simulation/` contains only `object.py` +
`__init__.py`; the package re-exports `SimulatedObject` alone
(`__init__.py`: "re-exports `SimulatedObject` (only public name)").
`grep -rn "ObjectBuilder\|object_builder\|register_object\|build_object\|add_object\|rm_object" softlab/ tests/ docs/` finds **no**
object-builder/registry artifact anywhere in the repository.

Runtime probe (verbatim):

```
== B: object builder/registry APIs present? ==
  softlab.tu.simulation.ObjectBuilder: False
  softlab.tu.simulation.register_object_builder: False
  softlab.tu.simulation.get_object_builder: False
```

The only existing builder/registry convention is the device one:
`DeviceBuilder` (device.py lines 515–553: `model` identity line
535–538, `build(name, **kwargs)` line 543–553 raising
`NotImplementedError` in the base), module-global `_device_builders`
dict (line 556–557), `register_device_builder` (560–567: type check +
duplicate-model `ValueError` lines 564–566), `get_device_builder`
(570–572). Registration stores the builder; it constructs nothing
(line 567).

**Finding GAP-2:** no object builder, no model-keyed object registry,
no station-level object construction. R3-TC-04j/04k fail by
`AttributeError` today. The device convention to mirror is fully
characterized above and by R3-TC-04l.

## 4. Existing device-membership behavior — characterization (no-regression anchor)

Verbatim probe results (script in temp dir, synthetic fixtures only):

```
== C: naming rules (devices) ==
  duplicate device name -> ValueError: Device with name dev1 has exist
  non-device add -> TypeError: Invalid device type <class 'str'>
  device named 'snapshot': attr access station.snapshot -> method
  explicit lookup station.device('snapshot') -> <class 'softlab.tu.station.device.Device'>/snapshot
  device named 'devices': attr station.devices -> dict_keys
  explicit lookup station.device('devices') -> <class 'softlab.tu.station.device.Device'>/devices
  rename after add: station.device('orig') -> <class 'softlab.tu.station.device.Device'>/renamed; station.device('renamed') -> None
  station.devices keys after rename: ['orig']
```

Findings:

- **C-1 (existing rule to mirror):** duplicate device names are
  rejected explicitly (`ValueError`, station.py lines 122–123); wrong
  types rejected (`TypeError`, lines 120–121). The object side must
  gain the same explicitness (R3-TC-04f/04g).
- **C-2 (existing rule to preserve):** method-name collisions resolve
  by delegation precedence — real members win, so `station.snapshot`
  stays the method even with a device named `snapshot`; **explicit
  lookup `station.device('snapshot')` still returns the device**
  (OBS-006 category, documented at device.py lines 129–133). R3-TC-04d
  requires the identical precedence plus an explicit-lookup escape
  hatch for objects.
- **C-3 (existing defect, out of scope):** renaming a device after
  `add_device` orphans the membership key — the dict keeps key `'orig'`
  while the device's name becomes `'renamed'`; lookups by either name
  are then wrong. R3-TC-04i asserts the object side does **not**
  reproduce this; fixing the device side is Release 1 debt and not
  authorized here.
- **C-4:** `rm_device` is a lenient `pop(name, None)` (station.py line
  128) — removing a missing name is a no-op returning None; object
  removal should mirror this (R3-TC-04c).

## 5. Device build-failure safety — characterization (analog to match)

Verbatim probe:

```
== D: device build failure safety ==
  failing builder -> RuntimeError: boom in build
  membership unchanged: True, device('newdev') -> None
  unknown model -> RuntimeError: Failed to get builder with model missing-model
  membership still unchanged: True
```

Mechanism: `build_device` calls `add_device(builder.build(...))`
(station.py line 145) — a raising `build` propagates **before**
insertion; unknown model raises `RuntimeError` at lines 142–144. No
partial registration occurs on the device path today.

**Finding C-5 (analog):** the object construction path must provide
the same atomicity — failed builds leave membership unchanged
(PR R3-AC-04 "failed-build safety"). Pinned by R3-TC-04n–04p. The
device behavior above must also survive unchanged (R3-TC-04l).

## 6. Instance independence — characterization

Verbatim probes:

```
== E: instance independence via device builder ==
  a.p=1.0 b.p=0.0 (independent: True)

== F: SimulatedObject standalone construction ==
  two independently constructed objects: obj.x=1.0, obj2.x=0.0 (independent: True)
  SimulatedObject has name attr: 'acc'
```

Two devices built from one `DeviceBuilder` hold independent
parameters; two independently constructed `SimulatedObject`s
(constructor at object.py lines 282–306; non-empty `str` name guard
lines 294–296) do not share state. **GAP-3:** there is no builder
path that could violate independence for objects — the risk is
forward-looking (a builder closing over shared mutable state, or
returning one instance twice), pinned by R3-TC-04q (mutation
isolation incl. ndarray values) and R3-TC-04r (already-owned instance
rejection, per architecture-plan lines 32–33).

## 7. Existing test coverage touching station membership (compatibility anchor)

`grep` over `tests/` for membership/builder calls:

- `tests/test_tu_contracts.py` lines 179–194
  (`test_station_builder_registry_and_default_are_restored`): device
  builder registry — duplicate registration `ValueError`,
  `get_device_builder` identity, `build_device` + `station.device()`
  read-back, snapshot device node, `rm_device` non-None, unknown model
  `RuntimeError`; uses `patch.dict(module._device_builders, {},
  clear=True)` isolation and default-station cleanup. **This is the
  single test exercising the device builder registry.**
- `tests/test_tu_integration.py` lines 166, 301–302:
  `add_device` + `assertIs(station.device('legacy'), device)` identity
  lookup.
- `tests/test_tu_descriptions.py` line 66: `add_device` in the
  station description fixture.
- `tests/test_tu_simulation.py` / `test_tu_simulation_integration.py`:
  **no `Station` usage at all** (grep for `Station|add_device` returns
  nothing) — standalone simulation tests never touch membership.
- `grep -rn "must return exactly" tests/` (callback-key coverage) —
  R3-001 already established zero coverage there; unchanged here.

**Finding C-6:** zero existing tests exercise object membership,
object naming, an object registry, object build-failure safety or
builder-based object construction — every R3-002 capability is
unguarded today (expected for new capability; listed explicitly per
process rules).

## 8. Baseline defects, warnings, skips and limits (explicit ledger)

| Item | Status | Evidence / disposition |
| --- | --- | --- |
| GAP-1 object membership absent on `Station` | Confirmed | §2, verbatim probes |
| GAP-2 object builder/registry absent | Confirmed | §3, verbatim probes + grep |
| GAP-3 object construction/independence paths absent (unguarded forward risk) | Confirmed | §6 |
| C-1/C-2/C-4 device naming/lookup/removal rules (behavior to mirror & preserve) | Confirmed working | §4, verbatim |
| C-3 device rename-after-add key orphaning (device-side defect) | Confirmed, **out of scope** | §4; Release 1 debt, not authorized for R3-002 |
| C-5 device build-failure safety (behavior to mirror & preserve) | Confirmed working | §5, verbatim |
| C-6 zero existing coverage of any R3-002 capability area | Confirmed | §7 |
| Suite warnings | **Zero** observed | §1, `-W default` re-run, grep count 0 |
| Skips | None | No `skip` output in suite |
| `-W error::Warning` full gate on THIS branch | Not run here | Unverified-here; executed as R3-TC-08b |
| Release 1 debt OBS-004/005/006, DEFECT-2 | Unchanged, tracked | No warning fired in this run; no silent waiver |
| Real instruments / UI gates | N/A per task | No hardware access; no UI |
| Notebook examples (`tests/test_*.ipynb`) | Not executed | Not required for this baseline |
| Python 3.9 / Linux behavior | Not verified here | Environment limit; candidate CI (R3-TC-08f) covers the matrix |
| Environment limit | `conda activate` unavailable non-interactively | Used `$PWD/.venv/bin/python` directly; same interpreter |

No silent waivers: nothing above was exempted, suppressed or repaired
as part of this baseline. Findings beyond the authorized scope (C-3)
are recorded and explicitly **not** expanded into scope.

## 9. Notes

- This is a fresh baseline on the current branch; it does not reuse
  the R3-001 "135 tests" figure except as arithmetic cross-check
  (147 = 135 + 12 R3-001 cases, confirmed by running the suite).
- Characterization scripts were one-off temp files outside the
  repository; the permanent regression/characterization tests with
  assertions will be added at the R3-002 implementation/testing phases
  per `r3-002-test-plan.md` (TDD: every [PLAN] case currently fails by
  `AttributeError` — the vacuous-error pre-implementation state the
  workflow requires).
- The connected-removal and shared-active-ownership clauses of
  R3-AC-04, and all connection/clock/reset capability, are deliberately
  uncharacterized here: they belong to R3-003/R3-004.
