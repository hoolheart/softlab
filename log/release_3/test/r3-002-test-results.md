# R3-002 test results — object builders/registry and station object membership

Tester: sw-mike | Date: 2026-10-11
Branch: `codex/r3-002-builders` (HEAD `93939b0`, synced with
`origin/codex/r3-002-builders`; execution adds only this record)
Plan: `log/release_3/test/r3-002-test-plan.md` (Revision 2, approved)
Requirements: `log/release_3/prd.md` (R3-AC-04 membership/builder scope;
R3-AC-08 compatibility/evidence portion)
Implementation under test: commits `1ff6d6b..ceec02f` (code review
APPROVED, zero issues, at `8d20542`)

## Verdict

**PASS — all 24 planned case IDs pass (23 automated + CHK-04-1 manual
record check). Full suite 165 tests, OK, exit code 0, zero warnings
(including under the `-W error::Warning` gate).** Nothing below is marked
passed that was not run. The 165 = 147 pre-existing (unchanged) + 18 new
R3-002 tests (17 in `tests/test_tu_station_objects.py` + 1 companion
extension in `tests/test_tu_contracts.py`), matching the code review's
independently reproduced count.

## Environment

| Item | Value |
| --- | --- |
| Python | 3.13.15 (`$PWD/.venv`, conda env per AGENTS.md; invoked as `.venv/bin/python`) |
| softlab version | `0.3.0` |
| Platform | macOS (darwin) |
| New test dependencies | none (stdlib + NumPy only) |
| Unverifiable items | none |

## Commands and verbatim results

### 1. unittest full suite (R3-TC-04a–04r, R3-TC-08a–08c)

Command:

```
python -m unittest discover -s tests -p 'test_*.py' -v
```

Result (final confirmation run):

```
----------------------------------------------------------------------
Ran 165 tests in 0.488s

OK
```

- Test count: **165** (147 pre-existing unchanged + 18 new R3-002:
  17 in `test_tu_station_objects.py` + 1 companion device-side guard in
  `test_tu_contracts.py`), every verbose line ends in `... ok`.
- Result: **OK**, exit code **0**.
- The two backend lines ("Connected to sqlite3 backend …", "Connected to
  HDF5 backend …") are informational prints from the data-backend tests,
  not warnings.
- Per-case verbose confirmations (all `... ok`):
  `test_04a`–`test_04i`, `test_04j`, `test_04k`, `test_04h`, `test_04m`–
  `test_04r`, and `test_station_shared_namespace_rejects_device_shadowing_object`.

### 2. Warning gate (R3-TC-08b)

Command:

```
python -W error::Warning -m unittest discover -s tests -p 'test_*.py'
```

Result: **Ran 165 tests … OK**, exit code **0** — every warning class is
an error under this gate, so a green run proves zero warnings. No
`DeprecationWarning`/`UserWarning`/other warning fired; no Release 1 debt
warning (OBS-004/005/006, DEFECT-2) fired — none observed, nothing waived
silently.

### 3. compileall (R3-TC-08c)

Command: `python -m compileall -q softlab`
Exit code **0**, no output.

### 4. import smoke (R3-TC-08c)

Command: `python -c "import softlab; print(softlab.__version__)"`
Output: `0.3.0`, exit code **0**.

### 5. Public re-export and import-identity checks (R3-TC-08c/08d)

Commands:

```
python -c "from softlab.tu.station import Station; \
from softlab.tu.simulation import ObjectBuilder, SimulatedObject, \
    register_object_builder, get_object_builder; \
import softlab.tu.simulation.builder as b; \
import softlab.tu.simulation.object as o; \
assert b.ObjectBuilder is ObjectBuilder; \
assert b.register_object_builder is register_object_builder; \
assert b.get_object_builder is get_object_builder; \
assert softlab.tu.simulation.SimulatedObject is o.SimulatedObject"
```

All assertions pass — public class identity holds; the new names are
importable from the design-documented path `softlab.tu.simulation`
(design lines 444–445, 684) and its backing module
`softlab.tu.simulation.builder`; `SimulatedObject` identity
(`softlab.tu.simulation.SimulatedObject is
softlab.tu.simulation.object.SimulatedObject`, Release 2 SIM-001
contract) holds.

Note on invocation phrasing: the pre-existing `softlab/tu/__init__.py`
collects submodules only and does not re-export `Station` at the
`softlab.tu` top level; the documented public path (used by every
existing test and the design) is `softlab.tu.station`. `from softlab.tu
import Station` fails identically on `dev` — `softlab/tu/__init__.py` is
byte-identical to `dev` (`diff` of `git show dev:softlab/tu/__init__.py`
vs. worktree copy: no differences) and is untouched by this task's diff.
This is pre-existing repo structure, not an R3-002 regression; R3-TC-08c's
compatibility anchor (existing import paths keep working) holds on all
documented paths.

### 6. Registry coexistence without interference (R3-TC-08d)

- `git diff dev...HEAD --stat -- softlab/` → only
  `softlab/tu/simulation/__init__.py` (+5), `softlab/tu/simulation/builder.py`
  (+101, NEW), `softlab/tu/station/station.py` (+110).
  `softlab/tu/station/device.py` (the entire device-builder registry,
  `get_device_builder`/`register_device_builder`) is **untouched**.
- Automated proof of coexistence: `test_04k_registry_semantics` (b) —
  an object builder and a device builder registered under the same model
  string `'shared'` are both retrievable from their own registries.
  `... ok`.
- Device-side semantics regression: the pre-existing
  `test_station_builder_registry_and_default_are_restored`
  (R3-TC-04l anchor) passes unmodified.

### 7. No new required dependencies (R3-TC-08e)

Command: `git diff dev...HEAD -- pyproject.toml setup.py | wc -l`
Output: `0` — packaging metadata untouched; optional extras untouched.

### 8. Scope gate + connected-simulation absence (CHK-04-1, part 1)

Tester-side independent verification:

- `git diff dev...HEAD --stat -- softlab/huo softlab/jin softlab/shui softlab/mu`
  → empty, exit 0.
- `git diff dev...HEAD --stat -- softlab/` → 3 files, all under
  `softlab/tu/` (listed in section 6). Production runtime changes
  confined to `softlab/tu/` — matches the approved design's file-level
  change list exactly.
- Connected-simulation grep over the production diff:
  `git diff dev...HEAD -- softlab/ > /tmp/r3002_prod.diff`;
  `grep -inE 'coordinator|sim_dt|tick|clock' /tmp/r3002_prod.diff` → no
  matches, exit 1; `grep -c 'edge'` → 0 matches. No coordinator/edges/
  clock production code in this task's diff (that scope belongs to
  R3-003).

### 9. CHK-04-1 docstring citation (unknown-model error documented)

The station-level object-construction entry is
`Station.build_object` (`softlab/tu/station/station.py:216-255`). Its
docstring Errors block documents the unknown/unregistered-model error
verbatim:

- **station.py:232-235** —
  ```
  232:         Errors:
  233:         - ValueError --- ``name`` is not a ``str`` or is empty
  234:         - RuntimeError --- no object builder registered under ``model``
  235:           (``Failed to get object builder with model ...``)
  ```

The RuntimeError documentation is at **station.py:234**, exactly the
file:line cited in the code review (`8d20542`). This closes the
"documented" half of R3-TC-04o. Additionally, the automated case
`test_04o_unknown_model_is_documented_runtime_error` pins
`'RuntimeError' in Station.build_object.__doc__` at runtime — `... ok`.
Docstring citation recorded; **PASS**.

### 10. R3-TC-04m disposition (manual-condition assessment)

The plan makes R3-TC-04m a manual file/line-citation check **only if**
"the design lands construction in a non-station module". The approved
design landed `build_object` directly in the station module
(`softlab/tu/station/station.py:216-255`), so the manual wiring-citation
condition does **not** apply; the automated case governs:
`test_04m_build_object_inserts_member` — build via the station entry,
member retrievable by explicit lookup, devices listing unchanged —
`... ok`. **PASS** (automated).

### 11. Companion-ruling disposition (device-side guard, `ceec02f`)

The companion ruling to R3-TC-04b (adding a device whose name is held by
an object member must be rejected) was implemented as a production guard
in `add_device` (`station.py:147-149`) with dedicated coverage added at
`ceec02f`: `DeviceContracts.test_station_shared_namespace_rejects_device_shadowing_object`
(`tests/test_tu_contracts.py:196-212`). Isolated run: `Ran 1 test … OK`.
The same ruling is also asserted inline in
`test_04b_membership_namespaces_are_independent`
(`test_tu_station_objects.py:94-99`). **PASS** — device-side guard
coverage implemented and green.

### 12. Baseline-vs-regression (R3-TC-08a)

- Pre-implementation baseline (`r3-002-baseline.md`): 147 tests OK,
  zero warnings, recorded 2026-10-07 against tip `f279958`.
- Post-implementation: 165 tests OK (section 1). The 147 pre-existing
  tests are the complement of the 165 after removing exactly the 18 new
  R3-002 tests, and every pre-existing test file other than
  `test_tu_contracts.py` (+18 lines, one new method only) is byte-identical
  to `dev` (`git diff dev...HEAD --stat -- tests/` shows only
  `test_tu_station_objects.py` NEW and `test_tu_contracts.py` +18).
  All 147 re-ran green, unmodified, inside the 165-suite. **PASS.**

### 13. Candidate CI evidence (R3-TC-08f)

Command: `gh run view 38126151850 --json headBranch,headSha,conclusion,jobs`

- Run **38126151850**: `headBranch=codex/r3-002-builders`,
  `headSha=93939b045ffb2b445e1f970ea9ec4d31202b780f` (current HEAD
  `93939b0`), `conclusion=success`.
- Both matrix jobs green: `compatibility (3.13)` (job 114430825116) and
  `compatibility (3.9)` (job 114430825237), each including
  `Regression suite`, `Compile sources` and `Import smoke check` steps
  all `conclusion=success`.
- Pre-merge task-branch run per the R3-001 REC-1 lesson: regression,
  compile/import and warning evidence all precede integration. **PASS.**

## Per-case results

| Case ID | Method | Verdict | Evidence |
| --- | --- | --- | --- |
| R3-TC-04a | automated | PASS | `StationObjectMembership.test_04a_add_and_look_up_object` |
| R3-TC-04b | automated | PASS | `test_04b_membership_namespaces_are_independent` (incl. inline device-side guard companion ruling) |
| R3-TC-04c | automated | PASS | `test_04c_remove_object_is_lenient` |
| R3-TC-04d | automated | PASS | `test_04d_explicit_lookup_survives_method_name_collision` |
| R3-TC-04e | automated | PASS | `test_04e_rejects_non_object` |
| R3-TC-04f | automated | PASS | `test_04f_rejects_object_shadowing_device` |
| R3-TC-04g | automated | PASS | `test_04g_rejects_duplicate_object_name` |
| R3-TC-04h | automated | PASS | `test_04h_invalid_name_rejected_before_mutation` |
| R3-TC-04i | automated | PASS | `test_04i_object_names_are_immutable_for_members` |
| R3-TC-04j | automated | PASS | `ObjectBuilderRegistry.test_04j_builder_convention_mirrors_device_builder` |
| R3-TC-04k | automated | PASS | `test_04k_registry_semantics` |
| R3-TC-04l | automated (regression) | PASS | `tests/test_tu_contracts.py` `test_station_builder_registry_and_default_are_restored` re-ran green in the 165-suite |
| R3-TC-04m | automated | PASS | `StationBuildObject.test_04m_build_object_inserts_member`; manual-citation condition not triggered (construction landed in the station module — see §10) |
| R3-TC-04n | automated | PASS | `test_04n_builder_exception_propagates_unchanged` |
| R3-TC-04o | automated | PASS | `test_04o_unknown_model_is_documented_runtime_error`; docstring citation §9 |
| R3-TC-04p | automated | PASS | `test_04p_membership_validation_failure_after_build` |
| R3-TC-04q | automated | PASS | `test_04q_built_instances_have_independent_state` |
| R3-TC-04r | automated | PASS | `test_04r_already_owned_instance_rejected` |
| R3-TC-08a | automated (baseline/regression) | PASS | §12 — 147 pre-existing tests pass unchanged inside 165 OK |
| R3-TC-08b | automated | PASS | §2 — 165 tests OK under `-W error::Warning`, exit 0 |
| R3-TC-08c | automated | PASS | §§1, 3, 4, 5 — suites pass, compile exit 0, import smoke 0.3.0, identity holds |
| R3-TC-08d | automated | PASS | §6 — device.py untouched; same-model coexistence proven by test_04k(b); device registry semantics green |
| R3-TC-08e | automated | PASS | §7 — zero packaging-metadata changes |
| R3-TC-08f | manual record check | PASS | §13 — CI run 38126151850 green, both matrix jobs, head 93939b0 |
| CHK-04-1 | manual record check | PASS | §§8, 9 — scope gate empty; production confined to softlab/tu/; no connected-simulation code; docstring citation station.py:234 recorded |

## Per-AC coverage summary

| AC (R3-002 scope) | Verdict | Covered by |
| --- | --- | --- |
| R3-AC-04 — existing device use remains compatible | PASS | R3-TC-04l, R3-TC-08a/08c/08d |
| R3-AC-04 — object membership | PASS | R3-TC-04a–04e |
| R3-AC-04 — explicit lookup | PASS | R3-TC-04d (+ device side pinned by 04l) |
| R3-AC-04 — naming (no silent shadowing) | PASS | R3-TC-04f–04i (+ companion device-side ruling) |
| R3-AC-04 — builder discovery/construction | PASS | R3-TC-04j, 04k, 04m |
| R3-AC-04 — failed-build safety | PASS | R3-TC-04n–04p (+ device-path analog in 04l) |
| R3-AC-04 — independent instances | PASS | R3-TC-04q, 04r |
| R3-AC-08 — compatibility and evidence | PASS | R3-TC-08a–08f + CHK-04-1 |

## Key behavioral pins recorded during execution

- **Membership/naming (04a–04i):** objects live in a namespace parallel
  to devices; `add_object` rejects non-objects (`TypeError`), duplicate
  object names and device-name collisions (`ValueError`); `rm_object` is
  lenient (returns the object, then `None`); explicit `station.object(name)`
  lookup survives method-name collisions (`snapshot`, `add_device`,
  `devices`); object names are immutable for members (read-only property,
  `AttributeError` on set), so the baseline device rename-orphan defect
  cannot reproduce on the object side.
- **Builders/registries (04j–04m):** `ObjectBuilder` mirrors
  `DeviceBuilder` (model identity, `build(name, **kwargs)`,
  `<ObjectBuilder>{model}` repr, base `NotImplementedError`); the
  model-keyed object registry rejects duplicate models and non-builders,
  never constructs at registration, and is fully distinct from the device
  registry (same model string coexists); `build_object` validates the
  name before any registry access (gate order pinned by
  `build_object('missing', '')` raising `ValueError`).
- **Failure safety (04n–04p):** a raising builder propagates the
  identical exception object (`assertIs`) with membership bit-identical;
  an unknown model raises the documented `RuntimeError` with membership
  unchanged; a membership-validation failure after a successful build
  leaves both namespaces untouched.
- **Independence (04q–04r):** two objects built from one builder have
  independent mutable state (`x` evolves to 1.0 on one, stays 0.0 on the
  other); a builder re-returning an already-owned instance is rejected on
  the second insertion and cannot alias one mutable object under two
  names.

## Warnings observed

**None.** Under `python -W error::Warning` the suite is green (165
tests, OK, exit 0), which is only possible with zero emitted warnings.
No pre-existing Release 1 debt warning (OBS-004/OBS-005/OBS-006 or
DEFECT-2) fired. No waiver, silent or otherwise, was applied.

## CI reference

Local evidence above was produced on Python 3.13.15. The candidate-commit
CI (`.github/workflows/ci.yml`: unittest regression, compile and import
checks on Ubuntu, Python 3.9/3.13 matrix) ran green on the task branch at
the current head — run 38126151850, both matrix jobs `success`
(§13). Integration to `dev` per AGENTS.md additionally requires that
pre-merge CI to remain green at the merge candidate; no CI result is
marked green before it exists.

## Issues found

None. No production-code defects were observed during this execution; no
bug reports to sw-tom arise from this run.
