# R3-002 test plan — object builders/registry and station object membership

Tester: sw-mike | Date: 2026-10-07
Branch: `codex/r3-002-builders` (from `dev` @ `6dc4d36`, tip `f279958`)
Requirements: `log/release_3/prd.md` | Task: `log/release_3/tasks.md` R3-002
Characterization baseline: see `r3-002-baseline.md` (147 tests OK, zero
warnings; membership/builder capability confirmed ABSENT with verbatim
evidence; existing device-membership behavior characterized as the
no-regression anchor).

## Status of this document

This is a **test plan only**. Every case is marked:

- **[BASELINE-READY]** — can be executed against the current baseline
  without any new production code (compatibility re-runs of existing
  station/device suites, characterization assertions of behavior that
  must not change). These pass today and must keep passing.
- **[PLAN]** — requires the R3-002 implementation. Against the current
  baseline every [PLAN] case fails by `AttributeError` (the public API
  does not exist yet — see baseline §2); per repo AGENTS.md these are
  the assertion-pinned minimal reproductions for the new capability:
  they must fail/vacuously-error before implementation and pass after.

No production API names are hard-coded where the mechanism is design
work: cases pin **observable behavior** (station holds objects under
names; builders construct via a model-keyed registry; failed builds
leave membership unchanged; built instances share no mutable state) and
accept whichever class/method names the approved detailed design
documents. Where this plan uses placeholder spellings
(`add_object`/`build_object`/`ObjectBuilder`), they denote the
capability, not the contract.

## Acceptance criteria under test (verbatim from `log/release_3/prd.md`)

> **R3-AC-04** | Station membership and builders | Existing device use
> remains compatible. Object membership, explicit lookup, naming,
> builder discovery/construction, failed-build safety and independent
> instances work; connected removal and shared-active-ownership
> conflicts are rejected.

> **R3-AC-08** | Compatibility and usable delivery | Existing standalone
> object/device/theory behavior and imports remain compatible except the
> explicitly corrected gaps. A worked connected example demonstrates the
> clock, delayed feedback, mock interaction and reset. Regression,
> compile/import, warning and candidate CI evidence precede integration;
> architecture describes implemented capability only after delivery.

**Scope split.** This plan covers the R3-002 task row exactly:
"Object builder/registry and station object membership, explicit
lookup, naming, build failure safety and independent instances" —
i.e. from R3-AC-04: *existing device use remains compatible*, *object
membership*, *explicit lookup*, *naming*, *builder
discovery/construction*, *failed-build safety*, *independent
instances*. The final AC-04 clause — *connected removal and
shared-active-ownership conflicts are rejected* — belongs to the
connected-simulation work (R3-003/R3-004 per tasks.md) and is
**excluded** here. From R3-AC-08 this task covers the
compatibility/regression/compile/import/warning/CI-evidence portion;
the worked connected example is R3-004 scope.

The PRD capability statement pinned by this plan (`prd.md` line 90–97):

> Provide object builders following the **actual** device convention: a
> builder has a model identity and `build(name, **kwargs)` constructs an
> object; a model-keyed registry supports discovery and station
> construction. Keep object and device registries distinct and preserve
> existing DeviceBuilder behavior (including its existing registration
> semantics); do not refactor it into an unrelated fluent builder.
> Invalid/failed object builds leave station membership unchanged. Built
> instances must have independent mutable state. The detailed class
> names, methods and representation remain design work.

Plus the PRD membership/naming statements (lines 79–83):

> A station can add, look up, list, build and remove simulated objects,
> while existing device operations retain their behavior. New object
> names cannot silently shadow devices or another object. Explicit
> lookup remains available for names colliding with station methods.

And the architecture-plan constraints (`architecture-plan.md` lines
29–34): separate registry, duplicate-model rejection,
discovery/lookup, construction and membership validation succeed
before station insertion, independent instances own independent
mutable values, **a builder returning an already owned participant is
rejected**, registration does not construct an object, no device
builder refactor.

## Planned test files

| File | Covers |
| --- | --- |
| `tests/test_tu_station_objects.py` (new) | R3-TC-04a–04k: object membership, naming/collision, builder registry, build-failure safety, instance independence (automated `unittest`; synthetic fixtures only) |
| `tests/test_tu_contracts.py` (extend) | R3-TC-04l: device builder registry semantics remain intact alongside the new object registry (new test method appended; no existing method weakened) |
| (no new file) | R3-TC-08a–08f: full-suite regression, compile/import/warning gates, dependency/scope checks |
| (manual record checks) | R3-TC-04m, CHK-04-1: API/documentation review against the approved detailed design; reviewer scope checklist |

All synthetic fixtures; stdlib + NumPy only; temp dirs only; no real
instruments; no connected simulation; UI/hardware gates N/A.

Naming note: `tests/test_tu_station_objects.py` is a placeholder file
name; the tester may adjust it to the approved design's module layout
at implementation time and record the adjustment in the results
document.

---

## R3-AC-04 — area 1: object membership (add / look up / list / remove)

**Purpose:** a station can hold simulated objects alongside devices;
membership operations work under explicit lookup; existing device
operations are untouched. Baseline (§2/§3 of `r3-002-baseline.md`):
no object membership API exists — `Station` today has only
`add_device`/`rm_device`/`device`/`build_device` (station.py
lines 118–145) and rejects `SimulatedObject` via `add_device` with
`TypeError: Invalid device type` (station.py line 120–121).

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| R3-TC-04a **[PLAN]** (AttributeError on baseline) | Add and look up an object | Station; accumulator `SimulatedObject` fixture | Add object under its name; look it up by that name; list station object members | Object retrievable by explicit lookup; appears exactly once in the membership listing; devices listing unchanged |
| R3-TC-04b **[PLAN]** | Object membership independent of device membership | Same station holds a device `d` and object `o` | Add both; look up each by name | Device `d` found via device lookup only; object `o` via object lookup only; neither namespace leaks into the other |
| R3-TC-04c **[PLAN]** | Remove an object | Station holding object `o` | Remove by name; look up; re-remove | First removal returns the object (or per design) and lookup returns nothing; re-removal is a no-op (returns None or per design) — mirrors existing `rm_device` contract (station.py line 126–128) |
| R3-TC-04d **[PLAN]** | Explicit lookup survives method-name collisions | Object given the name of a real station method (e.g. `snapshot`, `add_device`, `devices`) | Add it; attempt attribute-style access; use explicit lookup | Attribute access follows the existing delegation rule (real members win — baseline §C: `station.snapshot` stays the method even with a device named `snapshot`); **explicit lookup by name returns the object** — the OBS-006-style guarantee, now also for objects |
| R3-TC-04e **[PLAN]** | Adding a non-object is rejected | Station | Attempt to add a plain `Device`, a string, None via the object-membership API | `TypeError` naming the invalid type; membership unchanged; mirrors `add_device` guard (station.py line 120–121) |

## R3-AC-04 — area 2: naming (no silent shadowing)

**Purpose:** new object names cannot silently shadow devices or another
object. Baseline: device-side duplicate rejection exists
(`ValueError: Device with name … has exist`, station.py lines 122–123);
there is no cross-namespace rule because objects cannot be members yet.

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| R3-TC-04f **[PLAN]** | Object name colliding with an existing device name | Station holding device `acc`; object also named `acc` | Attempt to add the object | Explicit rejection (`ValueError` or per design); device remains the only `acc`; no silent shadowing of either namespace |
| R3-TC-04g **[PLAN]** | Duplicate object names rejected | Station holding object `acc`; second object named `acc` | Attempt to add | Explicit rejection naming the collision; first object still retrievable and unmodified |
| R3-TC-04h **[PLAN]** (passes vacuously on baseline; keep passing) | Empty/non-string object name rejected | Station | Via the public construction/insertion entry, attempt build-and-insert with an empty or non-string `name` argument (e.g. `name=''` or `name=123`): the invalid name enters at the entry point's `name` argument, not as pre-existing object state (cf. existing `SimulatedObject` constructor guard, object.py lines 294–296; `Device` name guard, device.py lines 143–145) | Explicit rejection before membership changes; no partial registration |
| R3-TC-04i **[PLAN]** | Renaming an in-station object cannot orphan its lookup key | Station holding object `o` under name `n` | Per the design's mutability contract, attempt whatever rename path exists (or assert names are immutable once members) | Either rename is forbidden for station members, or the membership key follows the rename atomically — the baseline device defect (rename after `add_device` orphans the key: `device('orig')` returns the renamed device, `device('renamed')` returns None — baseline §C) must NOT be reproduced for objects |

## R3-AC-04 — area 3: object builder and model-keyed registry

**Purpose:** object builders follow the actual device convention
(model identity + `build(name, **kwargs)`); a model-keyed registry
supports discovery and station construction; registries stay distinct;
existing `DeviceBuilder` behavior is preserved. Baseline: only the
device registry exists (`_device_builders`, device.py lines
556–572; `register_device_builder` rejects duplicate models with
`ValueError`, lines 564–566; registration stores the builder and
constructs nothing, line 567).

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| R3-TC-04j **[PLAN]** | Object builder convention matches the device convention | An object builder declaring a model identity and `build(name, **kwargs)` returning a `SimulatedObject` | Register it; discover it by model; compare conventions against `DeviceBuilder` (model property, build signature) | Builder has `model` identity and `build(name, **kwargs)`; registry `get` by model returns the same instance; convention visibly mirrors `DeviceBuilder` without subclassing/refactoring it |
| R3-TC-04k **[PLAN]** | Registry semantics: duplicate rejection, distinctness, no construction at registration | Object builder registry; a device builder registered under the same model string | (a) register object builder, re-register same model → explicit rejection; (b) a device builder and object builder may share a model string without collision (registries distinct); (c) registration does not invoke `build` (fixture builder with a call counter) | (a) `ValueError` naming the model; (b) both retrievable from their own registries; (c) build count zero after registration |
| R3-TC-04l **[BASELINE-READY]** (passes today; regression) | Existing device builder registry semantics intact | Existing `test_tu_contracts.py` `test_station_builder_registry_and_default_are_restored` (lines 179–194) | Re-run as part of the suite | Duplicate-device-builder registration still raises `ValueError`; `build_device` + `station.device()` + snapshot still work; unknown model still raises `RuntimeError` (station.py lines 142–144) — pins "preserve existing DeviceBuilder behavior including its existing registration semantics" |
| R3-TC-04m **[PLAN]** | Station construction from object builder | Registered object builder; station | Build-and-insert via the station-level construction entry (device analog: `build_device`, station.py lines 134–145) | Object constructed by the builder is a station member under the given name; retrievable by explicit lookup |

## R3-AC-04 — area 4: build-failure safety

**Purpose:** invalid/failed object builds leave station membership
unchanged. Baseline: the device path already has this property —
`build_device` calls `add_device(builder.build(...))`, so a raising
`build` propagates before insertion (verbatim evidence: membership
count and lookups unchanged after a `RuntimeError`-raising builder and
after an unknown model — baseline §D). Object path must match.

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| R3-TC-04n **[PLAN]** | Builder raising during construction leaves membership unchanged | Station holding a device and an object; registered object builder armed to raise in `build` | Snapshot membership (names + identities); attempt station construction; catch original exception; re-read membership | **Identical exception object propagates unchanged** (`assertIs`); membership bit-identical — no partial registration, no name reservation, object lookup returns None |
| R3-TC-04o **[PLAN]** | Unknown/unregistered model leaves membership unchanged | Station; no builder registered under model `missing` | Attempt station construction with model `missing`; catch; re-read membership | Explicit error (device analog: `RuntimeError`, station.py lines 142–144 — exact class is design work, asserted only to be explicit and documented); membership unchanged |
| R3-TC-04p **[PLAN]** | Membership-validation failure leaves membership unchanged | Object builder whose constructed object violates a membership rule (e.g. name collision with an existing device, per R3-TC-04f) | Station holding device `acc`; attempt station construction of an object named `acc` | Construction-side or insertion-side rejection; membership unchanged; the device `acc` untouched |

## R3-AC-04 — area 5: independent instances

**Purpose:** built instances must have independent mutable state.
Baseline: two independently constructed `SimulatedObject`s do not
share state (verbatim: `obj.x=1.0, obj2.x=0.0` — baseline §E), and two
devices built from one device builder have independent parameters
(baseline §E); there is no object-builder path yet.

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| R3-TC-04q **[PLAN]** | Two objects from one builder do not share mutable state | Registered object builder (accumulator model); station | Build `a` and `b` via the station construction entry; `a.set_input('u', 1.0)`; `a.evolve_once()`; read states of both | `a.get_state('x') == 1.0`; `b.get_state('x') == 0.0` — mutating one instance never affects the other (ndarray-valued fixtures use values that would expose aliasing, e.g. an ndarray state) |
| R3-TC-04r **[PLAN]** | Builder returning an already owned participant is rejected | Builder that returns the **same** `SimulatedObject` instance on every `build` call; station | Build-and-insert `a`; attempt build-and-insert `b` with the same builder | Second insertion explicitly rejected; `a` remains a member, unmodified; single-instance reuse cannot create two station names aliasing one mutable object (architecture-plan line 32–33) |

## R3-AC-08 (this task's portion) — compatibility and evidence

The worked connected example, clock, delayed feedback and connected
mock interaction are **excluded** (R3-003/R3-004 scope).

| ID | Case | Preconditions | Steps | Expected result |
| --- | --- | --- | --- | --- |
| R3-TC-08a **[BASELINE-READY]** | Pre-implementation regression | Current branch | `python -m unittest discover -s tests -p 'test_*.py'` | 147 tests, OK, exit 0 — matches `r3-002-baseline.md` §1 (135 Release 1+2 baseline + 12 R3-001 cases); recorded pre-implementation |
| R3-TC-08b **[PLAN]** | Post-implementation regression + gates | R3-002 implementation on this branch | Re-run unittest; `python -m compileall -q softlab`; `python -c "import softlab; print(softlab.__version__)"`; `python -W error::Warning -m unittest discover -s tests -p 'test_*.py'` | All pre-existing tests pass unmodified; new R3-002 cases pass; zero warnings in all runs; compile exit 0; import smoke clean; no OBS-004/005/006 or DEFECT-2 warning fires (any firing recorded verbatim and dispositioned) |
| R3-TC-08c **[PLAN]** | Standalone compatibility beyond new capability | Implementation present | Existing `tests/test_tu_*.py` suites pass unmodified; public import identity `softlab.tu.simulation.SimulatedObject is softlab.tu.simulation.object.SimulatedObject` (Release 2 SIM-001 contract) | Standalone object/device/theory behavior and imports compatible except the R3-001-corrected gaps; no other behavior change |
| R3-TC-08d **[PLAN]** | Registry coexistence without interference | Implementation present | `get_device_builder`/`register_device_builder` results identical before/after the object registry exists; device model strings need not change | Object and device registries are fully distinct; no existing device builder registration is disturbed |
| R3-TC-08e **[PLAN]** | No new required dependencies | Implementation present | `git diff dev...HEAD -- pyproject.toml setup.py` | No new required dependencies; optional extras untouched |
| R3-TC-08f **[PLAN]** | Candidate CI evidence | Candidate commit pushed | CI run on the task branch (both Python 3.9 and 3.13 matrix jobs) green | Recorded run ID, branch, head SHA and job conclusions in the results document — regression, compile/import and warning evidence precede integration (citing the pre-merge task-branch run, per the R3-001 REC-1 lesson) |

## Traceability summary

| AC (R3-002 scope) | Test cases |
| --- | --- |
| R3-AC-04 — existing device use remains compatible | R3-TC-04l (automated regression); R3-TC-08a/08c (automated) |
| R3-AC-04 — object membership | R3-TC-04a–04e (automated) |
| R3-AC-04 — explicit lookup | R3-TC-04d (automated); existing device-side behavior pinned by R3-TC-04l |
| R3-AC-04 — naming (no silent shadowing) | R3-TC-04f–04i (automated) |
| R3-AC-04 — builder discovery/construction | R3-TC-04j, 04k, 04m (automated) |
| R3-AC-04 — failed-build safety | R3-TC-04n–04p (automated); device-path analog already pinned by R3-TC-04l |
| R3-AC-04 — independent instances | R3-TC-04q, 04r (automated) |
| R3-AC-08 — compatibility and evidence | R3-TC-08a–08f (automated); CHK-04-1 (reviewer checklist) |
| (out of scope, R3-003/004) | connected removal rejection; shared-active-ownership conflicts; connected example; clock/tick/reset — **not planned here** |

Manual vs automated, explicitly: automated `unittest` cases are
R3-TC-04a–04l, 04n–04r and R3-TC-08a–08f; manual record checks are
R3-TC-04m if the design lands construction in a non-station module
(then its station wiring is verified by file/line citation against the
approved design), plus reviewer checklist CHK-04-1 (scope gate, same
convention as Release 2 CHK-06-1 / R3-001 CHK-08-1):
`git diff dev...HEAD --stat -- softlab/huo softlab/jin softlab/shui
softlab/mu` must be empty; production runtime changes confined to
`softlab/tu/`; no connected-simulation production code
(coordinator/edges/clock) may appear in this task's diff — that scope
belongs to R3-003. In addition, CHK-04-1 pins the "documented" half of
R3-TC-04o: the station-level object-construction entry's docstring must
document the unknown/unregistered-model error (class or category, per
the approved design), verified by a file/line citation of that docstring
recorded in the results document.

## Risks and notes for the design/implementation review

1. **API names are design work.** This plan pins observable behavior;
   placeholder spellings (`add_object`, `build_object`,
   `ObjectBuilder`, `register_object_builder`) denote capabilities.
   The tester will rebind fixtures to the approved detailed design and
   record the rebind in the results document.
2. **Rename-after-add defect exists on the device side (baseline §C).**
   R3-TC-04i asserts the object side does not inherit it; fixing the
   device side is Release 1 debt, **not** in scope here.
3. **Exception class for unknown object model** is design work
   (device analog raises `RuntimeError`); R3-TC-04o asserts only an
   explicit documented error with unchanged membership — "explicit" is
   pinned by the automated case, "documented" by CHK-04-1's docstring
   file/line citation.
4. **Delegation precedence is existing behavior** (real members win
   over delegated names; device.py lines 129–133 document the
   OBS-006 category). R3-TC-04d requires the same precedence and the
   same explicit-lookup escape hatch for objects; the delegation
   wiring for a second member dict is design work.
5. **Global registry state leakage between tests:** the device
   registry is module-global (device.py line 556). New object-registry
   tests must isolate with the same `patch.dict(..., clear=True)`
   pattern as the existing device-registry test
   (test_tu_contracts.py lines 184–186) and must not leak builders
   into other tests — the existing test already models the cleanup
   discipline.
6. **No connected simulation in this task.** Any test needing edges,
   a coordinator, ownership or `sim_dt` is out of scope; CHK-04-1
   enforces this at review.

## Revision history

- **2026-10-07 — Revision 1** (tester: sw-mike): initial plan,
  written against the characterization baseline recorded the same day
  in `r3-002-baseline.md` (branch `codex/r3-002-builders`, tip
  `f279958`).
- **2026-10-07 — Revision 2** (tester: sw-mike): review-issue fixes —
  R3-TC-04h reworded so the invalid name enters via the public
  construction/insertion entry's `name` argument (expectation
  unchanged); CHK-04-1 extended to pin the docstring documentation of
  the unknown/unregistered-model error with a file/line citation
  (closing the "documented" half of R3-TC-04o); risk note 3 updated
  accordingly. Case IDs and all other content unchanged.
