# Code Review — R3-002: Object builder/registry and station object membership

Reviewer: sw-celeste | Date: 2026-10-11
Branch: `codex/r3-002-builders` | Range reviewed:
`c25c47a..04f56fe` (implementation commits `1ff6d6b`, `90282ba`,
`06cd8b3`, `a4b717a`, `ceec02f`; phase-record commit `04f56fe` touches
`log/release_3/sprint-board.md` only)
Binding specification:
[r3-002-detailed-design.md](../design/r3-002-detailed-design.md)
(APPROVED at `6973d9b`, architect rulings 1–3 ACCEPT) | Test plan:
[r3-002-test-plan.md](../test/r3-002-test-plan.md) (Revision 2) |
Baseline: [r3-002-baseline.md](../test/r3-002-baseline.md)

## Verdict: APPROVED

- **Total issues: 0** (critical: 0, high: 0, medium: 0, low: 0)
- The `a4b717a` fixture corrections were audited and are genuinely
  fixture-side (see the fixture-correction audit below); neither masks
  a production defect.

## What was executed (commands + results)

All commands from the repository root (or scratch worktrees under the
system temp dir) using the project `.venv` (Python 3.13), matching
AGENTS.md verification §1:

| Command | Result |
| --- | --- |
| `git fetch origin`; HEAD check | HEAD at `04f56fe`, working tree clean, in sync with `origin/codex/r3-002-builders` |
| `git diff --stat c25c47a..04f56fe` | 6 files: `softlab/tu/simulation/builder.py` (+101, NEW), `softlab/tu/simulation/__init__.py` (+5), `softlab/tu/station/station.py` (+110), `tests/test_tu_station_objects.py` (+317, NEW), `tests/test_tu_contracts.py` (+18), `log/release_3/sprint-board.md` (phase record) — exactly the design's file-level change list plus the phase record |
| `git diff c25c47a..04f56fe --stat -- softlab/huo softlab/jin softlab/shui softlab/mu` | **empty** — production changes confined to `softlab/tu` (CHK-04-1 scope gate satisfied) |
| `git diff c25c47a..04f56fe -- pyproject.toml setup.py` | **empty** — no dependency/config changes (R3-TC-08e satisfied trivially) |
| Connected-simulation grep (`coordinator`/`sim_dt`/`edge`/`tick`) over `softlab/tu` | only pre-existing `visa.py` doc text and the designed `rm_object` forward note (station.py:207) — no coordinator/edge/clock production code (CHK-04-1 satisfied) |
| `git diff c25c47a..04f56fe --check` | clean, exit 0 |
| `awk 'length > 80 …'` over all four touched Python files | no line exceeds 80 chars |
| `python -m unittest discover -s tests -p 'test_*.py'` | **Ran 165 tests, OK** (147 baseline anchors + 18 new) |
| `python -W error::Warning -m unittest discover -s tests -p 'test_*.py'` | **Ran 165 tests, OK, zero warnings** |
| `python -m compileall -q softlab` | exit 0 |
| Import smoke + class identity (`softlab.tu.simulation.SimulatedObject is softlab.tu.simulation.object.SimulatedObject`; new names importable from the package) | pass, version `0.3.0` (R3-TC-08c pattern) |
| Red-commit verification (`git worktree` at `1ff6d6b`, run `tests.test_tu_station_objects`) | **fails by `ImportError: cannot import name 'ObjectBuilder' from 'softlab.tu.simulation'`** — the documented red mode; the sprint-board TDD-red claim is truthful |
| Vacuous-pass / mutation checks (worktree at `04f56fe`, 5 targeted production mutations, each reverted after the run) | all 5 mutants killed by exactly the designed tests — see the mutation table below |
| Board record check (`git show 04f56fe -- log/release_3/sprint-board.md`) | TDD red/green commits, fixture-fix commit, companion-ruling disposition ("implemented, passing") and self-test evidence (165 OK, zero warnings, compile/import clean) all recorded and independently reproduced above |

### Mutation results (vacuous-pass guard)

| # | Mutation applied to `station.py` (worktree) | Killed by |
| --- | --- | --- |
| A | Remove `build_object` gate-1 name validation (station.py:248–250) | `test_04h` — the gate-order assertion (`build_object('missing', '')` raises `RuntimeError` instead of `ValueError`) |
| B | Remove `add_object` duplicate-object check (station.py:195–196) | `test_04g`, `test_04r` |
| C | Remove `add_object` device-collision check (station.py:197–199) | `test_04f`, `test_04p` |
| D | Remove the `add_device` object-collision guard (station.py:147–149) | `test_04b` (companion extension), `test_station_shared_namespace_rejects_device_shadowing_object` |
| E | Wrap `builder.build` exception in `RuntimeError(...) from exc` (breaks identity propagation, station.py:255) | `test_04n` (`assertIs` on the exception object) |

The final tests are not ImportError-trivial: they assert real behavior
(identities, membership snapshots, state independence, gate ordering)
and detect each designed failure mode individually.

## Design compliance

- [x] **`ObjectBuilder` member-for-member mirror of `DeviceBuilder`
  (device.py:515–553)**: `__init__` with `str(model)` coercion and
  `ValueError('Empty object builder model')` (builder.py:27–37);
  read-only `model` property (39–42); `__repr__` as
  `f'<ObjectBuilder>{self.model}'` (44–45); base `build` raising
  `NotImplementedError` (47–73) with the design-prescribed subclass
  contract (fresh instance per call, already-owned rejection note,
  exception propagation note) stated verbatim in the docstring.
- [x] **Registry mirror of device.py:556–572**: module-global
  `_object_builders: Dict[str, ObjectBuilder] = {}` with the same
  docstring line (builder.py:76–77); `register_object_builder` with
  `TypeError` type guard, duplicate-model `ValueError`
  (`'Object builder with model {model} has exist'`), store-only
  semantics — registration constructs nothing (80–96);
  `get_object_builder` as `_object_builders.get(model, None)` (99–101).
  The two registries are fully distinct module globals.
- [x] **`build_object` gate ordering (D5, station.py:216–255)**: gate 1
  `if type(name) is not str or len(name) == 0` — **no `str()` coercion**,
  short-circuit order safe for `None` — before any registry access
  (248–250); gate 2 `RuntimeError(f'Failed to get object builder with
  model {model}')` (251–254), the verbatim device analog, documented in
  the docstring (line 234 — the CHK-04-1 citation target); build with
  **no try/except anywhere on the path** (255); insertion via the
  single-mutation `add_object`. Exactly one `build` call per
  `build_object` call.
- [x] **`add_object` (station.py:175–200)**: `TypeError` type guard →
  duplicate-object `ValueError` → device-collision `ValueError` → single
  dict store, in the designed order; mutation is the last statement, so
  failure atomicity is structural. Docstring carries the key == name
  invariant rationale (D3).
- [x] **`rm_object` leniency (station.py:202–210)**:
  `pop(str(object_name), None)`, no cleanup, R3-003 forward note only —
  mirrors `rm_device`.
- [x] **`object()` explicit lookup (station.py:212–214)**:
  `_objects.get(str(object_name), None)` — the OBS-006 escape hatch,
  mirrors `device()`.
- [x] **`objects` property (station.py:84–87)**: live `_objects.keys()`
  view, mirrors `devices`.
- [x] **Membership storage + delegation (station.py:65–66)**:
  `self._objects: Dict[str, SimulatedObject] = {}` followed immediately
  by `self.add_delegate_attr_dict('_objects')`, placed directly after
  the `_devices` lines per the design's implementation note.
- [x] **`add_device` additive guard (station.py:147–149)**: one `if`
  after the existing type check, before the duplicate-device check, with
  the designed message; fires only in a state unreachable before this
  task (D2 compatibility argument holds — verified by the unchanged
  147-test anchor plus mutation D above).
- [x] **Class docstring updated additively** (station.py:31–53): shared
  namespace paragraph, `objects` property, and the four new methods
  listed; no existing line removed.
- [x] **`__init__` re-export order** (`softlab/tu/simulation/__init__.py`):
  `SimulatedObject` first, then `ObjectBuilder`,
  `register_object_builder`, `get_object_builder`; `builder.py` imports
  `SimulatedObject` from `softlab.tu.simulation.object` directly
  (builder.py:16), so no cycle exists; import direction honors the
  architecture plan (nothing under `tu/simulation/` imports
  `tu/station`). Import smoke passes.
- [x] **Untouched files as designed**: `object.py`, `device.py`,
  `build_device` body, `snapshot()`, `describe()` are byte-identical to
  the base (verified by diff); the Release 1 debt (C-3 rename-after-add
  orphaning, OBS-004/005/006, DEFECT-2) is untouched.
- [x] **Zero third-party dependencies** — the design's dependency table
  is empty and the diff adds none.

## Test-plan traceability (walked against the code)

| Case | Implemented? | Basis |
| --- | --- | --- |
| R3-TC-04a | ✓ | `test_04a_add_and_look_up_object`: `assertIs` explicit lookup, delegation access, exactly-once listing, devices listing unchanged |
| R3-TC-04b | ✓ (+ companion ruling) | `test_04b_membership_namespaces_are_independent`: cross-lookup `None` both ways; **companion-ruling extension** asserts `add_device(Device('o'))` → `ValueError` with the object member untouched — the architect-authorized fixture extension, disposition recorded on the board |
| R3-TC-04c | ✓ | `test_04c_remove_object_is_lenient`: first removal returns the object, lookup `None`, re-removal and missing-name removal return `None` |
| R3-TC-04d | ✓ | `test_04d_explicit_lookup_survives_method_name_collision`: names `snapshot`/`add_device`/`devices`; real members win; explicit `object(name)` returns the member |
| R3-TC-04e | ✓ | `test_04e_rejects_non_object`: `Device`, `str`, `None` all → `TypeError`; both memberships unchanged |
| R3-TC-04f | ✓ | `test_04f_rejects_object_shadowing_device`: `ValueError`; device `acc` stays the only `acc`; object list empty (mutation C confirms it kills the missing check) |
| R3-TC-04g | ✓ | `test_04g_rejects_duplicate_object_name`: `ValueError`; first member `assertIs`-retrievable and unmodified (mutation B confirms) |
| R3-TC-04h | ✓ | `test_04h_invalid_name_rejected_before_mutation`: `''`, `123`, `None` enter via the **public entry's `name` argument** (Revision 2 wording); gate order pinned by `build_object('missing', '')` → `ValueError` (mutation A confirms the gate, not the constructor, is what is tested) |
| R3-TC-04i | ✓ | `test_04i_object_names_are_immutable_for_members`: rename raises `AttributeError`; key == name invariant holds — the "rename forbidden" branch of the expectation |
| R3-TC-04j | ✓ | `test_04j_builder_convention_mirrors_device_builder`: `model` identity, `repr` format (design-pinned, not over-constraint), base `build` → `NotImplementedError`, registry round-trip under `patch.dict(..., clear=True)` |
| R3-TC-04k | ✓ | `test_04k_registry_semantics`: (a) duplicate `ValueError`; (b) device and object builders share model `'shared'` across **both patched registries** without interference; (c) `build_count == 0` after registration; plus `TypeError` on non-builder and `ValueError` on empty model |
| R3-TC-04l | ✓ (regression) | `test_station_builder_registry_and_default_are_restored` passes unmodified inside the 165-test run; `device.py` byte-untouched |
| R3-TC-04m | ✓ (automated) | `test_04m_build_object_inserts_member`: construction lands in the station module per design, so the manual fallback does not apply; `kwargs` forwarding exercised (`ignored=True`) |
| R3-TC-04n | ✓ | `test_04n_builder_exception_propagates_unchanged`: **`assertIs` exception identity**; membership snapshot equality **plus per-member `assertIs` loops** for both namespaces — bit-identical pinning as required (mutation E confirms the identity pin) |
| R3-TC-04o | ✓ | `test_04o_unknown_model_is_documented_runtime_error`: `RuntimeError`, membership unchanged, and the CHK-04-1 docstring pin (`'RuntimeError' in Station.build_object.__doc__`); manual citation: station.py:234 |
| R3-TC-04p | ✓ | `test_04p_membership_validation_failure_after_build`: build succeeds, insertion-side `ValueError`; device `acc` untouched; membership snapshot equal (mutation C confirms) |
| R3-TC-04q | ✓ | `test_04q_built_instances_have_independent_state`: two `build_object` calls, `assertIsNot`, mutate `a` only, states `1.0` vs `0.0`; no shared fixture state, no same-instance-twice — registry is `patch.dict`-isolated per test |
| R3-TC-04r | ✓ | `test_04r_already_owned_instance_rejected`: same-instance builder; second insertion → `ValueError`; first member unmodified; single-instance reuse cannot alias two names (mutation B confirms the rejection is the duplicate-name check) |
| R3-TC-08a/08b | ✓ | 147-test baseline recorded in `r3-002-baseline.md`; post-implementation run: 165 OK, zero warnings under `-W error::Warning`, compileall exit 0, import smoke clean (reproduced by this review) |
| R3-TC-08c | ✓ | all pre-existing `tests/test_tu_*.py` suites pass unmodified; SIM-001 import identity re-verified |
| R3-TC-08d | ✓ | registries distinct (04k-b); no device registration disturbed (04l green) |
| R3-TC-08e | ✓ | `pyproject.toml`/`setup.py` diff empty |
| R3-TC-08f | ⏳ tester-owned | candidate CI evidence is recorded by sw-mike in the test-results document; not this review's gate |
| CHK-04-1 | ✓ (reviewer-owned) | scope diff empty; production confined to `softlab/tu/`; no connected-simulation code; `build_object` docstring names the unknown-model `RuntimeError` — citation: `softlab/tu/station/station.py:234` (recorded here for the test-results document) |

## Fixture-correction audit (`a4b717a`)

Both corrections are genuinely fixture-side; neither masks a production
defect:

1. **`FailingBuilder` missing `__init__`** (test_04n): without
   `super().__init__('failing')`, constructing the fixture itself would
   raise `TypeError` before any production code ran — a pure fixture
   defect. The corrected fixture now exercises the designed
   exception-propagation path (verified by mutation E).
2. **04r lookup key `a` → `alias`**: the misbehaving
   `SameInstanceBuilder` names its single pre-owned instance `'alias'`
   and `build` ignores the `name` argument; per D3 the station inserts
   under `obj.name`, so the first member lands under `'alias'`. The
   original assertions looked up `'a'`, contradicting the design's
   insertion rule — a fixture misconception, not a production defect.
   The corrected test still pins the designed rejection: the second
   insertion is rejected by the duplicate-name check (mutation B kills
   it), the first member is unmodified, and no `'b'` key is created.

## Assertion-integrity audit (full test diff)

- [x] No existing assertion weakened or removed: `test_tu_contracts.py`
  gains one appended test method (+18 lines); every pre-existing method
  is byte-identical (verified by diff).
- [x] No byte-overconstraint beyond the plan: error **classes** are
  asserted, not message texts; the only representation pin
  (`repr(builder) == '<ObjectBuilder>accumulator'`) is a format the
  design itself prescribes verbatim.
- [x] Registry isolation follows the established
  `patch.dict(..., clear=True)` pattern (test-plan risk note 5); no
  builder leaks into other tests (full-suite order-independent pass).
- [x] 04q/04r independence confirmed: fresh stations per test, fresh
  builders per registration, no shared mutable fixture state.

## Conventions (AGENTS.md)

- [x] English docstrings in the repo style; `build_object`,
  `ObjectBuilder` and the registry functions carry the Release 2
  structured Args/Returns/Errors/Side-effects form; short member methods
  match the surrounding `station.py` style
- [x] Type annotations on all new public API; signatures match the
  design's interface tables verbatim
- [x] Black line width 80 on all touched Python files (verified
  mechanically)
- [x] No new dependencies; no `pyproject.toml`/`setup.py` change
- [x] No unrelated changes; no Release 1 debt repairs smuggled in
- [x] No compiler or lint warnings (`compileall` clean; suite clean
  under warnings-as-errors)
- [x] Records truthful: board claims (TDD red/green commits, 165 OK,
  zero warnings, compile/import clean, companion-ruling disposition)
  independently reproduced by this review

## Simplicity audit

- [x] No third-party dependency added — stdlib `typing` plus existing
  repository modules only, per the design's empty dependency table
- [x] No over-engineering: exactly two new dicts of state
  (`Station._objects`, `builder._object_builders`); no identity scan in
  `add_object` (D6 — the key == name invariant makes it unnecessary);
  no rename handling (D3); no snapshot/describe schema addition (D1);
  no shared/fluent builder refactor (scope guard)
- [x] New public surface is exactly the designed six names
  (`ObjectBuilder`, `register_object_builder`, `get_object_builder`,
  `add_object`/`rm_object`/`object`/`objects`/`build_object`) — nothing
  more
- [x] Test fixtures are plain synthetic builder subclasses and closures;
  the only test helper (`membership_snapshot`) directly serves the
  04n/04o/04p bit-identical pins

Non-blocking observation (no action required): `StationBuildObject.setUp`
loops over a 1-tuple of patch targets — harmless and future-proofing;
left as-is.

## Quality gates

- [x] No compiler errors / warnings; zero warnings under
  `-W error::Warning`
- [x] Regression: 165 tests OK (147 R3-AC-08 anchors + 18 new), suite
  clean in full-discovery and single-file runs
- [x] Import smoke and SIM-001 class-identity check
- [x] CHK-04-1 verified by `git diff` inspection plus docstring
  citation (station.py:234) — recorded here for the test-results record

## Approval

- [x] Implementation faithful to the approved detailed design — every
  interface signature, gate ordering, error class and message, and
  docstring contract matched member-for-member; no undocumented
  deviation
- [x] Implementation faithful to the approved test plan (Revision 2),
  including the companion-ruling extension and both `a4b717a` fixture
  corrections (audited fixture-side)
- [x] Code meets quality and simplicity standards
- [x] **APPROVED — ready for testing by sw-mike** (R3-TC execution
  phase). CHK-04-1 is discharged by this review (citation:
  `softlab/tu/station/station.py:234`) and should be recorded as
  verified in the test-results document.
