# Release 12 (M03 Phase 2) — Schema/Codec Test Results

Owner: Tester (sw-mike) | 2026-11-01 Asia/Shanghai
Branch: `codex/m03-p2-schema`
Executable suite: `tests/test_m03_p2_schema.py` (commit see below)
Plan: [schema-test-plan.md](schema-test-plan.md) (delta-revised `25a0c2e`,
developer APPROVED `8035958`)
Implementation under test: `a437dc3` (code review APPROVED `57fc19d`,
zero defects)
Verdict: **ALL AREAS PASS — zero defects found, zero warnings**

## Execution record

Exact commands (from the repository root, project venv):

```sh
conda activate "$PWD/.venv"
python -m unittest discover -s tests -p 'test_*.py'
python -m compileall -q softlab
python -c "import softlab; print(softlab.__version__)"
```

| Check | Command | Result |
| --- | --- | --- |
| Phase 2 suite alone | `python -m unittest tests.test_m03_p2_schema` | exit 0 — **Ran 54 tests, OK** |
| Full regression + Phase 2 suite | `python -m unittest discover -s tests -p 'test_*.py'` | exit 0 — **Ran 459 tests, OK** (405 pre-existing baseline + 54 new) |
| Bytecode compilation | `python -m compileall -q softlab` | exit 0, no output |
| Import smoke | `python -c "import softlab; print(softlab.__version__)"` | exit 0 — `0.3.0` |
| Warnings scan | `python -W once -m unittest discover -s tests -p 'test_*.py'` | zero warning lines in the entire 459-test run |

Environment actually observed:

| Item | Observed value |
| --- | --- |
| Python | `3.13.15 \| packaged by conda-forge \| (main, Aug 10 2026, 13:01:04) [Clang 19.1.7]` |
| Platform | `macOS-26.6.2-arm64-arm-64bit-Mach-O` |
| softlab | `0.3.0` |
| numpy | `2.5.3` |
| pandas | `3.0.6` |
| h5py | `3.16.0` |

### Warnings observed (verbatim record)

**None.** The full 459-test run (including the 54 new tests) emits zero
warnings; `python -W once` re-run confirms nothing is hidden by default
filters. No warning is suppressed or filtered inside test code. This
contrasts with the Phase 1 baseline record, where one expected
`sqlite3.version` deprecation was observed — that probe lives only in the
Phase 1 suite; the Phase 2 suite adds no new warning source.

Informational stdout lines from the S6.2 legacy-fixture example execution
(`Connected to HDF5 backend, data is stored in
/var/folders/.../softlab-m03-p2-*/thermal.h5.`) are the example module's
own prints, not warnings; every backend created there is tracked by the
harness double and disconnected on cleanup.

## Per-area verdicts

| Area | Tests | Verdict |
| --- | --- | --- |
| S1 valid round-trips (S1.1–S1.7) | 7/7 | **PASS** |
| S1.8 group-construction seam (recorder double) | 4/4 | **PASS** |
| S2 failure matrix F1–F17 | 19/19 | **PASS** |
| S3 type strictness / containers / huge integers / null-vs-absent | 7/7 | **PASS** |
| S4 size/depth/count limits + constant-sourced values | 5/5 | **PASS** |
| S5 side-effect-free failure | 5/5 | **PASS** |
| S6 legacy preservation + export surface | 3/3 (+ S6.1 via full suite) | **PASS** |
| S7 environment and hygiene | 4/4 | **PASS** |
| **Total** | **54/54** | **PASS** |

## S1 — Valid round-trips

- **S1.1/S1.2 (7 probes incl. subtests):** canonical thermal (coordinated
  and direct variants, frozen delay/tick pairs `1/'tick-1'` and `0/'tick'`)
  and signal envelopes encode→decode deep-equal, decode idempotent;
  signal stored columns are exactly `timestamp, signal, reference` with
  `difference` absent; every column carries a nonempty explicit unit.
- **S1.3:** byte-identical output from independently constructed equal
  inputs across all three envelope variants; encoded text verified
  byte-equal to `json.dumps(..., sort_keys=True, allow_nan=False,
  ensure_ascii=True, separators=(',', ':'))` of its own reparse and is
  ASCII-only — canonical serialization frozen in design §2.2/§8.6 holds.
- **S1.4:** `execution_id`/`group_id` survive with roles unchanged;
  `execution_id == group_id` hard-rejected
  (`execution_id: must differ from group_id`); uppercase, hyphen-stripped
  and brace-wrapped UUID text rejected (`<path>: expected canonical UUID`)
  on both identity fields.
- **S1.5:** column order preserved exactly; frozen count relations hold
  (thermal `requested == completed == configuration.points ==
  procedure.requested_points == boundary.final_tick`; signal
  `payload.points == requested_rows`); empty-string unit rejected.
- **S1.6:** snapshot/acquisition/stage literals round-trip verbatim;
  later-stage success and failed acquisition rejected with frozen
  unsupported-literal messages.
- **S1.7:** available/unavailable fact structure asserted verbatim
  (64-hex digest, 40/64-hex revision, boolean dirty, null bindings,
  bounded vocabulary); both states round-trip; captured-git variant
  exercised. Structural-only responsibility split per [R6] respected —
  no capture-truthfulness claim is made or tested.
- **S1.8 (4 tests):** real-group behavior (empty `DataGroup`, id from
  envelope, meta dict exactly `{run_record: text}`); recorder double
  proves encode-before-construct ordering, no `meta=` constructor
  argument, exactly one `set_meta` with the exact encoded text;
  failure atomicity (invalid envelope ⇒ `RecordValidationError`, zero
  constructor calls); name probes (`5` ⇒ builtin `TypeError
  'run_group: name must be an exact str'`, `''` ⇒ builtin `ValueError
  'run_group: name must be nonempty'`, both before construction).

## S2 — Failure matrix F1–F17

All 19 methods pass; every probe asserts exact frozen exception type
(`assertIs(type(...))`, never a subclass loophole) and verbatim frozen
message:

- **F1** malformed JSON ⇒ `RecordDecodeError`, parser detail verbatim.
- **F2** wrong root type (array/text/number/bool/null) ⇒
  `RecordDecodeError, 'run_record: root must be an object'`.
- **F3** duplicate keys at top level and nested inside the client
  payload (raw-text injection) ⇒ `run_record: duplicate object key '...'`.
- **F4** object/set/bytes/complex/NumPy scalar/NumPy array/callable/
  benign-`__str__` instance ⇒ `<path>: unsupported value type '<typename>'`
  — no stringification fallback.
- **F5** `NaN`/`Infinity`/`-Infinity` tokens ⇒ parse_constant message;
  `1e999` overflow literal caught by the recursive finiteness sweep at
  top level and three levels down (`a.b.c: nonfinite number`); encode
  side `nan`/`inf` rejected with `<path>: nonfinite number`.
- **F6** each of the ten root keys, encode and decode ⇒
  `missing required field '<key>'`.
- **F7** missing thermal `boundary` / signal `read_policy` attributed at
  `payload: ...`.
- **F8** versions `2, 0, '1', 1.0, True` ⇒
  `version: unsupported version (supported: 1)`.
- **F9** payload version 2, both profiles ⇒
  `payload.version: unsupported version (supported: 1)`.
- **F10** unknown profile ⇒ `profile: unsupported client profile`.
- **F11** closed policy at every layer (root, `record`, column, `source`,
  fact, payload), including case-differing `Namespace` and trailing-
  underscore `namespace_` ⇒ `<path>: unknown field '<key>'`; decode side
  shares the contract message. The rejected alternative
  (unknown-fields-preserved) is recorded here per the plan: closed records
  are mandated by decision §1.1 namespace row and §6 item 4; changing this
  boundary is an explicit decision revision, not a test relaxation.
- **F12** strict-type probes (text/`1.0`/`True` at integer fields, `1`/
  `'true'` at boolean, number at text, object at array, tuple at array
  via sweep) ⇒ frozen `expected exact integer | boolean | text | array`
  messages.
- **F13** profile semantics: thermal six-column ordering, smuggled
  fourth signal column, coordinated/direct clock-policy pairs,
  configuration positivity and `0 < dt*G/C <= 1`, boundary cross-checks
  (`final_tick`, `final_sim_time` within 1e-10), frozen record names
  (`thermal_trace`/`signal`), frozen `THERMAL_MODEL` drift (equation,
  method, validity, unit value change, unit removal) and
  `SIGNAL_OFFSETS` drift (value/zero/negative/extra/renamed/missing key)
  with the exact offsets message; count-mismatch probes (a/b/c) assert
  the three frozen messages on encode, plus the signal decode side.
- **F14** explicit null without assigned meaning probed across ten field
  classes (root, nested object, text, integer, real, fact object) ⇒
  `<path>: explicit null has no assigned meaning`.
- **F15** malformed digest (non-hex, bad length, uppercase), wrong-typed
  fact values on available facts, invalid state literal, empty
  `source.reference` ⇒ frozen `<path>: invalid available value` /
  `expected nonempty text`. Structural-only scope respected.
- **F16** reason outside vocabulary, unavailable-without-reason,
  available-with-reason, git state binding (`git_dirty.state` vs
  `git_revision.state`) ⇒ all three frozen binding templates.
- **F17** unpaired-surrogate decode input ⇒
  `RecordDecodeError, 'run_record: input text is not encodable as UTF-8
  (unpaired surrogate)'`.

## S3 — Type strictness, containers, huge integers, null policy

- Boolean≠integer both directions (encode and JSON-token decode sides);
  boolean field fed `1`/`0` rejected; no implicit stringification
  (benign-`__str__` object fails; numeric-looking strings rejected);
  `1.0` rejected at integer fields, accepted and normalized to `float`
  at real fields (int-configured reals also normalize); negative zero
  round-trips exactly (`-0.0` with copysign check).
- Container non-normalization: root `OrderedDict`/`MappingProxyType`/
  list/tuple/text/int ⇒ builtin `TypeError, 'run_record: expected dict'`;
  nested dict subclass/mappingproxy/tuple ⇒
  `<path>: unsupported value type '<typename>'`; codec/builder
  signatures carry only concrete `Dict`/`List` parameters.
- Huge integers (signal-profile pinning): `10**1023` (smallest 1024-digit
  value) encodes/decodes with matching counts; `10**1024` rejected on
  encode (`outcome.requested_rows: integer digit count exceeds limit
  1024`) and on decode by the sign-stripped token digit bound
  (`run_record: integer digit count exceeds limit 1024`); negative
  1024-digit magnitude accepted by the bound on both layers (contract
  layer then rejects the probe's unknown key — the sweep acceptance is
  what is asserted); finite-real field fed `10**310` maps the builtin
  `OverflowError` to `<path>: expected finite real`.
- Null vs absent: absence ⇒ F6; present-null accepted only in the bound
  fact slots (unavailable `value:null`, available `reason:null`) and
  round-trips; mis-bound nulls rejected via F15/F16 templates; the three
  states are asserted semantically distinct; decode of non-`str` raises
  builtin `TypeError, 'run_record: expected JSON text'`. The vacuous
  "optional field with assigned null meaning" case is recorded as vacuous
  by design (no optional fields exist, design §8.5).

## S4 — Limits

- **Depth:** 8-deep container at an extra root key passes the sweep and
  fails at contract (`unknown field 'extra'`); scalar child of a depth-8
  container accepted by the sweep; 9-deep rejected by the sweep on both
  encode (`RecordValidationError`) and decode (`RecordDecodeError`) with
  `run_record: nesting depth exceeds limit 8`; 20,000-deep raw text
  (40,000 bytes, within the size bound) rejected with
  `RecordDecodeError, 'run_record: nesting exceeds parser recursion
  limit'` — builtin `RecursionError` mapped, never escapes.
- **Size:** exactly 131,072 bytes (`{}` + whitespace) passes the size
  gate and fails contract validation (`missing required field
  'namespace'` — proving gate acceptance); 131,073 bytes rejected by the
  gate with `run_record: encoded size exceeds limit 131072 bytes`.
- **Maximal valid envelope:** four free-text fields at the 1,024-code-point
  cap with worst-case non-BMP escaping plus signal counts at the
  1,024-digit cap encodes to a size `<= 66472` bytes — well under
  `MAX_ENCODED_BYTES` (≈1.97× headroom) — and round-trips exactly.
- **Counts:** array 64/65 and object 64/65 layered (64 passes sweep ⇒
  contract failure; 65 rejected by the sweep with frozen messages, both
  layers for the array); string 1024 accepted in a valid envelope, 1025
  rejected by the sweep; reason length probed at the contract layer
  (64-char reason passes the length gate and fails vocabulary; 65 chars
  rejected with `reason length exceeds limit 64` on both encode and
  decode).
- **S4.4:** all seven limits asserted equal to the frozen design values
  read from `softlab.shui.run_record`'s own constants, and asserted
  absent from `softlab.shui`.

## S5 — Side-effect-free failure

Failed decode leaves no contamination (repeated failures then success on
the shared module-level instance); failed encode (contract-stage deep in
the payload, and sweep-stage with an equality-stable probe value) leaves
the caller's envelope deep-equal to its pre-encode snapshot; errors raise
— a recording log handler observed zero records during a representative
failure; `encode_envelope`/`decode_envelope`/`build_run_group` signatures
expose no backend/file parameter (positional-or-keyword only); the suite
never calls `sys.set_int_max_str_digits` and the 1,024-digit acceptance
proves the codec bound — not interpreter state — enforces the integer
limit.

## S6 — Legacy preservation and export surface

- **S6.1:** the full discovery run keeps every one of the 405 baseline
  tests green unmodified (459 = 405 + 54); no pre-existing test was
  removed, skipped, or weakened.
- **S6.2:** the thermal example runs against per-adapter temp dirs
  (coordinated ⇒ manifest v1, direct ⇒ manifest v2); stored text decodes
  through the legacy reader with content equality asserted (schema,
  version, `run_id` == group id, points, completed rows, clock delay).
  Fixtures are generated in-suite; nothing binary is committed.
- **S6.3:** the `softlab.shui` feature re-export surface is exactly the
  frozen §8 table — `RunRecordCodec`, `JsonRunRecordCodec`,
  `encode_envelope`, `decode_envelope`, `build_envelope`,
  `build_thermal_payload`, `build_signal_payload`, `build_run_group`,
  `RunRecordError`, `RecordDecodeError`, `RecordValidationError` (11
  names) — with no other `__all__` name leaking; all seven `MAX_*`
  limits raise `ImportError` from the package and import cleanly from
  `softlab.shui.run_record`; the other module-only constants
  (`NAMESPACE` ... `SIGNAL_OFFSETS`) stay module-only; pre-existing
  exports (`DatabaseBackend`, `catch_error`, `data`, `profile`)
  unchanged.
- **S6.5:** a group with no additive key behaves exactly as before
  (`get_meta`/`meta` empty).

## S7 — Environment and hygiene

- Fixtures are fully programmatic (UUIDs, versions, digests captured at
  test time); temp dirs only; no notebooks executed.
- No `autospec`/`wraps` keywords at any mock.patch call site — enforced
  by an AST self-inspection test, not by convention.
- `isinstance(..., ValueError)` is asserted only for the two leaf error
  classes (`RecordDecodeError`, `RecordValidationError`); `RunRecordError`
  is asserted by exact type/relationship only, matching the design §3
  class table and the developer-review executable-suite note.
- No hardcoded environment facts: versions are captured at runtime;
  dependency versions are recorded above as observations, never asserted.
- No schedulers/VISA resources opened; the S6.2 example's HDF5 backends
  are tracked and disconnected.

## Deviations and open items (honest record)

1. **Re-export list cardinality:** the handoff instruction described a
   "15-name re-export list"; the approved plan (`8035958`) and frozen
   design §8 table specify **11** package-level feature names. The suite
   asserts the approved 11 (15 plausibly counts the 4 unchanged
   pre-existing exports `DatabaseBackend`, `catch_error`, `data`,
   `profile` alongside). No discrepancy in the implementation.
2. **Python 3.9 evidence (plan S7.7):** no local 3.9 interpreter exists
   (`conda env list` shows none); this run is Python 3.13.15 only. Per
   S7.7 the 3.9 portability evidence must be recorded from CI for the
   candidate commit before handoff — the suite was written for version
   portability (token-based integer bound, mapped `RecursionError`,
   no version-string assertions), but a 3.13-only pass is not claimed as
   portability proof. **Status: pending CI 3.9 run.**
3. No genuine implementation defects were found; no assertion was
   weakened relative to the approved plan.

## Traceability

| Plan area | Suite class | Result |
| --- | --- | --- |
| S1.1–S1.7 | `S1ValidRoundTrips` | 7/7 PASS |
| S1.8 | `S1GroupConstructionSeam` | 4/4 PASS |
| S2 F1–F17 | `S2FailureMatrix` | 19/19 PASS |
| S3 (incl. S3.5) | `S3TypeStrictness` | 7/7 PASS |
| S4 (incl. S4.4) | `S4Limits` | 5/5 PASS |
| S5 | `S5SideEffectFreeFailure` | 5/5 PASS |
| S6 | `S6LegacyPreservation` + full-suite run | 3/3 + 405/405 PASS |
| S7 | `S7EnvironmentHygiene` | 4/4 PASS |
