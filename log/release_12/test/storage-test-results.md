# Release 12 (M03 Phase 3) — Storage and Inspection Test Results

Owner: Tester (sw-mike) | 2026-11-03 Asia/Shanghai
Branch: `codex/m03-p3-storage`
Executable suite: `tests/test_m03_p3_storage.py` (commit see below)
Plan: [storage-test-plan.md](storage-test-plan.md) (ordering-delta revised
`d7d7169`, developer re-review APPROVED `5ca51d1`)
Implementation under test: `0217593` (code review APPROVED `7b1d771`,
zero blocking findings)
Verdict: **ALL AREAS PASS (A1–A8) — zero defects found; A9 recorded
below. Zero warnings on Python 3.13.15; only the recorded pandas 3.9
baseline warning on Python 3.9.23.**

## Execution record

Exact commands (from the repository root):

```sh
conda activate "$PWD/.venv"   # Python 3.13.15
python -m unittest discover -s tests -p 'test_*.py'
python -m compileall -q softlab
python -c "import softlab; print(softlab.__version__)"
python -W once -m unittest discover -s tests -p 'test_*.py'
# Python 3.9.23 scratch env (per Phase 1 correction record):
/tmp/py39/bin/python -m unittest discover -s tests -p 'test_*.py'
/tmp/py39/bin/python -m compileall -q softlab \
    tests/test_m03_p3_storage.py tests/run_record_inspect_bootstrap.py
/tmp/py39/bin/python -c "import softlab; print(softlab.__version__)"
```

| Check | Interpreter | Command | Result |
| --- | --- | --- | --- |
| Phase 3 suite alone | 3.13.15 | `python -m unittest tests.test_m03_p3_storage` | exit 0 — **Ran 48 tests, OK** |
| Full regression + Phase 3 suite | 3.13.15 | `python -m unittest discover -s tests -p 'test_*.py'` | exit 0 — **Ran 507 tests, OK** (459 baseline + 48 new) |
| Bytecode compilation | 3.13.15 | `python -m compileall -q softlab` | exit 0, no output |
| Import smoke | 3.13.15 | `python -c "import softlab; print(softlab.__version__)"` | exit 0 — `0.3.0` |
| Warnings scan | 3.13.15 | `python -W once -m unittest discover -s tests -p 'test_*.py'` | zero warning lines in the entire 507-test run |
| Phase 3 suite alone | 3.9.23 | `/tmp/py39/bin/python -m unittest tests.test_m03_p3_storage` | exit 0 — **Ran 48 tests, OK** |
| Full regression + Phase 3 suite | 3.9.23 | `/tmp/py39/bin/python -m unittest discover -s tests -p 'test_*.py'` | exit 0 — **Ran 507 tests, OK** |
| Bytecode compilation | 3.9.23 | `/tmp/py39/bin/python -m compileall -q softlab tests/test_m03_p3_storage.py tests/run_record_inspect_bootstrap.py` | exit 0, no output |
| Import smoke | 3.9.23 | `/tmp/py39/bin/python -c "import softlab; print(softlab.__version__)"` | exit 0 — `0.3.0` |

Environment actually observed:

| Item | Python 3.13.15 (project venv `.venv`) | Python 3.9.23 (scratch `/tmp/py39`) |
| --- | --- | --- |
| Platform | macOS-26.6.2-arm64-arm-64bit-Mach-O | macOS-26.6.2-arm64-arm-64bit |
| softlab | `0.3.0` | `0.3.0` |
| numpy | `2.5.3` | `2.0.2` |
| pandas | `3.0.6` | `2.3.3` |
| h5py | `3.16.0` | `3.14.0` |
| libhdf5 | `2.2.0` | `1.14.6` |
| sqlite3 (library) | `3.53.4` | `3.54.1` |
| pyvisa-sim | installed (project venv) | `0.6.0` (installed before the final 3.9 full run — see Deviations) |

### Warnings observed (verbatim record)

**Python 3.13.15: none.** The full 507-test run (including the 48 new
tests) emits zero warnings; the `-W once` re-run confirms nothing is
hidden by default filters. No warning is suppressed or filtered inside
test code.

**Python 3.9.23: one distinct warning, repeated** (211 occurrences in
the full-suite run), always verbatim:

```
/Users/edward/workspace/softlab/softlab/shui/data/base.py:314: FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated. In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes. To retain the old behavior, exclude the relevant entries before the concat operation.
```

(The text matches the `PANDAS_BASELINE_WARNING` literal mirrored
verbatim in both bootstrap modules' warning policies — verified
character-exact against
`tests/run_record_inspect_bootstrap.py` at runtime.)

This is the pre-existing pandas 2.3.3 baseline documented in the Phase 1
correction record and the design §4.1 applicability probe: it fires on
Python 3.9 only, originates in unchanged production code
(`DataRecord.add_rows`, `softlab/shui/data/base.py:314`), is **not**
emitted by Python 3.13.15 (pandas 3.0.6), and is not suppressed or
filtered anywhere in the Phase 3 suite. The fresh-process bootstrap
child carries the exact approved allowance for it. No new warning
source is introduced by the Phase 3 code or the Phase 3 suite.

Informational stdout lines (the legacy backend's
`Connected to HDF5 backend, data is stored in <temp>/….h5.` connect
message and the sqlite3 equivalent) are operational prints of unchanged
production code, not warnings; every test-injected backend is tracked
by the harness double and disconnected at cleanup, and seam-owned
backends are verified-disconnected by construction.

## Per-area verdicts

| Area | Tests | Verdict |
| --- | --- | --- |
| A1 owned write path (incl. A1.6 dual-write) | 6/6 | **PASS** |
| A2 safe read path (incl. A2.8 signal empty set) | 8/8 | **PASS** |
| A3 fresh-process inspection | 4/4 | **PASS** |
| A4 fault matrix (A4.1–A4.9 + R2/R6 rows) | 13/13 | **PASS** |
| A5 stage honesty | 4/4 | **PASS** |
| A6 unsupported paths | 4/4 | **PASS** |
| A7 legacy compatibility | 4/4 (+ full-suite evidence) | **PASS** |
| A8 hygiene and portability | 5/5 | **PASS** |
| **Total** | **48/48** | **PASS** |

## A1 — Owned write path (decision §2 ownership; §6 items 1, 3, 5;
## §4 D1; PRD M03-AC03/AC04; matrix item 3; design §2, §9)

- **A1.1:** end-to-end owned save on synthetic temp files: file appears
  at the declared path; `StorageReport` frozen fields all asserted;
  owned lifecycle verified by recorder injection — inventory exactly
  `['connect', 'save', 'disconnect']`, backend disconnected and raw
  handle released afterwards. Failed/cancelled envelopes cannot even
  encode (`RecordValidationError` at `encode_envelope`), and
  hand-crafted failed-acquisition text is rejected by the seam's
  pre-connect decode with **zero** backend contact (recorder inventory
  empty) and no artifact at the target.
- **A1.2 (ORDERING DELTA):** every relation asserted by opening the
  stored file directly with `h5py` on a disposable byte copy: group
  name and on-disk `id` attribute equal the envelope `group_id`;
  `execution_id` appears in no stored UUID slot; `run_record` present
  as text and decodes deep-equal to the input envelope; **by-name**
  column association on a signal fixture whose declared order
  `(timestamp, signal, reference)` is not lexicographic — name sets
  equal, each declared column's four attributes match its same-named
  dataset exactly, physical enumeration observed lexicographic and
  non-semantic; stored row count equals `outcome.completed_rows`;
  samples equal the construction-time expectation **by row index**.
- **A1.3:** save without a successful prior connect fails visibly:
  `StorageConnectError` with the frozen `connect: HDF5 backend connect
  failed: ` prefix, recorder inventory exactly one `connect` and zero
  `save_group` calls, backend left disconnected.
- **A1.4 (D1):** recorder doubles patched without autospec/wraps: the
  seam never constructs a `DataGroup` (recorder list empty — no
  `meta=` path), performs no `set_meta` call at all (hence never
  `set_meta(key, None)`), and the pre-connect decode runs before any
  HDF5 write (order `['decode', 'save']`).
- **A1.5:** stored facts are independently stable: mutating the
  in-memory group after save (garbage `run_record` text, extra row)
  changes neither the artifact's bytes (sha256 equal) nor any
  disk-read fact; the decoded envelope from disk still equals the
  input.
- **A1.6:** example dual-write, both adapters: the stored group carries
  **both** `thermal_manifest` and `run_record`; the legacy manifest
  text is byte-equal to the example's own legacy builder output for the
  same run (`_encode_manifest(_make_manifest(...))`), decodes as
  `softlab.thermal-reference` v1 (coordinated) / v2 (direct), carries
  exactly the eleven closed fields, and the timing meanings are
  unchanged (`1/'tick-1'`, `0/'tick'`); the additive envelope decodes
  via the Phase 2 codec, cross-checks through the safe-path inspection,
  carries the `after_count`-retained `execution_id`, and the original
  file's sha256 is unchanged by inspection. Call-site gate evidence:
  the envelope exists only with `outcome.acquisition == 'completed'`
  and `result.acquisition == 'completed'` (a `shui`-level save cannot
  observe the gate — asserted here at the example layer per plan).

## A2 — Safe read path (decision §2 steps 1–4; §4 F-A/F-B; PRD
## M03-AC04 "original data is protected"; matrix item 3; design §3)

- **A2.1:** sha256 before, internal `shutil.copy2` to owned scratch
  (`softlab-m03-p3-read-` prefix observed on the connect target),
  sha256 after — byte equality on every inspection, including repeats;
  `InspectionResult.original_sha256` equals the test's own hash. Two
  sequential inspections of one original each succeed (a fresh copy
  per inspection, structurally).
- **A2.2:** h5py-level on-disk facts independent of `DataGroup`:
  `record_facts` agree with an independent raw-h5py re-read of a fresh
  byte copy (per-column four attributes, row counts); identity and
  decoded envelope asserted from the stored artifact.
- **A2.3:** recorder inventory on the injected backend's `load_group`:
  exactly one legacy load per inspection, invoked with the canonical
  `str(group_id)`; two inspections → two loads, both succeed.
- **A2.4 (F-B):** additive hydration preserves `run_record` verbatim
  alongside the four thermal legacy keys (exact text equality, raw
  texts in `legacy_meta`); no arbitrary custom-key restoration; an
  absent requested key is not an error; a present non-text requested
  key fails visibly with the frozen `<key>: present value is not text`
  template; the delivered fixture's bytes unchanged.
- **A2.5:** decode/validate/cross-check on the copy: three separate
  one-defect fixtures produce the three frozen `cross-check:` templates
  (identity, stored row count vs `outcome.completed_rows`, stored
  columns vs declaration).
- **A2.6 (F-A):** the supported inspection never opens the original
  with a legacy backend loader — every recorded connect targeted the
  internal disposable copy; the original's sha256 is unchanged after
  inspections both successful and failed. (No Phase 3 test performs a
  plain `load_group` on an original file as a supported operation; the
  only destructive legacy loads in this suite run on synthetic copies,
  exactly as Phase 1 did.)
- **A2.7:** scratch cleanup with independent diagnostics: success path
  carries `cleanup_ok=True, cleanup=()`; no `softlab-m03-p3-read-*`
  directories remain in the temp root after inspection.
- **A2.8:** signal-profile positive fixture (applicability per decision
  §1: the library-level owned persistence path exists in Phase 3, so
  the row stays in Phase 3 per design §2.3): decoded envelope carries
  `profile == 'signal'`; hydration carries `run_record` and **no**
  legacy keys — `SIGNAL_HYDRATION_KEYS == ()` asserted as the frozen
  literal together with the thermal 4-tuple and the
  `PROFILE_HYDRATION_KEYS` mapping; no thermal key is imposed.

## A3 — Fresh-process inspection (decision §2 steps 1–4; PRD M03-AC04
## "independent fresh-process inspection"; matrix item 5; design §4)

- **A3.1:** parent writes via the owned path; a fresh interpreter
  (`sys.executable`, so the 3.9 run re-executes the child on 3.9)
  inspects via `tests/run_record_inspect_bootstrap.py`: exit 0; payload
  schema asserted key-for-key; `records[*].columns` asserted in h5py
  enumeration order (`['reference', 'signal', 'timestamp']` — the
  §4.3 payload rule exercised on a non-lexicographic declaration);
  declared order remains the envelope's; samples equal the
  construction-time expectation by row index; `original_sha256` equals
  the parent's hash; the parent re-hashes the original — unchanged.
- **A3.2:** no legacy context in the child: it receives only path,
  profile, and group id; every payload fact (envelope, raw legacy
  texts, per-column samples) equals facts the parent re-derives
  independently from a fresh byte copy of the same artifact.
- **A3.3:** three-way distinction, all six probes as fresh processes on
  delivered fixtures hashed before and after:
  - absent additive key (genuine legacy-only file written by the
    unchanged legacy writer): exit 0, `envelope_present: false`,
    `envelope: null`, `legacy_meta` raw texts populated, report
    `primary_ok` — never an error, never a fabricated envelope;
  - present non-text (`int` via raw h5py): exit 1,
    `StorageInspectionError('run_record: present value is not text')`;
  - malformed / wrong-namespace / unsupported-version / over-size text:
    exit 1 with the Phase 2 frozen templates surfaced unwrapped
    (`RecordDecodeError('run_record: malformed JSON: Expecting value:
    line 1 column 1 (char 0)')`, `RecordValidationError('namespace:
    unsupported namespace (supported: softlab.run-record)')`,
    `RecordValidationError('version: unsupported version (supported:
    1)')`, `RecordDecodeError('run_record: encoded size exceeds limit
    131072 bytes')`); every error payload carries its `report` field
    (`primary_ok: false`, `primary[0]` the frozen message), traceback
    on stderr, delivered-fixture sha256 unchanged.
- **A3.4:** multi-group file: without `--group-id` → exit 1 with
  `group_id: require exactly one stored group or explicit ID`; unknown
  id → `group_id: stored group '<id>' not found`; explicit correct id
  → exit 0 with the right identity; missing `--profile` → argparse
  exit 2 with the usage message on stderr. Discovery is via read-only
  HDF5 keys on the copy with explicit identity validation.

## A4 — Fault matrix (decision §2 status checks and independent
## diagnostics; §4 F-A; PRD M03-AC04 lifecycle/failure row; matrix
## item 4; design §6)

Injection discipline held throughout: every fault at its real seam
(frozen design §6.2 mechanisms), instrumentation only plain recording
spies patched without `autospec`/`wraps`, spies delegating to the real
code path. Universal assertions on every row: frozen error type
surfaced (never logged-and-swallowed, never a `None`-means-absence
result), original/fixture bytes sha256-preserved, and the R1–R6 report
population contract verified via `_assert_report_failure` (report
present, `primary[0] == str(exc)`, `cleanup_ok == (len(cleanup) == 0)`,
truthful empty-cleanup semantics per row stage).

- **A4.1 (serialization):** `'not json'` → `RecordDecodeError` and
  count-drift text → `RecordValidationError`, both at the pre-connect
  decode; recorder inventory **zero** connect/save calls; no artifact
  at either target; R4(a) report (`cleanup_ok=True, cleanup=()` — no
  cleanup obligations existed); codec error carries the report
  additively (R3) with type and message unchanged.
- **A4.2 (save):** externally closed h5py handle after a real connect →
  `StorageSaveError('save: HDF5 backend save failed: …')`; §6.3 pin
  asserted exactly: the empty connect-time file remains, opening
  read-only with **zero groups**; connect/disconnect state checked
  (backend disconnected); the owed verified disconnect ran clean —
  R4(b) report (`cleanup_ok=True, cleanup=()` truthful).
- **A4.3 (disconnect, inspection variant — frozen row):** external
  file-handle invalidation after a genuinely successful primary load →
  primary outcome **delivered** with `cleanup_ok=False` and the frozen
  `Failed to disconnect: AttributeError(…)` diagnostic in
  `report.cleanup`; original sha256 unchanged; scratch copy removed
  normally (no `softlab-m03-p3-read-*` leftovers); the spy-retained
  handle closed in teardown with its `id.valid` falsity asserted (zero
  leaked handles).
- **A4.3 (save variant, §6.2 supporting evidence):** save primary
  success with the same invalidation → report returned with
  `cleanup_ok=False`; §6.3 pin: the complete artifact remains (group,
  attributes, datasets; decoded envelope equals the input).
- **A4.4–A4.7 (invalid reload):** corrupt-fixture construction rule
  followed — genuine artifact via the owned path, byte copy, one
  `run_record` defect written via raw `h5py` on the copy, delivered
  copy hashed before/after inspection; all four rows surface the
  frozen codec templates unwrapped with the report attached (R3) and
  delivered-fixture bytes unchanged.
- **A4.8 (preflight):** hand-built h5py fixture missing reserved attrs
  → `StorageInspectionError("preflight: missing or nontext reserved
  attribute 'id'")` raised before any legacy load (recorder inventory
  zero loads); R4(b): the failure occurred after scratch creation, so
  the report's empty cleanup tuple truthfully records a cleanup that
  ran clean; fixture bytes unchanged.
- **A4.9 (connect):** existing directory at the target →
  `StorageConnectError` with the frozen `connect:` prefix at the
  connect stage; recorder inventory exactly one connect, **zero** save
  calls; the pre-existing directory and its marker file are
  byte-unchanged; no HDF5 file created; R4(a) report.
- **R6/R2 contract rows (design §10 names R1–R6 for A4; executed in
  addition to the literal A4.1–A4.9 matrix):** mid-inspection byte
  modification → `StorageInspectionError('safety: original bytes
  modified during inspection')` dominates with a single-entry
  `primary` and no `__cause__`; mid-inspection original deletion →
  `safety: original bytes could not be re-verified after inspection:
  <exc>`; coincident body failure + re-hash failure → the safety
  exception escapes with the codec error chained as `__cause__` and
  `report.primary` preserving **both** diagnostics (R2 coincident
  rule).
- **Not claimed (recorded, not tested as a promise):** durability of
  failed/cancelled/partial runs, rollback, retry, atomic publication,
  crash recovery (decision §2 NON-support). A4 asserts truthful failure
  evidence only, and every row states exactly what is and is not
  guaranteed (the §6.3 artifact pins).

## A5 — Stage honesty (decision §1.1 acquisition-outcome row, §6 item
## 5; PRD M03-AC03; design §7.1, §8)

- **A5.1:** stored envelopes asserted **from disk** carry
  `snapshot == 'completed_acquisition_before_persistence'`,
  `acquisition == 'completed'`, and
  `persistence/plotting/cleanup/overall == 'not_yet_established'`.
- **A5.2:** negative probes: late-stage outcome text
  (`persistence: 'completed'`) and failed-acquisition text are rejected
  at the seam's pre-connect decode (`RecordValidationError`); neither
  encode path can produce them either.
- **A5.3:** recorder over `DataGroup.set_meta` during a full save: zero
  calls — there is no code path that rewrites the outcome after save
  (the outcome lives inside the single `run_record` text; no separate
  outcome metadata key exists, so an "outcome key" assertion would be
  unimplementable, exactly as the plan records). Post-persistence save
  success proven by this suite is a **test observation** recorded here,
  never a claim stored inside the record; the stored record acquires no
  later outcome history.
- **A5.4:** stored unavailable facts carry the frozen
  `state: 'unavailable' / value: null / bounded reason` structure; the
  vocabulary `('capture_failed', 'source_mismatch')` is not extended by
  storage-level facts (frozen §8) — runtime truth lives in the
  `StorageReport`.

## A6 — Unsupported paths (decision §2 NON-support, §4 D2/F-C/F-D;
## PRD M03-AC04 "unsupported … cases are explicit"; matrix item 3;
## design §7)

- **A6.1:** `Sqlite3DataBackend` rejected by both entry points at step
  0, before any I/O, with the frozen
  `backend: unsupported backend type 'sqlite3' (supported: 'hdf5')`;
  `isinstance(exc, ValueError)` and `isinstance(exc, RunStorageError)`
  both hold; the sqlite backend is not connected and no file appears.
  (D2 remains tracked debt; selecting SQLite requires a separately
  reviewed decision.)
- **A6.2:** F-C re-assertion on unchanged production code:
  `io.reload_group` with populated valid `group.backend` returns
  `None` (the UUID-id defect; helper-created backends tracked and
  disconnected by the harness double). F-D re-assertion:
  `io.load_groups('sqlite3', …)` returns `[]` on a non-empty database
  — recorded as the reason empty generic-helper output is never
  interpreted as an empty store. Call inventory: zero generic-helper
  calls during an owned save+inspect round trip.
- **A6.3:** same-ID deletion/recreation observed and recorded: the
  second save deletes the existing same-id group before recreating it;
  the artifact holds exactly the recreated group with the new rows.
  This is non-atomic publication **not** claimed as supported.
- **A6.4:** partial/failed-run durability explicitly unsupported: no
  test stores a failed or cancelled acquisition (the codec refuses to
  encode one); this exclusion is recorded verbatim here per the plan.

## A7 — Legacy compatibility (decision §6 items 7–8; §4 F-B/F-C/F-D;
## PRD M03-AC05 legacy half; matrix item 3)

- **A7.1:** the six named legacy suites
  (`test_thermal_manifest`, `test_thermal_example`,
  `test_thermal_execution`, `test_thermal_adapters`,
  `test_thermal_cli_warnings`, `test_thermal_metadata_contracts`)
  resolve and load with their full inventories unmodified, and pass as
  part of the 507-test discover run on **both** interpreters; the
  dual-write leaves the legacy manifest bytes, versions (1/2), timing
  meanings, and the eleven-field closed contract unchanged (A1.6 byte
  equality).
- **A7.2:** synthetic legacy files (written by the unchanged legacy
  writer with the four thermal legacy texts and **no** additive key)
  read after the change: the example's additive `_load_group` takes
  the pre-change path for a legacy-only file (four legacy texts
  hydrated, no `run_record` appears), record tables compare
  content-equal, and the storage layer reports truthful legacy absence
  (A3.3 first bullet carried into the storage layer). The legacy
  destructive load ran on a byte copy per the F-A discipline — which
  this suite itself demonstrated by catching its own reader working on
  an original during development (the copy discipline is now asserted
  in the test).
- **A7.3:** full suite green: 459-test baseline (405 pre-existing + 54
  Phase 2) grew by exactly the 48 Phase 3 additions to **507**; zero
  pre-existing tests removed, skipped, or weakened — on both
  interpreters.
- **A7.4:** export surface is exactly the frozen nine-name delta at
  package level; `run_storage.__all__` carries exactly the thirteen
  frozen names; `StoredRecordFacts` and the three hydration constants
  stay module-only; pre-existing exports unchanged.
- **A7.5:** the Phase 2 suite is reused unmodified (54 tests loaded
  from the untouched file, part of the green full run).

### M02 legacy-test adjudication — test-owner verification record

The code review (`7b1d771`) adjudicated the two M02 alignments as
legitimate; as test owner I verified both independently before taking
over:

1. **`test_m02_w01_characterization.py` (reload-appends 9 → 11):**
   verified in source — `DataGroup.record()` returns
   `DataRecord.copy()` (`base.py:570-573`), whose `copy()` replays
   `add_rows(self.table)` (`base.py:200-203`), so each read-only
   `record()` call emits exactly one instrumented `add_rows` event;
   `split_trace` counts every non-acquisition replay as a reload
   append. The approved wiring adds exactly two such calls during the
   persistence stage (the §2.4 envelope attach and the §2.2 step-3
   pre-save cross-check): 9 + 2 = 11 with no unexplained residue. The
   clean-trace ordering, table-value, and pure-observe assertions are
   untouched; the count remains an observed characterization value and
   now characterizes the wired reality. **Confirmed faithful — both
   adapters green on both interpreters.**
2. **`test_m02_w04_integration.py` (writer-cleanup interruption):**
   verified in source — the old duck-typed `Writer` fake can no longer
   pass the seam's step-0 `isinstance(backend, HDF5DataBackend)` guard
   (the old injection seam ceased to exist by approved design). The
   replacement injects at the seam's real verified disconnect with a
   real `HDF5DataBackend` subclass and asserts the frozen observable:
   `assertIs(caught.exception, interruption)` heads the assertions
   (cleanup failure does not mask the interruption), the calls
   inventory is `['connect', 'save', 'disconnect', 'disconnect']`, and
   the structured report (`operation == 'save'`,
   `primary == ('save interrupted',)`, `cleanup_ok == False`, fault
   text in `report.cleanup`) is asserted. **Confirmed faithful — green
   on both interpreters.** Recorded observation (non-blocking, from
   the review): the legacy stderr line `Writer cleanup: writer cleanup
   fault` remains emitted by the example layer but is no longer
   asserted; the review's optional re-add was not taken (the structured
   report is the stronger, frozen assertion).

## A8 — Hygiene and portability (Phase 1 C6; Phase 2 S7; PRD M03-AC06
## environment half; design §10 A8 row)

- **A8.1:** all fixtures programmatically constructed (synthetic UUIDs,
  digests, versions captured at test time); all test file I/O under
  `tempfile` roots with the `softlab-m03-p3-` prefix; temp roots
  removed and their absence asserted.
- **A8.2:** every test-injected backend tracked by the harness double
  and disconnected at cleanup; the A4.3 retained raw handles are
  closed in teardown with `id.valid` falsity asserted; a full
  save+inspect round trip under
  `warnings.simplefilter('error', ResourceWarning)` emits zero
  `ResourceWarning`s; no scratch-directory leftovers after inspection.
- **A8.3:** AST call-site self-inspection of this suite: no
  `mock.patch` call uses `autospec` or `wraps` (recording spies
  delegate explicitly — the 3.9 gh-75988 lesson holds suite-wide).
- **A8.4:** AST self-inspection: no `sys.set_int_max_str_digits`, no
  `sys.setrecursionlimit`; no scheduler singletons, no module-global
  default backends; each test owns its resources.
- **A8.5:** environment facts captured at runtime and recorded above —
  no version string is asserted as a pass condition; the suite passed
  on **both** supported interpreters (3.13.15 project venv and 3.9.23
  scratch env), including every fresh-process bootstrap child (which
  re-executes on the running interpreter).
- **A8.6/7:** warnings recorded verbatim above; none suppressed or
  filtered inside test code; the only 3.9 warning is the pre-existing
  pandas baseline from unchanged production code.
- **A8.8:** the verification commands and their actual results are
  recorded in the execution table above.

## A9 — Results recording (traceability)

This file is the A9 deliverable. Verdicts map to: decision §1.1
(identity/sample-schema rows — A1.2 by-name/row-index), §2 (ownership,
read discipline, NON-support — A1, A2, A4, A6), §4 dispositions **D1**
(A1.4), **D2** (A6.1), **F-A** (A2.1/A2.6, A4 universal byte
preservation), **F-B** (A2.4, A2.8), **F-C/F-D** (A6.2), §6 items 3–8
(A1, A3, A5); PRD **M03-AC03** (A1, A5), **M03-AC04** (A2, A3, A4,
A6), **M03-AC05** legacy half (A7), **M03-AC06** environment half
(A8); plan evidence-matrix items **3** (A1, A2, A6, A7) and **4**
(A4); design §2 (A1), §3 (A2), §4 (A3), §6 (A4, including §6.2 seam
evidence and §6.3 pins), §7–§8 (A5/A6), §9 (A7.4), §10 (all areas).
Phase 3 does **not** claim M03-AC05 oracle evidence (Phase 4) and does
not alter the Phase 1 decision or the Phase 2 codec contract. Phase 2
codec results (54/54 PASS) are reused unchanged; Phase 1
characterization facts are reused only where content is unchanged
(F-C/F-D mechanisms, D2 shape, the pandas baseline), and everything
affected by Phase 3 code was re-executed here.

## Deviations and observations (all recorded, none weakening an
## assertion)

1. **A1.6 legacy reference oracle:** cross-run byte equality of the
   legacy manifest is impossible because `run_id` (the group UUID)
   differs per run. The oracle used is the example's own legacy
   manifest builder applied to the same run (`_encode_manifest(
   _make_manifest(...))` byte equality), which proves the dual-write
   stored exactly the legacy builder's product; the unchanged M02 and
   thermal suites provide the cross-run behavioral evidence. Approach
   recorded per the plan's "never left ambiguous" rule.
2. **A4 extension rows:** the R6 byte-safety dominance pair and the R2
   coincident-failure row are executed as additional A4 tests beyond
   the literal A4.1–A4.9 table, per design §10's freeze table naming
   the R1–R6 population contract for A4. No plan row was weakened or
   dropped.
3. **A4.3 save-variant ordering:** the spy-retained raw handle is
   closed before (not only after) the artifact pin, because an h5py
   file copied while its writer handle is still open can yield an
   inconsistent copy; the close itself remains asserted
   (`id.valid` falsity) — the frozen teardown discipline is preserved,
   only the pin's position moved (test-side necessity, no production
   implication).
4. **A7.2 reader-copy discipline:** caught by this suite during its
   own development — pointing the legacy `_load_group` reader at an
   original demonstrated the characterized F-A destructive load
   stripping the reserved attributes (the test then failed at the
   storage preflight). The test now routes the legacy reader over a
   byte copy, exactly like the example's `_reload_group`, and the
   original's hash is asserted unchanged. This is precisely the F-A
   discipline the plan mandates; no source change followed.
5. **Python 3.9 scratch environment:** the initial 3.9 full-suite run
   showed 2 errors + 1 cascade failure, all caused by the missing
   optional dependency `pyvisa-sim` (not installed in `/tmp/py39`;
   it belongs to the `notebooks` extra). Environment failure, not a
   product failure — the failing modules (`test_tu_visa_contracts`,
   `test_tu_coordination`) are untouched by Phase 3 and fail at
   `import pyvisa_sim` before any Phase 3 code runs. After installing
   `pyvisa-sim 0.6.0` into the scratch env, the full 3.9 suite is
   green (507/507). Recorded per the product/environment-failure
   distinction.
6. **Code-review observations (non-blocking, unchanged):** observation
   1 (unenforced per-column row-count agreement invariant in
   `_preflight`) and observation 2 (unreachable `report=None`
   fallback in the bootstrap) were reviewed and recorded by sw-celeste
   as no-fix-required; this suite constructed no fixture relying on
   per-column disagreement, and the bootstrap's exit-1 payload schema
   is asserted non-null in every A3.3/A3.4 error row.

## Defects

**None.** Zero defects found in the Phase 3 implementation under test;
`bugs.md` receives no new entries. Every planned assertion was
executed as written; the only deviations are the recorded
above-board choices, none of which hides a discrepancy.
