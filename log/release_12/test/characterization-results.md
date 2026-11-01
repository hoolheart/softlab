# Release 12 (M03 Phase 1) — Characterization Results

Owner: Tester (sw-mike) | 2026-11-01 Asia/Shanghai
Branch: `codex/m03-p1-characterize`
Executable suite: `tests/test_m03_p1_characterization.py` (this commit)
Plan: [characterization-test-plan.md](characterization-test-plan.md) (APPROVED,
commit `de239e0`; developer re-review closed F1–F4)
Verdict: **ALL AREAS CHARACTERIZED — no area BLOCKED**

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
| Full regression + characterization suite | `python -m unittest discover -s tests -p 'test_*.py'` | exit 0 — **Ran 405 tests, OK** (358 pre-existing + 47 new in this suite) |
| Bytecode compilation | `python -m compileall -q softlab` | exit 0, no output |
| Import smoke | `python -c "import softlab; print(softlab.__version__)"` | exit 0 — `0.3.0` |

Environment actually observed:

| Item | Observed value |
| --- | --- |
| Python | `3.13.15 \| packaged by conda-forge \| (main, Aug 10 2026, 13:01:04) [Clang 19.1.7]` |
| softlab | `0.3.0` |
| numpy | `2.5.3` |
| pandas | `3.0.6` |
| h5py | `3.16.0` (libhdf5 `2.2.0` via `h5py.version.hdf5_version`) |
| sqlite3 | module `2.6.0` (`sqlite3.version`), library `3.53.4` (`sqlite3.sqlite_version`) |
| matplotlib | `3.11.2` |

### Warnings observed (verbatim, none waived or suppressed)

1. `DeprecationWarning: version is deprecated and will be removed in Python 3.14`
   — raised by reading `sqlite3.version` (Python 3.13 deprecation). Observed
   inside `Hygiene.test_environment_actually_observed`, captured with
   `warnings.catch_warnings(record=True)` and asserted as the single expected
   deprecation. No other warning is emitted anywhere in the 405-test run.
2. Baseline comparison: with this suite temporarily removed, the remaining 358
   tests emit **zero** warnings; an early unhygienic draft of this suite leaked
   4 `ResourceWarning: unclosed database in <sqlite3.Connection ...>` (from
   `pandas/core/internals/blocks.py:650` and `matplotlib/_api/__init__.py:137`)
   because the production `io.save_group`/`reload_group`/`load_groups` helpers
   never disconnect. The final suite tracks and disconnects every `data_io`
   backend via a harness double; the full run is back to **zero** warnings.
3. One expected stderr line (logging, not a warning), from the deliberate
   empty-backend probe in C4:
   `Exception in reloading group 888dde5b-81e8-44b5-81bb-3c99899f7e58: ValueError('Backend type is empty')`
   — produced by `softlab/shui/data/io.py`'s own `_logger.error`, visible
   evidence of the observed `reload_group` failure behavior.

## C1 — Legacy thermal manifest characterization

**Verdict: CHARACTERIZED.**

- **Entry points (C1.1).** All exercised on synthetic runs in
  `tempfile.TemporaryDirectory` (`softlab-m03-p1-*` prefixes):
  - In-process `execute()` save/reopen verification: `result.persistence ==
    'verified'`, `result.loaded` is a `DataGroup` with the run's id.
  - `inspect_stored(copy)` without `group_id` and with explicit `group_id`
    both return `(DataGroup, manifest)` with matching identity on a
    single-group file. A two-group file without explicit id raises
    `ValueError('group_id: require exactly one stored group or explicit ID')`.
    With an explicit id, a group **without** the legacy `_META_KEYS` fails
    `Missing or nontext disk metadata: example` (wrapped in `_HelperFailure`) —
    the helper is thermal-specific, not generic.
  - CLI `--inspect-file` on a disposable copy (fresh child via
    `tests/thermal_cli_bootstrap.py`, `-W error`): exit 0, JSON payload with
    `context: 'available'`, matching `run_id` and row count.
- **Strict validation (C1.2).** One synthetic defect at a time against a
  genuine completed run manifest; observed diagnostics (verbatim):
  non-object → `manifest: expected object`; missing/extra top-level key →
  `manifest: expected exact object fields`; bad schema →
  `schema: unsupported manifest schema`; version 3 →
  `version: unsupported manifest version`; non-canonical/uppercase run_id →
  `run_id: expected canonical UUID`; `configuration.dt` as text →
  `configuration.dt: expected finite real`; physically invalid combination
  (dt=100) → `Require 0 < dt * conductance / heat_capacity <= 1` (from
  `_validate_config`); model drift →
  `model: unsupported model semantics or units`; procedure row-count drift →
  `procedure: inconsistent declared acquisition`; column count/definition
  drift → `columns: expected ordered thermal schema` /
  `columns.0: invalid column definition`; clock timing drift →
  `clock: inconsistent acquisition timing`; bad digest →
  `source.digest: invalid digest or availability`; outcome drift →
  `outcome: inconsistent completed acquisition snapshot`; malformed JSON →
  `json.JSONDecodeError` (a `ValueError`); duplicate key →
  `manifest: duplicate object key schema` (via `object_pairs_hook`);
  `NaN` token → `manifest: nonfinite JSON token NaN`; non-text input →
  `TypeError: manifest: expected JSON text`. `_validate_manifest_data`
  diagnostics: `run_id: differs from stored group identity`;
  `outcome.completed_rows: differs from stored row count`.
- **Absent vs invalid (C1.3).** (a) No `thermal_manifest` attr → accepted,
  `manifest is None`, CLI reports `context: 'legacy_unavailable'`.
  (b) Present non-text (int 42) → `_HelperFailure` whose primary is
  `ValueError('thermal_manifest: present value is not text')`. (c) Present
  invalid text (`'not json'`) → `_HelperFailure` whose primary is a
  `ValueError` from JSON decoding. Invalid-present is **never** treated as
  absent in any observed path. The delivered synthetic original file's sha256
  is unchanged after every probe (all probes ran on `shutil.copy2` copies).
- **Outcome snapshot (C1.4).** Parsed from on-disk bytes on a disposable copy:
  `snapshot == 'completed_acquisition_before_persistence'`,
  `acquisition == 'completed'`, `requested_rows == completed_rows ==
  final_tick == 8` (default config), `final_sim_time == 8.0`;
  `persistence`, `plotting`, `cleanup`, `overall` are all
  `'not_yet_established'` — the later stages are explicitly not yet recorded
  at snapshot time. Gate probe: `_make_manifest` with
  `result.acquisition = 'failed'` raises
  `ValueError('outcome.acquisition: manifest requires completed acquisition')`.
- **Execution UUID vs group UUID (C1.5).** Observed without source changes by
  wrapping the module-global `run_count` and capturing the `CountResult` seen
  by the example's `after_count` wrapper: `CountResult.run_id` is a
  `uuid.UUID`, and `str(run_id) != str(result.group.id)` for the same run
  (execution UUID and persistent group UUID are distinct). Verbatim fact:
  the example-local `RunResult` retains **no** execution UUID — `run_id` is
  not among its dataclass fields and `result.run_id` raises
  `AttributeError`.

## C2 — DataGroup / metadata characterization

**Verdict: CHARACTERIZED — two plan deviations recorded (D1, D2) and two
findings beyond the plan (F-A, F-B).**

- **Constructor (C2.1, R4-BASE-META-01).** `DataGroup('probe', meta={'a': 1})`:
  `group.meta == {}`, `group.get_meta('a') is None`; the caller's dict is not
  copied but written back in place (invisible for all-str keys).
  **Deviation D1 from the plan's coercion probe:** with any non-str key
  (`{2: 'x'}` or `{'a': 1, 2: 'x'}`) construction raises
  `RuntimeError: dictionary changed size during iteration` — the loop writes
  the coerced key back into the mapping it is iterating — and the caller's
  dict is left mutated holding **both** keys (`{2: 'x', '2': 'x'}`). So the
  observed semantics are: meta never populates `_meta`; the caller's mapping
  is mutated in place; non-str keys additionally crash construction midway.
- **`set_meta(None)` (C2.2).** On an existing key: the key **remains** in
  `meta` with value `None` (docstring claims "will be removed");
  `get_meta('k')` returns `None`, indistinguishable from an absent key;
  `pop_meta('k')` pops and returns the stored `None`. On an absent key:
  `set_meta('new', None)` **creates** the key with value `None`.
- **HDF5 storage (C2.3).** A synthetic group with meta text (incl. non-ASCII
  `héllo`), int, float, list, dict and `None` values was saved:
  - On disk (h5py read-only on a sha256-verified copy): every value is stored
    as `str(value)` (all attrs are text; e.g. `7` → `'7'`, `[1, 2]` →
    `'[1, 2]'`, `None` → `'None'`); no value had a problematic `str()`.
  - **Plain backend load path** (`HDF5DataBackend.load_group`): observed
    `loaded.meta == {}` — the live attrs proxy is dropped by R4-BASE-META-01
    after the reserved-attr pops. **Finding F-A (R4-BASE-HDF5-01 observed end
    to end):** because `connect_impl` opens the file `'a'`, the pops of
    `name`/`id`/`timestamp`/`backend` persist to the file on close; a
    **second** plain load of the same file then fails with
    `AttributeError("'UUID' object has no attribute 'replace'")` (uuid.UUID's
    str default is fed a UUID object), recorded in `backend.errors` as
    `Failed to load group: ...`, load observing `None`.
  - **Example hydration path** (`_load_group`/`_reload_group`):
    `loaded.meta == {'example': ..., 'configuration': ..., 'timing': ...}` —
    only the three `_META_KEYS` hydrate. **Finding F-B (beyond plan):** every
    custom meta key is dropped by the example load path entirely (not even
    lossily); only legacy keys plus the manifest rehydrate. The original
    file's sha256 is preserved throughout (the reader works on its own copy).
- **SQLite capability (C2.4).** Observed schema (`pragma table_info(datagroup)`):
  `id TEXT, experiment TEXT, sample TEXT, run_id INTEGER, timestamp TIMESTAMP,
  backend DICT`. Quirk confirmed verbatim: `run_id` is declared `INTEGER`
  (pragma reports uppercase) while the saved value is unconstrained — a text
  run_id (`'run-42'`) persists as text and reloads as text. Only `sample`
  (default `'demo'`) and `run_id` (default `0`) are persisted; arbitrary extra
  metadata is dropped (no column, no table). **Deviation D2 from the plan:**
  the plan expected the plain `Sqlite3DataBackend.load_group` reconstruction
  to be exactly `{'sample', 'run_id'}`; the observed meta is **`{}`** —
  `load_impl` (sqlite3.py:159-162) builds that dict for
  `DataGroup(..., meta=)`, but R4-BASE-META-01 drops it exactly as in HDF5.
  The `{'sample', 'run_id'}` intent is observable only in source and in raw
  SQL, not in any loaded object.

## C3 — HDF5 on-disk metadata inspection

**Verdict: CHARACTERIZED — no blocker arose.**

- For every synthetic file (v1 coordinated output, v2 direct output,
  hand-built h5py fixture, backend-written probe file): `shutil.copy2` to a
  scratch file inside the temp root, `hashlib.sha256` of the original
  recorded before and after all reads, byte equality asserted. All
  inspections used `h5py.File(copy, 'r')`.
- Disk attrs of both example files: `id`, `name`, `timestamp`, `backend`,
  `example`, `configuration`, `timing`, `thermal_manifest` — every value is
  text; `id == group directory name == manifest run_id`; `backend` parses as
  JSON `{'type': 'hdf5', ...}`. Manifest JSON parsed from disk bytes
  cross-checks C1: `version` 1 vs 2 by adapter, `clock.measurement_delay_ticks`
  1 vs 0, `clock.source_tick 'tick-1'` vs `'tick'`, outcome snapshot and
  stages identical to the in-memory decode; `configuration`/`timing` legacy
  text attrs equal the manifest content.
- Hand-built h5py fixture (no softlab writer): native attr types preserved —
  observed verbatim `numpy.int64` for `7` and `numpy.float64` for `0.5`
  (h5py scalar types), text stays `str` — contrasting with backend-written
  files where every attr is text.
- h5py `3.16.0`, libhdf5 `2.2.0` recorded (also in C4/C6).
- The C3 blocker protocol was not triggered: a safe read-only path exists and
  was used; the destructive reader behavior (F-A) is exercised only on
  synthetic byte-copies and the delivered originals stay byte-identical.

## C4 — Backend caller characterization

**Verdict: CHARACTERIZED — two production-path defects recorded as findings
(F-C, F-D).**

- **Static inventory (verbatim grep evidence):** `HDF5DataBackend` is
  instantiated in `softlab/shui/data/io.py` (`get_data_backend`, L47),
  `examples/thermal_experiment.py` (`_reload_group` L698 reader, `execute`
  L832 writer), `softlab/shui/data/hdf5.py` (`__main__` example, L179) and
  the test suites (`test_compatibility.py` L112, `test_thermal_example.py`,
  `test_thermal_execution.py`, `test_thermal_manifest.py`,
  `test_thermal_metadata_contracts.py`, M02/M03 characterization tests).
  `Sqlite3DataBackend` is instantiated in `io.py` (L49),
  `test_compatibility.py` (L111) and used by this suite; **no notebook
  references any DataBackend** (all `tests/*.ipynb` scanned, zero matches),
  and `sqlite3.py` has no `__main__` example (unlike `hdf5.py`).
  Callers outside the thermal example: the production io entry points and
  the test suites only — a complete ownership map for the scope decision.
- **What io reads/writes from `group.backend` (verbatim):** `save_group`
  with an explicit `backend_type` **writes** `group.backend = {'type':
  backend.backend_type, 'arguments': backend.arguments}` (observed
  `{'type': 'sqlite3', 'arguments': {'path': ...}}` after save); with
  `backend_type=None` it **reads** `group.backend` via
  `get_data_backend_by_info`; `reload_group` **reads** `group.backend` the
  same way. Empty/default backend info (`{'type': '', 'arguments': {}}`)
  makes `reload_group` return `None` (ValueError logged by io).
- **Finding F-C (defect, observed):** with **populated, valid**
  `group.backend`, `io.reload_group(group)` still returns `None` on **both**
  backends, because it passes `group.id` (a `uuid.UUID`) to `load_group`:
  sqlite3 records `Failed to load group: ProgrammingError("Error binding
  parameter 1: type 'UUID' is not supported")`; hdf5 records
  `Failed to load group: TypeError("Accessing a group is done with bytes or
  str, not <class 'uuid.UUID'>")`. Both errors are swallowed by
  `catch_error`, so the failure is silent. The identical load with
  `str(group.id)` succeeds on both backends (id type is the sole cause).
- **Finding F-D (defect, observed):** `io.load_groups('sqlite3', ...)`
  returns `[]` for a non-empty database, because `list_impl` returns
  `fetchall()` row **tuples** and `load_group(tuple)` fails silently for
  every row. The hdf5 `load_groups` works — but only on a fresh file: after
  any plain load has destructively removed the reserved attrs (F-A), the
  subsequent load fails with the F-A `AttributeError` chain.
- **Runtime confirmation (harness doubles only):** wrapping
  `HDF5DataBackend.connect/save_group/load_group` during a full
  `thermal.execute` recorded exactly: writer `connect({'path':
  <output>/thermal.h5})`, one `save_group(original)`, reader `connect` on
  the internal disposable copy, one `load_group(str(group.id))`.
  `get_data_backend`/`get_data_backend_by_info` were exercised directly for
  both backend types (and reject `''` with `ValueError`, unknown types with
  `NotImplementedError`).
- **Versions observed at runtime:** h5py `3.16.0` / libhdf5 `2.2.0`;
  sqlite3 module `2.6.0`, library `3.53.4`; Python `3.13.15`;
  softlab `0.3.0`; numpy `2.5.3`.

## C5 — Two-client inventory

**Verdict: CHARACTERIZED.**

### C5a. Thermal client — truthful fields today

Both adapters were run on fresh synthetic output directories; the stored
record (manifest v1 for `coordinated`, v2 for `direct`) truthfully provides
today, field by field (asserted in
`ThermalClientInventory.assert_common_fields`):

| Field | Truthful content observed |
| --- | --- |
| `schema` | `softlab.thermal-reference` |
| `version` | `1` (coordinated) / `2` (direct) |
| `run_id` | canonical UUID, equal to the persisted `DataGroup.id` |
| `configuration` | exact `ThermalConfig` values (types: float fields float, `points` int) |
| `model` | name/method/equation/validity plus units for every configuration key |
| `procedure` | `thermal_count`, `once_before_each_getter_row`, `constant_power`, requested points |
| `columns` | the six ordered thermal column definitions, equal to the stored record's |
| `clock` | `dt`, `simulation_unit: 's'`, `measurement_delay_ticks` 1 vs 0, `source_tick 'tick-1'` vs `'tick'`, wall-timestamp policy |
| `software` | `softlab.__version__` and `platform.python_version()` |
| `source` | reference path, `sha256` digest of the exact example bytes (verified equal to the file hash), digest scope, plus git revision (40/64-hex) with dirty flag **or** a recorded unavailable reason |
| `outcome` | completed-acquisition snapshot: requested/completed rows, final tick, final sim time |

Reproducible inputs recorded: `ThermalConfig` values, adapter name, softlab
and Python versions, source digest, delay policy (1 vs 0). Stored samples
validate against the **independent Euler oracle** (re-derived in this suite;
same recurrence as M02 `ThermalPolicyComparison.euler`): temperature matches
within `atol=1e-10` for both adapters, and `tick`, `measurement_source_tick`,
`sim_time_s`, `commanded_power_w` match exactly.

**Not truthfully available today (recorded as unavailable, not implied):**
later outcome stages (`persistence`/`plotting`/`cleanup`/`overall` are
`'not_yet_established'` at snapshot time), no environment lock, no dependency
lock — the manifest's key set is exactly the eleven closed fields.

### C5b. Signal-monitor client — in-memory only today

- **Construction:** only the example-local `acquire()` builds the
  `DataGroup`/`DataRecord` in memory; the module defines no other function,
  and its source contains no backend/manifest/save/h5py reference at all.
  Ownership of any future stored-record construction is a Phase 1 design
  input, not implemented here.
- **Characterized as written:** `acquire(points=3)` yields signal
  `[6, 7, 8]` (`tick + 5`), reference `[3, 4, 5]` (`tick + 2)`, differences
  `[3, 3, 3]`, with the read trace `[('signal', 1), ('reference', 1), ...,
  ('signal', 3), ('reference', 3)]`. Reads are deterministic and
  **non-consumptive** (`read` returns `state['tick'] + offset`; nothing
  queued is consumed): two fresh `acquire(points=3)` calls reproduce every
  non-timestamp value identically (only wall-clock timestamps differ).
  The consumptive `pop(0)` semantics belong to the M02 `_MonitorSource`
  test double and the test-local `run_monitor` client only — cited as
  test-double context, never attributed to this example.
- **Clean-session rerun inputs:** `points`; the example-owned read-policy
  offsets (signal 5, reference 2, `signal_monitor.py:31/33`); scheduler/loop
  ownership (`run_count` owns a fresh `SerialScheduler`,
  `bounded.py:142` — verified: the module-global scheduler singleton is the
  same object before and after a run). No consumed-value re-declaration is a
  required rerun input.
- **Selected backend:** none today; the characterized candidates from C2/C4
  are the decision inputs (HDF5 all-text lossy meta + destructive plain
  loads; SQLite `sample`/`run_id`-only meta plus F-C/F-D io defects).
- **Field origins:** `timestamp`, `signal`, `reference` come from the
  `DataRecord` column definitions; `difference` is computed **in memory by
  the caller** (`signal_monitor.py:39`) and is not a stored column; timing
  comes from the scheduler-owned count (wall-clock timestamp column).
- **Independent oracle:** for `points=3`, expected differences are
  `[3, 3, 3]`, computed from the example's own `tick + offset` semantics
  (offsets 5 and 2) independently of any stored object; self-comparison
  (rerun vs rerun) is not used as validation evidence.

## C6 — Safety and hygiene

**Verdict: CHARACTERIZED.**

- All fixtures synthetic; every path under
  `tempfile.TemporaryDirectory(prefix='softlab-m03-p1-...')` (the thermal
  example's own scratch copies use its `softlab-thermal-read-` prefix);
  cleanup asserted (`root.exists()` is False after cleanup; produced
  artifacts are exactly `thermal.h5` + `thermal.png` inside the temp root).
- HDF5/SQLite inspection performed only on disposable byte-copies with
  sha256 equality before/after (C3); the R4-BASE-HDF5-01 destructive chain
  was exercised exclusively on synthetic copies.
- No VISA, no hardware, no `@sim` backend; no `softlab/` or `examples/`
  source change (suite lives entirely in `tests/`).
- Connections: `data_io` helpers never disconnect; this suite tracks every
  backend they create via a harness double and disconnects them at cleanup
  (verified: zero `ResourceWarning` in the full run, matching the pre-suite
  baseline of zero).
- Environment recorded per run (see Execution record); the single observed
  warning (sqlite3 `version` deprecation) is captured and asserted verbatim.
- No global default instances leak: the scheduler singleton is asserted
  unchanged across thermal/signal-monitor runs; all owned loops are closed by
  the bounded seam (`result.loop_closed` asserted).

## Deviations from plan expectations and new findings

| ID | Plan expectation | Observed actual | Disposition |
| --- | --- | --- | --- |
| D1 (C2.1) | key-coercion probe shows caller-dict mutation | mutation confirmed, but any non-str key also raises `RuntimeError: dictionary changed size during iteration`, leaving both keys in the caller's dict | recorded as-is; strengthens R4-BASE-META-01 evidence |
| D2 (C2.4) | plain SQLite load meta is exactly `{'sample', 'run_id'}` | plain SQLite load meta is `{}` — R4-BASE-META-01 drops the reconstruction dict too | recorded as-is |
| F-A (C2.3/C3) | R4-BASE-HDF5-01 described as attr pops on an open group | confirmed end to end, plus compounding: a second plain load of the same file fails `AttributeError: 'UUID' object has no attribute 'replace'` | recorded as-is |
| F-B (C2.3) | example hydration rehydrates meta from captured attrs | it hydrates ONLY the 3 `_META_KEYS` (+ manifest); custom keys are dropped entirely | recorded as-is |
| F-C (C4) | `reload_group`/`save_group` depend on `group.backend` | dependency confirmed, but `reload_group` returns `None` on BOTH backends even with valid backend info (UUID id vs str id; errors swallowed) | recorded as-is; scoped-decision input |
| F-D (C4) | `load_groups` is a production entry point | `load_groups('sqlite3', ...)` returns `[]` on non-empty DBs (row-tuple ids); hdf5 works only on untouched files (F-A chain) | recorded as-is; scoped-decision input |

## Phase 1 decision inputs (Product/Architect)

Two-client inventory in one view: the **thermal client** persists a closed,
self-validating v1/v2 manifest with truthful fields enumerated in C5a and an
independent Euler oracle; its gaps are the not-yet-established later outcome
stages and the absence of environment/dependency locks. The
**signal-monitor client** is in-memory only (`acquire()`), needs stored-record
construction ownership, a backend selection (candidates characterized with
their defects in C2/C4: F-A destructive HDF5 loads, D2 SQLite meta loss,
F-C/F-D broken io paths), and would store `timestamp/signal/reference` plus a
caller-computed `difference`. Backend ownership outside the example is the io
module plus tests only (C4). Independent oracles available: Euler recurrence
(thermal) and `tick + offset` differences (signal monitor). No area is
BLOCKED; nothing herein licenses any contract change.

## Traceability

- Plan areas C1–C6 → suite classes: `ManifestReaderEntryPoints` (C1.1),
  `ManifestStrictValidation` (C1.2), `ManifestAbsentVsInvalid` (C1.3),
  `ManifestOutcomeSnapshot` (C1.4), `ExecutionVsGroupUuid` (C1.5),
  `DataGroupMetaConstructor`/`SetMetaNoneSemantics` (C2.1/C2.2),
  `Hdf5MetaRoundTrip` (C2.3), `SqliteMetaCapability` (C2.4),
  `Hdf5OnDiskInspection` (C3), `BackendCallers` (C4),
  `ThermalClientInventory`/`SignalMonitorInventory` (C5a/C5b),
  `Hygiene` (C6).
- M02 evidence reused: the Euler oracle formula (re-derived, same
  recurrence) and the example-loading convention; all other evidence is new
  runtime observation on this branch.
