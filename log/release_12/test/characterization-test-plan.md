# Release 12 (M03 Phase 1) — Characterization Test Plan

Owner: Tester (sw-mike) | 2026-11-01 Asia/Shanghai
Status: **DRAFT — awaiting independent review before any executable test is written**
Branch: `codex/m03-p1-characterize`
PRD: [log/release_12/prd.md](../prd.md) (Phase 1 row, M03-AC01) |
Plan: [plan/milestone-03.md](../../../plan/milestone-03.md)

## Scope and ground rules

This is a **planning document only**. No test code is written, no source under
`softlab/` is modified, and no runtime characterization is executed until this
plan passes independent review (developer re-review, then architect/design
review per the project gate order). All planned runtime work uses synthetic
values in `tempfile.TemporaryDirectory` locations; HDF5 inputs are disposable
byte-copies; no VISA, hardware or real instrument is touched. Characterization
asserts **actual observed behavior of the unchanged production baseline**;
nothing observed may be silently repaired, relaxed or broadened, and no
observed defect is fixed under this phase.

### Known-defect disposition statement

The following tracked defects are characterized **as-is**; their dispositions
remain tracked and are out of scope for Phase 1 fixes:

- **R4-BASE-META-01** — `DataGroup.__init__(meta=...)` mutates the caller's
  dict and does not populate `self._meta` (see `softlab/shui/data/base.py`,
  where the `isinstance(meta, Dict)` loop rewrites the caller's mapping
  without assigning to `_meta`).
- **R4-BASE-HDF5-01** — the legacy HDF5 reader mutates reserved attributes:
  `load_data_group_from_hdf5_group` does `meta.pop('name'/'id'/'timestamp'/
  'backend')` on `hdf5_data_grp.attrs` (`softlab/shui/data/hdf5.py`), i.e. a
  load can destructively remove reserved attrs from an open HDF5 group.
- **`set_meta(None)` mismatch** — the docstring says `value is None` removes
  the key, but the implementation pops the key and then unconditionally
  re-assigns `self._meta[key] = None`, so the key remains with value `None`.
  Planned probes must record which semantics are observed and must not assume
  either without evidence.

Characterization results are evidence for the Product Owner / Architect
evidence decision (common vs separate record scope). They are **not** a
license to change any contract.

## Results recording

- Executable tests (after review approval, in a later step) will live in
  `tests/` following the `test_m02_w01_characterization.py` convention:
  synthetic fixtures, assertion-based probes of the unchanged baseline,
  defects recorded as findings.
- Execution results will be recorded in
  **`log/release_12/test/characterization-results.md`** with one clearly
  headed section per characterization area (C1–C6 below), each ending with a
  per-area **verdict**: `CHARACTERIZED` (observed behavior asserted with
  evidence), `BLOCKED` (with recorded blocker evidence and the scoped
  decision input it feeds), or `PARTIAL` (what was and was not observed).
- Every recorded run includes the exact commands, Python/dependency versions
  actually observed, all warnings emitted (none waived), and applicability
  notes for any reused M02 evidence. M02 evidence is reused only where
  content is unchanged and this is stated explicitly per item.

## C1 — Legacy thermal manifest characterization (v1 coordinated / v2 direct)

**Object:** `examples/thermal_experiment.py` manifest machinery: `_MANIFEST_KEY`,
`_make_manifest`, `_check_manifest`, `_encode_manifest`, `_decode_manifest`,
`_validate_manifest_data`, `_load_group`, `_reload_group`, and the CLI
inspect path.

Procedures and expected evidence:

1. **Reader entry points.** Enumerate and probe each path that reads a stored
   manifest: in-process `execute()` save/reopen verification, CLI
   `--inspect-file` on a disposable copy, and the no-`group_id` /
   explicit-`group_id` variants of the inspection helper. Evidence: the exact
   entry point exercised, its inputs, and its returned
   `(DataGroup, manifest)` or failure.
2. **Strict validation behavior.** Feed synthetic manifests with one defect
   at a time (wrong type, missing key, bad `configuration`/`model`/`clock`/
   `columns`/`source`/`outcome` field, row-count mismatch, `run_id` differing
   from stored group identity). Evidence: the specific exception type and
   diagnostic text observed for each defect class; malformed JSON text.
3. **Absent vs invalid metadata distinction.** Compare (a) a synthetic HDF5
   file with **no** `thermal_manifest` attribute, (b) a file whose
   `thermal_manifest` attribute is present but non-text, (c) present text
   that fails `_check_manifest`. Evidence: that legacy-absence (a) is accepted
   as "absent legacy envelope only" while (b) and (c) fail with accurate,
   distinct diagnostics; explicitly assert invalid-present is never treated as
   absent.
4. **Completed-acquisition snapshot semantics.** Assert the manifest
   `outcome` block observed on disk: `snapshot ==
   'completed_acquisition_before_persistence'`; `acquisition == 'completed'`;
   `persistence`, `plotting`, `cleanup`, `overall` all
   `'not_yet_established'`. Evidence: the stored outcome stages that **are**
   recorded (acquisition, requested/completed rows, final tick, final sim
   time) and the later stages that are **not** recorded, verified from disk
   bytes (C3), not from in-memory objects. Also probe `_make_manifest` raising
   when `result.acquisition != 'completed'` to characterize the gate.
5. **Execution UUID vs persistent group UUID distinctness.** Record both the
   in-memory bounded-execution identity used during the run (where available
   via the M02 shared seam / `RunResult`) and the persisted `DataGroup.id`
   used as manifest `run_id`. Evidence: the two UUID values in a single run
   and an assertion of inequality (or, if equality is ever observed, that
   fact recorded verbatim as a finding).

## C2 — DataGroup / metadata characterization

**Object:** `softlab/shui/data/base.py` (`DataGroup.__init__`, `meta`
property/setter, `get_meta`, `set_meta`, `pop_meta`, `clear_meta`),
`softlab/shui/data/hdf5.py` (save/load), `softlab/shui/data/sqlite3.py`.
Characterize **actual** behavior; backend parity is **not** assumed.

Procedures and expected evidence:

1. **Constructor defect R4-BASE-META-01.** Construct `DataGroup(name,
   meta={'a': 1})` and record: whether `group.meta` contains `'a'`, whether
   the caller's dict was mutated (e.g. a `{'a': 1, 2: 'x'}`-style key
   coercion probe), and `group.get_meta('a')`. Evidence: verbatim observed
   constructor semantics vs the docstring.
2. **`set_meta(None)` mismatch.** Call `set_meta('k', None)` on a group where
   `'k'` exists and where it does not; record observed `meta` content,
   `get_meta('k')`, and `pop_meta('k')` behavior. Compare against the
   docstring ("will be removed"). Evidence: the observed implementation
   semantics, recorded as-is.
3. **HDF5 metadata storage.** Save a synthetic group whose meta contains
   text, integer, float, list, dict, None and non-ASCII text values;
   reopen and record what comes back. Expected evidence: save writes
   `str(value)` (`hdf5.py:81-82`), so non-text values are lossy (e.g. `1`
   returns as `'1'`, a list as its `str(...)`); characterize exactly which
   values round-trip as text and which are lossy. Also note `attrs[key]` for
   a value whose `str()` is problematic, if any.
4. **SQLite metadata capability.** Exercise `SQLiteDataBackend` on a
   temporary synthetic database: inspect the actual schema (table columns)
   and record which meta keys are persisted (`sample`, `run_id` per
   `sqlite3.py` save/load paths) and what happens to arbitrary extra
   metadata. Evidence: an explicit statement of which metadata SQLite
   actually stores vs drops, derived from observed reload content and the
   on-disk schema — no assumption that it persists arbitrary metadata.

## C3 — HDF5 on-disk metadata inspection (independent of DataGroup)

**Object:** synthetic HDF5 files produced by the thermal example (and by a
hand-built `h5py` fixture), inspected **directly on disk** with `h5py` in
read-only mode on a **disposable byte-copy**, independent of any returned
`DataGroup`.

Procedures and expected evidence:

1. For each synthetic file: `shutil.copy2` to a scratch file; record
   `hashlib.sha256` of the original **before and after** all reads; assert
   byte equality (original-byte preservation evidence).
2. Inspect `disk[<group_id>].attrs` directly: enumerate attribute names,
   types, and exact values for `id`, `name`, `timestamp`, `backend`,
   `example`, `configuration`, `timing`, `thermal_manifest` (v1 and v2
   files). Parse the manifest JSON from disk bytes and cross-check against
   C1 outcomes (outcome stages, run_id, versions 1 vs 2).
3. Record the `h5py` version actually used, and the libhdf5 version string
   from `h5py.version.hdf5_version`.
4. **Blocker protocol.** If no safe read-only path exists for any required
   inspection (e.g. opening mutates the file, or the legacy reader is the
   only available path and R4-BASE-HDF5-01 makes even copy-based reads
   destructive in a way that prevents evidence collection), the result is
   recorded as `BLOCKED` in `characterization-results.md` with exact
   reproduction evidence; the blocker is then reported to Product Owner /
   Architect as a scoped-decision input. A blocked inspection is never
   reported as a pass.

## C4 — Backend caller characterization

**Object:** who actually calls `HDF5DataBackend` and `SQLiteDataBackend`
today.

Procedures and expected evidence:

1. Static caller inventory (source/document inspection, recorded in this
   plan's results): `examples/thermal_experiment.py` (writer in `execute`,
   reader in `_reload_group`/`_load_group`, CLI inspect), `tests/` suites
   (`test_thermal_example.py`, `test_thermal_manifest.py`,
   `test_thermal_metadata_contracts.py`, M02 characterization tests),
   notebooks and any `__main__` examples; same for `SQLiteDataBackend`.
2. Runtime confirmation on synthetic temporary data: instrument (via test
   doubles / call recording in the test harness, not source changes) which
   backend entry points each caller exercises, with which arguments.
3. Record observed versions: `h5py.__version__` + `h5py.version.hdf5_version`,
   `sqlite3.sqlite_version` and module version, plus the Python version and
   key dependency versions actually observed at run time.
4. State explicitly, per backend, which callers exist **outside** the thermal
   example, so the persistence-boundary decision in Phase 1 has a complete
   ownership map.

## C5 — Two-client inventory

### C5a. Thermal client (existing, truthful fields today)

Procedures and expected evidence:

1. Run both adapters (`coordinated` → manifest v1, `direct` → manifest v2)
   on fresh synthetic output directories; list, field by field, what the
   stored record truthfully provides today: `schema`, `version`, `run_id`,
   `configuration`, `model` (+units), `procedure`, `columns`, `clock`/timing,
   `software` (softlab + Python version), `source` (reference path, sha256
   digest, git query result or unavailable reason), `outcome` snapshot.
2. Record the reproducible inputs: `ThermalConfig` values, adapter name,
   softlab/Python versions, source digest, delay policy (1 vs 0), and the
   independent Euler oracle from M02 (`test_m02_w01_characterization.py`,
   `ThermalPolicyComparison.euler`) used to validate stored samples.
3. Note fields that are **not** truthfully available today (e.g. later
   outcome stages, environment lock, dependency lock) so the scope decision
   records them as unavailable rather than implied.

### C5b. Signal-monitor client (in-memory only today)

**Object:** `examples/signal_monitor.py` (`acquire(points=3)` using
`run_count` with a synthetic signal/reference source).

Procedures and expected evidence — an inventory of what a *stored* record
would require, with each item answered by evidence or explicitly flagged
unavailable:

1. **Who constructs it:** record that today only the example-local
   `acquire()` builds the `DataGroup`/`DataRecord` in memory; no writer or
   manifest code exists for this client. State which future component would
   own construction (a Phase 1 design input, not an implementation).
2. **Clean-session rerun inputs:** enumerate what a rerun would need to be
   deterministic: source construction parameters (signal/reference value
   sequences or generator spec), `points`, scheduler/loop ownership,
   boundary count — and record that the consumptive source semantics (each
   read consumes one queued value, per the M02 `_MonitorSource` double)
   make input re-declaration a required rerun input.
3. **Selected backend:** record that no backend is selected today; list the
   candidate evidenced in C2/C4 (HDF5 text-meta behavior, SQLite limited
   meta) as decision inputs with their characterized constraints.
4. **Field origins:** map each candidate stored field to its origin
   (DataRecord column definitions, derived `difference` computed by the
   caller, timing from the scheduler), marking which fields are computed
   in-memory only today.
5. **Independent sample oracle:** define the analytic oracle to be used in
   later phases — e.g. for queued signal values `[10, 13, 17]` and
   references `[1, 2, 4]`, expected differences `[9, 11, 13]` computed
   independently of any stored object — and record that self-comparison
   (rerun vs rerun without an external oracle) is **not** accepted as
   validation evidence.

## C6 — Safety and hygiene

- All runtime fixtures synthetic; all paths under `tempfile.TemporaryDirectory`
  with unique prefixes; no writes outside the temp root.
- HDF5/SQLite inspection only on disposable byte-copies of synthetic files;
  original-byte preservation checked by sha256 before/after (C3).
- No real VISA/hardware; the `@sim` backend is not needed and not used.
- Environment recording per run: exact command lines, `sys.version`,
  `softlab.__version__`, and actually-observed `numpy`, `pandas`, `h5py`
  (+libhdf5), `sqlite3`, `matplotlib` versions.
- All warnings observed are recorded verbatim in the results file; none are
  waived, suppressed or filtered (existing baseline warnings are recorded as
  baseline, not hidden).
- Tests leave scheduler, backend connections and VISA resources released;
  no global default instances leak between tests.

## Gate linkage

Per-area verdicts in `characterization-results.md` (C1–C6) are the test-side
evidence for M03-AC01. The evidence decision itself (minimal supported
boundary vs separate records vs blocker) is owned by Product Owner and
Architect and is **not** a tester verdict. If any area ends `BLOCKED`, the
results file records the reproduction evidence and the decision is escalated
rather than waived. No schema implementation begins before that decision is
approved.
