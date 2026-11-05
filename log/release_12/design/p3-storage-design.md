# Detailed Design — M03 Phase 3: Storage integration and safe inspection

## Design Information

- **Designer**: sw-celeste
- **Date**: 2026-11-01 Asia/Shanghai (third revision 2026-11-02)
- **Task**: Release 12 (M03) Phase 3 — storage and inspection
  ([PRD](../prd.md) Phase 3 row, **M03-AC03**, **M03-AC04**)
- **References**:
  - Architecture: [arch.md](../../../arch.md) (current architecture;
    `run_storage.py` is a planned addition, marked as such)
  - Binding boundary: [p1-boundary-decision.md](p1-boundary-decision.md)
    (option (a); §2 supported backend/ownership and read discipline;
    §4 dispositions **D1, D2, F-A, F-B, F-C, F-D**; §6 items 3–8)
  - Phase 2 design: [p2-schema-design.md](p2-schema-design.md)
    (frozen codec contract; §4 `build_run_group` seam; **§9 deferral
    list** — every item is picked up here)
  - Test plan: [storage-test-plan.md](../test/storage-test-plan.md)
    (APPROVED, commit `30f48a9`, reviewer closure `b31d687`) — all
    seven placeholders **[P3P-01]–[P3P-07]** are frozen in this
    document; the freeze table is §10
  - Characterization: [characterization-results.md](../test/characterization-results.md)
    (F-A destructive plain load, F-B hydration key loss, F-C/F-D io
    helper breakage, D2 SQLite empty meta; the 3.9 `autospec`+`wraps`
    lesson)
  - Grounding source: `softlab/shui/data/hdf5.py`,
    `softlab/shui/data/backend.py`, `softlab/shui/backend.py`,
    `softlab/shui/data/base.py` (`DataGroup.add_record`/`set_meta`),
    `softlab/shui/run_group.py`, `softlab/shui/run_record.py`,
    `examples/thermal_experiment.py` (save/hydration paths),
    `examples/signal_monitor.py`
  - Empirical probes executed for this design on **both supported
    interpreters** — Python 3.13.15 (project venv, h5py 3.16.0,
    libhdf5 2.2.0) and Python 3.9.23 (scratch conda env, h5py 3.14.0,
    libhdf5 1.14.6): recorded in §6.2 (fault seams) and §4.1 (import
    warnings)
- **Status**: REVISED (third round) per architecture re-review at
  `44c3d95`
  ([p3-design-architecture-review.md](../reviews/p3-design-architecture-review.md))
  — finding 3 closed by restructuring the inspection algorithm so every
  post-scratch-creation failure passes through the cleanup/re-hash
  `try/finally` before any exception escapes, narrowing R4 to genuinely
  pre-resource raises, and freezing report-bearing re-hash-failure
  handling (new rule R6; §3.1.1, §6.1); finding 4 closed by adding the
  boundary-decision §2-step-3 read-side delta to the §3.5.1 follow-up
  table; finding 7 closed by executing the withdrawn A4.3 candidate (b)
  probe on Python 3.9.23 so the vacuous-seam evidence is genuinely
  both-interpreter (§6.2). Findings 1–6 of the original review
  (commit `26a13b6`) remain resolved as recorded in the previous
  revision — resubmitted for re-review by sw-jerry

## Overview

Phase 3 delivers the storage half of the additive run record: one
**owned save path** and one **safe read/inspection path**, both in a
new module `softlab/shui/run_storage.py`, plus the minimal example
wiring that dual-writes the additive `run_record` key alongside the
unchanged legacy thermal manifest. Everything deferred by the Phase 2
design §9 is picked up here and only that; nothing in the Phase 2
codec contract changes.

Design principles applied:

- **Simplicity first (如无必要，勿增实体).** One new library module,
  two entry points, five exception classes, three small frozen
  containers, three hydration-key constants. No new third-party
  dependency (h5py is already a project dependency, exercised through
  the existing `HDF5DataBackend`; direct `h5py` use in the inspection
  path is the decision-§2-mandated preflight). No backend repair:
  F-A, F-B, F-C, F-D, D1, D2 remain tracked debt and are designed
  **around**, never fixed.
- **The legacy example is the disciplined writer it already is.** The
  thermal example's owned writer block (status checks, verified
  disconnect, independent cleanup diagnostics) is the characterized
  discipline decision §2 mandates. Phase 3 lifts that discipline into
  the library seam so the second client and the test surface share
  exactly one implementation, and routes the example through it with
  its legacy failure surface preserved exactly (§2.4).
- **The F-A guard is structural, not advisory.** All legacy backend
  reads happen on a disposable sha256-verified byte copy, at most one
  legacy load per copy, with the additive key captured by `h5py`
  preflight and re-hydrated explicitly (F-B). A plain
  `HDF5DataBackend.load_group` on an original file is never the
  supported path (§3.6).
- **Truthful failure evidence.** Every fault is injected at a real
  seam whose failure stage was empirically verified on **both**
  Python 3.13.15 and Python 3.9.23 during this design (§6.2); seams
  that do not fail were probed, documented as vacuous on both
  interpreters, and rejected. Primary and cleanup diagnostics are
  independently observable in one frozen container carried by every
  escaping exception (§6.1).

### Module placement (五行 justification)

`softlab/shui/run_storage.py`. `shui` owns data and configuration
management; the owned persistence and inspection of a stored run
record is a data-layer concern, not scheduling (`huo`), abstraction
(`tu`), tooling (`jin`), or application (`mu`). The placement mirrors
the reviewed Phase 2 precedent: `run_record.py` (pure codec, stdlib
only) and `run_group.py` (the only module importing `shui.data`).
`run_storage.py` imports `HDF5DataBackend`/`DataBackend` from
`softlab.shui.data`, the codec from `softlab.shui.run_record`, `h5py`
(already a project dependency), and standard-library modules
(`dataclasses`, `hashlib`, `shutil`, `tempfile`, `typing`). Nothing
imports `run_storage` from `run_record` or `run_group`, so no import
cycle exists regardless of `__init__` order.

## 1. Component structure

### 1.1 Static structure (module/class diagram)

```mermaid
classDiagram
    class save_run_group {
        <<module function, run_storage.py>>
        +save_run_group(group, path, *, backend=None)$ StorageReport
    }
    class inspect_run_group {
        <<module function, run_storage.py>>
        +inspect_run_group(path, group_id=None, *, legacy_keys=(), backend=None)$ InspectionResult
    }
    class hydration_constants {
        <<module constants, run_storage.py>>
        THERMAL_HYDRATION_KEYS: tuple
        SIGNAL_HYDRATION_KEYS: tuple  (empty — frozen)
        PROFILE_HYDRATION_KEYS: dict
    }

    class RunStorageError {
        <<exception>> base — never raised directly
        +report: StorageReport
    }
    class StorageConnectError { <<exception>> }
    class StorageSaveError { <<exception>> }
    class StorageInspectionError { <<exception>> }
    class UnsupportedBackendError { <<exception>> }
    RunStorageError <|-- StorageConnectError
    RunStorageError <|-- StorageSaveError
    RunStorageError <|-- StorageInspectionError
    RunStorageError <|-- UnsupportedBackendError

    class StorageReport {
        <<frozen dataclass>>
        +operation: str
        +path: str
        +primary_ok: bool
        +primary: Tuple[str, ...]
        +cleanup_ok: bool
        +cleanup: Tuple[str, ...]
    }
    class StoredRecordFacts {
        <<frozen dataclass>>
        +name: str
        +columns: Tuple[Dict, ...]
        +row_count: int
    }
    class InspectionResult {
        <<frozen dataclass>>
        +group: DataGroup
        +envelope: Optional[Dict]
        +legacy_meta: Dict[str, str]
        +record_facts: Dict[str, StoredRecordFacts]
        +original_sha256: str
        +report: StorageReport
    }

    class HDF5DataBackend { <<softlab.shui.data.hdf5>> }
    class DataGroup { <<softlab.shui.data.base>> }
    class codec { <<softlab.shui.run_record>> encode/decode }
    class thermal_example { <<examples/thermal_experiment.py>> }

    save_run_group ..> HDF5DataBackend : owns connect/save/verified disconnect
    save_run_group ..> codec : decode_envelope (pre-connect validation)
    save_run_group ..> StorageReport : returns / attaches to raised errors
    inspect_run_group ..> HDF5DataBackend : exactly one load, on the copy only
    inspect_run_group ..> codec : decode_envelope (post-capture)
    inspect_run_group ..> InspectionResult : returns
    inspect_run_group ..> StorageReport : inside InspectionResult / on raised errors
    thermal_example ..> save_run_group : writer stage (§2.4)
    note for inspect_run_group "h5py preflight on disposable copy;\nnever a plain load of the original (F-A)"
    note for save_run_group "never DataGroup(meta=...) (D1);\nnever set_meta(key, None)"
```

## 2. Owned save path

### 2.1 The exact write seam

The supported write path is, end to end:

1. **Envelope construction (client, Phase 2 builders).** The client
   builds its closed payload (`build_thermal_payload` /
   `build_signal_payload`) and the envelope (`build_envelope`). The
   envelope carries `execution_id` (bounded execution UUID) and
   `group_id` (persistent group UUID) as distinct fields.
2. **Run group construction (Phase 2 seam).**
   `build_run_group(name, envelope)` returns an **empty** `DataGroup`
   whose only metadata entry is `run_record` = validated encoded text
   (no `meta=` constructor path — D1; no `set_meta(key, None)`).
3. **Sample attachment (caller).** The caller attaches exactly one
   `DataRecord` per envelope `record` block via the existing public
   **`DataGroup.add_record(record)`** API
   (`softlab/shui/data/base.py:577` — "Add or update a record",
   raises `ValueError` for a non-`DataRecord`). Columns come from the
   envelope's `record.columns`; rows are the client's acquired
   samples. **This is the [P3P-01](a) freeze: branch (a) is
   selected.** Rationale (simplicity): `build_run_group` deliberately
   returns an empty group (Phase 2 frozen); samples are owned by the
   client acquisition, not by the storage seam; a second
   attachment mechanism inside the save signature (branch (b)) would
   duplicate the existing public API without adding honesty, because
   the save seam validates the attachment anyway (§2.2 step 3).
4. **Save (Phase 3 seam).** `save_run_group(group, path,
   backend=None)` persists the group through an owned
   `HDF5DataBackend` rooted at `path`, with connect/save status
   checks and a verified disconnect, returning a `StorageReport`.

The **completed-acquisition gate is a call-site property**: the
envelope can only encode `acquisition == 'completed'` with
`completed_rows == requested_rows` (Phase 2 §1.5), and the decision
to *construct and save* the record only after acquisition completion
is enforced and observed at the client call site (§2.4 for thermal),
never inside the library — a `shui`-level function cannot observe the
client's acquisition stage (plan A1.1).

### 2.2 `save_run_group` — signature and algorithm

```python
def save_run_group(group: DataGroup, path: str, *,
                   backend: Optional[DataBackend] = None
                   ) -> StorageReport:
    """Persist one run-record group through an owned HDF5 backend.

    The group must carry the encoded additive record under
    METADATA_KEY (attached by build_run_group or by an equivalent
    encode-then-set_meta client path) and exactly the record
    attachment the envelope declares. Persistence is HDF5-only:
    any other backend type is rejected before any I/O
    (UnsupportedBackendError, §7). The function owns the backend
    lifecycle: it connects, saves, and disconnects with the
    verified-disconnect discipline mirrored from the thermal
    example (status flags, error-list delta, raw-handle validity).
    On success it returns the StorageReport; on primary failure it
    raises the frozen RunStorageError subclass carrying the report
    in `.report`. The report is constructed at entry, before any
    raise path can escape, so EVERY exception that leaves this
    function carries `.report` — RunStorageError subclasses by
    construction, unwrapped codec errors by additive instance
    attachment at the boundary (type and message unchanged; §6.1
    population contract). A cleanup-only failure never raises: it is
    reported in `report.cleanup` with `cleanup_ok == False`
     ([P3P-03], §6.1). The target artifact is never deleted or
     truncated on failure — no rollback (decision §2 NON-support;
     artifact-state pins in §6.3).
     Clarification (acceptance Issue 2 scope): the previous sentence
     bounds the seam's own actions only — it is NOT a guarantee that
     pre-existing target content survives a failed save; the unchanged
     writer deletes an existing same-ID group before recreating it
     (§7 A6.3), so writes over an existing ID are unsupported and can
     lose prior group content on failure.
     """
```

Algorithm (failure ordering is contractual; the report exists from
entry so steps 0–3, which never touch a backend or file handle, also
raise with a report attached):

```
entry. report bookkeeping begins BEFORE any raise can escape:
     report = StorageReport(operation='save', path=str(path),
                            primary_ok=False, primary=(),
                            cleanup_ok=True, cleanup=())
     (primary_ok/cleanup_ok and the message tuples are finalized
     per the §6.1 population contract at every raise/return point)
0. Backend resolution (before anything else):
     backend is None        → backend = HDF5DataBackend()
     not isinstance(backend, HDF5DataBackend)
                            → raise UnsupportedBackendError(
                                "backend: unsupported backend type "
                                "'<type>' (supported: 'hdf5')")
                              with .report attached (pre-connect rule:
                              cleanup_ok=True, cleanup=() — no cleanup
                              obligations existed; §6.1 rule R4(a))
1. text = group.get_meta(METADATA_KEY)
     type(text) is not str  → raise StorageSaveError(
                                'run_record: additive key missing '
                                'from group metadata')  — .report attached
2. envelope = decode_envelope(text)
     — Phase 2 codec errors (RecordDecodeError /
       RecordValidationError) propagate with unchanged type and
       message; the seam attaches the assembled report to the
       exception instance before re-raising (§6.1 rule R3);
       A4.1 rejects here, before any connect; zero HDF5 attribute
       or dataset writes can have occurred (decision §6 item 3)
3. Pre-connect consistency cross-checks (each raises StorageSaveError
   with the frozen template of §2.5, .report attached):
     a. str(group.id) == envelope['group_id']            (identity)
     b. envelope['record']['name'] in group.records      (attachment)
     c. group.record(name).columns == envelope['record']['columns']
        — ORDER-SENSITIVE, against the in-memory record handed to the
        seam; the declared order is the envelope order (§3.5's
        lexicographic-enumeration finding applies only on disk)
     d. len(group.record(name)) == envelope['outcome']['completed_rows']
4. try:
     rst = backend.connect({'path': path})
     if not rst or not backend.connected or backend.errors:
         raise StorageConnectError(
             f'connect: HDF5 backend connect failed: {backend.last_error}')
     rst = backend.save_group(group)
     if not rst or not backend.just_succeeded or backend.errors:
         raise StorageSaveError(
             f'save: HDF5 backend save failed: {backend.last_error}')
   finally:
     cleanup (never raises; failure diagnostics appended to
     report.cleanup per the frozen templates of §2.6):
       if backend.file is not None or backend.connected:
           verified disconnect — same discipline as the example's
           _disconnect, with the SAME literals (§2.6 frozen table):
           capture the raw handle; call backend.disconnect();
           collect any raised exception text and the
           backend.errors delta verbatim; flag
           'Backend still connected' if backend.connected; flag
           'Backend disconnect left its file handle open' if the
           captured raw handle's id.valid; best-effort raw
           handle.close() if still valid, appending
           f'Raw handle close failed: {exc}' on failure
5. finalize and escape (§6.1 population contract):
     primary failure → report = replace(report, primary_ok=False,
                        primary=(str(exc),), cleanup_ok=<bool>,
                        cleanup=<tuple>); exc.report = report; raise
     success → return StorageReport(operation='save', path=<str>,
                 primary_ok=True, primary=(),
                 cleanup_ok=<bool>, cleanup=<tuple>)
```

Notes:

- **Writer status checks (plan A1.3).** A save is attempted only
  after a connect that returned truthy **and** left the backend
  connected **and** recorded no errors; otherwise
  `StorageConnectError` is raised and `save_group` is never called
  (assertable by call inventory: zero save calls).
- **No `DataGroup(meta=...)`, no `set_meta(key, None)`.** The seam
  never constructs a group and never writes metadata; it only reads
  `group.meta` and hands the group to `save_impl`, whose per-key
  `str(value)` conversion receives the already-encoded text (decision
  §2: "already-encoded text is preserved by the characterized HDF5
  writer").
- **No post-save rewrite.** The outcome lives inside the single
  `run_record` text encoded before connect; the seam contains no code
  path that touches metadata after the save (stage honesty, §8).
- **No post-save re-read inside the seam.** Verification from the
  stored artifact is the caller's/test's job through the safe
  inspection path (§3); keeping the save seam write-only is what
  makes the F-A guard trivially hold on the write side.

### 2.3 Who calls it — signal-profile applicability

`save_run_group` is **profile-generic**: it validates and stores any
envelope the Phase 2 codec accepts, thermal or signal. Per decision
§1, signal-monitor support is conditional on the client's owned
persistence and inspection path existing. This design delivers that
owned path at the library level in Phase 3; the signal **example**
wiring (`examples/signal_monitor.py` acquiring, building, and saving)
is Phase 4 client integration work (design §9 of Phase 2; the
conditional clause of decision §1 is cited). The test plan's A2.8
signal-profile positive fixture — a library-level
build/attach/save/inspect round trip with a synthetic record — stays
in Phase 3 because the owned path it exercises exists here.

### 2.4 Who calls it — thermal example integration point (A1.6 kept in Phase 3)

`examples/thermal_experiment.py` dual-writes the additive record
inside `execute()`, with its legacy failure surface preserved
exactly. The wiring, in `execute()`'s persistence stage:

1. **Identity capture (additive, no legacy change).** `RunResult`
   gains one field, `execution_id: Optional[str] = None`. The
   existing `after_count` seam in `_acquire` — where the `CountResult`
   snapshot is already visible — retains it:
   `result.execution_id = str(snapshot.run_id)`
   (`softlab/huo/process/bounded.py:38`). This resolves the Phase 2
   design's "today discarded; retaining it is Phase 4 wiring" note:
   A1.6 (kept in Phase 3 per the approved plan) requires the field in
   Phase 3. The addition is purely additive; characterization C1.5
   remains historically true (the pre-Phase-3 `RunResult` had no such
   field).
2. **Additive attach (after the legacy manifest attach, before the
   writer block):**
   ```python
   payload = build_thermal_payload(
       adapter=adapter, configuration=asdict(config), model=_MODEL,
       boundary={'final_tick': result.final_tick,
                 'final_sim_time': result.final_sim_time})
   envelope = build_envelope(
       profile=PROFILE_THERMAL,
       execution_id=result.execution_id,
       group_id=str(original.id),
       record_name='thermal_trace',
       columns=original.record('thermal_trace').columns,
       requested_rows=result.requested_rows,
       completed_rows=result.completed_rows,
       software={'softlab_version': softlab.__version__,
                 'python_version': platform.python_version()},
       source=_source_facts(),   # mapping below
       payload=payload)
   original.set_meta(METADATA_KEY, encode_envelope(envelope))
   ```
   `_source_facts()` maps the example's existing
   `_capture_source_identity()` result onto the Phase 2
   availability-fact shape: `digest`/`git_revision`/`git_dirty` each
   become `{'state': 'available', 'value': <captured>, 'reason':
   None}` when captured, else `{'state': 'unavailable', 'value':
   None, 'reason': <code>}` where `<code>` is `'source_mismatch'` iff
   the captured reason is the tracked-reference-file failure
   (`'Source is not the tracked reference file'`, `_query_source_git`)
   and `'capture_failed'` otherwise. Free-text reasons are never
   stored (Phase 2 §1.4); `git_dirty.state == git_revision.state`
   holds by construction (one query produces both).
3. **Save through the owned seam, legacy surface preserved.** The
   writer block is replaced by:
   ```python
   writer = HDF5DataBackend()
   try:
       report = save_run_group(original, str(data_path), backend=writer)
   except StorageConnectError as exc:
       raise RuntimeError(
           f'HDF5 writer connect failed: {writer.last_error}') from exc
   except StorageSaveError as exc:
       raise RuntimeError(
           f'HDF5 save failed: {writer.last_error}') from exc
   if not report.cleanup_ok:
       raise RuntimeError('; '.join(report.cleanup))
   writer = None
   ```
   Legacy-observable behavior is preserved: the same class-level
   backend methods are invoked in the same order (writer connect →
   save → verified disconnect, all before any reader activity — the
   `attempts == ['writer', 'reader', 'scratch']` ordering probed by
   `test_thermal_execution.py` is unchanged); failure messages are
   the legacy literals; a cleanup failure raises at the persistence
   stage exactly as the example's own `_disconnect` raise did. The
   example passes its own `writer` instance so the surrounding
   `finally` keeps its existing writer-reference discipline.
4. **Reader hydration (F-B integration for the thermal client).**
   `_load_group` is extended — additively — to capture and hydrate
   `run_record` alongside its existing keys: after the `_MANIFEST_KEY`
   block, if `METADATA_KEY` is present in the pre-captured attrs it
   must be text (`ValueError('run_record: present value is not
   text')`, mirroring the manifest guard) and is included in the
   hydrated metadata. Legacy files (no `run_record`) take exactly the
   pre-change path; `_verify_round_trip` is unchanged because both
   sides of its meta comparison now carry the key.

What the example does **not** do: no legacy manifest change (bytes,
versions 1/2, timing meanings, eleven-field contract all untouched —
A7.1), no migration of old files, no change to `inspect_stored`'s
legacy-manifest semantics (a legacy-only file still yields
`context: 'legacy_unavailable'` at the example CLI).

### 2.5 Storage error taxonomy [P3P-02 freeze]

Five classes in `softlab/shui/run_storage.py`. The base subclasses
`Exception`; the leaves multiply-inherit `ValueError` so idiomatic
`except ValueError` keeps working (the Phase 2 convention). Every
raised instance carries `.report: StorageReport` (§6.1). Codec errors
(`RecordDecodeError`/`RecordValidationError`) are **not** wrapped —
they propagate with unchanged type and message from the decode steps
(plan A3.3: "the codec's frozen diagnostics surfaced by the inspection
path"); the storage seam attaches the assembled `StorageReport` to the
propagating exception **instance** as an additive `.report` attribute
at the boundary before re-raising (§6.1 rule R3). The Phase 2 classes
themselves are untouched; every Phase 2 type/template assertion keeps
holding verbatim.

| Class | Bases | Raised for | Frozen message templates |
| --- | --- | --- | --- |
| `RunStorageError` | `Exception` | Base; never raised directly. | — |
| `StorageConnectError` | `RunStorageError, ValueError` | Connect-stage failure (A4.9); the save-never-attempted guard when connect status/error checks fail (A1.3). | `connect: HDF5 backend connect failed: <backend.last_error>` |
| `StorageSaveError` | `RunStorageError, ValueError` | Missing additive key; pre-connect envelope↔group inconsistency (identity, attachment, columns, row count); save-stage failure (A4.2). | `run_record: additive key missing from group metadata`; `group_id: group identity '<a>' differs from envelope group_id '<b>'`; `record: missing declared record '<name>'`; `record.columns: differ from envelope declaration`; `record.rows: stored row count <n> differs from outcome.completed_rows <m>`; `save: HDF5 backend save failed: <backend.last_error>` |
| `StorageInspectionError` | `RunStorageError, ValueError` | Unreadable/unhashable original at baseline (pre-resource, R4(a)); scratch copy failure; preflight failure (A4.8, before any legacy load); group resolution/identity; present-non-text additive or legacy key; legacy-load failure; envelope↔artifact cross-check mismatch; original-byte safety violation or re-verification failure. | `read: original artifact unreadable: <exc>`; `group_id: require exactly one stored group or explicit ID`; `group_id: stored group '<id>' not found`; `preflight: missing or nontext reserved attribute '<key>'`; `preflight: group identity mismatch: disk id '<a>' differs from requested '<b>'`; `run_record: present value is not text`; `<key>: present value is not text`; `load: HDF5 backend load failed: <backend.last_error>`; `cross-check: envelope group_id '<a>' differs from stored group identity '<b>'`; `cross-check: stored columns differ from envelope declaration`; `cross-check: stored row count <n> differs from outcome.completed_rows <m>`; `safety: original bytes modified during inspection`; `safety: original bytes could not be re-verified after inspection: <exc>` |
| `UnsupportedBackendError` | `RunStorageError, ValueError` | Any non-HDF5 backend passed to either entry point (A6.1). Raised at algorithm step 0, before any I/O. | `backend: unsupported backend type '<backend_type>' (supported: 'hdf5')` |

Type assertions in tests use the exact leaf classes;
`isinstance(exc, ValueError)` and `isinstance(exc, RunStorageError)`
also hold. Backend-inner texts (`<backend.last_error>`) are
runtime-recorded values, **not** frozen literals — tests assert the
template prefix and the report, never the h5py-version-dependent
inner text (§6.2 evidence).

### 2.6 Frozen cleanup message templates (disconnect / scratch) [P3P-02 second half]

The cleanup diagnostics are frozen **verbatim** against the legacy
thermal `_disconnect()` (`examples/thermal_experiment.py:319-338`),
whose literals are quoted here as the reconciliation baseline:

```python
# legacy baseline (thermal_experiment.py, verbatim):
errors.append('Backend still connected')                              # :330
errors.append('Backend disconnect left its file handle open')         # :332
errors.append(f'Raw handle close failed: {exc}')                      # :336
# ... joined by the legacy writer-block caller as:
raise RuntimeError('; '.join(errors))                                 # :338
# legacy reader/scratch wrappers (the example's _reload_group, verbatim):
errors.append(f'Reader cleanup: {exc}')                               # :725
errors.append(f'Scratch cleanup at {directory.name}: {exc}')          # :729
```

| Cleanup condition | Frozen template (library, verbatim) | Source / mapping rule |
| --- | --- | --- |
| Exception escapes `backend.disconnect()` | `str(exc)` verbatim (unexpected path — `disconnect_impl` is `catch_error`-wrapped, so this is defensive) | same as legacy `_disconnect` |
| New `backend.errors` entries after the disconnect call | the delta entries **verbatim**, e.g. `Failed to disconnect: <repr>` | prefix `Failed to disconnect: ` is frozen by production code (`backend.py:186`, the `catch_error` template `f'Failed to {action}: {e!r}'` with `action='disconnect'`); `<repr>` is runtime/version-dependent data, never asserted literally |
| `backend.connected` still true after disconnect | `Backend still connected` | legacy literal, character-exact (capital **B**) |
| Captured raw handle still valid after disconnect | `Backend disconnect left its file handle open` | legacy literal, character-exact |
| Best-effort raw `handle.close()` raises | `Raw handle close failed: <exc>` | legacy template, character-exact; `<exc>` runtime data |
| Scratch copy/directory removal raises (inspection path only) | `Scratch cleanup at <scratch_dir>: <exc>` | legacy template from `_reload_group` (`:729`), character-exact, with `<scratch_dir>` the scratch path (runtime data) and `<exc>` runtime data |

**Mapping rule (frozen):** the library cleanup messages are exactly
the legacy diagnostics for every condition both share — the
`_disconnect` inner literals verbatim for the disconnect conditions,
and the `_reload_group` scratch template verbatim for scratch
removal. The thermal example's writer-block mapping
`raise RuntimeError('; '.join(report.cleanup))` (§2.4) therefore
reproduces the legacy `_disconnect` raise message
**character-for-character** whenever the same conditions occur, and
the surrounding legacy wrappers (`Writer cleanup: <exc>` at `:888`,
`Reader cleanup: <exc>` at `:725`) continue to wrap at their own
unchanged example-layer call sites — the claim "legacy failure
surface preserved exactly" holds by construction, not by assertion.
The legacy `Reader cleanup:`/`Scratch cleanup at …:` wrappers remain
in the example layer for its own legacy reader path
(`_reload_group`/`inspect_stored` are unchanged, §2.4); the library
report carries the inner diagnostics, and the inspection seam's
scratch template keeps the legacy text so both layers speak one
literal. The ordering of entries within `report.cleanup` is also
frozen: exception text (if any), then error-delta entries in
`backend.errors` order, then `Backend still connected`, then the
open-handle flag, then the raw close failure, then scratch cleanup
(inspection only) — the legacy `_disconnect`/`_reload_group` append
order. Embedded h5py/backend exception texts remain runtime data;
tests assert the frozen prefix/template around them, never the
version-dependent inner text.

## 3. Safe read/inspection path

### 3.1 Discipline (decision §2 steps 1–4, made structural)

`inspect_run_group` implements the four-step discipline exactly:

1. **Hash and byte-copy.** Record `hashlib.sha256` of the original;
   `shutil.copy2` into a `tempfile.TemporaryDirectory` with prefix
   `softlab-m03-p3-read-`; all legacy backend reads occur only on the
   copy.
2. **h5py preflight.** `h5py.File(copy, 'r')`: enumerate groups;
   resolve `group_id` (exactly-one rule); verify reserved attributes
   `id`, `name`, `timestamp`, `backend` are present and text;
   verify `attrs['id'] == group_id`; capture the `run_record` text
   and the requested legacy keys; collect per-record on-disk facts
   (`StoredRecordFacts`). All of this happens **before** any backend
   connect — a preflight failure (A4.8) raises with zero legacy loads
   (assertable by call inventory).
3. **At most one legacy load per copy.** Connect the owned backend on
   the copy; call `backend.load_group(group_id)` exactly once, with
   the canonical `str` id (F-C condition); validate the result
   (`isinstance DataGroup`, `just_succeeded`, no new errors).
   **Hydrate explicitly**: `loaded.set_meta(key, value)` for the
   captured `run_record` text and each captured legacy key (F-B seam,
   §3.3). Decode and cross-check (§3.4).
4. **Cleanup and byte verification.** Verified disconnect of the
   reader backend and scratch cleanup, with diagnostics independent
   of the primary result; re-hash the original and require byte
   equality — a mismatch raises
   `StorageInspectionError('safety: original bytes modified during
   inspection')` even when a primary result exists, because the
   inspection's core guarantee failed; a re-hash that itself cannot
   complete raises
   `StorageInspectionError('safety: original bytes could not be
   re-verified after inspection: <exc>')` — the guarantee is
   unprovable, which is likewise a primary failure, never a silent
   pass (exact ordering and report population: §3.1.1, rule R6). On
   success the post-inspection hash is exposed as
   `InspectionResult.original_sha256` so tests can assert it against
   their own pre/post hashes.

### 3.1.1 Inspection algorithm and report/finally ordering (frozen)

The same report-at-entry contract as the save path (§2.2, §6.1)
governs the inspection; every escaping exception carries `.report`.
The structural rule (frozen, re-review finding 3): **every operation
from scratch creation onward executes inside the single
cleanup/re-hash `try/finally`**, and final report population plus
exception attachment happens only **after** that `finally` has merged
its diagnostics. Only genuinely pre-resource raises — step 0 backend
resolution and a baseline-hash failure before `TemporaryDirectory`
creation — occur outside it, and for those the absence of cleanup
obligations is provable (rule R4(a)).

```
entry. report = StorageReport(operation='inspect', path=str(path),
                              primary_ok=False, primary=(),
                              cleanup_ok=True, cleanup=())   (R1)
0. Backend resolution — identical to save step 0
   (UnsupportedBackendError with .report; pre-resource, R4(a)).
1. baseline = sha256(original)   — an unreadable/unhashable original
   raises StorageInspectionError(
   'read: original artifact unreadable: <exc>') with .report BEFORE
   any owned resource exists: no scratch directory, no backend
   connect (R4(a), provable by recorder inventory and §6.3 pins).
2. body_exc = None; safety_diag = None
   scratch = None; reader_connect_attempted = False
   try:
     3. scratch = TemporaryDirectory(prefix='softlab-m03-p3-read-');
        shutil.copy2(original, copy)
        — the scratch cleanup obligation exists from TemporaryDirectory
        creation; a copy failure raises StorageInspectionError with
        .report INSIDE the guarded region, so the owed scratch removal
        (and re-hash) run before the exception escapes.
     4. h5py preflight on the copy (before any backend connect):
        enumerate groups; resolve group_id (exactly-one rule);
        reserved attributes present and text; disk id == group_id;
        capture run_record / requested legacy texts; collect
        StoredRecordFacts. Any failure → StorageInspectionError
        (preflight templates §2.5); zero legacy loads (recorder
        inventory); the finally below still executes.
     5. present non-text run_record → StorageInspectionError(
        'run_record: present value is not text'); the finally below
        still executes.
     6. reader_connect_attempted = True
        connect the owned backend on the copy (status/error checks →
        StorageConnectError);
        loaded = backend.load_group(str(group_id))  — exactly once;
        validate (isinstance DataGroup, just_succeeded, no new errors);
        hydrate run_record + requested legacy keys (set_meta);
        if run_record text present:
            envelope = decode_envelope(text)   — codec errors propagate
            with unchanged type/message (R3);
            cross-checks (§3.4) → StorageInspectionError;
        else:
            envelope = None                    — truthful legacy absence
   except Exception as exc:
     body_exc = exc    — escape is DEFERRED to step 7 so the finally
     cleanup is merged into the attached report before the raise
   finally:             — never raises (R6)
     7a. if reader_connect_attempted and (backend.file is not None or
         backend.connected): verified reader disconnect — §2.6 frozen
         templates appended in the frozen order.
     7b. if scratch is not None: best-effort scratch removal; failure
         → 'Scratch cleanup at <scratch_dir>: <exc>' appended (§2.6).
     7c. guarded original re-hash (the safety capture):
         try: post = sha256(original)
         except Exception as rehash_exc:
             safety_diag = 'safety: original bytes could not be
                 re-verified after inspection: <rehash_exc>'
         else:
             if post != baseline: safety_diag =
                 'safety: original bytes modified during inspection'
7. finalize — deterministic; every escape carries the fully populated
   report (R1/R2/R5/R6):
   - safety_diag is not None → the inspection's core guarantee is
     void and DOMINATES: raise StorageInspectionError(safety_diag)
     with .report; if body_exc exists it is chained
     ('raise ... from body_exc', __cause__ = body_exc) and preserved
     in the report — primary == (safety_diag, str(body_exc)) per R2's
     coincident-failure rule; otherwise primary == (safety_diag,).
     primary_ok=False; cleanup slots merged from 7a/7b.
   - elif body_exc is not None → finalize report with
     primary_ok=False, primary == (str(body_exc),); attach:
     RunStorageError subclasses by construction, codec instances by
     additive .report attachment (R3 — the finally has already run,
     so the attached report carries complete cleanup diagnostics);
     raise body_exc.
   - else → return InspectionResult(..., original_sha256=post,
     report = replace(report, primary_ok=True, primary=(),
     cleanup_ok=<bool>, cleanup=<tuple>)).
```

Deterministic outcomes for every payload (§6.1 rules R1–R6):
`primary_ok`/`cleanup_ok` booleans and the two message tuples are
always present and populated per the contract — no path exists where
`primary_ok=False` co-occurs with an empty `primary` tuple, no
escaping exception lacks `.report`, and no raise path exists where an
empty `cleanup` tuple co-occurs with an unexecuted cleanup obligation:
post-scratch-creation failures (copy failure, A4.8 preflight,
present-non-text, connect/load/decode/cross-check) all pass through
the `finally`, so their reports truthfully record either a clean
cleanup or the frozen cleanup failure diagnostics.

### 3.2 Signature and result structures [P3P-01 freeze]

```python
def inspect_run_group(path: str, group_id: Optional[str] = None, *,
                      legacy_keys: Sequence[str] = (),
                      backend: Optional[DataBackend] = None
                      ) -> InspectionResult:
```

- `path` — the **original** artifact; it is never opened by a legacy
  backend loader (§3.6).
- `group_id` — canonical UUID text of the stored group; `None`
  triggers the exactly-one rule.
- `legacy_keys` — the client-declared legacy hydration set
  (§3.3); default `()` hydrates only the additive key.
- `backend` — optional injection seam for test instrumentation
  (recording spies per the Phase 1 technique); same resolution and
  `UnsupportedBackendError` guard as the save path. The function owns
  its lifecycle regardless of provenance.

```python
@dataclasses.dataclass(frozen=True)
class StoredRecordFacts:
    """h5py-level on-disk record facts, independent of DataGroup."""
    name: str
    columns: Tuple[Dict[str, Any], ...]   # per-dataset name/label/
                                          # unit/dependent attrs, in
                                          # h5py enumeration order
                                          # (lexicographic — §3.5)
    row_count: int                        # dataset shape[0]; all
                                          # column datasets must agree

@dataclasses.dataclass(frozen=True)
class InspectionResult:
    group: DataGroup                      # hydrated group from the
                                          # single legacy load
    envelope: Optional[Dict[str, Any]]    # decoded envelope; None iff
                                          # legacy absence (§3.4)
    legacy_meta: Dict[str, str]           # raw legacy text attributes,
                                          # requested keys only
    record_facts: Dict[str, StoredRecordFacts]  # keyed by record name
    original_sha256: str                  # post-inspection hash (==
                                          # pre hash on success)
    report: StorageReport
```

### 3.3 Additive-key hydration and per-profile key sets [P3P-01(b) freeze]

The F-B seam: the single legacy load drops **all** custom metadata
keys (characterized: `loaded.meta == {}` after a plain load, per
R4-BASE-META-01). The inspection therefore captures required text
attributes during preflight and re-attaches them explicitly after the
load. The hydration set is exactly: `run_record` (always, when
present) plus the caller-declared `legacy_keys`. Frozen per-profile
constants in `run_storage.py` (re-declared literals, never imported
from `examples/` — the Phase 2 `THERMAL_MODEL` discipline):

```python
THERMAL_HYDRATION_KEYS = ('example', 'configuration', 'timing',
                          'thermal_manifest')
SIGNAL_HYDRATION_KEYS: Tuple[str, ...] = ()   # the empty set — itself
                                              # a frozen literal
PROFILE_HYDRATION_KEYS = {
    PROFILE_THERMAL: THERMAL_HYDRATION_KEYS,
    PROFILE_SIGNAL: SIGNAL_HYDRATION_KEYS,
}
```

Binding rules (F-B condition):

- Hydration never promises arbitrary custom-key restoration: only
  `run_record` and the explicitly requested keys are re-attached.
- The thermal keys are never imposed on the signal profile: a signal
  inspection hydrates `run_record` and nothing else (A2.8 asserts
  this positively).
- An absent requested legacy key is **not** an error (legacy absence
  semantics unchanged); a present non-text value of any requested key
  is `StorageInspectionError('<key>: present value is not text')`.

**[P3P-01](c) freeze — "legacy context still available":** the
inspection result carries the **raw legacy text attributes** in
`legacy_meta` (and on the hydrated group). Decoding the legacy
manifest is **not** part of the storage seam: manifest decoding lives
only in the example layer (`_decode_manifest`), and this design does
not move it (interacts with [P3P-06], §9 — the seam is in
`softlab/shui/`, which must not depend on `examples/`). For A3.3's
absence outcome, "legacy context available" means: `envelope is
None`, `legacy_meta` holds the raw requested texts, and the caller
(example layer) may decode them with its own legacy codec.

### 3.4 Three-way distinction at the storage level (A3.3)

| On-disk state of `run_record` | Outcome | Type |
| --- | --- | --- |
| **Absent** (genuine legacy-only file) | Truthful legacy absence: `envelope is None`, `legacy_meta` populated, no error, never a fabricated envelope | — (success) |
| **Present, non-text** (e.g. an int written via raw h5py) | Visible failure | `StorageInspectionError('run_record: present value is not text')` |
| **Present text: malformed / wrong namespace / unsupported version / over-size** | Visible failure with the Phase 2 frozen diagnostics, surfaced unwrapped | `RecordDecodeError` / `RecordValidationError` |

When the envelope decodes, the inspection cross-checks it against the
stored artifact; any mismatch is `StorageInspectionError` with the
frozen `cross-check:` templates (§2.5): envelope `group_id` vs stored
group name/`id` attribute; declared columns vs stored columns
(per-name association, §3.5); `outcome.completed_rows` vs stored row
count.

### 3.5 On-disk column order — characterized constraint (probe-verified)

HDF5 group iteration is **lexicographic by name**, not insertion
order, under the characterized writer (verified during this design,
Python 3.13.15 / h5py 3.16.0: a record saved in order `(timestamp,
signal, reference)` enumerates on disk and reloads as `(reference,
signal, timestamp)`; per-dataset `name`/`label`/`unit`/`dependent`
attributes and per-column row data are preserved exactly). This is a
property of the unchanged legacy writer (`hdf5.py` `create_dataset`
without `track_order`) and is **not repaired** in Phase 3.

Consequences, frozen:

- The **declared column order** lives in the envelope
  (`record.columns`) and is verified order-sensitively against the
  **in-memory** record handed to `save_run_group` (§2.2 step 3c).
- On disk, ordered-schema verification is a **by-name association**:
  the set of stored column dataset names equals the set of declared
  names, and each declared column (in envelope order) matches its
  same-named dataset's four attributes exactly. Row order within each
  dataset is array order and round-trips exactly; row-count relations
  are per §3.4.
- `StoredRecordFacts.columns` is documented as h5py enumeration order
  (lexicographic); tests must not equate it with declaration order.
- For the **thermal** client the frozen six-column declaration is
  already lexicographically ordered, so the legacy order-sensitive
  reload comparisons (`_verify_round_trip`) keep passing unchanged —
  no legacy behavior change. For the **signal** profile the declared
  order `(timestamp, signal, reference)` is not lexicographic; the
  order claim rests on the envelope declaration plus the by-name
  association, which is exactly the decision-§1.1 requirement
  ("association between those declarations and the group's ordered
  samples") realized against the characterized backend.

### 3.5.1 Ordering-contract clarification (architecture-review finding 4)

The frozen, source-faithful reading of the ordering boundary —
adopted by this design and **submitted for explicit Product
Owner/Architect confirmation** — is:

1. The **envelope's declared column sequence is the canonical stored
   semantic order**. "Stored column order" in the binding documents
   means this declared order, which the save seam enforces
   order-sensitively against the in-memory record before any write.
2. **Physical HDF5 key enumeration is non-semantic** — a
   characterized property of the unchanged legacy writer
   (`create_dataset` without `track_order`), not repaired in Phase 3.
3. The **disk cross-check is by-name**: for each declared column, in
   envelope order, the same-named dataset exists and its four
   attributes match exactly; the name sets are equal.
4. The **sample-order check is by row index**: per-column sample
   arrays are compared element-wise by row index against the
   expectation (Phase 4 oracle), never via enumeration order.
5. **Phase 4 by-name reconstruction rule (frozen here for Phase 4):**
   the numeric/sample oracle iterates the envelope's declared column
   sequence, retrieves each same-named dataset (or hydrated record
   column), and compares sample arrays by row index. It must not
   treat `StoredRecordFacts.columns` or a reloaded
   `DataRecord.columns` order as declaration order. Under this rule
   the signal tick/offset and derived-difference checks remain
   implementable without any backend repair.

This clarification changes the **literal wording** of the currently
approved stored-order requirement (physical enumeration cannot equal
declaration order for signal), so the following follow-up delta
revisions are named explicitly — they require sw-mike plan alignment
plus sw-tom/developer and sw-jerry re-review before test code is
written; nothing here is silently re-interpreted:

| Document | Section | Required delta |
| --- | --- | --- |
| `log/release_12/test/storage-test-plan.md` | **A1.2**, fourth bullet ("stored column order equals envelope `record.columns` order (decision §1.1 sample-schema row)") | Reword to the §3.5.1 rule: envelope order is canonical; disk verification is by-name association; samples by row index. |
| `log/release_12/test/storage-test-plan.md` | **A2.5** ("ordered schema/units ... cross-checked against the stored artifact") | Same by-name/row-index wording for the read-side cross-check. |
| `log/release_12/test/storage-test-plan.md` | **A3.1** ("ordered schema/units" in the fresh-process payload assertions) | Align with the §4.3 payload rule: `records[*].columns` in h5py enumeration order, envelope carries declared order, by-name comparison. |
| `log/release_12/design/p1-boundary-decision.md` | **§1.1 sample-schema row** ("Envelope order must match stored column order and sample row order.") | Clarification note (not a silent edit): "stored column order" = the declared semantic order carried by the envelope and enforced pre-save; physical HDF5 enumeration is non-semantic; by-name association + row-index sample comparison realize the requirement. |
| `log/release_12/design/p1-boundary-decision.md` | **§2 supported read/inspection discipline, step 3** ("decode, validate, and cross-check identity, ordered schema/units, row counts, and samples") | Read-side addendum to the same clarification set: "ordered schema/units" means the §3.5.1 by-name association (declared order is the envelope order; physical enumeration is non-semantic), and "samples" are compared by row index — so the boundary's write-side (§1.1) and read-side (§2 step 3) ordering rules share one interpretation and cannot diverge. |
| `log/release_12/design/p2-schema-design.md` | **§1.3** ("Envelope column order must match stored column order; the cross-check against the actual stored datasets is …") | Addendum cross-referencing §3.5.1 here (the Phase 2 document stays historically intact; the deferral to Phase 3 already anticipated the stored-artifact cross-check definition). |

The alternative — persisting physical creation order (e.g.
`track_order`) — would be a separately reviewed storage change and is
**not** introduced here; it would touch the legacy writer and break
the no-repair discipline.

**Scope of this delta list (frozen, third revision):** the table above
is the complete set of approved-document wording affected by the
ordering clarification, now including the boundary's read-side
statement (re-review finding 4). The third-revision fixes for
re-review findings 3 and 7 are design-internal freeze completions —
the §3.1.1 ordering restructure, the R2 coincident-failure rule, R4's
narrowing, the new R6 and its two §2.5 templates, and the §6.2
candidate-(b) evidence completion change **no** approved-plan or
boundary literal: plan A4 universal assertion (c) is satisfied by the
two-entry coincident rule (both diagnostics remain independently
retrievable), the new safety/read templates complete this design's
own [P3P-02] freeze, and the plan's A1.2/A2.5/A3.1 ordering literals
are covered by the rows above. No further delta rows are owed.

### 3.6 The F-A guard

- The supported inspection entry point never opens the original file
  with a legacy backend loader; the only legacy load targets the
  disposable copy, at most once per copy.
- A second inspection of the same original uses a **fresh copy**
  (each `inspect_run_group` call creates its own scratch) — verified
  during this design (probe: two sequential inspections of one
  original, each on a fresh copy, both succeed; original sha256
  unchanged throughout).
- Repeated loads of one copy are unsupported by construction: the
  function performs the load exactly once and discards the copy.
- Plain destructive `load_group` on an original file is never the
  supported path; the destructive chain (F-A) is exercised only on
  synthetic copies, exactly as Phase 1 did.

## 4. Fresh-process inspection [P3P-04 freeze]

### 4.1 Entry point

A **test-side bootstrap module** `tests/run_record_inspect_bootstrap.py`,
modeled on `tests/thermal_cli_bootstrap.py`: a standalone module that
installs the warning policy and runs the inspection CLI. It is a test
instrument, not production surface — the library gains no CLI
(simplicity: one consumer, no general CLI contract to support).

- **stdout discipline (frozen).** `HDF5DataBackend.connect_impl()`
  prints its connection message to **stdout** (`hdf5.py:40`), and the
  inspection entry point performs a real connect on the disposable
  copy. The bootstrap therefore wraps the entire inspection call in
  `contextlib.redirect_stdout(sys.stderr)` — exactly the established
  precedent in the thermal CLI (`examples/thermal_experiment.py:948-949`,
  `with redirect_stdout(sys.stderr): stored, manifest =
  inspect_stored(...)`), with the corresponding
  `from contextlib import redirect_stdout` import. **stdout carries
  the single JSON payload and nothing else**; stderr carries backend
  operational prints, diagnostics, and tracebacks.
- **Warning policy (frozen, mirrored verbatim).**
  `warnings.simplefilter('error')` plus **every** applicable exact
  allowance from the reviewed `tests/thermal_cli_bootstrap.py`
  precedent — quoted here verbatim, including category and module
  matching:
  ```python
  DEPENDENCY_WARNINGS = (
      ("'oneOf' deprecated - use 'one_of'", "matplotlib._fontconfig_pattern"),
      ("'parseString' deprecated - use 'parse_string'", "matplotlib._fontconfig_pattern"),
      ("'resetCache' deprecated - use 'reset_cache'", "matplotlib._fontconfig_pattern"),
      ("'enablePackrat' deprecated - use 'enable_packrat'", "matplotlib._mathtext"),
  )
  # each allowed as: warnings.filterwarnings(
  #     "default", message="^" + re.escape(message) + "$",
  #     category=PyparsingDeprecationWarning,
  #     module="^" + re.escape(module) + "$")
  PANDAS_BASELINE_WARNING = (
      "The behavior of DataFrame concatenation with empty or all-NA entries is "
      "deprecated. In a future version, this will no longer exclude empty or "
      "all-NA columns when determining the result dtypes. To retain the old "
      "behavior, exclude the relevant entries before the concat operation."
  )
  # allowed as: category=FutureWarning,
  # module=r"^softlab\.shui\.data\.base$"
  ```
  **Applicability is probe-verified, not assumed** (executed during
  this design): the child imports `softlab.shui.run_storage`, whose
  parent-package initialization eagerly imports `softlab.jin.vis` →
  matplotlib → pyparsing. On Python 3.9.23 (matplotlib 3.9.4,
  pyparsing 3.3.3), `python -W error -c "import
  softlab.shui.run_group, softlab.shui.data.hdf5"` **fails** with
  `pyparsing.warnings.PyparsingDeprecationWarning: 'oneOf' deprecated
  - use 'one_of'` from `matplotlib/_fontconfig_pattern.py`; a
  warning-capture run recorded 14 warnings, **every one matching one
  of the four quoted allowances exactly** (message + module). On
  Python 3.13.15 (matplotlib 3.10.x) the same import under `-W error`
  is clean. The pandas baseline fires on 3.9/pandas 2.3.3 at
  `DataRecord.add_rows` (`base.py:314`, observed verbatim in the §6.2
  probe run) and does not fire on 3.13/pandas 3.0.6. The policy is
  mirrored, not imported, so the two bootstrap modules stay decoupled.
  Every other warning stays fatal and is recorded verbatim; no broad
  suppression is added.
- **Spawning convention (A3.1):** the parent test spawns a fresh
  interpreter with the project venv:
  `python tests/run_record_inspect_bootstrap.py <path> --profile
  {thermal,signal} [--group-id <uuid>]`, captures stdout/stderr, and
  asserts on the JSON payload and exit code. Where warnings must
  surface, the policy makes them fatal in the child.

### 4.2 CLI surface

| Argument | Required | Meaning |
| --- | --- | --- |
| `path` (positional) | yes | Original artifact path (the child copies it internally; the original is never legacy-loaded). |
| `--profile {thermal,signal}` | yes | Selects the frozen `PROFILE_HYDRATION_KEYS` set for legacy-key hydration. |
| `--group-id <uuid>` | no | Explicit stored group id; omitting it exercises the exactly-one rule. |

**Exit codes (frozen):** `0` = inspection completed (payload on
stdout; includes legacy absence and cleanup-only failure — cleanup
truth is in the payload's `report`); `1` = inspection failure (error
payload on stdout, traceback on stderr); `2` = CLI usage error
(argparse default, message on stderr).

### 4.3 JSON payload schema (frozen)

Success (exit 0):

```json
{
  "status": "ok",
  "group_id": "<canonical uuid text>",
  "envelope_present": true,
  "envelope": {"namespace": "softlab.run-record", "...": "..."},
  "legacy_meta": {"example": "...", "configuration": "...", "timing": "...", "thermal_manifest": "..."},
  "records": {
    "<record name>": {
      "columns": [{"name": "...", "label": "...", "unit": "...", "dependent": true}],
      "row_count": 8,
      "samples": {"<column>": [0.0, 1.0]}
    }
  },
  "original_sha256": "<64 lowercase hex>",
  "report": {"operation": "inspect", "path": "<path>",
             "primary_ok": true, "primary": [],
             "cleanup_ok": true, "cleanup": []}
}
```

- `envelope` is `null` and `envelope_present` is `false` exactly in
  the legacy-absence case (A3.3 first bullet).
- `records[*].columns` is h5py enumeration order (lexicographic,
  §3.5); the envelope carries the declared order. `samples` values
  come from the hydrated group's record tables via `.tolist()`
  (JSON-native); the CLI is a bounded-fixture instrument — payloads
  are asserted against the small synthetic fixtures of A1–A3.
- `report` serializes the frozen container one-for-one (§6.1).

Failure (exit 1):

```json
{"status": "error",
 "error_type": "StorageInspectionError",
 "error_message": "preflight: missing or nontext reserved attribute 'id'",
 "report": {"operation": "inspect", "path": "<path>",
            "primary_ok": false, "primary": ["..."],
            "cleanup_ok": true, "cleanup": []}}
```

`error_type` is the exact leaf class name (`StorageConnectError`,
`StorageSaveError`, `StorageInspectionError`,
`UnsupportedBackendError`, `RecordDecodeError`,
`RecordValidationError`). The `report` field is **always present** on
exit-1 payloads: for `RunStorageError` subclasses it serializes the
raised `.report`; for unwrapped codec errors it serializes the
additively attached `.report` (§6.1 rule R3) — the schema is uniform
and the codec exception's type/message remain the Phase 2 frozen
surface. `report.primary` carries the primary diagnostic (first entry: the
escaping exception's message, per §6.1 rule R2; a second entry
appears only in the R2 coincident-failure case — an in-flight body
exception preserved alongside the dominating byte-safety failure) and
`report.cleanup` the cleanup diagnostics; neither masks the other.

### 4.4 No-test-state guarantee (A3.2)

The child imports only: `argparse`, `contextlib` (`redirect_stdout`,
§4.1), `json`, `re`/`warnings` (policy), `sys`,
`pyparsing.warnings` (`PyparsingDeprecationWarning` — the allowance
category, same as the reviewed precedent), and
`softlab.shui.run_storage` (with its transitive dependencies). It
receives the file path, optional group id, and profile — nothing
else. It holds no thermal example state, no prior `DataGroup`, and no
in-memory envelope; every fact in its payload comes from the stored
artifact via the safe path. This is asserted by construction: the
module has no parameter through which test state could enter.

## 5. [Reserved — see §6 for fault design]

## 6. Fault design

### 6.1 Primary-vs-cleanup diagnostics container [P3P-03 freeze]

```python
@dataclasses.dataclass(frozen=True)
class StorageReport:
    """Separately observable primary and cleanup diagnostics."""
    operation: str            # 'save' | 'inspect'
    path: str                 # target artifact path (the original,
                              # never the scratch copy)
    primary_ok: bool          # True iff the primary stage completed
    primary: Tuple[str, ...]  # primary-stage diagnostic messages
    cleanup_ok: bool          # True iff cleanup completed cleanly
    cleanup: Tuple[str, ...]  # cleanup-stage diagnostic messages
```

Observable independence contract (frozen):

- `primary` and `cleanup` are separate named fields; each is always
  present (possibly an empty tuple) regardless of the sibling's
  state. Either is retrievable when the other is absent **or itself
  failed** — including the row where the cleanup diagnostic reports a
  failure (`primary_ok=True, cleanup_ok=False, cleanup=(...)`).
- On success — including a cleanup-only failure — the entry point
  **returns** the report (cleanup failure alone never raises;
  plan A4.3: "Primary result still delivered; cleanup failure
  reported in its own diagnostic slot — neither diagnostic masks the
  other").
- On primary failure the raised exception carries the same container
  in `.report`, with `primary_ok=False` and the cleanup slots
  populated by the finally-block cleanup that always runs.

Population contract (frozen — every raise path, architecture-review
finding 3):

- **R1 — report at entry.** Both entry points construct the report
  before any contractually reported raise can escape
  (`StorageReport(operation, path=str(path), primary_ok=False,
  primary=(), cleanup_ok=True, cleanup=())`). Step-0
  (`UnsupportedBackendError`), missing-key, cross-check, and
  preflight raises all carry `.report` — no raise path is
  report-less.
- **R2 — primary diagnostics are never empty on failure.** When a
  primary failure escapes, the **first** entry of `report.primary` is
  the escaping exception's own message (a §2.5 frozen template, or
  the Phase 2 codec template for codec errors). On success
  `primary_ok=True` and `primary=()`. No path exists where
  `primary_ok=False` co-occurs with an empty `primary` tuple.
  **Coincident-failure rule (inspection only):** when a byte-safety
  failure (R6) coincides with an in-flight body exception, the safety
  exception escapes with the body exception chained as `__cause__`,
  and `report.primary == (safety_message, str(body_exc))` — both
  diagnostics preserved, neither masks the other. Every other failure
  has exactly one entry.
- **R3 — codec errors carry the report additively.** Unwrapped
  `RecordDecodeError`/`RecordValidationError` keep their Phase 2
  frozen type and message; the seam attaches the assembled report to
  the exception **instance** (`.report`) at the boundary before
  re-raising. The classes are untouched; wrapping remains forbidden.
  For the inspection path the finally-block cleanup runs **before**
  the exception escapes, so the attached report carries complete
  cleanup diagnostics (§3.1.1 ordering).
- **R4 — truthful empty-cleanup semantics.** `cleanup_ok=True,
  cleanup=()` has exactly two truthful meanings, distinguishable by
  construction:
  - **(a) pre-resource raise** — the failure escaped before any owned
    resource existed: save steps 0–3 (backend resolution, missing
    key, codec rejection, cross-checks — all pre-connect) and
    inspection steps 0–1 (backend resolution, baseline hash of the
    original before `TemporaryDirectory` creation). No backend was
    connected and no scratch directory exists — separately provable
    by recorder inventory (zero connect calls) and the §6.3 artifact
    pins, so the empty cleanup tuple can never masquerade as a clean
    cleanup of real resources.
  - **(b) cleanup ran clean** — the failure occurred after an owned
    resource existed and therefore passed through the `finally`
    (inspection §3.1.1 steps 3–6: copy failure, A4.8 preflight,
    present-non-text, connect/load/decode/cross-check; save step 4):
    the owed cleanup (scratch removal from `TemporaryDirectory`
    creation; verified disconnect from connect) **was executed**
    before the exception escaped, and `cleanup=()` means it found
    nothing to report (`cleanup_ok=True` by R5). Had that cleanup
    itself failed, the frozen §2.6 diagnostics would be present.
  No raise path exists where an empty `cleanup` tuple co-occurs with
  an unexecuted cleanup obligation.
- **R5 — cleanup tuple holds failure diagnostics only.** `cleanup`
  contains only frozen-template failure diagnostics (§2.6), in the
  frozen append order; `cleanup_ok == (len(report.cleanup) == 0)`.
  Informational messages are never appended.
- **R6 — the inspection `finally` never raises; safety outcomes are
  captured, then decided.** Verified reader disconnect and scratch
  removal append failure diagnostics per §2.6 and never raise. The
  original-byte re-hash is the last `finally` action and is itself
  guarded: a re-hash I/O failure is captured as the frozen safety
  diagnostic `safety: original bytes could not be re-verified after
  inspection: <exc>` (§2.5) instead of raising out of the `finally`;
  a completed re-hash is compared against the baseline and a mismatch
  is captured as `safety: original bytes modified during inspection`.
  The finalize step (§3.1.1 step 7) then decides deterministically:
  a captured safety failure dominates and escapes as
  `StorageInspectionError` (chaining any in-flight body exception per
  R2's coincident-failure rule); otherwise the in-flight body
  exception escapes with its finalized report; otherwise success.
  Every escape — including a re-hash failure — carries a fully
  populated report; the save path is unaffected (its re-hash-free
  cleanup already never raises).

### 6.2 Real-seam fault-injection mechanisms [P3P-05 freeze]

Every mechanism below was **empirically executed during this design
on both supported interpreters** with the results quoted — the
P3P-05 pre-freeze evidence gate is satisfied here, not deferred:

- **Python 3.13.15** (project venv `.venv`, conda-forge; h5py 3.16.0,
  libhdf5 2.2.0, numpy 2.5.3, pandas 3.0.6; macOS arm64) —
  command: `.venv/bin/python p3_fault_probes.py`.
- **Python 3.9.23** (scratch conda env created for this design:
  `conda create -p /tmp/py39 python=3.9 -y`, then
  `/tmp/py39/bin/python -m pip install -e <repo>`; h5py 3.14.0,
  libhdf5 1.14.6, numpy 2.0.2, pandas 2.3.3; macOS arm64) —
  command: `/tmp/py39/bin/python p3_fault_probes.py`.

The probe script exercises each mechanism against **existing
production code only** (the real `HDF5DataBackend`, the Phase 2
codec, `build_run_group`, raw `h5py`) — no Phase 3 code exists yet
and no mock replaces any production code path. **Every row below
failed at its named stage identically on both interpreters**; the
version-dependent inner texts differ only in runtime-data positions
(h5py repr text, temp paths), which tests never assert literally
(§2.5). The candidate-commit CI matrix re-confirms on Ubuntu as a
gate, but the pre-freeze evidence obligation is met by this table.

| A4 row | Frozen injection mechanism (real seam; no mock that skips real code) | Verified stage and observed evidence (3.13.15 **and** 3.9.23, identical stages) |
| --- | --- | --- |
| A4.1 Serialization | Hand-build a plain `DataGroup`, `set_meta('run_record', <defective text>)` (bypassing `build_run_group`), call `save_run_group`. The real codec decode at step 2 rejects before any connect. | Pre-connect, both interpreters. Probe: `decode_envelope('not json')` → `RecordDecodeError: run_record: malformed JSON: Expecting value: line 1 column 1 (char 0)`; count-drift envelope → `RecordValidationError: outcome.completed_rows: differs from outcome.requested_rows` (identical text on 3.9.23 and 3.13.15). Mechanism is the Phase 2 codec contract itself (459-test evidence on both interpreters); recorder double asserts zero connect calls and target-path non-existence. |
| A4.2 Save | **Externally closed h5py handle:** a recording spy on `backend.connect` (plain function, no autospec) delegates to the real connect, then closes `backend.file` — an external environmental fault; the real `save_impl` then executes against the closed handle. | **Save stage, verified on both interpreters:** `save_group` returned `False`; `backend.errors == ["Failed to save group: ValueError('Unable to synchronously create group (invalid identifier type to function)')"]` (identical on 3.9.23/h5py 3.14.0 and 3.13.15/h5py 3.16.0); the target file existed after connect and after the failed save, opened read-only, and contained **zero groups** (pinned artifact state, §6.3). The inner h5py text is version-dependent — assertions target the frozen `StorageSaveError` and the report, never the inner text. |
| A4.3 Disconnect | **External file-handle invalidation** (re-frozen per architecture-review finding 1 — the double-disconnect mechanism was disabled by the cleanup guard and is withdrawn): a recording spy wraps the primary backend call — `load_group` on the **inspection** path (the plan's stated primary outcome), `save_group` on the save path — delegates to the real method so primary success is genuinely established, then **detaches and retains the raw handle and clears `backend.file` to `None`** (an external environmental state fault on a backend object the harness legitimately holds — the same injection class as A4.2's externally closed handle). The owned cleanup guard `backend.file is not None or backend.connected` then passes (`connected` is still `True` — status `ActionSucceeded`), the owned cleanup's **real** `disconnect_impl` executes `None.close()`, and the failure is recorded by the real `catch_error`. The spy retains the detached handle; the test closes it in teardown and asserts the close (zero leaked handles, A8.3). | **Disconnect stage, verified on both interpreters, both paths:** after the fault, guard evaluates `True`; `disconnect()` recorded `["Failed to disconnect: AttributeError(\"'NoneType' object has no attribute 'close'\")"]` in `backend.errors` (identical text on both interpreters); status `NotConnected` after; **inspection variant**: primary load result already in hand (`isinstance DataGroup`, `just_succeeded`, no errors) before the fault — the primary inspection outcome is delivered with `cleanup_ok=False` and the failure in `report.cleanup`; original sha256 unchanged; scratch copy removed normally. **Save variant**: primary save succeeded before the fault; the complete artifact remains (§6.3). **Probed-and-vacuous candidates, rejected with both-interpreter evidence:** (a) externally closed handle then `disconnect()` — returned normally, error delta `[]` (h5py `File.close()` is idempotent; confirmed vacuous on 3.9.23 and 3.13.15); (b) scratch directory removed before disconnect — returned normally, `errors=[]` (POSIX: close on an unlinked open file succeeds; **confirmed vacuous on 3.9.23/h5py 3.14.0 and 3.13.15/h5py 3.16.0** — probe executed during this third revision in the §6.2-preamble environments: connect, `shutil.rmtree` the scratch directory with the handle still open, then `disconnect()` completes with error delta `[]`, no exception, status `NotConnected`). Both documented so the choice is auditable rather than guessed. |
| A4.4–A4.7 Invalid reload | **Corrupt-fixture construction rule:** produce a genuine artifact through the real owned save path; byte-copy it; mutate **only** the `run_record` attribute on the copy via raw `h5py` in `'r+'` mode (never through the backend), writing the one defect under test (`'not json'`, wrong-namespace text, version-2 text, over-`MAX_ENCODED_BYTES` text). The mutated copy becomes the test's delivered "original": sha256 before, inspect, sha256 after — inspection internally copies it again per §3.1, so the delivered fixture's bytes are never touched by a legacy load. | Decode/validate stage, both interpreters, per the Phase 2 frozen templates — probe through a real backend-saved artifact mutated via raw h5py: `RecordDecodeError: run_record: malformed JSON: Expecting value: line 1 column 1 (char 0)` (A4.4); `RecordValidationError: namespace: unsupported namespace (supported: softlab.run-record)` (A4.5); `RecordValidationError: version: unsupported version (supported: 1)` (A4.6); `RecordDecodeError: run_record: encoded size exceeds limit 131072 bytes` (A4.7) — identical on both interpreters; pristine original sha256 unchanged. |
| A4.8 Preflight | Hand-built fixture written **directly with h5py** (never the backend): a group named `<uuid>` with the reserved attribute set missing `id`/`backend` (or carrying non-text values). | **Preflight stage, before any legacy load, both interpreters:** probe fixture enumerated read-only; missing reserved attributes `['id', 'backend']` detectable without creating any backend; the seam raises `StorageInspectionError` with the `preflight:` template; call inventory asserts zero `load_group` calls. |
| A4.9 Connect | **Existing directory at the target path.** | **Connect stage, verified on both interpreters:** `backend.connect({'path': dir})` returned `False`, `connected == False`, and recorded `"Failed to connect: IsADirectoryError(21, ...)"` (h5py inner text version-dependent; prefix frozen by `catch_error`); the pre-existing directory and its contents byte-unchanged. `save_group` is never called (recorder inventory: zero save calls). |

**Spy discipline (binding):** instrumentation uses the Phase 1
recording-spy technique — plain functions appending `mock.call(...)`
and delegating to the original, patched **without** `autospec=True`
combined with `wraps` (CPython gh-75988: 3.9 autospec does not
delegate to `wraps`; a 3.13-only pass is not evidence — the Phase 1
CI correction record).

### 6.3 Post-connect artifact-state policy [P3P-03 second half, frozen]

The seam **never deletes or truncates** the target artifact on
failure — no rollback, no atomic publication (decision §2
NON-support). Clarification (acceptance Issue 2 scope): this sentence
bounds the seam's own actions and the §6.3 pins only — the design's
byte-preservation guarantees belong to the inspection path's original
copy, never to save atomicity; the unchanged writer's
delete-then-recreate of an existing same-ID group (§7 A6.3) means a
failed save over an existing ID can lose prior group content, and
same-ID overwrites remain unsupported. Because `connect_impl` opens
`'a'` mode and creates the
target at connect (`hdf5.py:39`), any post-connect fault leaves an
artifact behind. The pinned state per row (asserted exactly, never
left unasserted):

| Row | Pinned artifact state |
| --- | --- |
| A4.1 (pre-connect rejection) | **No artifact** at the target path (non-existence asserted; zero HDF5 writes provable by recorder inventory). |
| A4.9 (connect failure via existing directory) | **No HDF5 file created**; the pre-existing directory and its contents are byte-unchanged. |
| A4.2 (save failure via closed handle) | **An empty connect-time file remains**: it opens read-only and contains exactly the connect-time superblock — zero groups, zero datasets, zero record content (probe-verified, §6.2). |
| A4.3 (disconnect failure via external file-handle invalidation) | **Inspection variant (the frozen row — plan A4.3's stated primary outcome):** the original's bytes are sha256-unchanged; the scratch copy is removed normally (only the reader disconnect failed); the primary inspection result is delivered with `cleanup_ok=False` and the frozen disconnect diagnostics in `report.cleanup`. **Save variant (probed supporting evidence, §6.2):** **the complete artifact remains** — save succeeded before the fault; content equals a normal save (group, attributes, and datasets all present). |

Universal truths across every row: the frozen error type is raised
(or, for the cleanup-only row, surfaced in `report.cleanup` — never
logged-and-swallowed, never a `None`-means-absence result, per the
F-C lesson); original/fixture bytes are sha256-verified unchanged;
primary and cleanup diagnostics are independently observable; no
partial state is presented as success and the absence of atomicity is
never hidden (the observable outcome states exactly what is and is
not guaranteed — the artifact pins above).

## 7. Unsupported paths

- **SQLite (A6.1, D2).** `save_run_group` and `inspect_run_group`
  accept only `HDF5DataBackend` instances (or `None`, which
  constructs one). Passing a `Sqlite3DataBackend` — or any other
  `DataBackend` — raises `UnsupportedBackendError(
  "backend: unsupported backend type 'sqlite3' (supported: 'hdf5')")`
  at algorithm step 0, **before any I/O**: a truthful, explicit
  rejection, never a silent no-op and never an empty-meta "success"
  (D2 remains tracked debt; selecting SQLite requires a separately
  reviewed decision).
- **Generic io helpers (A6.2, F-C/F-D).** The supported path never
  imports or calls `io.save_group`, `io.reload_group`, or
  `io.load_groups`; group discovery uses read-only HDF5 keys on the
  disposable copy with explicit identity validation, and empty or
  `None` generic-helper output is never interpreted as an empty
  store. The A6.2 F-C/F-D probes are characterization re-assertions
  on unchanged production code (the defects remain tracked, not
  repaired); the supported path's call inventory shows zero
  generic-helper calls.
- **Same-ID deletion/recreation (A6.3).** Not atomic publication:
  `save_impl` deletes an existing same-id group before recreating it
  (`hdf5.py:73-74`); a probe records the actual non-atomic behavior
  without claiming it as supported publication. The supported save
  targets a **new** run id within the reviewed output-ownership
  scope.
- **Partial/failed-run durability (A6.4).** Unsupported by
  construction: only `acquisition == 'completed'` envelopes can
  encode (Phase 2 §1.5), and the results file records this exclusion
  verbatim.

### 7.1 Stage honesty guarantees (A5)

- The stored outcome is fixed at encode time:
  `snapshot == 'completed_acquisition_before_persistence'`,
  `acquisition == 'completed'`, and
  `persistence`/`plotting`/`cleanup`/`overall` all
  `'not_yet_established'` — asserted from the stored artifact, never
  from the in-memory envelope.
- The write path contains **no code path** that rewrites the outcome
  after save: the outcome lives inside the single `run_record` text
  encoded before connect; there is no separate outcome metadata key,
  and the seam performs no `set_meta` call after the save (assertable
  by recorder inventory).
- Post-persistence observations (save succeeded, disconnect clean)
  are runtime facts in `StorageReport`, recorded by tests as test
  observations — never written back into the record (§8).

## 8. Unavailable-reason policy [P3P-07 freeze]

Storage-level facts the envelope cannot capture structurally —
connect/save/disconnect outcomes, scratch-cleanup outcome,
original-byte verification — **remain outside the record**. The
frozen Phase 2 vocabulary `UNAVAILABLE_REASONS = ('capture_failed',
'source_mismatch')` is **not extended** (extending it is a Phase 2
schema revision requiring its own re-review, and none of these facts
is a capture-time source-identity fact). Rationale: the envelope is
the pre-persistence acquisition snapshot; later-stage states are
`not_yet_established` by contract, and M03 does not update stored
outcomes after the fact (decision §2 NON-support: "later-outcome
updates, outcome audit history"). The runtime truth about
persistence and cleanup is carried by the `StorageReport` diagnostics
container — observable, testable, and never persisted.

## 9. Export surface delta [P3P-06 freeze]

Seam placement: the write/read seam lives in **`softlab/shui/`**
(new module `run_storage.py`); the example layer keeps only client
wiring (§2.4). `softlab/shui/run_storage.py` defines an explicit
`__all__`. `softlab/shui/__init__.py` gains **exactly nine names**
— no wildcard imports, no further drift:

| Package-level name (`softlab.shui`) | Origin | Why package-level |
| --- | --- | --- |
| `save_run_group`, `inspect_run_group` | `run_storage` | the two client-facing entry points |
| `RunStorageError`, `StorageConnectError`, `StorageSaveError`, `StorageInspectionError`, `UnsupportedBackendError` | `run_storage` | the frozen storage error taxonomy |
| `StorageReport`, `InspectionResult` | `run_storage` | the frozen result/diagnostics containers |

Everything else — `StoredRecordFacts`, `THERMAL_HYDRATION_KEYS`,
`SIGNAL_HYDRATION_KEYS`, `PROFILE_HYDRATION_KEYS` — stays on
`softlab.shui.run_storage` only and is imported from the module
directly where needed (the Phase 2 S4.4 convention: tests read
feature constants from the owning module, never re-declare test-local
copies). The existing `softlab.shui` exports (Phase 2 table and
pre-existing names) are unchanged; A7.4 asserts exactly this delta.

## 10. Test freeze table (for sw-mike)

Concrete design facts the approved test plan
([storage-test-plan.md](../test/storage-test-plan.md), commits
`30f48a9`/`b31d687`) **MUST** assert verbatim. Plan placeholders are
frozen as stated; where a plan sentence needs the §3.5 clarification
it is called out explicitly.

| Plan area | Frozen design facts |
| --- | --- |
| A1 — Owned write path | Seam: `build_run_group` (empty group) → attach via public `DataGroup.add_record(record)` (**[P3P-01](a): branch (a) selected**) → `save_run_group(group, path, backend=None)` (**[P3P-01]** module `softlab/shui/run_storage.py`; signature §2.2). Pre-connect rejection of invalid envelopes via `decode_envelope` at step 2 (A4.1 mechanism). Lifecycle asserted via `backend` injection + recorder: connect→save→verified disconnect; return `StorageReport` (**[P3P-03]**, §6.1). A1.2's "stored column order equals envelope `record.columns` order" is asserted per §3.5/§3.5.1: order-sensitive against the in-memory record pre-save; **by-name association** from disk (h5py enumeration is lexicographic, probe-verified on both interpreters) — **this reading changes the approved plan's literal wording and is submitted as an explicit clarification requiring the §3.5.1 follow-up deltas (plan A1.2/A2.5/A3.1, decision §1.1 sample-schema row, decision §2 read-discipline step 3, Phase 2 §1.3), with sw-mike + sw-tom re-review; it is not a unilateral re-interpretation.** Identity facts: group name == `str(group_id)`, on-disk `id` attribute == envelope `group_id`; `execution_id` appears in **no** stored UUID slot. Error types per §2.5. A1.6 stays in Phase 3: integration point §2.4 (with the `RunResult.execution_id` additive field and the `_load_group` additive hydration noted as the F-B client seam). |
| A2 — Safe read path | Entry: `inspect_run_group(path, group_id=None, legacy_keys=(), backend=None)` → `InspectionResult` (§3.2). sha256 before/after asserted by the test; the function additionally self-checks and raises `safety: original bytes modified during inspection` on violation (or `safety: original bytes could not be re-verified after inspection: <exc>` when the re-hash itself cannot complete — both primary failures with fully populated reports, §3.1.1 step 7 / R6). Exactly one `load_group` per copy with `str(group_id)` (recorder inventory); fresh copy per inspection. Hydration **[P3P-01](b)**: `run_record` always (when present) plus requested legacy keys; `THERMAL_HYDRATION_KEYS = ('example', 'configuration', 'timing', 'thermal_manifest')`; `SIGNAL_HYDRATION_KEYS = ()` (the empty set, a frozen literal); `PROFILE_HYDRATION_KEYS` maps both. No thermal keys on signal (A2.8 stays in Phase 3 per §2.3; the signal **example** wiring is Phase 4 per decision §1's conditional clause). |
| A3 — Fresh-process inspection | Bootstrap: `tests/run_record_inspect_bootstrap.py` (**[P3P-04]**; CLI §4.2, exit codes 0/1/2, payload schema §4.3, no-test-state §4.4; stdout discipline and verbatim warning-policy mirroring §4.1 — `redirect_stdout(sys.stderr)` around the inspection call so stdout carries only the JSON payload). Three-way distinction §3.4: absent → `envelope_present: false` + `legacy_meta` raw texts (**[P3P-01](c)**: legacy context = raw text, decoding stays in the example layer); present non-text → `StorageInspectionError('run_record: present value is not text')`; malformed/wrong-namespace/version/over-size → unwrapped Phase 2 `RecordDecodeError`/`RecordValidationError` with the Phase 2 §8.4 templates and an additively attached `.report` (§6.1 R3 — the exit-1 payload's `report` field is always present, §4.3). Multi-group: `group_id: require exactly one stored group or explicit ID` / `group_id: stored group '<id>' not found`; discovery via read-only HDF5 keys on the copy (F-D condition). |
| A4 — Fault matrix | Mechanisms **[P3P-05]** per §6.2 table — each verified at its frozen stage on **both** Python 3.13.15 and Python 3.9.23 during this design (pre-freeze gate satisfied; evidence and environments quoted in §6.2; CI matrix re-confirms on Ubuntu). Error taxonomy **[P3P-02]** per §2.5 table plus the §2.6 frozen cleanup templates. Diagnostics container **[P3P-03]** per §6.1, including population rules R1–R6 (report at entry; primary diagnostics never empty on failure — first entry the escaping exception's message, the coincident safety+body case preserving both as two entries with `__cause__` chaining; codec errors carry `.report` additively; R4 truthful empty-cleanup semantics — pre-resource raise vs cleanup-ran-clean; failure-diagnostics-only cleanup tuple; R6 inspection `finally` never raises, safety outcomes captured and decided at finalize). Artifact-state pins per §6.3 table (A4.1 no artifact; A4.9 no file, directory untouched; A4.2 empty connect-time file with zero groups; A4.3 inspection variant: original sha256-unchanged, scratch removed, primary outcome delivered with `cleanup_ok=False`; save variant: complete artifact remains). A4.3's frozen seam is **external file-handle invalidation** (§6.2) — re-frozen per architecture-review finding 1 after the double-disconnect mechanism proved non-executable under the cleanup guard; the two plan candidates were probed and are vacuous on both interpreters (evidence §6.2). |
| A5 — Stage honesty | Stored literals asserted from disk (Phase 2 §8.1). No post-save `set_meta` on any key anywhere in the write path (recorder inventory). Later-outcome facts are runtime `StorageReport` facts only (**[P3P-07]**, §8) — outside the record; vocabulary unchanged. |
| A6 — Unsupported paths | SQLite rejection: `UnsupportedBackendError("backend: unsupported backend type 'sqlite3' (supported: 'hdf5')")` at step 0 before any I/O (§7). Zero generic-io calls in the supported path (call inventory); F-C/F-D probes are re-assertions on unchanged code. Same-ID deletion probe records non-atomic behavior only. No failed/cancelled-run persistence anywhere. |
| A7 — Legacy compatibility | The legacy suites pass unmodified against the §2.4 wiring: legacy writer failure surface preserved by exact mapping (connect/save `RuntimeError` literals, stage/cause/`attempts` ordering); `_load_group` extension is additive (absent `run_record` ⇒ pre-change path); legacy files read identically before/after (A7.2 via A3.3's legacy absence). Export surface: exactly the §9 nine-name delta (**[P3P-06]**); Phase 2 suite `tests/test_m03_p2_schema.py` untouched. |
| A8 — Hygiene | Temp prefix `softlab-m03-p3-` (tests) / `softlab-m03-p3-read-` (inspection scratch, §3.1). No `autospec=True`+`wraps` (§6.2 spy discipline). Both-interpreter evidence executed during this design (§6.2: 3.13.15 project venv and 3.9.23 scratch conda env, stages identical); the suite is re-checked on both interpreters before handoff and the candidate CI matrix re-confirms. Zero `ResourceWarning` (owned backends tracked and disconnected; the seam's verified disconnect checks raw-handle validity; the A4.3 spy retains the detached handle and the test closes it in teardown). Fresh-process child: stdout redirected to stderr around the inspection call (§4.1), warning policy mirroring all four pyparsing/matplotlib allowances plus the pandas baseline verbatim (applicability probe-verified on 3.9, §4.1). |
| A9 — Results recording | Traceability targets: this design's §2 (A1), §3 (A2), §4 (A3), §6 (A4), §7–§8 (A5/A6), §9 (A7.4), §10 (all); decision §1.1/§2/§4 conditions and PRD M03-AC03/AC04 as the plan maps them. |

## 11. Mermaid UML — dynamic behavior

### 11.1 Save flow (owned write path)

```mermaid
sequenceDiagram
    participant Client as Client (test / thermal execute)
    participant Builders as Phase 2 builders (run_record.py)
    participant Group as build_run_group (run_group.py)
    participant Save as save_run_group (run_storage.py)
    participant Codec as JsonRunRecordCodec
    participant BE as owned HDF5DataBackend
    participant FS as filesystem (target path)

    Client->>Builders: build_*_payload + build_envelope
    Builders-->>Client: validated envelope dict
    Client->>Group: build_run_group(name, envelope)
    Group-->>Client: empty DataGroup {run_record: encoded text}
    Client->>Client: group.add_record(record)  [P3P-01(a): branch (a)]
    Client->>Save: save_run_group(group, path, backend=None)
    Note over Save: entry: report = StorageReport(...) constructed<br/>BEFORE any raise can escape (§6.1 R1)
    Save->>Save: step 0: resolve/validate backend<br/>(non-HDF5 → UnsupportedBackendError with .report, no I/O)
    Save->>Save: step 1: read group.get_meta('run_record')
    Save->>Codec: step 2: decode_envelope(text) — pre-connect
    Codec-->>Save: envelope (or RecordValidationError → A4.1,<br/>.report attached additively per R3,<br/>zero backend contact, no artifact)
    Save->>Save: step 3: identity/attachment/columns/rows cross-checks<br/>(StorageSaveError with .report before connect on mismatch)
    Save->>BE: step 4: connect({'path': path})
    BE->>FS: h5py.File(path, 'a') — creates target at connect
    BE-->>Save: status flags + errors checked<br/>(failure → StorageConnectError; save never called)
    Save->>BE: save_group(group)
    BE->>FS: group + attrs + record datasets written
    BE-->>Save: status flags + errors checked<br/>(failure → StorageSaveError; artifact pinned per §6.3)
    Note over Save,BE: finally — verified disconnect (always):<br/>status flags, errors delta, raw-handle validity;<br/>frozen §2.6 templates → report.cleanup, never raises
    alt primary success
        Save-->>Client: StorageReport(primary_ok=True, cleanup slots independent)
    else primary failure
        Save-->>Client: raises with .report (primary=(str(exc),) per R2)
    end
```

### 11.2 Inspect flow (safe read path)

```mermaid
sequenceDiagram
    participant Caller as Caller (test / fresh-process CLI)
    participant Insp as inspect_run_group (run_storage.py)
    participant FS as filesystem (original)
    participant Tmp as disposable copy (softlab-m03-p3-read-*)
    participant H5 as h5py.File(copy, 'r')
    participant BE as owned HDF5DataBackend
    participant Codec as JsonRunRecordCodec

    Caller->>Insp: inspect_run_group(path, group_id=None,<br/>legacy_keys=(), backend=None)
    Note over Insp: entry: report constructed BEFORE any raise (R1);<br/>step 0 backend resolution (UnsupportedBackendError, R4(a))
    Insp->>FS: sha256(original) — baseline hash<br/>(unreadable → 'read: original artifact unreadable: ...', R4(a))
    Note over Insp,Tmp: try/finally begins HERE — every operation from<br/>scratch creation onward is inside the cleanup/re-hash finally
    Insp->>Tmp: TemporaryDirectory + shutil.copy2(original, copy)<br/>(scratch obligation exists from creation; copy failure still<br/>passes through the finally)
    Insp->>H5: preflight: enumerate groups; resolve group_id<br/>(exactly-one rule); reserved attrs present+text;<br/>disk id == group_id — BEFORE any backend load
    H5-->>Insp: identity + captured run_record / legacy texts<br/>+ per-record facts (columns, row counts)
    Note over Insp,H5: preflight failure → StorageInspectionError,<br/>zero legacy loads (A4.8) — finally still runs (R4(b))
    Insp->>BE: connect({'path': copy})
    Insp->>BE: load_group(str(group_id)) — EXACTLY ONE load per copy
    BE-->>Insp: DataGroup (meta empty — R4-BASE-META-01)
    Insp->>Insp: explicit hydration (F-B): set_meta run_record<br/>+ requested legacy keys only
    alt run_record text present
        Insp->>Codec: decode_envelope(text)
        Codec-->>Insp: envelope (codec errors propagate unwrapped with<br/>additively attached .report, R3 — A4.4–A4.7)
        Insp->>Insp: cross-check identity, by-name columns/units,<br/>row counts vs stored facts (mismatch → cross-check error)
    else absent (legacy file)
        Insp->>Insp: envelope = None — truthful legacy absence,<br/>legacy_meta raw texts still returned, never an error
    end
    Note over Insp,Tmp: finally (never raises, R6) — verified reader disconnect<br/>+ scratch cleanup (independent diagnostics, frozen order);<br/>guarded sha256 re-check CAPTURED (mismatch or re-hash<br/>failure → safety diagnostic, not a finally raise)
    Note over Insp: finalize (step 7): safety failure dominates —<br/>raises with chained body exception per R2; else body exception<br/>escapes with finalized report; else success
    Insp-->>Caller: InspectionResult(group, envelope, legacy_meta,<br/>record_facts, original_sha256, report)
```

## Third-Party Dependencies

| Module | Version | Purpose | Justification |
| --- | --- | --- | --- |
| — | — | — | **None new.** `h5py` (3.16.0 observed) is an existing project dependency, used directly only for the decision-§2-mandated read-only preflight on the disposable copy; all writes go through the existing `HDF5DataBackend`. Everything else is standard library (`dataclasses`, `hashlib`, `shutil`, `tempfile`, `typing`). Decision §6 item 9 forbids new dependencies. |

## Implementation Notes (for sw-tom)

- **Order of implementation:** exceptions + containers +
  `__all__` skeleton → `save_run_group` (entry report construction,
  then steps 0–5 in exactly the frozen order, §2.2) →
  `inspect_run_group` (entry report construction → baseline hash →
  the single `try` from `TemporaryDirectory` creation through
  preflight → single load → hydration → decode/cross-check, with
  body-exception capture; the `finally` performing verified
  disconnect → scratch removal → guarded re-hash capture and never
  raising; then the §3.1.1 step-7 finalize deciding safety → body →
  success, §6.1 R6) → hydration constants →
  `softlab/shui/__init__.py` nine-name delta → example
  wiring (§2.4: `RunResult.execution_id`, `after_count` retention,
  additive attach, writer-block replacement with exact legacy
  mapping, `_load_group` additive hydration) →
  `tests/run_record_inspect_bootstrap.py` (with §4.1 stdout
  redirection and the verbatim mirrored warning policy).
- **Never** use `DataGroup(meta=...)` (D1) or `set_meta(key, None)`;
  never import from `examples/` in library code (the
  `THERMAL_HYDRATION_KEYS` literals are re-declared, mirroring Phase
  2's `THERMAL_MODEL` discipline); never import
  `softlab.shui.data.io`.
- **Do not repair the backends.** F-A (destructive plain load), F-B
  (custom-key loss), F-C/F-D (io helper breakage), D1/D2 stay tracked
  debt. The design routes around every one of them; any "small fix"
  is out of scope and blocks review.
- **`DataGroup.record(name)` returns a copy** (`base.py:573`) — the
  save-side cross-checks read declarations from the copy, which is
  correct and sufficient; do not reach into `group._records`.
- **Column order trap (§3.5):** h5py enumerates datasets
  lexicographically; never compare `StoredRecordFacts.columns` or a
  reloaded record's column order directly against the envelope order.
  The envelope order is checked against the in-memory record pre-save
  and by-name on disk.
- **Error mapping discipline:** wrap only what §2.2/§3.1.1 specify;
  the Phase 2 codec errors must propagate with unchanged type and
  message (plan A3.3). Attach `.report` to every escaping exception:
  `RunStorageError` subclasses by construction, codec instances
  additively at the boundary (§6.1 R3) — no exception leaves either
  entry point report-less.
- **Spy discipline in tests:** plain recording functions delegating
  to originals, never `autospec=True` + `wraps` (3.9 gh-75988).
- **Python 3.9 compatibility:** `typing.Dict/List/Optional/Tuple`
  style (Black target `py39`); no 3.10+ syntax. Fault-seam stages are
  already verified on both interpreters (§6.2); the CI matrix
  re-confirms.
- **A4.3 instrumentation:** the spy wraps the real `load_group`
  (inspection row) or `save_group` (save variant), delegates, then
  detaches and retains the raw handle before clearing
  `backend.file`; the test closes the retained handle in teardown and
  asserts the close (zero leaked handles).
- **Docstrings** in English, typed signatures, repo style
  (AGENTS.md).

## Design Review

- **Reviewer**: sw-jerry
- **Review Date**: [to be filled after review]
- **Status**: PENDING
