"""
Owned run-record storage and safe inspection seam.

This module delivers the Phase 3 storage half of the additive run
record: one **owned save path** (:func:`save_run_group`) and one **safe
read/inspection path** (:func:`inspect_run_group`). Both entry points
own their ``HDF5DataBackend`` lifecycle (connect, status checks,
verified disconnect) and carry separately observable primary and cleanup
diagnostics in a frozen :class:`StorageReport` on every code path —
including every escaping exception (``.report``).

Safety disciplines (frozen by design):

- All legacy backend reads happen on a disposable sha256-verified byte
  copy of the original; the original file is never opened by a legacy
  loader, and at most one legacy load runs per copy.
- The additive ``run_record`` text and requested legacy keys are
  captured by a read-only ``h5py`` preflight on the copy and explicitly
  re-hydrated after the single legacy load (the characterized custom-key
  loss of a plain load is routed around, not repaired).
- The original's bytes are re-hashed after inspection; a mismatch or a
  re-hash failure is a primary failure, never a silent pass.
- The save seam performs no rollback and no atomic publication, and it
  never deletes or truncates the target itself; however, the
  characterized HDF5 writer deletes an existing same-ID group before
  recreating it, so a failed save over an existing same-ID group can
  leave the previous group content lost (delete-then-recreate). Write
  to a fresh target path when preserving prior content matters. The
  byte-preservation guarantees above concern only the original
  artifact during inspection, never save atomicity.

The module performs no backend repairs: the tracked backend defects
(destructive plain load, custom-key loss, io-helper breakage, SQLite
empty meta, ``DataGroup(meta=...)`` construction) are designed around,
never fixed. It never constructs a ``DataGroup`` and never writes
metadata; the save seam only reads ``group.meta`` and hands the group
to the characterized HDF5 writer. Codec errors
(:class:`~softlab.shui.run_record.RecordDecodeError` /
:class:`~softlab.shui.run_record.RecordValidationError`) propagate
unwrapped with unchanged type and message; the seam attaches the
assembled report to the propagating exception instance additively.
"""

import dataclasses
import hashlib
import os
import shutil
import tempfile
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    cast,
)

import h5py

from softlab.shui.data import (
    DataBackend,
    DataGroup,
    HDF5DataBackend,
)
from softlab.shui.run_record import (
    METADATA_KEY,
    PROFILE_SIGNAL,
    PROFILE_THERMAL,
    decode_envelope,
)

__all__ = [
    'save_run_group',
    'inspect_run_group',
    'RunStorageError',
    'StorageConnectError',
    'StorageSaveError',
    'StorageInspectionError',
    'UnsupportedBackendError',
    'StorageReport',
    'InspectionResult',
    'StoredRecordFacts',
    'THERMAL_HYDRATION_KEYS',
    'SIGNAL_HYDRATION_KEYS',
    'PROFILE_HYDRATION_KEYS',
]

_SCRATCH_PREFIX = 'softlab-m03-p3-read-'
"""Frozen scratch-directory prefix of the inspection path."""

_RESERVED_ATTRS = ('id', 'name', 'timestamp', 'backend')
"""Reserved group attributes verified by the h5py preflight."""

_COLUMN_ATTRS = ('name', 'label', 'unit', 'dependent')
"""Per-column dataset attributes captured as stored record facts."""

# --- Frozen per-profile hydration key sets (design §3.3) ---

THERMAL_HYDRATION_KEYS = ('example', 'configuration', 'timing',
                          'thermal_manifest')
"""Legacy text keys the thermal client requests for hydration."""

SIGNAL_HYDRATION_KEYS: Tuple[str, ...] = ()
"""The signal profile's legacy hydration set — itself a frozen literal."""

PROFILE_HYDRATION_KEYS = {
    PROFILE_THERMAL: THERMAL_HYDRATION_KEYS,
    PROFILE_SIGNAL: SIGNAL_HYDRATION_KEYS,
}
"""Frozen mapping from client profile to its legacy hydration key set."""


# --- Diagnostics containers (design §6.1) ---


@dataclasses.dataclass(frozen=True)
class StorageReport:
    """Separately observable primary and cleanup diagnostics.

    ``primary`` and ``cleanup`` are independent named fields, each
    always present (possibly empty) regardless of the sibling's state.
    ``cleanup`` holds frozen-template failure diagnostics only, in the
    frozen append order; ``cleanup_ok == (len(cleanup) == 0)``. On
    success — including a cleanup-only failure — the entry point
    returns this report; on primary failure the escaping exception
    carries it in ``.report`` with ``primary_ok=False`` and the first
    entry of ``primary`` being the escaping exception's own message.
    """

    operation: str
    """``'save'`` or ``'inspect'``."""
    path: str
    """Target artifact path (the original, never the scratch copy)."""
    primary_ok: bool
    """True iff the primary stage completed."""
    primary: Tuple[str, ...]
    """Primary-stage diagnostic messages."""
    cleanup_ok: bool
    """True iff cleanup completed cleanly."""
    cleanup: Tuple[str, ...]
    """Cleanup-stage diagnostic messages."""


@dataclasses.dataclass(frozen=True)
class StoredRecordFacts:
    """h5py-level on-disk record facts, independent of ``DataGroup``.

    ``columns`` is in h5py enumeration order (lexicographic by dataset
    name — a characterized property of the unchanged legacy writer);
    it must never be equated with the envelope's declared order.
    ``row_count`` is the column datasets' ``shape[0]``; all column
    datasets of one record must agree.
    """

    name: str
    """Record name (the HDF5 subgroup name)."""
    columns: Tuple[Dict[str, Any], ...]
    """Per-dataset name/label/unit/dependent attributes, h5py order."""
    row_count: int
    """Number of stored rows (dataset ``shape[0]``)."""


@dataclasses.dataclass(frozen=True)
class InspectionResult:
    """Result of one safe inspection of a stored run-record artifact."""

    group: DataGroup
    """Hydrated group from the single legacy load on the copy."""
    envelope: Optional[Dict[str, Any]]
    """Decoded envelope; ``None`` iff genuine legacy absence."""
    legacy_meta: Dict[str, str]
    """Raw legacy text attributes, requested keys only."""
    record_facts: Dict[str, StoredRecordFacts]
    """On-disk record facts keyed by record name."""
    original_sha256: str
    """Post-inspection hash of the original (equals the pre-hash)."""
    report: StorageReport
    """Primary/cleanup diagnostics of this inspection."""


# --- Storage error taxonomy (design §2.5) ---


class RunStorageError(Exception):
    """Base class of run-storage failures; never raised directly.

    Every raised instance carries the assembled :class:`StorageReport`
    in ``.report``.
    """

    report: StorageReport


class StorageConnectError(RunStorageError, ValueError):
    """Connect-stage failure, or save attempted without a clean connect."""


class StorageSaveError(RunStorageError, ValueError):
    """Missing additive key, pre-connect envelope/group inconsistency."""


class StorageInspectionError(RunStorageError, ValueError):
    """Any failure of the safe read/inspection path."""


class UnsupportedBackendError(RunStorageError, ValueError):
    """A non-HDF5 backend passed to either entry point, before any I/O."""


# --- Internal helpers ---


def _entry_report(operation: str, path: Any) -> StorageReport:
    """Build the report-at-entry contract state (design §6.1 rule R1)."""
    return StorageReport(operation=operation, path=str(path),
                         primary_ok=False, primary=(),
                         cleanup_ok=True, cleanup=())


def _raise_with_report(exc: BaseException,
                       report: StorageReport) -> None:
    """Populate ``report.primary`` per R2, attach, and raise (never returns)."""
    exc.report = dataclasses.replace(report, primary=(str(exc),))
    raise exc


def _resolve_backend(backend: Optional[DataBackend],
                     report: StorageReport) -> HDF5DataBackend:
    """Resolve step 0: default to HDF5, reject any other type pre-I/O."""
    if backend is None:
        return HDF5DataBackend()
    if not isinstance(backend, HDF5DataBackend):
        _raise_with_report(UnsupportedBackendError(
            'backend: unsupported backend type '
            f"'{backend.backend_type}' (supported: 'hdf5')"), report)
    return backend


def _verified_disconnect(backend: HDF5DataBackend,
                         cleanup: List[str]) -> None:
    """Verified disconnect whose diagnostics mirror the legacy example.

    The frozen message templates and append order are verbatim the
    legacy ``_disconnect`` of the thermal example: escaping disconnect
    exception text, then the ``backend.errors`` delta verbatim, then
    ``'Backend still connected'``, then the open-handle flag with the
    best-effort raw ``close()`` and its ``'Raw handle close failed: '``
    template. Never raises; appends failure diagnostics only.
    """
    handle = backend.file
    old_errors = len(backend.errors)
    try:
        backend.disconnect()
    except Exception as exc:
        cleanup.append(str(exc))
    for entry in backend.errors[old_errors:]:
        cleanup.append(entry)
    if backend.connected:
        cleanup.append('Backend still connected')
    if handle is not None and handle.id.valid:
        cleanup.append('Backend disconnect left its file handle open')
        try:
            handle.close()
        except Exception as exc:
            cleanup.append(f'Raw handle close failed: {exc}')


def _sha256_file(path: Any) -> str:
    """Stream the file at ``path`` through sha256 and return hex text."""
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(65536), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _preflight(copy_path: str, group_id: Optional[str],
               legacy_keys: Sequence[str]
               ) -> Tuple[str, Optional[str], Dict[str, str],
                          Dict[str, StoredRecordFacts]]:
    """Read-only h5py preflight on the disposable copy (design §3.1 step 2).

    Runs entirely before any backend connect. Resolves the stored group
    (exactly-one rule when ``group_id`` is ``None``), verifies the
    reserved attributes are present and text and that the disk ``id``
    equals the requested group id, then captures the ``run_record``
    text (``None`` when genuinely absent — truthful legacy absence),
    the requested legacy texts (absent keys are not errors; present
    non-text values are), and the per-record on-disk facts.

    Raises :class:`StorageInspectionError` with the frozen preflight
    templates on any failure; performs zero legacy loads.
    """
    with h5py.File(copy_path, 'r') as disk:
        identities = list(disk.keys())
        if group_id is None:
            if len(identities) != 1:
                raise StorageInspectionError(
                    'group_id: require exactly one stored group '
                    'or explicit ID')
            group_id = identities[0]
        if group_id not in identities:
            raise StorageInspectionError(
                f"group_id: stored group '{group_id}' not found")
        stored = disk[group_id]
        for key in _RESERVED_ATTRS:
            if key not in stored.attrs or type(stored.attrs[key]) is not str:
                raise StorageInspectionError(
                    "preflight: missing or nontext reserved attribute "
                    f"'{key}'")
        if stored.attrs['id'] != group_id:
            raise StorageInspectionError(
                'preflight: group identity mismatch: disk id '
                f"'{stored.attrs['id']}' differs from requested "
                f"'{group_id}'")
        run_record_text: Optional[str] = None
        if METADATA_KEY in stored.attrs:
            value = stored.attrs[METADATA_KEY]
            if type(value) is not str:
                raise StorageInspectionError(
                    'run_record: present value is not text')
            run_record_text = value
        legacy_meta: Dict[str, str] = {}
        for key in legacy_keys:
            if key in stored.attrs:
                value = stored.attrs[key]
                if type(value) is not str:
                    raise StorageInspectionError(
                        f'{key}: present value is not text')
                legacy_meta[key] = value
        record_facts: Dict[str, StoredRecordFacts] = {}
        for name in stored.keys():
            record_group = stored[name]
            columns: List[Dict[str, Any]] = []
            row_count: Optional[int] = None
            column_group = record_group['columns']
            for column_name in column_group.keys():
                dataset = column_group[column_name]
                columns.append({attr: dataset.attrs[attr]
                                for attr in _COLUMN_ATTRS})
                if row_count is None:
                    row_count = int(dataset.shape[0])
            record_facts[name] = StoredRecordFacts(
                name=name, columns=tuple(columns),
                row_count=row_count if row_count is not None else 0)
    return group_id, run_record_text, legacy_meta, record_facts


def _cross_check(envelope: Dict[str, Any], group_id: str,
                 record_facts: Dict[str, StoredRecordFacts]) -> None:
    """Cross-check a decoded envelope against the stored artifact.

    Identity is the envelope ``group_id`` against the stored group
    identity; columns are associated **by name** (each declared column,
    in envelope order, must match its same-named dataset's four
    attributes exactly and the name sets must be equal — physical HDF5
    enumeration is lexicographic and non-semantic); the stored row
    count must equal ``outcome.completed_rows``.
    """
    if envelope['group_id'] != group_id:
        raise StorageInspectionError(
            'cross-check: envelope group_id '
            f"'{envelope['group_id']}' differs from stored group "
            f"identity '{group_id}'")
    record = envelope['record']
    facts = record_facts.get(record['name'])
    if facts is None:
        raise StorageInspectionError(
            'cross-check: stored columns differ from envelope declaration')
    stored_by_name = {column['name']: column for column in facts.columns}
    if set(stored_by_name) != {column['name']
                               for column in record['columns']}:
        raise StorageInspectionError(
            'cross-check: stored columns differ from envelope declaration')
    for column in record['columns']:
        stored = stored_by_name.get(column['name'])
        if stored is None or stored != column:
            raise StorageInspectionError(
                'cross-check: stored columns differ from '
                'envelope declaration')
    completed_rows = envelope['outcome']['completed_rows']
    if facts.row_count != completed_rows:
        raise StorageInspectionError(
            f'cross-check: stored row count {facts.row_count} differs '
            f'from outcome.completed_rows {completed_rows}')


# --- Entry point 1: owned save path (design §2.2) ---


def save_run_group(group: DataGroup, path: str, *,
                   backend: Optional[DataBackend] = None
                   ) -> StorageReport:
    """Persist one run-record group through an owned HDF5 backend.

    The group must carry the encoded additive record under
    ``METADATA_KEY`` (attached by ``build_run_group`` or by an
    equivalent encode-then-``set_meta`` client path) and exactly the
    record attachment the envelope declares. Persistence is HDF5-only:
    any other backend type is rejected before any I/O
    (:class:`UnsupportedBackendError`). The function owns the backend
    lifecycle: it connects, saves, and disconnects with the
    verified-disconnect discipline mirrored from the thermal example
    (status flags, error-list delta, raw-handle validity). On success
    it returns the :class:`StorageReport`; on primary failure it raises
    the frozen :class:`RunStorageError` subclass carrying the report in
    ``.report``. The report is constructed at entry, before any raise
    path can escape, so every exception that leaves this function
    carries ``.report`` — subclasses by construction, unwrapped codec
    errors by additive instance attachment at the boundary (type and
    message unchanged). A cleanup-only failure never raises: it is
    reported in ``report.cleanup`` with ``cleanup_ok == False``. The
    seam performs no rollback and no atomic publication and never
    deletes or truncates the target itself; however, the characterized
    HDF5 writer deletes an existing same-ID group before recreating it,
    so a failed save over an existing same-ID group can leave the
    previous group content lost (delete-then-recreate).

    Steps, in the frozen order: backend resolution; additive-key read;
    pre-connect codec decode (zero HDF5 writes can have occurred on
    rejection); pre-connect envelope/group consistency cross-checks
    (identity, attachment, order-sensitive columns against the
    in-memory record, row count); then connect/save with status checks
    and the always-run verified disconnect. The outcome lives inside
    the single ``run_record`` text encoded before connect; the seam
    contains no code path that touches metadata after the save.
    """
    report = _entry_report('save', path)
    # Step 0 — backend resolution before anything else.
    writer = _resolve_backend(backend, report)
    # Step 1 — additive key must be present text.
    text = group.get_meta(METADATA_KEY)
    if type(text) is not str:
        _raise_with_report(StorageSaveError(
            'run_record: additive key missing from group metadata'), report)
    # Step 2 — pre-connect codec decode; codec errors propagate
    # unwrapped with the assembled report attached additively.
    try:
        envelope = decode_envelope(text)
    except Exception as exc:
        exc.report = dataclasses.replace(report, primary=(str(exc),))
        raise
    # Step 3 — pre-connect consistency cross-checks.
    if str(group.id) != envelope['group_id']:
        _raise_with_report(StorageSaveError(
            'group_id: group identity '
            f"'{str(group.id)}' differs from envelope group_id "
            f"'{envelope['group_id']}'"), report)
    record_name = envelope['record']['name']
    if record_name not in group.records:
        _raise_with_report(StorageSaveError(
            f'record: missing declared record '
            f"'{record_name}'"), report)
    attached = group.record(record_name)
    if attached.columns != envelope['record']['columns']:
        _raise_with_report(StorageSaveError(
            'record.columns: differ from envelope declaration'), report)
    if len(attached) != envelope['outcome']['completed_rows']:
        _raise_with_report(StorageSaveError(
            f'record.rows: stored row count {len(attached)} differs '
            f"from outcome.completed_rows "
            f"{envelope['outcome']['completed_rows']}"), report)
    # Step 4 — connect/save with status checks; the finally cleanup
    # never raises and merges frozen-template diagnostics.
    cleanup: List[str] = []
    primary_exc: Optional[BaseException] = None
    try:
        rst = writer.connect({'path': path})
        if not rst or not writer.connected or writer.errors:
            raise StorageConnectError(
                'connect: HDF5 backend connect failed: '
                f'{writer.last_error}')
        rst = writer.save_group(group)
        if not rst or not writer.just_succeeded or writer.errors:
            raise StorageSaveError(
                f'save: HDF5 backend save failed: {writer.last_error}')
    except BaseException as exc:
        # BaseException is captured too so EVERY exception that leaves
        # this function carries the populated report (§2.2 contract),
        # including interruptions: the finally cleanup below always
        # runs, and the finalize step attaches the report before the
        # interruption continues to propagate.
        primary_exc = exc
    finally:
        if writer.file is not None or writer.connected:
            _verified_disconnect(writer, cleanup)
    # Step 5 — finalize and escape (design §6.1 population contract).
    if primary_exc is not None:
        _raise_with_report(primary_exc, dataclasses.replace(
            report, cleanup_ok=not cleanup, cleanup=tuple(cleanup)))
    return dataclasses.replace(report, primary_ok=True,
                               cleanup_ok=not cleanup,
                               cleanup=tuple(cleanup))


# --- Entry point 2: safe read/inspection path (design §3) ---


def inspect_run_group(path: str, group_id: Optional[str] = None, *,
                      legacy_keys: Sequence[str] = (),
                      backend: Optional[DataBackend] = None
                      ) -> InspectionResult:
    """Safely inspect a stored run-record artifact via a disposable copy.

    ``path`` is the original artifact; it is never opened by a legacy
    backend loader. The four-step discipline is structural: (1) record
    the sha256 of the original and ``shutil.copy2`` it into owned
    scratch space (prefix ``softlab-m03-p3-read-``); (2) run a read-only
    ``h5py`` preflight on the copy — group resolution (exactly-one rule
    when ``group_id`` is ``None``), reserved-attribute verification,
    capture of the ``run_record`` text and the requested ``legacy_keys``
    texts, and per-record on-disk facts — before any backend connect;
    (3) connect the owned backend on the copy and perform exactly one
    legacy ``load_group(str(group_id))``, then explicitly re-hydrate
    the captured texts (a plain legacy load drops all custom metadata
    keys; hydration is the designed-around seam, not a backend repair);
    (4) verified reader disconnect and scratch cleanup with diagnostics
    independent of the primary result, then a guarded re-hash of the
    original — a byte mismatch or a re-hash failure is a primary
    failure (``safety: ...``), never a silent pass.

    A present-text ``run_record`` is decoded with the Phase 2 codec
    (codec errors propagate unwrapped with the report attached
    additively) and cross-checked against the stored artifact by-name
    (identity, columns/units, row count). A genuinely absent
    ``run_record`` is truthful legacy absence: ``envelope is None``,
    ``legacy_meta`` carries the raw requested texts, and no error is
    raised. A present non-text value is the visible failure
    ``'run_record: present value is not text'``.

    Every escaping exception carries the fully populated report in
    ``.report``; every operation from scratch creation onward passes
    through the cleanup/re-hash ``finally`` before any exception
    escapes, so owed cleanup is always executed and truthfully
    reported.
    """
    report = _entry_report('inspect', path)
    # Step 0 — backend resolution (pre-resource, R4(a)).
    reader = _resolve_backend(backend, report)
    # Step 1 — baseline hash of the original (pre-resource, R4(a)).
    try:
        baseline = _sha256_file(path)
    except Exception as exc:
        _raise_with_report(StorageInspectionError(
            f'read: original artifact unreadable: {exc}'), report)
    # Steps 2–6 — everything from scratch creation onward executes
    # inside the cleanup/re-hash try/finally (frozen ordering, R6).
    body_exc: Optional[Exception] = None
    safety_diag: Optional[str] = None
    scratch: Optional[tempfile.TemporaryDirectory] = None
    reader_connect_attempted = False
    cleanup: List[str] = []
    post: Optional[str] = None
    loaded: Optional[DataGroup] = None
    envelope: Optional[Dict[str, Any]] = None
    legacy_meta: Dict[str, str] = {}
    record_facts: Dict[str, StoredRecordFacts] = {}
    resolved_id = ''
    try:
        scratch = tempfile.TemporaryDirectory(prefix=_SCRATCH_PREFIX)
        copy_path = os.path.join(scratch.name, os.path.basename(str(path)))
        shutil.copy2(path, copy_path)
        resolved_id, run_record_text, legacy_meta, record_facts = \
            _preflight(copy_path, group_id, legacy_keys)
        reader_connect_attempted = True
        rst = reader.connect({'path': copy_path})
        if not rst or not reader.connected or reader.errors:
            raise StorageConnectError(
                'connect: HDF5 backend connect failed: '
                f'{reader.last_error}')
        candidate = reader.load_group(str(resolved_id))
        if (not isinstance(candidate, DataGroup)
                or not reader.just_succeeded or reader.errors):
            raise StorageInspectionError(
                'load: HDF5 backend load failed: '
                f'{reader.last_error}')
        loaded = candidate
        # Explicit hydration of the captured texts (F-B seam).
        if run_record_text is not None:
            loaded.set_meta(METADATA_KEY, run_record_text)
        for key, value in legacy_meta.items():
            loaded.set_meta(key, value)
        if run_record_text is not None:
            # Codec errors propagate unwrapped; the report is attached
            # additively at the finalize step after the finally cleanup.
            envelope = decode_envelope(run_record_text)
            _cross_check(envelope, resolved_id, record_facts)
    except Exception as exc:
        body_exc = exc
    finally:
        if reader_connect_attempted and (reader.file is not None
                                         or reader.connected):
            _verified_disconnect(reader, cleanup)
        if scratch is not None:
            try:
                scratch.cleanup()
            except Exception as exc:
                cleanup.append(
                    f'Scratch cleanup at {scratch.name}: {exc}')
        try:
            post = _sha256_file(path)
        except Exception as exc:
            safety_diag = (
                'safety: original bytes could not be re-verified '
                f'after inspection: {exc}')
        else:
            if post != baseline:
                safety_diag = (
                    'safety: original bytes modified during inspection')
    # Step 7 — deterministic finalize; every escape carries the fully
    # populated report (R1/R2/R5/R6).
    if safety_diag is not None:
        if body_exc is not None:
            primary_entries = (safety_diag, str(body_exc))
        else:
            primary_entries = (safety_diag,)
        exc = StorageInspectionError(safety_diag)
        exc.report = dataclasses.replace(
            report, primary_ok=False, primary=primary_entries,
            cleanup_ok=not cleanup, cleanup=tuple(cleanup))
        if body_exc is not None:
            raise exc from body_exc
        raise exc
    if body_exc is not None:
        body_exc.report = dataclasses.replace(
            report, primary_ok=False, primary=(str(body_exc),),
            cleanup_ok=not cleanup, cleanup=tuple(cleanup))
        raise body_exc
    return InspectionResult(
        group=cast(DataGroup, loaded),
        envelope=envelope,
        legacy_meta=legacy_meta,
        record_facts=record_facts,
        original_sha256=cast(str, post),
        report=dataclasses.replace(report, primary_ok=True,
                                   cleanup_ok=not cleanup,
                                   cleanup=tuple(cleanup)))
