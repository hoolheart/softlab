"""A synthetic paired-signal count client using the bounded execution seam.

The source is intentionally non-consumptive: each channel is read once per
cycle, and each read returns the current tick plus the channel's fixed
offset (signal 5, reference 2), consuming no queued value.
This example owns its configuration, its derived difference policy, and its
owned run-record persistence: a completed-acquisition snapshot envelope
written through the shared Phase 2/3 seams to one explicit HDF5 path.
"""
from dataclasses import dataclass
import hashlib
from pathlib import Path
import platform
import subprocess
import uuid

import softlab

from softlab.huo.process import CountResult, count, run_count
from softlab.shui.data import DataGroup, DataRecord
from softlab.shui.run_group import build_run_group
from softlab.shui.run_record import (
    PROFILE_SIGNAL,
    SIGNAL_OFFSETS,
    build_envelope,
    build_signal_payload,
)
from softlab.shui.run_storage import (
    StorageConnectError,
    StorageReport,
    StorageSaveError,
    save_run_group,
)
from softlab.tu.station import Parameter

_RECORD_NAME = 'signal'
_SOURCE_REFERENCE = 'examples/signal_monitor.py'


def acquire(points=3):
    """Return terminal evidence, record, differences, and channel read trace."""
    state = {'tick': 0, 'reads': []}
    group = DataGroup('signal_monitor')
    record = DataRecord('signal', [
        {'name': 'timestamp', 'unit': 's', 'dependent': False},
        {'name': 'signal', 'unit': 'V', 'dependent': True},
        {'name': 'reference', 'unit': 'V', 'dependent': True},
    ])
    group.add_record(record)

    def advance():
        state['tick'] += 1

    def read(channel, offset):
        state['reads'].append((channel, state['tick']))
        return state['tick'] + offset

    getters = [
        Parameter('signal', settable=False,
                  before_get=lambda _: read('signal', 5)),
        Parameter('reference', settable=False,
                  before_get=lambda _: read('reference', 2)),
    ]
    process = count('signal', group=group, record=record, getters=getters,
                    times=points, hook_before_get=advance)
    result = run_count(process, points, lambda: len(record),
                       lambda: state['tick'])
    differences = (record.table['signal'] - record.table['reference']).tolist()
    return result, record, differences, list(state['reads'])


@dataclass(frozen=True)
class SavedRun:
    """Owned persistence evidence of one supported signal run record."""

    execution_id: str
    group_id: str
    path: Path
    report: StorageReport


_SOURCE_TRACKED_FAILURE = 'Source is not the tracked reference file'


def _capture_source_identity() -> dict:
    """Capture this file's exact bytes and best-effort repository identity."""
    source = Path(__file__).resolve()
    identity = {'reference': _SOURCE_REFERENCE,
                'digest_algorithm': 'sha256',
                'digest_scope': 'exact reference script bytes at production',
                'digest': None, 'digest_unavailable_reason': None}
    try:
        identity['digest'] = hashlib.sha256(source.read_bytes()).hexdigest()
    except OSError as exc:
        identity['digest_unavailable_reason'] = str(exc) or type(exc).__name__
    unavailable = {'git_revision': None, 'git_dirty': None}
    try:
        root = Path(_git(source, 'rev-parse', '--show-toplevel')).resolve()
        relative = source.relative_to(root)
        tracked = _git(source, 'ls-files', '--full-name', '--error-unmatch',
                       '--', source.name)
        if tracked != relative.as_posix():
            raise ValueError(_SOURCE_TRACKED_FAILURE)
        identity['git_revision'] = _git(source, 'rev-parse', '--verify', 'HEAD')
        identity['git_dirty'] = bool(_git(
            source, 'status', '--porcelain', '--untracked-files=normal'))
        identity['git_unavailable_reason'] = None
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        identity.update(unavailable)
        identity['git_unavailable_reason'] = str(exc) or type(exc).__name__
    return identity


def _git(source: Path, *arguments: str) -> str:
    """Run one bounded repository query beside the reference file."""
    completed = subprocess.run(
        ['git', *arguments], cwd=str(source.parent),
        check=True, capture_output=True, text=True, timeout=5)
    return completed.stdout.strip()


def _source_facts() -> dict:
    """Map captured identity onto the availability-fact shape of the record.

    Free-text reasons are never stored: unavailable facts carry only the
    bounded codes ``'source_mismatch'`` or ``'capture_failed'``, and
    ``git_dirty.state`` always equals ``git_revision.state``.
    """
    captured = _capture_source_identity()
    facts = {'reference': captured['reference'],
             'digest_algorithm': captured['digest_algorithm'],
             'digest_scope': captured['digest_scope']}
    for key in ('digest', 'git_revision', 'git_dirty'):
        value = captured[key]
        if value is not None:
            facts[key] = {'state': 'available', 'value': value,
                          'reason': None}
        else:
            reason_key = ('digest_unavailable_reason' if key == 'digest'
                          else 'git_unavailable_reason')
            reason = captured[reason_key]
            facts[key] = {
                'state': 'unavailable', 'value': None,
                'reason': ('source_mismatch'
                           if reason == _SOURCE_TRACKED_FAILURE
                           else 'capture_failed')}
    return facts


def persist(result: CountResult, record: DataRecord, path) -> SavedRun:
    """Persist one acquired record as a supported run record at ``path``.

    Gate first: a failed, cancelled, or incomplete acquisition (anything
    short of ``result.overall_success``) raises before any envelope is
    constructed or any save is attempted. The envelope declares the
    example-owned frozen signal payload, the bounded execution identity
    ``str(result.run_id)``, and one fresh distinct persistent ``group_id``.
    The record is attached to the group built by :func:`build_run_group`
    through the public ``DataGroup.add_record`` before
    :func:`save_run_group`; that seam owns the HDF5 backend lifecycle and
    its verified disconnect. HDF5 is the only supported backend.
    """
    if not isinstance(result, CountResult):
        raise TypeError('result must be a CountResult')
    if not isinstance(record, DataRecord):
        raise TypeError('record must be a DataRecord')
    if not result.overall_success:
        raise RuntimeError(
            f'Refusing to persist an unsuccessful acquisition: '
            f'acquisition={result.acquisition}; '
            f'completed_rows={result.completed_rows}; '
            f'requested_rows={result.requested_rows}')
    if record.name != _RECORD_NAME:
        raise ValueError(f'record name must be {_RECORD_NAME!r}')
    if result.completed_rows != result.requested_rows:
        raise RuntimeError('Acquisition row count differs from the request')
    execution_id = str(result.run_id)
    group_id = str(uuid.uuid4())
    payload = build_signal_payload(points=result.requested_rows,
                                   offsets=SIGNAL_OFFSETS)
    envelope = build_envelope(
        profile=PROFILE_SIGNAL,
        execution_id=execution_id,
        group_id=group_id,
        record_name=_RECORD_NAME,
        columns=list(record.columns),
        requested_rows=result.requested_rows,
        completed_rows=result.completed_rows,
        software={'softlab_version': softlab.__version__,
                  'python_version': platform.python_version()},
        source=_source_facts(),
        payload=payload)
    group = build_run_group(group_id, envelope)
    group.add_record(record)
    target = Path(path).absolute()
    try:
        report = save_run_group(group, str(target))
    except StorageConnectError as exc:
        raise RuntimeError(f'HDF5 writer connect failed: {exc}') from exc
    except StorageSaveError as exc:
        raise RuntimeError(f'HDF5 save failed: {exc}') from exc
    if not (report.primary_ok and report.cleanup_ok):
        raise RuntimeError('; '.join(report.primary + report.cleanup))
    return SavedRun(execution_id=execution_id, group_id=group_id,
                    path=target, report=report)
