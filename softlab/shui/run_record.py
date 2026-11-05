"""
Bounded run-record schema, strict JSON codec, and pure builders.

This module defines the additive common run record (a closed JSON-text
envelope with two closed client payloads, ``thermal`` and ``signal``),
the strict standard-library codec implementing :class:`RunRecordCodec`,
and the pure dictionary builders of the client construction seam.

The module is pure and side-effect free: it accepts plain ``dict`` and
``list`` values, validates them against the closed contract, and
produces or consumes strict JSON text. It never mutates caller input
(validated output is freshly built), never performs I/O, and imports
only the standard library, so it stays decoupled from the data
backends and from :class:`softlab.shui.data.base.DataGroup`.

All contract constants (namespace, metadata key, format and payload
versions, profile identifiers, outcome and availability literals, the
frozen model/offsets declarations, and the numeric limits) live in
this module; the numeric limits are intentionally not re-exported at
the ``softlab.shui`` package level.
"""

import abc
import json
import math
import re
import uuid
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
)

__all__ = [
    'NAMESPACE',
    'METADATA_KEY',
    'FORMAT_VERSION',
    'THERMAL_PAYLOAD_VERSION',
    'SIGNAL_PAYLOAD_VERSION',
    'PROFILE_THERMAL',
    'PROFILE_SIGNAL',
    'OUTCOME_SNAPSHOT',
    'ACQUISITION_COMPLETED',
    'STAGE_NOT_YET_ESTABLISHED',
    'STATE_AVAILABLE',
    'STATE_UNAVAILABLE',
    'UNAVAILABLE_REASONS',
    'THERMAL_MODEL',
    'SIGNAL_OFFSETS',
    'MAX_ENCODED_BYTES',
    'MAX_DEPTH',
    'MAX_ARRAY_LENGTH',
    'MAX_OBJECT_ENTRIES',
    'MAX_STRING_LENGTH',
    'MAX_UNAVAILABLE_REASON_LENGTH',
    'MAX_INTEGER_DIGITS',
    'RunRecordError',
    'RecordDecodeError',
    'RecordValidationError',
    'RunRecordCodec',
    'JsonRunRecordCodec',
    'build_thermal_payload',
    'build_signal_payload',
    'build_envelope',
    'encode_envelope',
    'decode_envelope',
]

# --- Frozen namespace, key, version, and profile identifiers (§1.1) ---

NAMESPACE = 'softlab.run-record'
"""Additive namespace of the common run record."""

METADATA_KEY = 'run_record'
"""Single shared metadata key carrying the encoded record text."""

FORMAT_VERSION = 1
"""Common envelope format version (exact integer, closed contract)."""

PROFILE_THERMAL = 'thermal'
"""Recognized thermal client-profile identifier."""

PROFILE_SIGNAL = 'signal'
"""Recognized signal-monitor client-profile identifier."""

THERMAL_PAYLOAD_VERSION = 1
"""Thermal payload version, independent of every other version."""

SIGNAL_PAYLOAD_VERSION = 1
"""Signal payload version."""

# --- Frozen outcome and availability literals (§1.4, §1.5) ---

OUTCOME_SNAPSHOT = 'completed_acquisition_before_persistence'
"""Only supported stored outcome: completed acquisition before persistence."""

ACQUISITION_COMPLETED = 'completed'
"""Only supported acquisition state."""

STAGE_NOT_YET_ESTABLISHED = 'not_yet_established'
"""Frozen state of persistence, plotting, cleanup, and overall stages."""

STATE_AVAILABLE = 'available'
"""Availability-fact state literal for a captured fact."""

STATE_UNAVAILABLE = 'unavailable'
"""Availability-fact state literal for an uncaptured fact."""

UNAVAILABLE_REASONS = ('capture_failed', 'source_mismatch')
"""Bounded vocabulary of availability-fact unavailable reasons."""

# --- Frozen profile declarations (§1.6, §1.7) ---

THERMAL_MODEL = {
    'name': 'lumped_thermal_body', 'method': 'explicit_euler',
    'equation': 'T_next = T + dt * (P - G * (T - T_ambient)) / C',
    'validity': 'educational; constant parameters; 0 < dt*G/C <= 1; '
                'no physical calibration',
    'units': {'initial_temperature': 'degC', 'ambient_temperature': 'degC',
              'heat_capacity': 'J/K', 'conductance': 'W/K', 'dt': 's',
              'power': 'W', 'points': '1'},
}
"""Frozen thermal model declaration; payloads must match it exactly."""

SIGNAL_OFFSETS = {'signal': 5, 'reference': 2}
"""Frozen signal read-policy offsets; payloads must match them exactly."""

# --- Frozen numeric limits (§2.3) ---

MAX_ENCODED_BYTES = 131072
"""Encoded text length bound (encode: ASCII bytes; decode: UTF-8 bytes)."""

MAX_DEPTH = 8
"""Container nesting bound; the root object is depth 1."""

MAX_ARRAY_LENGTH = 64
"""Element-count bound of any array."""

MAX_OBJECT_ENTRIES = 64
"""Key-count bound of any object."""

MAX_STRING_LENGTH = 1024
"""Length bound of any string value, in code points."""

MAX_UNAVAILABLE_REASON_LENGTH = 64
"""Per-field bound of availability-fact reason strings."""

MAX_INTEGER_DIGITS = 1024
"""Decimal-digit bound of any exact-integer value or token."""

_MAX_INTEGER_ABS = 10 ** MAX_INTEGER_DIGITS
"""Private encode-side magnitude bound, computed once at import."""

# --- Closed schema key sets and frozen profile tables ---

_ENVELOPE_KEYS = ('namespace', 'version', 'profile', 'execution_id',
                  'group_id', 'record', 'software', 'source', 'outcome',
                  'payload')

_RECORD_KEYS = ('name', 'columns')
_COLUMN_KEYS = ('name', 'label', 'unit', 'dependent')
_SOFTWARE_KEYS = ('softlab_version', 'python_version')
_SOURCE_KEYS = ('reference', 'digest_algorithm', 'digest_scope', 'digest',
                'git_revision', 'git_dirty')
_FACT_KEYS = ('state', 'value', 'reason')
_OUTCOME_KEYS = ('snapshot', 'acquisition', 'requested_rows',
                 'completed_rows', 'persistence', 'plotting', 'cleanup',
                 'overall')
_STAGE_KEYS = ('persistence', 'plotting', 'cleanup', 'overall')

_THERMAL_CONFIG_REALS = ('initial_temperature', 'ambient_temperature',
                         'heat_capacity', 'conductance', 'dt', 'power')
_THERMAL_CONFIG_KEYS = _THERMAL_CONFIG_REALS + ('points',)
_THERMAL_PAYLOAD_KEYS = ('version', 'adapter', 'configuration', 'model',
                         'procedure', 'timing', 'boundary')
_THERMAL_PROCEDURE_KEYS = ('name', 'advancement', 'command',
                           'requested_points')
_THERMAL_TIMING_KEYS = ('dt', 'simulation_unit', 'acquisition',
                        'measurement_delay_ticks', 'source_tick',
                        'wall_timestamp')
_THERMAL_BOUNDARY_KEYS = ('final_tick', 'final_sim_time')

_SIGNAL_PAYLOAD_KEYS = ('version', 'points', 'read_policy', 'timing',
                        'scheduler', 'derived')
_SIGNAL_READ_POLICY_KEYS = ('semantics', 'consumptive', 'offsets')

_THERMAL_COLUMNS: Tuple[Dict[str, Any], ...] = (
    {'name': 'commanded_power_w', 'label': 'Commanded heater power',
     'unit': 'W', 'dependent': False},
    {'name': 'measurement_source_tick',
     'label': 'Measurement source boundary', 'unit': '1', 'dependent': False},
    {'name': 'sim_time_s', 'label': 'Simulation acquisition time',
     'unit': 's', 'dependent': False},
    {'name': 'temperature_degc', 'label': 'Delivered temperature',
     'unit': 'degC', 'dependent': True},
    {'name': 'tick', 'label': 'Acquisition boundary', 'unit': '1',
     'dependent': False},
    {'name': 'timestamp', 'label': 'Wall-clock acquisition timestamp',
     'unit': 's', 'dependent': False},
)
"""Frozen ordered thermal column schema."""

_SIGNAL_COLUMNS: Tuple[Dict[str, Any], ...] = (
    {'name': 'timestamp', 'label': '', 'unit': 's', 'dependent': False},
    {'name': 'signal', 'label': '', 'unit': 'V', 'dependent': True},
    {'name': 'reference', 'label': '', 'unit': 'V', 'dependent': True},
)
"""Frozen ordered signal column schema."""

_PROFILE_RECORD_NAMES = {
    PROFILE_THERMAL: 'thermal_trace',
    PROFILE_SIGNAL: 'signal',
}
_PROFILE_COLUMNS = {
    PROFILE_THERMAL: _THERMAL_COLUMNS,
    PROFILE_SIGNAL: _SIGNAL_COLUMNS,
}
_THERMAL_TIMING = {
    'coordinated': (1, 'tick-1'),
    'direct': (0, 'tick'),
}
_THERMAL_PROCEDURE_LITERALS = (
    ('name', 'thermal_count'),
    ('advancement', 'once_before_each_getter_row'),
    ('command', 'constant_power'),
)

_DIGEST_PATTERN = re.compile(r'[0-9a-f]{64}')
_REVISION_PATTERN = re.compile(r'[0-9a-f]{40}|[0-9a-f]{64}')


# --- Error taxonomy (§3) ---


class RunRecordError(Exception):
    """Base class of run-record failures; never raised directly."""


class RecordDecodeError(RunRecordError, ValueError):
    """Text/parse-stage failure of ``RunRecordCodec.decode``.

    Raised for malformed JSON text, a wrong root type, duplicate object
    keys at any nesting depth, nonfinite ``NaN``/``Infinity`` tokens,
    overflow literals such as ``1e999`` caught by the recursive
    post-parse finiteness sweep, integer tokens over
    ``MAX_INTEGER_DIGITS``, parser recursion exhaustion, input text
    not encodable as UTF-8, and decode-side size/depth/count/length
    limit violations.
    """


class RecordValidationError(RunRecordError, ValueError):
    """Contract-stage failure of ``RunRecordCodec.encode`` and ``decode``.

    Raised for missing or unknown fields, strict type errors, present
    null without an assigned meaning, unsupported namespace, version,
    or profile, identity violations, nonfinite or unsupported values,
    over-limit integers, finite-real conversion overflow, frozen
    model/offsets drift, availability binding violations, and
    encode-side limit violations.
    """


# --- Message and low-level validation helpers ---


def _at(path: str, detail: str) -> str:
    """Render a ``'<path>: <detail>'`` message, bare when path is empty."""
    return f'{path}: {detail}' if path else detail


def _join(path: str, key: Any) -> str:
    """Join a dotted field path with one more key or index."""
    return f'{path}.{key}' if path else str(key)


def _checked_object(value: Any, path: str,
                    keys: Tuple[str, ...]) -> Dict[str, Any]:
    """Require a closed object: no null, exact dict, no missing/unknown keys.

    ``path`` is the dotted path of the object itself; missing-field and
    unknown-field messages name the object as their path.
    """
    if value is None:
        raise RecordValidationError(
            f'{path}: explicit null has no assigned meaning')
    if type(value) is not dict:
        raise RecordValidationError(f'{path}: expected object')
    for key in keys:
        if key not in value:
            raise RecordValidationError(
                _at(path, f"missing required field '{key}'"))
    for key in sorted(value):
        if key not in keys:
            raise RecordValidationError(
                _at(path, f"unknown field '{key}'"))
    return value


def _text(value: Any, path: str, *, nonempty: bool = True) -> str:
    """Require an exact string, optionally nonempty."""
    if value is None:
        raise RecordValidationError(
            f'{path}: explicit null has no assigned meaning')
    if type(value) is not str:
        raise RecordValidationError(f'{path}: expected text')
    if nonempty and value == '':
        raise RecordValidationError(f'{path}: expected nonempty text')
    return value


def _integer(value: Any, path: str, *, positive: bool = True) -> int:
    """Require an exact integer (never a bool), optionally positive."""
    if value is None:
        raise RecordValidationError(
            f'{path}: explicit null has no assigned meaning')
    if type(value) is not int:
        raise RecordValidationError(f'{path}: expected exact integer')
    if positive and value <= 0:
        raise RecordValidationError(
            f'{path}: expected positive exact integer')
    return value


def _real(value: Any, path: str) -> float:
    """Normalize a finite real (int or float, never bool) to ``float``.

    The ``float()`` conversion runs under the legacy overflow guard so
    a huge integer can never leak a builtin ``OverflowError``.
    """
    if value is None:
        raise RecordValidationError(
            f'{path}: explicit null has no assigned meaning')
    if type(value) not in (int, float):
        raise RecordValidationError(f'{path}: expected finite real')
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise RecordValidationError(
            f'{path}: expected finite real') from exc
    if not math.isfinite(number):
        raise RecordValidationError(f'{path}: expected finite real')
    return number


def _boolean(value: Any, path: str) -> bool:
    """Require an exact boolean (never ``0``/``1``)."""
    if value is None:
        raise RecordValidationError(
            f'{path}: explicit null has no assigned meaning')
    if type(value) is not bool:
        raise RecordValidationError(f'{path}: expected boolean')
    return value


def _literal(value: Any, path: str, expected: Any) -> Any:
    """Require exact equality with one frozen literal."""
    if value != expected:
        raise RecordValidationError(
            f'{path}: unsupported literal (supported: {expected})')
    return value


def _version_field(value: Any, path: str, expected: int) -> int:
    """Require the exact frozen version integer.

    Any other value — higher, lower, zero, float, text, or bool — is
    rejected with the frozen unsupported-version message.
    """
    if value is None:
        raise RecordValidationError(
            f'{path}: explicit null has no assigned meaning')
    if type(value) is not int or value != expected:
        raise RecordValidationError(
            f'{path}: unsupported version (supported: {expected})')
    return value


def _canonical_uuid(value: Any, path: str) -> str:
    """Require canonical UUID text (``str(uuid.UUID(v)) == v``)."""
    if value is None:
        raise RecordValidationError(
            f'{path}: explicit null has no assigned meaning')
    if type(value) is not str:
        raise RecordValidationError(f'{path}: expected text')
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise RecordValidationError(
            f'{path}: expected canonical UUID') from exc
    if str(parsed) != value:
        raise RecordValidationError(f'{path}: expected canonical UUID')
    return value


def _available_value_valid(value: Any, kind: str) -> bool:
    """Check the structural shape of one available availability value."""
    if kind == 'digest':
        return (type(value) is str
                and _DIGEST_PATTERN.fullmatch(value) is not None)
    if kind == 'git_revision':
        return (type(value) is str
                and _REVISION_PATTERN.fullmatch(value) is not None)
    return type(value) is bool


def _availability(value: Any, path: str, kind: str) -> Dict[str, Any]:
    """Validate one availability fact (§1.4).

    ``kind`` selects the structural value check: ``digest`` (64
    lowercase hex), ``git_revision`` (40- or 64-lowercase hex), or
    ``git_dirty`` (boolean). Explicit ``null`` has an assigned meaning
    only here: ``value`` iff state is unavailable, ``reason`` iff
    state is available.
    """
    fact = _checked_object(value, path, _FACT_KEYS)
    state = fact['state']
    if state is None:
        raise RecordValidationError(
            f'{path}.state: explicit null has no assigned meaning')
    if type(state) is not str:
        raise RecordValidationError(f'{path}.state: expected text')
    reason = fact['reason']
    if state == STATE_AVAILABLE:
        if not _available_value_valid(fact['value'], kind):
            raise RecordValidationError(f'{path}: invalid available value')
        if reason is not None:
            raise RecordValidationError(
                f'{path}: available state requires value and null reason')
        return {'state': state, 'value': fact['value'], 'reason': None}
    if state == STATE_UNAVAILABLE:
        if fact['value'] is not None:
            raise RecordValidationError(
                f'{path}: unavailable state requires null value '
                'and bounded reason')
        if reason is None:
            raise RecordValidationError(
                f'{path}: unavailable state requires null value '
                'and bounded reason')
        if type(reason) is not str:
            raise RecordValidationError(f'{path}.reason: expected text')
        if len(reason) > MAX_UNAVAILABLE_REASON_LENGTH:
            raise RecordValidationError(
                f'{path}: reason length exceeds limit '
                f'{MAX_UNAVAILABLE_REASON_LENGTH}')
        if reason not in UNAVAILABLE_REASONS:
            raise RecordValidationError(
                f'{path}: reason outside bounded vocabulary')
        return {'state': state, 'value': None, 'reason': reason}
    raise RecordValidationError(f'{path}: invalid available value')


def _validate_columns(value: Any, path: str,
                      profile: str) -> List[Dict[str, Any]]:
    """Validate the ordered frozen column schema of one profile (§1.3)."""
    if value is None:
        raise RecordValidationError(
            f'{path}: explicit null has no assigned meaning')
    if type(value) is not list:
        raise RecordValidationError(f'{path}: expected array')
    definitions = _PROFILE_COLUMNS[profile]
    if len(value) != len(definitions):
        raise RecordValidationError(
            f'{path}: expected ordered {profile} column schema')
    normalized: List[Dict[str, Any]] = []
    for index, definition in enumerate(definitions):
        column_path = f'{path}.{index}'
        column = _checked_object(value[index], column_path, _COLUMN_KEYS)
        entry = {
            'name': _text(column['name'], f'{column_path}.name'),
            'label': _text(column['label'], f'{column_path}.label',
                           nonempty=False),
            'unit': _text(column['unit'], f'{column_path}.unit'),
            'dependent': _boolean(column['dependent'],
                                  f'{column_path}.dependent'),
        }
        if entry != definition:
            raise RecordValidationError(
                f'{path}: expected ordered {profile} column schema')
        normalized.append(entry)
    return normalized


def _thermal_configuration(value: Any, path: str) -> Dict[str, Any]:
    """Validate the closed thermal configuration and physical rules (§1.6)."""
    configuration = _checked_object(value, path, _THERMAL_CONFIG_KEYS)
    normalized = {
        key: _real(configuration[key], _join(path, key))
        for key in _THERMAL_CONFIG_REALS
    }
    normalized['points'] = _integer(configuration['points'],
                                    _join(path, 'points'))
    if (normalized['heat_capacity'] <= 0 or normalized['conductance'] <= 0
            or normalized['dt'] <= 0):
        raise RecordValidationError(
            f'{path}: expected positive heat_capacity, conductance, dt')
    ratio = (normalized['dt'] * normalized['conductance']
             / normalized['heat_capacity'])
    if not math.isfinite(ratio) or not 0 < ratio <= 1:
        raise RecordValidationError(
            f'{path}: expected 0 < dt*conductance/heat_capacity <= 1')
    return normalized


def _validate_thermal_payload(value: Any,
                              requested_rows: Optional[int]) -> Dict[str, Any]:
    """Validate the closed thermal payload (§1.6).

    ``requested_rows`` is the envelope-level requested row count; when
    ``None`` (builder mode), ``configuration.points`` is used as the
    reference count for the internal cross-checks instead.
    """
    payload = _checked_object(value, 'payload', _THERMAL_PAYLOAD_KEYS)
    _version_field(payload['version'], 'payload.version',
                   THERMAL_PAYLOAD_VERSION)
    adapter = _text(payload['adapter'], 'payload.adapter')
    if adapter not in _THERMAL_TIMING:
        raise RecordValidationError(
            'payload.adapter: unsupported clock adapter '
            '(supported: coordinated, direct)')
    configuration = _thermal_configuration(
        payload['configuration'], 'payload.configuration')
    model = payload['model']
    if model is None:
        raise RecordValidationError(
            'payload.model: explicit null has no assigned meaning')
    if type(model) is not dict:
        raise RecordValidationError('payload.model: expected object')
    if model != THERMAL_MODEL:
        raise RecordValidationError(
            'payload.model: unsupported model semantics or units')
    procedure = _checked_object(payload['procedure'], 'payload.procedure',
                                _THERMAL_PROCEDURE_KEYS)
    for key, expected in _THERMAL_PROCEDURE_LITERALS:
        _literal(_text(procedure[key], f'payload.procedure.{key}'),
                 f'payload.procedure.{key}', expected)
    procedure_points = _integer(procedure['requested_points'],
                                'payload.procedure.requested_points')
    timing = _checked_object(payload['timing'], 'payload.timing',
                             _THERMAL_TIMING_KEYS)
    timing_dt = _real(timing['dt'], 'payload.timing.dt')
    if timing_dt != configuration['dt']:
        raise RecordValidationError(
            'payload.timing.dt: inconsistent with configuration.dt')
    _literal(_text(timing['simulation_unit'], 'payload.timing.simulation_unit'),
             'payload.timing.simulation_unit', 's')
    _literal(_text(timing['acquisition'], 'payload.timing.acquisition'),
             'payload.timing.acquisition', 'post_step')
    _literal(_text(timing['wall_timestamp'], 'payload.timing.wall_timestamp'),
             'payload.timing.wall_timestamp', 'unix_seconds')
    delay = timing['measurement_delay_ticks']
    if delay is None:
        raise RecordValidationError(
            'payload.timing.measurement_delay_ticks: explicit null has '
            'no assigned meaning')
    if type(delay) is not int:
        raise RecordValidationError(
            'payload.timing.measurement_delay_ticks: expected exact integer')
    source_tick = _text(timing['source_tick'], 'payload.timing.source_tick')
    expected_delay, expected_tick = _THERMAL_TIMING[adapter]
    if delay != expected_delay or source_tick != expected_tick:
        raise RecordValidationError(
            'payload.timing.source_tick: inconsistent with '
            'measurement_delay_ticks')
    boundary = _checked_object(payload['boundary'], 'payload.boundary',
                               _THERMAL_BOUNDARY_KEYS)
    final_tick = _integer(boundary['final_tick'], 'payload.boundary.final_tick')
    final_sim_time = _real(boundary['final_sim_time'],
                           'payload.boundary.final_sim_time')
    reference = (requested_rows if requested_rows is not None
                 else configuration['points'])
    if configuration['points'] != reference:
        raise RecordValidationError(
            'configuration.points: inconsistent with outcome.requested_rows')
    if procedure_points != reference:
        raise RecordValidationError(
            'procedure.requested_points: inconsistent with '
            'outcome.requested_rows')
    if final_tick != reference:
        raise RecordValidationError(
            'boundary.final_tick: inconsistent with outcome.requested_rows')
    try:
        expected_time = float(configuration['points']) * configuration['dt']
    except OverflowError as exc:
        raise RecordValidationError(
            'payload.boundary.final_sim_time: inconsistent with '
            'configuration.points * configuration.dt') from exc
    if abs(final_sim_time - expected_time) > 1e-10:
        raise RecordValidationError(
            'payload.boundary.final_sim_time: inconsistent with '
            'configuration.points * configuration.dt')
    return {
        'version': THERMAL_PAYLOAD_VERSION,
        'adapter': adapter,
        'configuration': configuration,
        'model': model,
        'procedure': {
            'name': procedure['name'],
            'advancement': procedure['advancement'],
            'command': procedure['command'],
            'requested_points': procedure_points,
        },
        'timing': {
            'dt': timing_dt,
            'simulation_unit': timing['simulation_unit'],
            'acquisition': timing['acquisition'],
            'measurement_delay_ticks': delay,
            'source_tick': source_tick,
            'wall_timestamp': timing['wall_timestamp'],
        },
        'boundary': {
            'final_tick': final_tick,
            'final_sim_time': final_sim_time,
        },
    }


def _validate_signal_payload(value: Any,
                             requested_rows: Optional[int]) -> Dict[str, Any]:
    """Validate the closed signal payload (§1.7).

    ``requested_rows`` is the envelope-level requested row count; when
    ``None`` (builder mode), the payload's own ``points`` is the
    reference count.
    """
    payload = _checked_object(value, 'payload', _SIGNAL_PAYLOAD_KEYS)
    _version_field(payload['version'], 'payload.version',
                   SIGNAL_PAYLOAD_VERSION)
    points = _integer(payload['points'], 'payload.points')
    reference = requested_rows if requested_rows is not None else points
    if points != reference:
        raise RecordValidationError(
            'payload.points: inconsistent with outcome.requested_rows')
    read_policy = _checked_object(payload['read_policy'],
                                  'payload.read_policy',
                                  _SIGNAL_READ_POLICY_KEYS)
    _literal(_text(read_policy['semantics'],
                   'payload.read_policy.semantics'),
             'payload.read_policy.semantics', 'tick_plus_offset')
    consumptive = _boolean(read_policy['consumptive'],
                           'payload.read_policy.consumptive')
    if consumptive is not False:
        raise RecordValidationError(
            'payload.read_policy.consumptive: unsupported literal '
            '(supported: False)')
    offsets = read_policy['offsets']
    if offsets is None:
        raise RecordValidationError(
            'payload.read_policy.offsets: explicit null has no '
            'assigned meaning')
    if type(offsets) is not dict:
        raise RecordValidationError(
            'payload.read_policy.offsets: expected object')
    if offsets != SIGNAL_OFFSETS:
        raise RecordValidationError(
            'payload.read_policy.offsets: unsupported read offsets '
            "(supported: {'signal': 5, 'reference': 2})")
    timing = _checked_object(payload['timing'], 'payload.timing',
                             ('timestamp',))
    _literal(_text(timing['timestamp'], 'payload.timing.timestamp'),
             'payload.timing.timestamp', 'wall_clock')
    scheduler = _checked_object(payload['scheduler'], 'payload.scheduler',
                                ('ownership',))
    _literal(_text(scheduler['ownership'], 'payload.scheduler.ownership'),
             'payload.scheduler.ownership', 'run_count_owned')
    derived = _checked_object(payload['derived'], 'payload.derived',
                              ('difference',))
    difference = _checked_object(derived['difference'],
                                 'payload.derived.difference',
                                 ('formula', 'storage'))
    _literal(_text(difference['formula'],
                   'payload.derived.difference.formula'),
             'payload.derived.difference.formula', 'signal - reference')
    _literal(_text(difference['storage'],
                   'payload.derived.difference.storage'),
             'payload.derived.difference.storage',
             'caller_derived_not_stored')
    return {
        'version': SIGNAL_PAYLOAD_VERSION,
        'points': points,
        'read_policy': {
            'semantics': read_policy['semantics'],
            'consumptive': consumptive,
            'offsets': offsets,
        },
        'timing': {'timestamp': timing['timestamp']},
        'scheduler': {'ownership': scheduler['ownership']},
        'derived': {
            'difference': {
                'formula': difference['formula'],
                'storage': difference['storage'],
            },
        },
    }


_PROFILE_VALIDATORS = {
    PROFILE_THERMAL: _validate_thermal_payload,
    PROFILE_SIGNAL: _validate_signal_payload,
}


def _sweep_value(value: Any, path: str, depth: int,
                 error: Type[Exception], build: bool) -> Any:
    """Shared recursive sweep over containers and scalars (§7.2).

    Depth is container-only: the sweep recurses into containers with a
    incremented depth and never depth-checks scalars. With ``build``
    the sweep returns a freshly built normalized copy; otherwise it
    validates in place and returns ``None``. ``error`` selects the
    raised exception class: ``RecordValidationError`` on encode,
    ``RecordDecodeError`` on decode.
    """
    if type(value) is dict:
        if depth > MAX_DEPTH:
            raise error(
                f'run_record: nesting depth exceeds limit {MAX_DEPTH}')
        if len(value) > MAX_OBJECT_ENTRIES:
            raise error(_at(
                path, f'object entry count exceeds limit '
                      f'{MAX_OBJECT_ENTRIES}'))
        result: Dict[str, Any] = {}
        for key, item in value.items():
            if type(key) is not str:
                raise error(_at(
                    path, f'unsupported value type '
                          f'{type(key).__name__!r}'))
            child = _sweep_value(item, _join(path, key), depth + 1,
                                 error, build)
            if build:
                result[key] = child
        return result if build else None
    if type(value) is list:
        if depth > MAX_DEPTH:
            raise error(
                f'run_record: nesting depth exceeds limit {MAX_DEPTH}')
        if len(value) > MAX_ARRAY_LENGTH:
            raise error(_at(
                path, f'array length exceeds limit {MAX_ARRAY_LENGTH}'))
        items: List[Any] = []
        for index, item in enumerate(value):
            child = _sweep_value(item, _join(path, index), depth + 1,
                                 error, build)
            if build:
                items.append(child)
        return items if build else None
    if value is None or type(value) is bool:
        return value if build else None
    if type(value) is int:
        if value >= _MAX_INTEGER_ABS or value <= -_MAX_INTEGER_ABS:
            raise error(_at(
                path, f'integer digit count exceeds limit '
                      f'{MAX_INTEGER_DIGITS}'))
        return value if build else None
    if type(value) is float:
        if not math.isfinite(value):
            raise error(_at(path, 'nonfinite number'))
        return value if build else None
    if type(value) is str:
        if len(value) > MAX_STRING_LENGTH:
            raise error(_at(
                path, f'string length exceeds limit {MAX_STRING_LENGTH}'))
        return value if build else None
    raise error(_at(path,
                    f'unsupported value type {type(value).__name__!r}'))


def _validate_and_normalize(envelope: Dict[str, Any]) -> Dict[str, Any]:
    """Run the encode-side sweep and the closed contract validation."""
    swept = JsonRunRecordCodec._sweep_encode(envelope, '', 1)
    return JsonRunRecordCodec._validate_envelope(swept)


# --- Codec interface (§2) ---


class RunRecordCodec(abc.ABC):
    """Encode/decode contract for the additive run record (DIP seam)."""

    @abc.abstractmethod
    def encode(self, envelope: Dict[str, Any]) -> str:
        """Validate and encode an envelope dict to JSON text.

        Raises ``TypeError`` unless the argument is exactly a dict,
        ``RecordValidationError`` for any contract violation. Never
        mutates the input; performs no I/O.
        """

    @abc.abstractmethod
    def decode(self, text: str) -> Dict[str, Any]:
        """Decode and validate JSON text to a normalized envelope dict.

        Raises ``TypeError`` for non-str input, ``RecordDecodeError``
        for any text/parse-stage failure, ``RecordValidationError``
        for any contract violation. Never performs I/O.
        """


class JsonRunRecordCodec(RunRecordCodec):
    """Stateless strict JSON implementation of :class:`RunRecordCodec`.

    Safe to share across callers: the codec keeps no state, never
    mutates its inputs, and performs no I/O. Serialization is
    deterministic (``sort_keys``, fixed separators, ASCII-only
    escaping) and both paths share one validation code path, so
    ``decode(encode(x))`` is deep-equal to the validated ``x``.
    """

    @staticmethod
    def _bounded_int(token: str) -> int:
        """Decode-side integer bound: digit-count the sign-stripped token.

        The check runs on the token string before any conversion, so
        it is interpreter-independent (never trips the 3.13 integer
        conversion guard) and accepts exactly the encode-side set.
        """
        digits = token[1:] if token.startswith('-') else token
        if len(digits) > MAX_INTEGER_DIGITS:
            raise RecordDecodeError(
                f'run_record: integer digit count exceeds limit '
                f'{MAX_INTEGER_DIGITS}')
        return int(token)

    @staticmethod
    def _sweep_encode(value: Any, path: str, depth: int) -> Any:
        """Encode-side sweep: bounded allowlist check plus normalization."""
        return _sweep_value(value, path, depth, RecordValidationError, True)

    @staticmethod
    def _sweep_decode(value: Any, path: str, depth: int) -> None:
        """Decode-side sweep: bounded allowlist check over parsed text."""
        _sweep_value(value, path, depth, RecordDecodeError, False)

    @staticmethod
    def _parse_strict(text: str) -> Any:
        """Parse strict JSON, rejecting duplicates and nonfinite tokens.

        Hook rejections (duplicate keys, nonfinite constants,
        over-limit integer tokens) propagate untouched; malformed text
        and parser recursion exhaustion map to ``RecordDecodeError``.
        """
        def pairs(items: List[Tuple[str, Any]]) -> Dict[str, Any]:
            result: Dict[str, Any] = {}
            for key, value in items:
                if key in result:
                    raise RecordDecodeError(
                        f"run_record: duplicate object key '{key}'")
                result[key] = value
            return result

        def reject(token: str) -> None:
            raise RecordDecodeError(
                f"run_record: nonfinite JSON token '{token}'")

        try:
            return json.loads(text, object_pairs_hook=pairs,
                              parse_constant=reject,
                              parse_int=JsonRunRecordCodec._bounded_int)
        except RecordDecodeError:
            raise
        except json.JSONDecodeError as exc:
            raise RecordDecodeError(
                f'run_record: malformed JSON: {exc}') from exc
        except RecursionError as exc:
            raise RecordDecodeError(
                'run_record: nesting exceeds parser recursion limit') from exc

    @staticmethod
    def _validate_envelope(value: Dict[str, Any]) -> Dict[str, Any]:
        """Validate the closed common envelope and dispatch the profile.

        Assumes the value already passed the recursive sweep. Returns
        a freshly built normalized envelope; finite-real fields are
        normalized to ``float``.
        """
        root = _checked_object(value, '', _ENVELOPE_KEYS)
        namespace = _text(root['namespace'], 'namespace')
        if namespace != NAMESPACE:
            raise RecordValidationError(
                f'namespace: unsupported namespace (supported: {NAMESPACE})')
        _version_field(root['version'], 'version', FORMAT_VERSION)
        profile = _text(root['profile'], 'profile')
        if profile not in _PROFILE_VALIDATORS:
            raise RecordValidationError('profile: unsupported client profile')
        execution_id = _canonical_uuid(root['execution_id'], 'execution_id')
        group_id = _canonical_uuid(root['group_id'], 'group_id')
        if execution_id == group_id:
            raise RecordValidationError(
                'execution_id: must differ from group_id')
        record = _checked_object(root['record'], 'record', _RECORD_KEYS)
        record_name = _text(record['name'], 'record.name')
        expected_name = _PROFILE_RECORD_NAMES[profile]
        if record_name != expected_name:
            raise RecordValidationError(
                f'record.name: unsupported record name '
                f'(supported: {expected_name})')
        columns = _validate_columns(record['columns'], 'record.columns',
                                    profile)
        software = _checked_object(root['software'], 'software',
                                   _SOFTWARE_KEYS)
        softlab_version = _text(software['softlab_version'],
                                'software.softlab_version')
        python_version = _text(software['python_version'],
                               'software.python_version')
        source = _checked_object(root['source'], 'source', _SOURCE_KEYS)
        reference = _text(source['reference'], 'source.reference')
        digest_algorithm = _text(source['digest_algorithm'],
                                 'source.digest_algorithm')
        _literal(digest_algorithm, 'source.digest_algorithm', 'sha256')
        digest_scope = _text(source['digest_scope'], 'source.digest_scope')
        digest = _availability(source['digest'], 'source.digest', 'digest')
        git_revision = _availability(source['git_revision'],
                                     'source.git_revision', 'git_revision')
        git_dirty = _availability(source['git_dirty'], 'source.git_dirty',
                                  'git_dirty')
        if git_dirty['state'] != git_revision['state']:
            raise RecordValidationError(
                'source.git_dirty.state: differs from '
                'source.git_revision.state')
        outcome = _checked_object(root['outcome'], 'outcome', _OUTCOME_KEYS)
        snapshot = _literal(_text(outcome['snapshot'], 'outcome.snapshot'),
                            'outcome.snapshot', OUTCOME_SNAPSHOT)
        acquisition = _literal(
            _text(outcome['acquisition'], 'outcome.acquisition'),
            'outcome.acquisition', ACQUISITION_COMPLETED)
        requested_rows = _integer(outcome['requested_rows'],
                                  'outcome.requested_rows')
        completed_rows = _integer(outcome['completed_rows'],
                                  'outcome.completed_rows')
        if completed_rows != requested_rows:
            raise RecordValidationError(
                'outcome.completed_rows: differs from '
                'outcome.requested_rows')
        stages = {
            key: _literal(_text(outcome[key], f'outcome.{key}'),
                          f'outcome.{key}', STAGE_NOT_YET_ESTABLISHED)
            for key in _STAGE_KEYS
        }
        payload = _PROFILE_VALIDATORS[profile](root['payload'],
                                               requested_rows)
        return {
            'namespace': namespace,
            'version': FORMAT_VERSION,
            'profile': profile,
            'execution_id': execution_id,
            'group_id': group_id,
            'record': {'name': record_name, 'columns': columns},
            'software': {
                'softlab_version': softlab_version,
                'python_version': python_version,
            },
            'source': {
                'reference': reference,
                'digest_algorithm': digest_algorithm,
                'digest_scope': digest_scope,
                'digest': digest,
                'git_revision': git_revision,
                'git_dirty': git_dirty,
            },
            'outcome': {
                'snapshot': snapshot,
                'acquisition': acquisition,
                'requested_rows': requested_rows,
                'completed_rows': completed_rows,
                **stages,
            },
            'payload': payload,
        }

    def encode(self, envelope: Dict[str, Any]) -> str:
        """Validate and encode an envelope dict to deterministic JSON text.

        Raises ``TypeError`` unless the argument is exactly a dict,
        ``RecordValidationError`` for any contract violation. The
        caller's dict is read only; validation output is freshly
        built. Performs no I/O.
        """
        if type(envelope) is not dict:
            raise TypeError('run_record: expected dict')
        validated = _validate_and_normalize(envelope)
        text = json.dumps(validated, sort_keys=True, allow_nan=False,
                          ensure_ascii=True, separators=(',', ':'))
        if len(text.encode('ascii')) > MAX_ENCODED_BYTES:
            raise RecordValidationError(
                f'run_record: encoded size exceeds limit '
                f'{MAX_ENCODED_BYTES} bytes')
        return text

    def decode(self, text: str) -> Dict[str, Any]:
        """Decode and validate JSON text to a normalized envelope dict.

        Raises ``TypeError`` for non-str input, ``RecordDecodeError``
        for any text/parse-stage failure, ``RecordValidationError``
        for any contract violation. Performs no I/O.
        """
        if type(text) is not str:
            raise TypeError('run_record: expected JSON text')
        try:
            size = len(text.encode('utf-8'))
        except UnicodeEncodeError as exc:
            raise RecordDecodeError(
                'run_record: input text is not encodable as UTF-8 '
                '(unpaired surrogate)') from exc
        if size > MAX_ENCODED_BYTES:
            raise RecordDecodeError(
                f'run_record: encoded size exceeds limit '
                f'{MAX_ENCODED_BYTES} bytes')
        value = self._parse_strict(text)
        self._sweep_decode(value, '', 1)
        if type(value) is not dict:
            raise RecordDecodeError('run_record: root must be an object')
        return self._validate_envelope(value)


# --- Pure builders (§4) ---


def build_thermal_payload(*, adapter: str,
                          configuration: Dict[str, Any],
                          model: Dict[str, Any],
                          boundary: Dict[str, Any]) -> Dict[str, Any]:
    """Build and validate the closed thermal payload (§1.6).

    ``configuration`` carries the seven ``ThermalConfig`` values;
    ``model`` the client-declared name/method/equation/validity/units
    evidence, which must equal the frozen :data:`THERMAL_MODEL`
    exactly; ``boundary`` the ``{final_tick, final_sim_time}``
    acquisition-boundary evidence. ``procedure`` and ``timing`` are
    derived from the frozen profile literals and ``adapter`` /
    ``configuration`` (delay ``1``/``'tick-1'`` for ``'coordinated'``,
    ``0``/``'tick'`` for ``'direct'``). Returns a normalized dict.

    Raises ``TypeError`` for non-dict container arguments and
    ``RecordValidationError`` for any contract violation.
    """
    if type(adapter) is not str:
        raise TypeError('run_record: expected text')
    if adapter not in _THERMAL_TIMING:
        raise RecordValidationError(
            'payload.adapter: unsupported clock adapter '
            '(supported: coordinated, direct)')
    if type(configuration) is not dict:
        raise TypeError('run_record: expected dict')
    if type(model) is not dict:
        raise TypeError('run_record: expected dict')
    if type(boundary) is not dict:
        raise TypeError('run_record: expected dict')
    normalized_config = _thermal_configuration(
        JsonRunRecordCodec._sweep_encode(configuration, '', 1),
        'payload.configuration')
    delay, source_tick = _THERMAL_TIMING[adapter]
    payload = {
        'version': THERMAL_PAYLOAD_VERSION,
        'adapter': adapter,
        'configuration': normalized_config,
        'model': model,
        'procedure': {
            'name': 'thermal_count',
            'advancement': 'once_before_each_getter_row',
            'command': 'constant_power',
            'requested_points': normalized_config['points'],
        },
        'timing': {
            'dt': normalized_config['dt'],
            'simulation_unit': 's',
            'acquisition': 'post_step',
            'measurement_delay_ticks': delay,
            'source_tick': source_tick,
            'wall_timestamp': 'unix_seconds',
        },
        'boundary': boundary,
    }
    return _validate_thermal_payload(
        JsonRunRecordCodec._sweep_encode(payload, '', 1), None)


def build_signal_payload(*, points: int,
                         offsets: Dict[str, Any]) -> Dict[str, Any]:
    """Build and validate the closed signal payload (§1.7).

    ``offsets`` is the client-declared ``{'signal': 5, 'reference': 2}``
    evidence, which must equal the frozen :data:`SIGNAL_OFFSETS`
    exactly. ``read_policy``/``timing``/``scheduler``/``derived``
    literals are filled by the builder. Returns a normalized dict.

    Raises ``RecordValidationError`` for any contract violation.
    """
    payload = {
        'version': SIGNAL_PAYLOAD_VERSION,
        'points': points,
        'read_policy': {
            'semantics': 'tick_plus_offset',
            'consumptive': False,
            'offsets': offsets,
        },
        'timing': {'timestamp': 'wall_clock'},
        'scheduler': {'ownership': 'run_count_owned'},
        'derived': {
            'difference': {
                'formula': 'signal - reference',
                'storage': 'caller_derived_not_stored',
            },
        },
    }
    return _validate_signal_payload(
        JsonRunRecordCodec._sweep_encode(payload, '', 1), None)


def build_envelope(*, profile: str, execution_id: str, group_id: str,
                   record_name: str, columns: List[Dict[str, Any]],
                   requested_rows: int, completed_rows: int,
                   software: Dict[str, Any], source: Dict[str, Any],
                   payload: Dict[str, Any]) -> Dict[str, Any]:
    """Assemble and validate a complete envelope (§1).

    The ``outcome`` object is constructed from the frozen literals
    plus the supplied row counts; all common, profile, and
    cross-layer checks run. Returns a normalized dict suitable for
    :func:`encode_envelope`.

    Raises ``RecordValidationError`` for any contract violation.
    """
    envelope = {
        'namespace': NAMESPACE,
        'version': FORMAT_VERSION,
        'profile': profile,
        'execution_id': execution_id,
        'group_id': group_id,
        'record': {'name': record_name, 'columns': columns},
        'software': software,
        'source': source,
        'outcome': {
            'snapshot': OUTCOME_SNAPSHOT,
            'acquisition': ACQUISITION_COMPLETED,
            'requested_rows': requested_rows,
            'completed_rows': completed_rows,
            'persistence': STAGE_NOT_YET_ESTABLISHED,
            'plotting': STAGE_NOT_YET_ESTABLISHED,
            'cleanup': STAGE_NOT_YET_ESTABLISHED,
            'overall': STAGE_NOT_YET_ESTABLISHED,
        },
        'payload': payload,
    }
    return _validate_and_normalize(envelope)


# --- Module-level convenience entry points (§2.2) ---

_default_codec = JsonRunRecordCodec()


def encode_envelope(envelope: Dict[str, Any]) -> str:
    """Encode ``envelope`` with the shared module-level codec instance.

    The codec is stateless, so the shared instance carries no
    cross-call state. Raises ``TypeError`` for a non-dict argument and
    ``RecordValidationError`` for any contract violation.
    """
    return _default_codec.encode(envelope)


def decode_envelope(text: str) -> Dict[str, Any]:
    """Decode ``text`` with the shared module-level codec instance.

    The codec is stateless, so the shared instance carries no
    cross-call state. Raises ``TypeError`` for a non-str argument,
    ``RecordDecodeError`` for any text/parse-stage failure, and
    ``RecordValidationError`` for any contract violation.
    """
    return _default_codec.decode(text)
