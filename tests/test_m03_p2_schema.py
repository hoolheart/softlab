"""M03 phase 2 executable schema/codec suite (S1-S7, F1-F17).

Assertion-based probes against the phase 2 run-record codec and the
bounded in-memory group-construction seam, per the approved plan
(log/release_12/test/schema-test-plan.md, delta-revised 25a0c2e,
developer APPROVED 8035958) and the frozen design
(log/release_12/design/p2-schema-design.md, second revision 4b4b6db).

Areas:
- S1: valid round-trips (both profiles, both thermal clock variants),
  byte determinism, UUID distinctness hard rejection, count relations,
  outcome literals, availability semantics, and the group-construction
  seam (recorder-double assertions; no autospec+wraps anywhere).
- S2: failure matrix F1-F17 with the frozen exception types and
  verbatim message templates from design §8.4.
- S3: boolean-vs-number strictness, container non-normalization, and
  the huge-integer boundary (10**1023 accepted both directions,
  10**1024 rejected, negative 1024-digit token accepted).
- S4: size/depth/count limits layered per the design §2.3 reachability
  table, constant-sourced limits (module-only MAX_* constants).
- S5: side-effect-free failure (caller inputs unmutated, no
  log-and-continue, no write-before-validate by signature contract).
- S6: legacy preservation (v1/v2 manifests readable, export surface
  frozen, package-level MAX_* absence).
- S7: environment and hygiene (programmatic fixtures only, no
  hardcoded environment facts, isinstance(ValueError) asserted only
  for the two leaf error classes).

All fixtures are synthetic and programmatically constructed; observed
behavior that contradicts a plan expectation is reported in
log/release_12/test/schema-test-results.md, never silently weakened.
"""

import collections
import copy
import hashlib
import importlib
import importlib.util
import inspect
import json
import logging
import math
import platform
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from unittest import mock

import numpy as np

import softlab
from softlab.shui import run_group as run_group_module
from softlab.shui import run_record
from softlab.shui.run_group import build_run_group
from softlab.shui.run_record import (
    MAX_ARRAY_LENGTH,
    MAX_DEPTH,
    MAX_ENCODED_BYTES,
    MAX_INTEGER_DIGITS,
    MAX_OBJECT_ENTRIES,
    MAX_STRING_LENGTH,
    MAX_UNAVAILABLE_REASON_LENGTH,
    METADATA_KEY,
    SIGNAL_OFFSETS,
    THERMAL_MODEL,
    RecordDecodeError,
    RecordValidationError,
    RunRecordError,
    decode_envelope,
    encode_envelope,
)
from softlab.shui.data import DataGroup


ROOT = Path(__file__).resolve().parents[1]

# Frozen limits (design §8.3); read from the module constants in the
# body of the suite (S4.4) — these imports ARE the module constant
# reads, asserted against the frozen design values there.

_ENVELOPE_KEYS = ('namespace', 'version', 'profile', 'execution_id',
                  'group_id', 'record', 'software', 'source', 'outcome',
                  'payload')

_THERMAL_COLUMNS = (
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

_SIGNAL_COLUMNS = (
    {'name': 'timestamp', 'label': '', 'unit': 's', 'dependent': False},
    {'name': 'signal', 'label': '', 'unit': 'V', 'dependent': True},
    {'name': 'reference', 'label': '', 'unit': 'V', 'dependent': True},
)

_FEATURE_EXPORTS = (
    'RunRecordCodec', 'JsonRunRecordCodec', 'encode_envelope',
    'decode_envelope', 'build_envelope', 'build_thermal_payload',
    'build_signal_payload', 'build_run_group', 'RunRecordError',
    'RecordDecodeError', 'RecordValidationError',
)

_LIMIT_EXPORTS = (
    'MAX_ENCODED_BYTES', 'MAX_DEPTH', 'MAX_ARRAY_LENGTH',
    'MAX_OBJECT_ENTRIES', 'MAX_STRING_LENGTH',
    'MAX_UNAVAILABLE_REASON_LENGTH', 'MAX_INTEGER_DIGITS',
)

_MODULE_ONLY_NAMES = (
    'NAMESPACE', 'METADATA_KEY', 'FORMAT_VERSION', 'THERMAL_PAYLOAD_VERSION',
    'SIGNAL_PAYLOAD_VERSION', 'PROFILE_THERMAL', 'PROFILE_SIGNAL',
    'OUTCOME_SNAPSHOT', 'ACQUISITION_COMPLETED', 'STAGE_NOT_YET_ESTABLISHED',
    'STATE_AVAILABLE', 'STATE_UNAVAILABLE', 'UNAVAILABLE_REASONS',
    'THERMAL_MODEL', 'SIGNAL_OFFSETS',
)


def _hex64(seed):
    return hashlib.sha256(seed.encode('utf-8')).hexdigest()


def _hex40(seed):
    return hashlib.sha1(seed.encode('utf-8')).hexdigest()


def _software():
    return {'softlab_version': str(softlab.__version__),
            'python_version': platform.python_version()}


def _source(reference, git_available=False):
    if git_available:
        return {
            'reference': reference,
            'digest_algorithm': 'sha256',
            'digest_scope': 'exact reference script bytes at production',
            'digest': {'state': 'available', 'value': _hex64('digest'),
                       'reason': None},
            'git_revision': {'state': 'available',
                             'value': _hex40('revision'), 'reason': None},
            'git_dirty': {'state': 'available', 'value': True,
                          'reason': None},
        }
    return {
        'reference': reference,
        'digest_algorithm': 'sha256',
        'digest_scope': 'exact reference script bytes at production',
        'digest': {'state': 'available', 'value': _hex64('digest'),
                   'reason': None},
        'git_revision': {'state': 'unavailable', 'value': None,
                         'reason': 'capture_failed'},
        'git_dirty': {'state': 'unavailable', 'value': None,
                      'reason': 'capture_failed'},
    }


def _outcome(points):
    return {
        'snapshot': 'completed_acquisition_before_persistence',
        'acquisition': 'completed',
        'requested_rows': points,
        'completed_rows': points,
        'persistence': 'not_yet_established',
        'plotting': 'not_yet_established',
        'cleanup': 'not_yet_established',
        'overall': 'not_yet_established',
    }


def _thermal_payload(adapter, points=8):
    if adapter == 'coordinated':
        delay, tick = 1, 'tick-1'
    elif adapter == 'direct':
        delay, tick = 0, 'tick'
    else:
        raise ValueError(adapter)
    configuration = {
        'initial_temperature': 20.0, 'ambient_temperature': 20.0,
        'heat_capacity': 10.0, 'conductance': 1.0, 'dt': 1.0,
        'power': 10.0, 'points': points,
    }
    return {
        'version': 1,
        'adapter': adapter,
        'configuration': configuration,
        'model': copy.deepcopy(THERMAL_MODEL),
        'procedure': {
            'name': 'thermal_count',
            'advancement': 'once_before_each_getter_row',
            'command': 'constant_power',
            'requested_points': points,
        },
        'timing': {
            'dt': 1.0, 'simulation_unit': 's', 'acquisition': 'post_step',
            'measurement_delay_ticks': delay, 'source_tick': tick,
            'wall_timestamp': 'unix_seconds',
        },
        'boundary': {'final_tick': points,
                     'final_sim_time': float(points) * 1.0},
    }


def _signal_payload(points=3):
    return {
        'version': 1,
        'points': points,
        'read_policy': {
            'semantics': 'tick_plus_offset',
            'consumptive': False,
            'offsets': {'signal': 5, 'reference': 2},
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


def thermal_envelope(adapter='coordinated', points=8, git_available=False):
    """Canonical minimal thermal envelope (design §8.6)."""
    return {
        'namespace': 'softlab.run-record',
        'version': 1,
        'profile': 'thermal',
        'execution_id': str(uuid.uuid4()),
        'group_id': str(uuid.uuid4()),
        'record': {'name': 'thermal_trace',
                   'columns': copy.deepcopy(list(_THERMAL_COLUMNS))},
        'software': _software(),
        'source': _source('examples/thermal_experiment.py', git_available),
        'outcome': _outcome(points),
        'payload': _thermal_payload(adapter, points),
    }


def signal_envelope(points=3, git_available=False):
    """Canonical minimal signal envelope (design §8.6)."""
    return {
        'namespace': 'softlab.run-record',
        'version': 1,
        'profile': 'signal',
        'execution_id': str(uuid.uuid4()),
        'group_id': str(uuid.uuid4()),
        'record': {'name': 'signal',
                   'columns': copy.deepcopy(list(_SIGNAL_COLUMNS))},
        'software': _software(),
        'source': _source('examples/signal_monitor.py', git_available),
        'outcome': _outcome(points),
        'payload': _signal_payload(points),
    }


def envelope_text(envelope):
    """Serialize an envelope dict as JSON text for decode-side probes."""
    return json.dumps(envelope)


def chain(leaf, containers):
    """Wrap ``leaf`` in ``containers`` nested single-key dicts."""
    value = leaf
    for _ in range(containers):
        value = {'x': value}
    return value


class _BenignStr:
    """Probe object whose ``__str__`` is benign (S3.3)."""

    def __str__(self):
        return '3'


class _UnstableValue:
    """Sweep-rejected probe whose deep copies compare equal.

    Unlike ``object()``, an instance of this class compares equal to any
    other instance, so a deep-copied snapshot stays assertEqual-comparable
    after a failed encode (S5.2).
    """

    def __eq__(self, other):
        return isinstance(other, _UnstableValue)


class CodecAssertMixin:
    """Shared helpers for frozen-type/message assertions."""

    def assert_frozen(self, func, exc_type, message):
        """Assert ``func()`` raises exactly ``exc_type`` with ``message``."""
        with self.assertRaises(exc_type) as caught:
            func()
        self.assertIs(type(caught.exception), exc_type)
        self.assertEqual(str(caught.exception), message)
        return caught.exception

    def assert_encode_rejects(self, envelope, message):
        return self.assert_frozen(lambda: encode_envelope(envelope),
                                  RecordValidationError, message)

    def assert_decode_rejects(self, text, exc_type, message):
        return self.assert_frozen(lambda: decode_envelope(text),
                                  exc_type, message)


class S1ValidRoundTrips(CodecAssertMixin, unittest.TestCase):
    """S1.1-S1.7: valid envelopes round-trip exactly and deterministically."""

    def test_s1_1_thermal_both_clock_variants_round_trip(self):
        for adapter in ('coordinated', 'direct'):
            with self.subTest(adapter=adapter):
                envelope = thermal_envelope(adapter)
                text = encode_envelope(envelope)
                decoded = decode_envelope(text)
                self.assertEqual(decoded, envelope)
                self.assertEqual(decode_envelope(text), decoded)
                if adapter == 'direct':
                    self.assertEqual(
                        decoded['payload']['timing']['measurement_delay_ticks'], 0)
                    self.assertEqual(
                        decoded['payload']['timing']['source_tick'], 'tick')
                else:
                    self.assertEqual(
                        decoded['payload']['timing']['measurement_delay_ticks'], 1)
                    self.assertEqual(
                        decoded['payload']['timing']['source_tick'], 'tick-1')

    def test_s1_2_signal_round_trip_columns(self):
        envelope = signal_envelope()
        decoded = decode_envelope(encode_envelope(envelope))
        self.assertEqual(decoded, envelope)
        names = [column['name'] for column in decoded['record']['columns']]
        self.assertEqual(names, ['timestamp', 'signal', 'reference'])
        self.assertNotIn('difference', names)
        for column in decoded['record']['columns']:
            self.assertIs(type(column['unit']), str)
            self.assertNotEqual(column['unit'], '')

    def test_s1_3_byte_determinism_and_canonical_serialization(self):
        for builder in (lambda: thermal_envelope(),
                        lambda: signal_envelope(),
                        lambda: thermal_envelope('direct')):
            first = builder()
            # Independently constructed but deep-equal input: same field
            # values (including the caller-supplied identity UUIDs), fresh
            # container objects.
            second = copy.deepcopy(first)
            self.assertIsNot(first, second)
            self.assertEqual(first, second)
            text_one = encode_envelope(first)
            text_two = encode_envelope(second)
            self.assertEqual(text_one, text_two)
            # Cross-check: encoded text is strict JSON and canonical.
            reparsed = json.loads(text_one)
            self.assertEqual(reparsed, first)
            self.assertEqual(
                text_one,
                json.dumps(reparsed, sort_keys=True, allow_nan=False,
                           ensure_ascii=True, separators=(',', ':')))
            text_one.encode('ascii')  # ASCII-only by construction

    def test_s1_4_identity_fields_distinct_and_canonical(self):
        envelope = thermal_envelope()
        envelope['execution_id'] = str(uuid.uuid4())
        envelope['group_id'] = str(uuid.uuid4())
        self.assertNotEqual(envelope['execution_id'], envelope['group_id'])
        decoded = decode_envelope(encode_envelope(envelope))
        self.assertEqual(decoded['execution_id'], envelope['execution_id'])
        self.assertEqual(decoded['group_id'], envelope['group_id'])

        same = thermal_envelope()
        same['group_id'] = same['execution_id']
        self.assert_encode_rejects(same,
                                   'execution_id: must differ from group_id')

        raw_id = str(uuid.uuid4())
        for probe in (raw_id.upper(), raw_id.replace('-', ''),
                      '{' + raw_id + '}'):
            with self.subTest(probe=probe):
                bad = thermal_envelope()
                bad['execution_id'] = probe
                self.assert_encode_rejects(bad,
                                           'execution_id: expected canonical '
                                           'UUID')
        bad = thermal_envelope()
        bad['group_id'] = bad['group_id'].upper()
        self.assert_encode_rejects(bad, 'group_id: expected canonical UUID')

    def test_s1_5_schema_ordering_units_and_count_relations(self):
        thermal = decode_envelope(encode_envelope(thermal_envelope()))
        names = [c['name'] for c in thermal['record']['columns']]
        self.assertEqual(names, [c['name'] for c in _THERMAL_COLUMNS])
        outcome = thermal['outcome']
        payload = thermal['payload']
        for count in (outcome['requested_rows'], outcome['completed_rows'],
                      payload['configuration']['points'],
                      payload['procedure']['requested_points'],
                      payload['boundary']['final_tick']):
            self.assertIs(type(count), int)
        self.assertEqual(outcome['requested_rows'],
                         outcome['completed_rows'])
        self.assertEqual(payload['configuration']['points'],
                         outcome['requested_rows'])
        self.assertEqual(payload['procedure']['requested_points'],
                         outcome['requested_rows'])
        self.assertEqual(payload['boundary']['final_tick'],
                         outcome['requested_rows'])
        for column in thermal['record']['columns']:
            self.assertTrue(column['unit'])  # explicit unit, '1' for unitless

        signal = decode_envelope(encode_envelope(signal_envelope()))
        self.assertEqual(signal['payload']['points'],
                         signal['outcome']['requested_rows'])

        empty_unit = thermal_envelope()
        empty_unit['record']['columns'][0]['unit'] = ''
        self.assert_encode_rejects(
            empty_unit, 'record.columns.0.unit: expected nonempty text')

    def test_s1_6_acquisition_snapshot_only_outcome(self):
        envelope = thermal_envelope()
        decoded = decode_envelope(encode_envelope(envelope))
        self.assertEqual(
            decoded['outcome']['snapshot'],
            'completed_acquisition_before_persistence')
        self.assertEqual(decoded['outcome']['acquisition'], 'completed')
        for stage in ('persistence', 'plotting', 'cleanup', 'overall'):
            self.assertEqual(decoded['outcome'][stage], 'not_yet_established')

        late = thermal_envelope()
        late['outcome']['persistence'] = 'completed'
        self.assert_encode_rejects(
            late, 'outcome.persistence: unsupported literal '
                  '(supported: not_yet_established)')

        failed = thermal_envelope()
        failed['outcome']['acquisition'] = 'failed'
        self.assert_encode_rejects(
            failed, 'outcome.acquisition: unsupported literal '
                    '(supported: completed)')

    def test_s1_7_availability_fact_semantics(self):
        # Uncaptured facts (canonical): state/value/reason binding verbatim.
        envelope = thermal_envelope()
        decoded = decode_envelope(encode_envelope(envelope))
        for key in ('digest', 'git_revision', 'git_dirty'):
            fact = decoded['source'][key]
            self.assertEqual(set(fact), {'state', 'value', 'reason'})
        digest = decoded['source']['digest']
        self.assertEqual(digest['state'], 'available')
        self.assertEqual(len(digest['value']), 64)
        int(digest['value'], 16)  # lowercase hex
        self.assertEqual(digest['value'], digest['value'].lower())
        self.assertIsNone(digest['reason'])
        git = decoded['source']['git_revision']
        self.assertEqual(git['state'], 'unavailable')
        self.assertIsNone(git['value'])
        self.assertIn(git['reason'], ('capture_failed', 'source_mismatch'))

        # Captured git facts variant: 40- or 64-hex revision, bool dirty.
        captured = signal_envelope(git_available=True)
        decoded = decode_envelope(encode_envelope(captured))
        revision = decoded['source']['git_revision']
        self.assertEqual(revision['state'], 'available')
        self.assertIn(len(revision['value']), (40, 64))
        self.assertTrue(decoded['source']['git_dirty']['value'])
        self.assertEqual(decoded, captured)


class _RecorderGroup:
    """Hand-held recorder double for DataGroup construction/set_meta.

    No autospec and no wraps (S7.3): the double records constructor
    keyword arguments and ``set_meta`` calls; any save/connect/disconnect
    call would hit a plain AttributeError on this double.
    """

    constructed = []

    def __init__(self, **kwargs):
        self.constructor_kwargs = kwargs
        self.set_meta_calls = []
        self.meta = {}
        self.name = kwargs.get('name')
        self.id = kwargs.get('id')
        _RecorderGroup.constructed.append(self)

    def set_meta(self, key, value):
        self.set_meta_calls.append((key, value))
        if value is None:
            self.meta.pop(key, None)
            return
        self.meta[key] = value


class S1GroupConstructionSeam(CodecAssertMixin, unittest.TestCase):
    """S1.8: build_run_group seam with recorder-double assertions."""

    def setUp(self):
        _RecorderGroup.constructed = []

    def test_s1_8_real_group_behavior(self):
        envelope = signal_envelope()
        group = build_run_group('run-1', envelope)
        self.assertIsInstance(group, DataGroup)
        self.assertEqual(str(group.id), envelope['group_id'])
        self.assertEqual(group.records, [])
        text = encode_envelope(envelope)
        self.assertEqual(group.meta, {METADATA_KEY: text})
        # Legacy absence side: no other additive keys exist.
        self.assertIsNone(group.get_meta('thermal_manifest'))

    def test_s1_8_recorder_double_order_and_single_write(self):
        envelope = signal_envelope()
        expected_text = encode_envelope(envelope)
        order = []

        def encode_spy(value):
            order.append('encode')
            return encode_envelope(value)

        def construct(**kwargs):
            order.append('construct')
            return _RecorderGroup(**kwargs)

        with mock.patch.object(run_group_module, 'encode_envelope',
                               side_effect=encode_spy), \
                mock.patch.object(run_group_module, 'DataGroup',
                                  side_effect=construct):
            result = run_group_module.build_run_group('run-1', envelope)

        self.assertEqual(order, ['encode', 'construct'])
        self.assertIsInstance(result, _RecorderGroup)
        self.assertEqual(len(_RecorderGroup.constructed), 1)
        recorder = _RecorderGroup.constructed[0]
        self.assertNotIn('meta', recorder.constructor_kwargs)
        self.assertEqual(recorder.constructor_kwargs['name'], 'run-1')
        self.assertEqual(str(recorder.constructor_kwargs['id']),
                         envelope['group_id'])
        self.assertEqual(recorder.set_meta_calls,
                         [(METADATA_KEY, expected_text)])
        self.assertEqual(recorder.meta, {METADATA_KEY: expected_text})

    def test_s1_8_failure_atomicity(self):
        envelope = signal_envelope()
        envelope['outcome']['requested_rows'] = '8'
        with mock.patch.object(run_group_module, 'DataGroup', _RecorderGroup):
            with self.assertRaises(RecordValidationError):
                run_group_module.build_run_group('run-1', envelope)
        self.assertEqual(_RecorderGroup.constructed, [])

    def test_s1_8_name_contract_probes(self):
        envelope = signal_envelope()
        with mock.patch.object(run_group_module, 'DataGroup', _RecorderGroup):
            exc = self.assert_frozen(
                lambda: run_group_module.build_run_group(5, envelope),
                TypeError, 'run_group: name must be an exact str')
            self.assertIs(type(exc), TypeError)
            self.assertEqual(_RecorderGroup.constructed, [])

            exc = self.assert_frozen(
                lambda: run_group_module.build_run_group('', envelope),
                ValueError, 'run_group: name must be nonempty')
            self.assertIs(type(exc), ValueError)
            self.assertEqual(_RecorderGroup.constructed, [])


class S2FailureMatrix(CodecAssertMixin, unittest.TestCase):
    """F1-F17: one defect per probe, frozen types and messages (§8.4)."""

    def test_f1_malformed_json(self):
        exc = self.assert_decode_rejects(
            '{not json', RecordDecodeError,
            'run_record: malformed JSON: '
            'Expecting property name enclosed in double quotes: line 1 '
            'column 2 (char 1)')
        self.assertIsInstance(exc, ValueError)

    def test_f2_wrong_root_type(self):
        for text in ('[]', '"x"', '5', 'true', 'null'):
            with self.subTest(text=text):
                self.assert_decode_rejects(text, RecordDecodeError,
                                           'run_record: root must be an '
                                           'object')

    def test_f3_duplicate_keys_any_depth(self):
        self.assert_decode_rejects(
            '{"a": 1, "a": 2}', RecordDecodeError,
            "run_record: duplicate object key 'a'")
        envelope = signal_envelope()
        text = envelope_text(envelope)
        injected = text.replace(
            '"reference": 2}',
            '"reference": 2, "reference": 2}')
        self.assertIn('"reference": 2, "reference": 2', injected)
        self.assert_decode_rejects(injected, RecordDecodeError,
                                   "run_record: duplicate object key "
                                   "'reference'")

    def test_f4_unsupported_value_types_encode(self):
        envelope = thermal_envelope()
        probes = (
            (object(), 'object'),
            ({1, 2}, 'set'),
            (b'bytes', 'bytes'),
            (complex(1, 2), 'complex'),
            (np.int64(3), 'int64'),
            (np.array([1, 2]), 'ndarray'),
            ((lambda: 0), 'function'),
            (_BenignStr(), '_BenignStr'),
        )
        for index, (value, typename) in enumerate(probes):
            with self.subTest(typename=typename):
                bad = thermal_envelope()
                bad['bad'] = value
                self.assert_encode_rejects(
                    bad, f"bad: unsupported value type '{typename}'")

    def test_f5_nonfinite_numbers(self):
        for token in ('NaN', 'Infinity', '-Infinity'):
            with self.subTest(token=token):
                self.assert_decode_rejects(
                    '{"a": %s}' % token, RecordDecodeError,
                    "run_record: nonfinite JSON token '%s'" % token)
        # Overflow literal: parse_constant does not fire; sweep must catch.
        self.assert_decode_rejects('{"a": 1e999}', RecordDecodeError,
                                   'a: nonfinite number')
        self.assert_decode_rejects('{"a": {"b": {"c": 1e999}}}',
                                   RecordDecodeError,
                                   'a.b.c: nonfinite number')
        envelope = thermal_envelope()
        envelope['payload']['configuration']['power'] = float('nan')
        self.assert_encode_rejects(
            envelope, 'payload.configuration.power: nonfinite number')
        envelope = thermal_envelope()
        envelope['payload']['configuration']['power'] = float('inf')
        self.assert_encode_rejects(
            envelope, 'payload.configuration.power: nonfinite number')

    def test_f6_missing_common_field(self):
        # Root-level missing-field messages carry no path prefix (the
        # root object's path is the empty string, design §3 message
        # discipline: bare detail when the path is empty).
        for key in _ENVELOPE_KEYS:
            with self.subTest(missing=key):
                envelope = signal_envelope()
                del envelope[key]
                self.assert_encode_rejects(
                    envelope, f"missing required field '{key}'")
                self.assert_frozen(lambda: decode_envelope(envelope_text(
                    envelope)), RecordValidationError,
                    f"missing required field '{key}'")

    def test_f7_missing_client_payload_field(self):
        envelope = thermal_envelope()
        del envelope['payload']['boundary']
        self.assert_encode_rejects(
            envelope, "payload: missing required field 'boundary'")
        envelope = signal_envelope()
        del envelope['payload']['read_policy']
        self.assert_encode_rejects(
            envelope, "payload: missing required field 'read_policy'")

    def test_f8_unsupported_common_version(self):
        for version in (2, 0, '1', 1.0, True):
            with self.subTest(version=version):
                envelope = signal_envelope()
                envelope['version'] = version
                self.assert_encode_rejects(
                    envelope,
                    'version: unsupported version (supported: 1)')

    def test_f9_unsupported_payload_version(self):
        envelope = thermal_envelope()
        envelope['payload']['version'] = 2
        self.assert_encode_rejects(
            envelope, 'payload.version: unsupported version (supported: 1)')
        envelope = signal_envelope()
        envelope['payload']['version'] = 2
        self.assert_encode_rejects(
            envelope, 'payload.version: unsupported version (supported: 1)')

    def test_f10_unknown_profile(self):
        envelope = signal_envelope()
        envelope['profile'] = 'spectrum'
        self.assert_encode_rejects(envelope,
                                   'profile: unsupported client profile')

    def test_f11_unknown_fields_closed_policy(self):
        cases = []
        envelope = signal_envelope()
        envelope['extra'] = 1
        cases.append((envelope, "unknown field 'extra'"))
        envelope = signal_envelope()
        envelope['record']['extra'] = 1
        cases.append((envelope, "record: unknown field 'extra'"))
        envelope = signal_envelope()
        envelope['record']['columns'][0]['Unit'] = 's'
        cases.append((envelope,
                      "record.columns.0: unknown field 'Unit'"))
        envelope = signal_envelope()
        envelope['source']['extra'] = 1
        cases.append((envelope, "source: unknown field 'extra'"))
        envelope = signal_envelope()
        envelope['source']['digest']['extra'] = 1
        cases.append((envelope, "source.digest: unknown field 'extra'"))
        envelope = signal_envelope()
        envelope['payload']['extra'] = 1
        cases.append((envelope, "payload: unknown field 'extra'"))
        envelope = signal_envelope()
        envelope['Namespace'] = 'softlab.run-record'
        cases.append((envelope, "unknown field 'Namespace'"))
        envelope = signal_envelope()
        envelope['namespace_'] = 'softlab.run-record'
        cases.append((envelope, "unknown field 'namespace_'"))
        for envelope, message in cases:
            with self.subTest(message=message):
                self.assert_encode_rejects(envelope, message)
        # Decode side shares the contract validation.
        envelope = signal_envelope()
        envelope['extra'] = 1
        self.assert_frozen(lambda: decode_envelope(envelope_text(envelope)),
                           RecordValidationError,
                           "unknown field 'extra'")

    def test_f12_type_errors_on_present_fields(self):
        envelope = signal_envelope()
        envelope['outcome']['requested_rows'] = '8'
        self.assert_encode_rejects(
            envelope, 'outcome.requested_rows: expected exact integer')
        envelope = signal_envelope()
        envelope['outcome']['requested_rows'] = 1.0
        self.assert_encode_rejects(
            envelope, 'outcome.requested_rows: expected exact integer')
        envelope = signal_envelope()
        envelope['outcome']['requested_rows'] = True
        self.assert_encode_rejects(
            envelope, 'outcome.requested_rows: expected exact integer')
        envelope = signal_envelope()
        envelope['record']['columns'][0]['dependent'] = 1
        self.assert_encode_rejects(
            envelope,
            'record.columns.0.dependent: expected boolean')
        envelope = signal_envelope()
        envelope['record']['columns'][0]['dependent'] = 'true'
        self.assert_encode_rejects(
            envelope,
            'record.columns.0.dependent: expected boolean')
        envelope = signal_envelope()
        envelope['software']['softlab_version'] = 5
        self.assert_encode_rejects(
            envelope, 'software.softlab_version: expected text')
        envelope = signal_envelope()
        envelope['record']['columns'] = {'name': 'x'}
        self.assert_encode_rejects(envelope,
                                   'record.columns: expected array')
        envelope = signal_envelope()
        envelope['record']['columns'] = ('a', 'b')
        self.assert_encode_rejects(
            envelope, "record.columns: unsupported value type 'tuple'")

    def test_f13_profile_semantic_violations(self):
        # Column ordering.
        envelope = thermal_envelope()
        columns = envelope['record']['columns']
        columns[0], columns[1] = columns[1], columns[0]
        self.assert_encode_rejects(
            envelope,
            'record.columns: expected ordered thermal column schema')
        # Signal: derived difference smuggled into stored columns.
        envelope = signal_envelope()
        envelope['record']['columns'].append(
            {'name': 'difference', 'label': '', 'unit': 'V',
             'dependent': True})
        self.assert_encode_rejects(
            envelope,
            'record.columns: expected ordered signal column schema')
        # Clock/delay policy (coordinated => 1 + 'tick-1').
        envelope = thermal_envelope('coordinated')
        envelope['payload']['timing']['measurement_delay_ticks'] = 0
        self.assert_encode_rejects(
            envelope,
            'payload.timing.source_tick: inconsistent with '
            'measurement_delay_ticks')
        # v1/v2 source-tick distinction (direct => 0 + 'tick').
        envelope = thermal_envelope('direct')
        envelope['payload']['timing']['source_tick'] = 'tick-1'
        self.assert_encode_rejects(
            envelope,
            'payload.timing.source_tick: inconsistent with '
            'measurement_delay_ticks')
        # Configuration physical validity.
        envelope = thermal_envelope()
        envelope['payload']['configuration']['heat_capacity'] = 0.0
        self.assert_encode_rejects(
            envelope,
            'payload.configuration: expected positive heat_capacity, '
            'conductance, dt')
        envelope = thermal_envelope()
        envelope['payload']['configuration']['conductance'] = 20.0
        self.assert_encode_rejects(
            envelope,
            'payload.configuration: expected 0 < '
            'dt*conductance/heat_capacity <= 1')
        # Boundary cross-checks.
        envelope = thermal_envelope()
        envelope['payload']['boundary']['final_tick'] = 7
        self.assert_encode_rejects(
            envelope,
            'boundary.final_tick: inconsistent with outcome.requested_rows')
        envelope = thermal_envelope()
        envelope['payload']['boundary']['final_sim_time'] = 8.5
        self.assert_encode_rejects(
            envelope,
            'payload.boundary.final_sim_time: inconsistent with '
            'configuration.points * configuration.dt')
        # Frozen record names.
        envelope = thermal_envelope()
        envelope['record']['name'] = 'thermal'
        self.assert_encode_rejects(
            envelope,
            'record.name: unsupported record name '
            '(supported: thermal_trace)')
        envelope = signal_envelope()
        envelope['record']['name'] = 'signal_trace'
        self.assert_encode_rejects(
            envelope,
            "record.name: unsupported record name (supported: signal)")

    def test_f13_frozen_model_and_offsets_drift(self):
        # Thermal model drift: equation, method, validity, units.
        for mutate in (
            lambda m: m.update(equation='T_next = T'),
            lambda m: m.update(method='implicit_euler'),
            lambda m: m.update(validity='calibrated'),
            lambda m: m['units'].update(heat_capacity='J'),
            lambda m: m['units'].update(power='kW'),
            lambda m: m['units'].pop('power'),
        ):
            envelope = thermal_envelope()
            mutate(envelope['payload']['model'])
            self.assert_encode_rejects(
                envelope,
                'payload.model: unsupported model semantics or units')
        # Signal offsets drift: values, zero/negative, extra/renamed keys.
        for offsets in (
            {'signal': 4, 'reference': 2},
            {'signal': 0, 'reference': 2},
            {'signal': -5, 'reference': 2},
            {'signal': 5, 'reference': 2, 'extra': 1},
            {'sig': 5, 'reference': 2},
            {'signal': 5},
        ):
            envelope = signal_envelope()
            envelope['payload']['read_policy']['offsets'] = offsets
            self.assert_encode_rejects(
                envelope,
                "payload.read_policy.offsets: unsupported read offsets "
                "(supported: {'signal': 5, 'reference': 2})")

    def test_f13_count_mismatch_probes(self):
        envelope = signal_envelope()
        envelope['outcome']['completed_rows'] = 4
        self.assert_encode_rejects(
            envelope,
            'outcome.completed_rows: differs from outcome.requested_rows')
        envelope = thermal_envelope()
        envelope['payload']['configuration']['points'] = 4
        self.assert_encode_rejects(
            envelope,
            'configuration.points: inconsistent with outcome.requested_rows')
        envelope = signal_envelope()
        envelope['payload']['points'] = 4
        self.assert_encode_rejects(
            envelope,
            'payload.points: inconsistent with outcome.requested_rows')
        # Decode side shares the same contract message.
        envelope = signal_envelope()
        envelope['payload']['points'] = 4
        self.assert_frozen(
            lambda: decode_envelope(envelope_text(envelope)),
            RecordValidationError,
            'payload.points: inconsistent with outcome.requested_rows')

    def test_f14_explicit_null_without_assigned_meaning(self):
        cases = []
        envelope = signal_envelope()
        envelope['namespace'] = None
        cases.append((envelope, 'namespace: explicit null has no assigned '
                               'meaning'))
        envelope = signal_envelope()
        envelope['version'] = None
        cases.append((envelope, 'version: explicit null has no assigned '
                               'meaning'))
        envelope = signal_envelope()
        envelope['record'] = None
        cases.append((envelope, 'record: explicit null has no assigned '
                               'meaning'))
        envelope = signal_envelope()
        envelope['record']['name'] = None
        cases.append((envelope, 'record.name: explicit null has no assigned '
                               'meaning'))
        envelope = signal_envelope()
        envelope['software']['softlab_version'] = None
        cases.append((envelope,
                      'software.softlab_version: explicit null has no '
                      'assigned meaning'))
        envelope = signal_envelope()
        envelope['source']['reference'] = None
        cases.append((envelope, 'source.reference: explicit null has no '
                               'assigned meaning'))
        envelope = signal_envelope()
        envelope['source']['digest'] = None
        cases.append((envelope, 'source.digest: explicit null has no assigned '
                               'meaning'))
        envelope = signal_envelope()
        envelope['outcome']['requested_rows'] = None
        cases.append((envelope, 'outcome.requested_rows: explicit null has '
                               'no assigned meaning'))
        envelope = signal_envelope()
        envelope['payload'] = None
        cases.append((envelope, 'payload: explicit null has no assigned '
                               'meaning'))
        envelope = thermal_envelope()
        envelope['payload']['configuration']['initial_temperature'] = None
        cases.append(
            (envelope, 'payload.configuration.initial_temperature: '
                       'explicit null has no assigned meaning'))
        for envelope, message in cases:
            with self.subTest(message=message):
                self.assert_encode_rejects(envelope, message)

    def test_f15_structurally_invalid_availability(self):
        envelope = signal_envelope()
        envelope['source']['digest']['value'] = 'not-hex-at-all'
        self.assert_encode_rejects(envelope,
                                   'source.digest: invalid available value')
        envelope = signal_envelope()
        envelope['source']['digest']['value'] = _hex64('x')[:-1] + 'Z'
        self.assert_encode_rejects(envelope,
                                   'source.digest: invalid available value')
        envelope = signal_envelope()
        envelope['source']['digest']['value'] = _hex64('x')[:63]
        self.assert_encode_rejects(envelope,
                                   'source.digest: invalid available value')
        envelope = signal_envelope()
        envelope['source']['digest']['value'] = _hex64('x').upper()
        self.assert_encode_rejects(envelope,
                                   'source.digest: invalid available value')
        # Wrong value type for the fact, probed on AVAILABLE facts (on an
        # unavailable fact a non-null value trips the state binding first).
        envelope = signal_envelope(git_available=True)
        envelope['source']['git_revision']['value'] = 'zzz'
        self.assert_encode_rejects(
            envelope, 'source.git_revision: invalid available value')
        envelope = signal_envelope(git_available=True)
        envelope['source']['git_dirty']['value'] = 'yes'
        self.assert_encode_rejects(envelope,
                                   'source.git_dirty: invalid available value')
        envelope = signal_envelope()
        envelope['source']['digest']['state'] = 'present'
        self.assert_encode_rejects(envelope,
                                   'source.digest: invalid available value')
        # Empty text where nonempty is required.
        envelope = signal_envelope()
        envelope['source']['reference'] = ''
        self.assert_encode_rejects(envelope,
                                   'source.reference: expected nonempty text')

    def test_f16_availability_state_reason_binding(self):
        envelope = signal_envelope()
        envelope['source']['git_revision']['reason'] = 'hardware_failed'
        self.assert_encode_rejects(
            envelope,
            'source.git_revision: reason outside bounded vocabulary')
        envelope = signal_envelope()
        envelope['source']['git_revision']['reason'] = None
        self.assert_encode_rejects(
            envelope,
            'source.git_revision: unavailable state requires null value '
            'and bounded reason')
        envelope = signal_envelope(git_available=True)
        envelope['source']['digest']['reason'] = 'capture_failed'
        self.assert_encode_rejects(
            envelope,
            'source.digest: available state requires value and null reason')
        # Git binding: both facts individually valid but in different
        # states (start from two captured facts, demote one).
        envelope = signal_envelope(git_available=True)
        envelope['source']['git_revision'] = {
            'state': 'unavailable', 'value': None, 'reason': 'capture_failed'}
        self.assert_encode_rejects(
            envelope,
            'source.git_dirty.state: differs from '
            'source.git_revision.state')
        # Available state with null value has no valid structural shape.
        envelope = signal_envelope()
        envelope['source']['digest']['value'] = None
        self.assert_encode_rejects(envelope,
                                   'source.digest: invalid available value')

    def test_f17_unpaired_surrogate_decode_input(self):
        self.assert_decode_rejects(
            '{"a": "\ud800"}', RecordDecodeError,
            'run_record: input text is not encodable as UTF-8 '
            '(unpaired surrogate)')


class S3TypeStrictness(CodecAssertMixin, unittest.TestCase):
    """S3: boolean-vs-number, containers, huge integers, null policy."""

    def test_s3_1_boolean_not_integer(self):
        envelope = signal_envelope()
        envelope['outcome']['requested_rows'] = True
        self.assert_encode_rejects(
            envelope, 'outcome.requested_rows: expected exact integer')
        envelope = signal_envelope()
        envelope['outcome']['completed_rows'] = False
        self.assert_encode_rejects(
            envelope, 'outcome.completed_rows: expected exact integer')
        # Decode side: JSON true token at an integer field.
        envelope = signal_envelope()
        envelope['outcome']['requested_rows'] = True
        self.assert_frozen(
            lambda: decode_envelope(envelope_text(envelope)),
            RecordValidationError,
            'outcome.requested_rows: expected exact integer')

    def test_s3_2_number_not_boolean(self):
        envelope = signal_envelope()
        envelope['record']['columns'][0]['dependent'] = 1
        self.assert_encode_rejects(
            envelope, 'record.columns.0.dependent: expected boolean')
        envelope = signal_envelope()
        envelope['record']['columns'][0]['dependent'] = 0
        self.assert_encode_rejects(
            envelope, 'record.columns.0.dependent: expected boolean')

    def test_s3_3_no_implicit_stringification(self):
        envelope = signal_envelope()
        envelope['bad'] = _BenignStr()
        self.assert_encode_rejects(
            envelope, "bad: unsupported value type '_BenignStr'")
        envelope = signal_envelope()
        envelope['outcome']['requested_rows'] = '3'
        self.assert_encode_rejects(
            envelope, 'outcome.requested_rows: expected exact integer')
        envelope = thermal_envelope()
        envelope['payload']['configuration']['dt'] = '0.1'
        self.assert_encode_rejects(
            envelope, 'payload.configuration.dt: expected finite real')

    def test_s3_4_integer_real_distinction_and_negative_zero(self):
        # 1.0 rejected where an exact integer is required.
        envelope = signal_envelope()
        envelope['outcome']['requested_rows'] = 3.0
        self.assert_encode_rejects(
            envelope, 'outcome.requested_rows: expected exact integer')
        # Finite reals accept 1.0 and normalize to float.
        envelope = thermal_envelope()
        envelope['payload']['configuration']['initial_temperature'] = 1.0
        decoded = decode_envelope(encode_envelope(envelope))
        self.assertIs(type(decoded['payload']['configuration']
                           ['initial_temperature']), float)
        # Int-configured reals normalize to float on decode.
        envelope = thermal_envelope()
        envelope['payload']['configuration']['initial_temperature'] = 20
        decoded = decode_envelope(encode_envelope(envelope))
        value = decoded['payload']['configuration']['initial_temperature']
        self.assertIs(type(value), float)
        self.assertEqual(value, 20.0)
        # Negative zero is valid and round-trips exactly.
        envelope = thermal_envelope()
        envelope['payload']['configuration']['initial_temperature'] = -0.0
        text = encode_envelope(envelope)
        self.assertIn('-0.0', text)
        decoded = decode_envelope(text)
        value = decoded['payload']['configuration']['initial_temperature']
        self.assertEqual(value, 0.0)
        self.assertEqual(math.copysign(1.0, value), -1.0)
        self.assertEqual(decoded, envelope)

    def test_s3_5_container_types_not_normalized(self):
        # Root argument must be exactly dict.
        envelope = signal_envelope()
        for root in (collections.OrderedDict(envelope),
                     types.MappingProxyType(envelope),
                     [1, 2], (1, 2), 'text', 5):
            with self.subTest(root=type(root).__name__):
                self.assert_frozen(lambda: encode_envelope(root), TypeError,
                                   'run_record: expected dict')
        # Nested containers are not normalized either.
        envelope = thermal_envelope()
        envelope['payload']['configuration'] = collections.OrderedDict(
            envelope['payload']['configuration'])
        self.assert_encode_rejects(
            envelope,
            "payload.configuration: unsupported value type 'OrderedDict'")
        envelope = signal_envelope()
        envelope['payload'] = types.MappingProxyType(envelope['payload'])
        self.assert_encode_rejects(
            envelope, "payload: unsupported value type 'mappingproxy'")
        envelope = signal_envelope()
        envelope['record']['columns'] = tuple(envelope['record']['columns'])
        self.assert_encode_rejects(
            envelope, "record.columns: unsupported value type 'tuple'")
        # Signatures promise only Dict/List (never abstract Mapping);
        # unbound ABC methods carry the conventional ``self`` first.
        for func, expected in (
            (run_record.RunRecordCodec.encode, 'envelope'),
            (run_record.RunRecordCodec.decode, 'text'),
            (encode_envelope, 'envelope'),
            (decode_envelope, 'text'),
        ):
            parameters = [name for name in inspect.signature(func).parameters
                          if name != 'self']
            self.assertEqual(parameters, [expected])

    def test_s3_6_huge_integer_boundary(self):
        at_limit = 10 ** (MAX_INTEGER_DIGITS - 1)  # smallest 1024-digit value
        over_limit = 10 ** MAX_INTEGER_DIGITS      # smallest 1025-digit value
        # Encode: 1024 digits accepted (signal profile pinning).
        envelope = signal_envelope()
        envelope['outcome']['requested_rows'] = at_limit
        envelope['outcome']['completed_rows'] = at_limit
        envelope['payload']['points'] = at_limit
        decoded = decode_envelope(encode_envelope(envelope))
        self.assertEqual(decoded, envelope)
        # Encode: 1025 digits rejected.
        envelope = signal_envelope()
        envelope['outcome']['requested_rows'] = over_limit
        envelope['outcome']['completed_rows'] = over_limit
        envelope['payload']['points'] = over_limit
        self.assert_encode_rejects(
            envelope,
            'outcome.requested_rows: integer digit count exceeds limit '
            '1024')
        # Decode: 1024-digit token round-trips; 1025-digit token rejected
        # by the parse_int bound before any conversion.
        envelope = signal_envelope()
        envelope['outcome']['requested_rows'] = at_limit
        envelope['outcome']['completed_rows'] = at_limit
        envelope['payload']['points'] = at_limit
        self.assertEqual(decode_envelope(envelope_text(envelope)), envelope)
        envelope['outcome']['requested_rows'] = over_limit
        envelope['outcome']['completed_rows'] = over_limit
        envelope['payload']['points'] = over_limit
        self.assert_decode_rejects(
            envelope_text(envelope), RecordDecodeError,
            'run_record: integer digit count exceeds limit 1024')
        # Sign is never counted as a digit: '-' + 1024 digits accepted by
        # the bound on both encode (magnitude probe) and decode (token
        # probe) — the contract layer then rejects the unknown key.
        envelope = signal_envelope()
        envelope['extra'] = -(10 ** (MAX_INTEGER_DIGITS - 1))
        self.assert_encode_rejects(envelope, "unknown field 'extra'")
        envelope = signal_envelope()
        envelope['extra'] = -(10 ** (MAX_INTEGER_DIGITS - 1))
        self.assert_frozen(
            lambda: decode_envelope(envelope_text(envelope)),
            RecordValidationError, "unknown field 'extra'")
        # Finite-real field fed a huge int: OverflowError mapped.
        envelope = thermal_envelope()
        envelope['payload']['configuration']['initial_temperature'] = \
            10 ** 310
        self.assert_encode_rejects(
            envelope,
            'payload.configuration.initial_temperature: expected finite real')

    def test_s3_5_null_versus_absent_policy(self):
        # Distinctness for the two fact slots: absent, present-null,
        # present-value are three distinct results.
        envelope = signal_envelope()
        del envelope['source']['digest']['reason']
        self.assert_encode_rejects(
            envelope, "source.digest: missing required field 'reason'")
        # value=null accepted iff unavailable; reason=null iff available.
        envelope = signal_envelope()
        self.assertIsNone(envelope['source']['git_revision']['value'])
        decoded = decode_envelope(encode_envelope(envelope))
        self.assertIsNone(decoded['source']['git_revision']['value'])
        captured = signal_envelope(git_available=True)
        self.assertIsNone(captured['source']['digest']['reason'])
        decoded = decode_envelope(encode_envelope(captured))
        self.assertIsNone(decoded['source']['digest']['reason'])
        # Mis-bound nulls are rejected (F16 templates).
        envelope = signal_envelope()
        envelope['source']['git_revision']['value'] = 'nonnull'
        self.assert_encode_rejects(
            envelope,
            'source.git_revision: unavailable state requires null value '
            'and bounded reason')
        envelope = signal_envelope(git_available=True)
        envelope['source']['digest']['value'] = None
        self.assert_encode_rejects(envelope,
                                   'source.digest: invalid available value')
        # decode of non-str raises builtin TypeError.
        self.assert_frozen(lambda: decode_envelope(b'text'), TypeError,
                           'run_record: expected JSON text')


class S4Limits(CodecAssertMixin, unittest.TestCase):
    """S4: frozen limits, layered per the §2.3 reachability table."""

    def test_s4_1_depth_layered(self):
        # Depth-8 container passes the sweep, fails at the contract layer.
        envelope = signal_envelope()
        envelope['extra'] = chain(42, 7)  # innermost container at depth 8
        self.assert_encode_rejects(envelope, "unknown field 'extra'")
        # Scalar child of a depth-8 container is never depth-checked.
        envelope = signal_envelope()
        envelope['extra'] = chain('text', 7)
        self.assert_encode_rejects(envelope, "unknown field 'extra'")
        # Depth 9 fails at the sweep (both layers).
        envelope = signal_envelope()
        envelope['extra'] = chain(42, 8)
        self.assert_encode_rejects(
            envelope, 'run_record: nesting depth exceeds limit 8')
        self.assert_frozen(
            lambda: decode_envelope(envelope_text(envelope)),
            RecordDecodeError,
            'run_record: nesting depth exceeds limit 8')
        # Parser recursion exhaustion is mapped, never escapes (nesting
        # far beyond the interpreter recursion limit, within the size
        # bound: 40,000 bytes < 131,072).
        text = '[' * 20000 + ']' * 20000
        self.assertLess(len(text.encode('utf-8')), MAX_ENCODED_BYTES)
        self.assert_decode_rejects(
            text, RecordDecodeError,
            'run_record: nesting exceeds parser recursion limit')

    def test_s4_2_size_boundary_pair(self):
        # Exactly 131,072 bytes passes the size gate; the '{}' object then
        # fails contract validation (proving the gate accepted the size).
        at_limit = '{}' + ' ' * (MAX_ENCODED_BYTES - 2)
        self.assertEqual(len(at_limit.encode('utf-8')), MAX_ENCODED_BYTES)
        # the root object's path is empty, so the frozen message is the
        # bare detail (design §3 message discipline).
        self.assert_frozen(lambda: decode_envelope(at_limit),
                           RecordValidationError,
                           "missing required field 'namespace'")
        over_limit = '{}' + ' ' * (MAX_ENCODED_BYTES - 1)
        self.assertEqual(len(over_limit.encode('utf-8')),
                         MAX_ENCODED_BYTES + 1)
        self.assert_decode_rejects(
            over_limit, RecordDecodeError,
            'run_record: encoded size exceeds limit 131072 bytes')

    def test_s4_2_maximal_valid_envelope(self):
        # Constructible maximal envelope (design §2.3): four free-text
        # fields at the 1024-code-point cap with worst-case non-BMP
        # escaping; signal counts at the 1024-digit cap.
        big_text = '\U0001f600' * MAX_STRING_LENGTH
        points = 10 ** (MAX_INTEGER_DIGITS - 1)
        envelope = signal_envelope(points=3)
        envelope['software']['softlab_version'] = big_text
        envelope['software']['python_version'] = big_text
        envelope['source']['reference'] = big_text
        envelope['source']['digest_scope'] = big_text
        envelope['outcome']['requested_rows'] = points
        envelope['outcome']['completed_rows'] = points
        envelope['payload']['points'] = points
        text = encode_envelope(envelope)
        size = len(text.encode('ascii'))
        self.assertLessEqual(size, 66472)  # design §2.3 sweep ceiling
        self.assertLess(size, MAX_ENCODED_BYTES)  # ~1.97x headroom
        self.assertEqual(decode_envelope(text), envelope)

    def test_s4_3_array_object_string_reason_bounds(self):
        # Array 64/65 (layered at an extra root key).
        envelope = signal_envelope()
        envelope['extra'] = [0] * MAX_ARRAY_LENGTH
        self.assert_encode_rejects(envelope, "unknown field 'extra'")
        envelope = signal_envelope()
        envelope['extra'] = [0] * (MAX_ARRAY_LENGTH + 1)
        self.assert_encode_rejects(
            envelope, 'extra: array length exceeds limit 64')
        self.assert_frozen(
            lambda: decode_envelope(envelope_text(envelope)),
            RecordDecodeError, 'extra: array length exceeds limit 64')
        # Object entries 64/65.
        envelope = signal_envelope()
        envelope['extra'] = {f'k{i}': i for i in range(MAX_OBJECT_ENTRIES)}
        self.assert_encode_rejects(envelope, "unknown field 'extra'")
        envelope = signal_envelope()
        envelope['extra'] = {f'k{i}': i
                             for i in range(MAX_OBJECT_ENTRIES + 1)}
        self.assert_encode_rejects(
            envelope, 'extra: object entry count exceeds limit 64')
        # String 1024 accepted in a valid envelope; 1025 rejected.
        envelope = signal_envelope()
        envelope['source']['digest_scope'] = 'x' * MAX_STRING_LENGTH
        decoded = decode_envelope(encode_envelope(envelope))
        self.assertEqual(decoded, envelope)
        envelope = signal_envelope()
        envelope['source']['digest_scope'] = 'x' * (MAX_STRING_LENGTH + 1)
        self.assert_encode_rejects(
            envelope,
            'source.digest_scope: string length exceeds limit 1024')
        # Reason length 64/65 at the contract layer (both directions).
        envelope = signal_envelope()
        envelope['source']['git_revision']['reason'] = 'x' * \
            MAX_UNAVAILABLE_REASON_LENGTH
        self.assert_encode_rejects(
            envelope,
            'source.git_revision: reason outside bounded vocabulary')
        envelope = signal_envelope()
        envelope['source']['git_revision']['reason'] = 'x' * \
            (MAX_UNAVAILABLE_REASON_LENGTH + 1)
        self.assert_encode_rejects(
            envelope,
            'source.git_revision: reason length exceeds limit 64')
        self.assert_frozen(
            lambda: decode_envelope(envelope_text(envelope)),
            RecordValidationError,
            'source.git_revision: reason length exceeds limit 64')

    def test_s4_4_constant_sourced_limits(self):
        frozen = {
            'MAX_ENCODED_BYTES': 131072,
            'MAX_DEPTH': 8,
            'MAX_ARRAY_LENGTH': 64,
            'MAX_OBJECT_ENTRIES': 64,
            'MAX_STRING_LENGTH': 1024,
            'MAX_UNAVAILABLE_REASON_LENGTH': 64,
            'MAX_INTEGER_DIGITS': 1024,
        }
        self.assertEqual(tuple(frozen), _LIMIT_EXPORTS)
        for name, value in frozen.items():
            with self.subTest(name=name):
                self.assertEqual(getattr(run_record, name), value)
        # Module-only: never re-exported at package level (S6.3).
        package = importlib.import_module('softlab.shui')
        for name in _LIMIT_EXPORTS:
            self.assertFalse(hasattr(package, name), name)


class S5SideEffectFreeFailure(CodecAssertMixin, unittest.TestCase):
    """S5: failures raise visibly, leave inputs and shared state alone."""

    def test_s5_1_failed_decode_no_contamination(self):
        self.assert_frozen(lambda: decode_envelope('{bad'), RecordDecodeError,
                           'run_record: malformed JSON: Expecting property '
                           'name enclosed in double quotes: line 1 column 2 '
                           '(char 1)')
        envelope = signal_envelope()
        text = encode_envelope(envelope)
        self.assertEqual(decode_envelope(text), envelope)
        # Repeated failures then success on the shared module instance.
        for _ in range(3):
            with self.assertRaises(RecordDecodeError):
                decode_envelope('[]')
        self.assertEqual(decode_envelope(text), envelope)

    def test_s5_2_failed_encode_leaves_caller_input_untouched(self):
        # Contract-stage failure deep inside the client payload.
        envelope = thermal_envelope()
        envelope['payload']['configuration']['dt'] = 'not-a-number'
        snapshot = copy.deepcopy(envelope)
        with self.assertRaises(RecordValidationError):
            encode_envelope(envelope)
        self.assertEqual(envelope, snapshot)
        # Sweep-stage failure (unsupported value type) equally leaves the
        # caller's declarations untouched.
        envelope = thermal_envelope()
        envelope['payload']['configuration']['power'] = _UnstableValue()
        snapshot = copy.deepcopy(envelope)
        with self.assertRaises(RecordValidationError):
            encode_envelope(envelope)
        self.assertEqual(envelope, snapshot)
        # A subsequent valid encode on the shared instance still works.
        fresh = signal_envelope()
        self.assertEqual(decode_envelope(encode_envelope(fresh)), fresh)

    def test_s5_3_errors_raised_not_logged(self):
        records = []

        class _Handler(logging.Handler):
            def emit(self, record):
                records.append(record)

        handler = _Handler()
        root = logging.getLogger()
        root.addHandler(handler)
        try:
            with self.assertRaises(RecordValidationError):
                encode_envelope({'namespace': 'wrong'})
        finally:
            root.removeHandler(handler)
        self.assertEqual(records, [])

    def test_s5_4_no_write_before_validate_by_signature(self):
        allowed = {
            encode_envelope: ('envelope',),
            decode_envelope: ('text',),
            build_run_group: ('name', 'envelope'),
        }
        for func, names in allowed.items():
            parameters = inspect.signature(func).parameters
            self.assertEqual(tuple(parameters), names)
            for parameter in parameters.values():
                self.assertIs(parameter.kind,
                              inspect.Parameter.POSITIONAL_OR_KEYWORD)

    def test_s5_5_no_interpreter_guard_mutation(self):
        # The suite never calls sys.set_int_max_str_digits; the 1024-digit
        # acceptance below proves the codec bound — not interpreter state —
        # enforces the integer limit on this interpreter.
        envelope = signal_envelope()
        envelope['outcome']['requested_rows'] = 10 ** 1023
        envelope['outcome']['completed_rows'] = 10 ** 1023
        envelope['payload']['points'] = 10 ** 1023
        self.assertEqual(decode_envelope(encode_envelope(envelope)),
                         envelope)


class S6LegacyPreservation(CodecAssertMixin, unittest.TestCase):
    """S6: legacy v1/v2 readability, export surface, absence semantics."""

    def _load_example(self, name, path):
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module

    def _track_backends(self, module_io, created):
        original = module_io.get_data_backend

        def spy(backend_type, args=None, connect=True):
            backend = original(backend_type, args, connect)
            created.append(backend)
            return backend

        patcher = mock.patch.object(module_io, 'get_data_backend',
                                    side_effect=spy)
        patcher.start()
        self.addCleanup(patcher.stop)

        def disconnect_all():
            for backend in created:
                if backend.connected:
                    backend.disconnect()

        self.addCleanup(disconnect_all)

    def test_s6_2_legacy_v1_v2_fixtures_remain_readable(self):
        example = self._load_example(
            'thermal_m03_p2_schema',
            ROOT / 'examples' / 'thermal_experiment.py')
        from softlab.shui.data import io as data_io
        for adapter, version in (('coordinated', 1), ('direct', 2)):
            with self.subTest(adapter=adapter):
                created = []
                self._track_backends(data_io, created)
                with tempfile.TemporaryDirectory(
                        prefix='softlab-m03-p2-') as temp:
                    result = example.execute(
                        example.ThermalConfig(), Path(temp) / adapter,
                        adapter=adapter)
                    text = result.group.get_meta('thermal_manifest')
                    self.assertIsInstance(text, str)
                    manifest = example._decode_manifest(text)
                    self.assertEqual(manifest['schema'],
                                     'softlab.thermal-reference')
                    self.assertEqual(manifest['version'], version)
                    self.assertEqual(manifest['run_id'],
                                     str(result.group.id))
                    self.assertEqual(manifest['configuration']['points'], 8)
                    self.assertEqual(manifest['outcome']['completed_rows'],
                                     8)
                    self.assertEqual(
                        manifest['clock']['measurement_delay_ticks'],
                        1 if version == 1 else 0)

    def test_s6_3_export_surface_frozen(self):
        package = importlib.import_module('softlab.shui')
        # Exactly the frozen feature re-export list (design §8 table).
        for name in _FEATURE_EXPORTS:
            with self.subTest(name=name):
                self.assertTrue(hasattr(package, name), name)
        imported = {name for name in dir(package)
                    if name in run_record.__all__ or
                    name in run_group_module.__all__}
        self.assertEqual(imported, set(_FEATURE_EXPORTS))
        # The seven MAX_* limits are absent from the package level.
        for name in _LIMIT_EXPORTS:
            with self.subTest(absent=name):
                self.assertFalse(hasattr(package, name), name)
                with self.assertRaises(ImportError):
                    exec(f'from softlab.shui import {name}', {})
        # Everything else stays module-only.
        for name in _MODULE_ONLY_NAMES:
            self.assertTrue(hasattr(run_record, name), name)
            self.assertFalse(hasattr(package, name), name)
        # Pre-existing exports unchanged.
        for name in ('DatabaseBackend', 'catch_error', 'data', 'profile'):
            self.assertTrue(hasattr(package, name), name)

    def test_s6_5_legacy_absence_semantics(self):
        # A group with no additive key behaves exactly as before.
        group = DataGroup(name='legacy')
        self.assertIsNone(group.get_meta('run_record'))
        self.assertIsNone(group.get_meta('thermal_manifest'))
        self.assertEqual(group.meta, {})


class S7EnvironmentHygiene(CodecAssertMixin, unittest.TestCase):
    """S7: portability rules, warnings discipline, resource hygiene."""

    def test_s7_3_no_autospec_wraps_anywhere(self):
        # No mock.patch call site in this suite may use autospec or wraps
        # (the plan forbids autospec combined with wraps; the suite uses
        # plain side_effect patching and a hand-held recorder double).
        # AST call-site inspection ignores comments, docstrings and this
        # assertion's own spelling of the forbidden keywords.
        import ast
        tree = ast.parse(Path(__file__).read_text(encoding='utf-8'))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            target = node.func
            dotted = ''
            while isinstance(target, ast.Attribute):
                dotted = '.' + target.attr + dotted
                target = target.value
            if isinstance(target, ast.Name):
                dotted = target.id + dotted
            if 'mock.patch' not in dotted:
                continue
            keywords = {keyword.arg for keyword in node.keywords}
            self.assertNotIn('autospec', keywords, ast.dump(node))
            self.assertNotIn('wraps', keywords, ast.dump(node))

    def test_s7_3_leaf_error_classes_subclass_value_error(self):
        # isinstance(ValueError) is asserted only for the two leaf
        # classes; the base RunRecordError is asserted by exact type only.
        self.assertTrue(issubclass(RecordDecodeError, ValueError))
        self.assertTrue(issubclass(RecordValidationError, ValueError))
        self.assertTrue(issubclass(RecordDecodeError, RunRecordError))
        self.assertTrue(issubclass(RecordValidationError, RunRecordError))
        envelope = signal_envelope()
        envelope['version'] = 2
        with self.assertRaises(RecordValidationError) as caught:
            encode_envelope(envelope)
        self.assertIsInstance(caught.exception, ValueError)
        with self.assertRaises(RecordDecodeError) as caught:
            decode_envelope('{bad')
        self.assertIsInstance(caught.exception, ValueError)

    def test_s7_3_no_hardcoded_environment_facts(self):
        # Versions are captured at runtime, never asserted as literals.
        envelope = thermal_envelope()
        self.assertEqual(envelope['software']['softlab_version'],
                         str(softlab.__version__))
        self.assertEqual(envelope['software']['python_version'],
                         platform.python_version())

    def test_s7_5_no_resource_hygiene_violations(self):
        # The codec suite opens no schedulers/backends/VISA resources;
        # shared codec instances carry no cross-test state.
        codec_one = run_record.JsonRunRecordCodec()
        codec_two = run_record.JsonRunRecordCodec()
        envelope = signal_envelope()
        text = codec_one.encode(envelope)
        self.assertEqual(codec_two.decode(text), envelope)
        self.assertEqual(run_record.RunRecordCodec.__abstractmethods__,
                         frozenset({'encode', 'decode'}))


if __name__ == '__main__':
    unittest.main()
