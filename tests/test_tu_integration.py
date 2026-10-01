"""Integration acceptance cases for TU-008 (release integration).

Verifies the combined usage of the TU-002--TU-007 extensions exactly as a
library user would adopt them, plus the documentation artifacts the PRD
requires. Production code under ``softlab/tu`` is exercised read-only;
``huo`` production code is never touched --- count/scan run through their
public ``softlab.huo.process`` entry points against a virtual device and
plain parameters.

Case map (see ``log/release_1/tests/TU-008.md`` for the gate record):

1. ``test_new_api_public_imports_and_method_surface`` --- every new public
   API is importable from its documented path and exposed on the classes
   through the ``softlab.tu`` re-export namespace.
2. ``test_count_against_described_prepared_virtual_device`` ---
   representative ``huo`` count flow against a described, prepared virtual
   device: description causes no acquisition, preparation gates usage,
   the recorded values stay legacy value-only.
3. ``test_scan_records_settings_beside_reading_contract`` ---
   representative ``huo`` scan flow recording setters and getters while
   the new ``read()``/``describe_reading()`` measurement contract is
   exercised on the same parameter beside the legacy flow.
4. ``test_device_lifecycle_journey_through_huo_run`` --- full resource
   owner journey: capability detection, prepare, run, cleanup (owned
   resource released), repeated cleanup safe.
5. ``test_numerical_model_round_trip_through_new_contracts`` --- a
   numerical model round trip: describe / supported_configuration /
   configuration (JSON-safe) / configure / evaluate_features, with the
   legacy ``features`` path unchanged beside the strict path.
6. ``test_migration_free_legacy_usage_unchanged`` --- a legacy-style user
   who never calls any new API keeps working unchanged (value-only reads,
   legacy model features, original import paths).
7. ``test_documentation_artifacts_exist_and_cover_added_apis`` ---
   documentation gate (expected RED at case definition): the user-facing
   document of added APIs and limitations exists and names every new API
   surface plus a migration-free usage statement.

Recorded manual gate procedures (zero-warning compile, full suite under
``-W error``, package-artifact inspection) are defined in
``log/release_1/tests/TU-008.md`` and are intentionally not automated
here. All fixtures are synthetic; no real hardware, no new dependencies.
"""

import json
import unittest
from pathlib import Path

from softlab.huo.process import (
    count,
    run_process,
    scan,
)
from softlab.huo.scheduler import get_scheduler
from softlab.jin.validator import (
    ValInt,
    ValNumber,
)
from softlab.tu.station import (
    Device,
    Parameter,
    Reading,
    Station,
    VisaCommand,
    VisaHandle,
    default_station,
    get_device_builder,
    register_device_builder,
    set_default_station,
)
from softlab.tu.station.visa import AbortReport
from softlab.tu.theory import (
    Mapping,
    TheoryModel,
    batch_mapping,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS_PATH = REPO_ROOT / 'docs' / 'tu_extensions.md'


class VirtualDevice(Device):
    """Synthetic virtual device with one numeric parameter."""

    def __init__(self, name: str = 'virt'):
        super().__init__(name)
        self._signal = Parameter('signal', ValNumber())
        self._signal(1.0)
        self.add_parameter(self._signal)


def _make_model() -> TheoryModel:
    """Fixture model with validated, described attributes."""
    class Model(TheoryModel):
        def __init__(self, name=None):
            super().__init__(name)
            self.add_attribute(
                'value', ValInt(0, 10), 2, description='seed value')
            self.add_attribute('gain', ValNumber(0.0), 1.5)

        def calculate_features(self):
            return {'double': self.value() * 2}

    return Model('model')


class PublicImportTests(unittest.TestCase):
    """Case 1: every new API importable from its documented path."""

    def test_new_api_public_imports_and_method_surface(self):
        # Class-level surface through the documented public paths
        for cls, names in (
            (Parameter, ('describe', 'describe_reading', 'read')),
            (Device, ('describe', 'prepare', 'cleanup', 'initialized',
                      'supports')),
            (Station, ('describe',)),
            (VisaHandle, ('initialized', 'supports', 'prepare', 'cleanup',
                          'timeout_seconds', 'abort', 'confirm_stop')),
            (VisaCommand, ('execute', 'describe_operation')),
            (TheoryModel, ('describe', 'supported_configuration',
                           'configuration', 'configure',
                           'evaluate_features')),
        ):
            for name in names:
                # properties are intentionally not callable: presence in
                # the class dict (not the Delegated fallback) is the check
                self.assertIn(name, cls.__dict__,
                              f'{cls.__name__}.{name} missing')

        # Value-level public names keep their documented identity
        self.assertIs(Reading, Parameter('p', ValNumber()).read().__class__)
        self.assertTrue(isinstance(AbortReport._fields, tuple))

        # Re-export namespace: softlab.tu.station / softlab.tu.theory
        import softlab.tu.station as station_mod
        import softlab.tu.theory as theory_mod
        self.assertIs(station_mod.Parameter, Parameter)
        self.assertIs(station_mod.Device, Device)
        self.assertIs(station_mod.Station, Station)
        self.assertIs(station_mod.VisaHandle, VisaHandle)
        self.assertIs(station_mod.VisaCommand, VisaCommand)
        self.assertIs(theory_mod.TheoryModel, TheoryModel)
        self.assertIs(theory_mod.Mapping, Mapping)
        self.assertIs(theory_mod.batch_mapping, batch_mapping)

        # Station builder helpers remain importable (legacy surface)
        self.assertTrue(callable(register_device_builder))
        self.assertTrue(callable(get_device_builder))
        self.assertTrue(callable(set_default_station))
        self.assertTrue(callable(default_station))


class CombinedUsageTests(unittest.TestCase):
    """Cases 2-4: huo count/scan against new-API device usage."""

    def setUp(self):
        self._scheduler = get_scheduler()
        self._scheduler.start()

    def tearDown(self):
        if self._scheduler.is_running:
            self._scheduler.stop()

    def test_count_against_described_prepared_virtual_device(self):
        device = VirtualDevice('meter')
        station = Station('bench')
        station.add_device(device)

        # Description is inspection-only: no acquisition happens
        gets = []
        device.signal._before_get = lambda value: gets.append(value) or value
        description = station.describe()
        self.assertEqual(description['schema_version'], 1)
        self.assertIn('meter', description['devices'])
        self.assertEqual(gets, [])

        # Unsupported capability detectable; preparation gates usage
        self.assertFalse(device.supports('read'))
        self.assertFalse(device.initialized)
        device.prepare()
        self.assertTrue(device.initialized)

        # Representative legacy count flow against the prepared device
        proc = count('acquire', None, None, device.signal, times=3)
        success, _ = run_process(proc, self._scheduler, verbose=False)
        self.assertTrue(success)
        table = proc.record.table
        self.assertEqual(len(table), 3)
        self.assertEqual(table['signal'].tolist(), [1.0, 1.0, 1.0])
        # Legacy value-only semantics: raw floats, not Reading objects
        for value in table['signal']:
            self.assertIsInstance(float(value), float)
        self.assertEqual(len(gets), 3)  # one acquisition per count

        # Release the owned resource afterwards
        device.cleanup()
        self.assertFalse(device.initialized)

    def test_scan_records_settings_beside_reading_contract(self):
        device = VirtualDevice('meter')
        device.prepare()
        setter = Parameter('frequency', ValNumber())
        setter(0.0)

        # Representative legacy scan flow (setter swept, getter recorded)
        proc = scan('sweep', [device.signal], None, None,
                    setter, [10.0, 20.0, 30.0])
        success, _ = run_process(proc, self._scheduler, verbose=False)
        self.assertTrue(success)
        table = proc.record.table
        self.assertEqual(table['frequency'].tolist(), [10.0, 20.0, 30.0])
        self.assertEqual(table['signal'].tolist(), [1.0, 1.0, 1.0])
        self.assertEqual(setter(), 30.0)

        # New measurement contract on the same parameter, no huo change
        reading = device.signal.read()
        self.assertIsInstance(reading, Reading)
        self.assertEqual(reading.value, 1.0)
        self.assertEqual(reading.quality, 'ok')
        self.assertIsNone(reading.error)
        self.assertIsNotNone(reading.acquired_at)
        described = device.signal.describe_reading()
        self.assertEqual(described['schema_version'], 1)
        device.cleanup()

    def test_device_lifecycle_journey_through_huo_run(self):
        device = VirtualDevice('meter')

        # Capability detection before any preparation
        self.assertFalse(device.initialized)
        device.prepare()
        self.assertTrue(device.initialized)
        # Repeated preparation is safe
        device.prepare()
        self.assertTrue(device.initialized)

        proc = count('run', None, None, device.signal, times=2)
        success, _ = run_process(proc, self._scheduler, verbose=False)
        self.assertTrue(success)

        # Owned resource released; repeated cleanup is safe
        device.cleanup()
        self.assertFalse(device.initialized)
        device.cleanup()
        self.assertFalse(device.initialized)


class ModelRoundTripTests(unittest.TestCase):
    """Case 5: numerical model round trip through new theory contracts."""

    def test_numerical_model_round_trip_through_new_contracts(self):
        model = _make_model()

        # Identity and semantic description (no evaluation side effects)
        description = model.describe()
        self.assertEqual(description['schema_version'], 1)
        self.assertEqual(description['name'], 'model')
        self.assertEqual(
            description['attributes']['value']['description'],
            'seed value')
        self.assertEqual(
            description['attributes']['gain']['description'], '')

        # Supported serializable configuration, JSON-safe round trip
        supported = model.supported_configuration()
        self.assertEqual(set(supported), {'value', 'gain'})
        initial = model.configuration()
        json.dumps(initial)  # must not raise
        self.assertEqual(initial, {'value': 2, 'gain': 1.5})

        model.configure({'value': 4, 'gain': 2.5})
        self.assertEqual(model.configuration(),
                         {'value': 4, 'gain': 2.5})
        # Single source of truth: delegated access sees the same values
        self.assertEqual(model.value(), 4)
        self.assertEqual(model.gain(), 2.5)

        # Evaluation: strict and lenient paths, legacy features unchanged
        self.assertEqual(model.evaluate_features(strict=True),
                         {'double': 8})
        self.assertEqual(model.evaluate_features(), {'double': 8})
        self.assertEqual(model.features, {'double': 8})
        with self.assertRaises(ValueError):
            model.configure({'value': 99})
        self.assertEqual(model.configuration()['value'], 4)


class MigrationFreeTests(unittest.TestCase):
    """Case 6: legacy usage without any new API call keeps working."""

    def test_migration_free_legacy_usage_unchanged(self):
        # Plain parameters: set/get/call exactly as before the release
        freq = Parameter('freq', ValNumber())
        freq(6.0)
        self.assertEqual(freq(), 6.0)
        self.assertEqual(freq.get(), 6.0)

        # Plain device and station usage through original paths
        device = Device('legacy')
        device.add_parameter(freq)
        station = Station('legacy-bench')
        station.add_device(device)
        self.assertIs(station.device('legacy'), device)
        self.assertIn('freq', device.snapshot()['parameters'])

        # Legacy count flow: value-only results, no Reading involvement
        scheduler = get_scheduler()
        scheduler.start()
        try:
            proc = count('legacy-count', None, None, freq, times=2)
            success, _ = run_process(proc, scheduler, verbose=False)
            self.assertTrue(success)
            values = proc.record.table['freq'].tolist()
            self.assertEqual(values, [6.0, 6.0])
            self.assertTrue(all(isinstance(v, float) for v in values))
        finally:
            if scheduler.is_running:
                scheduler.stop()

        # Legacy model features path with the characterized fallback
        model = _make_model()
        self.assertEqual(model.features, {'double': 4})
        with patch_model_failure(model):
            self.assertEqual(model.features, {})


def patch_model_failure(model):
    """Context: make ``calculate_features`` fail for the legacy path."""
    from unittest.mock import patch
    return patch.object(type(model), 'calculate_features',
                        side_effect=RuntimeError('boom'))


class DocumentationTests(unittest.TestCase):
    """Case 7 (expected RED): documentation artifacts required by PRD."""

    REQUIRED_TOKENS = (
        'describe', 'prepare', 'cleanup', 'initialized', 'supports',
        'read', 'describe_reading', 'execute', 'describe_operation',
        'timeout_seconds', 'abort', 'confirm_stop',
        'evaluate_features', 'configure', 'supported_configuration',
        'migration',
    )

    def test_documentation_artifacts_exist_and_cover_added_apis(self):
        self.assertTrue(
            DOCS_PATH.is_file(),
            f'{DOCS_PATH} missing: user-facing documentation of the '
            'added APIs and limitations is a TU-008 acceptance artifact')
        content = DOCS_PATH.read_text(encoding='utf-8').lower()
        for token in self.REQUIRED_TOKENS:
            self.assertIn(token, content,
                          f'documentation must mention {token!r}')
        self.assertIn('limitation', content,
                      'documented limitations are required by the PRD')


if __name__ == '__main__':
    unittest.main()
