"""M01-W04 alternate thermal adapter observable contracts."""

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    'thermal_adapter_contract', ROOT / 'examples' / 'thermal_experiment.py')
thermal = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = thermal
SPEC.loader.exec_module(thermal)


class AdapterContractTests(unittest.TestCase):
    def test_both_observation_boundaries_and_persisted_context(self):
        with tempfile.TemporaryDirectory() as directory:
            records = []
            for adapter, delay in (('coordinated', 1), ('direct', 0)):
                result = thermal.execute(thermal.ThermalConfig(),
                                         Path(directory) / adapter,
                                         adapter=adapter)
                self.assertTrue(result.overall_success, result.cause)
                self.assertTrue(result.work_drained and result.loop_closed)
                self.assertTrue(result.data_path.is_file())
                self.assertTrue(result.plot_path.is_file())
                record = result.loaded.record('thermal_trace')
                records.append(record)
                table = record.table
                self.assertEqual(len(table), 8)
                np.testing.assert_array_equal(table['tick'], range(1, 9))
                np.testing.assert_array_equal(table['measurement_source_tick'],
                                              range(1 - delay, 9 - delay))
                np.testing.assert_allclose(table['temperature_degc'],
                    [30 - 10 * 0.9 ** j for j in range(1 - delay, 9 - delay)],
                    rtol=0, atol=1e-10)
                timing = json.loads(result.loaded.get_meta('timing'))
                self.assertEqual(timing['measurement_delay_ticks'], delay)
                manifest = thermal._decode_manifest(
                    result.loaded.get_meta('thermal_manifest'))
                self.assertEqual(manifest['clock']['measurement_delay_ticks'], delay)
                self.assertEqual(manifest['version'], 1 if delay else 2)
                reopened = thermal._reload_group(result.data_path, str(result.group.id))
                self.assertEqual(reopened.get_meta('thermal_manifest'),
                                 result.loaded.get_meta('thermal_manifest'))
            self.assertEqual(records[0].columns, records[1].columns)

    def test_direct_stop_retains_completed_rows_and_closes_resources(self):
        with tempfile.TemporaryDirectory() as directory:
            stop = thermal.StopRequest()
            result = thermal.execute(thermal.ThermalConfig(), Path(directory) / 'run',
                adapter='direct', stop=stop,
                after_row=lambda r: stop.request() if r.completed_rows == 3 else None)
            self.assertEqual(result.acquisition, 'cancelled')
            self.assertEqual(result.completed_rows, 3)
            self.assertEqual(result.final_tick, 3)
            self.assertTrue(stop.acknowledged)
            self.assertFalse(result.overall_success)
            self.assertTrue(result.work_drained and result.loop_closed)
            self.assertFalse(result.data_path.exists())

    def test_direct_adapter_order_and_read_failure(self):
        events = []
        original_write = thermal._ControlAdapter.write
        original_control_read = thermal._ControlAdapter.read
        original_measure_read = thermal._DirectMeasurement.read
        error = ValueError('sensor fault')

        def write(control, value):
            events.append(('write', value))
            return original_write(control, value)

        def control_read(control):
            events.append(('control_read',))
            return original_control_read(control)

        def measure_read(sensor):
            events.append(('measure_read',))
            if sum(event[0] == 'measure_read' for event in events) == 4:
                raise error
            return original_measure_read(sensor)

        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(thermal._ControlAdapter, 'write', write), \
                mock.patch.object(thermal._ControlAdapter, 'read', control_read), \
                mock.patch.object(thermal._DirectMeasurement, 'read', measure_read):
            result = thermal.execute(thermal.ThermalConfig(), Path(directory) / 'run',
                                     adapter='direct')
            self.assertEqual(result.acquisition, 'failed')
            self.assertIs(result.cause, error)
            self.assertEqual(result.completed_rows, 3)
            self.assertEqual(result.final_tick, 4)
            self.assertFalse(result.overall_success)
            self.assertTrue(result.work_drained and result.loop_closed)
            self.assertEqual(events, [('write', 10.0)] +
                [('control_read',), ('measure_read',)] * 4)

    def test_direct_control_write_failure_does_not_create_rows(self):
        error = ValueError('control fault')
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(thermal._ControlAdapter, 'write', side_effect=error), \
                mock.patch.object(thermal._DirectMeasurement, 'read') as read:
            result = thermal.execute(thermal.ThermalConfig(), Path(directory) / 'run',
                                     adapter='direct')
            self.assertEqual(result.acquisition, 'failed')
            self.assertIs(result.cause, error)
            self.assertEqual(result.completed_rows, 0)
            self.assertEqual(result.final_tick, 0)
            self.assertFalse(result.overall_success)
            self.assertTrue(result.work_drained and result.loop_closed)
            read.assert_not_called()

    def test_direct_manifest_rejects_mixed_version_and_timing(self):
        with tempfile.TemporaryDirectory() as directory:
            result = thermal.execute(thermal.ThermalConfig(), Path(directory) / 'run',
                                     adapter='direct')
            self.assertTrue(result.overall_success, result.cause)
            manifest = thermal._decode_manifest(
                result.loaded.get_meta('thermal_manifest'))
            for change in ('version', 'delay', 'source', 'unsupported'):
                altered = json.loads(json.dumps(manifest))
                if change == 'version':
                    altered['version'] = 1
                elif change == 'delay':
                    altered['clock']['measurement_delay_ticks'] = 1
                elif change == 'source':
                    altered['clock']['source_tick'] = 'tick-1'
                else:
                    altered['version'] = 3
                with self.subTest(change=change), self.assertRaises(ValueError):
                    thermal._check_manifest(altered)
            result.loaded.set_meta('timing', json.dumps(thermal._timing(1)))
            with self.assertRaises(ValueError):
                thermal._validate_manifest_data(manifest, result.loaded)

    def test_unknown_adapter_rejected_before_output(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / 'run'
            with self.assertRaises(ValueError):
                thermal.execute(thermal.ThermalConfig(), destination, adapter='unknown')
            self.assertFalse(destination.exists())


if __name__ == '__main__':
    unittest.main()
