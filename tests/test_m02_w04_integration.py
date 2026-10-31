"""Independent contracts for thermal integration with bounded execution."""
import contextlib
import io
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

from softlab.huo.process import run_count

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    'thermal_w04_independent', ROOT / 'examples' / 'thermal_experiment.py')
thermal = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = thermal
SPEC.loader.exec_module(thermal)


class ThermalIntegrationTests(unittest.TestCase):
    def test_both_shipped_modes_use_shared_runner(self):
        with tempfile.TemporaryDirectory() as root:
            for adapter in ('coordinated', 'direct'):
                with self.subTest(adapter=adapter), mock.patch.object(
                    thermal, 'run_count', wraps=run_count, create=True
                ) as runner:
                    result = thermal.execute(thermal.ThermalConfig(points=2),
                                             Path(root) / adapter, adapter=adapter)
                    self.assertTrue(result.overall_success, result.cause)
                    self.assertEqual(runner.call_count, 1)

    def test_setup_failures_report_no_unallocated_scheduler_cleanup(self):
        for stage in ('build', 'prepare', 'write'):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as root:
                cause = ValueError(stage)
                if stage == 'build':
                    patch = mock.patch.object(thermal, '_build_simulation', side_effect=cause)
                elif stage == 'prepare':
                    patch = mock.patch.object(thermal.Coordinator, 'prepare', side_effect=cause)
                else:
                    patch = mock.patch.object(thermal.MockControl, 'write', side_effect=cause)
                with patch:
                    result = thermal.execute(thermal.ThermalConfig(), Path(root) / 'run')
                self.assertIs(result.cause, cause)
                self.assertTrue(result.terminal)
                self.assertFalse(result.work_drained)
                self.assertFalse(result.loop_closed)
                self.assertFalse(result.data_path.exists())
                with self.assertRaises(RuntimeError):
                    thermal.reset_after_run(result)

    def test_callback_snapshot_and_stop_identity(self):
        with tempfile.TemporaryDirectory() as root:
            stop = thermal.StopRequest()
            snapshots = []
            def after(result):
                snapshots.append((result, result.stop, result.completed_rows,
                                  result.final_tick, result.stage,
                                  result.acquisition, result.terminal,
                                  result.work_drained, result.loop_closed))
                if result.completed_rows == 1:
                    stop.request()
            result = thermal.execute(thermal.ThermalConfig(points=3),
                                     Path(root) / 'run', stop=stop, after_row=after)
            self.assertEqual(len(snapshots), 1)
            actual, observed_stop, rows, tick, stage, acquisition, terminal, drained, closed = snapshots[0]
            self.assertIs(actual, result)
            self.assertIs(observed_stop, stop)
            self.assertEqual((rows, tick, stage, acquisition), (1, 1, 'setup', 'not_started'))
            self.assertFalse(terminal or drained or closed)
            self.assertEqual(result.acquisition, 'cancelled')
            self.assertTrue(stop.acknowledged)
            self.assertEqual(result.completed_rows, 1)

    def test_runner_preflight_failure_has_no_claimed_cleanup(self):
        cause = ValueError('runner preflight')
        with tempfile.TemporaryDirectory() as root, mock.patch.object(
            thermal, 'run_count', side_effect=cause, create=True
        ):
            result = thermal.execute(thermal.ThermalConfig(), Path(root) / 'run')
        self.assertIs(result.cause, cause)
        self.assertEqual(result.acquisition, 'failed')
        self.assertFalse(result.work_drained)
        self.assertFalse(result.loop_closed)
        self.assertFalse(result.data_path.exists())



    def test_both_modes_numeric_timing_manifest_and_fresh_repeat(self):
        with tempfile.TemporaryDirectory() as root:
            for adapter, delay in (('coordinated', 1), ('direct', 0)):
                results = [thermal.execute(thermal.ThermalConfig(points=3),
                           Path(root) / f'{adapter}-{i}', adapter=adapter)
                           for i in range(2)]
                for result in results:
                    self.assertTrue(result.overall_success, result.cause)
                    table = result.loaded.record('thermal_trace').table
                    self.assertEqual(list(table['tick']), [1, 2, 3])
                    self.assertEqual(list(table['measurement_source_tick']),
                                     [1-delay, 2-delay, 3-delay])
                    for tick, temperature in zip(range(1-delay, 4-delay),
                                                 table['temperature_degc']):
                        self.assertAlmostEqual(temperature, 30 - 10 * 0.9**tick)
                    manifest = thermal._decode_manifest(
                        result.loaded.get_meta('thermal_manifest'))
                    self.assertEqual(manifest['version'], 1 if delay else 2)
                    self.assertEqual(manifest['run_id'], str(result.group.id))
                self.assertNotEqual(results[0].group.id, results[1].group.id)
                a = results[0].loaded.record('thermal_trace').table
                b = results[1].loaded.record('thermal_trace').table
                for column in a.columns:
                    if column != 'timestamp':
                        self.assertEqual(list(a[column]), list(b[column]))

    def test_both_modes_read_failure_after_advance_keeps_partial_rows(self):
        for adapter in ('coordinated', 'direct'):
            with self.subTest(adapter=adapter), tempfile.TemporaryDirectory() as root:
                cause = ValueError('reading failure')
                cls = thermal.MockMeasurement if adapter == 'coordinated' else thermal._DirectMeasurement
                original = cls.read
                calls = []
                def read(sensor):
                    calls.append('read')
                    if len(calls) == 2:
                        raise cause
                    return original(sensor)
                with mock.patch.object(cls, 'read', read):
                    result = thermal.execute(thermal.ThermalConfig(points=3),
                                             Path(root) / 'run', adapter=adapter)
                self.assertIs(result.cause, cause)
                self.assertEqual(result.acquisition, 'failed')
                self.assertEqual((result.completed_rows, result.final_tick), (1, 2))
                self.assertEqual(len(result.group.record('thermal_trace')), 1)
                self.assertEqual(len(calls), 2)
                self.assertTrue(result.work_drained and result.loop_closed)
                self.assertFalse(result.data_path.exists())

    def test_runner_cleanup_failure_keeps_completion_and_skips_storage(self):
        from softlab.huo.scheduler import SerialScheduler
        cause = RuntimeError('owned stop failure')
        with tempfile.TemporaryDirectory() as root, mock.patch.object(
            SerialScheduler, 'stop', side_effect=cause
        ):
            result = thermal.execute(thermal.ThermalConfig(points=2), Path(root) / 'run')
        self.assertEqual(result.acquisition, 'completed')
        self.assertFalse(result.overall_success)
        self.assertTrue(any('owned stop failure' in error for error in result.cleanup_errors))
        self.assertTrue(result.work_drained and result.loop_closed)
        self.assertFalse(result.data_path.exists())

    def test_interruption_from_callback_keeps_identity_and_stops(self):
        interruption = KeyboardInterrupt('callback stop')
        calls = []
        def after(result):
            calls.append((result.completed_rows, result.final_tick))
            raise interruption
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(KeyboardInterrupt) as caught:
                thermal.execute(thermal.ThermalConfig(points=3), Path(root) / 'run',
                                after_row=after)
            self.assertIs(caught.exception, interruption)
            self.assertEqual(calls, [(1, 1)])
            self.assertFalse((Path(root) / 'run' / 'thermal.h5').exists())

    def test_acquisition_primary_survives_cleanup_interruption(self):
        from softlab.huo.scheduler import SerialScheduler
        primary = ValueError('sample failure')
        interruption = KeyboardInterrupt('stop interruption')
        stderr = io.StringIO()
        def read(_):
            raise primary
        with tempfile.TemporaryDirectory() as root, \
             mock.patch.object(thermal.MockMeasurement, 'read', read), \
             mock.patch.object(SerialScheduler, 'stop', side_effect=interruption), \
             contextlib.redirect_stderr(stderr):
            with self.assertRaises(KeyboardInterrupt) as caught:
                thermal.execute(thermal.ThermalConfig(points=2), Path(root) / 'run')
        self.assertIs(caught.exception, interruption)
        self.assertIs(interruption.count_result.cause, primary)
        self.assertIn('sample failure', stderr.getvalue())

    def test_broken_stderr_cannot_mask_interruption(self):
        interruption = KeyboardInterrupt('original')
        class BrokenStderr:
            def write(self, text):
                raise OSError('stderr unavailable')
            def flush(self):
                pass
        def interrupt(_):
            raise interruption
        with tempfile.TemporaryDirectory() as root, contextlib.redirect_stderr(BrokenStderr()):
            with self.assertRaises(KeyboardInterrupt) as caught:
                thermal.execute(thermal.ThermalConfig(points=2), Path(root) / 'run',
                                after_row=interrupt)
        self.assertIs(caught.exception, interruption)

    def test_writer_cleanup_failure_does_not_mask_persistence_interruption(self):
        interruption = KeyboardInterrupt('save interrupted')
        calls = []
        class Writer:
            file = object()
            connected = True
            errors = []
            def connect(self, config):
                calls.append('connect')
                return True
            def save_group(self, group):
                calls.append('save')
                raise interruption
        def disconnect(writer):
            calls.append('disconnect')
            raise RuntimeError('writer cleanup fault')
        stderr = io.StringIO()
        with tempfile.TemporaryDirectory() as root, \
             mock.patch.object(thermal, 'HDF5DataBackend', Writer), \
             mock.patch.object(thermal, '_disconnect', side_effect=disconnect), \
             contextlib.redirect_stderr(stderr):
            with self.assertRaises(KeyboardInterrupt) as caught:
                thermal.execute(thermal.ThermalConfig(points=1), Path(root) / 'run')
        self.assertIs(caught.exception, interruption)
        self.assertEqual(calls, ['connect', 'save', 'disconnect'])
        self.assertIn('writer cleanup fault', stderr.getvalue())

    def test_last_observed_boundary_is_not_overwritten_by_teardown(self):
        from softlab.huo.process import CountResult
        cause = ValueError('boundary unavailable after advance')
        def fake_runner(process, total, rows, boundary, *, stop, after_row):
            snapshot = CountResult(total, stop)
            snapshot.acquisition = 'failed'
            snapshot.cause = cause
            snapshot.boundary = (7, 7.0)
            snapshot.boundary_observed = True
            snapshot.work_drained = snapshot.loop_closed = snapshot.terminal = True
            return snapshot
        with tempfile.TemporaryDirectory() as root, \
             mock.patch.object(thermal, 'run_count', side_effect=fake_runner):
            result = thermal.execute(thermal.ThermalConfig(points=2), Path(root) / 'run')
        self.assertIs(result.cause, cause)
        self.assertEqual((result.final_tick, result.final_sim_time), (7, 7.0))

    def test_row_inspection_interruption_does_not_mask_earlier_interrupt(self):
        first = KeyboardInterrupt('first interruption')
        second = SystemExit('inspection interruption')
        captured = []
        def after(result):
            captured.append(result)
            raise first
        original = thermal.DataGroup.record
        def record(group, name):
            if captured and group is captured[0].group:
                raise second
            return original(group, name)
        stderr = io.StringIO()
        with tempfile.TemporaryDirectory() as root, \
             mock.patch.object(thermal.DataGroup, 'record', record), \
             contextlib.redirect_stderr(stderr):
            with self.assertRaises(KeyboardInterrupt) as caught:
                thermal.execute(thermal.ThermalConfig(points=2), Path(root) / 'run',
                                after_row=after)
        self.assertIs(caught.exception, first)
        self.assertTrue(any('inspection interruption' in error
                            for error in captured[0].cleanup_errors))
        self.assertTrue(captured[0].terminal)

if __name__ == '__main__':
    unittest.main()
