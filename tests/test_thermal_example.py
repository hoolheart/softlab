"""M01-W01 thermal reference acceptance tests; synthetic, headless fixtures."""

import asyncio
import contextlib
import dataclasses
from decimal import Decimal
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import h5py
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.figure import Figure

from softlab.huo.scheduler import Action, Scheduler
from softlab.shui.data import HDF5DataBackend
from softlab.tu.simulation import Coordinator

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / 'examples' / 'thermal_experiment.py'
SPEC = importlib.util.spec_from_file_location('thermal_acceptance_example', EXAMPLE)
thermal = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = thermal
SPEC.loader.exec_module(thermal)

COLUMNS = [
    ('commanded_power_w', 'Commanded heater power', 'W', False),
    ('measurement_source_tick', 'Measurement source boundary', '1', False),
    ('sim_time_s', 'Simulation acquisition time', 's', False),
    ('temperature_degc', 'Delivered temperature', 'degC', True),
    ('tick', 'Acquisition boundary', '1', False),
    ('timestamp', 'Wall-clock acquisition timestamp', 's', False),
]
TEMPERATURES = [20, 21, 21.9, 22.71, 23.439, 24.0951,
                24.68559, 25.217031]


def schema(record):
    """Return ordered public column definitions."""
    return [tuple(col[k] for k in ('name', 'label', 'unit', 'dependent'))
            for col in record.columns]


def digest(path):
    """Hash a closed delivered artifact."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


class ThermalAcceptanceTests(unittest.TestCase):
    """Assert observable outcomes rather than the callback implementation."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = thermal.ThermalConfig()
        self.figures = set(plt.get_fignums())
        self.addCleanup(self.close_test_figures)

    def close_test_figures(self):
        for number in set(plt.get_fignums()) - self.figures:
            plt.close(number)

    def run_fixture(self, name='run'):
        return thermal.run(self.config, self.root / name)

    def assert_default(self, group):
        self.assertEqual(group.name, 'thermal_experiment')
        self.assertEqual(list(group.records), ['thermal_trace'])
        record = group.record('thermal_trace')
        self.assertEqual(schema(record), COLUMNS)
        self.assertEqual(len(record), 8)
        self.assertTrue(np.isfinite(record.table.to_numpy(dtype=float)).all())
        for field, expected in {
            'tick': list(range(1, 9)),
            'sim_time_s': list(range(1, 9)),
            'measurement_source_tick': list(range(8)),
            'commanded_power_w': [10] * 8,
            'temperature_degc': TEMPERATURES,
        }.items():
            np.testing.assert_allclose(record.table[field], expected,
                                       rtol=0, atol=1e-10)
        independent = [float(Decimal(30) - Decimal(10) *
                             Decimal('0.9') ** j) for j in range(8)]
        np.testing.assert_allclose(record.table['temperature_degc'], independent,
                                   rtol=0, atol=1e-10)
        self.assertEqual(group.get_meta('example'), 'thermal_experiment')
        self.assertEqual(json.loads(group.get_meta('configuration')),
                         dataclasses.asdict(self.config))
        self.assertEqual(json.loads(group.get_meta('timing')), {
            'acquisition': 'post_step', 'measurement_delay_ticks': 1,
            'source_tick': 'tick-1', 'wall_timestamp': 'unix_seconds',
        })
        return record

    def test_default_roundtrip_plot_and_exact_boundaries(self):
        instances = []
        real_build = thermal._build_simulation
        real_step = Coordinator.step
        boundaries = []

        def build(config):
            result = real_build(config)
            instances.append(result)
            return result

        def step(coordinator):
            real_step(coordinator)
            boundaries.append((coordinator.tick, coordinator.sim_time))

        with mock.patch.object(thermal, '_build_simulation', side_effect=build), \
                mock.patch.object(Coordinator, 'step', autospec=True,
                                  side_effect=step), \
                mock.patch.object(Coordinator, 'reset', autospec=True) as reset:
            group = self.run_fixture()
        self.assert_default(group)
        self.assertEqual(boundaries, [(k, float(k)) for k in range(1, 9)])
        reset.assert_not_called()
        coordinator, heater, probe = instances[0]
        self.assertEqual(coordinator.tick, 8)
        self.assertEqual(heater.read(), 10.0)
        self.assertAlmostEqual(probe.read(), TEMPERATURES[-1], places=10)
        self.assertEqual(coordinator.tick, 8)  # reads do not tick
        png = self.root / 'run' / 'thermal.png'
        image = plt.imread(png)
        self.assertGreater(image.shape[0], 0)
        self.assertGreater(float(image.max() - image.min()), 0)
        self.assertEqual(set(plt.get_fignums()), self.figures)

    def test_invalid_config_precedes_resources_and_outputs(self):
        invalid = []
        for field in ('initial_temperature', 'ambient_temperature',
                      'heat_capacity', 'conductance', 'dt', 'power'):
            invalid.extend((field, value) for value in
                           (float('nan'), float('inf'), -float('inf'), True, 'x'))
        invalid.extend((field, value) for field in
                       ('heat_capacity', 'conductance', 'dt')
                       for value in (0.0, -1.0))
        invalid.extend(('points', value) for value in (0, -1, True, False, 1.5, '8'))
        invalid.append(('dt', 11.0))
        for index, (field, value) in enumerate(invalid):
            with self.subTest(field=field, value=value), \
                    mock.patch.object(thermal, '_build_simulation') as build, \
                    mock.patch.object(thermal, '_make_scheduler') as scheduler:
                target = self.root / str(index)
                config = dataclasses.replace(self.config, **{field: value})
                with self.assertRaises((TypeError, ValueError, RuntimeError)):
                    thermal.run(config, target)
                build.assert_not_called()
                scheduler.assert_not_called()
                self.assertFalse((target / 'thermal.h5').exists())
                self.assertFalse((target / 'thermal.png').exists())

    def test_valid_boundary_config(self):
        config = dataclasses.replace(self.config, points=1, dt=10.0,
                                     initial_temperature=-5.0,
                                     ambient_temperature=-5.0, power=-1.0)
        group = thermal.run(config, self.root / 'boundary')
        record = group.record('thermal_trace')
        self.assertEqual(len(record), 1)
        self.assertEqual(record.table['temperature_degc'].iloc[0], -5.0)
        self.assertEqual(record.table['sim_time_s'].iloc[0], 10.0)

    def test_collision_and_symlink_preserve_existing_bytes(self):
        target = self.root / 'existing'
        target.mkdir()
        sentinel = target / 'thermal.h5'
        sentinel.write_bytes(b'existing experiment')
        link = self.root / 'link'
        link.symlink_to(target, target_is_directory=True)
        ordinary_file = self.root / 'file'
        ordinary_file.write_bytes(b'existing file')
        for location in (target, link, ordinary_file):
            with self.subTest(location=location), \
                    mock.patch.object(thermal, '_build_simulation') as build:
                with self.assertRaises((ValueError, FileExistsError, RuntimeError)):
                    thermal.run(self.config, location)
                build.assert_not_called()
        self.assertEqual(sentinel.read_bytes(), b'existing experiment')
        self.assertEqual(ordinary_file.read_bytes(), b'existing file')

    def test_source_integrity_successive_reload_and_repeat_isolation(self):
        first = self.run_fixture('first')
        second = self.run_fixture('second')
        self.assertNotEqual(first.id, second.id)
        self.assert_default(first)
        self.assert_default(second)
        for name, original in (('first', first), ('second', second)):
            path = self.root / name / 'thermal.h5'
            before = digest(path)
            with h5py.File(path, 'r') as disk:
                attrs = dict(disk[str(original.id)].attrs)
                self.assertEqual(attrs['id'], str(original.id))
                self.assertEqual(attrs['name'], original.name)
                for key in ('timestamp', 'backend', 'configuration', 'timing'):
                    self.assertIn(key, attrs)
                np.testing.assert_allclose(
                    disk[str(original.id)]['thermal_trace']['columns']
                    ['temperature_degc'][:, 0], TEMPERATURES, rtol=0, atol=1e-10)
            for _ in range(2):
                loaded = thermal._reload_group(path, str(original.id))
                self.assert_default(loaded)
                self.assertEqual(loaded.id, original.id)
                self.assertEqual(loaded.timestamp, original.timestamp)
                self.assertEqual(Path(loaded.backend['arguments']['path']), path)
                with h5py.File(path, 'r') as disk:
                    self.assertEqual(dict(disk[str(original.id)].attrs), attrs)
                self.assertEqual(digest(path), before)
            copy = self.root / (name + '-independent.h5')
            shutil.copy2(path, copy)
            backend = HDF5DataBackend()
            try:
                self.assertTrue(backend.connect({'path': str(copy)}))
                loaded = backend.load_group(str(original.id))
                self.assertIsNotNone(loaded)
                self.assertEqual(loaded.id, original.id)
                self.assertEqual(schema(loaded.record('thermal_trace')), COLUMNS)
            finally:
                backend.disconnect()

    def test_plot_uses_reopened_record_and_preserves_borrowed_figure(self):
        borrowed = plt.figure()
        real_plot = thermal.plot_record
        seen = []

        def plot(record, *args, **kwargs):
            figure = real_plot(record, *args, **kwargs)
            axes = figure.axes[0]
            np.testing.assert_allclose(axes.lines[0].get_xdata(), range(1, 9))
            np.testing.assert_allclose(axes.lines[0].get_ydata(), TEMPERATURES,
                                       rtol=0, atol=1e-10)
            self.assertEqual(axes.get_xlabel(), 'Simulation time (s)')
            self.assertEqual(axes.get_ylabel(), 'Delivered temperature (degC)')
            seen.append(record)
            return figure

        with mock.patch.object(thermal, 'plot_record', side_effect=plot):
            group = self.run_fixture()
        reopened = group.record('thermal_trace')
        self.assertEqual(schema(seen[0]), schema(reopened))
        np.testing.assert_allclose(seen[0].table.to_numpy(dtype=float),
                                   reopened.table.to_numpy(dtype=float),
                                   rtol=0, atol=1e-10)
        self.assertTrue(plt.fignum_exists(borrowed.number))
        self.assertEqual(set(plt.get_fignums()) - self.figures, {borrowed.number})

    def test_model_and_sample_failures_do_not_retry_or_persist(self):
        for stage in ('prepare', 'model', 'sample'):
            with self.subTest(stage=stage):
                coordinator, heater, thermometer = thermal._build_simulation(self.config)
                original_step = coordinator.step
                original_read = thermometer.read
                body = coordinator.station.object('body')
                original_evolve = body._evolve
                steps = []

                def evolve(inputs, states):
                    if stage == 'model' and coordinator.tick == 2:
                        raise ValueError('injected model')
                    return original_evolve(inputs, states)

                def step():
                    original_step()
                    steps.append(coordinator.tick)

                def read():
                    if stage == 'sample' and coordinator.tick == 3:
                        raise ValueError('injected sample')
                    return original_read()

                prepare_context = (mock.patch.object(coordinator, 'prepare',
                                   side_effect=ValueError('injected prepare'))
                                   if stage == 'prepare' else contextlib.nullcontext())
                with mock.patch.object(thermal, '_build_simulation',
                                       return_value=(coordinator, heater, thermometer)), \
                        mock.patch.object(coordinator, 'step', side_effect=step), \
                        mock.patch.object(body, '_evolve', side_effect=evolve), \
                        mock.patch.object(thermometer, 'read', side_effect=read), \
                        prepare_context, \
                        mock.patch.object(thermal, '_reload_group') as reload_group:
                    with self.assertRaises(RuntimeError):
                        thermal.run(self.config, self.root / stage)
                    reload_group.assert_not_called()
                if stage == 'prepare':
                    self.assertFalse(coordinator.prepared)
                    self.assertEqual(steps, [])
                    with self.assertRaisesRegex(RuntimeError, 'not started'):
                        _ = coordinator.tick
                else:
                    expected_tick = {'model': 2, 'sample': 3}[stage]
                    self.assertEqual(coordinator.tick, expected_tick)
                    self.assertEqual(
                        steps, list(range(1, expected_tick + 1)))
                self.assertFalse((self.root / stage / 'thermal.h5').exists())

    def test_backend_false_none_and_raised_errors_abort_before_plot(self):
        cases = [('connect', False), ('save_group', False), ('load_group', None)]
        for index, (method, result) in enumerate(cases):
            for raises in (False, True):
                with self.subTest(method=method, raises=raises), \
                        mock.patch.object(HDF5DataBackend, method,
                                          **({'side_effect': OSError('injected backend')}
                                             if raises else {'return_value': result})), \
                        mock.patch.object(thermal, '_plot_record') as plot:
                    with self.assertRaises(RuntimeError):
                        thermal.run(self.config, self.root / f'{index}-{raises}')
                    plot.assert_not_called()

    def test_corrupted_disk_metadata_identity_and_schema_are_not_repaired(self):
        original = self.run_fixture()
        source = self.root / 'run' / 'thermal.h5'
        for index, key in enumerate(('configuration', 'timing', 'id', 'name')):
            copy = self.root / f'corrupt-{index}.h5'
            shutil.copy2(source, copy)
            with h5py.File(copy, 'a') as disk:
                del disk[str(original.id)].attrs[key]
            before = digest(copy)
            with self.subTest(key=key), self.assertRaises((RuntimeError, ValueError)):
                loaded = thermal._reload_group(copy, str(original.id))
                thermal._verify_round_trip(original, loaded)
            self.assertEqual(digest(copy), before)
        loaded = thermal._reload_group(source, str(original.id))
        altered = loaded.pop_record('thermal_trace')
        altered.table = altered.table.assign(temperature_degc=0.0)
        loaded.add_record(altered)
        self.assertTrue(
            (loaded.record('thermal_trace').table['temperature_degc'] == 0).all())
        with self.assertRaises((RuntimeError, ValueError, AssertionError)):
            thermal._verify_round_trip(original, loaded)

    def test_partial_copy_failure_cleans_scratch_preserves_delivered_file(self):
        group = self.run_fixture()
        source = self.root / 'run' / 'thermal.h5'
        before = digest(source)
        scratch = []

        def fail_copy(src, dst, *args, **kwargs):
            destination = Path(dst)
            destination.write_bytes(b'partial copy')
            scratch.append(destination)
            raise OSError('injected partial copy')

        with mock.patch.object(thermal.shutil, 'copy2', side_effect=fail_copy):
            with self.assertRaises((RuntimeError, OSError)):
                thermal._reload_group(source, str(group.id))
        self.assertTrue(scratch)
        self.assertTrue(all(not path.parent.exists() for path in scratch))
        self.assertEqual(digest(source), before)

    def test_reader_connect_failure_leaves_source_and_removes_copy(self):
        group = self.run_fixture()
        source = self.root / 'run' / 'thermal.h5'
        before = digest(source)
        paths = []
        def connect(backend, arguments=None):
            path = Path(arguments['path'])
            paths.append(path)
            self.assertNotEqual(path, source)
            return False

        for raises in (False, True):
            with self.subTest(raises=raises), mock.patch.object(
                    HDF5DataBackend, 'connect', autospec=True,
                    side_effect=OSError('reader marker') if raises else connect):
                with self.assertRaises((RuntimeError, OSError)):
                    thermal._reload_group(source, str(group.id))
            self.assertEqual(digest(source), before)
        self.assertTrue(paths)
        self.assertTrue(all(not path.parent.exists() for path in paths))

    def test_suppressed_disconnect_failure_is_visible_and_handle_closed(self):
        instances = []
        real_connect = HDF5DataBackend.connect

        def connect(backend, arguments=None):
            result = real_connect(backend, arguments)
            if backend.file is not None:
                instances.append((backend, backend.file))
            return result

        def suppressed(backend):
            backend.record_error('suppressed disconnect marker')
            return False

        with mock.patch.object(HDF5DataBackend, 'connect', autospec=True,
                               side_effect=connect), mock.patch.object(
                HDF5DataBackend, 'disconnect_impl', autospec=True,
                side_effect=suppressed):
            with self.assertRaises(RuntimeError) as caught:
                self.run_fixture()
        self.assertIn('suppressed disconnect marker', str(caught.exception))
        self.assertTrue(instances)
        for backend, handle in instances:
            self.assertFalse(backend.connected)
            self.assertFalse(handle.id.valid)

    def test_scratch_cleanup_failure_is_not_reported_as_success(self):
        group = self.run_fixture()
        source = self.root / 'run' / 'thermal.h5'
        before = digest(source)
        real_directory = tempfile.TemporaryDirectory
        directories = []

        class ObstructedDirectory:
            def __init__(inner, *args, **kwargs):
                inner.directory = real_directory(*args, **kwargs)
                inner.name = inner.directory.name
                directories.append(inner)

            def __enter__(inner):
                return inner.name

            def cleanup(inner):
                raise OSError('scratch cleanup marker: ' + inner.name)

            def __exit__(inner, *args):
                inner.cleanup()

        try:
            with mock.patch.object(thermal.tempfile, 'TemporaryDirectory',
                                   ObstructedDirectory):
                with self.assertRaises((RuntimeError, OSError)) as caught:
                    thermal._reload_group(source, str(group.id))
            self.assertIn('scratch cleanup marker', str(caught.exception))
            self.assertTrue(directories)
            self.assertIn(directories[0].name, str(caught.exception))
            self.assertEqual(digest(source), before)
        finally:
            for directory in directories:
                directory.directory.cleanup()

    def test_plot_failures_close_new_figures_and_keep_verified_data(self):
        def broken_plot(*args, **kwargs):
            plt.figure()
            raise ValueError('injected plot')

        for name, patch in (
                ('plot', mock.patch.object(thermal, 'plot_record', side_effect=broken_plot)),
                ('savefig', mock.patch.object(
                    Figure, 'savefig', side_effect=OSError('injected PNG')))):
            with self.subTest(name=name), patch:
                with self.assertRaises(RuntimeError) as caught:
                    thermal.run(self.config, self.root / name)
                self.assertIn('verified data', str(caught.exception).lower())
            self.assertTrue((self.root / name / 'thermal.h5').exists())
            self.assertEqual(set(plt.get_fignums()), self.figures)

    def test_primary_and_cleanup_errors_visible_other_cleanup_still_runs(self):
        scheduler = thermal._make_scheduler()
        with mock.patch.object(thermal, '_make_scheduler', return_value=scheduler), \
                mock.patch.object(thermal, '_acquire', side_effect=ValueError('primary marker')), \
                mock.patch.object(scheduler, 'stop', side_effect=OSError('cleanup marker')):
            with self.assertRaises(RuntimeError) as caught:
                self.run_fixture()
        message = str(caught.exception)
        self.assertIn('primary marker', message)
        self.assertIn('cleanup marker', message)
        # stop obstruction must not prevent the separately owned loop close.
        self.assertTrue(scheduler._loop.is_closed())

    def test_loop_drain_and_close_failures_preserve_other_cleanup(self):
        """CR-01: drain failure cannot suppress independent loop/writer release."""
        for drain_fails, close_fails in ((True, False), (True, True),
                                         (False, True)):
            with self.subTest(drain=drain_fails, close=close_fails):
                scheduler = mock.Mock(spec=Scheduler)
                owned_loop = mock.Mock()
                scheduler._loop = owned_loop
                if drain_fails:
                    owned_loop.run_until_complete.side_effect = OSError(
                        'drain marker')
                if close_fails:
                    owned_loop.close.side_effect = OSError('close marker')
                pending = mock.Mock()
                drain = object()
                writer = mock.Mock(spec=HDF5DataBackend())
                writer.connect.return_value = True
                writer.connected = True
                writer.errors = []
                writer.save_group.side_effect = ValueError('primary marker')
                original = mock.Mock()
                original.record.return_value = []
                with mock.patch.object(thermal, '_make_scheduler',
                                       return_value=scheduler), \
                        mock.patch.object(thermal, '_acquire',
                                          return_value=original), \
                        mock.patch.object(thermal, 'HDF5DataBackend',
                                          return_value=writer), \
                        mock.patch.object(thermal, '_disconnect') as disconnect, \
                        mock.patch.object(thermal.asyncio, 'all_tasks',
                                          return_value={pending}), \
                        mock.patch.object(thermal.asyncio, 'gather',
                                          return_value=drain) as gather:
                    with self.assertRaises(RuntimeError) as caught:
                        thermal.run(self.config, self.root /
                                    f'loop-{drain_fails}-{close_fails}')
                message = str(caught.exception)
                self.assertIn('primary marker', message)
                if drain_fails:
                    self.assertIn('drain marker', message)
                if close_fails:
                    self.assertIn('close marker', message)
                pending.cancel.assert_called_once_with()
                gather.assert_called_once_with(pending, return_exceptions=True)
                owned_loop.run_until_complete.assert_called_once_with(drain)
                disconnect.assert_called_once_with(writer)
                owned_loop.close.assert_called_once_with()

    def test_owned_scheduler_loop_global_and_running_loop_rejection(self):
        import softlab.huo.scheduler.impl_scheduler as implementation
        singleton = implementation._scheduler
        borrowed_loop = asyncio.new_event_loop()
        self.addCleanup(borrowed_loop.close)
        asyncio.set_event_loop(borrowed_loop)
        self.addCleanup(asyncio.set_event_loop, None)
        schedulers = []
        factory = thermal._make_scheduler

        def make():
            scheduler = factory()
            schedulers.append(scheduler)
            return scheduler

        with mock.patch.object(thermal, '_make_scheduler', side_effect=make):
            self.run_fixture('one')
            self.run_fixture('two')
        self.assertIs(implementation._scheduler, singleton)
        self.assertIs(asyncio.get_event_loop(), borrowed_loop)
        self.assertFalse(borrowed_loop.is_closed())
        self.assertEqual(len(schedulers), 2)
        for scheduler in schedulers:
            self.assertFalse(scheduler.is_running())
            self.assertTrue(scheduler._loop.is_closed())

        async def running():
            with mock.patch.object(thermal, '_build_simulation') as build:
                with self.assertRaises(RuntimeError):
                    thermal.run(self.config, self.root / 'async-rejected')
                build.assert_not_called()
        borrowed_loop.run_until_complete(running())

    def test_cli_announces_success_only_after_outputs(self):
        target = self.root / 'cli'
        env = dict(os.environ, MPLBACKEND='Agg')
        command = [sys.executable, str(EXAMPLE), '--output-dir', str(target)]
        result = subprocess.run(command, cwd=ROOT, env=env, capture_output=True,
                                text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(str(target / 'thermal.h5'), result.stdout)
        self.assertIn(str(target / 'thermal.png'), result.stdout)
        another = self.root / 'cli-repeat'
        repeated = subprocess.run(
            [sys.executable, str(EXAMPLE), '--output-dir', str(another)],
            cwd=ROOT, env=env, capture_output=True, text=True, timeout=30)
        self.assertEqual(repeated.returncode, 0, repeated.stderr)
        ids = []
        for directory in (target, another):
            with h5py.File(directory / 'thermal.h5', 'r') as disk:
                ids.append(next(iter(disk)))
            self.assert_default(thermal._reload_group(
                directory / 'thermal.h5', ids[-1]))
        self.assertNotEqual(ids[0], ids[1])
        failed = subprocess.run(command, cwd=ROOT, env=env, capture_output=True,
                                text=True, timeout=30)
        self.assertNotEqual(failed.returncode, 0)
        self.assertTrue(failed.stderr)


class CountSchedulerContractTests(unittest.TestCase):
    """Public Action dispatch of the approved bounded example scheduler."""

    def setUp(self):
        self.scheduler = thermal._make_scheduler()
        self.addCleanup(self.scheduler._loop.close)
        self.addCleanup(self.scheduler.stop)

    def test_serial_action_success_failure_and_unsupported_actions(self):
        scheduler = self.scheduler
        self.assertIsInstance(scheduler, Scheduler)
        point = scheduler.acquire_point()
        self.assertFalse(scheduler.commit_action(Action('', point)))
        self.assertTrue(scheduler.start())
        seen = []

        async def body():
            seen.append('executed')
        self.assertTrue(scheduler.commit_action(Action('', point, func=body)))
        self.assertFalse(scheduler.commit_action(Action('', point, func=body)))
        self.assertTrue(scheduler.wait_point(point))
        self.assertEqual(seen, ['executed'])
        self.assertTrue(scheduler.wait_point(point))
        self.assertEqual(seen, ['executed'])  # waiting twice never repeats action
        for begin, threaded in (('dependency', False), ('', True)):
            end = scheduler.acquire_point()
            self.assertFalse(scheduler.commit_action(Action(begin, end, threaded)))
        self.assertFalse(scheduler.commit_action(Action('', 'unknown')))

        def fail():
            raise ValueError('dispatch marker')
        end = scheduler.acquire_point()
        self.assertTrue(scheduler.commit_action(Action('', end, func=fail)))
        self.assertFalse(scheduler.wait_point(end))
        scheduler.clear_done_points()
        self.assertIsInstance(scheduler.snapshot(), dict)


if __name__ == '__main__':
    unittest.main()
