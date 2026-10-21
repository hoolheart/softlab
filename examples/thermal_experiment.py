"""Synthetic thermal reference; helpers are not supported public APIs."""

import argparse
import asyncio
from dataclasses import asdict, dataclass
import inspect
import json
import math
from numbers import Real
import os
from pathlib import Path
import shutil
import sys
import tempfile
from typing import Any, Dict, Optional, Sequence, Tuple
import uuid

os.environ.setdefault('MPLBACKEND', 'Agg')
import matplotlib

if __name__ == '__main__':
    matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

from softlab.huo.process import count, run_process
from softlab.huo.scheduler import Action, Scheduler
from softlab.jin.vis import plot_record
from softlab.shui.data import DataGroup, DataRecord, HDF5DataBackend
from softlab.tu.simulation import (
    Coordinator, Endpoint, MockControl, MockMeasurement, SimulatedObject,
)
from softlab.tu.station import Parameter, Station


@dataclass(frozen=True)
class ThermalConfig:
    """Euler fixture inputs; C/G/dt must be positive with dt*G/C <= 1."""

    initial_temperature: float = 20.0
    ambient_temperature: float = 20.0
    heat_capacity: float = 10.0
    conductance: float = 1.0
    dt: float = 1.0
    power: float = 10.0
    points: int = 8


class _CountScheduler(Scheduler):
    """Owned serial Action executor for this count; no chains or threads."""

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._running = False
        self._points: Dict[str, Optional[Action]] = {}
        self._done: Dict[str, Optional[bool]] = {}
        self.last_error = ''

    def start(self) -> bool:
        """Enable commits without changing the thread's event loop."""
        self._running = True
        return True

    def stop(self) -> None:
        """Disable commits and discard owned action references."""
        self._running = False
        self._points.clear()
        self._done.clear()

    def is_running(self) -> bool:
        """Return whether commits are enabled."""
        return self._running

    def acquire_point(self) -> str:
        """Allocate a fresh serial completion key."""
        point = str(uuid.uuid4())
        self._points[point] = None
        return point

    def commit_action(self, action: Action) -> bool:
        """Accept one unchained, nonthreaded action for an unused endpoint."""
        if (not self._running or not isinstance(action, Action)
                or action.begin_point or action.to_thread
                or action.end_point not in self._points
                or self._points[action.end_point] is not None):
            return False
        self._points[action.end_point] = action
        return True

    def wait_point(self, point: str,
                   timeout: Optional[float] = None) -> Optional[bool]:
        """Run once; return false on failure or None on timeout."""
        if point in self._done:
            return self._done[point]
        action = self._points.get(point)
        if not self._running or action is None:
            return False

        async def dispatch() -> None:
            func, args, kwargs = action.body
            result = func(*args, **kwargs)
            if inspect.isawaitable(result):
                await result

        try:
            coroutine = dispatch()
            if timeout is not None:
                coroutine = asyncio.wait_for(coroutine, timeout)
            self._loop.run_until_complete(coroutine)
            result = True
        except asyncio.TimeoutError as exc:
            self.last_error = str(exc) or 'Action timed out'
            result = None
        except Exception as exc:
            self.last_error = f'{type(exc).__name__}: {exc}'
            result = False
        self._done[point] = result
        return result

    def clear_done_points(self) -> None:
        """Discard completed endpoints without executing anything."""
        for point in self._done:
            self._points.pop(point, None)
        self._done.clear()

    def snapshot(self) -> Dict[str, Any]:
        """Describe this bounded executor's current state."""
        return {'type': '_CountScheduler', 'running': self._running,
                'points': len(self._points), 'completed': len(self._done)}


def _make_scheduler() -> _CountScheduler:
    """Create a private executor with a separately owned event loop."""
    return _CountScheduler()


def _validate_config(config: ThermalConfig) -> None:
    """Reject invalid scalar inputs before creating resources or outputs."""
    if not isinstance(config, ThermalConfig):
        raise TypeError('Expected ThermalConfig')
    for key, value in asdict(config).items():
        if key == 'points':
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError('points must be an integer excluding bool')
            if value < 1:
                raise ValueError('points must be positive')
        elif isinstance(value, bool) or not isinstance(value, Real):
            raise TypeError(f'{key} must be a finite real number')
        elif not math.isfinite(value):
            raise ValueError(f'{key} must be finite')
    for key in ('heat_capacity', 'conductance', 'dt'):
        if getattr(config, key) <= 0:
            raise ValueError(f'{key} must be positive')
    ratio = config.dt * config.conductance / config.heat_capacity
    if not math.isfinite(ratio) or not 0 < ratio <= 1:
        raise ValueError('Require 0 < dt * conductance / heat_capacity <= 1')


def _prepare_output(output_dir: Path) -> Tuple[Path, Path]:
    """Reserve a new/empty nonsymlink directory; never overwrite content."""
    directory = Path(output_dir).absolute()
    if directory.is_symlink():
        raise ValueError('Output directory must not be a symlink')
    if directory.exists():
        if not directory.is_dir() or any(directory.iterdir()):
            raise FileExistsError(
                f'Output directory must be empty: {directory}')
    else:
        directory.mkdir(parents=True)
    return directory / 'thermal.h5', directory / 'thermal.png'


def _build_simulation(config: ThermalConfig) -> Tuple[
        Coordinator, MockControl, MockMeasurement]:
    """Create the private thermal topology without preparing or stepping it."""
    def evolve(inputs: Dict[str, float], states: Dict[str, float]
               ) -> Dict[str, float]:
        temperature = states['temperature']
        return {'temperature': temperature + inputs['dt'] * (
            inputs['power'] - config.conductance *
            (temperature - config.ambient_temperature)) / config.heat_capacity}

    def observe(states: Dict[str, float]) -> Dict[str, float]:
        return {'reading': states['temperature']}

    station = Station('thermal_station')
    station.add_object(SimulatedObject(
        'body', inputs={'power': 0.0, 'dt': config.dt},
        states={'temperature': config.initial_temperature}, outputs=['reading'],
        evolve=evolve, observe=observe))
    station.sim_dt = config.dt
    coordinator = Coordinator(station)
    coordinator.enroll('body')
    heater = MockControl('heater', 0.0)
    thermometer = MockMeasurement('thermometer', config.initial_temperature)
    coordinator.add_control(heater)
    coordinator.add_measurement(thermometer)
    coordinator.connect(Endpoint('heater'), Endpoint('body', 'power'))
    coordinator.connect(Endpoint('body', 'reading'), Endpoint('thermometer'))
    coordinator.bind_interval('body', 'dt')
    return coordinator, heater, thermometer


_SCHEMA = [
    ('commanded_power_w', 'Commanded heater power', 'W', False),
    ('measurement_source_tick', 'Measurement source boundary', '1', False),
    ('sim_time_s', 'Simulation acquisition time', 's', False),
    ('temperature_degc', 'Delivered temperature', 'degC', True),
    ('tick', 'Acquisition boundary', '1', False),
    ('timestamp', 'Wall-clock acquisition timestamp', 's', False),
]
_META_KEYS = ('example', 'configuration', 'timing')


def _acquire(config: ThermalConfig, coordinator: Coordinator,
             heater: MockControl, thermometer: MockMeasurement,
             scheduler: Scheduler) -> DataGroup:
    """Prepare and execute exactly one step per count getter row."""
    group = DataGroup('thermal_experiment')
    group.set_meta('example', 'thermal_experiment')
    group.set_meta('configuration', json.dumps(
        asdict(config), sort_keys=True, allow_nan=False))
    group.set_meta('timing', json.dumps({
        'acquisition': 'post_step', 'measurement_delay_ticks': 1,
        'source_tick': 'tick-1', 'wall_timestamp': 'unix_seconds',
    }, sort_keys=True))
    record = DataRecord('thermal_trace', [dict(zip(
        ('name', 'label', 'unit', 'dependent'), definition))
        for definition in _SCHEMA])
    group.add_record(record)
    readers = {
        'commanded_power_w': heater.read,
        'measurement_source_tick': lambda: coordinator.tick - 1,
        'sim_time_s': lambda: coordinator.sim_time,
        'temperature_degc': thermometer.read,
        'tick': lambda: coordinator.tick,
    }
    getters = [(key, Parameter(key, settable=False,
                before_get=lambda _, read=reader: read()))
               for key, reader in readers.items()]
    try:
        coordinator.prepare()
        heater.write(config.power)
        process = count(name='thermal_trace', group=group, record=record,
                        getters=getters, times=config.points,
                        hook_before_get=coordinator.step)
        if not scheduler.start():
            raise RuntimeError('Scheduler did not start')
        succeeded, _ = run_process(process, scheduler, verbose=False)
        if not succeeded:
            raise RuntimeError(getattr(scheduler, 'last_error', '') or
                               'Count execution failed')
        if coordinator.tick != config.points or len(record) != config.points:
            raise RuntimeError('Acquisition boundary/row count mismatch')
        if not np.isfinite(record.table.to_numpy(dtype=float)).all():
            raise RuntimeError('Acquired nonfinite numeric values')
    except Exception as exc:
        raise RuntimeError(
            f'Acquisition failed with {len(record)} rows: {exc}') from exc
    return group


def _disconnect(backend: HDF5DataBackend) -> None:
    """Release an owned backend and verify its raw handle despite void API."""
    handle = backend.file
    old_errors = len(backend.errors)
    errors = []
    try:
        backend.disconnect()
    except Exception as exc:
        errors.append(str(exc))
    errors.extend(backend.errors[old_errors:])
    if backend.connected:
        errors.append('Backend still connected')
    if handle is not None and handle.id.valid:
        errors.append('Backend disconnect left its file handle open')
        try:
            handle.close()
        except Exception as exc:
            errors.append(f'Raw handle close failed: {exc}')
    if errors:
        raise RuntimeError('; '.join(errors))


def _load_group(backend: HDF5DataBackend, group_id: str) -> DataGroup:
    """Load once from a copy and hydrate its verified disk text metadata."""
    attrs = backend.file[group_id].attrs
    for key in ('id', 'name', 'timestamp', 'backend') + _META_KEYS:
        if key not in attrs or not isinstance(attrs[key], str):
            raise RuntimeError(f'Missing or nontext disk metadata: {key}')
    if attrs['id'] != group_id:
        raise RuntimeError('Disk group identity mismatch')
    metadata = {key: attrs[key] for key in _META_KEYS}
    loaded = backend.load_group(group_id)
    if (not isinstance(loaded, DataGroup) or not backend.just_succeeded
            or backend.errors):
        raise RuntimeError(f'HDF5 load failed: {backend.last_error}')
    for key, value in metadata.items():
        loaded.set_meta(key, value)
    return loaded


def _reload_group(path: Path, group_id: str) -> DataGroup:
    """Reload a disposable copy, preserving the delivered source."""
    backend = HDF5DataBackend()
    errors = []
    primary = None
    loaded = None
    directory = tempfile.TemporaryDirectory(prefix='softlab-thermal-read-')
    try:
        copy = Path(directory.name) / 'thermal.h5'
        shutil.copy2(path, copy)
        if (not backend.connect({'path': str(copy)})
                or not backend.connected or backend.errors):
            raise RuntimeError(
                f'HDF5 reader connect failed: {backend.last_error}')
        loaded = _load_group(backend, group_id)
        loaded.backend = {
            'type': 'hdf5',
            'arguments': {'path': str(Path(path).absolute())},
        }
    except BaseException as exc:
        primary = exc
    finally:
        if backend.file is not None or backend.connected:
            try:
                _disconnect(backend)
            except Exception as exc:
                errors.append(f'Reader cleanup: {exc}')
        try:
            directory.cleanup()
        except Exception as exc:
            errors.append(f'Scratch cleanup at {directory.name}: {exc}')
    if primary is not None or errors:
        if primary is not None and not isinstance(primary, Exception):
            for error in errors:
                print(error, file=sys.stderr)
            raise primary
        message = '; '.join(([str(primary)] if primary else []) + errors)
        raise RuntimeError(message) from primary
    return loaded


def _verify_round_trip(original: DataGroup, loaded: DataGroup) -> None:
    """Require identity, disk metadata, exact schema and all numeric values."""
    if (original.id != loaded.id or original.name != loaded.name
            or original.timestamp != loaded.timestamp
            or original.meta != loaded.meta
            or list(original.records) != list(loaded.records)):
        raise RuntimeError('HDF5 group identity/metadata mismatch')
    for name in original.records:
        before, after = original.record(name), loaded.record(name)
        if before.columns != after.columns or before.shape != after.shape:
            raise RuntimeError('HDF5 record schema/shape mismatch')
        left, right = (r.table.to_numpy(dtype=float) for r in (before, after))
        if (not np.isfinite(right).all()
                or not np.allclose(left, right, rtol=0, atol=1e-10)):
            raise RuntimeError('HDF5 numeric values mismatch')


def _plot_record(record: DataRecord, path: Path) -> None:
    """Save reopened temperature data, closing only newly created figures."""
    existing = set(plt.get_fignums())
    previous = plt.gcf() if existing else None
    primary = None
    errors = []
    try:
        figure = plot_record(record, 'sim_time_s', ['temperature_degc'],
                             xlabel='Simulation time (s)',
                             ylabel='Delivered temperature (degC)')
        figure.savefig(path)
    except BaseException as exc:
        primary = exc
    finally:
        for number in set(plt.get_fignums()) - existing:
            try:
                plt.close(number)
            except Exception as exc:
                errors.append(f'Figure {number} close failed: {exc}')
        if previous is not None and plt.fignum_exists(previous.number):
            plt.figure(previous.number)
    if primary is not None or errors:
        if primary is not None and not isinstance(primary, Exception):
            for error in errors:
                print(error, file=sys.stderr)
            raise primary
        message = '; '.join(([str(primary)] if primary else []) + errors)
        raise RuntimeError(message) from primary


def run(config: ThermalConfig, output_dir: Path) -> DataGroup:
    """Return verified reopened data only after owned cleanup succeeds."""
    _validate_config(config)
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError(
            'The synchronous thermal example cannot run inside an event loop')
    data_path, plot_path = _prepare_output(output_dir)
    scheduler = None
    coordinator = None
    writer = None
    original = None
    loaded = None
    primary = None
    errors = []
    verified = False
    stage = 'setup'
    try:
        scheduler = _make_scheduler()
        coordinator, heater, thermometer = _build_simulation(config)
        stage = 'acquisition'
        original = _acquire(config, coordinator, heater, thermometer, scheduler)
        stage = 'persistence'
        writer = HDF5DataBackend()
        if (not writer.connect({'path': str(data_path)})
                or not writer.connected or writer.errors):
            raise RuntimeError(
                f'HDF5 writer connect failed: {writer.last_error}')
        if (not writer.save_group(original) or not writer.just_succeeded
                or writer.errors):
            raise RuntimeError(f'HDF5 save failed: {writer.last_error}')
        _disconnect(writer)
        writer = None
        stage = 'reopen verification'
        loaded = _reload_group(data_path, str(original.id))
        _verify_round_trip(original, loaded)
        verified = True
        stage = 'plotting'
        _plot_record(loaded.record('thermal_trace'), plot_path)
    except BaseException as exc:
        primary = exc
    finally:
        # Inspection only: teardown never advances or resets the graph.
        boundary = 'unprepared'
        if coordinator is not None and coordinator.prepared:
            boundary = (f'tick={coordinator.tick}, '
                        f'sim_time={coordinator.sim_time}')
        rows = (len(original.record('thermal_trace'))
                if original is not None else 'see acquisition error')
        if scheduler is not None:
            try:
                scheduler.stop()
            except Exception as exc:
                errors.append(f'Scheduler stop: {exc}')
            try:
                pending = asyncio.all_tasks(scheduler._loop)
                for task in pending:
                    task.cancel()
                if pending:
                    scheduler._loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True))
            except Exception as exc:
                errors.append(f'Owned loop drain: {exc}')
            try:
                scheduler._loop.close()
            except Exception as exc:
                errors.append(f'Owned loop close: {exc}')
        if writer is not None and (writer.file is not None or writer.connected):
            try:
                _disconnect(writer)
            except Exception as exc:
                errors.append(f'Writer cleanup: {exc}')
    if primary is not None or errors:
        if verified:
            storage = (f'verified data: {data_path}; '
                       f'incomplete PNG: {plot_path}')
        else:
            storage = (
                f'incomplete/unverified artifact (if present): {data_path}')
        message = f'{stage} failed; {boundary}; rows={rows}; {storage}; '
        message += '; '.join(([str(primary)] if primary else []) + errors)
        if primary is not None and not isinstance(primary, Exception):
            print(message, file=sys.stderr)
            raise primary
        raise RuntimeError(message) from primary
    return loaded


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Parse the documented command; announce completion only after cleanup."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', required=True, type=Path)
    for key, value in asdict(ThermalConfig()).items():
        parser.add_argument('--' + key.replace('_', '-'), default=value,
                            type=int if key == 'points' else float)
    arguments = vars(parser.parse_args(argv))
    output_dir = arguments.pop('output_dir')
    try:
        group = run(ThermalConfig(**arguments), output_dir)
    except Exception as exc:
        print(f'Thermal experiment failed: {exc}', file=sys.stderr)
        return 1
    print(f'Complete: run {group.id}')
    print(f'HDF5: {output_dir.absolute() / "thermal.h5"}')
    print(f'PNG: {output_dir.absolute() / "thermal.png"}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
