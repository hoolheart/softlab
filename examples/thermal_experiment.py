"""Synthetic thermal reference; helpers are not supported public APIs."""

import argparse
from contextlib import redirect_stdout
import hashlib
import asyncio
from dataclasses import asdict, dataclass, field
import json
import math
from numbers import Real
import os
import platform
import re
from pathlib import Path
import shutil
import sys
import subprocess
import tempfile
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
import uuid

os.environ.setdefault('MPLBACKEND', 'Agg')
import matplotlib

if __name__ == '__main__':
    matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import h5py

import softlab

from softlab.huo.process import StopRequest, count, run_count
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


@dataclass
class RunResult:
    """Example-local terminal evidence; reset never rewrites these snapshots."""

    requested_rows: int
    data_path: Path
    plot_path: Path
    stop: StopRequest
    completed_rows: int = 0
    final_tick: Optional[int] = None
    final_sim_time: Optional[float] = None
    acquisition: str = 'not_started'
    persistence: str = 'not_attempted'
    plotting: str = 'not_attempted'
    cleanup_errors: List[str] = field(default_factory=list)
    cause: Optional[BaseException] = None
    stage: str = 'setup'
    group: Optional[DataGroup] = None
    loaded: Optional[DataGroup] = None
    terminal: bool = False
    work_drained: bool = False
    loop_closed: bool = False
    _coordinator: Optional[Coordinator] = field(default=None, repr=False)

    @property
    def overall_success(self) -> bool:
        """Require acquisition, verified storage, plot and owned cleanup."""
        return (self.acquisition == 'completed'
                and self.persistence == 'verified'
                and self.plotting == 'completed' and not self.cleanup_errors
                and self.work_drained and self.loop_closed and self.terminal)


def _diagnostic(result: RunResult) -> str:
    """Describe independent acquisition, storage and cleanup outcomes."""
    storage = (f'verified data: {result.data_path}'
               if result.persistence == 'verified' else
               f'incomplete/unverified artifact (if present): {result.data_path}')
    return (f'{result.stage}: acquisition={result.acquisition}; '
            f'rows={result.completed_rows}; tick={result.final_tick}; '
            f'sim_time={result.final_sim_time}; '
            f'requested={result.stop.requested}; '
            f'acknowledged={result.stop.acknowledged}; '
            f'persistence={result.persistence}; plotting={result.plotting}; '
            f'{storage}; cause={result.cause}; '
            + '; '.join(result.cleanup_errors))


class RunFailure(RuntimeError):
    """Unsuccessful compatibility call retaining inspectable execution evidence."""

    def __init__(self, result: RunResult) -> None:
        self.result = result
        super().__init__(_diagnostic(result))


class _HelperFailure(RuntimeError):
    """Retain helper operation cause and ordered owned-cleanup diagnostics."""

    def __init__(self, primary: Optional[Exception],
                 cleanup_errors: List[str]) -> None:
        self.primary = primary
        self.cleanup_errors = list(cleanup_errors)
        super().__init__('; '.join(
            ([str(primary)] if primary is not None else [])
            + self.cleanup_errors))


class _AcquisitionEnd(Exception):
    """Carry incomplete acquisition to the owning execute call."""

    def __init__(self, result: RunResult) -> None:
        self.result = result
        super().__init__(_diagnostic(result))


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


class _DirectMeasurement:
    """Observe this example's pure model output at its committed boundary."""

    def __init__(self, body: SimulatedObject) -> None:
        self._body = body

    def read(self) -> Any:
        return self._body.observe_outputs()['reading']


class _ControlAdapter:
    """Forward the example's control operations to the model-backed endpoint."""

    def __init__(self, control: MockControl) -> None:
        self._control = control

    def write(self, value: Any) -> None:
        self._control.write(value)

    def read(self) -> Any:
        return self._control.read()


def _timing(delay: int) -> Dict[str, Any]:
    return {'acquisition': 'post_step', 'measurement_delay_ticks': delay,
            'source_tick': 'tick-1' if delay else 'tick',
            'wall_timestamp': 'unix_seconds'}


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
             heater: MockControl, thermometer: MockMeasurement, *,
             stop: Optional[StopRequest] = None,
             after_row: Optional[Callable[[RunResult], None]] = None,
             result: Optional[RunResult] = None,
             delay: int = 1) -> DataGroup:
    """Run thermal acquisition through the owned bounded count seam."""
    stop = stop if stop is not None else StopRequest()
    if result is None:
        result = RunResult(config.points, Path('thermal.h5'), Path('thermal.png'), stop)
    group = DataGroup('thermal_experiment')
    group.set_meta('example', 'thermal_experiment')
    group.set_meta('configuration', json.dumps(
        asdict(config), sort_keys=True, allow_nan=False))
    group.set_meta('timing', json.dumps(_timing(delay), sort_keys=True))
    record = DataRecord('thermal_trace', [dict(zip(
        ('name', 'label', 'unit', 'dependent'), definition))
        for definition in _SCHEMA])
    group.add_record(record)
    result.group = group
    readers = {
        'commanded_power_w': heater.read,
        'measurement_source_tick': lambda: coordinator.tick - delay,
        'sim_time_s': lambda: coordinator.sim_time,
        'temperature_degc': thermometer.read,
        'tick': lambda: coordinator.tick,
    }
    getters = [(key, Parameter(key, settable=False,
                before_get=lambda _, read=reader: read()))
               for key, reader in readers.items()]

    def map_count(snapshot):
        result.completed_rows = snapshot.completed_rows
        if snapshot.boundary_observed:
            result.final_tick, result.final_sim_time = snapshot.boundary

    def after_count(snapshot):
        map_count(snapshot)
        if not np.isfinite(record.table.to_numpy(dtype=float)).all():
            raise RuntimeError('Acquired nonfinite numeric values')
        if after_row is not None:
            after_row(result)

    def terminal(snapshot):
        map_count(snapshot)
        result.acquisition = snapshot.acquisition
        result.cause = snapshot.cause
        result.cleanup_errors.extend(snapshot.cleanup_errors)
        result.work_drained = snapshot.work_drained
        result.loop_closed = snapshot.loop_closed

    try:
        coordinator.prepare()
        heater.write(config.power)
        process = count(name='thermal_trace', group=group, record=record,
                        getters=getters, times=config.points,
                        hook_before_get=coordinator.step)
        try:
            snapshot = run_count(process, config.points, lambda: len(record),
                                 lambda: (coordinator.tick, coordinator.sim_time),
                                 stop=stop, after_row=after_count)
        except BaseException as exc:
            snapshot = getattr(exc, 'count_result', None)
            if snapshot is not None:
                terminal(snapshot)
            raise
        terminal(snapshot)
        if (result.acquisition != 'completed' or result.cleanup_errors
                or not result.work_drained or not result.loop_closed):
            raise _AcquisitionEnd(result)
        if coordinator.tick != config.points:
            raise RuntimeError('Acquisition boundary/row count mismatch')
    except _AcquisitionEnd:
        raise
    except Exception as exc:
        result.acquisition = 'failed'
        result.cause = exc
        raise _AcquisitionEnd(result) from exc
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


_MANIFEST_KEY = 'thermal_manifest'
_MODEL = {
    'name': 'lumped_thermal_body', 'method': 'explicit_euler',
    'equation': 'T_next = T + dt * (P - G * (T - T_ambient)) / C',
    'validity': 'educational; constant parameters; 0 < dt*G/C <= 1; no physical calibration',
    'units': {'initial_temperature': 'degC', 'ambient_temperature': 'degC',
              'heat_capacity': 'J/K', 'conductance': 'W/K', 'dt': 's',
              'power': 'W', 'points': '1'},
}


def _manifest_object(value: Any, keys: Sequence[str], path: str) -> None:
    """Require one closed manifest object, with no implicit key conversion."""
    if (type(value) is not dict or any(type(k) is not str for k in value)
            or set(value) != set(keys)):
        raise ValueError(f'{path}: expected exact object fields {list(keys)}')


def _manifest_text(value: Any, path: str) -> None:
    """Require nonempty text at a declared manifest path."""
    if type(value) is not str or not value:
        raise ValueError(f'{path}: expected nonempty text')


def _manifest_real(value: Any, path: str) -> float:
    """Normalize a declared finite JSON real, excluding booleans."""
    if type(value) not in (int, float):
        raise ValueError(f'{path}: expected finite real')
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise ValueError(f'{path}: expected finite real') from exc
    if not math.isfinite(number):
        raise ValueError(f'{path}: expected finite real')
    return number


def _manifest_integer(value: Any, path: str) -> None:
    """Require a positive exact integer, excluding bool and integral floats."""
    if type(value) is not int or value <= 0:
        raise ValueError(f'{path}: expected positive integer')


def _check_manifest(manifest: Any) -> dict:
    """Validate the closed v1 schema and return normalized declared real fields."""
    if type(manifest) is not dict:
        raise ValueError('manifest: expected object')
    if (type(manifest.get('schema')) is not str
            or manifest['schema'] != 'softlab.thermal-reference'):
        raise ValueError('schema: unsupported manifest schema')
    if type(manifest.get('version')) is not int or manifest['version'] not in (1, 2):
        raise ValueError('version: unsupported manifest version')
    _manifest_object(manifest, ('schema', 'version', 'run_id', 'configuration',
                     'model', 'procedure', 'columns', 'clock', 'software',
                     'source', 'outcome'), 'manifest')
    result = dict(manifest)
    _manifest_text(result['run_id'], 'run_id')
    try:
        if str(uuid.UUID(result['run_id'])) != result['run_id']:
            raise ValueError('noncanonical UUID')
    except ValueError as exc:
        raise ValueError('run_id: expected canonical UUID') from exc
    config = result['configuration']
    _manifest_object(config, tuple(asdict(ThermalConfig())), 'configuration')
    config = dict(config)
    for key in config:
        if key == 'points':
            _manifest_integer(config[key], f'configuration.{key}')
        else:
            config[key] = _manifest_real(config[key], f'configuration.{key}')
    _validate_config(ThermalConfig(**config))
    result['configuration'] = config
    model = result['model']
    _manifest_object(model, tuple(_MODEL), 'model')
    _manifest_object(model['units'], tuple(_MODEL['units']), 'model.units')
    for key in ('name', 'method', 'equation', 'validity'):
        _manifest_text(model[key], f'model.{key}')
    for key, value in model['units'].items():
        _manifest_text(value, f'model.units.{key}')
    if model != _MODEL:
        raise ValueError('model: unsupported model semantics or units')
    procedure = result['procedure']
    _manifest_object(procedure, ('name', 'advancement', 'command',
                               'requested_points'), 'procedure')
    for key in ('name', 'advancement', 'command'):
        _manifest_text(procedure[key], f'procedure.{key}')
    _manifest_integer(procedure['requested_points'], 'procedure.requested_points')
    if procedure != {'name': 'thermal_count',
                     'advancement': 'once_before_each_getter_row',
                     'command': 'constant_power',
                     'requested_points': config['points']}:
        raise ValueError('procedure: inconsistent declared acquisition')
    columns = result['columns']
    if type(columns) is not list or len(columns) != len(_SCHEMA):
        raise ValueError('columns: expected ordered thermal schema')
    for index, (column, definition) in enumerate(zip(columns, _SCHEMA)):
        keys = ('name', 'label', 'unit', 'dependent')
        _manifest_object(column, keys, f'columns.{index}')
        for key in ('name', 'label', 'unit'):
            _manifest_text(column[key], f'columns.{index}.{key}')
        if type(column['dependent']) is not bool or column != dict(zip(keys, definition)):
            raise ValueError(f'columns.{index}: invalid column definition')
    clock = result['clock']
    _manifest_object(clock, ('dt', 'simulation_unit', 'acquisition',
                     'measurement_delay_ticks', 'source_tick', 'wall_timestamp'), 'clock')
    clock = dict(clock)
    clock['dt'] = _manifest_real(clock['dt'], 'clock.dt')
    for key in ('simulation_unit', 'acquisition', 'source_tick', 'wall_timestamp'):
        _manifest_text(clock[key], f'clock.{key}')
    if (type(clock['measurement_delay_ticks']) is not int or clock != {
            'dt': config['dt'], 'simulation_unit': 's', 'acquisition': 'post_step',
            **_timing(1 if manifest['version'] == 1 else 0)}):
        raise ValueError('clock: inconsistent acquisition timing')
    result['clock'] = clock
    software = result['software']
    _manifest_object(software, ('softlab_version', 'python_version'), 'software')
    for key, value in software.items():
        _manifest_text(value, f'software.{key}')
    source = result['source']
    _manifest_object(source, ('reference', 'digest_algorithm', 'digest_scope',
                     'digest', 'digest_unavailable_reason', 'git_revision',
                     'git_dirty', 'git_unavailable_reason'), 'source')
    for key, value in {
            'reference': 'examples/thermal_experiment.py',
            'digest_algorithm': 'sha256',
            'digest_scope': 'exact reference script bytes at production'}.items():
        if type(source[key]) is not str or source[key] != value:
            raise ValueError(f'source.{key}: invalid declared scope')
    if source['digest'] is None:
        _manifest_text(source['digest_unavailable_reason'], 'source.digest_unavailable_reason')
    elif (type(source['digest']) is not str
          or re.fullmatch('[0-9a-f]{64}', source['digest']) is None
          or source['digest_unavailable_reason'] is not None):
        raise ValueError('source.digest: invalid digest or availability')
    if source['git_revision'] is None:
        if source['git_dirty'] is not None:
            raise ValueError('source.git_dirty: unavailable revision requires null')
        _manifest_text(source['git_unavailable_reason'], 'source.git_unavailable_reason')
    elif (type(source['git_revision']) is not str
          or re.fullmatch(r'[0-9a-f]{40}|[0-9a-f]{64}', source['git_revision']) is None
          or type(source['git_dirty']) is not bool
          or source['git_unavailable_reason'] is not None):
        raise ValueError('source.git_revision: invalid revision/state availability')
    outcome = result['outcome']
    _manifest_object(outcome, ('snapshot', 'acquisition', 'requested_rows',
                     'completed_rows', 'final_tick', 'final_sim_time',
                     'persistence', 'plotting', 'cleanup', 'overall'), 'outcome')
    outcome = dict(outcome)
    for key in ('requested_rows', 'completed_rows', 'final_tick'):
        _manifest_integer(outcome[key], f'outcome.{key}')
    outcome['final_sim_time'] = _manifest_real(outcome['final_sim_time'], 'outcome.final_sim_time')
    expected = {'snapshot': 'completed_acquisition_before_persistence',
                'acquisition': 'completed', 'requested_rows': config['points'],
                'completed_rows': config['points'], 'final_tick': config['points'],
                'final_sim_time': config['points'] * config['dt'],
                **{key: 'not_yet_established' for key in
                   ('persistence', 'plotting', 'cleanup', 'overall')}}
    for key in ('snapshot', 'acquisition', 'persistence', 'plotting', 'cleanup', 'overall'):
        _manifest_text(outcome[key], f'outcome.{key}')
    time = outcome['final_sim_time']
    if (time < 0 or not math.isclose(time, expected['final_sim_time'],
                                       rel_tol=0, abs_tol=1e-10)
            or any(outcome[key] != value for key, value in expected.items()
                   if key != 'final_sim_time')):
        raise ValueError('outcome: inconsistent completed acquisition snapshot')
    result['outcome'] = outcome
    return result


def _encode_manifest(manifest: dict) -> str:
    """Encode only validated closed v1 values, without fallback serialization."""
    return json.dumps(_check_manifest(manifest), sort_keys=True, allow_nan=False)


def _decode_manifest(text: str) -> dict:
    """Decode strict JSON and v1 types; reject duplicates/nonfinite constants."""
    if type(text) is not str:
        raise TypeError('manifest: expected JSON text')

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError(f'manifest: duplicate object key {key}')
            result[key] = value
        return result

    def invalid_constant(value):
        raise ValueError(f'manifest: nonfinite JSON token {value}')

    return _check_manifest(json.loads(text, object_pairs_hook=pairs,
                                      parse_constant=invalid_constant))


def _query_source_git(source_path: Path) -> dict:
    """Query tracked source's repository with bounded calls, ignoring caller cwd."""
    unavailable = {'git_revision': None, 'git_dirty': None}
    try:
        source_path = source_path.resolve()

        def query(*arguments):
            completed = subprocess.run(
                ['git', *arguments], cwd=str(source_path.parent),
                check=True, capture_output=True, text=True, timeout=5)
            return completed.stdout.strip()

        root = Path(query('rev-parse', '--show-toplevel')).resolve()
        relative = source_path.relative_to(root)
        tracked = query('ls-files', '--full-name', '--error-unmatch',
                        '--', source_path.name)
        if tracked != relative.as_posix():
            raise ValueError('Source is not the tracked reference file')
        revision = query('rev-parse', '--verify', 'HEAD')
        dirty = bool(query('status', '--porcelain', '--untracked-files=normal'))
        return {'git_revision': revision, 'git_dirty': dirty,
                'git_unavailable_reason': None}
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        return {**unavailable, 'git_unavailable_reason': str(exc) or type(exc).__name__}


def _capture_source_identity() -> dict:
    """Capture exact reference bytes and best-effort source repository identity."""
    source = Path(__file__).resolve()
    identity = {'reference': 'examples/thermal_experiment.py',
                'digest_algorithm': 'sha256',
                'digest_scope': 'exact reference script bytes at production',
                'digest': None, 'digest_unavailable_reason': None}
    try:
        identity['digest'] = hashlib.sha256(source.read_bytes()).hexdigest()
    except OSError as exc:
        identity['digest_unavailable_reason'] = str(exc) or type(exc).__name__
    identity.update(_query_source_git(source))
    return identity


def _make_manifest(config: ThermalConfig, group: DataGroup,
                   result: RunResult, delay: int = 1) -> dict:
    """Build and validate a completed-acquisition snapshot before persistence."""
    if result.acquisition != 'completed':
        raise ValueError('outcome.acquisition: manifest requires completed acquisition')
    manifest = {
        'schema': 'softlab.thermal-reference', 'version': 1 if delay else 2,
        'run_id': str(group.id), 'configuration': asdict(config),
        'model': {**_MODEL, 'units': dict(_MODEL['units'])},
        'procedure': {'name': 'thermal_count',
                      'advancement': 'once_before_each_getter_row',
                      'command': 'constant_power', 'requested_points': config.points},
        'columns': group.record('thermal_trace').columns,
        'clock': {'dt': config.dt, 'simulation_unit': 's',
                  **_timing(delay)},
        'software': {'softlab_version': softlab.__version__,
                     'python_version': platform.python_version()},
        'source': _capture_source_identity(),
        'outcome': {'snapshot': 'completed_acquisition_before_persistence',
                    'acquisition': result.acquisition,
                    'requested_rows': result.requested_rows,
                    'completed_rows': result.completed_rows,
                    'final_tick': result.final_tick,
                    'final_sim_time': result.final_sim_time,
                    **{key: 'not_yet_established' for key in
                       ('persistence', 'plotting', 'cleanup', 'overall')}},
    }
    manifest = _check_manifest(manifest)
    _validate_manifest_data(manifest, group)
    return manifest


def _validate_manifest_data(manifest: dict, group: DataGroup) -> None:
    """Check disk-derived rows and independent Euler recurrence without acquisition."""
    manifest = _check_manifest(manifest)
    if manifest['run_id'] != str(group.id):
        raise ValueError('run_id: differs from stored group identity')
    if list(group.records) != ['thermal_trace']:
        raise ValueError('columns: expected one thermal_trace record')
    record = group.record('thermal_trace')
    if record.columns != manifest['columns']:
        raise ValueError('columns: differ from stored definitions')
    config = manifest['configuration']
    n, dt = config['points'], config['dt']
    if len(record) != n:
        raise ValueError('outcome.completed_rows: differs from stored row count')
    for key in ('configuration', 'timing'):
        text = group.get_meta(key)
        if type(text) is not str:
            raise ValueError(f'{key}: missing legacy text metadata')
        legacy = json.loads(text)
        expected = config if key == 'configuration' else _timing(
            1 if manifest['version'] == 1 else 0)
        if legacy != expected:
            raise ValueError(f'{key}: differs from manifest')
    values = record.table
    if not np.isfinite(values.to_numpy(dtype=float)).all():
        raise ValueError('samples: nonfinite numeric value')
    ticks = np.arange(1, n + 1)
    delay = 1 if manifest['version'] == 1 else 0
    expected_columns = {'tick': ticks, 'measurement_source_tick': ticks - delay,
                        'sim_time_s': ticks * dt,
                        'commanded_power_w': np.full(n, config['power'])}
    temperature = config['initial_temperature']
    expected_temperatures = []
    for _ in range(n):
        if delay:
            expected_temperatures.append(temperature)
        temperature += dt * (config['power'] - config['conductance'] *
                             (temperature - config['ambient_temperature'])) / config['heat_capacity']
        if not delay:
            expected_temperatures.append(temperature)
    expected_columns['temperature_degc'] = expected_temperatures
    for key, expected in expected_columns.items():
        actual = values[key].to_numpy(dtype=float)
        if key in ('tick', 'measurement_source_tick'):
            matches = np.array_equal(actual, expected)
        else:
            matches = np.allclose(actual, expected, rtol=0, atol=1e-10)
        if not np.isfinite(expected).all() or not matches:
            raise ValueError(f'samples.{key}: inconsistent declared recurrence/timing')


def inspect_stored(path: Path, group_id: Optional[str] = None
                   ) -> Tuple[DataGroup, Optional[dict]]:
    """Inspect a disposable copy; None context means absent legacy envelope only."""
    if group_id is None:
        with h5py.File(path, 'r') as disk:
            identities = list(disk.keys())
        if len(identities) != 1:
            raise ValueError('group_id: require exactly one stored group or explicit ID')
        group_id = identities[0]
    group = _reload_group(path, group_id)
    text = group.get_meta(_MANIFEST_KEY)
    manifest = _decode_manifest(text) if text is not None else None
    return group, manifest


def _load_group(backend: HDF5DataBackend, group_id: str) -> DataGroup:
    """Load once from a copy and hydrate its verified disk text metadata."""
    attrs = backend.file[group_id].attrs
    for key in ('id', 'name', 'timestamp', 'backend') + _META_KEYS:
        if key not in attrs or not isinstance(attrs[key], str):
            raise RuntimeError(f'Missing or nontext disk metadata: {key}')
    if attrs['id'] != group_id:
        raise RuntimeError('Disk group identity mismatch')
    metadata = {key: attrs[key] for key in _META_KEYS}
    if _MANIFEST_KEY in attrs:
        if type(attrs[_MANIFEST_KEY]) is not str:
            raise ValueError('thermal_manifest: present value is not text')
        metadata[_MANIFEST_KEY] = attrs[_MANIFEST_KEY]
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
        if _MANIFEST_KEY in loaded.meta:
            _validate_manifest_data(
                _decode_manifest(loaded.get_meta(_MANIFEST_KEY)), loaded)
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
        raise _HelperFailure(primary, errors) from primary
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
        raise _HelperFailure(primary, errors) from primary


def execute(config: ThermalConfig, output_dir: Path, *,
            stop: Optional[StopRequest] = None,
            after_row: Optional[Callable[[RunResult], None]] = None,
            adapter: str = 'coordinated') -> RunResult:
    """Execute once, retaining partial rows and operational failure evidence.

    Validation and output-collision errors raise before acquisition. Operational
    exceptions return a terminal result; BaseException is reraised after cleanup.
    The synchronous after_row callback must not mutate execution state.
    """
    _validate_config(config)
    if adapter not in ('coordinated', 'direct'):
        raise ValueError('Unsupported adapter')
    delay = 1 if adapter == 'coordinated' else 0
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError(
            'The synchronous thermal example cannot run inside an event loop')
    data_path, plot_path = _prepare_output(output_dir)
    stop = stop if stop is not None else StopRequest()
    result = RunResult(config.points, data_path, plot_path, stop)
    coordinator = None
    writer = None
    original = None
    loaded = None
    primary = None
    errors = []
    stage = 'setup'
    try:
        coordinator, heater, thermometer = _build_simulation(config)
        if adapter == 'direct':
            heater = _ControlAdapter(heater)
            thermometer = _DirectMeasurement(coordinator.station.object('body'))
        result._coordinator = coordinator
        stage = 'acquisition'
        original = _acquire(config, coordinator, heater, thermometer,
                            stop=stop, after_row=after_row, result=result, delay=delay)
        result.group = original
        result.acquisition = 'completed'
        result.persistence = 'failed'
        stage = 'persistence'
        original.set_meta(_MANIFEST_KEY, _encode_manifest(
            _make_manifest(config, original, result) if delay else
            _make_manifest(config, original, result, delay=delay)))
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
        result.loaded = loaded
        result.persistence = 'verified'
        result.plotting = 'failed'
        stage = 'plotting'
        _plot_record(loaded.record('thermal_trace'), plot_path)
        result.plotting = 'completed'
    except _AcquisitionEnd:
        primary = result.cause
    except _HelperFailure as exc:
        errors.extend(exc.cleanup_errors)
        primary = exc.primary
        result.cause = exc.primary
    except BaseException as exc:
        primary = exc
        if result.cause is None:
            result.cause = exc
        if stage == 'acquisition':
            result.acquisition = 'failed'
    finally:
        # Independent inspections and caller-owned cleanup must not mask the
        # first interruption or suppress later cleanup attempts.
        try:
            if (result.final_tick is None and coordinator is not None
                    and coordinator.prepared):
                result.final_tick = coordinator.tick
                result.final_sim_time = coordinator.sim_time
        except BaseException as exc:
            errors.append(f'Boundary inspection: {exc}')
            if not isinstance(exc, Exception) and (
                    primary is None or isinstance(primary, Exception)):
                primary = exc
        try:
            if result.group is not None:
                result.completed_rows = len(result.group.record('thermal_trace'))
        except BaseException as exc:
            errors.append(f'Row inspection: {exc}')
            if not isinstance(exc, Exception) and (
                    primary is None or isinstance(primary, Exception)):
                primary = exc
        try:
            if writer is not None and (writer.file is not None or writer.connected):
                _disconnect(writer)
        except BaseException as exc:
            errors.append(f'Writer cleanup: {exc}')
            if not isinstance(exc, Exception) and (
                    primary is None or isinstance(primary, Exception)):
                primary = exc
    result.cleanup_errors.extend(errors)
    result.stage = stage
    result.terminal = True
    if primary is not None and not isinstance(primary, Exception):
        try:
            print(_diagnostic(result), file=sys.stderr)
        except BaseException:
            pass
        raise primary
    return result


def run(config: ThermalConfig, output_dir: Path) -> DataGroup:
    """Return verified reopened data, raising RunFailure on operational failure."""
    result = execute(config, output_dir)
    if not result.overall_success:
        raise RunFailure(result) from result.cause
    return result.loaded


def reset_after_run(result: RunResult) -> None:
    """Reset retained model only after terminal, drained and closed execution.

    Raises RuntimeError when lifecycle prerequisites are unmet. Coordinator reset
    failures propagate without rewriting the terminal execution snapshot.
    """
    if (not result.terminal or not result.work_drained or not result.loop_closed
            or result._coordinator is None
            or result.acquisition not in ('completed', 'cancelled', 'failed')):
        raise RuntimeError('Reset requires terminal, drained and closed execution')
    result._coordinator.reset()


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Parse the documented command; announce completion only after cleanup."""
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--output-dir', type=Path)
    mode.add_argument('--inspect-file', type=Path)
    parser.add_argument('--group-id')
    for key, value in asdict(ThermalConfig()).items():
        parser.add_argument('--' + key.replace('_', '-'), default=value,
                            type=int if key == 'points' else float)
    parser.add_argument('--stop-after', type=int)
    parser.add_argument('--adapter', choices=('coordinated', 'direct'),
                        default='coordinated')
    arguments = vars(parser.parse_args(argv))
    inspect_file = arguments.pop('inspect_file')
    group_id = arguments.pop('group_id')
    stop_after = arguments.pop('stop_after')
    adapter = arguments.pop('adapter')
    output_dir = arguments.pop('output_dir')
    try:
        if inspect_file is not None:
            if stop_after is not None:
                raise ValueError('--stop-after requires generation mode')
            with redirect_stdout(sys.stderr):
                stored, manifest = inspect_stored(inspect_file, group_id)
            record = stored.record('thermal_trace')
            print(json.dumps({
                'context': 'available' if manifest is not None else 'legacy_unavailable',
                'manifest': manifest, 'run_id': str(stored.id),
                'columns': [column['name'] for column in record.columns],
                'rows': record.table.to_numpy(dtype=float).tolist(),
            }, sort_keys=True, allow_nan=False))
            return 0
        if group_id is not None:
            raise ValueError('--group-id requires inspection mode')
        config = ThermalConfig(**arguments)
        if stop_after is not None and not 0 <= stop_after <= config.points:
            raise ValueError('--stop-after must be between zero and points')
        stop = StopRequest()
        if stop_after == 0:
            stop.request()

        def after_row(result: RunResult) -> None:
            if result.completed_rows == stop_after:
                stop.request()

        result = execute(config, output_dir, stop=stop, after_row=after_row,
                         adapter=adapter)
        if not result.overall_success:
            raise RunFailure(result) from result.cause
        group = result.loaded
    except Exception as exc:
        print(f'Thermal experiment failed: {exc}', file=sys.stderr)
        return 1
    print(f'Complete: run {group.id}')
    print(f'HDF5: {output_dir.absolute() / "thermal.h5"}')
    print(f'PNG: {output_dir.absolute() / "thermal.png"}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
