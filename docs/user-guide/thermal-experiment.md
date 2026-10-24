# A reproducible simulated thermal experiment

This reference connects softlab's existing layers into a complete local run:
`tu` owns the thermal model, heater and thermometer; `huo` performs a bounded
count; `shui` records and saves observations; `jin` plots the reopened result.
It uses synthetic data only and never discovers or contacts instruments.
The helpers in `examples/thermal_experiment.py` are example code, not supported
public run APIs. No notebook or display server is required.

## Setup and command

From the repository root, activate a Python environment with the core package
installed. The repository's local environment can be selected with
`conda activate "$PWD/.venv"`. Install with:

```sh
python -m pip install -e .
```

Run with a new or empty output directory:

```sh
MPLBACKEND=Agg python examples/thermal_experiment.py --output-dir /tmp/thermal-run-01
```

Use another directory, such as `/tmp/thermal-run-02`, for the next invocation.
An existing nonempty directory, file or directory symlink is refused before
acquisition. Nothing in it is overwritten. This preflight does not protect
against another process writing to the directory concurrently; give each run
its own directory.

A successful command prints `Complete`, the fresh run UUID and absolute paths
to `thermal.h5` and `thermal.png`, after verified reopening, plotting and cleanup.
HDF5 connection diagnostics can appear before that final message and do not
indicate overall success. A nonzero exit and stderr diagnostic indicate failure;
argument syntax errors use argparse's exit status. For restricted environments,
set `MPLCONFIGDIR` and `XDG_CACHE_HOME` to writable temporary directories if
Matplotlib/fontconfig reports an unwritable cache.

## Model and timing

The educational explicit Euler model is:

```text
T_next = T + dt * (P - G * (T - T_ambient)) / C
```

Defaults are `T_initial = T_ambient = 20 degC`, `C = 10 J/K`, `G = 1 W/K`,
`dt = 1 s`, fixed heater command `P = 10 W`, and eight samples. The command
accepts `--initial-temperature`, `--ambient-temperature`, `--heat-capacity`,
`--conductance`, `--dt`, `--power`, and `--points`. All scalar inputs must be
finite real numbers, C/G/dt positive, and points a positive integer; supported
stability is `0 < dt * G / C <= 1`. Finite negative temperatures or power are
allowed. Extreme finite inputs may still overflow during evolution; nonfinite
observations fail before persistence. This is neither a solver nor a calibrated
claim about physical thermal equipment.

The topology is one heater control → body power input and one body reading
output → thermometer measurement, with the body's dt input bound to the
station interval. `prepare()` establishes boundary zero; the command is written
once before acquisition. Each count's before-get hook advances the coordinator
exactly once, then getters read the delivered values and append a complete row.
Reads, writes and teardown never advance simulation. No wall-clock sleeps pace
the model. See the [simulation contracts](simulation.md) and
[common acquisition hooks](../../softlab/huo/process/common.py).

Coordinator edges deliver the previous boundary's values. At tick 1, the heater
command affects the body's first candidate evolution, while the thermometer
receives the body's boundary-zero output. Eight delivered temperatures are:

```text
20, 21, 21.9, 22.71, 23.439, 24.0951, 24.68559, 25.217031 degC
```

The body itself reaches `25.6953279 degC` at boundary eight. Every row pairs
simulation acquisition time `k * dt` with the delivered reading sourced from
boundary `k - 1`. The PNG uses acquisition time, so its first point at one second
shows the initial temperature. It does not remove or compensate for this latency.

## Stored result and safe inspection

The `thermal_experiment` DataGroup contains a `thermal_trace` record with these
columns in HDF5-compatible alphabetical order:

| Column | Meaning | Unit |
| --- | --- | --- |
| commanded_power_w | Staged heater command | W |
| measurement_source_tick | Object output's source boundary | 1 |
| sim_time_s | Published acquisition time | s |
| temperature_degc | Delivered thermometer value | degC |
| tick | Published acquisition boundary | 1 |
| timestamp | AtomJob's wall-clock acquisition timestamp | s |

Only temperature is marked dependent. `timestamp` is Unix wall time, separate
from simulation time; exclude it and the UUID when comparing repeat traces.
Group metadata retains `example`, JSON `configuration` and JSON `timing`,
and adds the closed JSON-text `thermal_manifest` envelope for new complete runs.
The timing text states post-step acquisition, one-tick measurement delay,
source `tick-1` and Unix wall seconds. The manifest below captures bounded
producer context; it does not lock dependencies or certify physical calibration.

The runner checks save/connect results and backend errors, disconnects the writer,
then reopens through a new backend on an owned temporary copy. It compares UUID,
name, timestamp, declared metadata, ordered schema and every numeric value
(absolute tolerance `1e-10`). It plots that reopened record with existing
`plot_record`, labels seconds/degC, saves a headless PNG, and closes new figures.

There are existing backend limitations: plain HDF5 `load_group` consumes reserved
attributes on its open file, and the DataGroup constructor drops metadata.
This example preserves `thermal.h5` by copying it before legacy loading and
hydrating its three legacy text values plus the optional manifest from that same
copy. A present manifest is decoded and checked against the actual stored rows.
It never repairs missing disk values using an earlier in-memory group. Returned
backend information points to the delivered file, not the deleted copy. These
are local compatibility measures, not storage-format changes or library fixes.
For inspection, use h5py read-only, or load the example's `_reload_group` helper
on a fresh copy as the runner does. For example, from the repository root:

```python
import importlib.util
from pathlib import Path
import sys
import h5py

spec = importlib.util.spec_from_file_location(
    'thermal_reference', 'examples/thermal_experiment.py')
example = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = example
spec.loader.exec_module(example)
path = Path('/tmp/thermal-run-01/thermal.h5')
with h5py.File(path, 'r') as disk:
    run_id = next(iter(disk))
group = example._reload_group(path, run_id)
print(group.meta)
print(group.record('thermal_trace').table)
```

Never pass an experimental original to the legacy destructive reader merely to
inspect it. The reference helper makes a new disposable copy for every call;
the delivered source retains its identity and can be inspected repeatedly.

## Failure and ownership

A failed tick publishes no new boundary. A sample failure after a successful
tick can leave one advanced boundary without a row; there is no rollback or
automatic retry. Acquisition must finish before saving or plotting. Failures
include stage, available final boundary/row count and artifact status. Failed
save/reopen/verification leaves any HDF5 file explicitly incomplete/unverified;
saving is not atomic. Plotting failure retains verified stored data but reports
overall failure and a potentially incomplete PNG. Do not interpret the presence
of a file as command completion. Cleanup failures remain visible alongside the
primary error, including residual temporary paths when removal is obstructed.

Every invocation creates a fresh graph, process, record, UUID, backend and a
small example-only serial Scheduler extension. Its independently owned event
loop executes public Action bodies for this count; it rejects threaded/chained
actions and is not a general scheduler. It neither replaces nor stops the global
scheduler, sets the thread event loop, nor closes borrowed loops. `run` is
synchronous and rejects invocation inside an already running event loop before
creating the experiment. Run the CLI separately for notebook use.

Cleanup ceases stepping, inspects the final committed boundary and discards the
private graph without `Coordinator.reset()`. It independently attempts scheduler
stop, owned task draining/loop close, backend disconnect/raw-handle fallback,
scratch removal and figure closure, preserving preexisting figures. Interruption
gets best-effort cleanup when control returns to Python; process death and
synchronous work cannot be preempted reliably. The cooperative controls below stop acquisition at software boundaries. There is
no durable resume, automatic retry, hardware adapter or physical-stop guarantee.

## Verification

The independent fixture and failure/ownership assertions are in
[the acceptance tests](../../tests/test_thermal_example.py). Run from the root
with writable caches and Agg as appropriate:

```sh
MPLBACKEND=Agg python -m unittest discover -s tests -p 'test_thermal_example.py'
python -m unittest discover -s tests -p 'test_*.py'
python -m compileall -q softlab examples tests
python -c "import softlab; print(softlab.__version__)"
```

Local execution evidence and configured candidate CI are separate release gates;
a configured Python matrix does not by itself certify either version.

## Controlled interruption and explicit inspection

From the repository root, a stop after three complete rows exits 1 and creates
no HDF5 or PNG. Choose a fresh output directory each time:

```sh
python examples/thermal_experiment.py --output-dir /tmp/thermal-stop-03 --stop-after 3
```

`--stop-after 0` stops before any tick; `--stop-after 8` is a late request for
the default eight-point run and completes normally. A request is distinct from
acknowledgment: synchronous evolution/getters finish their current cycle, then
acknowledgment prevents another cycle. Failure in that cycle takes precedence.
This is cooperative software cancellation and cannot interrupt blocking work.

For in-memory partial inspection, reset and two fresh repeats, run this from the
repository root (all files remain in an automatically removed temporary folder):

```sh
python - <<'PYTHON'
import importlib.util
from pathlib import Path
import sys
import tempfile

spec = importlib.util.spec_from_file_location(
    'thermal_controlled', 'examples/thermal_experiment.py')
example = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = example
spec.loader.exec_module(example)
with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)
    stop = example.StopRequest()
    def after_row(result):
        if result.completed_rows == 3:
            stop.request()
    partial = example.execute(example.ThermalConfig(), root / 'partial',
                              stop=stop, after_row=after_row)
    assert partial.acquisition == 'cancelled'
    assert partial.completed_rows == partial.final_tick == 3
    assert partial.stop.acknowledged and partial.work_drained and partial.loop_closed
    assert partial.persistence == 'not_attempted'
    print(partial.group.record('thermal_trace').table)
    example.reset_after_run(partial)
    assert partial.final_tick == 3  # terminal snapshot is preserved
    runs = [example.execute(example.ThermalConfig(), root / str(i)) for i in (1, 2)]
    assert all(result.overall_success for result in runs)
    assert runs[0].group.id != runs[1].group.id
    columns = [c for c in runs[0].group.record('thermal_trace').table
               if c != 'timestamp']
    assert runs[0].group.record('thermal_trace').table[columns].equals(
        runs[1].group.record('thermal_trace').table[columns])
PYTHON
```

These helpers belong to this example, not the supported library API. `execute`
returns a terminal `RunResult` for operational exceptions, with original `cause`,
complete original `group`, optional verified `loaded`, actual final tick/time,
separate acquisition/persistence/plotting statuses and ordered cleanup errors.
Validation/output collisions still raise before acquisition. `run` preserves its
successful reopened-DataGroup return and raises `RunFailure` with `.result` and
original exception chaining on operational failure. BaseException interruption
is reraised after best-effort cleanup. Callbacks inspect live evidence and must
not mutate group/configuration/lifecycle.

A model failure after row three retains three rows/tick three; a sampling failure
after the next successful step retains three rows/tick four. Neither fabricates a
row, retries, persists partial data nor resets during cleanup. Save/reload failure
leaves acquisition completed and persistence failed. Plot failure leaves storage
verified while the overall result fails. Cleanup obstruction also prevents overall
success and remains visible alongside the original cause.

`reset_after_run` requires terminal acquisition, drained work and a closed owned
loop. It explicitly resets the retained model and starts no acquisition; reset
factory failure propagates with the previous committed model state preserved.
Prior terminal evidence remains available. Repetition uses a fresh execute call,
process, stop request, record and resources; legacy Process.reset is not a promise
that a cancelled process can be reused or that its record is cleared. Successful
repeats have equal numeric traces, distinct UUIDs and fresh files. Only completed acquisition receives the stored manifest described below;
interruption/failure partials remain in memory without a durable outcome artifact.

## Typed stored context and a fresh-process inspector

New completed acquisitions contain `thermal_manifest`, schema
`softlab.thermal-reference`, integer version `1`, encoded as one JSON string in
the same HDF5 group as the samples. No sidecar or backend-wide serializer is used.
The schema is closed at every object; missing/undeclared fields, duplicate keys,
wrong types, arbitrary objects and nonfinite numbers are rejected. Encoding has
no fallback stringification. Declared real fields decode as Python `float` and
counts/version as exact `int`, excluding booleans. JSON null is permitted only
for the declared unavailable source-identity fields, and booleans only for column
dependency and available Git dirty state.

| Field | Meaning and declared types |
| --- | --- |
| schema, version, run_id | Fixed schema text, integer1, canonical UUID text tied to the stored group. |
| configuration | All seven ThermalConfig fields; points positive int, other values finite floats with the same stability validation as generation. |
| model | Fixed lumped thermal body/explicit Euler equation, educational validity text, and complete parameter-unit object. |
| procedure | Fixed thermal_count/constant-power/once-before-row semantics and requested_points int. |
| columns | Ordered list of name/label/unit strings plus dependent bool; exact six-column stored definitions above. |
| clock | dt float, seconds/post_step, integer one-boundary delay, tick-1 source and Unix wall timestamp meanings. |
| software | Producer softlab_version and python_version strings. |
| source | Reference path/digest algorithm/byte scope strings; digest/revision strings or null; dirty bool or null; explicit unavailable-reason strings when unavailable. |
| outcome | Named completed-acquisition snapshot, requested/completed rows and final_tick ints, final_sim_time float; later persistence/plot/cleanup/overall states explicitly not_yet_established. |

Generate and inspect in two separate Python invocations from the repository root:

```sh
python examples/thermal_experiment.py --output-dir /tmp/thermal-context-01
python examples/thermal_experiment.py --inspect-file /tmp/thermal-context-01/thermal.h5
```

Inspection emits exactly one JSON object on stdout with `context`, `manifest`,
`run_id`, ordered `columns` and numeric `rows`. Backend informational messages go
to stderr in inspection mode. `context` is `available` with the typed manifest,
or `legacy_unavailable` with manifest null only when that attribute is absent.
Use `--group-id UUID` when inspecting a file containing multiple groups; omitting
it requires exactly one group. Inspection and generation output modes are mutually
exclusive; stop controls apply only to generation. The Python helper
`inspect_stored(Path(...), group_id=None)` returns `(group, manifest_or_none)`.
These remain example-local helpers, not supported library APIs.

The inspector uses the disposable-copy loader and never steps a model. It checks
UUID, exact ordered schema/units, counts/final tick/time, finite samples, actual
tick/source-boundary/time/power columns and the independently recomputed delayed
Euler temperatures. Legacy configuration/timing values must agree with the
manifest. It does not repair values from current defaults or compare producer
identity with the reader's current checkout. These checks establish internal
explanatory consistency; a coordinated rewrite of all metadata and samples can
remain self-consistent and is not authenticated by this format.

For a different declared experiment, choose another fresh path:

```sh
python examples/thermal_experiment.py --output-dir /tmp/thermal-context-changed --dt 0.5 --power 6 --points 5
python examples/thermal_experiment.py --inspect-file /tmp/thermal-context-changed/thermal.h5
```

The output recovers the changed configuration and five delayed observations.
Fresh default repeats have different UUIDs, equal configuration/numeric traces,
and different wall timestamps; source revision/state may change between producers.
The digest is SHA256 of exact reference script bytes at production, not the whole
repository, installed environment or data. Git queries are scoped to the source
repository and require a tracked source file. An available full revision is paired
with a repository worktree/index/untracked dirty flag. Dirty true means that commit
alone does not identify all producer changes. Git missing/unavailable/untracked
installation yields null revision/dirty and a reason without blocking generation.
Unreadable source bytes yield null digest with a reason. Capture is best effort,
not an atomic snapshot of concurrent checkout changes or a dependency lock.

The stored outcome is `completed_acquisition_before_persistence`: it records actual
completed acquisition and boundary before save/reload/plot/cleanup. It cannot
forecast success of those later operations. The current in-memory RunResult is
authoritative for them; verified HDF5 acquisition can coexist with overall failure
after plotting or cleanup. Cancelled/failed acquisition still skips manifest
creation/save/plot, retaining its complete partial rows only in memory. Invalid
manifest encoding fails persistence before a writer is constructed; later failed
writes may leave incomplete bytes, not an atomic or byte-free failure guarantee.

## Legacy, corruption and version policy on disposable copies

Old files without the attribute remain numerically inspectable with explicitly
unavailable manifest context. Present malformed/nontext/invalid data or unsupported
schema/version fails visibly (exit1), never downgrading to legacy or announcing
available context. There is no automatic migration. Exercise the distinctions on
copies of a synthetic run, keeping the delivered original unchanged:

```sh
python - <<'CONTEXT_FIXTURES'
from pathlib import Path
import json
import shutil
import h5py
source = Path('/tmp/thermal-context-01/thermal.h5')
for state in ('legacy', 'malformed', 'version'):
    destination = source.with_name(state + '.h5')
    if destination.exists():
        raise FileExistsError(destination)
    shutil.copy2(source, destination)
    with h5py.File(destination, 'a') as disk:
        attrs = disk[next(iter(disk))].attrs
        manifest = json.loads(attrs['thermal_manifest'])
        del attrs['thermal_manifest']
        if state == 'malformed':
            attrs['thermal_manifest'] = '{'
        elif state == 'version':
            manifest['version'] = 999
            attrs['thermal_manifest'] = json.dumps(manifest)
CONTEXT_FIXTURES
python examples/thermal_experiment.py --inspect-file /tmp/thermal-context-01/legacy.h5
# The following commands intentionally exit 1 with visible diagnostics:
python examples/thermal_experiment.py --inspect-file /tmp/thermal-context-01/malformed.h5
python examples/thermal_experiment.py --inspect-file /tmp/thermal-context-01/version.h5
```

Retain the earlier copy-only warning for the legacy backend. This envelope changes
only the thermal example's declared metadata; arbitrary softlab metadata and
Parameter values are not restricted to JSON by this feature.
