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
Group metadata is minimal text: `example`, JSON `configuration` and JSON `timing`.
The timing text states post-step acquisition, one-tick measurement delay,
source `tick-1` and Unix wall seconds. Rich setup/environment provenance is
outside this weekly example.

The runner checks save/connect results and backend errors, disconnects the writer,
then reopens through a new backend on an owned temporary copy. It compares UUID,
name, timestamp, declared metadata, ordered schema and every numeric value
(absolute tolerance `1e-10`). It plots that reopened record with existing
`plot_record`, labels seconds/degC, saves a headless PNG, and closes new figures.

There are existing backend limitations: plain HDF5 `load_group` consumes reserved
attributes on its open file, and the DataGroup constructor drops metadata.
This example preserves `thermal.h5` by copying it before legacy loading and
hydrating only its three declared text metadata values read from that same copy.
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
synchronous work cannot be preempted reliably. This example adds no cancellation,
resume, partial-run recovery, hardware adapter or physical-stop guarantee.

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
