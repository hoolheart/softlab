# M02-W01 caller inventory completeness check (AC01)

Reviewer: sw-celeste (Software Designer) | Date: 2026-10-30 Asia/Shanghai
Verdict: **COMPLETE** — no missed caller or usage site changes the W02 boundary.
Condition from [scope agreement](scope-agreement.md), item 1; checks ownership/
impact claims of the [evidence decision](../design/evidence-decision.md).

## Method

Desk check only. `rg` over `softlab/`, `tests/`, `examples/` and all notebooks
for `count`, `scan`, `run_process`, `Scheduler`, `get_scheduler`,
`_SchedulerImpl`, `commit`, `join`, `reset`; `.venv`, build artifacts and git
internals excluded. Word matches in `jin/`, `mu/`, `shui/`, `tu/` and in
`README.rst` verified incidental (docstrings, variable names) — no API callers
outside `huo`.

## Inventory (file → usage)

### Production (`softlab/huo/` only)
- `scheduler/scheduler.py` — `Scheduler` interface definition.
- `scheduler/impl_scheduler.py` — `_SchedulerImpl`, module-global `_scheduler`,
  `get_scheduler()`; `__main__` demo via singleton.
- `process/process.py` — `Process.commit/join/reset`, `run_process`,
  `SimpleProcessWrapper`; `__main__` demo.
- `process/simple.py`, `process/composite.py` — `commit/join` overrides;
  `__main__` demos via `get_scheduler()`.
- `process/common.py` — `count`, `scan`, `grid_scan` builders.
- `process/__init__.py`, `scheduler/__init__.py` — public exports.

### Tests
- `test_compatibility.py` — `count`, `scan`, `run_process`, `get_scheduler`
  (singleton).
- `test_tu_integration.py`, `test_tu_simulation_integration.py` — `count`,
  `scan`, `run_process` through the `get_scheduler()` singleton.
- `test_huo_execution_contracts.py` — `count`, `run_process`; direct
  `_SchedulerImpl` on a borrowed loop (stop-cancels-without-drain).
- `test_thermal_example.py`, `test_thermal_metadata_contracts.py`,
  `test_thermal_manifest.py` — thermal example: `_CountScheduler`,
  `_make_scheduler`, `Scheduler`/`Action` contract fixtures.
- `test_m02_w01_characterization.py` — direct `_SchedulerImpl`, `get_scheduler`,
  thermal `_CountScheduler`, `Process.commit/join/reset` (the W01 suite).
- `test_common_proc.ipynb` — `count`, `scan`, `grid_scan`, `run_process`,
  `get_scheduler`.
- `test_basic.ipynb`, `test_visa.ipynb`, `sprike_plotly.ipynb` — no API callers
  (incidental words only).

### Examples
- `examples/thermal_experiment.py` — `count`, `Scheduler`, `Action`,
  `_CountScheduler`, `Process.commit/join/reset` (the W02 prototype client).

## Completeness verdict

Every usage site falls into a category the evidence decision already owns:
public-interface consumers on the unchanged `run_process`/`Process` path;
singleton `get_scheduler()` users (legacy behavior preserved); the thermal
prototype and its tests (W02 seam source); the characterization suite; and
direct `_SchedulerImpl` probes consistent with refuted private ownership (F-01).
No caller outside these categories exists; none alters the W02 boundary.

Non-gap observations: (1) `grid_scan` is a public API consumed via
`run_process` (notebook) but is not named in decision section 3.1's interface
enumeration; it is covered transitively by "legacy imports and behavior
preserved" — recommend noting it in the T01 consolidation, no boundary impact.
(2) `test_huo_execution_contracts.py`'s direct `_SchedulerImpl` usage is
consistent with the recorded ownership findings and changes nothing.

**Verdict: COMPLETE.**
