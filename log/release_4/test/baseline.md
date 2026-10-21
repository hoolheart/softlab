# M01-W01-001 baseline and independent oracle

Owner: sw-mike (Tester), 2026-10-18. Code baseline: setup `828a397` on
`codex/m01-w01-thermal-example`. No production code changed.

## Actual checks

Python `.venv/bin/python`: 3.13.15 conda-forge, Clang 19.1.7, macOS.
softlab 0.3.0; numpy 2.5.3, scipy 1.18.1, pandas 3.0.6,
matplotlib 3.11.2, h5py 3.16.0, pyvisa 1.16.2.

| Command | Actual result |
| --- | --- |
| `.venv/bin/python -m unittest discover -s tests -p 'test_*.py'` | Exit 0; 210 tests, 1.567s, OK, no skips reported. |
| `.venv/bin/python -m compileall -q softlab` | Exit 0, no compilation output. |
| `.venv/bin/python -c "import softlab; print(softlab.__version__)"` | Exit 0; 0.3.0. |
| Same discovery with `-W error` and temporary writable caches/Agg | Exit 0; 210 tests, 1.518s, OK, no skips or Python warnings reported. |
| Same compileall and import with `-W error` and caches/Agg | Exit 0, version 0.3.0, no warnings/output besides version. |
| Independent oracle below, `-W error`, caches/Agg | Exit 0; eight Decimal/literal/clock assertions pass, final object 25.695327899999995. |

Initial default-cache invocation emitted Matplotlib unwritable home-cache and
fontconfig cache diagnostics. This is a sandbox environment limitation, not a
missing library or production defect; no code/environment installation change
was made. Reruns set `MPLCONFIGDIR=/tmp/softlab-w01-mpl`,
`XDG_CACHE_HOME=/tmp/softlab-w01-cache`, `MPLBACKEND=Agg`; first cache creation
printed its informational font-building message. Subsequent oracle/compile/import
were quiet. These named checks certify no Python warnings only; no general
lint/static-analysis claim. Full test output was captured in temporary files,
not committed. No real instruments or experimental datasets were accessed.

## Executed independent oracle

The following temporary script was run with `.venv/bin/python -W error` and
the three environment variables above. It uses existing public simulation APIs,
not a future example callback. Its Decimal expectation derives the closed form
independently, and also checks the literal PRD trace. No production changes are
needed for this topology. An initial fixture attempt reused `temperature` as
both state/output and was correctly rejected; corrected fixture uses distinct
`reading` output, consistent with the documented namespace contract.

```python
from decimal import Decimal
from softlab.tu.simulation import (
    Coordinator, Endpoint, MockControl, MockMeasurement, SimulatedObject,
)
from softlab.tu.station import Station

s = Station('oracle-fixture')
o = SimulatedObject(
    'body', inputs={'power': 0.0, 'dt': 1.0},
    states={'temperature': 20.0}, outputs=['reading'],
    evolve=lambda u, x: {'temperature': x['temperature'] + u['dt'] *
                        (u['power'] - (x['temperature'] - 20.0)) / 10.0},
    observe=lambda x: {'reading': x['temperature']},
)
s.add_object(o)
s.sim_dt = 1.0
c = Coordinator(s)
c.enroll('body')
h = MockControl('heater', 0.0)
m = MockMeasurement('thermometer', 20.0)
c.add_control(h)
c.add_measurement(m)
c.connect(Endpoint('heater'), Endpoint('body', 'power'))
c.connect(Endpoint('body', 'reading'), Endpoint('thermometer'))
c.bind_interval('body', 'dt')
c.prepare()
h.write(10.0)
assert c.tick == 0 and m.read() == 20.0
assert o.get_state('temperature') == 20.0
expected = [20.0, 21.0, 21.9, 22.71, 23.439, 24.0951,
            24.68559, 25.217031]
actual = []
for k, want in enumerate(expected, 1):
    c.step()
    actual.append(m.read())
    independent = Decimal(30) - Decimal(10) * Decimal('0.9') ** (k - 1)
    assert abs(actual[-1] - float(independent)) <= 1e-10
    assert abs(actual[-1] - want) <= 1e-10
    assert c.tick == k and c.sim_time == float(k)
assert abs(o.get_state('temperature') - 25.6953279) <= 1e-10
```

Observed delivered values:
`[20.0, 21.0, 21.9, 22.709999999999997, 23.438999999999997,
24.095099999999995, 24.685589999999994, 25.217030999999995]`.
No scheduler/backend/figure was created by the oracle; the graph was discarded
without reset/extra steps. Full existing suite exercises its own resource cleanup.

## Scope and remaining evidence

No PRD contradiction or required missing dependency established. Existing HDF5
column enumeration/string metadata/reserved-attribute load behavior is recorded
in the test plan for Designer compatibility, not silently repaired. Preexisting
architecture debt OBS-004/005/006 and DEFECT-2 remains outside this task.
Future example assertions, red/green execution, guide command, independent review,
completion principles, candidate CI and integration are pending. Local Python 3.9
was not run; neither this baseline nor configured CI matrix certifies candidate
CI. UI/Figma, hardware and publication are N/A.
