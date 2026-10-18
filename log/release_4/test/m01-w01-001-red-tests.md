# M01-W01-001 executable red-test handoff

Owner: sw-mike (Tester), 2026-10-18. Approved design: `5fdb984`, Architect
DR-01 closure/handoff `3a5d9f1`. Test commit: `840df0e` (pushed).
Production example is intentionally absent at this stage.

## Delivered assertions

`tests/test_thermal_example.py` contains 18 unittest methods with parameterized
subcases. Importlib registers and loads the designed example path without CLI
execution. Fixtures use core dependencies, mocks and temporary synthetic files.
Assertions cover independent Decimal/literal numeric oracle, exact eight steps
and clock, validation/collision/symlink preflight, bounded supported boundary
configuration, ordered units/schema/text metadata, source SHA256/reserved
attributes and UUID/name/timestamp across successive fresh-copy reloads and
fresh runs, independent legacy read on separate copy, reopened plotting line
values/axes/figure ownership, real model-callback and post-step sample failure,
false/None/raised backend operations, disk metadata/identity corruption and value
mismatch, partial-copy cleanup, reader connection failure, suppressed disconnect
failure plus fallback handle closure, scratch cleanup obstruction, plot/savefig
failures, primary plus cleanup diagnostics, independent scheduler cleanup/global
singleton/default-loop isolation, running-loop rejection, repeated CLI execution,
collision unsuccessful exit, and public serial Scheduler Action dispatch.

No expected fixture calls the future production evolution callback. Model failure
is injected into the fixture object's callback to exercise atomic tick behavior;
measurement failure occurs at published tick 3. Tests never repair production
HDF5/scheduler defects or call destructive legacy load on delivered data. Scratch
cleanup obstruction is explicitly removed by the test's finalizer after asserting
that it remains a visible failure. Owned loop release assertions inspect the
example scheduler loop (not the borrowed thread loop). Developer executable-test
review follows; any seam contradiction returns to Designer rather than implicit
implementation redesign.

## Actual red and syntax evidence

Executed from repository root on the Python 3.13.15 environment recorded in
[baseline](baseline.md):

```sh
MPLCONFIGDIR=/tmp/softlab-w01-mpl XDG_CACHE_HOME=/tmp/softlab-w01-cache MPLBACKEND=Agg \
  .venv/bin/python -W error -m unittest discover -s tests -p 'test_thermal_example.py'
.venv/bin/python -m py_compile tests/test_thermal_example.py
git diff --check
```

Unittest exits **1**, one loader error: `FileNotFoundError` for
`examples/thermal_experiment.py`, raised from `SPEC.loader.exec_module(thermal)`.
`Ran 1 test ... FAILED (errors=1)` describes the loader placeholder, **not**
execution of the 18 test methods. This is the expected red stage: missing approved
example implementation, not a missing environment dependency. No warning was
reported by this named invocation. Py_compile and diff check exit **0**. Temporary
captured output is not committed. The prior 210-test baseline remains evidence
only for preimplementation code; no postimplementation/full-suite pass, test
failure closure, CI, integration or release acceptance is claimed.

Next: Developer implementability review of these committed assertions, then
production implementation. Independent green execution, remaining static guide/
diff checks, full regression, named warning checks and configured candidate CI
remain required. Tester alone closes observed runtime failures after execution.
