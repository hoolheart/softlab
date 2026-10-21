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

## Tester correction closure — existing public contracts

During paused development, Developer reported three assertion defects. Tester
independently read Coordinator.tick/prepared and DataGroup.record/pop_record/
add_record plus DataRecord.table. Confirmed and corrected test-only defects:

- **TEST-01 CLOSED (Tester):** before first successful prepare, tick is undefined
  and raises RuntimeError, rather than reading zero. Failed preparation now
  asserts prepared=False, zero step calls and the unreadable-tick contract.
  Correction committed/pushed at `64e72f3`.
- **TEST-02 CLOSED (Tester):** DataGroup.record returns a fresh DataRecord copy.
  Plot data is compared by ordered schema and numeric table, rather than requiring
  identity with a newly requested copy. This preserves plot-line/axes assertions.
- **TEST-03 CLOSED (Tester):** modifying a record returned by record() does not
  modify its group. The corruption fixture now pop_record/change/add_record and
  asserts the group actually holds zeroed temperatures before verification must
  reject it. Previously this fixture failed to inject the intended corruption.

Focused independent scripts ran with Python 3.13.15, `-W error`, Agg and writable
temporary cache variables from the baseline: failed-prepare lifecycle assertions
PASS; independent record-copy values/identity, detached modification isolation
and pop/change/add group mutation assertions PASS. Py_compile for the test file
and git diff --check PASS. These scripts use existing library contracts only;
no full suite was run against the incomplete Developer draft and no future green
or production correctness claim is made. Existing untracked examples draft is
preserved and excluded from both test/log commits. Production implementation
may resume after coordinator formal verification of this correction handoff.

## CR-01 focused cleanup regression — red

Reviewer finding CR-01 at `832e8bb` requires independent owned-loop close after
an owned-task drain error. Tester added one focused method with three subcases
in test commit `b147ff0`: drain failure; combined drain/close failure; successful
drain with close failure. Mocks use existing scheduler factory/backend/acquisition
seams and loop task/gather calls, without real pending tasks or leaked resources.
Each asserts pending cancellation and drain attempt, preserved primary error,
independent writer release, close attempt and all injected cleanup diagnostics.

Executed on current production `6f8b26d` from repository root:

```sh
MPLCONFIGDIR=/tmp/softlab-w01-mpl XDG_CACHE_HOME=/tmp/softlab-w01-cache MPLBACKEND=Agg \
  .venv/bin/python -W error -m unittest discover -s tests \
  -p 'test_thermal_example.py' -k loop_drain_and_close
git diff --check
```

Unittest actual exit **1**: one test method in 0.004s, **two failed subcases**.
Drain-error subcase observes close called zero times. Combined-error subcase
observes absent close-error diagnostic because close is skipped. Successful-drain/
close-error subcase passes, including primary error and writer cleanup evidence.
No warning reported; diff check exit 0. No full suite run, no production edits,
no green/failure closure, and CR-01 remains Reviewer-owned OPEN. Next Developer
fixes independent release; Reviewer reinspects before Tester green/full execution.
