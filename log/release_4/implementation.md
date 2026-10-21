# M01-W01-001 developer implementation evidence

Owner: sw-tom (Developer), 2026-10-18. Production commit `6f8b26d`;
guide commit `864891a`, on `codex/m01-w01-thermal-example`.
Status: implementation/self-checks complete; independent Code Review next.
This record does not close Tester failures or Reviewer findings and does not
claim candidate CI, release acceptance or integration.

## Delivered scope

Added examples/thermal_experiment.py and docs/user-guide/thermal-experiment.md,
linked from the simulation guide. No library sources, exports, dependencies,
package version, supported Python range, tests or design were changed by Developer.
Frozen scalar configuration and output preflight precede acquisition; a fresh
Station/object/control/measurement graph runs through count's before-get hook.
Exactly one explicit step precedes each read/row, with no teardown reset/step.
The example-only public Scheduler adapter dispatches unchained nonthreaded Action
bodies using its own loop without changing the global scheduler/thread loop.

The owned HDF5 writer disconnects before verification. Each reload copies the
closed delivered file into an owned temporary directory, reads declared metadata
from that disk copy, calls the legacy reader once and verifies identity/schema/
values. It preserves delivered bytes/attributes and returns the real output path.
The plot uses reopened values; cleanup reports primary/secondary errors, releases
owned handles/loops/new figures, and preserves borrowed figures/global objects.
The guide explains exact command, units/latency, safe repeated reloads, failure
artifacts, existing backend defects and W01 lifecycle/physical-model limitations.

## Test-contract correction handoff

Developer reviewed executable tests at `840df0e` and approved before development
in `2d13511`. Initial self-run exposed three assertions contradicting existing
contracts: unprepared Coordinator.tick raises; DataGroup.record returns copies;
mutating a returned record cannot mutate the stored group. Developer paused and
reported; Tester corrected/closed those findings in `64e72f3`, `5be1060` and
`2582302`. Developer reviewed the corrections before resuming, without editing
those tests. Own draft fixes explicitly register the record and guard final
boundary inspection when preparation has not completed. Initial failed draft
checks and ResourceWarnings from masked teardown are not the final evidence;
they disappeared after correcting the example's cleanup path.

## Actual self-checks

Environment: repository .venv Python 3.13.15, softlab 0.3.0; package/dependency
baseline versions remain in test/baseline.md. All runtime commands below set
MPLCONFIGDIR=/tmp/softlab-w01-mpl, XDG_CACHE_HOME=/tmp/softlab-w01-cache,
MPLBACKEND=Agg. Outputs/data used temporary directories only.

| Command/check | Actual result |
| --- | --- |
| `.venv/bin/python -W error -m unittest discover -s tests -p test_thermal_example.py` | Exit 0; 18 tests, 10.368s, OK; no warning/error diagnostics. |
| `.venv/bin/python -W error -m unittest discover -s tests -p 'test_*.py'` | Exit 0; 228 tests, 8.667s, OK; no skips or warning/error diagnostics. |
| `.venv/bin/python -W error -m compileall -q softlab examples tests` | Exit 0, no compilation output. |
| `.venv/bin/python -W error -c 'import softlab; print(softlab.__version__)'` | Exit 0; 0.3.0; no warnings. |
| `.venv/bin/python -W error examples/thermal_experiment.py --output-dir <fresh temporary>/run` | Exit 0; completion UUID and absolute HDF5/PNG paths; HDF5 14632 bytes, PNG 22723 bytes; no stderr. |
| Guide relative-link and example line-length checks | Referenced local paths exist; source lines at most 80 characters. |
| `git diff --check` and scope/status inspection | Pass; only example/guide/link and this task's process artifacts; no generated data committed. |

The final line-wrapping-only edit was also exercised by the documented CLI.
No Black installation was available; formatting was checked locally rather than
adding a tooling dependency. No configured lint/static-analysis claim is made.
Python 3.9 and candidate CI remain unrun by Developer; local 3.13 results do not
certify them. Independent code review, Tester committed-candidate checks,
completion principles and candidate CI remain required before dev integration.
Baseline defects tracked in the Architect review remain unfixed/outside scope.

## CR-01 developer correction — 2026-10-18

Correction commit `5c5f6c4` separates owned-loop draining and close into
independent attempts, recording `Owned loop drain` and `Owned loop close`
diagnostics separately. A drain exception no longer suppresses close; either
failure still permits independently owned writer cleanup and preserves the
primary failure. No tests, design, library code or dependency changed.
CR-01 remains OPEN pending sw-celeste reinspection; Developer does not close it.

Reviewed Tester regression `b147ff0` / red evidence `2559279`, then ran with
MPLCONFIGDIR=/tmp/softlab-w01-mpl, XDG_CACHE_HOME=/tmp/softlab-w01-cache,
MPLBACKEND=Agg:

- `.venv/bin/python -W error -m unittest discover -s tests -p test_thermal_example.py -k test_loop_drain_and_close_failures_preserve_other_cleanup`:
  exit 0, 1 test / 3 subcases, 0.003s, OK.
- `.venv/bin/python -W error -m unittest discover -s tests -p 'test_*.py'`:
  exit 0, 229 tests, 4.802s, OK, no skips/warning/error diagnostics.
- `.venv/bin/python -W error -m compileall -q softlab examples tests`:
  exit 0, no output.
- `.venv/bin/python -W error -c 'import softlab; print(softlab.__version__)'`:
  exit 0, 0.3.0, no warnings.
- `git diff --check`: pass; diff is only the independent close/diagnostic change
  plus these process records. Prior CLI is unchanged; full suite includes its
  subprocess/repetition assertions. No unnecessary separate CLI rerun.

Next serial owner is sw-celeste for code-review reinspection and closure only
if approved, followed by Tester independent committed-candidate verification.
No CI/integration/release gate is claimed passed by these self-checks.
