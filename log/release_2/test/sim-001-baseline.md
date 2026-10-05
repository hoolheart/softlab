# SIM-001 baseline characterization record

Tester: sw-mike | Date: 2026-10-05
Branch: `codex/sim-001-simulation-foundation` (from `dev` @ `f04e783`)
Recorded BEFORE any SIM-001 test files or production changes exist.

## Environment

| Item | Value |
| --- | --- |
| Python | 3.13.15 (`$PWD/.venv`, conda env) |
| Interpreter path | `/Users/edward/workspace/softlab/.venv/bin/python` |
| softlab version | `0.3.0` |
| Platform | macOS (darwin) |

## Commands and verbatim results

### 1. Full regression suite

Command:

```
python -m unittest discover -s tests -p 'test_*.py'
```

Output (verbatim):

```
...................................................................................................
----------------------------------------------------------------------
Ran 99 tests in 0.482s

OK
Connected to sqlite3 backend, data is stored in /var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/tmpy4_d1gav/readings.db.
Connected to HDF5 backend, data is stored in /var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/tmpy4_d1gav/readings.hdf5.
```

The two backend lines are informational prints from the data-backend tests,
not warnings.

- Test count: **99**
- Result: **OK**
- Exit code: **0**
- Warnings: **none printed** (no `DeprecationWarning`/`UserWarning` lines in
  output; repeated run under `python -W default -m unittest discover ...`
  also produced zero warnings and exit code 0).

A separate import smoke check
(`warnings.simplefilter('always')` + `import softlab; import softlab.tu`)
recorded **zero warnings on import**.

### 2. Compile check

Command: `python -m compileall -q softlab`
Exit code: **0**, no output.

### 3. Import smoke check

Command: `python -c "import softlab; print(softlab.__version__)"`
Output: `0.3.0`, exit code **0**.

## Environmental diagnostics

- No matplotlib cache/fontconfig messages observed on this baseline.
- Tests print two informational backend-connection lines (sqlite3 and HDF5
  temp stores under `/var/folders/.../T/tmp*/`); these are informational,
  not warnings or failures.
- No test failures, skips, or error output observed.

## Release 1 debt status on this baseline

Tracked in `log/release_1/compatibility.md` and `arch.md`:

| Item | Status on this baseline |
| --- | --- |
| OBS-004 (subclass init/set-hook hazard in `QuantizedParameter`/`VisaParameter`) | Not exercised by the suite; no warning/failure observed. Recorded debt, no waiver requested or granted. |
| OBS-005 (`TheoryModel.features` swallows exceptions, returns `{}`) | Deliberately characterized by `tests/test_tu_theory.py`; suite passes; behavior unchanged. |
| OBS-006 (delegated-name collisions with `child`/`device`) | Tests use explicit lookup; suite passes. |
| DEFECT-2 (deliberate warning when constructing a neither-settable-nor-gettable `Parameter`) | Warning site is construction-time; no test constructs such a parameter, so **no warning fires** in this baseline run. This is an absence of the trigger, not a fix or waiver. |

No silent waivers: nothing was exempted, suppressed, or repaired as part of
this baseline. If SIM-001 work later causes any of the above to fire
differently, it will be reported explicitly.

## Notes

- This is a fresh baseline on the current branch; it does not reuse the
  historical "99 tests accepted" figure from Release 1 except as a cross-check
  — the count matches (99 tests, OK).
- Results above are the pre-change baseline. SIM-AC-07 requires re-running
  this same suite after implementation and comparing against this record.
