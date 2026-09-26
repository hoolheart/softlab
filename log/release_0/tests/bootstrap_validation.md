# BOOT-001 validation plan and baseline

Owner: sw-mike (tester). Date: 2026-09-26.
Branch: `codex/workflow-preparation`; integration target: `dev`.
Baseline revision: `83e85c4e4f809882a2f2755d13123e40129cabc7`.
This report records testing, not release acceptance or architecture approval.

## Acceptance/validation plan

| Case | Expected result | Evidence/status |
| --- | --- | --- |
| Existing integration baseline | Regression suite passes, including simulated VISA; compilation and import pass | PASS locally, results below |
| Required workflow artifacts | Principles, current-state architecture, AGENTS guidance, requirements/backlog and reusable process templates exist and agree | Pending final bootstrap inspection |
| Branch rules | Task branches integrate into `dev`; releases into `main`; one active task; pushes recorded | Pending final branch/remote inspection |
| Scope preservation | No production Python/API/dependency changes; future `tu` tasks remain unimplemented | Pending final diff inspection |
| CI setup | Python 3.9 and 3.13 jobs install the project plus simulator and execute the checks below | Pending implementation and actual remote runs |
| Failure/edge handling | Tests fail CI on regression; optional VISA test cannot silently skip in CI; warnings/limitations are reported accurately | Require explicit simulator import before discovery; inspect CI results |
| Evidence gates | Review, principle inspection and acceptance have explicit verdicts; no unrun checks claimed PASS | Pending coordinator/independent review |
| Diff hygiene | `git diff --check` succeeds and final status contains no unexpected files | Pending final bootstrap inspection |

These documentation/CI preparation checks replace production TDD cases for this
bootstrap only, as permitted by principle 3. No UI or real-device validation is
applicable. Future `tu` behavior changes need new characterization/regression
assertions before implementation.

## Actual local environment

- Interpreter: repository `.venv/bin/python`, CPython 3.13.15, conda-forge,
  Clang 19.1.7.
- Platform: macOS 26.6.2, arm64.
- Project version: 0.3.0.
- Dependencies: numpy 2.5.3; scipy 1.18.1; pandas 3.0.6; matplotlib 3.11.2;
  plotly 7.1.0; imageio 2.37.4; ipywidgets 8.1.9; pyvisa 1.16.2;
  h5py 3.16.0; pyvisa-sim 0.7.1.
- No environment installation, runtime dependency change or hardware access.

## Commands and observed results

Executed from the repository root:

```sh
.venv/bin/python -m unittest discover -s tests -p 'test_*.py'
.venv/bin/python -m compileall -q softlab
.venv/bin/python -c 'import softlab; print(softlab.__version__)'
```

All commands exited 0. The suite ran **5 tests, 0 failures/errors, 0 skips** in
0.086 seconds. It covers wavement quantization, synchronous/asynchronous process
wrapping, count/scan values, SQLite/HDF5 round trips, and VISA simulation.
Persistence fixtures use temporary directories; VISA uses `tests/visa_sim.yaml`
with `@sim`. Compileall produced no output. Import printed `0.3.0`.

Initial unittest/import processes emitted cache-environment diagnostics:
`/Users/edward/.matplotlib is not a writable directory`, Matplotlib's fallback
cache notice, and four `Fontconfig error: No writable cache directories` lines
per process. Both processes still completed successfully. These are recorded
baseline environment diagnostics, not dismissed as a clean zero-warning run.

To isolate writable-cache configuration, repeated the same three commands with:

```sh
validation_cache=$(mktemp -d /tmp/softlab-validation.XXXXXX)
export MPLCONFIGDIR="$validation_cache/matplotlib"
export XDG_CACHE_HOME="$validation_cache/cache"
export MPLBACKEND=Agg
```

The repeat exited 0, ran **5 tests, 0 failures/errors, 0 skips** in 0.056 seconds,
compiled silently and printed `0.3.0`. The unwritable-cache/Fontconfig diagnostics
did not recur. First cache construction printed the informational message
`Matplotlib is building the font cache; this may take a moment.` No source fix
was made. The writable-cache configuration is the reproducible disposition of
this sandbox-specific diagnostic; CI should use writable cache directories too.

## Recommended CI baseline

Use an Ubuntu matrix containing Python **3.9** (declared minimum) and **3.13**
(local compatibility target). Let pip resolve Python-compatible dependencies
from existing metadata; do not alter the declared support range or introduce a
lockfile during preparation. Minimal commands:

```sh
python -m pip install --upgrade pip
python -m pip install -e . pyvisa-sim
python -c 'import pyvisa_sim'
python -m unittest discover -s tests -p 'test_*.py'
python -m compileall -q softlab
python -c 'import softlab; print(softlab.__version__)'
```

Set `MPLBACKEND=Agg` and writable `MPLCONFIGDIR`/`XDG_CACHE_HOME` under the runner
temporary directory. Keep each validation command a separate CI step so each
exit code gates the job. The explicit simulator import prevents the suite's
optional-dependency skip from masquerading as complete CI coverage. Log Python
and resolved dependency versions for reproducibility.

Python 3.9 and Linux were **not run locally**. The proposed matrix is a validation
target, not proof of compatibility. Remote jobs must pass before integration.
No packaging build, notebooks, real hardware, Windows, additional Python
versions, lint/static analysis or warning-as-error gate were executed here.
Therefore this report makes no global zero-warning or cross-platform claim.

## Final bootstrap verification

Tester: sw-mike. Candidate: `44cb265acbb6cafe93eacfe268b12b857ee9ed87`.
Date: 2026-09-26. Verdict: **PASS for the applicable bootstrap validation**.
This verdict does not replace product acceptance or principle inspection.

- Required principles, current-state architecture, AGENTS guidance, backlog,
  seven reusable templates, CI and independent review artifacts exist.
  A Python pathlib/regular-expression check inspected 17 Markdown files and
  found zero missing repository-relative Markdown link destinations.
- `git diff --name-only main..HEAD` contains documentation and CI only; no
  production Python, tests, dependencies or version metadata changed.
  `git diff --check 83e85c4..HEAD` exited 0 with no output.
- `git branch -vv` confirms `dev` tracks `origin/dev`, and the task branch
  tracks `origin/codex/workflow-preparation` at the inspected candidate.
  Integration and release are still separate pending operations.
- Repeated the three local validation commands above using a fresh writable
  temporary cache and `MPLBACKEND=Agg`, preceded by
  `.venv/bin/python -c 'import pyvisa_sim'`. Simulator import succeeded;
  unittest ran **5 tests in 0.076 seconds, zero failures/errors/skips**;
  compileall was silent; import printed `0.3.0`. The shell exited 0.
  First-use font-cache informational output appeared; earlier unwritable-cache
  diagnostics did not recur. No hardware was accessed.
- Coordinator-owned sprint-board and principle report are committed alongside
  this evidence unchanged; their pending gates are not tester PASS claims.

### Remote CI evidence and failure closure

GitHub's public Actions API reports the inspected candidate's
[run 36252706086](https://github.com/hoolheart/softlab/actions/runs/36252706086)
completed successfully. Both matrix jobs and all required validation steps
(simulator import, regression suite, compilation and project import) succeeded:

- [Ubuntu Python 3.9](https://github.com/hoolheart/softlab/actions/runs/36252706086/job/108433606627)
- [Ubuntu Python 3.13](https://github.com/hoolheart/softlab/actions/runs/36252706086/job/108433606751)

The initial invalid-workflow failure at `6fbd1f8`
([run 36252511432](https://github.com/hoolheart/softlab/actions/runs/36252511432))
is **CLOSED by sw-mike**: corrected cache initialization at `f95b5cc` executed
successfully in [run 36252582324](https://github.com/hoolheart/softlab/actions/runs/36252582324),
and the reviewed candidate repeated both successful matrix jobs. The original
failure remains in the history; it is not reclassified as a successful run.

Evidence was retrieved with bounded `curl --max-time 20` requests to the Actions
runs/jobs endpoints. The first sandbox request could not resolve the host;
the permitted escalated read succeeded. Job statuses prove required CI steps
passed; logs were not audited for a global zero-warning assertion. No packaging,
notebook, real-device, lint/static-analysis or additional platform validation
is claimed. Subsequent commits still require their own CI before integration.
