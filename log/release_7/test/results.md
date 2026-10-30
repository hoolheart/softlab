# M01-W04 independent test results

Date: 2026-10-30. Environment: existing local `.venv`, Python 3.13.15, macOS;
not a newly installed environment. Runs used `MPLCONFIGDIR=/private/tmp/softlab-mpl`
and `XDG_CACHE_HOME=/private/tmp/softlab-cache`; walkthrough additionally used
`MPLBACKEND=Agg`. Synthetic data and fresh output paths only.

- Full `python -m unittest discover -s tests -p 'test_*.py'`: **PASS**, 278 tests
  in 34.413 seconds, no skips or warnings reported. This run included five W04
  adapter tests. A sixth control-write-failure test was added afterward; the
  separate focused run below covers it. Subsequent developer edits to helper
  annotations and guide wording do not change runtime behavior.
- `python -m unittest discover -s tests -p 'test_thermal_adapters.py'`: **PASS**,
  six tests. Independent checks cover both numeric source boundaries and schemas,
  persisted timing, cancellation, operation order, write and read failures,
  cleanup, mixed version/timing rejection and unsupported adapter.
- `python -m compileall -q softlab`: **PASS**. `python -c 'import softlab;
  print(softlab.__version__)'`: **PASS**, output `0.3.0`. `git diff --check`:
  **PASS** at the time of this review.
- Guide command with `.venv/bin/python` and a fresh output directory, selecting
  `--adapter direct`: **PASS**, exit 0 and complete HDF5 plus PNG. A separate
  fresh Python inspection returned `context=available`, manifest version 2,
  delay 0 and eight rows. The PNG is 22,327 bytes. Automated adapter checks
  assert the independent temperature oracle and round-trip record schema.
- Initial pre-implementation red check found the missing adapter argument. An
  earlier unconfigured cache run emitted unwritable Matplotlib/fontconfig cache
  notices and was interrupted, not counted as a pass or failure of the suite.

No outstanding test failures found. Existing project baseline limitations remain
as recorded in the PRD; the full-suite result does not establish Python 3.9, CI, physical hardware,
VISA simulation or release acceptance.
Exact candidate CI and promotion verification remain separate gates.

## Fresh-environment independent walkthrough

Reviewed the developer's [fresh-environment setup](fresh-environment.md), then
independently repeated the documented direct command in that isolated Python 3.9.6
venv with a separate fresh output directory. Exit 0; independent fresh-process
inspection recovered context available, manifest v2, delay 0 and eight rows; PNG
size was 21,633 bytes. Both run and inspection emitted pandas 2.3.3 DataFrame
concatenation FutureWarnings at `softlab/shui/data/base.py:314`. Initial offline
install failed on DNS; a network-enabled retry and a pip upgrade from 21.2.4 to
26.0.1 were needed before editable install succeeded. This demonstrates the
walkthrough in the stated isolated environment, not full Python 3.9 regression.
