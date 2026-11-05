# Release 12 (M03 Phase 4) — Walkthrough Results

Owner: Tester (sw-mike) | 2026-11-05 Asia/Shanghai | Branch: `codex/m03-p4-acceptance`
Verdict: **ALL PASS** (W1–W7)

Suite under test: `.venv/bin/python -m unittest discover -s tests -p 'test_*.py'`
Run once (small-batches mode, bounded single execution).
Binding inputs: [walkthrough-test-plan.md](walkthrough-test-plan.md), frozen oracle
design at `63ed461`, Phase 3 storage design, boundary decision, prior phase
results.

## Execution record (exact commands actually run)

| # | Command (repo root, project `.venv` = Python 3.13.15) | Exit |
| --- | --- | --- |
| 1 | `.venv/bin/python -m unittest discover -s tests -p 'test_*.py'` | **0** — `Ran 521 tests in 485.433s` / `OK` |
| 2 | `.venv/bin/python -m compileall -q softlab` | **0** (no output) |
| 3 | `.venv/bin/python -c "import softlab; print(softlab.__version__)"` | **0** — prints `0.3.0` |

Suite count split: 508 pre-existing baseline tests + 13 new Phase 4
walkthrough test methods (W1 ×1, W2 ×1, W3 ×1, W5 ×1, W6 ×2, W7 ×1,
W4 ×6) = **521**, zero failures, zero errors, zero skips in the outer run.
Inside W6, one nested full-suite subprocess run executed the same 521 tests
(`Ran 521 tests in 233.784s` / `OK (skipped=1)` — the single skip is the
suite-in-suite re-entrancy guard inside that one subprocess only; the outer
invocation executed the full run).

42 machine-readable `[walkthrough-evidence ...]` lines were emitted into the
test log by the walkthrough classes; every fact below is transcribed from
those lines and from this run's console output. No value is copied from the
characterization record.

## Environment actually observed

Project `.venv` (all W1–W3/W5–W7 sessions and the outer suite):
Python 3.13.15 (conda-forge), softlab 0.3.0 (editable install of this
checkout). W4 recorded its own fresh-environment table (below).

## Warnings observed (verbatim, categorized)

1. **Product — approved, deliberate, documented (W6 re-confirmation of
   DEFECT-2):** `Parameter probe is neither settable and gettable`
   (`UserWarning`), observed in child process PID 58790 firing exactly once.
   This is the established deliberate programmer-error warning; retained per
   the release_8 disposition, not a new warning.
2. **Environment — approved recorded baseline (W4 version probe,
   Python 3.13.15):** `DeprecationWarning: version is deprecated and will be
   removed in Python 3.14` — emitted exactly once by reading
   `sqlite3.version` on Python ≥ 3.13; approved baseline per freeze §6
   item 4 / `install_warning_policy`. W4 item 5 disposition result:
   `warning_count: 0` across all fresh-environment subprocesses,
   `undispositioned: 0` — i.e. no new warning was observed anywhere; the
   sqlite3 baseline was captured by the dedicated version probe, and the
   3.9-only pyparsing/pandas baselines are gated off on this interpreter.
3. **Environment — informational notices, not warnings (W4 pip install):**
   `[notice] A new release of pip is available: 26.2.1 -> 26.3` and the
   corresponding update notice. Recorded verbatim; not warnings.
4. The 3.9-gated baselines (four `PyparsingDeprecationWarning`,
   pandas `pd.concat` `FutureWarning`) were **not applicable**: the host has
   no Python 3.9 interpreter (see Limitations); gating follows the observed
   interpreter exactly.

No undispositioned new warning exists on Python 3.13.15 in either the
project environment or the W4 fresh environment.

## Per-scenario verdicts

| Scenario | Description | Verdict | Key evidence | Defects filed |
| --- | --- | --- | --- | --- |
| W1 | Thermal v1 (`coordinated`) original vs clean-session rerun | **PASS** | Sessions A/B PIDs 58153/58169, temp roots `softlab-m03-p4-ucmrg8i1` / `...-txv0aw5h`; `execution_id` 26050347-… vs 17adf1fc-…; `group_id` 3c63adac-… vs 8e5c81b5-… (distinct asserted); sha256 `298edc8b…` / `31f26780…` preserved throughout; 4 bootstrap inspections (PIDs 58186/58196/58198/58208), all `status: ok` on fresh copies `inspect-copy-03..06.h5`; §2.1 oracle, §3 tolerances, §4 retained assertions and freeze §5.3 recorded-configuration comparison all held per artifact and A-vs-B | none |
| W2 | Thermal v2 (`direct`) original vs clean-session rerun | **PASS** | Sessions A/B PIDs 58216/58232, roots `...-b97610r2` / `...-q4hg0v_x`; `execution_id` f21745e6-… vs 3a798027-…; `group_id` 06b44aa1-… vs b0dfd5fe-…; sha256 `9c597fc3…` / `266691a2…` preserved; 4 inspections (PIDs 58250/58258/58271/58279) all `ok` on copies `inspect-copy-11..14.h5`; §2.2 oracle (delay 0, manifest v2, `source_tick='tick'`), tolerances, exclusions, §5.3 comparison all held | none |
| W3 | Signal original vs clean-session rerun (DEP-1) | **PASS** — DEP-1 present, all six required inputs asserted | Sessions A/B PIDs 58287/58301; stored columns `signal=[6,7,8]`, `reference=[3,4,5]` exact; caller-derived `difference=[3,3,3]` recomputed from stored samples and asserted **absent** from stored columns; acquisition-time read trace `[('signal',1),('reference',1),('signal',2),('reference',2),('signal',3),('reference',3)]` from the `acquire()` return, identical in both sessions; non-consumptive reads asserted true; declared column order `(timestamp, signal, reference)`; `completed_rows == requested_rows == 3`; identities distinct (7c03587d-…/63d80e3a-… vs 3b0a0251-…/7ceaa782-…); 4 inspections (PIDs 58315/58325/58333/58341) all `ok`; sha256 `bdc1a74d…` / `684a34c4…` preserved | none |
| W4 | Fresh-environment walkthrough (freeze §6, items 1–6) | **PASS** (with one recorded executed-scope deviation, item 3) | See "W4 environment facts" below; items 1, 2, 4, 5, 6 executed fully with verbatim evidence; item 3 executed in its implemented (narrower) scope, recorded in "Deviations"; `limitation: null` | none |
| W5 | Clean-process walkthrough | **PASS** | See "W5 parent-baseline fact" below; session PIDs 58728/58744 (≠ parent 57701), inspection PIDs 58762/58772/58782 all distinct; fresh-copy carries across roots decode identically; parent softlab-module set unchanged after all activity | none |
| W6 | Compatibility and debt verification | **PASS** | Legacy suite green in nested full run (PID 58815, `Ran 521 tests … OK (skipped=1)`, project interpreter); debt table fully re-confirmed (below) | none |
| W7 | Guidance verification (DEP-3) | **PASS** — `docs/run-record.md` present | Guide thermal CLI ran verbatim (PID 60554, exit 0, artifact `/tmp/thermal-run-01/thermal.h5` produced and cleaned up); guide signal recipe ran verbatim (PID 60570, exit 0, printed exactly 3 fields); §3 bootstrap executed for both profiles against the guide's own produced artifacts (PIDs 60596 signal, 60606 thermal, both exit 0, `status: ok`) | none |

## W4 environment facts (verbatim)

- **Item 1 — creation and isolation.** Command:
  `/Users/edward/workspace/softlab/.venv/bin/python -m venv /var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-freshenv-gkn1ums_/venv`,
  exit 0, empty stdout/stderr. Observed in the venv: `prefix ==
  venv dir`, `base_prefix = /Users/edward/workspace/softlab/.venv` (≠
  prefix), project `.venv` site-packages **not** on `sys.path`
  (`base_site_packages_on_path: false`), project-only extras not importable
  (`nest_asyncio: false`, `pyvisa_sim: false`). `SOFTLAB_*` environment
  variables present: none.
- **Item 2 — editable install.** Command:
  `<fresh venv>/bin/python -m pip install -e /Users/edward/workspace/softlab`,
  **exit 0**. Resolved dependency set, verbatim:
  `Successfully installed asttokens-3.0.2 comm-0.2.3 contourpy-1.4.0
  cycler-0.12.1 executing-2.3.0 fonttools-4.68.0 h5py-3.16.0 imageio-2.38.1
  ipython-9.18.1 ipython-pygments-lexers-1.1.1 ipywidgets-8.1.9 jedi-0.20.1
  jupyterlab_widgets-3.0.17 kiwisolver-1.5.1 matplotlib-3.11.2
  matplotlib-inline-0.2.2 narwhals-2.28.0 numpy-2.5.4 packaging-26.3
  pandas-3.1.1 parso-0.8.7 pexpect-4.9.0 pillow-13.0.0 plotly-7.1.0
  prompt_toolkit-3.0.53 psutil-7.2.2 ptyprocess-0.7.0 pure-eval-0.2.4
  pygments-2.21.0 pyparsing-3.3.3 python-dateutil-4.0.0 pyvisa-1.16.2
  scipy-1.18.1 softlab-0.3.0 stack_data-0.6.3 traitlets-5.16.1
  typing_extensions-4.16.0 tzdata-2026.6 wcwidth-0.9.4
  widgetsnbextension-4.0.16`.
  `pip list --format=freeze` captured verbatim in the suite log
  (42 entries incl. `pip==26.2.1`). `pip_warnings: []`; stderr tail = the
  two pip upgrade notices recorded above. Index reachability: demonstrated
  by the successful resolution/download of the full declared set plus build
  isolation prerequisites.
- **Item 3 — documented command sequence.** Recorded exactly as executed.
  The fresh environment executed the documented minimal workflow: thermal
  CLI
  `MPLBACKEND=Agg python examples/thermal_experiment.py --output-dir
  <fresh-root>/thermal-out --adapter coordinated` exit 0, artifact
  `<fresh-root>/thermal-out/thermal.h5` (sha256
  `585c09b4ae1fb1d8a348779f99b2992c2e57b41a91387acc1f6e37e4330475d7`,
  `execution_id` 1315be36-…, `group_id` 4c2e13ed-…); bootstrap inspection on
  a fresh verified copy exit 0, `status: ok`, `primary_ok`/`cleanup_ok`
  true, original sha256 preserved. The plan's broader item-3 enumeration
  (fresh-env `unittest discover` full run, `compileall`, import smoke,
  W1–W3 sessions A/B inside the fresh environment) was **narrowed by the
  implemented and reviewed test** to this minimal workflow plus the
  in-suite legacy-suite evidence of W6 — the W6 nested 521-test run and the
  W1–W3 A/B sessions all executed under the project interpreter
  (3.13.15), not inside the fresh venv. Recorded here as an executed-scope
  deviation from the plan's item-3 list; no claim of a fresh-env full-suite
  run is made. The full suite, `compileall` and the import smoke are
  evidenced in the project environment in the Execution record above.
- **Item 4 — versions actually observed (in the fresh environment):**
  Python `3.13.15 | packaged by conda-forge | (main, Aug 10 2026, 13:01:04)
  [Clang 19.1.7 ]`; softlab `0.3.0`; numpy `2.5.4`; pandas `3.1.1`;
  h5py `3.16.0` / libhdf5 `2.0.0`; matplotlib `3.11.2`; sqlite3 module
  `2.6.0` / library `3.53.4`.
- **Item 5 — warnings:** verbatim list above; `undispositioned: 0`.
- **Item 6 — limitations:** `null` — no creation/install failures; no
  optional extras installed (fresh env holds only the core dependency set).

## W5 parent-baseline fact

The walkthrough parent (PID 57701) is the unittest runner process; under
full-suite discovery the runner has **already imported ~80 `softlab.*`
modules** before W5 begins (full module list recorded verbatim in the
`w5-parent-baseline` evidence line). This is recorded as an environment
fact: it is runner preload state, **not** walkthrough activity. The control
assertion holds regardless: W5's own code imports nothing from softlab,
every session ran in a fresh interpreter with `clean_prefix: true` (no
`softlab` module state before import) and no `SOFTLAB_*` environment
variables, every decode happened in a separate bootstrap child process, and
the parent's `softlab`-module set was **identical before and after** all W5
activity (no in-process decode path exists or was exercised anywhere in
this suite).

## W3 invocation detail (recorded per plan)

`examples.signal_monitor` has no module entry point, so the plan-pinned
driver form was used: a fresh `python` subprocess running a driver script
with `sys.path` set to the repo root, calling `acquire(points=3)` **and**
the DEP-1 owned-persistence wiring (`persist(result, record, <temp-root>/out/signal.h5)`)
in the **same process**, with the HDF5 target path explicit under the
session's `softlab-m03-p4-*` temp root. This satisfies the plan's
invocation-form requirement (either the CLI or a driver; the wiring has no
entry point, so a driver-only call of `acquire()` would be insufficient).
Both sessions recorded this form, `clean_prefix: true`, and empty
`SOFTLAB_*` environment variables.

## W7 deviations (recorded, all harness-necessity, no guide edits)

1. The guide's §3 bootstrap examples use placeholder paths (`/path/to/*.h5`);
   per the approved substitution rule they were executed against the
   artifacts the guide's own §2 production sections produced; everything
   else verbatim.
2. Setup hygiene: a stale `/tmp/thermal-run-01` from any earlier run is
   removed before the verbatim thermal CLI (the example refuses a non-empty
   output dir), and the produced directory is removed at the end.
3. The guide's signal recipe uses a `TemporaryDirectory`, so its artifact
   self-deletes; a persistent-artifact variant (identical recipe with an
   explicit temp root) fed the §3 bootstrap command. Recorded in the test;
   the verbatim recipe itself was also executed and passed.

## Debt re-confirmation (W6)

All rows of the plan's debt table re-confirmed against the current tree and
executed evidence:

- **R4-BASE-SCHED-01 / F-01 / F-02** — `SerialScheduler(_capture_failures=True)`
  present in `softlab/huo/process/bounded.py`; no `_SchedulerImpl` /
  `get_scheduler` in the serial seam, in `signal_monitor.py`, or in
  `thermal_experiment.py` (both route through `run_count(`); release_8
  dispositions recorded.
- **R4-BASE-META-01** — runtime probe (child PID 58790): `DataGroup(meta=…)`
  still yields `group_meta: {}` and `get_meta('a') is None`; caller dict
  unmutated (`caller_meta_after: {"a": 1}`); supported construction in
  `run_group.py` passes `meta=` never and hydrates via
  `DataGroup(name=name, id=uuid.UUID(envelope['group_id']))`.
- **R4-BASE-HDF5-01 / F-A** — every inspection in W1–W3/W5/W7 ran on a fresh
  verified copy via the subprocess bootstrap; per-run original sha256
  preservation asserted (all sha256_before values above unchanged).
- **OBS-004/005/006, DEFECT-2** — markers retained in the release_8 record;
  DEFECT-2 warning re-confirmed firing verbatim at runtime (PID 58798,
  `UserWarning`, exact message above).
- **D1/D2** — deviation table intact in characterization-results; boundary
  decision retains D1/D2 dispositions; Phase 1 characterization suite part
  of the green legacy run.
- **F-A…F-D** — no supported flow in W1–W3 used a plain original load,
  thermal's three-key hydration for signal data, `io.reload_group`, or
  `io.load_groups`; the thermal example's private `_reload_group` remains
  its own scoped disposable-copy reader.
- **No new debt invisibility observed** — no Phase 4 path touched a
  routed-around path.

## Deviations (recorded, none silent)

1. **W4 item 3 — executed scope narrower than the plan's enumeration.** The
   implemented (and developer-reviewed) W4 class executes the documented
   minimal workflow in the fresh environment (thermal CLI + bootstrap
   inspection) but does **not** re-run the full unittest suite, `compileall`,
   the import smoke, or the W1–W3 sessions inside the fresh venv; those ran
   under the project interpreter (W6 nested run, Execution record above).
   Recorded here per "no claims beyond what executed"; the fresh-env
   creation, isolation, install, versions and warnings evidence (freeze §6
   items 1, 2, 4, 5, 6) is complete and verbatim.
2. **W3 invocation detail** — recorded in its own section above (driver
   form; no module entry point exists; `acquire()` + wiring in one process;
   explicit HDF5 target under the session temp root).
3. **W5 parent-baseline fact** — recorded in its own section above (runner
   preload state in the parent PID 57701 vs `clean_prefix` child sessions;
   zero in-process decode; parent module set unchanged).

## Limitations

1. **Single interpreter.** The walkthrough host has no Python 3.9
   interpreter; per plan W4 the absence is recorded as a limitation and
   single-interpreter execution (Python 3.13.15) with the observed version
   table is acceptable; version-portable assertions only were used.
2. W4's fresh environment deliberately contains **no optional extras**
   (`notebooks` group: jupyterlab/nest_asyncio/pyvisa-sim absent) — recorded
   as a fact in item 6, not silently scoped away; the isolation probe
   asserts these are not importable there.
3. No real instruments and no pre-existing experiment data were used
   anywhere; all artifacts live in synthetic `tempfile` roots
   (`/var/folders/.../softlab-m03-p4-*`, plus W4's fresh-env root) that are
   deleted on test teardown. The W7 guide thermal run used the guide's own
   documented `/tmp/thermal-run-01` path and cleaned it up.
4. Characterization-era version values (Python 3.13.15, numpy 2.5.3,
   pandas 3.0.6, …) remain historical context only; the tables above were
   observed in this run (note numpy 2.5.4 / pandas 3.1.1 in the fresh
   environment — the fresh env resolves its own current set from the
   index, which is exactly what freeze §6 item 1 requires).

## AC traceability matrix

| Plan element | M03-AC05 | M03-AC06 |
| --- | --- | --- |
| W1/W2/W3 oracles, tolerances, exclusions, A-vs-B, recorded configuration (freeze §2–§5.3) | ✔ primary evidence (all PASS, per-artifact + A-vs-B) | rerun/inspection commands also feed W4 |
| W4 fresh-environment creation/install/commands/versions/warnings/limitations (freeze §6) | rerun determinism cross-check (CLI + bootstrap re-executed in fresh env) | ✔ primary evidence (items 1–6, verbatim) |
| W5 process separation (freeze §5.2 steps 4/6) | ✔ (PIDs, clean prefixes, parent-baseline fact, no in-process decode) | ✔ |
| W6 compatibility, legacy suite, debt inventory | ✔ (521-test green run; debt table re-confirmed) | ✔ (legacy suite green; 521 tests, zero failures) |
| W7 guidance vs actual behavior | — | ✔ (docs/run-record.md; every documented command executed verbatim) |

Exit criteria of the plan: 1–7 all satisfied (no FAIL/BLOCKED; no freeze
reopen — zero deviation between C5b in-memory evidence and the executed
stored path was observed).
