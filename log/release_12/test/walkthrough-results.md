# Release 12 (M03 Phase 4) — Walkthrough Results

Owner: Tester (sw-mike) | 2026-11-05 Asia/Shanghai | Branch: `codex/m03-p4-acceptance`
Verdict: **ALL PASS** (W1–W7) — **corrected 2026-11-05: the W4
fresh-environment verdict is now based on the COMPLETE fresh-environment
execution below; the earlier narrowed W4 PASS is superseded (see
"Correction — acceptance Issue 1" and the W4 section).**

## Correction — acceptance Issue 1 (tester-owned closure, 2026-11-05)

Product acceptance (`log/release_12/acceptance.md`, Issue 1, Blocker)
found that the W4 scenario reported PASS while its fresh environment had
only executed the minimal thermal coordinated workflow; the plan-required
fresh-environment full unittest discovery, `compileall`, the exact import
smoke command, and the W1–W3 A/B sessions had not run there. That finding
is correct. **The earlier W4 PASS was narrowed and is superseded by the
complete fresh-environment execution recorded in "Fresh-environment
execution record (complete, Issue 1 correction)" below**, executed by the
tester on the then-frozen acceptance candidate `6fe5acb` of
`codex/m03-p4-acceptance`.

This correction also fixes the AC06 traceability claims (matrix below now
states precisely which evidence is fresh-environment vs project-
environment) and the "no gate skipped" overclaim in
`principle_compliance_report.md` Inspection 4, which is annotated there.
The in-suite automated W4 class (items 1–6) is unchanged and remains
PASS; the previously recorded item-3 executed-scope deviation is closed
by the new evidence, not by re-interpreting the old run.

**Final correction (2026-11-05, after acceptance re-review of `fefecf3`):**
the re-review found the fresh-environment EXECUTION recorded but the
VERBATIM per-session command transcript missing (no real argv for the six
A/B driver invocations and twelve bootstrap inspections; F1 abbreviated
as `python -m venv <tmp>/...`). That finding is correct. Closure:
(1) the walkthrough suite now emits the real argv of every spawned
session driver and bootstrap inspection into its evidence
(commit `93336e0`, test-only instrumentation); (2) the tester re-executed
F1–F3 and F7 in a NEW fresh venv (`m03-ac1-transcript`, candidate
`93336e0`) — all W1–W3 A/B sessions PASS with the full command/exit/key-
output transcript committed in "Verbatim per-session command transcript"
below; F4–F6 stand from the `6fe5acb` correction run with an explicit
applicability note; (3) the F1 literal creation command (selected
interpreter path + target path, no placeholder) is recorded; (4) every
"complete"/"verbatim" claim in this file was audited against the
committed transcript and now matches it exactly.

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

## Fresh-environment execution record (complete, Issue 1 correction)

Environment: tester-created isolated venv
`/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/opencode/m03-ac1-transcript/venv`,
created by `python -m venv` from the project interpreter (base_prefix =
project `.venv`; the venv's own site-packages only). Contents: the
current checkout installed editable (`pip install -e
/Users/edward/workspace/softlab`, candidate `93336e0` — the walkthrough
suite at this revision emits the real per-session argv, see the verbatim
transcript below), plus the test dependency **`pyvisa-sim==0.7.1`** (with
`PyYAML-6.0.3`, `stringparser-0.7`) — installed because the full suite
needs it and it is NOT a core dependency:
`tests/test_tu_visa_contracts.py` Group D drives a real `@sim` backend
without a skip guard, and `tests/test_compatibility.py::test_visa_simulator`
is gated on `find_spec("pyvisa_sim")`. Installing it is recording the
environment the suite actually requires, not hiding a failure. No
`PYTHONPATH`/`PYTHONHOME` was propagated into the fresh env. (An earlier
correction run at `6fe5acb` used the sibling directory
`m03-ac1-correction/`; this transcript environment supersedes it for the
W1–W3 command record and stands together with the standing F4–F6 records,
whose applicability is stated below.)

| # | Command (repo root, fresh venv python = Python 3.13.15) | Exit | Duration |
| --- | --- | --- | --- |
| F1 | `/Users/edward/workspace/softlab/.venv/bin/python -m venv /var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/opencode/m03-ac1-transcript/venv` (literal creation argv: selected interpreter `/Users/edward/workspace/softlab/.venv/bin/python`, target path as shown) | **0** (empty stdout/stderr) | ~3 s |
| F2 | `/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/opencode/m03-ac1-transcript/venv/bin/python -m pip install -e /Users/edward/workspace/softlab` | **0** — `Successfully installed asttokens-3.0.2 comm-0.2.3 contourpy-1.4.0 cycler-0.12.1 executing-2.3.0 fonttools-4.68.0 h5py-3.16.0 imageio-2.38.1 ipython-9.18.1 ipython-pygments-lexers-1.1.1 ipywidgets-8.1.9 jedi-0.20.1 jupyterlab_widgets-3.0.17 kiwisolver-1.5.1 matplotlib-3.11.2 matplotlib-inline-0.2.2 narwhals-2.28.0 numpy-2.5.4 packaging-26.3 pandas-3.1.1 parso-0.8.7 pexpect-4.9.0 pillow-13.0.0 plotly-7.1.0 prompt_toolkit-3.0.53 psutil-7.2.2 ptyprocess-0.7.0 pure-eval-0.2.4 pygments-2.21.0 pyparsing-3.3.3 python-dateutil-4.0.0 pyvisa-1.16.2 scipy-1.18.1 softlab-0.3.0 stack_data-0.6.3 traitlets-5.16.1 typing_extensions-4.16.0 tzdata-2026.6 wcwidth-0.9.4 widgetsnbextension-4.0.16` | ~40 s |
| F3 | `/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/opencode/m03-ac1-transcript/venv/bin/python -m pip install pyvisa-sim` | **0** — `Successfully installed PyYAML-6.0.3 pyvisa-sim-0.7.1 stringparser-0.7` | ~8 s |
| F4 | `python -m unittest discover -s tests -p 'test_*.py'` | **0** — `Ran 521 tests in 480.586s` / `OK` (zero failures, zero errors, zero skips) — **standing record from the `6fe5acb` correction run; applicability stated below** | 8 min 03 s wall |
| F5 | `python -m compileall -q softlab` | **0** (no output) — **standing record from the `6fe5acb` correction run; applicability stated below** | <1 s |
| F6 | `python -c "import softlab; print(softlab.__version__)"` | **0** — prints `0.3.0` — **standing record from the `6fe5acb` correction run; applicability stated below** | 1.8 s |
| F7 | `/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/opencode/m03-ac1-transcript/venv/bin/python -m unittest tests.test_m03_p4_walkthrough.W1ThermalV1OriginalVsRerun tests.test_m03_p4_walkthrough.W2ThermalV2OriginalVsRerun tests.test_m03_p4_walkthrough.W3SignalOriginalVsRerun -v` | **0** — `Ran 3 tests in 109.289s` / `OK` (all three `... ok`) | 2 min 07 s wall |

F4–F6 applicability (recorded, not re-run): F4 full discovery, F5
compileall and F6 import smoke stand from the previous correction run on
candidate `6fe5acb` (same product-code content — the four commits since
then, `2697fd0` and `9a69cfb` (log documents only), `218d8fc` (docstring
whitespace only) and `fefecf3` (acceptance document only), touch no
product code; the instrumentation commit `93336e0` adds only evidence
fields to the walkthrough suite and changes no product code, so the F4
suite result remains applicable to it by content). The 8-minute full
discovery is not repeated; F7 above re-executes the W1–W3 A/B scenarios
in THIS new fresh venv with per-session command logging, which is the
gap the acceptance re-review identified.

F4 note: this full-suite run includes the in-suite W4 class, which created
its OWN nested fresh venv (`.../softlab-m03-p4-freshenv-2gpf8b9a/venv`)
inside the run — expected per design. That nested W4 venv (core
dependency set only, no pyvisa-sim) recorded creation exit 0, editable
install exit 0, isolation facts (`base_site_packages_on_path: false`,
`project_only_importable: {nest_asyncio: false, pyvisa_sim: false}`), and
its minimal thermal coordinated workflow passed (`cli_exit: 0`,
`status: ok`, `execution_id` 9034fb55-180c-40e9-8bfb-487e59aa47a0,
`group_id` 9eadb22b-5761-47df-8b66-ac41f5c7cee8, sha256
`f48af3e490d47dc94096c3560ae6be1c3cd1935452877c84c6c12c1ca7da081e`).
The nested W6 legacy-suite child ran the same 521 tests
(`Ran 521 tests in 233.532s` / `OK (skipped=1)` — the one skip is the
suite-in-suite re-entrancy guard inside that subprocess only). No nested
environment-creation failure occurred; nothing was silently skipped.

F7 per-scenario outcomes (fresh environment, candidate `93336e0`, all
sessions `clean_prefix: true`, empty `SOFTLAB_*` env vars,
driver_stderr empty):

- **W1 (thermal v1 `coordinated`) PASS.** Sessions A/B PIDs 97753/98019;
  `execution_id` bf0e82f9-d3a0-4da1-82b7-9bb6c11b5058 vs
  2ff39537-3d1e-4601-87f7-66472e7232d8; `group_id` eee3fce4-ae21-406f-b5f3-a6beff63dd0d vs
  7d4720e7-f680-40db-8c23-351664e68265 (distinct); sha256 `fe048471…` / `606d2070…`;
  4 bootstrap inspections (child PIDs 98035/98039/98047/98055) all `status: ok` on
  fresh copies; oracle, tolerances, and A-vs-B assertions held.
- **W2 (thermal v2 `direct`) PASS.** Sessions A/B PIDs 98065/98081;
  `execution_id` 05f9481d-0c7f-4d40-8f96-1a5f8276fda3 vs
  59d6c8ca-ba48-4a3b-9b37-d1709cf15160; `group_id` 18ba82c8-9c12-4060-a1c8-3f5e94cf0ae8 vs
  7f127cf4-da1b-49d8-9138-38313a29ada2; sha256 `28949542…` / `1e62a3e6…`; 4 inspections (child PIDs
  98097/98107/98109/98119) all `ok`; oracle and A-vs-B held.
- **W3 (signal) PASS — DEP-1 present.** Sessions A/B PIDs 98127/98139;
  stored columns `signal=[6,7,8]`, `reference=[3,4,5]` exact;
  caller-derived `difference=[3,3,3]` recomputed and asserted absent from
  stored columns; read trace `[('signal',1),('reference',1),('signal',2),
  ('reference',2),('signal',3),('reference',3)]` identical in both
  sessions; non-consumptive asserted; declared column order
  `(timestamp, signal, reference)`; identities distinct
  (7bcc7f7c-84d0-4757-a4dc-7eb7cebc76fb/e335e998-162e-4ec5-ad92-d9f248c0f9c5 vs
  054cbbb8-8f87-478b-a21e-e5ffe16098f2/648876a7-b1e4-4030-9367-0ebd52020f09);
  4 inspections (child PIDs 98153/98161/98163/98180) all `ok`; sha256 `fe38928b…` /
  `8a599a82…` preserved.

### Verbatim per-session command transcript (W1–W3, fresh environment)

Every command below is the real argv array exactly as spawned, emitted by
the instrumented walkthrough suite (`93336e0`) into the F7 run log and
transcribed here verbatim; all exited **0**. Key outputs are the emitted
evidence facts. Temp roots are `tempfile` directories deleted on teardown;
their real paths are recorded here before deletion. In the argv arrays,
`$FE` = `/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/opencode/m03-ac1-transcript/venv/bin/python`
(the fresh-env interpreter).

**Session drivers (6 — two per scenario):**

1. W1 A — `["$FE", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-7ps0felx/driver_01.py", "/Users/edward/workspace/softlab", "coordinated", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-7ps0felx/out"]`
   exit 0; key output: `overall_success: true`, `acquisition: completed`, 8/8 rows, `execution_id` bf0e82f9-d3a0-4da1-82b7-9bb6c11b5058, `group_id` eee3fce4-ae21-406f-b5f3-a6beff63dd0d, artifact `…/7ps0felx/out/thermal.h5`, sha256_before `fe048471…`, `clean_prefix: true`.
2. W1 B — `["$FE", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-s3q5s0f9/driver_02.py", "/Users/edward/workspace/softlab", "coordinated", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-s3q5s0f9/out"]`
   exit 0; key output: `execution_id` 2ff39537-3d1e-4601-87f7-66472e7232d8, `group_id` 7d4720e7-f680-40db-8c23-351664e68265, artifact `…/s3q5s0f9/out/thermal.h5`, sha256_before `606d2070…`, `clean_prefix: true`.
3. W2 A — `["$FE", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-oairuqaa/driver_09.py", "/Users/edward/workspace/softlab", "direct", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-oairuqaa/out"]`
   exit 0; key output: `execution_id` 05f9481d-0c7f-4d40-8f96-1a5f8276fda3, `group_id` 18ba82c8-9c12-4060-a1c8-3f5e94cf0ae8, artifact `…/oairuqaa/out/thermal.h5`, sha256_before `28949542…`, `clean_prefix: true`.
4. W2 B — `["$FE", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-bxdvuoqo/driver_10.py", "/Users/edward/workspace/softlab", "direct", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-bxdvuoqo/out"]`
   exit 0; key output: `execution_id` 59d6c8ca-ba48-4a3b-9b37-d1709cf15160, `group_id` 7f127cf4-da1b-49d8-9138-38313a29ada2, artifact `…/bxdvuoqo/out/thermal.h5`, sha256_before `1e62a3e6…`, `clean_prefix: true`.
5. W3 A — `["$FE", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-h6dsnndh/driver_17.py", "/Users/edward/workspace/softlab", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-h6dsnndh/out"]`
   exit 0; key output: `acquire(points=3)` + DEP-1 `persist` in one process; `execution_id` 7bcc7f7c-84d0-4757-a4dc-7eb7cebc76fb, `group_id` e335e998-162e-4ec5-ad92-d9f248c0f9c5, artifact `…/h6dsnndh/out/signal.h5`, sha256_before `fe38928b…`, `non_consumptive: true`, `differences: [3,3,3]`, `clean_prefix: true`.
6. W3 B — `["$FE", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-8jv8cuso/driver_18.py", "/Users/edward/workspace/softlab", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-8jv8cuso/out"]`
   exit 0; key output: `execution_id` 054cbbb8-8f87-478b-a21e-e5ffe16098f2, `group_id` 648876a7-b1e4-4030-9367-0ebd52020f09, artifact `…/8jv8cuso/out/signal.h5`, sha256_before `8a599a82…`, `non_consumptive: true`, `differences: [3,3,3]`, `clean_prefix: true`.

**Bootstrap inspections (12 — two per artifact; each on its own fresh
sha256-verified copy, originals never loaded in place):**

7. W1 A #1 — `["$FE", "/Users/edward/workspace/softlab/tests/run_record_inspect_bootstrap.py", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-7ps0felx/inspect-copy-03.h5", "--profile", "thermal"]` exit 0, `status: ok` (child PID 98035).
8. W1 A #2 — `["$FE", "/Users/edward/workspace/softlab/tests/run_record_inspect_bootstrap.py", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-7ps0felx/inspect-copy-04.h5", "--profile", "thermal"]` exit 0, `status: ok` (child PID 98039).
9. W1 B #1 — `["$FE", "/Users/edward/workspace/softlab/tests/run_record_inspect_bootstrap.py", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-s3q5s0f9/inspect-copy-05.h5", "--profile", "thermal"]` exit 0, `status: ok` (child PID 98047).
10. W1 B #2 — `["$FE", "/Users/edward/workspace/softlab/tests/run_record_inspect_bootstrap.py", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-s3q5s0f9/inspect-copy-06.h5", "--profile", "thermal"]` exit 0, `status: ok` (child PID 98055).
11. W2 A #1 — `["$FE", "/Users/edward/workspace/softlab/tests/run_record_inspect_bootstrap.py", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-oairuqaa/inspect-copy-11.h5", "--profile", "thermal"]` exit 0, `status: ok` (child PID 98097).
12. W2 A #2 — `["$FE", "/Users/edward/workspace/softlab/tests/run_record_inspect_bootstrap.py", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-oairuqaa/inspect-copy-12.h5", "--profile", "thermal"]` exit 0, `status: ok` (child PID 98107).
13. W2 B #1 — `["$FE", "/Users/edward/workspace/softlab/tests/run_record_inspect_bootstrap.py", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-bxdvuoqo/inspect-copy-13.h5", "--profile", "thermal"]` exit 0, `status: ok` (child PID 98109).
14. W2 B #2 — `["$FE", "/Users/edward/workspace/softlab/tests/run_record_inspect_bootstrap.py", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-bxdvuoqo/inspect-copy-14.h5", "--profile", "thermal"]` exit 0, `status: ok` (child PID 98119).
15. W3 A #1 — `["$FE", "/Users/edward/workspace/softlab/tests/run_record_inspect_bootstrap.py", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-h6dsnndh/inspect-copy-19.h5", "--profile", "signal"]` exit 0, `status: ok` (child PID 98153).
16. W3 A #2 — `["$FE", "/Users/edward/workspace/softlab/tests/run_record_inspect_bootstrap.py", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-h6dsnndh/inspect-copy-20.h5", "--profile", "signal"]` exit 0, `status: ok` (child PID 98161).
17. W3 B #1 — `["$FE", "/Users/edward/workspace/softlab/tests/run_record_inspect_bootstrap.py", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-8jv8cuso/inspect-copy-21.h5", "--profile", "signal"]` exit 0, `status: ok` (child PID 98163).
18. W3 B #2 — `["$FE", "/Users/edward/workspace/softlab/tests/run_record_inspect_bootstrap.py", "/var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/softlab-m03-p4-8jv8cuso/inspect-copy-22.h5", "--profile", "signal"]` exit 0, `status: ok` (child PID 98180).

This transcript is committed evidence; the temporary raw run log
(`/var/folders/.../T/opencode/m03-ac1-transcript/f7-command-transcript.log`)
is retained only as provenance and is not required to audit this record.

Fresh-env versions actually observed (probed in the fresh venv, never
copied; re-probed in the `m03-ac1-transcript` environment on
2026-11-05 with identical values): Python `3.13.15 | packaged by
conda-forge | (main, Aug 10 2026, 13:01:04) [Clang 19.1.7 ]` on
macOS-26.6.2-arm64; softlab `0.3.0`
(editable, `softlab/__init__.py` from this checkout); numpy `2.5.4`;
pandas `3.1.1`; h5py `3.16.0` / libhdf5 `2.0.0`; matplotlib `3.11.2`;
pyvisa `1.16.2`; pyvisa_sim `0.7.1`; PyYAML `6.0.3`; stringparser `0.7`;
sqlite3 module `2.6.0` / library `3.53.4` (re-probed in the transcript
environment, identical values). Editable-install resolved set, verbatim
(from F2 in the `m03-ac1-transcript` environment, identical to the
correction-run set):
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
widgetsnbextension-4.0.16`; then `Successfully installed PyYAML-6.0.3
pyvisa-sim-0.7.1 stringparser-0.7`. Full `pip list --format=freeze` in
the transcript environment (44 entries incl. `pip==26.2.1`), verbatim:
`asttokens==3.0.2 comm==0.2.3 contourpy==1.4.0 cycler==0.12.1
executing==2.3.0 fonttools==4.68.0 h5py==3.16.0 ImageIO==2.38.1
ipython==9.18.1 ipython_pygments_lexers==1.1.1 ipywidgets==8.1.9
jedi==0.20.1 jupyterlab_widgets==3.0.17 kiwisolver==1.5.1
matplotlib==3.11.2 matplotlib-inline==0.2.2 narwhals==2.28.0 numpy==2.5.4
packaging==26.3 pandas==3.1.1 parso==0.8.7 pexpect==4.9.0 pillow==13.0.0
pip==26.2.1 plotly==7.1.0 prompt_toolkit==3.0.53 psutil==7.2.2
ptyprocess==0.7.0 pure_eval==0.2.4 Pygments==2.21.0 pyparsing==3.3.3
python-dateutil==4.0.0 PyVISA==1.16.2 PyVISA-sim==0.7.1 PyYAML==6.0.3
scipy==1.18.1 softlab==0.3.0 stack-data==0.6.3 stringparser==0.7
traitlets==5.16.1 typing_extensions==4.16.0 tzdata==2026.6 wcwidth==0.9.4
widgetsnbextension==4.0.16`. `pip_warnings: []`; only the two pip upgrade
notices (verbatim: `[notice] A new release of pip is available: 26.2.1 ->
26.3` and the update-command notice).

Fresh-env warnings, verbatim and dispositioned:

1. `DeprecationWarning: version is deprecated and will be removed in
   Python 3.14` — exactly once, from the version-facts probe reading
   `sqlite3.version` on Python 3.13.15; approved recorded baseline
   (freeze §6 item 4 / `install_warning_policy`).
2. `Parameter probe is neither settable and gettable` (`UserWarning`) —
   re-confirmed firing in the fresh-env full-suite W6 debt probe
   (child PID 78308); the established deliberate DEFECT-2 warning,
   documented since release_8, not a new warning.
3. The in-suite nested W4 item-5 disposition: `warning_count: 0` across
   its fresh-environment subprocesses, `undispositioned: 0` (its sqlite3
   baseline captured by its dedicated version probe).
4. No undispositioned new warning was observed anywhere in the fresh
   environment; the 3.9-gated pyparsing/pandas baselines were not
   applicable (no Python 3.9 interpreter on this host).

Post-correction sanity check (2026-11-05, project `.venv` = Python
3.13.15): after the documentation corrections in this file and the
principle report, the walkthrough module was re-run bounded as
`.venv/bin/python -m unittest tests.test_m03_p4_walkthrough -v` —
`Ran 13 tests in 404.443s` / `OK`, exit 0 (W4 created its nested venv
and W6 ran its nested legacy suite again as designed). Nothing regressed
from the doc fixes. After the argv instrumentation commit `93336e0`, the
same bounded W1–W3 invocation was re-run in the project environment:
`Ran 3 tests in 34.926s` / `OK`, exit 0 — instrumentation changed only
the emitted evidence, not behavior.

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
| W4 | Fresh-environment walkthrough (freeze §6, items 1–6) | **PASS — corrected**: complete fresh-environment evidence now recorded | In-suite items 1, 2, 4, 5, 6: see "W4 environment facts" below (verbatim). Item 3 and the plan's full fresh-env command sequence: **complete execution** — fresh venv (literal F1 creation command with interpreter/target paths, editable install, `pyvisa-sim` test dependency), full unittest discovery 521/521 OK (480.586s, standing `6fe5acb` record with applicability note), `compileall` clean, import smoke `0.3.0`, and explicit W1–W3 A/B sessions all PASS in the fresh venv on `93336e0` WITH the verbatim per-session command transcript committed below (six driver + twelve bootstrap argv lines); the in-suite run's nested W4 venv was created and passed inside discovery. See "Fresh-environment execution record (complete, Issue 1 correction)" and "Verbatim per-session command transcript". The earlier narrowed PASS is superseded | none |
| W5 | Clean-process walkthrough | **PASS** | See "W5 parent-baseline fact" below; session PIDs 58728/58744 (≠ parent 57701), inspection PIDs 58762/58772/58782 all distinct; fresh-copy carries across roots decode identically; parent softlab-module set unchanged after all activity | none |
| W6 | Compatibility and debt verification | **PASS** | Legacy suite green in nested full run (PID 58815, `Ran 521 tests … OK (skipped=1)`, project interpreter); debt table fully re-confirmed (below) | none |
| W7 | Guidance verification (DEP-3) | **PASS** — `docs/run-record.md` present | Guide thermal CLI ran verbatim (PID 60554, exit 0, artifact `/tmp/thermal-run-01/thermal.h5` produced and cleaned up); guide signal recipe ran verbatim (PID 60570, exit 0, printed exactly 3 fields); §3 bootstrap executed for both profiles against the guide's own produced artifacts (PIDs 60596 signal, 60606 thermal, both exit 0, `status: ok`) | none |

## W4 environment facts (verbatim)

These facts describe the in-suite W4 class executed under full-suite
discovery (its own nested fresh venv, core dependency set only). The
tester-executed complete fresh-environment evidence (separate venv,
editable install + `pyvisa-sim`) is in "Fresh-environment execution
record (complete, Issue 1 correction)" above; both stand together.

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
- **Item 3 — documented command sequence.** Recorded exactly as executed
  by the in-suite W4 class.
  **Correction (2026-11-05):** the narrowing recorded in this item's
  original text and in "Deviations" below is **closed** — the complete
  fresh-environment execution of the plan's item-3 enumeration (full
  unittest discovery, `compileall`, import smoke, and W1–W3 A/B sessions
  in the fresh venv) was performed by the tester on candidate `6fe5acb`
  and is recorded verbatim in "Fresh-environment execution record
  (complete, Issue 1 correction)" above. The original record is kept
  below for traceability of what the automated class itself executes.
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

1. **W4 item 3 — executed scope narrower than the plan's enumeration.**
   **CLOSED by correction (2026-11-05).** The implemented (and
   developer-reviewed) W4 class executes the documented minimal workflow
   in the fresh environment (thermal CLI + bootstrap inspection) but does
   **not** itself re-run the full unittest suite, `compileall`, the import
   smoke, or the W1–W3 sessions inside the fresh venv. That narrowing was
   acceptance Issue 1 (Blocker): the W4 verdict had been reported PASS on
   the narrower scope, which was an overclaim. Correction executed: the
   tester ran the complete plan-required fresh-environment commands and
   all three A/B scenarios on the frozen candidate `6fe5acb` in a
   tester-created fresh venv (with the `pyvisa-sim` test dependency the
   full suite actually needs); all passed and are recorded verbatim in
   "Fresh-environment execution record (complete, Issue 1 correction)".
   The automated W4 class itself is unchanged (its items 1–6 remain as
   recorded under "W4 environment facts").
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
2. The **in-suite W4** fresh environment deliberately contains **no
   optional extras** (`notebooks` group: jupyterlab/nest_asyncio/
   pyvisa-sim absent) — recorded as a fact in item 6, not silently scoped
   away; the isolation probe asserts these are not importable there. The
   **tester-executed complete fresh environment** (Issue 1 correction)
   intentionally differs in exactly one recorded way: it additionally
   contains `pyvisa-sim==0.7.1` (+ `PyYAML`, `stringparser`), the test
   dependency required by the full suite (`test_tu_visa_contracts.py`
   Group D real `@sim` backend; `test_compatibility.py::test_visa_simulator`).
   It still contains no jupyterlab/nest_asyncio, none of which the suite
   imports.
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

Corrected 2026-11-05 per acceptance Issue 1: each cell now states
precisely which evidence is fresh-environment (FE) versus project-
environment (PE). Prior versions of this matrix claimed items 1–6 for
AC06 while the fresh environment had executed only the minimal workflow;
that overclaim is withdrawn.

| Plan element | M03-AC05 | M03-AC06 |
| --- | --- | --- |
| W1/W2/W3 oracles, tolerances, exclusions, A-vs-B, recorded configuration (freeze §2–§5.3) | ✔ primary evidence (all PASS, per-artifact + A-vs-B) | ✔ FE: all three A/B scenarios re-executed explicitly in the tester fresh venv on `93336e0`, all PASS, with the committed verbatim transcript of all six driver argv and all twelve bootstrap argv (command, exit, key output) in "Verbatim per-session command transcript"; also executed under the standing FE full discovery (`6fe5acb`) |
| W4 fresh-environment creation/install/commands/versions/warnings/limitations (freeze §6) | rerun determinism cross-check (CLI + bootstrap re-executed in fresh env) | ✔ items 1, 2, 4, 5, 6: in-suite nested fresh venv, verbatim. ✔ item 3 complete sequence: F1 literal creation command (real interpreter and target paths), F2/F3 installs with verbatim resolved sets, full discovery 521/521 (standing `6fe5acb` record, applicability stated), compileall, import smoke, and W1–W3 A/B with the verbatim per-session command transcript (FE on `93336e0`). Versions/warnings: FE verbatim |
| W5 process separation (freeze §5.2 steps 4/6) | ✔ (PIDs, clean prefixes, parent-baseline fact, no in-process decode) | ✔ executed under FE full discovery |
| W6 compatibility, legacy suite, debt inventory | ✔ (521-test green run PE; debt table re-confirmed) | ✔ FE: legacy suite green inside the FE 521-test discovery run; DEFECT-2 re-confirmed firing in FE |
| W7 guidance vs actual behavior | — | PE (docs/run-record.md; every documented command executed verbatim); also executed under FE full discovery |

Exit criteria of the plan: 1–7 all satisfied (no FAIL/BLOCKED; no freeze
reopen — zero deviation between C5b in-memory evidence and the executed
stored path was observed). The W4 item-3 narrowing that originally left
plan element W4 item 3 partially evidenced is corrected by the
fresh-environment execution record above; no exit criterion rests on the
superseded narrowed run.
