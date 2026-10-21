# M01-W01-001 independent test results

Owner: sw-mike (Tester), 2026-10-18. Verdict: **PASS for tested candidate**
`a7953e344d54113fcacabf9881832d963239b287` (clean committed branch at start).
Code review CR-01 is Reviewer-closed at `c722b9d`; this report independently
verifies runtime behavior, rather than adopting Developer self-test claims.
Integration/final-log SHA CI and release acceptance remain later gates.

## Actual local commands and environment

Python `.venv/bin/python`: 3.13.15 conda-forge, macOS Clang 19.1.7;
softlab 0.3.0, numpy 2.5.3, scipy 1.18.1, pandas 3.0.6,
matplotlib 3.11.2, h5py 3.16.0, PyVISA 1.16.2, pyvisa-sim 0.7.1.
Set MPLCONFIGDIR=/tmp/softlab-final-mpl, XDG_CACHE_HOME=/tmp/softlab-final-cache,
MPLBACKEND=Agg for all Python executions. Outputs used owned temporary directories.

| Executed check | Actual result |
| --- | --- |
| `.venv/bin/python -W error -m unittest discover -s tests -p 'test_*.py'` | Exit 0; **229 tests**, **4.748s**, OK; no skipped tests or Python warnings reported. |
| `.venv/bin/python -W error -m compileall -q softlab examples tests` | Exit 0; no compilation errors/output. |
| `.venv/bin/python -W error -c "import softlab; print(softlab.__version__)"` (combined with environment collection) | Exit 0; 0.3.0; no Python warnings. |
| `MPLBACKEND=Agg python examples/thermal_experiment.py --output-dir <fresh temporary directory>` twice, PATH prefixed with repository .venv/bin | Both exit 0, no stderr; completion announces absolute paths and fresh UUID. Durations 1.003s and 1.014s (observed, not SLA). |
| Same CLI against each occupied output directory | Both exit 1, stderr diagnostic, delivered HDF5 SHA256 unchanged. |
| Independent h5py read-only/Matplotlib image decode after each CLI | Both pass exact schema, eight finite expected temperatures, boundary/source columns, UUID/name/reserved/text metadata; PNG (480,640,4), nonzero dimensions and varying image content. |
| Local link targets in simulation and thermal guides | Pass. |
| `git diff --check` and `git diff --check e034825..HEAD` | Exit 0. |
| Tracked-file/scope inspection | Only example/tests/guides/process records changed; no library source, dependency/Python metadata or generated experiment artifact tracked. Existing softlab/logo.png is unrelated preexisting asset. |

First fresh cache use printed informational `Matplotlib is building the font
cache`; no unwritable-cache/fontconfig diagnostic or Python warning occurred.
Named `-W error` checks passed; no unconfigured lint/static-analysis claim.
CLI independent UUIDs were `759c01b0-35b8-4638-aba1-9679e4238972` and
`e438945d-be79-4558-9a52-1076ff36d591`. Temporary output was removed after checks.

## Acceptance traceability

All 19 thermal test methods (including parameterized fixtures) ran within the
229-test discovery, alongside 210 existing regression tests. No skips.

| Requirement | Independent passing evidence |
| --- | --- |
| AC01 | Documented root CLI twice, headless PNG/HDF5 and announced paths; bounded subprocess timeout; no instrument calls introduced. |
| AC02 | Test validation matrix: bool/nonfinite/nonreal/invalid signs, unsupported stability, invalid point counts; no graph/scheduler creation or final artifacts. Positive N=1, ratio=1 and negative finite temperature/power fixture passes. |
| AC03/04 | Default test spies exactly eight successful step boundaries, no reset/warmup, literal temperatures and independent Decimal closed form, source ticks 0..7, time 1..8, power, finite values and ordered names/units/labels/dependency flags. Wall timestamp excluded from repeat comparison. |
| AC05 | Encoded disk-derived metadata/config/timing inspected and decoded; two independent fresh-run UUIDs differ. |
| AC06 | Separate owned-copy legacy read, full identity/schema/value verification, repeated fresh _reload_group calls preserve source digest and all reserved attrs; backend false/None/raised operations and corrupt disk fields rejected. Returned backend path is delivered source. |
| AC07 | Plot spy verifies reopened data table/schema and line x/y/seconds/degC; figures released, borrowed figure preserved; independent CLI PNG decode passes. |
| AC08 | File/nonempty/symlink collision tests refuse before graph creation and preserve sentinel bytes; both CLI collision retries preserve source hash. |
| AC09 | Prepare/model/sample, backend, corruption, partial-copy, plot/savefig, scratch and suppressed-disconnect failures remain visible and unsuccessful; no retry/save on failed acquisition; plotting failure retains verified-data diagnostic. |
| AC10 | Success/failure owned scheduler/loop/backend/figure release, singleton/default borrowed-loop isolation, final committed boundaries, primary+cleanup errors, partial-copy removal, handle-close fallback and scratch obstruction diagnostics pass. New CR-01 three-subcase regression verifies cancelled-task drain attempt, independent close after drain exception, both cleanup diagnostics and writer cleanup despite close obstruction. No release is claimed where a mock intentionally obstructs it. |
| AC11 | Two fresh in-process runs and two CLI runs: exactly eight rows, matching deterministic values, distinct UUIDs, independent reopening; no global reset. |
| AC12 | Tester followed guide command using installed core environment; local links, model/latency/timestamps/layers/output/ownership/failure limits and safe artifact-preserving inspection documented. |
| NFR01 | 210 existing tests plus new contract assertions pass; diff leaves public imports/hooks/codecs/count/scan/data implementation unchanged. |
| NFR02 | No dependency or Python support change; explicit step-count/no simulation sleep by code/diff inspection; actual 3.9/3.13 candidate CI below. |
| NFR03 | Eight acquisition steps/rows, bounded test/CLI completion, durations above. |
| NFR04 | Approved design/reviewer evidence plus inspected typed English example helpers; no five-element restructuring. |
| NFR05 | Temporary synthetic fixture storage; tracked-artifact and clean initial status checks; no generated experiment output committed. |

TEST-01/02/03 test-contract corrections remain closed by Tester. The runtime
failure exercised by CR-01 red regression is now **CLOSED by Tester** on this
candidate's passing full suite; Reviewer CR-01 closure remains Reviewer-owned.
No current runtime failure or affected missing dependency remains open.

## Exact candidate GitHub Actions evidence

Tester executed escalated read-only `gh run list --commit
 a7953e344d54113fcacabf9881832d963239b287 --json ...`, then `gh run view
39141867382 --json headSha,conclusion,jobs,url` and read its logs.
[Compatibility run 39141867382](https://github.com/hoolheart/softlab/actions/runs/39141867382)
reports completed/success for this exact SHA. Both jobs, including dependency
installation, simulator requirement, regression, source compilation and import,
report success:

- [compatibility (3.9)](https://github.com/hoolheart/softlab/actions/runs/39141867382/job/117717774726)
- [compatibility (3.13)](https://github.com/hoolheart/softlab/actions/runs/39141867382/job/117717774524)

CI workflow runs ordinary unittest/compileall softlab/import; local checks add
-W error and compile examples/tests. Do not reinterpret CI as an unconfigured
warning-as-error or hardware/UI gate. This report and later process commits
create newer SHAs: **the final integration candidate's CI must be rechecked before
dev merge**; this tested SHA's green CI does not certify future commits.

## Baseline debt and remaining gates

Initial baseline cache diagnostics were resolved by writable temporary environment
paths, with no production fix. R4-BASE-SCHED-01, R4-BASE-META-01 and
R4-BASE-HDF5-01 remain explicitly tracked unfixed library debt, excluded from
incidental repair per Architect disposition; approved bounded scheduler and
copy-only/disk-metadata glue are exercised above and preserve acceptance. Existing
OBS-004/005/006 and DEFECT-2 remain outside this example's unchanged public paths;
no new observed failure/warning from them in named checks. No silent waiver or
claim that these defects are fixed. Any wider remediation needs separate scope.

Next: Architect actual-implementation update and principle inspection, then exact
final-SHA CI/dev integration and PO release acceptance as coordinated. Main
promotion requires separate release acceptance. Hardware, UI/Figma, publication,
W02 cancellation/resume and W03 rich manifests are N/A to this tested scope.
