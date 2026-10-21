# M01-W02-001 independent test results

Tester: sw-mike. Date: 2026-10-22 Asia/Shanghai.
Subject: committed `2095c0865c530a920663a9c69b6bb913387d627d`, including
implementation 1ba603d, guide 6a0e6d3 and cleanup repair 27d8a17.
Branch: codex/m01-w02-controlled-execution. Designer iteration 2 approved
2095c08 and closed CR-W02-01 by source review before this testing gate.
Verdict: **PASS for independent local testing and guide walkthrough**.
All Tester-owned scoped feature/regression failures are CLOSED by actual green
execution below. Coordinator principle/formal gate and exact candidate CI are
pending; task integration/release acceptance is not certified here.

## Actual commands and environment

Repository-root environment for every runtime command:
`MPLBACKEND=Agg MPLCONFIGDIR=/tmp/softlab-w02-mpl
XDG_CACHE_HOME=/tmp/softlab-w02-cache`. Python is the existing `.venv/bin/python`,
3.13.15, conda-forge, Clang 19.1.7 on macOS. Versions: softlab 0.3.0,
NumPy 2.5.3, SciPy 1.18.1, pandas 3.0.6, Matplotlib 3.11.2, Plotly 7.1.0,
h5py 3.16.0, PyVISA 1.16.2, pyvisa-sim 0.7.1, ipywidgets 8.1.9,
imageio 2.37.4. Version evidence used importlib.metadata.version and
`import softlab; print(softlab.__version__)`; no dependency was installed/changed.

| Actual command / check | Result |
| --- | --- |
| `.venv/bin/python -W error -m unittest discover -s tests -p 'test_*.py'` | Exit 0; **257 tests, 15.382s, OK**, no skips. Includes W01 failure matrix, legacy characterization, Coordinator reset/failure tests and all W02 contracts. |
| `.venv/bin/python -W error -m unittest discover -s tests -p test_thermal_execution.py -v` | Exit 0; **21 methods, 6.975s, OK**, no skips; six nested cleanup subcases pass inside two methods. |
| `.venv/bin/python -W error -m compileall -q softlab examples tests` | Exit 0, no diagnostics. |
| `.venv/bin/python -W error` import/version collection | Exit 0, softlab 0.3.0 and versions above, no diagnostics. |
| Guide commands/snippets through independent temporary-path harness | Exit 0 after fixture correction described below; normal/early/mid/late CLI, stored inspection and exact partial/reset/repeat journey pass. |
| Python pathlib/re validation of guide plus release_5 Markdown | 18 files, 38 explicit local links resolve; final newlines and no trailing whitespace pass before this report. |
| `git diff --check` / `git status --short` | Exit 0, no diagnostics; clean before results records. |
| `git rev-parse HEAD '@{upstream}'` | Both 2095c0865c530a920663a9c69b6bb913387d627d before records. |
| `git diff --numstat e0b5a135 HEAD -- softlab pyproject.toml` | Empty: library source/configuration unchanged by implementation task. |

All named runtime checks used -W error: no Python warnings, cache-permission
warnings, unexpected logging warnings, compilation errors or skips were reported.
Backend connection prints are informational. Legacy expected failure warnings
are explicitly captured by tests. This does not certify unconfigured linters,
static analysis, other operating systems, other Python versions or hardware.

## Independent guide walkthrough

Read saved docs/user-guide/thermal-experiment.md, then from the repository root
ran its example command using `.venv/bin/python -W error` and fresh paths under
an automatically removed TemporaryDirectory; writable cache/Agg variables above.
No existing experiment directory was used or overwritten.

| Journey | Executed evidence |
| --- | --- |
| Normal CLI | `examples/thermal_experiment.py --output-dir <temporary>/normal`: exit 0, Complete announcement, HDF5 plus PNG exist; independently opened HDF5 read-only and asserted eight tick rows. |
| Before first tick | Same command plus `--stop-after 0`: exit 1, acquisition=cancelled diagnostic, no Complete announcement, no HDF5/PNG. |
| After three rows | Same command plus `--stop-after 3`: exit 1 with cancelled diagnostic, no Complete announcement or persisted/plot partial artifacts. |
| Late request | Same command plus `--stop-after 8`: exit 0, Complete announcement and both artifacts with eight stored rows. |
| Stored inspection | Executed the guide's importlib/h5py/_reload_group Python snippet with only /tmp/thermal-run-01 replaced by the fresh normal path. Added assertions for eight loaded rows and first temperature 20; exit 0. |
| Partial inspection, deliberate reset, fresh repeats | Extracted and executed the final documented PYTHON heredoc body verbatim. Its assertions prove cancelled 3-row/tick result, acknowledged stop, drained/closed work, no persistence, reset preserving terminal snapshot, two successful distinct-UUID fresh runs and identical numeric tables excluding timestamps. Exit 0. |

The independent temporary harness was run as
`.venv/bin/python -W error /tmp/softlab-w02-independent-walkthrough.py`.
It extracted the saved guide snippets rather than copying a different API path.
Initial harness execution reached successful inspection but then asserted that
pandas printed a full column name; normal pandas truncation hid that name.
Tester corrected this harness-only oracle to assert the actual loaded record,
then repeated the entire CLI/inspection/reset/repeat journey successfully. This
was not a product failure or production change. All synthetic output directories
were removed. The final guide describes software-only request/ack limits,
no resume/retry, separate stage outcomes, owned resources, reset/fresh repeats,
backend limits and original-cause/cleanup diagnostics consistently with tests.

## Acceptance traceability and Tester failure closure

| Acceptance | Actual assertions / outcome |
| --- | --- |
| AC-01 | Unchanged 19 W01 tests pass in full discovery: eight-row independent Decimal oracle, delayed source ticks, finite values, exact schema/units, fresh UUID, HDF5 roundtrip integrity and meaningful headless PNG; compatibility/import behavior retained. |
| AC-02 | Seven legacy execution characterization tests pass: reset abort flag versus termination, failed status/CancelledError behavior, count record append on reuse and stop versus explicit task drainage/borrowed loop ownership. |
| AC-03 | StopRequest idempotence/separate acknowledgment and real evolve/getter in-flight request tests pass; no unsupported preemption claim. |
| AC-04 | Zero-row early stop, exact row-3 stop with independent actual step observer through cleanup, no post-ack step, and late row-8 request completion priority pass. |
| AC-05 | Real evolve failure after 3 complete rows retains rows=3/tick=3 and exact cause, no retry/save/plot; failure takes precedence over same-cycle request. |
| AC-06 | Real sample failure after successful next step retains rows=3/tick=4 and exact cause, no fabricated/partial row, no retry/save/plot; request cannot erase failure. |
| AC-07 | Completed acquisition stays completed on save/reload failure; persistence failed and no plot. Plot/savefig/figure-close faults preserve separately verified data with unsuccessful overall result. Nested helper primary identity and cleanup_errors are asserted. |
| AC-08 | Empty/nonempty complete-row partials inspectable; not_attempted persistence/plot and no files. RunFailure remains RuntimeError, retains result/original chain. CLI cancellations fail visibly; W01 CLI error cases remain green. |
| AC-09 | Explicit reset only after terminal/drained/closed work; live/undrained/unclosed guards reject. Initial model tick/time/state restored without acquisition; terminal snapshots retained. Actual reset factory injection preserves previous committed state/boundary; existing Coordinator atomic-reset tests pass. |
| AC-10 | Two fresh runs after cancellation, model/sample failure and reset journeys have finite equal numeric traces, distinct IDs, N rows and reopenable fresh files. Two fresh successful runs after reset factory failure also pass. |
| AC-11 | Actual captured scheduler stop/closed loop and declared drain facts checked across paths. W01 independent stop/drain/close/backend/raw-handle/scratch/figure obstruction matrix remains green. New six nested cleanup cases assert structured errors, primary sentinel identity, reader handles closed, independent scratch attempt and borrowed figure preservation. Borrowed/default scheduler/loop/figure tests pass. No teardown reset/extra tick. |
| AC-12 | Root command and guide snippets independently executed above with actual row/artifact assertions; timing/outcome/resource/reset/no-resume explanations inspected. |

Tester closes the initial feature red outcomes recorded in M01-W02-001-red.md
(3 CLI failures and 24 absent-surface error outcomes) based on all 21 feature
methods now passing. Tester closes all six nested-helper regression failures
from M01-W02-001-review-regression.md based on their actual green subcases.
The earlier test-authoring getter and borrowed-loop fixture issues were corrected
and rerun as recorded in characterization/regression evidence; no fixture source
needed correction during this final gate. Designer-owned CR-W02-01 closure remains
the separate review verdict, not a Tester decision. No scoped test failure remains.

## Eleven-principle task evidence

| Principle | Tester assessment / remaining gate |
| --- | --- |
| 1 Architecture | Local example composes existing tu/huo/shui/jin; no softlab source change. Actual arch.md release-end update remains Architect work; do not treat PLANNED envelope as implemented documentation. |
| 2 Characterize first | Source/caller characterization d7b73a6 preceded design and behavior changes; seven preserved assertions plus W01 compatibility pass. |
| 3 TDD gates | Plan84597bb, Developer reviewd7036c4, design2bec5a3/Architectbe26e7e, redb331c13, implementation/review/fix2095c08, then this independent test evidence. |
| 4 One task | Setup integrated before sole active M01-W02-001 from dev e0b5a135; this gate does not mark Done or begin another task. |
| 5 Ownership | Tester adds/asserts/executes tests and closes own failures; Designer closed review issue; no product/architecture/code-review decision claimed. UI/Figma N/A. |
| 6 Evidence | Prior subject/upstream equality verified; these results/inspection/board require immediate docs(log) commit/push before handoff. |
| 7 Environment | Actual versions, full/focused/compile/import/warning and guide evidence above; configured candidate 3.9/3.13 CI still pending. |
| 8 Baseline debt | R4-BASE and historical debt explicitly remain open with scoped bounded measures; no new failure or warning found in named checks; no silent waiver. |
| 9 Instruments/data | Synthetic model only, fresh temporary files, read-only inspection/copy loading, owned-resource/failure assertions; no instrument access or physical stop claim. |
| 10 Minimality | No library/dependency/Python-support/storage-format change; optional example helpers preserve existing successful run path. |
| 11 Reproducibility | Local evidence/diff/links/scope complete; coordinator principle gate, exact candidate CI/dev integration, actual architecture closure and PO release acceptance remain pending. |

## Baseline disposition and limits

R4-BASE-SCHED-01 remains unfixed library debt: use the existing private serial
executor; legacy cancellation/status/stop-drain limitations are characterized,
not repaired. R4-BASE-META-01 remains: explicit text metadata hydration; W01
roundtrip checks pass. R4-BASE-HDF5-01 remains: copy-only reopening avoids destructive
legacy reader behavior, with source-integrity and cleanup tests passing. These
bounded accepted W01 measures were retained and regressed; no broad repair or
library-debt closure is claimed. OBS-004/005/006 and DEFECT-2 remain historical
out-of-scope debt under existing disposition, with no new observed failure/warning
in named checks. Coordinator/PO retain release-scope disposition authority.

Only local macOS Python 3.13 was executed. Ubuntu Python 3.9/3.13 must pass on
the exact integration candidate; configured matrix or prior setup CI is not this
candidate's evidence. Hardware/UI, durable partial persistence/resume, general
process reuse/preemption, W03 manifest/W04 adapters and publication are excluded.
No production changes were made by Tester in this gate. This PASS is not task
integration, release acceptance or main promotion.
