# M01-W01-001 assertion test plan

Owner: sw-mike (Tester), 2026-10-18. Candidate baseline: setup `828a397` on
`codex/m01-w01-thermal-example`. Status: **PROPOSED; Developer review required**.
No example implementation or detailed design is supplied by this plan.

## Independent oracle and characterization

The default object boundary satisfies T_j = 30 - 10*(0.9**j), j >= 0.
The delivered sample at acquisition k is T_(k-1), because the coordinator
freezes S_(k-1) before evolving the object to T_k. Thus acquisition 1 receives
20 despite the command affecting the first object transition. Expected literal
samples: `[20, 21, 21.9, 22.71, 23.439, 24.0951, 24.68559, 25.217031]`.
At tick 8 the object itself is 25.6953279. Absolute tolerance is 1e-10;
expectations must never invoke the example's evolution callback.

Independent execution of the existing Coordinator with scalar state/input,
interval binding, two edges and Decimal closed-form assertions confirms this
oracle; see [baseline](baseline.md). Read/write calls before stepping preserve
boundary zero. Existing connected-guide tests and coordinator tests characterize
previous-boundary delivery, atomic failed ticks and callback exceptions.

Read sources/callers: common.py AtomJob/Counter/count, SimpleProcess,
run_process, concrete scheduler, simulation guide §12–13 and connected guide tests,
Coordinator, DataRecord/DataGroup, HDF5DataBackend, plot_record. Hooks run before
getter reads and row append. Sample failure after a successful hook can advance
a boundary without appending; runner failure must be checked explicitly.
HDF5 load enumerates dataset keys (normally alphabetical), stringifies metadata,
and pops reserved file attributes. Designer must choose compatible column order
and fresh reopen/read ownership; do not repair the backend in this task.

## Required assertion groups

Proposed test location: `tests/test_thermal_example.py` (Designer confirms).
Use unittest, unittest.mock, tempfile, installed core libraries and Agg only.
Tester implements executable tests after approved detailed design, before
production development; run against committed code to capture red then green.
Exact helper names, command, column names/order and injection seams belong to
Designer and must be reflected in approved assertions without weakening PRD.

| Test | Coverage | Required assertions |
| --- | --- | --- |
| TC01 command | AC01, AC12; NFR02/03 | Execute exact documented root command in subprocess with fresh selected temporary output; exit 0 only after round trip/PNG; announced paths exist; no notebook/display/VISA discovery; bounded termination timeout catches hangs, elapsed duration recorded (no performance SLA). |
| TC02 preflight | AC02 | Table-driven zero/negative C/G/dt, dt*G/C >1, NaN/+inf/-inf for each float field; N=0/-1/True/False/1.5/string rejected; valid positive boundary dt*G/C=1 and N=1 accepted. Each rejection raises/exits visibly, zero coordinator steps, no completed HDF5/PNG. Initial/ambient/command nonfinite rejected; finite negative temperatures/command are not rejected merely by sign. |
| TC03 acquisition | AC03/04; NFR01/03 | Literal oracle and independent closed form agree; exactly eight step calls and rows, ticks 1..8, simulation times 1..8, power 10, source boundaries 0..7, every numeric field finite. Trace write→step→read→append ordering; no hidden warm-up/final step; read/write alone preserve tick. If timestamp retained, label wall time and exclude from comparison. |
| TC04 schema/group | AC04/05 | Exact reviewed order/names/labels/units/dependent flags; UUID valid, metadata is encoded text with config and latency policy independently decoded and compared to declared fixture; no arbitrary object serialization. |
| TC05 persistence | AC06 | Save to fresh temporary HDF5, disconnect, independently open using new backend; identity, declared metadata and ordered column definitions/units/data equal originals at 1e-10. Assert connect/save/load/disconnect results and backend error status. Never equate no exception with success. |
| TC06 plot | AC07 | Spy captures reopened record supplied to existing plot utility; line x/y arrays equal reopened simulation-time/temperature columns, axes identify s/degC; PNG decodes with nonzero dimensions/content; figure closed. No screenshot/manual UI required. |
| TC07 collision | AC08 | Existing HDF5/PNG/sentinel/nonempty directory and file-as-directory refused before steps; sentinel bytes/hash and existing outputs unchanged. New and empty directory accepted. |
| TC08 acquisition failures | AC09/10 | Inject evolve failure at early and later tick: no failed-boundary publication or fabricated row, no retry, no save/plot, visible unsuccessful result. Inject measurement failure after step: final boundary advanced by one relative to rows, no rollback/retry. Inject prepare failure: no steps/completed outputs. |
| TC09 persistence failures | AC06/09/10 | Inject connect=False, save=False, reopen=False, load=None, corrupted/mismatched reload identity/metadata/schema/data and raised operation errors; each causes unsuccessful command and no completion message. Existing paths untouched; partial file diagnostics explicitly incomplete; plot not attempted on invalid reload. |
| TC10 plotting/cleanup failures | AC09/10 | Inject plot call and savefig failures, including after figure creation: overall failure while valid persisted-data path identified separately. Inject disconnect/stop/figure-close failures: cleanup errors visible alongside primary error; remaining cleanup attempted; no false success. |
| TC11 ownership | AC10; NFR01 | On success and each injected failure owned scheduler inactive, owned backend disconnected, owned figures closed; no cleanup stepping/reset. Capture final coordinator boundary before disposal. Preexisting global scheduler/borrowed figure unchanged, no global singleton replacement/stop. No reusable borrowed backend interface required. Respect scheduler event-loop ownership: never close a borrowed loop. |
| TC12 repeat | AC11 | Two fresh in-process runs and two documented CLI invocations: equal finite traces within tolerance, distinct UUIDs, exactly eight rows each, both files independently reopen; no manual global reset or previous rows. |
| TC13 docs/scope | AC12; NFR01/02/04/05 | Independent documented setup/command walkthrough; links resolve; explains model, five layers, one-boundary delay, acquisition/source time, outputs, error/partial-file handling and W01 limits. Diff preserves legacy imports/hooks/codecs/count/scan/storage; no new dependencies/Python change/sleeps/hardware/UI/public API. English annotations/docstrings and generated-file exclusion reviewed. |

## Execution gates and failure ownership

For baseline and implementation record actual environment, command, duration,
exit, warnings/skips. Run `.venv/bin/python -m unittest discover -s tests -p
 'test_*.py'`, compileall for softlab plus new example/test paths, import/version,
new test module, and documented CLI under Agg with writable temporary caches.
Repeat named checks with `-W error` to expose Python warnings. No unconfigured
lint/static-analysis zero-warning claim. Existing baseline warnings/debt remain
tracked, never silently waived; no unrelated fixes.

Run `git diff --check` and scope/status checks; generated outputs stay temporary.
Candidate CI Python 3.9/3.13 is required before dev integration; local 3.13 alone
cannot certify 3.9 or CI. Tests/failure closure belong to Tester, code review to
Designer. No hardware, cancellation/resume, durable partial recovery, UI/Figma or
package publication checks apply. Any prerequisite production change returns to
Architect/PO scope review. Future execution results are **PENDING**.
