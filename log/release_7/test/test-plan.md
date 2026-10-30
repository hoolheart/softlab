# M01-W04 test plan

Status: PLANNED; test outcomes require a separate execution record. Scope: the
example-level adapter rehearsal and M01 acceptance in [PRD](../prd.md). All runs
use synthetic data and fresh temporary destinations; no physical or VISA test is
claimed. UI testing is not applicable.

## Characterization and acceptance checks

| ID | Observable assertion | PRD |
| --- | --- | --- |
| T01 | Run the same acquisition with both model-backed adapter choices. Assert ordered columns, labels, units, dependencies, point count, control values, and row order; exclude only wall timestamps from deterministic comparisons. | AC-01, AC-05 |
| T02 | Independently calculate expected temperatures from the documented discrete recurrence and each adapter's observation boundary, including initial and final points. Assert source ticks, simulation time and truthful recorded clock/delay fields; a wall-clock wait must not advance the model. | AC-02, AC-05 |
| T03 | Use recording control and measurement doubles to verify write/read sequence and count. Inject control and read failures at defined boundaries; assert original cause is visible, completed rows are retained, no fabricated rows or false completion, and owned scheduler, loop and storage resources are released. | AC-01, AC-05 |
| T04 | Round-trip the alternate result through HDF5 and the typed manifest loader. Verify declared adapter/timing context agrees with rows; unsupported or mismatched context is rejected visibly, while existing legacy/missing/versioned cases retain their prior behavior. | AC-02, AC-05 |
| T05 | Run the guide's single headless command from the repository root in a fresh output path. Verify exit status, persisted group and manifest, reload, nonempty plot and documented inspection. Record actual Python/dependencies and whether the environment was freshly created. | AC-03 |
| T06 | Inspect physical mapping for control/measurement operations, units, timing, lifecycle and the explicit distinction between simulated advancement and unverified physical settling. No hardware operation. | AC-04 |
| T07 | Execute existing W01 numerical/schema/persistence, W02 cancellation/repetition/outcome/cleanup, W03 manifest/legacy and relevant broader regression suites. Capture warnings, skips and baseline failures; investigate new or affected failures. | AC-05, AC-07 |
| T08 | Check the four milestone evidence links and backlog dispositions, then review guide/architecture consistency, diff hygiene and scope. Record applicable principle inspection and actual independent review and test issue closure. | AC-06, AC-07 |
| T09 | Record exact frozen integration-candidate CI and result before dev integration, product acceptance, promoted revision/tree and required promoted-state checks. A clean worktree and authoritative closure summary are required; CI reporting lag is not a pass. | AC-08 |

## Commands and reporting

Run the focused thermal tests and full suite with the repository Python:

```sh
.venv/bin/python -m unittest discover -s tests -p 'test_thermal*.py'
.venv/bin/python -m unittest discover -s tests -p 'test_*.py'
.venv/bin/python -m compileall -q softlab
.venv/bin/python -c 'import softlab; print(softlab.__version__)'
git diff --check
```

Record command, revision, environment, result, warnings, skips and any limits.
Run the final documented guide command separately; inspect its actual output.
Independent tester owns closure of test failures. Candidate CI, acceptance and
promotion are distinct gates and must not be inferred from local tests.

## Baseline characterization

The existing reference creates eight rows. Its numerical oracle is
`30 - 10 * 0.9**j` for source boundary `j = 0..7`; acquisition ticks are 1–8,
with previous-boundary source ticks 0–7. The accepted tests also check the ordered
six-column schema, output preservation, repeat isolation, cancellation and failure
outcomes, resource cleanup, typed manifest validation and legacy handling. Current
manifest validation fixes clock metadata to the previous-boundary policy; any
alternate timing needs an explicitly reviewed, truthful compatibility strategy.
This is an inspection finding, not evidence that W04 implementation passes.
