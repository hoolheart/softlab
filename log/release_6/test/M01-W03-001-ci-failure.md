# M01-W03-001 Python 3.9 CI failure investigation and proposal

Tester: sw-mike. Date: 2026-10-22 Asia/Shanghai.
Candidate cbb72fbe368dfee5cc2c9321ff046b7bbea729b5.
Status: **OPEN — integration blocked; Architect disposition required**.
No test/source/dependency/configuration changes proposed here are implemented.
Developer/integrator must not merge until corrected exact-candidate CI passes.

## Exact remote evidence

Read `/tmp/w03-ci39-failure.log` and queried
`gh run view 39786554366 --json jobs,headSha,url,conclusion`.
[Candidate run](https://github.com/hoolheart/softlab/actions/runs/39786554366)
conclusion failure; returned headSha exactly matches candidate above.

- [Python 3.9 job](https://github.com/hoolheart/softlab/actions/runs/39786554366/job/119851193245):
  failure in Regression suite, 273 tests/16.584s, two failures. Compile sources
  and import smoke steps skipped after failure; they must not be marked passing.
- [Python 3.13 job](https://github.com/hoolheart/softlab/actions/runs/39786554366/job/119851193430):
  success, including regression/compile/import. This does not waive failed3.9.

Failed environment Ubuntu24.04, CPython3.9.25, Matplotlib3.9.4,
pyparsing3.3.3, pandas2.3.3, NumPy2.0.2, SciPy1.13.1, h5py3.14.0,
PyVISA1.14.1/pyvisa-sim0.6.0 (recorded job install/environment output).
The job executes normal unittest discovery; the new fresh CLI children explicitly
pass -W error. Both test_complete_closed_schema_and_typed_fresh_process and the
absent subcase of test_absent_legacy_is_distinct_from_corrupt_present_metadata
fail because their child cannot import Matplotlib, before reaching argparse or
any inspection/manifest logic:

`pyparsing.warnings.PyparsingDeprecationWarning: 'oneOf' deprecated - use 'one_of'`.

Trace: example import matplotlib → rcParams/default font parsing →
matplotlib._fontconfig_pattern._make_fontconfig_parser line64 calls oneOf →
pyparsing.util._inner line472 warns. This is a dependency interaction newly made
fatal by stricter child warning policy, not evidence that manifest semantics or
fresh-process inspection rejected valid data. It remains a new failing CI gate.
Other invalid-content child assertions expecting exit1 cannot alone prove content
validation in this environment when import itself exits1; rerun them after fixing
the harness disposition to prevent false-positive interpretation.

## Warning policy, baseline and initial diagnostic

Read principles7/8/11 and release_6 integration warning record: missing/failed
checks block integration; baseline warnings need tracked explicit disposition;
no silent waiver or unsupported zero-warning claim. The log also contains pandas
DataFrame empty/all-NA concat FutureWarning at softlab/shui/data/base.py314
(650 occurrences in this log). Integrator describes them as existing baseline;
this investigation has not yet independently compared prior setup log evidence.
No pandas suppression/repair or blanket baseline acceptance is authorized here.
Track this separately as W03-CI-WARN-02 pending Architect/coordinator disposition.
Prior runner Node20/forced24/Ubuntu26 annotations are distinct platform notices.

Local pyparsing3.3.3 source confirms replaced_by_pep8 warns with stacklevel2:
warning attribution belongs to matplotlib._fontconfig_pattern, not pyparsing.util.
A local scratch diagnostic using real pyparsing.oneOf under that module identity
proved the exact-message/category/module action-default rule below emits one
visible warning, while project UserWarning, same warning from a project module,
and another deprecation in the Matplotlib module remain fatal under error policy.
This proves filter selectivity only, not full Matplotlib3.9 import compatibility.
No environment/dependency changes or actual Python3.9 rerun were performed locally.

## Proposed bounded repair — not yet approved or implemented

W03-CI-01 stays OPEN. Prefer a test-only fresh-child bootstrap that preserves
-W error and installs explicit narrowly matched dependency warnings before
runpy executes the actual example with its real CLI argv. Keep fresh-process
schema/types/numeric assertions and clean stdout JSON unchanged. The observed
candidate rule is action='default' (visible stderr, not ignore), exact category
pyparsing.warnings.PyparsingDeprecationWarning, message
`^'oneOf' deprecated - use 'one_of'$`, module
`^matplotlib\._fontconfig_pattern$`. No filters for softlab/example warnings,
pandas or all DeprecationWarning/UserWarning categories. No removal of -W error,
production warning filter, dependency pin/upgrade or CI configuration change.

Before implementation, inspect the versioned Matplotlib3.9.4 font/mathtext parser
sources and real pyparsing wrapper behavior to enumerate any further known import
compatibility messages. Do not widen to a wildcard category/module or blindly
repeat CI until green. Each proposed additional exact message/module needs source
or actual isolated-environment evidence and Architect disposition; unknown/new
warnings remain blocking. Preserve warning diagnostics/counts in evidence.

Verification after scope approval: test-only bootstrap selectivity checks prove
known dependency warning still visible, all project/unrelated warnings fatal;
actual subprocess success plus legacy/corrupt/version and typed/numeric cases run.
Use exact dependency stack in an isolated temporary runtime if already available
or explicitly authorized; no changing project dependencies. Rerun local focused/
full checks as applicable and exact candidate Python3.9/3.13 CI. Only actual green
checks and agreed bounded baseline warning disposition can close Tester failures.
Designer review follows any changed test harness; Architect owns scope decision.

## Interruption and handoff

The first attempt to compare prior setup CI logs was rejected before execution
because automatic approval review hit an account usage limit; it explicitly did
not judge the action unsafe. No bypass was attempted. The user has now resumed;
this evidence/proposal is saved first as requested, with no claimed earlier report.
No test, production, dependency or CI configuration edit by Tester. Baseline
R4/OBS/DEFECT debt remains open. Board/integrator record remains integration
blocked; dev6d332ce unchanged as supplied by coordinator. Commit/push immediately
with docs(log), then Architect scope/disposition review before test edits.
