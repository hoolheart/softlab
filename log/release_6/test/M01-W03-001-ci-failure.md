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

## Expanded source evidence before Architect review

After saving the initial report, fetched only versioned source text using GitHub
contents API (no installation/dependency/test edits): Matplotlib v3.9.4
[_fontconfig_pattern.py](https://github.com/matplotlib/matplotlib/blob/v3.9.4/lib/matplotlib/_fontconfig_pattern.py)
and [_mathtext.py](https://github.com/matplotlib/matplotlib/blob/v3.9.4/lib/matplotlib/_mathtext.py).
Executed the isolated versioned font parser with actual installed pyparsing3.3.3
under warnings capture and four normal font patterns: observed exactly these
three PyparsingDeprecationWarning messages attributed to that font module:

- `'oneOf' deprecated - use 'one_of'` (source line64).
- `'parseString' deprecated - use 'parse_string'` (line85).
- `'resetCache' deprecated - use 'reset_cache'` (line89).

The versioned _mathtext module executes ParserElement.enablePackrat at import
(line45). An AST-extracted execution of that exact call against pyparsing3.3.3
observed `'enablePackrat' deprecated - use 'enable_packrat'`, attributed to
matplotlib._mathtext. Its later parser construction/rendering also contains
oneOf/setName/setParseAction/leaveWhitespace and other deprecated aliases, but
those were not exercised by this import diagnostic. Do not approve wildcard
allowances for those later paths. The tested child mode is inspection, not plot
rendering. Actual exact-stack child verification remains required after approval.

Therefore oneOf-only handling is demonstrably insufficient. Proposed Architect
review scope is test-child-only action-default rules with exact category and
anchored message/module pairs for the three observed font messages plus the
single observed _mathtext import enablePackrat message. All remain visible on
stderr. Unknown warnings and every project/unrelated warning stay fatal. No
handling is implemented until approval. Local isolated parser diagnostics do not
claim the complete Matplotlib3.9 runtime was installed or tested.

## Independently confirmed prior warning baseline and unresolved consequence

`gh run view 39779059136 --log` now succeeded; read saved output from the
[accepted setup reconciliation run](https://github.com/hoolheart/softlab/actions/runs/39779059136).
It records the same Matplotlib3.9.4/pyparsing3.3.3/pandas2.3.3, 257-test runs and
409 occurrences of the exact DataFrame-concat FutureWarning across its full log,
versus650 occurrences in current failed candidate log. No PyparsingDeprecationWarning
is printed in that prior run. This confirms the concat warning predates W03
implementation, while the new child -W error policy newly exposes dependency
import deprecation as a failing check. Counts characterize logs, not distinct
underlying defects or permission to ignore them.

W03-CI-WARN-02 remains **OPEN for explicit baseline disposition**. Current HDF5
load_data_group_from_hdf5_group calls DataRecord.add_rows, whose concat path is
where pandas attributes the warning to softlab.shui.data.base. Thus after parser
import handling, the strict fresh child may next raise this baseline warning.
That later child path has not yet executed in the failed environment; source
inspection establishes a risk, not a reproduced second child failure. An exact
parser allowlist cannot be promised sufficient for green CI.

Preserving all project warnings as errors means **no pandas/project-module
suppression is proposed or silently approved here**. If exact-stack verification
reproduces this consequence, stop for Architect/PO/coordinator bounded prerequisite
or baseline-disposition decision; do not add a new warning filter opportunistically,
change dependencies or repair generic DataRecord/backend behavior without review.
Historical baseline disposition must be explicit and cannot become a blanket
zero-warning claim. Candidate failure remains OPEN until actual corrected checks
and approved scope/disposition evidence exist. No tests or runtime source changed.

## Separate conditional baseline disposition proposal for Architect and PO

Principle8 permits explicitly tracked baseline disposition; it does not permit
silent suppression. If an actual fresh-process run after the four parser rules
reproduces W03-CI-WARN-02, Architect and PO may review a separate child-only
**visible action-default exception** for this one demonstrated preexisting case:
category exact FutureWarning, anchored module `^softlab\.shui\.data\.base$`,
message exactly the following (use re.escape plus start/end anchors, no prefix or
wildcard matching):

`The behavior of DataFrame concatenation with empty or all-NA entries is deprecated. In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes. To retain the old behavior, exclude the relevant entries before the concat operation.`

The accepted setup/current logs both attribute it to base.py314. Approval would
be a bounded baseline-warning disposition, **not a zero-warning claim or product
fix**; the tracked warning/debt remains open. Keep the warning visible on child
stderr, preserve clean JSON stdout and actual typed/numeric/legacy/version checks.
Keep -W error for every other/new project or dependency warning. Require negative
checks for changed message/category/module so the exception cannot expand. No
parent-suite filter, production filter, dependency/config change or generic
DataRecord repair is proposed. No exception is installed merely because it might
be needed: first reproduce its actual blocking child consequence, then obtain
explicit Architect/PO/coordinator disposition before implementation. This
conditional option supplements, rather than silently overrides, the preceding
no-suppression/default-blocking position. Both CI failure and disposition issues
remain OPEN; no tests have been edited.

## Approved four-rule child harness implementation and local checks

Tester: sw-mike. 2026-10-22 Asia/Shanghai. Architect08e7e49,
PO338a9bf and coordinator approved exactly the four parser rules. Implemented
committed24e9f6b in tests/thermal_cli_bootstrap.py and
 tests/test_thermal_cli_warnings.py; only the manifest test's inspect_cli launch
now includes that bootstrap. No product/dependency/CI configuration change.
The helper retains -W error, installs error as baseline plus only four escaped,
anchored message/module and PyparsingDeprecationWarning action-default rules,
then preserves real example CLI argv under runpy.run_path in a fresh interpreter.
Known warnings remain visible on stderr; stdout JSON/typed/numeric/corrupt/version
assertions are unchanged. Importing the helper alone does not change parent policy.
No pandas rule or broad category/module allowance exists.

Selectivity assertions independently exercise all four permitted visible stderr
warnings and reject changed message/category/module, same-category project warning,
ordinary project warning and project-attributed pandas-category FutureWarning.
No synthetic warning injection is used as a replacement for real CLI assertions.

Actual repository-root commands, .venv/bin/python3.13.15 and prior unchanged
version inventory; MPLBACKEND=Agg, MPLCONFIGDIR=/tmp/softlab-w03-mpl,
XDG_CACHE_HOME=/tmp/softlab-w03-cache, each with -W error:

| Command after runtime prefix | Result |
| --- | --- |
| `-m unittest discover -s tests -p test_thermal_cli_warnings.py -v` | Two methods,0.001s,OK; repeated on committed24e9f6b with same result. |
| `-m unittest discover -s tests -p test_thermal_manifest.py -v` | Thirteen methods,11.518s,OK; actual fresh children and all present-invalid/legacy/version assertions pass locally. |
| `-m unittest discover -s tests -p 'test_*.py'` | 275 tests,32.680s,OK, no skips; same source bytes committed24e9f6b. |
| `-m compileall -q softlab examples tests` | Exit0; no diagnostics. |
| `-c 'import softlab; print(softlab.__version__)'` | Exit0;0.3.0. |
| `git diff --check` | Exit0, no whitespace diagnostics. |

No Python warnings or unexpected diagnostics in these named local3.13 checks;
permitted-warning selectivity probes deliberately capture their visible stderr.
This is neither zero-warning certification for3.9 nor dependency-debt closure.
Only three test/harness paths changed in source commit24e9f6b; no manifest
assertion was weakened and all preexisting contracts remain covered.

**W03-CI-01 and W03-CI-WARN-02 remain OPEN; integration remains BLOCKED.**
Designer must review this committed harness before exact candidate3.9/3.13 CI.
No local3.9 execution or corrected remote run is inferred. The known pandas child
risk has not been reproduced after these parser rules and no exception is added.
If it occurs, save actual trace and stop for separate disposition as required.
Prior local273 PASS remains historical; this275 includes two new policy tests.
Completed record/board update commit/push immediately as docs(log) before handoff.

## Actual parser-retry CI and reproduced pandas child failure

Tester: sw-mike. 2026-10-22 Asia/Shanghai.
Retry candidate1c0406c8ffac8018b173cf59e8a2569bc69d4bbb,
[run39790520996](https://github.com/hoolheart/softlab/actions/runs/39790520996).
Queried `gh run view 39790520996 --json headSha,jobs` and read
/tmp/w03-ci-1c0406c-py39.log. Exact candidate confirmed.
[Python3.9 job](https://github.com/hoolheart/softlab/actions/runs/39790520996/job/119865030335)
fails regression:275 tests,21.595s,two failures; compile/import remain skipped.
[Python3.13 job](https://github.com/hoolheart/softlab/actions/runs/39790520996/job/119865029975)
passes. Integration remains BLOCKED; these results do not close either issue.

All four approved parser messages are now emitted visibly without stopping the
child import; classic.mplstyle also prints its parseString/resetCache diagnostics.
Both previous valid-context/legacy child failures now reach actual HDF5 inspection
and exit1 with the exact diagnostic:
`Thermal experiment failed: HDF5 load failed: Failed to load group: FutureWarning(...)`.
The FutureWarning text exactly equals the full concat message in the conditional
baseline proposal above, not a new or partially matched warning. Existing
HDF5 load catches the warning-as-exception from DataRecord.add_rows/concat and
records a failed load; inspect then reports it. This is now an actual reproduced
child consequence, not merely the earlier static risk.

Compared exact strings with accepted setup log39779059136:409 occurrences at
softlab/shui/data/base.py314 there; retry has650 normal base.py314 warnings plus
two matching warning-as-exception child diagnostics (652 exact message copies).
Recorded stack remains Python3.9.25/pandas2.3.3, Matplotlib3.9.4/pyparsing3.3.3.
Thus category FutureWarning/message/project attribution match the demonstrated
preexisting baseline; no actual sample/manifest mismatch is shown by these exits.
The failed fresh-process acceptance assertions still remain unresolved.

**W03-CI-WARN-02 explicit disposition proposal is now ready for Architect/PO
review; not implemented.** Permit only a separate test-child action-default rule
for exact FutureWarning category, anchored `^softlab\.shui\.data\.base$` and the
full escaped/anchored concat message already quoted above. Keep it visible on
stderr, retain -W error for every other/new project/dependency warning, retain
real fresh CLI and all typed/numeric/invalid-content assertions, and add negative
selectivity checks for altered message/category/module. No ignore, wildcard,
parent/production filter, dependency/configuration change or library repair.
This is bounded baseline disposition rather than warning-free execution or a
fix of DataRecord debt. W03-CI-01/W03-CI-WARN-02 remain OPEN until approvals,
reviewed test change and actual corrected exact-candidate CI. No tests/source/
dependencies/configuration edited during this retry investigation. Immediate
docs(log) commit/push precedes handoff; no further speculative filter added.
