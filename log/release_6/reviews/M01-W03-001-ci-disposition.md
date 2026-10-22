# M01-W03-001 Architect CI scope and warning disposition

Architect: sw-jerry. 2026-10-22 Asia/Shanghai. Subject evidence48ec06b,
[test investigation](../test/M01-W03-001-ci-failure.md), failed candidate
cbb72fbe368dfee5cc2c9321ff046b7bbea729b5 on codex/m01-w03-stored-context.
Verdict: **APPROVED bounded parser test-harness plan; PO/coordinator scope
approval required before implementation. Integration remains BLOCKED.**
No tests/source/configuration/dependencies changed or executed by Architect.
This assesses scope/minimality/baseline disposition, not code review or failure
closure. Tester evidence is relied upon without claiming independent reruns.

## Parser compatibility — bounded approval

Exact [CI39786554366](https://github.com/hoolheart/softlab/actions/runs/39786554366)
failed Python3.9 regression before inspection logic, while Python3.13 passed.
The failed3.9 compile/import steps were skipped and remain unverified. Versioned
Matplotlib3.9.4 sources plus Tester's isolated actual pyparsing3.3.3 diagnostics
establish four concrete dependency-deprecation cases. OneOf-only handling is
insufficient. Approve test-child bootstrap action='default', retaining visible
stderr, for exactly these anchored message/module pairs and exact category
pyparsing.warnings.PyparsingDeprecationWarning:

| Exact message (full string) | Anchored module |
| --- | --- |
| 'oneOf' deprecated - use 'one_of' | ^matplotlib\._fontconfig_pattern$ |
| 'parseString' deprecated - use 'parse_string' | ^matplotlib\._fontconfig_pattern$ |
| 'resetCache' deprecated - use 'reset_cache' | ^matplotlib\._fontconfig_pattern$ |
| 'enablePackrat' deprecated - use 'enable_packrat' | ^matplotlib\._mathtext$ |

Escape and anchor each full message, with no wildcard expansion. Keep -W error,
actual fresh-process runpy example/CLI argv, JSON stdout and all typed/numeric/
invalid-content assertions. Parent suite, production warning policy and project
source remain unchanged. No generic DeprecationWarning/Pyparsing category-wide,
Matplotlib module-wide, rendering alias or unknown warning allowance. Warning
category import/bootstrap must itself retain error policy for other warnings.
Exact dependency-stack verification remains necessary: isolated parser evidence
alone does not certify full Matplotlib import or complete child execution.

This adjusts only the new child's stricter treatment of specifically evidenced
dependency warnings. It does not fix those dependencies or establish zero warnings.
Unknown failures remain blocking. Re-run corrupt/version children too: prior
exit1 could be an import failure, not successful validation evidence. Designer
must review the changed harness; Tester owns actual results and failure closure.

## Pandas W03-CI-WARN-02 — separate conditional decision

Prior accepted [setup CI39779059136](https://github.com/hoolheart/softlab/actions/runs/39779059136)
shows the exact DataFrame empty/all-NA concatenation FutureWarning before W03
implementation (409 full-log occurrences versus650 current-log occurrences).
This establishes baseline presence, not permission to hide a new blocking child
failure or certify a warning-free check. It originates from the current unchanged
DataRecord concat path; no broad backend/DataRecord repair is warranted here.

Architect finds the proposed narrowly visible child-only action-default option
technically minimal under principle8, **conditionally eligible, not yet authorized
for installation**. First obtain actual child reproduction after the four parser
rules on the relevant dependency stack. Record exact warning/category/module and
failed assertion/exit. Then obtain explicit PO/coordinator disposition plus
Architect confirmation that reproduced evidence matches this bounded option.
Do not add the pandas exception merely because source inspection predicts it.

Only that subsequent matching decision may permit exact FutureWarning, module
^softlab\.shui\.data\.base$, and the complete exact concat message quoted in
the investigation (re.escape plus start/end anchors). Keep stderr visible and
-W error for every other message/category/module, including other project
warnings. No parent-suite/production filters or ignore action; no dependency/
CI configuration change. Require negative checks for changed message/category/
module, visible-warning assertion and unchanged typed/numeric assertions. If the
actual case differs, STOP for new review rather than broaden matching.

## Principles, evidence and exit gate

Principles7/8/11: failed checks block integration; baseline debt is explicit and
tracked rather than silently waived. W03-CI-01 remains **OPEN**; W03-CI-WARN-02
remains **OPEN**, with pandas child consequence unverified. Parser deprecations
remain dependency debt and pandas remains baseline debt even after an approved
harness treatment. Local -W error checks with bounded visible exceptions must
report them explicitly and must not be described as zero-warning. Existing CI
platform notices and R4/OBS/DEFECT debt retain separate recorded dispositions.

Principles1/2/9/10: no library/API/storage/numeric behavior change, no instrument
access, no dependency upgrade/pin or general warning framework. Tests retain
real fresh-process behavior and synthetic owned outputs. Principle3/5: PO scope
review follows this Architect decision; Tester may implement the approved test
harness only after both approvals/coordinator gate, then Designer independent
review and Tester verification. Owner-only issue closure remains in force.
Principles4/6: same sole active task, immediate docs(log) commit/push, no dev merge.

Required completion evidence: bounded-filter selectivity/visible diagnostics,
actual valid and invalid child paths, appropriate focused/full/compile/import
checks, actual warning/environment/skip limits, Designer approval and corrected
exact-candidate Python3.9 AND3.13 CI regression/compile/import success. No earlier
candidate or one-version pass substitutes. Only Tester can close failures after
that evidence; coordinator gates integration. No PO acceptance, successful repair
or exact-stack pass is certified by this scope review.
