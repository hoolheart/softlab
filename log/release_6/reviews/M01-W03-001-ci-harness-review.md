# M01-W03-001 CI child harness independent review

Reviewer: sw-celeste (Designer). Date: 2026-10-22 Asia/Shanghai.
Subject94e6730, harness24e9f6b; branch codex/m01-w03-stored-context.
Verdict: **APPROVED for the approved test-only harness scope**.

Reviewed the three changed test files against Architect08e7e49, PO338a9bf and
coordinator authorization. No tests executed, production/test edits or dependency/
configuration changes by Reviewer. Local275 full/13 manifest/2 selectivity green
results are Tester-reported evidence; actual Python3.9/3.13 candidate CI remains
required before integration and no CI failure is closed by this review.

| Check | Finding |
| --- | --- |
| Warning scope | Four explicit message/module pairs only; regex strings use re.escape and start/end anchors. Category is PyparsingDeprecationWarning; module origins are the approved fontconfig/mathtext paths. Standard Python warning-filter category matching applies; no generic DeprecationWarning, pandas, project-wide or ignore rule added. |
| Visibility and strictness | Child installs error as the general policy and default only for approved dependency cases, so warnings remain visible on captured stderr rather than ignored. Other messages/categories/modules and project/pandas-like FutureWarning remain fatal under the general rule. |
| Process isolation | Only manifest inspection subprocess calls use the bootstrap. Parent test warning policy is unaffected; no production import path is filtered. New subprocess retains -W error, shifts argv to the real example path/arguments and executes that script through runpy with __main__. Fresh process and real CLI/schema/sample assertions remain active. |
| Selectivity assertions | Two test methods assert every approved warning's category/message is visible on stderr, and altered message/category/module plus project warnings raise. Each test uses catch_warnings so policy mutation is restored. |
| Minimality | Diff contains test bootstrap, selectivity tests and the inspection child invocation only. No production, pandas filters, dependencies or CI configuration changed. This is a bounded approved dependency-warning disposition, not blanket zero-warning certification. |

Issue IDs: **none**. Existing product code approval/CR-W03-01 closure is unchanged.
Designer harness review is complete; exact candidate CI is pending. Python3.9
failure from candidatecbb72fb/CI39786554366 remains OPEN until actual successful
candidate checks and Tester-owned closure. Board remains blocked for CI/integration;
this review does not certify the current SHA or any platform through static
inspection. Baseline debt/platform notices retain recorded dispositions.

Report and board are committed/pushed immediately with docs(log); resulting hash
and upstream equality supplied separately. No acceptance or dev merge authorized
by this report alone.

## Authorized pandas baseline extension — APPROVED

Reviewer: sw-celeste. Date: 2026-10-22 Asia/Shanghai.
Subject9070423, repairc8574bf. Verdict: **APPROVED for the expanded approved
child-only harness scope**. Architect91f024e/PO1fcc7371 and coordinator authorized
the exact reproduced pandas warning after the preceding CI attempt. This section
supersedes the earlier four-parser-only scope; it does not retroactively alter
that initial review's subject or disposition.

Inspected the added full pandas concatenation warning text, re.escape plus
start/end anchors, FutureWarning category and exact escaped module expression
^softlab\\.shui\\.data\\.base$. It uses default, preserving stderr visibility;
there is no generic pandas/FutureWarning/project ignore rule. The existing four
parser rules, error general policy, -W error subprocess invocation and real
runpy CLI dispatch remain intact. No production/dependency/configuration changes.

Selectivity source now asserts all five approved cases visible and rejects
changed pandas message/category/module and unrelated project origin, alongside
existing parser/project negatives. Parent policy restoration remains protected by
catch_warnings. Standard Python warning-filter semantics apply. No additional
review issues raised; original product CR-W03-01 closure remains unchanged.
Tester-reported275 full/13 manifest/2 selectivity local green checks are not
Reviewer execution or matrix evidence. Both CI issues remain OPEN until actual
successful configured Python3.9/3.13 results and Tester-owned closure. Exact
candidate CI is next; no dev integration or acceptance certified. This review and
board handoff are immediately committed/pushed with docs(log).
