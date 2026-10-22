# M01-W03-001 Product Owner CI scope disposition

Product Owner: sw-camille. Date: 2026-10-22 Asia/Shanghai.
Reviewed Tester investigation48ec06b and Architect disposition08e7e49 on
codex/m01-w03-stored-context. Verdict: **APPROVED bounded parser test-child
scope; integration remains BLOCKED pending corrected evidence**.

## Authorized bounded scope

The [investigation](../test/M01-W03-001-ci-failure.md) establishes that exact
candidatecbb72fb fails Python3.9 before the new inspection CLI reaches manifest
logic; Python3.13 passes. The [Architect decision](M01-W03-001-ci-disposition.md)
reviews concrete versioned-source and isolated parser evidence. PO accepts the
bounded test-harness adjustment as necessary to assess the authorized W03
fresh-process requirement without silently changing product scope.

Permit only the four exact escaped/anchored message/category/module pairs in
that Architect decision: oneOf, parseString and resetCache deprecations from
matplotlib._fontconfig_pattern, and enablePackrat from matplotlib._mathtext,
all exact PyparsingDeprecationWarning. Use action default so diagnostics remain
visible on child stderr; never ignore them. Keep -W error for every other
warning, including every project warning. Preserve real fresh-process execution,
CLI arguments, clean JSON stdout and typed/numeric/invalid-content assertions.
No parent-suite/production filter, wildcard expansion, dependency pin/upgrade,
CI configuration or library/backend repair is authorized.

This accepts a narrowly documented warning-policy disposition, not a production
fix or warning-free execution claim. The four dependency deprecations remain
recorded debt. Unknown or additional warnings remain fatal and require fresh
scope review; do not expand the list simply to obtain green CI.

## Separate pandas baseline consequence remains conditional

Prior accepted setup evidence establishes the DataFrame-concat FutureWarning
predates W03 implementation. Preserve W03-CI-WARN-02 as tracked baseline debt;
its possible new child-blocking consequence has not yet been reproduced.
**No pandas exception is authorized for installation by this decision.**

First reproduce the actual child failure after the four approved parser rules
on the relevant stack, recording exact message/category/module and failed exit
or assertion. Then obtain Architect matching-evidence confirmation and explicit
PO/coordinator disposition before any further change. The separately proposed
exact visible child-only option is eligible for that later review, not approved
speculatively. A different case requires new review. No generic DataRecord repair,
project-wide filtering, silent suppression or dependency change is authorized.

## Acceptance and remaining gates

All PRD AC-01–12 remain unchanged. In particular, valid fresh-process typed
recovery and legacy/corrupt/version dispositions require actual execution after
the harness adjustment; an invalid child exiting during import is not evidence
that metadata validation worked. Require visible/selective-warning assertions,
negative checks for other messages/categories/modules, Designer review of changed
tests, and Tester-owned verification/failure closure. Exact corrected candidate
Python3.9 and3.13 regression/compile/import CI must pass before dev integration.
Skipped failed-job steps remain unverified until executed successfully.

W03-CI-01 and W03-CI-WARN-02 remain OPEN. Existing R4/OBS/DEFECT debt and CI
platform notices retain their separate dispositions. Report actual warning counts,
environment and limits; do not label checks with visible exceptions zero-warning.
No technical finding is closed, no runtime check is performed and no release
acceptance is issued by this PO scope decision. Requirements/debt disposition
only; no code, tests, design or dependency changes by PO. Coordinator retains
the implementation/integration gate. Commit/push this record immediately with
docs(log) before handoff to the user-trusted origin.
