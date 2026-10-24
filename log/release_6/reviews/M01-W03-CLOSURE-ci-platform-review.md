# W03 closure CI platform prerequisite — Architect review

Architect: sw-jerry. 2026-10-24 Asia/Shanghai. Subject reportd50c698 and
[test evidence](../test/M01-W03-CLOSURE-ci-platform.md), exact candidate
399df915583ee156d2de6ba713aabbae3440a78c. Verdict: **APPROVED bounded one-line
runner pin, subject to PO/coordinator scope approval before Developer edit**.
Platform failure remains OPEN; no integration or promotion waiver.

Read actual .github/workflows/ci.yml, project principles and Tester-owned primary
job/official artifact findings. Python3.9 job120909146681 on
[CI40106490821](https://github.com/hoolheart/softlab/actions/runs/40106490821)
failed setup-python on Ubuntu26.04 before dependency/runtime checks; those steps
were skipped, not passed. Python3.13 job succeeded275 tests/compile/import but
cannot replace the missing matrix entry. Prior accepted W03 execution used24.04;
Tester's inspected official Python artifact manifest lists3.9 Linux x64 for24.04
and none for26.04 at inspection. This establishes a platform prerequisite, not a
manifest or production regression. Architect relies on owned evidence; no new
remote query or runtime execution is claimed.

Approve replacing only compatibility.runs-on ubuntu-latest with ubuntu-24.04.
Preserve Python3.9/3.13 entries, fail-fast policy, actions, permissions, triggers,
caches, dependencies and all VISA/regression/compile/import commands unchanged.
This selects a previously demonstrated runner family with listed setup artifacts;
it does not change supported Python range, pin/upgrade dependencies, certify all
Ubuntu releases or establish indefinite upstream availability. No production/
backend repair, action upgrade, warning filter or broader CI refactor is included.

Principles7/8/11 require actual named environment evidence and explicit baseline
limits: this is a bounded environment fix, not implicit success. Principles1/2/10
remain satisfied by unchanged library/API/behavior/dependencies; principles3/5
require PO scope decision, Developer minimal configuration edit, independent
Designer review and Tester actual verification. Same sole closure task remains
active under principle4; no extra concurrent work. Principle9 retains synthetic
CI fixtures; principle6 requires immediate committed/pushed records.

Next: PO/coordinator approval → Developer one-line diff and process record →
independent Designer review/configuration validation → exact replacement-candidate
both Python3.9/3.13 setup/dependency/VISA/regression/compile/import success →
Tester-owned platform failure closure and coordinator integration gate. Document
actual runner/Python versions, warnings/skips and exact hashes/jobs. A new setup/
runtime failure blocks its affected gate; no repeated speculative widening.
Documentation/configuration checks apply to the edit; do not invent production
TDD or claim local runtime results from remote logs.

Existing PO runtime/product acceptance remains its owned verdict, contingent on
unchanged product scope; this review does not reissue it or waive release CI.
Underlying parser/pandas debt and exact child-only visible-warning dispositions
remain unchanged/open. Node punycode/forced24 and runner notices remain disclosed;
no zero-warning CI claim. No tests/code/config edits or executions by Architect;
only this scope review is written. Formal commit/push/upstream verification is
required before handoff. Release integration/main promotion remain blocked until
actual corrected checks pass.
