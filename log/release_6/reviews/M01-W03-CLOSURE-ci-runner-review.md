# W03 closure runner configuration independent review

Reviewer: sw-celeste (Designer). Date: 2026-10-24 Asia/Shanghai.
Subject630dce6, configuration fixcf28530 on codex/m01-w03-release-closure.
Verdict: **APPROVED for the authorized one-line runner change**.
Issue IDs: **none**.

Inspected committed .github/workflows/ci.yml diff and full resulting workflow
against Architect9ef6e7d, PO18bb7be2 and coordinator's bounded approval.
The only changed workflow line replaces compatibility.runs-on ubuntu-latest with
ubuntu-24.04. Python3.9/3.13 matrix, fail-fast, actions/versions, triggers,
permissions, cache/environment configuration, dependency install and all
VISA/regression/compile/import commands remain byte-for-byte unchanged. No
runtime/source/test/dependency/support-range change accompanies this fix.

[Tester platform evidence](../test/M01-W03-CLOSURE-ci-platform.md) records actual
Python3.9 setup failure on Ubuntu26.04 before skipped runtime checks, independent
Python3.13 success, prior24.04 execution and inspected official Python artifact
availability. That evidence supports selecting the reviewed24.04 runner; it is
not proof that the new candidate has run. Architect/PO scopes preserve all checks
and explicitly reject indefinite platform-availability or warning-free claims.
This review relies on saved owned evidence and configuration inspection; no new
remote query, workflow/configuration edit, runtime/test execution or failure
closure by Reviewer.

Tester actual exact-candidate CI validation is next. Platform failure remains
**OPEN** until both actual configured Python3.9/3.13 jobs succeed and Tester closes
it. Prior PO product acceptance and historical successful code tests do not waive
this gate. Existing parser/pandas warning debt and child-only visible dispositions,
R4/OBS/DEFECT debt and platform notices remain unchanged. No dev integration or
main promotion is certified here. Report/board commit/push immediately as docs(log),
with exact resulting hash/upstream equality supplied separately.
