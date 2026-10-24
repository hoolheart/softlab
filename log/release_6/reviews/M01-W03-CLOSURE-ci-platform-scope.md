# W03 closure CI runner prerequisite — Product Owner scope decision

Product Owner: sw-camille. Date: 2026-10-24 Asia/Shanghai.
Reviewed Tester reportd50c698b and Architect review9ef6e7d.
Verdict: **APPROVED bounded one-line runner environment fix; CI gate OPEN**.

The [Tester evidence](../test/M01-W03-CLOSURE-ci-platform.md) establishes exact
candidate399df915 CI40106490821 failed Python3.9 setup on Ubuntu26.04 before
runtime checks. Those skipped checks remain unverified. Python3.13 passed275
and compile/import; it cannot substitute for3.9. The inspected official artifact
manifest lists3.9 x64 support for24.04 and none for26.04 at inspection.
[Architect review](M01-W03-CLOSURE-ci-platform-review.md) approves selecting the
previously demonstrated24.04 runner family. PO relies on owned evidence, without
new remote queries, technical execution or failure closure.

Authorize changing only compatibility.runs-on in .github/workflows/ci.yml from
ubuntu-latest to ubuntu-24.04. Preserve Python3.9/3.13, supported-Python range,
fail-fast behavior, actions, permissions, triggers, caches, dependency commands
and every environment/VISA/regression/compile/import check unchanged. No dropping
checks, dependency/action upgrades, new warning filters, production changes or
broader CI refactor. This is a bounded CI environment prerequisite, not a claim
about all Ubuntu releases or indefinite upstream artifact availability.

PRD AC01–12 and existing runtime/documentation product acceptance remain unchanged
while product scope/bytes remain unchanged. That acceptance does not waive this
failed gate or authorize promotion before corrected evidence. Same sole closure
task continues; no new concurrent work. Existing parser/pandas warning debt and
exact visible child-only dispositions, R4/OBS/DEFECT debt and platform notices
remain explicit/open, with no zero-warning CI or library-repair claim.

Coordinator gate precedes Developer's minimal configuration edit; independent
Designer review/configuration validation and exact replacement-candidate both
Python3.9/3.13 setup/dependency/VISA/regression/compile/import success are required
before Tester-owned platform failure closure and coordinator integration/promotion.
Record actual runner/Python/warnings/skips and primary exact-head results. Unknown
new failures block affected gates; no speculative widening or production TDD is
required for this configuration-only edit. Final closure dev/main checks remain
pending, not certified here. PO changed requirements/scope evidence only, with no
configuration, code, tests, execution or technical issue closure. Commit/push
immediately with docs(log) to the user-trusted origin before handoff.
