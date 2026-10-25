# M01-W03 accepted release integration

Integrator: sw-tom. 2026-10-24 Asia/Shanghai.
State: **CLOSED — accepted, promoted and both-version CI verified**.
Definitive current evidence: [closure.md](closure.md), recorded2026-10-25.
Final dev `055959c7f38fad082098159383d737b0a55ca39e`, promoted main
`bc3226f5031ec42029c9e444281d8b53eeaf802b`; both exact CI runs passed.
Only this post-release documentation branch's integration checks remain separate.

## Historical integration chronology — 2026-10-24

The entries below preserve phase-time decisions and pending/failed states.
They are historical and superseded for current release status by closure.md.


PO acceptance0e0986fd PASS maps AC01–12 to committed independent evidence.
Coordinator all11 readiness, actual architecture, Designer closure approval and
Tester final checks precede this handoff. Accepted runtime/test/guide/config
baseline is dev20fa9a0e179d62dbbfc765335d0d03de188f1dd8; closure changes remain
architecture/log/plan only. This integrator does not close technical findings or
perform new runtime tests. Scoped warning dispositions/debt remain visible/open;
no zero-system-warning or broad reproducibility claim.

Sequence: exact final candidate both3.9/3.13 success → dev merge → Done completion
records candidate CI/reconciliation → finaldev both CI → accepted no-ff promotion
to main → exactmain both CI. Hashes unavailable at candidate creation are reported
in final handoff, without fabricated circular records or recursive commits.
Historical GitHub run/job/check status lag is preserved in release evidence;
explicit exact-head both-version success is required before each integration.
No source/test/config changes, branch deletion, version/tag or publication.

## Closure candidate platform failure — 2026-10-24

Candidate399df915583ee156d2de6ba713aabbae3440a78c:
[CI40106490821](https://github.com/hoolheart/softlab/actions/runs/40106490821)
completed/failure. Python3.9 job120909146681 completed/failure at setup-python@v5:
Python3.9 x64 unavailable for Ubuntu26.04. Actual runner image ubuntu-26.04,
image20261012.163.1, provisioner20261021.618. Dependency installation, environment,
VISA requirement, regression, compile and import checks were skipped for3.9.
Python3.13 job120909146822 explicitly completed/success, including configured
regression/compile/import checks. Watch exited1. Exact log retained at
/tmp/w03-closure-ci399df91-py39.log and remote job URL. Existing platform notices
remain visible. This is a CI environment prerequisite, not a runtime acceptance
reversal or permission to waive the both-version gate.

No dev/main integration: dev20fa9a0/main9656b27 remain unchanged. Tester investigates
platform evidence/proposal before Architect/PO narrow runner disposition. Integrator
makes no workflow/dependency/source/test changes or failure closure; promotion held.

## Approved runner prerequisite implementation — 2026-10-24

Architect9ef6e7d/PO18bb7be2/Coordinator authorized exactly one workflow line
following Tester reportd50c698b. Commitcf28530 pushed:
compatibility.runs-on ubuntu-latest → ubuntu-24.04. Structural Python assertion
reversed that line and established every other workflow byte unchanged; git diff
--numstat is1 insertion/1 deletion, both matrix versions3.9/3.13 retained.
Git diff --check exit0. No fake production tests for configuration-only change;
no runtime/tests/dependencies/actions/support/permissions/triggers/check changes.

Designer config review then Tester remote verification/owned failure closure are
required before integration readiness. Automatic push CI may run; integrator does
not retry/merge or mark platform failure closed. Runtime PO acceptance unchanged;
platform gate OPEN. This evidence/board handoff commits/pushes immediately.

## Renewed final release readiness — 2026-10-24

Coordinator all11/formal readiness PASS at3397a743 follows independent Designer
runner reviewdcab463 and Tester-owned platform failure closure. Exactdcab463
[CI40107138176](https://github.com/hoolheart/softlab/actions/runs/40107138176)
explicitly passes both Python3.9.25/3.13.16 setup,275 tests,compile/import on
Ubuntu24.04. Missing-runner blocker is resolved, not waived. Runtime PO acceptance
0e0986fd AC01–12 remains applicable. Approved sole workflow change is runs-on
ubuntu-24.04; accepted runtime/tests/guide remain identical to dev20fa9a0.
Historical failed candidates, scoped warning dispositions/debt and API lag remain
recorded, superseded for readiness by actual successful owner-verified evidence.
Final exact candidate CI still precedes dev integration and accepted promotion.

## Accepted closure merge and completion handoff

Exact candidateec198da4d63ccb57246a8b25959249f0df72f1e7 passed
[CI40107483677](https://github.com/hoolheart/softlab/actions/runs/40107483677),
overall and both3.9/3.13 explicitly completed/success before dev no-ff merge
 a0b35539984400f850d6c46c4cbf5d8c294d40e6, pushed. Accepted runtime/tests/guide/
pyproject unchanged from20fa9a0; approved workflow-only exception remains the
single Ubuntu24.04 runner line. All release tasks Done; no next task started.
This completion candidate must pass CI before final dev reconciliation/checks,
then authorized main promotion/checks. Final exact hashes/URLs are handoff evidence
without recursive document commits. W04 and full M01 remain unaccepted; no tag,
package publication or branch deletion. Historical warnings/debt/status lag retained.
