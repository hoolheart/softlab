# Principle inspection

## Release start / TU-001 start — 2026-09-27

PASS for starting characterization. Reviewed principles.md and AGENTS.md.
The user has now authorized the previously proposed TU backlog. dev is clean
at db256b0; task branch exists and is pushed. TU-001 changes tests/documentation
only. Subsequent implementation is additive and follows the serial gates.
No new dependency, Python support change, physical instrument access, or main
promotion is authorized. Test fixtures must restore registries/defaults and
release simulated resources. Existing suspicious behavior must be documented
separately from intended compatibility guarantees. Task-completion and final
release verdicts remain pending actual evidence.

## TU-001 completion inspection

PASS for characterization scope: requirements recorded in release_1/prd.md;
developer accepted test implementability; architect approved the contract map;
independent reviewer requested and then confirmed two assertion corrections,
closing review issues in 1c2886e. Tester reran 22 tests, compile and import with
zero skips/warnings at b5599ff. Only tests, documentation and guidance changed;
no production implementation or hardware access. OBS-001–006 remain explicitly
open for later tasks. CI on the final task candidate is required before dev
integration. Next task cannot start until that integration is verified.

## TU-002 completion inspection

PASS for description implementation scope. Serial gates observed on
codex/tu-002-descriptions: test gate (c01d20b implementability approval,
19678cc expected-RED evidence), design gate (3f16452 proposed, 6d34670/0095fce
clarification round, dcb4a29 architect approval after corrections), development
(244e811, production confined to softlab/tu/), code review (b613f2a APPROVED,
zero issues — all review dimensions checked against design/TU-002.md), testing
gate (86a6063: 11/11 focused, 33/33 full suite, zero warnings under -W error,
compile and import smoke clean, tester independently reran everything).

Principle 1: test cases preceded design, design preceded implementation,
review and test reports exist and are committed/pushed. Principle 2: zero
review issues and zero test failures remain; nothing deferred. Principle 3:
no environment component was skipped — visa-sim notebooks out of scope as
recorded. Principle 4: N/A, no UI change. Principle 5: full suite green with
warnings-as-errors. Principle 6: every log/release_1 file above is committed
with docs(log): messages and pushed; no uncommitted artifacts.

No hardware accessed, no new runtime dependency, no Python support change,
no huo/mu production change. Integration candidate: branch tip must pass CI
before dev fast-forward; OBS-001–006 remain open for later tasks.

## TU-003 completion inspection

PASS for explicit-operations scope. Serial gates observed on
codex/tu-003-operations: test gate (RED evidence 1c6d97c, implementability
ACCEPTED ef10bed), design gate (0047a50 proposed, 14a7d9d architect APPROVED),
development (770bad6, +71 lines confined to softlab/tu/station/visa.py),
code review (9002b68 APPROVED, zero issues, arch observation discharged in
docstring), testing gate (beeb6b5 detachment case, ffc5f25 green evidence:
7/7 focused, 40/40 full suite under -W error, compile/import clean, tester
independent rerun).

Principle 1: tests preceded design, design preceded implementation, all
reports exist and are committed/pushed. Principle 2: zero open review or test
issues. Principle 3: no skipped environment component; mocked PyVISA only,
notebooks out of scope as recorded. Principle 4: N/A. Principle 5: warnings-as-
errors full suite green; two pre-existing parameter.py baseline warnings
(SyntaxWarning pair in __main__ example block, plus a warning at
Parameter.__init__ parameter.py:183 noted by the code reviewer) recorded as
baseline defects, not silently exempted. Principle 6: every log/release_1
artifact committed with docs(log): messages and pushed.

No hardware, no new dependency, no huo/mu change. Integration candidate must
pass CI before dev fast-forward.

## TU-004 completion inspection

PASS for optional-lifecycle scope. Serial gates observed on
codex/tu-004-lifecycle: test gate (RED 692b422, implementability ACCEPTED
7c3f0bb), design gate (3a2ecd0 proposed, 4d3b5c5 CHANGES REQUESTED — one
blocking re-entry issue, 3417ec9 corrected, 898d471 re-review APPROVED),
development (a94465e, +186 lines, 0 deletions, confined to device.py), code
review (b1d8780 APPROVED, zero issues), testing gate (9e884fe recovery-cycle
case, a9589eb green evidence: 10/10 focused, 50/50 full suite under -W error,
compile/import clean, independent rerun).

Principle 1: tests preceded design, design preceded implementation; one design
correction round was requested by the architect and closed by the designer
before implementation — review authority exercised, not bypassed. Principle 2:
zero open review or test issues. Principle 3: no skipped environment component;
mocks only. Principle 4: N/A. Principle 5: warnings-as-errors full suite green;
pre-existing parameter.py baseline warnings unchanged and still recorded.
Principle 6: every log/release_1 artifact committed with docs(log): messages
and pushed.

No hardware, no new dependency, no VISA change (OBS-003 stays deferred to
TU-006). Integration candidate must pass CI before dev fast-forward.
