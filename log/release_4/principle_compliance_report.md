# Principle compliance — M01-W01 start

Delegated inspector: sw-jerry (Architect); coordinator owns formal gate review.
Date: 2026-10-18. Verdict: **PASS — start readiness only**.
Inputs: [principles](../../principles.md), [activated PRD](prd.md) `c776fe8`,
[architecture plan](architecture-plan.md), [tasks](tasks.md), [board](sprint-board.md).
All eleven current repository principles are inspected below. Completion and
release inspections remain PENDING. No future gate is certified by this verdict.

| Principle | Start evidence / disposition | Verdict |
| --- | --- | --- |
| 1 Preserve five-element architecture | Existing primitives retain responsibilities; example-local glue; planned architecture explicitly labeled; actual arch.md unchanged | PASS |
| 2 Characterize before changing public behavior | Public behavior changes excluded; touched callers and independent characterization required in test/design gates before code | PASS readiness; execution pending |
| 3 Documented TDD gates | tasks.md preserves test/developer review → design/architect review → code → independent review → tests → principles → CI/integration | PASS readiness; gates pending |
| 4 One active task | Clean setup observed on codex/m01-w01-thermal-example; one task from dev e034825; dev integration precedes release/main | PASS |
| 5 Role/review ownership | PO activated scope; Architect owns this setup; Tester next; Designer closes review issues, Tester closes failures; no code/tests written here; UI N/A | PASS |
| 6 Committed/pushed evidence | Activation c776fe8 pushed per formal handoff; this completed setup must be docs(log) committed/pushed before handoff; no uncommitted evidence counts as phase completion | PASS conditional on formal commit/push verification |
| 7 Relevant environment | Python 3.13.15 and required modules discoverable in .venv; gh/Conda present; full version/baseline/runtime and matrix evidence assigned to Tester, not inferred | PASS readiness; runtime pending |
| 8 Baseline defects/regressions | OBS-004/005/006, DEFECT-2 retained as tracked baseline; no waiver or unrelated fix; Tester records fresh warnings/skips/failures before claims | PASS readiness; execution pending |
| 9 Instruments/data safety | Synthetic local scalar model, fresh output collision refusal, temporary tests, explicit owned resources; no instruments authorized | PASS readiness; failure tests pending |
| 10 Minimal compatible extensions | No framework/dependency/Python/version/export changes; existing count/hooks and HDF5; prerequisite defects require reviewed scope decision | PASS |
| 11 Reproducible evidence | Explicit candidate CI before dev, PO release acceptance before main; regression/compile/import, independent walkthrough, diff/status and honest limits required | PASS readiness; completion pending |

Inspection method: read-only repository/document assessment plus environment
module discovery. No tests, example, compile/import smoke or CI were run by this
Architect. Local dev matched origin/dev at e034825; initial worktree was clean.
SSH remote is origin github.com:hoolheart/softlab.git. Initial sandbox gh auth
reported an invalid token; Developer setup subsequently pushed activation, so
push is evidenced; live CI query capability must still be verified by its owner.
No runtime environmental blocker is established from module discovery alone.
Any new missing required component blocks its affected gate. Existing historical
210-test results are historical evidence, not this release's execution results.

Formal verification must confirm this setup's actual commit/push and clean status
before Tester handoff. Task completion, release completion, warning evidence,
review/test closure, CI and integration remain PENDING.


## Task completion and release preacceptance inspection — 2026-10-21

Delegated inspector: sw-jerry (Architect). Coordinator retains formal gate review.
Inspected implementation/guide disposition and evidence at handoff `0fdd9e1`;
actual architecture update `af1dc74`. **PASS — task completion readiness and
release preacceptance principles**, subject to completed-artifact commit/push
verification. This is not final-candidate CI approval, dev integration, Done,
release acceptance or main promotion. Those gates remain pending.

| Current repository principle | Actual evidence and disposition | Verdict |
| --- | --- | --- |
| 1 Five-element architecture | arch.md af1dc74 describes the actual local composition; tu/huo/shui/jin retain ownership; mu unchanged, no library framework | PASS |
| 2 Characterize before public changes | baseline/approved tester plan and red evidence precede implementation; no library/public import/codec/hook/storage source change; 210 legacy tests retained in 229 passing discovery | PASS |
| 3 Documented TDD gates | PRD c776fe8 → tester plan/developer approval ed02119 → revised design 5fdb984/Architect closure 3a5d9f1 → red tests/contract corrections → implementation6f8b26d → independent review/fix5c5f6c4/approvalc722b9d → independent testsa7953e3 → this inspection; final CI/integration next | PASS |
| 4 One active task | Only M01-W01-001 on codex/m01-w01-thermal-example from dev e034825; clean handoff; no next task, dev/main merge or history rewrite claimed | PASS |
| 5 Role/review ownership | PO owns PRD/acceptance; Architect design finding DR-01 closed by Architect, Reviewer CR-01 closed by Designer, test-contract/runtime failures closed by Tester; inspector did no code/tests/code review; UI N/A | PASS |
| 6 Committed/pushed evidence | Prior handoff0fdd9e1 pushed; architectureaf1dc74 pushed; all linked task records tracked with docs(log) history. This completed update must be committed/pushed before handoff, verified by coordinator | PASS readiness, formal final check required |
| 7 Environment | Independent Tester records Python3.13.15/dependency versions, 229 -W error tests with no skips/warnings, compile softlab/examples/tests, import0.3.0, two CLI runs; CI actually covers3.9/3.13. Inspector relies on owned Tester runtime evidence and independently re-queries CI, does not claim rerun | PASS |
| 8 Baseline/regressions | R4-BASE-SCHED-01/META-01/HDF5-01 remain tracked/unfixed with approved tested local measures; no new observed warning/failure remains. Old OBS-004/005/006/DEFECT-2 unchanged/outside unchanged public paths; cache diagnostics resolved via writable temp configuration. No waived/fixed debt claim or unconfigured lint claim | PASS bounded disposition |
| 9 Instruments/data | Synthetic temp fixtures, no hardware; preflight collisions, source-preserving copy reloads, owned scheduler/loop/backend/figure cleanup tested including CR-01; obstruction cases correctly fail visibly rather than claim release; no physical stop/cancellation promise | PASS |
| 10 Minimal compatible extensions | Scope diff only example, tests, guide/link, records and actual arch; package source/exports/dependencies/Python/version untouched; local bounded adapter/hydration/copy instead of new framework or production repair | PASS |
| 11 Reproducible evidence | Tester AC01–12/NFR01–05 mapping, documented command executed twice, independent read/PNG checks, exact CI records and diff hygiene; start/task/preacceptance inspection present; exact final SHA CI and dev merge remain explicit gates; PO acceptance still pending | PASS readiness, integration/release gates pending |

### Inspector method and limits

Read current AGENTS.md/principles, actual helper boundaries and reload/verification
implementation anchors, architecture, board, design/code reviews, red/green test
and implementation records. Inspected git diff scope from e034825 and clean
handoff status. This is principle/architecture verification, not another code
review or Tester execution. No tests/compile/example rerun; no code/test changes.
Independent `gh run view` on 2026-10-21 confirmed:

- [39141867382](https://github.com/hoolheart/softlab/actions/runs/39141867382): exact a7953e344d54113fcacabf9881832d963239b287, success.
- [39142118165](https://github.com/hoolheart/softlab/actions/runs/39142118165): exact23ae6056ee0d37f4afeea3f35bdc99b80b940aba, completed/success both compatibility(3.9) and compatibility(3.13), including install/regression/compile/import steps.

No future SHA is certified by these runs. Documentation commits after them need
exact candidate CI before dev integration. CI runs ordinary unittest and compiles
softlab; the broader warning/compile claim belongs only to named local Tester
checks. No new failure or unclosed review/test issue is established in scope.
Final `git diff --check` and status/remote verification accompany handoff.

### Baseline debt release disposition and remaining sequence

R4-BASE-SCHED-01: defer concrete singleton-callback repair to separately authorized
work; example uses tested public-interface serial dispatch. R4-BASE-META-01:
defer constructor repair; set_meta creation and disk-only hydration tested.
R4-BASE-HDF5-01: defer destructive-reader repair; copy-only legacy reads preserve
delivered output and guide explains limits. These are future backlog candidates,
not silently closed issues. Existing Release1 debt retains its original tracked
status and affects no changed library path in this example. This explicit bounded
disposition permits the example readiness verdict without certifying library-wide
remediation. Any later remediation needs characterization/scope review.

Next: coordinator formal verification → exact final-candidate CI → dev integration
→ PO release acceptance (and final release-state reconciliation). Do not mark task
Done before integration or release accepted before PO verdict. Main promotion is
not performed by this inspection. UI/Figma, hardware, package publication and
W02–W04 are N/A. Reinspect release state if scope/code/evidence changes or gates
fail; this preacceptance inspection is applicable only to the unchanged bounded
candidate and does not invent future completion.
