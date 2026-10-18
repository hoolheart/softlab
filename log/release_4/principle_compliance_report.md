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
