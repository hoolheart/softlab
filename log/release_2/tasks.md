# Tasks — Release 2 simulation foundation

Owner: sw-jerry | Date: 2026-10-05 | Status: preparation complete; paused before SIM-001
Requirements: [PRD](prd.md) | Technical review: [approved](reviews/prd.md)
Baseline: `80b0b05` from `dev`; preparation branch `codex/tu-simulation-foundation`.

## Single bounded task

| ID | Deliverable | Acceptance | Dependencies | Status |
| --- | --- | --- | --- | --- |
| SIM-001 | Deterministic simulated-object foundation and existing experiment-interface example | SIM-AC-01–08 | Release 1 accepted baseline; all serial gates below | Backlog |

This is one planned task across roles. Execution is paused before test planning,
detailed design and implementation until the user resumes it.
No next task or unrelated baseline repair is authorized by this decomposition.

## [PLANNED — not implemented] Architecture

Propose a focused `softlab/tu/simulation/` package and expose it through `tu`'s
package initializer. Its simulated-object abstraction owns declared inputs,
internal state and outputs. Keep it independent of `Device`,
`TheoryModel`, scheduling and storage. Model authors supply deterministic
transition and observation functions; the foundation validates declarations,
copies supported numerical values, evolves synchronously on explicit requests,
observes without advancing and restores initial conditions on reset.

The required behavior is `evolve(inputs, previous_states) -> next_states`;
observations derive from the resulting state. Exact API signatures remain
undecided. Time or dt may be model inputs or optional context, without requiring
a dedicated clock. Publish completed state/outputs only after callback execution
and validation succeed.
This defines transaction boundaries for the simulation object only. Concrete
signatures, supported-value rules, validator isolation, reentrancy policy and
exception cases belong in the reviewed detailed design.

Use existing `Parameter` callbacks to bridge inputs and observations to one or
more mock `Device` instances sharing one object. A user guide example should
show an accumulating model and an ndarray model; a count/scan integration should
put explicit stepping in an existing process hook. Parameter acquisition time
remains wall-clock time; any model-specific time is a separate concept.

Production edits are restricted to `softlab/tu/`. Tests in `tests/`, a guide in
`docs/`, process evidence here, and final `arch.md` updates are necessary outside
that directory. No `huo`, `shui`, `jin` or `mu` production changes, new dependencies,
Python-support changes, solver framework or device hierarchy are planned.

## Serial delivery and owners — pending user resumption

1. sw-mike records fresh Release 1 baseline and writes SIM-AC-01–08 tests/test
   plan, including mutable-alias and failure atomicity cases. sw-tom reviews
   implementability; disagreements return to the tester before design.
2. sw-celeste creates detailed design; sw-jerry reviews architectural fit,
   compatibility and explicit contracts before production work begins.
3. sw-tom implements only the approved design and guide, with committed red/green
   evidence and no unrelated repairs. Existing characterization remains intact.
4. sw-celeste performs independent code review and owns review issue closure.
5. sw-mike executes regression, example/integration, compile/import and warning
   checks and owns test-failure closure. Record actual environment and omissions.
6. Coordinator obtains task principle inspection and candidate CI evidence;
   only then may sw-tom integrate into `dev` using the repository branch policy.
7. sw-jerry updates actual architecture; sw-camille performs release acceptance
   after release principle inspection and verification. Promotion to `main`
   requires that acceptance; no direct task merge to `main`.

Every completed process update is committed/pushed immediately with `docs(log):`.
Board phases track this sequence. UI and hardware gates are N/A. Tests, design,
implementation, code review, task verification, CI and acceptance are pending.
Baseline checks started before the pause are recorded in the principle report;
they are not a completed task test gate.

## Risks and tracked limitations

OBS-004 initialization hooks, OBS-005 feature fallback, OBS-006 delegated names
and DEFECT-2 deliberate invalid-permission warning remain recorded Release 1
debt. Avoid depending on them. New tests must report actual baseline warnings
and failures; no blanket waiver or unrelated fix is authorized. Callback external
side effects and numerical validity remain model-author responsibilities.
