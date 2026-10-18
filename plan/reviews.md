# Planning review

Status: **APPROVED — planning documents only**.
Reviewer: independent Architect (sw-jerry), 2026-10-18.
Reviewed baseline: local `dev` baseline `82883ad`, planning branch
`codex/long-term-plan`. No implementation, runtime tests, CI, commit, push,
integration or release acceptance is certified by this review.

## Documents and method

Reviewed [plan overview](README.md), [roadmap](roadmap.md),
[first milestone](milestone-01.md) and [first-release PRD](release-01-prd.md)
against [project principles](../principles.md), [architecture](../arch.md),
common process/hooks, scheduler, coordinated simulation, HDF5 and plotting source.
This is source/document inspection, not execution evidence. Production
architecture remains unchanged.

## Findings owned by this reviewer

| ID | Severity | Finding and required correction | Status |
| --- | --- | --- | --- |
| AR-01 | Required clarification | PRD FR-10 requires stopping the Coordinator using its documented lifecycle, but Coordinator has no `stop()` or `cleanup()` operation. Its `reset()` is a fallible model transition that clears final timing/state; it must not be treated as unconditional teardown. Require the runner to cease issuing steps, inspect final state, then release its privately owned graph references. If reset is deliberately used, describe its purpose and report its failures. Continue to require actual scheduler/backend/figure cleanup. | CLOSED by Architect after reinspection: FR-10/AC-10 and lifecycle prose now require cessation, final-boundary inspection and graph disposal, explicitly excluding implicit reset or fictitious stop APIs. |

## Feasibility and dependency assessment

The monthly sequence is coherent: concrete reference workflow precedes common
execution extraction, provenance precedes adapters/scans and later reporting,
and a second domain precedes broader generalization. Every month has an
observable exit rather than an API-count target. M04/M10/M11 external-input
gates are explicitly conditional, not granted by calendar passage. M12 is
release acceptance, not an inference from earlier CI. Review checkpoints at
M03/M06/M09 limit speculative investment.

Four weekly releases are ambitious but bounded by a single educational scalar
case, example-local glue, and removal of optional presentation/VISA work before
required evidence. W01 is plausible in one focused week if prerequisites are
already sound; mandatory injected failures, independent walkthrough and CI are
part of its estimate. A prerequisite production defect triggers scope review,
not unplanned framework development. W02 cancellation and W03 metadata remain
bounded examples before M02/M03 reusable contracts. Later-month performance
bounds and domain choice require kickoff decisions, appropriately deferred.

The W01 hook is feasible by inspection:
[`AtomJob.body`](../softlab/huo/process/common.py) invokes synchronous
`hook_before_get` before getter iteration and appends a row only after all
getters complete; `count` forwards this hook. Base Parameters can expose tick,
simulation time, commanded power and source boundary without changing public
semantics. The legacy wall timestamp is collected earlier, so it must remain
separately labeled as required by the PRD. Scheduler and backend boolean/status
results require explicit checks; source inspection does not establish that the
future runner has implemented those checks.

The numerical oracle is correct under
[`Coordinator.step`](../softlab/tu/simulation/coordination.py): pending control
10 W feeds the first candidate evolution while the measurement receives
previous-boundary output. The recurrence is `T_next = 0.9*T + 3`; measurements
at ticks 1–8 are `20, 21, 21.9, 22.71, 23.439, 24.0951, 24.68559, 25.217031`.
The source boundary is `k-1`, acquisition boundary `k`, with no hidden warm-up.
This arithmetic/source check does not substitute for independent tester tests.

HDF5 stringifies metadata and is not atomic; the PRD correctly requires explicit
text encoding, fresh paths, reopen checks and truthful incomplete artifacts.
Storage preserves experiment context without promising arbitrary Python object
serialization. Plotting consumes reopened records using existing `plot_record`;
Matplotlib's headless selection must occur before eager package plotting imports.
Failure after an advanced tick does not imply rollback; failed sampling cannot
fabricate a success row. Software cancellation is separately scoped to W02 and
never claims physical stop. Fresh instances support deterministic numeric repeats
without pretending wall timestamps or UUIDs are identical.

## Principle inspection — planning applicability

| Principle | Planning evidence / disposition |
| --- | --- |
| 1 Five-element architecture | PRD layer boundaries and roadmap use existing `tu`/`huo`/`shui`/`jin`; future work labeled proposed; `arch.md` untouched. |
| 2 Characterize behavior | PRD baseline callers, NFR-01 and preimplementation characterization; execution evidence remains future. |
| 3 Documented TDD gates | README workflow and PRD tester/developer/designer/architect sequence; runtime TDD N/A to documentation-only change. |
| 4 One active task and branches | One planning task on `codex/long-term-plan`; docs preserve `codex` → `dev` → `main`; no integration performed. |
| 5 Role/review ownership | PO drafts, independent Architect reviews; AR-01 remains reviewer-owned; UI N/A. |
| 6 Committed/pushed evidence | Planning docs specify future `log` artifacts and `docs(log):`; no commit/push certified here. Handoff evidence remains coordinator/developer responsibility. |
| 7 Environment verification | PRD lists exact regression/compile/import commands and configured Python CI; no Python changed and runtime checks N/A to planning. |
| 8 Baseline versus regression | Existing OBS/DEFECT debt explicitly tracked; no silent waiver or incidental fix authorized. |
| 9 Instruments/data safety | Synthetic thermal case, temporary paths, no hardware authorization, collision refusal, ownership/failure requirements; AR-01 clarification verified and closed by Architect. |
| 10 Minimal compatible extensions | No new dependency/framework/version changes; example-local glue first and extraction only after demonstrated duplication. |
| 11 Reproducible evidence | Weekly/month exits, independent review, principle checks and candidate CI before `dev`; release acceptance before `main`; future gates explicitly unrun. |

AR-01 was corrected by the Product Owner and reinspected against FR-10/AC-10
and the lifecycle section. All technical planning findings are closed; the
planning verdict is APPROVED. This does not authorize implementation. Formal
commit/push and final link/diff evidence remain separate from this technical
verdict; implementation and release gates remain future work.
