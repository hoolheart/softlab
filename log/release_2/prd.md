# Requirements — Release 2: simulation foundation

Owner: sw-camille (Product Owner) | Status: requirements defined; technical
review and all delivery gates pending | Date: 2026-10-05
Baseline: `80b0b05`, branch `codex/tu-simulation-foundation` from `dev`.

## Goal and assessment

An experiment author should be able to prepare an experiment against an
in-memory simulated object, expose its controls and measurements through
existing device/parameter interfaces, and repeat a run from a known starting
condition without touching hardware.

The proposed separation is sound: a mock instrument represents how an
experiment controls or observes something; a simulated object represents the
thing being controlled or observed. Multiple instruments may observe the same
object, so object state must not implicitly belong to one instrument. Inputs,
internal state, outputs, an `evolve` function and an output function
are sufficient building blocks for a bounded foundation.

The user clarified that `evolve` maps inputs and previous states to next
states: conceptually `x_next = evolve(u, x_previous)`, then `y = G(x_next)`.
A memoryless model may ignore the previous state. These equations express
the required behavior; the detailed API and argument representation remain
pending. Time or `dt`, when a particular model needs them, can be represented
as inputs or optional context. A dedicated clock or time argument is a proposed
extension, not a user-imposed signature or a mandatory foundation feature.
The MVP
supports state-derived outputs as requested; direct input-to-output feedthrough,
continuous-time solvers and event simulation are future extensions. Model
authors remain responsible for scientific validity and numerical accuracy.

Release 1 supplied a compatible station/device/parameter foundation, not a
claim that all existing limitations were solved. Its acceptance and known
limitations remain the starting point for this additive work.

## Bounded scope and module boundary

Production changes are restricted to `softlab/tu/`, including public exports.
Provide a reusable simulated-object abstraction with explicit state evolution,
state-derived observation and reset; provide a documented, runnable way to
connect it to existing `Device`/`Parameter` interfaces. A small adapter is
permitted if needed, but neither a universal mock-driver hierarchy nor changes
to existing instrument behavior are required. Exact interfaces and placement
are the architect/designer's responsibility.

Changes outside `tu` are necessary only for regression/integration tests,
user documentation and architecture/process evidence (`tests/`, `docs/`,
`arch.md`, README pointer if useful, and `log/release_2/`). They establish and
explain the new contract rather than extend other production modules.
Existing `huo` consumers should work through their current parameter contract;
no scheduler, process, persistence, service or visualization changes are
required for this foundation. If technical review finds another production
module must change, explain the specific incompatibility to the user before
expanding scope.

Excluded: automatic wall-clock advancement, background workers, thread safety,
multi-object scheduling, ODE/PDE solvers, calibration/fitting, stochastic/noise
frameworks, model persistence, hardware qualification, real instrument access,
UI, new dependencies and Python-support changes. Deterministic user functions
are the MVP reproducibility contract; callbacks with external mutable state,
random generators or I/O require caller management and must be documented.

## Acceptance criteria

All criteria are Must; the tester owns concrete cases and actual outcomes.

| ID | User need / behavior | Verifiable acceptance condition |
| --- | --- | --- |
| SIM-AC-01 | Distinct input/state/output roles | A model exposes declared input, state and output variables; a documented scalar example and an ndarray-valued example demonstrate the supported value contract. Invalid declarations, unknown variables and incompatible values have documented errors. |
| SIM-AC-02 | Dynamic and memoryless evaluation | A deterministic accumulating model uses `evolve` to map inputs and previous states to next states; a memoryless model also works. Each explicit evolution invokes `evolve` once and produces outputs from the resulting state. Time/`dt` may be supplied as model input or optional context; no dedicated clock API is required. |
| SIM-AC-03 | Read without unintended evolution | Inspecting state, outputs or declarations does not invoke `evolve`, change committed state or access hardware. Repeated observations have stable results under the documented deterministic callback contract. Input assignment alone does not silently evolve state. |
| SIM-AC-04 | Repeatable preparation | Reset restores documented initial inputs, state and output behavior, including any time values represented there. Replaying the same deterministic input/evolution sequence reproduces the same observations; independently constructed objects do not share mutable state. |
| SIM-AC-05 | Predictable failures and ownership | Input validation, evolution and output failures are visible, with original callback exceptions preserved. A failed input update, evolution or reset does not leave partially committed object state. Mutable values supplied by callers, returned by inspection or passed to callbacks cannot silently mutate committed state; any deliberately unsupported value category is clearly rejected/documented. External callback side effects are outside rollback guarantees. |
| SIM-AC-06 | Existing experiment interfaces | A synthetic worked example uses existing `Device`/`Parameter` interfaces for inputs and measurements, shows explicit advancement, and can share one object between controls/observations. An integration assertion exercises an existing `huo` count or scan path without editing `huo`; define where advancement occurs rather than interpreting real sleeps as simulation steps. |
| SIM-AC-07 | Compatibility and imports | Existing Release 1 characterization/regression assertions remain green, including parameter permissions/codecs/hooks, virtual-device lifecycle, snapshots/descriptions, theory fallbacks and ndarray mappings. New public imports work without circular imports or new required dependencies. |
| SIM-AC-08 | Usable, honestly bounded delivery | English API docstrings and a user guide explain construction, `evolve`, observation, reset, errors, ownership, any optional time context and the mock-device/object distinction, with an executed deterministic example. Architecture reflects actual implementation only after delivery. Full unittest, compile/import, warning and candidate CI evidence is recorded before integration. |

## Compatibility, debt and gate applicability

Do not reinterpret `TheoryModel.features`, existing `Mapping`, device lifecycle
or parameter reads globally as simulation operations. No JSON serialization
requirement is imposed on numerical values by this release. Existing `Reading`
acquisition timestamps retain their wall-clock meaning; any model-specific
simulated time is a separate concept.

Release 1 accepted 99 tests on its recorded revisions; that is historical
evidence, not a fresh result on this baseline. Re-run characterization before
production changes and retain current results in the tester's record.
Known OBS-004 (subclass initialization/set-hook hazard), OBS-005 (legacy
feature-error fallback), OBS-006 (delegated-name collisions) and DEFECT-2
(deliberate warning for a parameter with neither permission) remain tracked in
`log/release_1/compatibility.md` and `arch.md`. This request does not authorize
unrelated debt repair. Avoid depending on those hazards; report any encountered
failure/warning explicitly and obtain its disposition through the workflow.

UI/Figma and real hardware gates are not applicable. The TDD test-plan review,
design review, independent code review, tester execution, principle inspection,
CI and release acceptance gates apply. Use one active task and the repository's
`codex/<task>` → `dev` → `main` policy; no generic direct-to-main skill merge.
This PRD records requirements only: no implementation, test, review or acceptance
gate is claimed complete by its creation.

## Preparation pause

The user requested continuation of release preparation only, then a pause
before the first task. This correction records their `evolve` terminology and
input/previous-state contract. Task execution, test-plan creation, detailed
design and implementation remain pending until the user resumes them.
