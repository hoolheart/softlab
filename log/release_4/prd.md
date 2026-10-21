# PRD — M01-W01: simulated thermal experiment vertical slice

## Activation and authoritative scope

The user authorized starting the first release of the first milestone on
2026-10-18 and reusing its prepared PRD after necessary setup. This activates
**M01-W01 only** from [the accepted planning PRD](../../plan/release-01-prd.md)
and [milestone plan](../../plan/milestone-01.md). The historical planning
status remains unchanged; this artifact records the later execution authorization.

Process namespace: **log/release_4** (unused historical namespace at kickoff).
Exactly one active task: **M01-W01-001 — simulated thermal reference example**,
on `codex/m01-w01-thermal-example`, created from `dev`. Roles hand off serially
within that task. [Project principles](../../principles.md) and
[AGENTS.md](../../AGENTS.md) govern `codex/<task>` → `dev` → `main`;
the invoked skill's generic direct-to-main/feature-branch rules do not apply.

The FR-01–FR-12 / AC-01–AC-12, NFR-01–NFR-05 and lifecycle requirements below
are adopted without behavioral scope changes from the planning PRD. The prior
[planning review](../../plan/reviews.md) closed AR-01 regarding coordinator
teardown; it supplies planning evidence only. No runtime, test, CI, principle
inspection, integration or release acceptance gate is certified by this activation.
The independent Tester must still verify the numerical oracle against existing
contracts before development. No known requirements contradiction was found in
this document review; discovered production prerequisites require Architect and
Product Owner scope review before expansion.

No real instruments, procurement, hardware/firmware, W02 cancellation/resume,
W03 rich manifest, W04 adapters, new framework/public API, dependency change,
package version/tag or publication is authorized. No UI change is planned;
UI/Figma gates are not applicable. Required preimplementation test-plan and
detailed-design reviews still apply despite authorization to begin implementation.
Acceptance requires the recorded FR/NFR evidence, independent review/test closure,
principle inspection and candidate CI before `dev` integration. Release acceptance
is separate from task completion and precedes any `main` promotion.

Status: **ACTIVATED — implementation authorized; execution gates pending**.
Kickoff: **2026-10-18**; one working week including required
reviews/testing/integration (capacity forecast, not a calendar deadline). Product Owner scope: a reference
experiment, not a reusable experiment framework. This ID is unrelated to
historical Release 1 or package version numbering.

## Problem and intended user outcome

softlab has simulation, execution, persistence and plotting primitives, but a
researcher must infer how to connect them. The existing coordinated guide proves
simulation behavior; the standalone `huo` hook example does not establish a
complete coordinated run with stored/reloaded results. A user should be able to
run one documented command, inspect a temperature response saved on disk, and
repeat the numeric result from declared inputs.

Primary persona: a Python-using experimental researcher preparing an experiment
before equipment is available. Secondary persona: an instrument/software
contributor learning where model, instrument, procedure and data responsibilities
belong. The first user journey is entirely local, headless and synthetic.

## Goals, scope and non-goals

Deliver a small executable reference example and guide, using existing public
primitives wherever possible. Run one deterministic scalar thermal body, one
heater control and one thermometer measurement through coordinated simulation
and an existing `huo` count/hook path. Store numeric observations in existing
`shui` structures, save/reopen through HDF5, then plot the reopened data through
`jin`/Matplotlib. A tester must independently assert the expected trace.

Production framework extraction, changes to legacy Process/Parameter behavior,
universal cancellation, durable partial-run recovery, comprehensive provenance,
new Station description nodes, SQLite parity, fitting, noise, physical accuracy,
VISA adapters, hardware, GUI, notebook requirements, graph/model serialization,
new dependencies, package version changes and public run APIs are excluded.
W02 owns supported interruption; W03 owns the richer manifest. No example-local
helper becomes a supported public API by implication.

## Baseline and proposed boundaries

Read callers before implementation: [common processes](../../softlab/huo/process/common.py),
[SimpleProcess](../../softlab/huo/process/simple.py),
[simulation guide](../../docs/user-guide/simulation.md),
[coordinator](../../softlab/tu/simulation/coordination.py),
[data model](../../softlab/shui/data/base.py),
[HDF5](../../softlab/shui/data/hdf5.py) and
[plotting](../../softlab/jin/vis/plot.py).

`tu` remains model/control/measurement ownership; `huo` executes acquisition;
`shui` records and persists; `jin` renders. Glue lives in the reference example.
No source defect is silently fixed under this PRD. If integration requires a
production behavior change, the Architect and Product Owner must revise scope,
characterization and estimate before development. Detailed APIs/files are a
later designer-owned artifact.

## Reference experiment and observable timing

The proposed scalar discrete model is
`T_next = T + dt * (P - G * (T - T_ambient)) / C`.
Use floats: `T_initial = T_ambient = 20.0 degC`, `C = 10.0 J/K`,
`G = 1.0 W/K`, `dt = 1.0 s`, fixed heater command `P = 10.0 W` and
`N = 8` acquisition points. This is an explicit Euler educational model;
`0 < dt * G / C <= 1` is the supported example stability range. No solver,
continuous-time accuracy or calibrated physical interpretation is promised.

Use one control→object-input edge and one object-output→measurement edge, with
model interval explicitly bound. `prepare()` establishes boundary zero. Write
the command before the first acquisition step; at every point advance exactly
once, then read the delivered measurement. There is no unrecorded warm-up tick.
The coordinator's existing previous-boundary edge rule is preserved: at tick 1
the pending control value is delivered to the object and used for its candidate
evolution, while the thermometer receives the previous object's output. The default delivered trace
for ticks 1–8 must therefore be
`[20.0, 21.0, 21.9, 22.71, 23.439, 24.0951, 24.68559, 25.217031]` degC,
assuming the reviewed topology uses initial input `0.0 W` and initial
measurement `20.0 degC`. The test plan must independently verify this derivation
against existing coordinator contracts before implementation, and correct the
PRD through review if a contradiction is found. Never remove the delay to make
the curve resemble an instantaneous thermometer.

Each row describes acquisition at the published boundary: `tick = k`,
`sim_time_s = k * dt`, commanded power, delivered temperature and its effective
source boundary `max(k - 1, 0)`. This source-boundary field describes the object
output feeding the measurement, not a claim that the control had already affected
that state. Wall-clock acquisition timestamps from existing `AtomJob` may be
retained as such, but never relabeled simulation time. Numeric comparisons use
absolute tolerance `1e-10` for this finite default fixture.

## Functional requirements

| ID | Proposed requirement | Observable acceptance |
| --- | --- | --- |
| FR-01 | Supply one documented repository-root command with explicit fresh output directory; run without notebook UI, display server or instruments. Default configuration is the fixture above. | AC-01: clean supported environment executes command and produces announced HDF5 and PNG paths; no device/resource discovery occurs. |
| FR-02 | Validate example configuration before starting acquisition or creating final result artifacts: finite floats, C/G/dt positive, supported stability range, N positive integer excluding bool, finite initial/ambient/command values. | AC-02: zero/negative/nonfinite and invalid N fixtures fail visibly with no ticks or completed artifacts. |
| FR-03 | Build one explicit station/object/control/measurement topology; perform exactly one `Coordinator.step()` per `huo` acquisition through a reviewed hook, after command write and before measurement read. Reads/writes alone never advance time. | AC-03: N=8 gives exactly 8 steps and rows; declared numeric oracle matches; recorded timing/source boundaries match the edge rule. |
| FR-04 | Record named numeric fields with units: tick, simulation time, commanded heater power and delivered temperature, plus measurement source boundary. If legacy wall timestamp remains, identify its separate meaning. | AC-04: exact column order/names/units and eight finite rows checked; wall time is excluded from repeat comparison. |
| FR-05 | Put the record in a `DataGroup` with fresh run UUID and minimal string metadata identifying reference example/configuration and timing policy. Use only explicitly encoded text for structured metadata; no arbitrary object serialization. | AC-05: run ID, minimal metadata and configuration can be inspected after reload; a second run has a distinct ID. Rich environment/setup provenance is deferred to W03. |
| FR-06 | Use an explicitly owned existing HDF5 backend, check connect/save/load results, reopen after disconnect and compare persisted record values, definitions and group identity. | AC-06: independent reopened group matches ID, declared metadata, numeric columns/units/rows; backend reports and error returns cannot be mistaken for success. No storage format change. |
| FR-07 | Plot the reopened temperature against simulation acquisition time using existing plotting utilities; label units and explain observation latency in the guide. Save a headless PNG and close the figure. | AC-07: nonempty decodable PNG exists, figure data matches reopened columns, axes carry seconds and degC; tests need no visual UI. |
| FR-08 | Treat existing output locations conservatively: require a new/empty selected output directory and refuse collisions before acquisition; never overwrite an existing result. Default tests use temporary directories. | AC-08: sentinel files remain byte-identical after collision refusal; no ticks occur. |
| FR-09 | Report acquisition, persistence or plotting failure visibly and terminate the command unsuccessfully. Only label the overall command complete after verified reopen and plot. | AC-09: injected model/sample failure, failed backend operation and plotting failure cannot emit successful completion. Completed rows may remain in memory; incomplete files are explicitly identified, not presented as valid completed runs. |
| FR-10 | Release resources created by the example in success/failure paths; use a private scheduler rather than replacing or stopping a borrowed global scheduler. Cease further coordinator steps, inspect the final committed boundary, then discard the privately owned graph; disconnect owned backends and close figures. Do not invent a coordinator stop/cleanup operation or reset it implicitly. | AC-10: success and injected failure leave owned scheduler inactive, backend disconnected and figure closed; no extra coordinator steps occur during teardown, and borrowed/global objects are unchanged. The final boundary remains inspectable before graph disposal. |
| FR-11 | A fresh invocation creates a fresh model/station/process/record; repeat the default command twice without manual global resets. | AC-11: both finite numeric traces match within tolerance, IDs differ, neither contains previous rows, and both outputs reopen. |
| FR-12 | Document setup, command, model, layer responsibilities, timing/latency, outputs, failure reporting and limitations with links to existing contracts. | AC-12: an independent reviewer can follow the guide using only documented dependencies and reproduce the default result. |

## Nonfunctional requirements

| ID | Proposed requirement | Evidence |
| --- | --- | --- |
| NFR-01 | Preserve legacy imports, hooks, parameter value/codec semantics, count/scan behavior and all existing data formats. | Characterization for any touched public path; full repository regression suite. |
| NFR-02 | No new runtime dependency or supported-Python change; avoid real-time pacing and wall-clock sleeps as simulation logic. | Dependency/config diff and executed step-count/timing assertions. Python 3.9/3.13 configured CI results required, not inferred from metadata. |
| NFR-03 | Keep default acquisition bounded at eight steps; model/sample operations are local deterministic scalar work. | Assert point count and termination. No universal latency/performance SLA; record execution duration and environment as evidence. |
| NFR-04 | English type-annotated example helpers/docstrings follow local style; no five-element restructuring. | Design/code review and diff hygiene. |
| NFR-05 | Generated data, PNGs, virtual environments and notebooks outputs are not committed. | Temporary-directory tests and final git status/diff review. |

## Lifecycle, failure and cancellation contract

Preparation is all-or-nothing according to existing Coordinator behavior. A
failed tick does not publish a new coordinator boundary; the reference runner
must not append a fabricated success row. A sample failure after a successful
tick may leave the coordinator advanced without a corresponding row; report
failure and do not roll back or claim transactional acquisition. No automatic
retry is permitted because it could duplicate advancement. Saving and plotting
occur only after successful acquisition in this weekly scope; partial-run durable
persistence is deferred.

Persistence is not claimed atomic: existing HDF5 can partially write on failure.
Fresh paths prevent damage to existing experiments. A failed save/reload leaves
an explicit incomplete artifact diagnostic, never a successful result. A plotting
failure can leave valid stored data; report the storage path and overall failure
separately. Cleanup failures must remain visible alongside the primary failure;
do not replace a primary failure with a misleading successful exit.

The coordinator has no stop/cleanup API and performs no autonomous stepping.
Teardown ceases calls, inspects the final committed boundary and discards the
privately owned graph; it does not reset it. `Coordinator.reset()` is a deliberate
model operation that can invoke fallible factories/callbacks and, on success,
replaces the final boundary with initial values. If a future reset example is
authorized, it must report reset failures and preserve the prior boundary on
failure according to the existing contract. It is not a resource-release action.

W01 does not add a cancellation API, resume API or concurrent/reset-while-running
contract. Do not use `SimpleProcess.reset()` as proof that execution stopped.
For user interruption, best-effort cleanup occurs when control returns to Python;
interruption cannot be promised to preempt synchronous work or process death.
W02 defines/test-drives cooperative cancellation. Repetition in W01 uses fresh
instances, not reset of an active process. No claim is made that cancelling
software stops equipment. Reuse of an externally owned scheduler/backend is not
part of the command interface.

## Acceptance traceability and test ownership

The Tester turns AC-01–12 and NFR-01–05 into an assertion-based test plan before
implementation; the Developer reviews implementability. Required scenario groups:
default numeric oracle and edge ordering (AC-03/04); invalid preflight/collisions
(AC-02/08); fresh-file round trip and plot (AC-01/05/06/07/12); failure injection
and resource ownership (AC-09/10); repeat isolation (AC-11). Fixture expectations
must be independent of the production callback, not calculated by calling it.

The Designer defines detailed glue/hook/lifecycle placement after the test plan;
the Architect reviews it. All implementation changes receive independent code
review, all failures close through the Tester, and principle inspection precedes
integration. Run and record actual commands:

```sh
python -m unittest discover -s tests -p 'test_*.py'
python -m compileall -q softlab
python -c "import softlab; print(softlab.__version__)"
git diff --check
```

Also execute the documented example command with a noninteractive Matplotlib
backend and a fresh temporary output directory. Its final command/path is chosen
in detailed design and must be identical in guide and tests. Record environment,
warnings/skips/baseline defects and configured candidate CI. No hardware, UI
prototype or package publication gate applies to this scope.

## Delivery sequence, estimate and definition of done

Assume one focused contributor plus available independent reviewer and tester:
Day 1 requirements/test-plan review and baseline characterization; Day 2 detailed
design/review; Days 3–4 small example/test implementation and independent review;
Day 5 regression, documentation walkthrough, principles and candidate integration.
These are capacity forecasts, not fixed dates or permission to overlap tasks.
Keep one active task and serial role handoffs on `codex/<task>` from `dev`.

If existing hooks cannot support explicit timing safely, stop feature expansion,
record the prerequisite and replan rather than adding a framework inside this
week. Optional cosmetic plot refinements can be removed. Required numeric,
persistence, failure/cleanup and independent gate evidence cannot be removed to
meet a date; extend the week or revise the accepted scope explicitly.

Done means all FR/NFR acceptance evidence exists, reviewers/testers close their
issues, docs reproduce the command, no unexplained new warning remains, relevant
principles pass, completed process records are committed/pushed under a newly
assigned log namespace, and candidate CI passes before `dev` integration.
Weekly acceptance is distinct from `main` promotion and a package publication.
Rollout is an opt-in reference example with no legacy migration; reverting that
example and its docs removes the capability without changing experimental files.

## Dependencies, risks and decisions

- **Timing misunderstanding:** coordinator delivery latency differs from direct
  object reads. Independent oracle and source-boundary columns make it visible.
- **Metadata loss:** HDF5 stringifies metadata. Keep minimal text encoding in
  W01; W03 reviews a versioned manifest. No backend-wide serialization change.
- **Failure conventions:** scheduler/backends may use bool/status instead of
  raised exceptions. Inspect each result and test failures; do not infer success
  merely because the wrapper returned.
- **Environment:** core installed dependencies include plotting and h5py; no
  notebook extras required. Record missing dependencies as blocking evidence for
  affected checks, not a reason to declare a pass.
- **Baseline debt:** OBS-004/005/006 and DEFECT-2 are recorded in architecture.
  Use base Parameters and explicit noncolliding names; scope any discovered
  prerequisite fix separately. Avoid unrelated cleanup.
- **Open design decisions:** exact example/test paths, minimal metadata field
  encoding and scheduler hookup belong to approved detailed design. Product
  requirements above remain fixed unless reviewed changes resolve a contradiction.
