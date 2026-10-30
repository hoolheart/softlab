# M02-W04 thermal integration detailed design

Owner: Software Designer | Status: PROPOSED — architect review required

Inputs: [W04 PRD](../prd.md), [M02 plan](../../../plan/milestone-02.md),
`examples/thermal_experiment.py`, `softlab/huo/process/bounded.py`, W01–W03
acceptance and existing thermal, manifest, adapter, execution and W01
characterization tests. Design is for review, not evidence of implementation.

## Boundary and timing

`execute` remains the public thermal orchestration entry: validate configuration,
adapter and running-loop preflight before reserving output, then build the model
and invoke `_acquire`; persist, reopen/verify and plot only after completed
acquisition and successful owned execution cleanup. `_acquire` constructs the
same DataGroup, actual DataRecord and getter list and prepares Coordinator and
heater exactly as now. Its fresh `count` uses `Coordinator.step` as
`hook_before_get` and preserves existing getter insertion order. It invokes
`softlab.huo.process.run_count(process, config.points, lambda: len(record),
lambda: (coordinator.tick, coordinator.sim_time), stop=stop, after_row=bridge)`.
The shared runner owns scheduler, loop, commit/join cycle and cleanup; remove the
example's scheduler, loop drain, close and duplicated cycle from the shipped
execution path. Model/control setup, finite-data checks, metadata, HDF5 writer,
reload and plot remain example-owned. Do not reach into shared private loop or
close global/borrowed resources.

`StopRequest` is re-exported from `softlab.huo.process` at the existing thermal
name; preserve constructor, mutable flags and `request()` semantics. The bridge
copies `completed_rows` and observed `(tick, time)` to the existing `RunResult`
before invoking the user's `after_row(RunResult)`, retaining stop object
identity. During the callback preserve the existing `RunResult.stage='setup'` (the local
execution stage is acquisition, but it is copied to the result only at exit),
`acquisition='not_started'`, `terminal=False`, and false cleanup flags; do not
map the runner's intermediate state as a final acquisition verdict. Check row numeric
finiteness before invoking the user callback, as before; a raised callback or
finite check is the runner's primary action failure. Preserve the completed
boundary-versus-row count validation, executed before persistence. In both
modes preparation and one power write precede acquisition; each count action
advances exactly once, then getters read commanded power, source tick, simulation
time, temperature and tick in the established order, then a complete row is
committed. Coordinated mode reads source tick `tick-1` and uses cached model
measurement; direct mode reads source tick `tick` and calls its existing pure
`observe_outputs` once per row. Runner adds no reads. No general idempotence
assumption for other sensors.

## Result and failure mapping

After `run_count` returns, copy `acquisition`, `completed_rows`, `cause`,
`cleanup_errors`, `work_drained` and `loop_closed` into `RunResult`; map the
last successfully observed tuple into `final_tick` and `final_sim_time` only if
`boundary_observed`, otherwise retain prior known values. Preserve the actual
in-memory group and partial record even on cancellation or failure. Keep the
existing thermal `RunResult` fields, `overall_success`, stage semantics,
`RunFailure` chaining and `run()` return contract. Add no shared retry/reset.
Cancelled/failed acquisitions and completed acquisition with cleanup diagnostics
must not proceed to persistence; retain acquisition status separately from cleanup
failure. In the bridge propagate user stop requests and update completed rows and
boundary before calling the callback. Keep completion checked before stop so
late stop does not change success. Preserve v1/v2 manifests and their data group
identity: manifest `run_id` remains `str(group.id)`; bounded `CountResult.run_id`
is distinct in-memory execution evidence and is not substituted for stored
identity or added to the schema. Do not assert equality between the two IDs.

Before runner invocation model build, prepare or initial heater write may fail.
As approved in the [product decision](../reviews/setup-failure-product-decision.md),
these paths have no shared runner resources: preserve original failure and partial
context, set `work_drained=False` and `loop_closed=False`, and make reset
ineligible. This intentionally changes the old prepare/write failure flags and
reset eligibility. Keep setup ordering, including pre-stopped execution; do not
create a throwaway loop or defer setup into an action.

A `run_count` preflight exception carries no `CountResult`: catch it as an
acquisition-stage failure in `execute`, retain the exact exception, report false
runner cleanup flags and do not proceed to persistence. This includes failures
before resource allocation; no cleanup is claimed. The thermal running-loop,
configuration and output validation preflights still raise before acquisition.

For an interrupted runner, inspect its attached `count_result` when available,
map its truthful terminal evidence to thermal result, perform example-owned
writer cleanup independently, then re-raise the same `BaseException`. If the
runner could not attach evidence, its stderr fallback is authoritative: retain
only independently observable group/record and coordinator boundary in the
thermal result, leave unobserved cleanup flags false, report no inferred runner
cleanup and re-raise the same interruption. The example may print its existing
diagnostic but must not mask that interruption. No caller-side scheduler cleanup
is introduced. When no runner result is available, partial rows and known
boundary may be inspected independently without rewriting the original cause.
`reset_after_run` remains the existing API and only resets the retained
coordinator after terminal/drained/closed evidence; optionally retain the
underlying `CountResult` privately and delegate to `reset_after_count`, but
preserve eligibility and failure identity and never rewrite prior evidence.

Persistence, reopen and plotting errors remain distinct in `RunResult`; release
HDF5 writer and temporary reload resources as before. Ensure teardown exceptions
do not replace acquisition primary error, and successful acquisition with failed
cleanup is not overall success. Fresh repeats build a fresh model, Counter,
record and runner with distinct group and bounded identities. No physical
termination, retry, rollback or durable partial data is implied.

## Legacy evidence and test disposition

W01 characterization tests directly use `_CountScheduler`, `_make_scheduler`
and `_acquire` as historical observation seams; these are private example
helpers and are not a promise of a supported public execution API. Do not keep
an unused duplicate scheduler solely to satisfy them. Move historical
characterization fixture into test-owned support (or retain it in a dedicated
historical test fixture), explicitly preserving its observed oracles and test
names; adapt tests patching example-private scheduling to inject failure at the
shared runner or scheduler boundary and verify equivalent behavior. Record each
adaptation and why historical baseline evidence remains applicable; do not
silently drop tests or weaken expectations. Keep public thermal and manifest
compatibility tests unchanged where feasible and document necessary private
fixture changes. Independent tests must exercise the actual shipped entry points
and both adapter modes using predeclared timing/numeric oracles, direct
observation counts, stop and partial failure, cleanup, reset and identity, v1/v2
round-trip and separate-process inspection, malformed/corrupt metadata, existing
CLI exit/output behavior, plus the distinct signal monitor. Verify no hidden
reads or caller-owned cleanup of runner resources. Test baseline W01–W03,
legacy `count`/`scan`, fresh repeat and an independent fresh-environment full
walkthrough with a new output directory. Capture environment warnings and run
full discovery, compile/import and exact candidate CI under project gates.
