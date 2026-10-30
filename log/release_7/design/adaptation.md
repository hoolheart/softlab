# M01-W04 — Thermal adaptation detailed design

Status: proposed for independent architecture review. Scope: example and its
contract tests and guide; no supported framework API or physical instrument use.

## Boundary and operation

Retain one acquisition routine and its existing count/getter order: prepare the
private coordinator, issue one power command, perform exactly one coordinator
step before each row, then read the command, source boundary, simulation
clock, temperature and tick. The alternate setup supplies example-local objects
with the same `write(value)`/`read()` control and `read()` measurement operations.
Its control delegates to the existing `MockControl`; its measurement reads the
thermal body's committed `reading` via `SimulatedObject.observe_outputs()` after
the step. That public operation invokes the deterministic observation callback
once and never advances state. Retain the existing coordinator measurement edge
for the original mode; the alternative does not treat its delivered sample as
its own reading. Both modes share the same model, control edge, count process,
record schema, cleanup and persistence path. Avoid private coordinator state.

Represent the example-local timing policy explicitly at setup, with its mode,
measurement delay and source boundary rule. Original mode is `tick-1`, one
boundary behind; alternative mode is `tick`, the just-committed model output.
The policy computes only row metadata; it must never invoke extra `step()` calls
or sleep. The alternate is direct model observation, not a physical sensor or a
claim about real settling. A physical mapping in the guide should identify power
write/read units (W), temperature read (degC), configuration and resource ownership,
settling and timeout/error policy, and explicit separate physical elapsed time;
no real command, connection, calibration or safety behavior is inferred.

Make adapter selection explicit in the example command and execution setup while
retaining the original default and existing calls. Reject unsupported choices
before acquisition. Avoid changing `ThermalConfig` or production-layer APIs.
Keep cooperative stop, completed-row accounting, exception visibility and
scheduler/backend/figure cleanup on the existing path. A failure following a
successful step can leave the boundary advanced without a row; never retry or
fabricate an observation. Contract doubles should confirm command and sample
order and propagation of write/read failures.

## Stored context and compatibility

The existing version 1 validator intentionally requires previous-boundary
metadata and its corresponding recurrence. Do not relax it to accept new
semantics. Continue producing and loading version 1 for the existing default.
Use a distinct version 2 envelope for alternate mode, with the same closed field
shape and clock values `measurement_delay_ticks: 0`, `source_tick: tick`, and
`acquisition: post_step`. Its timing metadata and rows must agree with that
clock. Dispatch validation by exact schema/version and reject other versions,
unknown keys and inconsistent timing as before. The version is the explicit
identity for this bounded alternate policy; no generic serializer is added.

For default configuration, alternative observations at boundaries 1–8 are
`[21.0, 21.9, 22.71, 23.439, 24.0951, 24.68559, 25.217031, 25.6953279]`
degC; original results remain `[20.0, 21.0, 21.9, 22.71, 23.439, 24.0951,
24.68559, 25.217031]`. Validate each stored result against the declared policy
and independent Euler recurrence, including source ticks, command and row
count, rather than trusting metadata alone. Fresh-process reload must preserve
both envelope versions and existing absent-manifest legacy handling.

## Verification and risks

Test both modes with independent expected traces, matching ordered columns/units,
one step per row and source boundaries; verify version-specific metadata after
reopen and reject mixed policy or malformed versions. Exercise alternate adapter
write/read failures, cancellation and owned cleanup, and run the existing W01–W03
regression suite. The documented fresh-directory headless command must run the
selected mode end-to-end and be checked for manifest, persisted rows and plot.

Direct observation invokes the observation callback again after the coordinator
has published its snapshot; this is safe for this example's pure deterministic
callback, but is not equivalent for a stateful or nondeterministic callback and
is not a general adapter contract. Stored source identity and timing must be
truthful for both modes. Neither delay policy establishes physical settling;
wall timestamps remain distinct from simulation time. W04 excludes VISA and real
hardware. Existing baseline limitations remain tracked, and new failures must
be reported rather than silently waived.
