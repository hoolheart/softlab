# Release 3 architecture plan — PLANNED, NOT IMPLEMENTED

Owner: sw-jerry | Date: 2026-10-05 | Baseline: `603ba85`
Requirements: [PRD](prd.md) | Technical review: [approved](reviews/prd.md)
Preparation only: no task design, executable specification or API is approved.

## Responsibilities and compatibility

Keep `SimulatedObject` as the model/state abstraction in `tu.simulation`.
Add an internal way to build detached evolution/reset candidates and validate
outputs before committing; reuse it for coordinated operations without changing
standalone evolve-versus-observe semantics. No public sequential rollback scheme.

A station-associated, opt-in coordinator owns participant enrollment, connections,
source snapshots, delivered samples, clock and lifecycle. Station manages object
membership alongside devices and exposes the shared interval. Ordinary
`Station(name)` and existing device use stay valid without simulation setup.
Graph mechanics should remain a focused simulation responsibility rather than
inflate device/parameter abstractions. Avoid circular imports: simulation core
must not depend on Station; Station may compose simulation capabilities.

Provide explicit opt-in mock signal storage/bridges. A mock control source owns
pending values; a mock measurement sink owns delivered samples. Neither requires
executing arbitrary device hooks or hardware access. Standalone bridge control
getters read authoritative object inputs; connected controllers instead read
pending source values and connected measurements read buffered delivery. Document
these distinct roles rather than silently changing existing Parameter behavior.

Object builders follow DeviceBuilder's model/build/registry convention with a
separate registry, duplicate-model rejection and discovery/lookup. Construction
and membership validation must succeed before station insertion. Independent
instances own independent mutable values; a builder returning an already owned
participant is rejected. Registration does not construct an object. No device
builder refactor is needed.

## One boundary snapshot for every edge

For each explicit transition k→k+1:

- Freeze pending external control sources at boundary k. Combine them with
  object outputs already committed at k; every edge reads this source set.
- Form detached destination inputs and mock delivered samples from that set;
  preserve unconnected pending/declared inputs. Fan-out values must not alias.
- Evolve every object from state k with its prepared inputs. Evaluate candidate
  outputs from candidate states and validate them, including compatibility with
  every connected destination. No candidate feeds an edge in this transition.
- Publish all candidates, source buffers, delivered samples and clock only when
  every callback and validation succeeded. Publication must invoke no user code.

This is simultaneous discrete evolution, independent of insertion order for
pure deterministic callbacks. Shared external side effects cannot gain that
order-independence guarantee. A→B→C advances at most one edge per transition;
feedback uses initialized boundary values. Controls written between ticks enter
the next boundary sampling operation, not a same-tick propagation shortcut.
A failed attempt leaves those accepted writes pending for a retry.

## Initialization, reset, clock and ownership

Explicit preparation builds tick-zero outputs from initial states, checks all
connections and stages initial mock samples without delivering the first edge
implicitly. Failure publishes no partial graph/clock/ownership activation.
Station inspection, descriptions and ordinary membership queries never prepare,
evolve, evaluate callbacks or acquire hardware.

Coordinated reset stages all initial inputs/states, control values, output
buffers and explicit measurement initial samples, then publishes tick/time zero
together. A factory/output failure leaves owned committed data and clock intact;
external effects are outside the guarantee. Reproducible factories/callbacks are
necessary for reproducible observations. Reset must retain coherent ownership
until an explicit safe detach/stopped boundary; it is not permission for two
stations to activate the same participant simultaneously.

The interval is explicit, finite, positive and non-boolean. It is station-owned,
fixed during a run, and changeable only at the reset boundary. Use an explicit
binding for any declared model dt input: validate compatibility, reject another
connection driver or contradictory writes, and never infer a variable by name.
Detailed design must settle representation, representability/overflow checks and
roundoff policy before implementation. Model callback arguments remain unchanged.
A future `huo` process supplies invocation timing, not another simulation clock.

Guard owned participants against standalone evolve/reset, connected-input writes,
mid-tick/reentrant control writes and topology changes that invalidate candidates.
Unconnected input writes may stage pending values through the controlled boundary.
Read access remains safe; release ownership only at a documented stopped boundary.
No thread-safety guarantee is added. Endpoint identity must survive mutable device
names by using stable membership references, not reparsing a live display name.

Connections are directional and single-driver per destination, with fan-out and
cycles permitted. Rejected endpoint/driver/value edits cannot partially alter the
graph. Connected participant removal is rejected; other removal remains bookkeeping
without cleanup. New object/device cross-name collisions must be rejected without
changing existing behavior where no object collision exists. Method-name collisions
remain accessible by explicit lookup. Preserve legacy device-only snapshot and
description contracts; any simulation description must be additive and inert.

## Scoped Release 2 correction and documentation

Correct mixed-type callback-key diagnostics without changing non-mapping errors.
Correct standalone bridge readback after direct set, reset and shared controllers.
Narrow reset claims in object docstrings, guide and architecture to owned-data
restoration/preservation, with external-state caveats. Reconcile Release 2 board
against acceptance `c69272a`, summary `603ba85`, integration `9408c79` and actual
promotion evidence; do not rewrite historical test results or invent promotion.

Only `softlab/tu/` runtime changes are planned. Tests, guide, architecture and
process records justify edits outside it. Solvers, configurable delays, graph
persistence, noise, real hardware, thread safety, real-time scheduling and the
concrete `huo` experiment process remain excluded. Actual `arch.md` capability
updates follow implementation, not this preparation.
