# Technical requirements review — Release 3 preparation

Owner: sw-jerry (Architect) | Candidate: `9586b2b` | Date: 2026-10-05
Requirements: [PRD](../prd.md) | Verdict: APPROVED
Scope: feasibility, boundaries and execution risks; no detailed API approval.

## Findings

| ID | Severity | Evidence and disposition | State | Reviewer closure |
| --- | --- | --- | --- | --- |
| — | — | No blocking product ambiguity found; implementation constraints below must be resolved in later reviewed design. | — | — |

## Feasibility and required constraints

The pending-control rule is consistent with EVERY edge using boundary-k values:
freeze pending external controls at the start of transition k→k+1 alongside
already committed object outputs. Destinations consume that frozen source set;
no later write or newly evolved output can affect this tick. A control write
is external boundary data, not a same-tick evolved value. Measurements retain
their own explicit initial sample until the first successful delivery. This
must be stated as ingress sampling, not an exception permitting instant edges.

`simulation/object.py` presently commits immediately in `evolve_once` and
`reset`, and observes separately. Serial public calls followed by rollback
cannot satisfy atomic network evolution/observation/reset or factory failures.
A private candidate/validation/commit seam is required; the standalone public
semantics must stay compatible. Candidate output validation must complete for
all connected destinations before publishing any state, sample, buffer or clock.
External callback effects remain unrollbackable, exactly as the PRD specifies.

`Station` currently owns only devices and has side-effect-free descriptions.
An opt-in station simulation coordinator can own graph, buffers and shared
clock without changing ordinary station construction or calling arbitrary
Parameter hooks. Object and mock signal ownership must prevent direct mutation
from invalidating a prepared snapshot, including reentrant mutation from user
callbacks. Interval bindings need one authoritative driver and validation;
initialization, reset and topology changes need explicit boundaries.

`DeviceBuilder` has a `model` property, `build(name, **kwargs)`, a separate
model-keyed registry, duplicate registration rejection, and lookup returning
None when absent (`station/device.py:515–573`). Follow that convention for
objects; preserve the existing device registry and `Station.build_device`
behavior. Do not invent a fluent builder or accept shared mutable instances.

All runtime work fits `tu`: it owns simulation state and explicit deterministic
transitions. The future `huo` process chooses when to call a transition; no
scheduler or concrete process belongs in this release. Tests/docs/process
records outside tu remain necessary. No dependency change is justified.

The four scoped Release 2 corrections are concrete and included in the ordered
backlog: authoritative standalone bridge readback, robust malformed-key errors,
honest reset/external-state wording, and closure-record reconciliation. Existing
Release 1 debt remains separate and open.

## Verdict and limits

APPROVED for preparation. No requirements change is needed. The proposed
architecture and backlog describe future work only; constructor signatures,
endpoint APIs, lifecycle methods and private transaction interfaces await
per-task test-plan and design reviews. All execution tasks remain Backlog and
the user pause before the first task remains in force. This review executed no
runtime checks and grants no code, testing, CI or acceptance approval.
