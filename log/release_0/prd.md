# Workflow preparation requirements

Owner: sw-camille (product owner). Date: 2026-09-26.
Task: BOOT-001. Integration target: `dev`.

## Vision, authorization and timing

Prepare softlab for disciplined, compatible development of its laboratory
abstractions. The user confirmed the preparation scope and remote pushes in the
conversation before work began. That conversation supplied the initial
requirements; this document formalizes them after preparation implementation and
verification, at revision `6d77369`. It is not a retrospective claim that a PRD
file or all formal records existed at task start.

Only workflow preparation is authorized. No `tu` implementation or promotion to
`main` is authorized by this document. The approved project adaptation is one
active task, `codex/<task>` -> `dev` -> accepted release on `main`.

## Must-have requirements and acceptance criteria

| ID | User story / requirement | Verifiable acceptance criterion |
| --- | --- | --- |
| B1 | As a maintainer, I need consistent development rules. | Root `principles.md` and updated `AGENTS.md` state compatibility, serial role gates, evidence ownership, verification and the approved branch flow. |
| B2 | As a contributor, I need to understand the existing design. | Root `arch.md` describes current five-element responsibilities and `tu` contracts; proposed behavior is explicitly separated. |
| B3 | As a maintainer, I need an integration branch. | Local `dev` tracks pushed `origin/dev`; preparation uses a pushed task branch; integration requires final gates and candidate CI. |
| B4 | As a planner, I need bounded follow-up work. | `tasks.md` provides ordered `tu` proposals and compatibility acceptance criteria, explicitly marked unapproved for implementation. |
| B5 | As a team member, I need reusable process records. | `log/README.md` and templates cover requirements, design, review, tests, principles, board and acceptance; actual evidence distinguishes pending and completed gates. |
| B6 | As a maintainer, I need repeatable validation. | CI includes Python 3.9/3.13 regression, compilation and import checks, requires the VISA simulator, and records environment; baseline results and limitations are saved. |
| B7 | As the project owner, I need scope preservation. | Preparation changes only documentation/workflow configuration; no production behavior, runtime dependency, supported-Python metadata or experimental data changes. |

## Non-functional requirements and contributor journey

Keep the existing Python library and public contracts. Use synthetic data and
simulated hardware; preserve user changes and shared Git history. Documentation
must be navigable, distinguish evidence from intent, and avoid unsupported
cross-platform or zero-warning claims. No new runtime performance requirement or
UI is applicable to this preparation.

A future contributor reads principles and architecture, obtains task approval,
branches from `dev`, records requirements and acceptance, completes the serial
test/design/implementation/review gates, and integrates only after verification,
principle inspection and CI. Release promotion is a separate acceptance decision.

## Failure and edge cases

Missing dependencies, failed CI, unresolved review findings or failed checks block
their affected gates. Record existing warnings, skips and environmental limits;
do not silently waive them or repair unrelated code during preparation. An
interrupted run requires inspection of actual files, branch and remote state
before resuming. Later commits require candidate CI; an earlier green run does
not prove a later candidate passed. UI/Figma and production TDD are inapplicable
to this documentation/CI bootstrap, not permanently waived for future work.

## Proposed future `tu` scope — not authorized

The ordered details and criteria are in [tasks.md](tasks.md). Characterization
must precede behavior changes. Preserve parameter calls and value types,
validation/codec/hook semantics, permissions, proxies, snapshots, command side
effects, construction behavior, public imports and fixed-shape mappings unless a
breaking migration is separately approved. Portable descriptions must not impose
JSON constraints on arbitrary runtime values.

Proposals cover side-effect-free descriptions; additive explicit commands;
optional lifecycle/capabilities and owned/borrowed resources; opt-in measurement
metadata/results; documented driver timeout/blocking/error/concurrency contracts;
and model descriptions/configuration with opt-in strict failures. `huo` scheduling,
`mu` services, persistence policy, real-hardware work and universal domain
taxonomies are excluded. Final APIs need future approved requirements and design.

All B1-B7 items are must-have. Further convenience documentation is should-have
only if needed to make those records usable; no optional tooling, dashboards or
application features are required for preparation acceptance.
