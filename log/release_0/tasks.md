# Release 0 task decomposition

## Authorized bootstrap

Only **BOOT-001** was authorized; it is now complete. This preparation changes workflow,
documentation and CI, not production APIs. User approval includes remote pushes
and the project-specific integration flow: task branch -> `dev`; approved release
-> `main`. Future tasks below are proposals, not permission to implement them.

| ID | Task | State | Acceptance criteria |
| --- | --- | --- | --- |
| BOOT-001 | Prepare project workflow | Done | `principles.md`, current-state `arch.md`, updated `AGENTS.md`, requirements/backlog, process templates and CI exist; baseline commands/results and limitations are recorded; `dev` and task branch are pushed; review and acceptance evidence are recorded before integration; production code is unchanged. |

Preparation artifacts are on `codex/workflow-preparation`. The coordinator owns
phase tracking and integration authorization. `main` is not the task integration
branch. No future task starts until authorized and its predecessor has completed
the agreed gates and merged into `dev`.

## Proposed `tu` backlog

Each task requires approved characterization/test cases, detailed design,
implementation, independent review, testing and principle inspection, in that
order. Estimates and final API names await requirements/design approval.

| Order / ID | Scope | Acceptance criteria |
| --- | --- | --- |
| 1 / TU-001 | Characterize existing contracts and consumers | Tests and a compatibility matrix document call/get/set behavior, permissions, validation/codec/hook order, initialization, proxies, snapshots, device paths/ownership links, builders/default station, VISA construction/commands/timeouts/errors, and model/mapping behavior. Inspect `huo`, notebooks and examples as callers. Separate existing defects from intended behavior; do not enshrine defects as permanent requirements. No production behavior change. |
| 2 / TU-002 | Add portable descriptions | Add opt-in versioned descriptions for parameters/devices/stations, retaining snapshots and arbitrary runtime values. Descriptions perform no device I/O and serialize with defined unsupported-metadata behavior. Public exports and caller compatibility pass. |
| 3 / TU-003 | Add explicit operations | Add an explicit command path and discoverable side-effect semantics. Existing `VisaCommand` parameter invocation remains compatible; generic inspection never invokes commands. Tests verify each execution count and permission behavior. |
| 4 / TU-004 | Add optional lifecycle and capabilities | Approve minimal contracts appropriate to virtual and physical devices, explicit owned/borrowed resources and readiness semantics. Verify repeated cleanup, failed initialization, shared handles and unsupported capabilities without adding a scheduler or forcing virtual devices to connect. |
| 5 / TU-005 | Add measurement semantics | Add optional unit/type/shape/channel metadata and richer result access with timestamp/quality and optional uncertainty/calibration references. Existing value-only reads and arbitrary values remain supported; serialization is explicit rather than imposed on all values. Reuse existing metadata concepts where possible. |
| 6 / TU-006 | Adapt VISA and document operation constraints | Align the adapter with approved lifecycle/operation contracts; characterize timeout units before migration. Specify blocking, concurrency, interruption and error behavior, retain original error causes, and distinguish abort request from confirmed stop. Verify with mocks and `@sim`; no real instrument access, `huo` changes or execution pools. |
| 7 / TU-007 | Extend theory contracts | Add model identity/configuration and semantic descriptions with an opt-in strict evaluation path. Preserve `features` compatibility and fixed-shape ndarray mappings; test model failures and round-trippable supported configuration. Do not duplicate existing mapping shape checks. |
| 8 / TU-008 | Confirm integration and document supported usage | Verify public imports and representative existing `huo` count/scan callers without changing `huo`; document the added APIs and limits, run the required suite and inspect source/package artifacts. Demonstrate virtual and simulated devices plus a numerical model; no claim of universal domain or hardware support. |

## Deferred scope

`mu` run/session/results services, calibration orchestration, scheduler adaptation,
remote APIs and domain-specific taxonomies are not part of this backlog. Runtime
dependency changes, Python support changes and breaking migrations require
separate justification and explicit scope approval.

## Verification and evidence

Use the repository commands in `AGENTS.md`. Future behavior changes need focused
assertions in addition to the existing regression suite. Fixtures must use
synthetic data, temporary paths and simulated hardware. Release records must
identify actual Python/dependency versions and commands, failures, warnings,
skips and unverified platforms; a source reading is not a passing test result.

## Bootstrap integration evidence

Accepted candidate: `9c67f86ca19f33aa729ec4f9f24bff27d8f82442`.
Both Python matrix jobs passed [run 36253102330](https://github.com/hoolheart/softlab/actions/runs/36253102330).
`dev` was fast-forwarded and pushed to that candidate after review, tester,
product-owner and principle gates. No merge commit or main promotion was made.
The completion-record commit follows the same CI-before-integration requirement.

## Release 1 continuation

The user authorized TU-001–TU-008 on 2026-09-27; the proposal wording above
is preparation history. Current requirements are in
[release_1/prd.md](../release_1/prd.md), and task state is tracked in the
[Release 1 board](../release_1/sprint-board.md). TU-001 integrated to `dev`
at `6f39f78066021a048da82364756bff79294da7d9` after both Python jobs passed
[CI 36302785650](https://github.com/hoolheart/softlab/actions/runs/36302785650).
TU-002–TU-008 remain pending their serial gates.
