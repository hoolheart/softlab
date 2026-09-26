# Release 1 requirements: compatible `tu` foundations

## Authorization and goal

On 2026-09-27 the user authorized continuing the remaining TU-001–TU-008
backlog after workflow preparation, including the previously confirmed push
scope. This release makes the existing device and numerical-model abstractions
easier to inspect and extend without breaking their established callers.
The historical preparation record remains in `log/release_0/`.

Researchers must retain convenient programmer-driven experiments; driver
authors must be able to describe optional behavior without implementing a
universal instrument hierarchy. This release does not claim support for every
scientific domain or instrument.

## User stories, functional requirements and acceptance

All eight items are must-have release outcomes, in the existing backlog order.
These are requirements, not API signatures or detailed designs.

| Task | User need | Verifiable acceptance criteria |
| --- | --- | --- |
| TU-001 | As a maintainer, know which existing behavior extensions must preserve. | A compatibility matrix links public behavior to callers and assertions: call/get/set, access permissions, validation/codec/hook order, initialization, proxies, snapshots, device paths/parent links, builders/default station, VISA construction/commands/timeouts/errors, and model/mapping behavior. Inspect `huo`, notebooks and examples. Characterization assertions run against unchanged production code. Record existing defects separately from intended contracts; no production behavior change. |
| TU-002 | As a client author, inspect an experimental setup without operating it. | Opt-in versioned parameter/device/station descriptions serialize using documented metadata rules and cause no device I/O. Unsupported metadata has a defined outcome. Existing snapshots, arbitrary runtime values and import paths remain compatible. |
| TU-003 | As an experiment author, invoke commands explicitly and recognize their side effects. | An explicit operation path is available and side-effect semantics are discoverable. Legacy `VisaCommand` invocation, access permissions and execution counts remain compatible. Description lookup does not execute commands. |
| TU-004 | As a device author, express optional lifecycle and capabilities safely. | Virtual devices require no meaningless connection operations. Readiness and ownership are explicit. Repeated cleanup, failed preparation/initialization and shared/borrowed resource handling are verified. Unsupported capabilities are detectable; borrowed resources are not released by a non-owner. |
| TU-005 | As a measurement consumer, understand a reading while retaining simple value access. | Optional unit/type/shape/channel descriptions and richer reading results expose acquisition time and quality, with optional uncertainty/calibration reference. Existing value-only reads and arbitrary values continue to work. Serialization limits are explicit rather than imposed on all values. |
| TU-006 | As a VISA driver user, understand resource and operation outcomes. | VISA adopts the approved optional lifecycle/operation contracts compatibly. Timeout units/defaults, blocking, concurrency, interruption and errors are documented and verified with mocks and `@sim`. Original causes remain available. Abort request and confirmed physical stop are not conflated. |
| TU-007 | As a model consumer, inspect configuration and observe evaluation failure explicitly. | Model identity, supported serializable configuration and semantic descriptions are available. An opt-in strict evaluation path exposes errors. Existing `features` behavior, fixed-shape ndarray mappings and public imports remain compatible; existing shape checks are retained rather than duplicated. Supported configuration round trips are verified. |
| TU-008 | As a library user, adopt the extensions through verified examples and documentation. | Public imports, representative existing `huo` count/scan usage, virtual/simulated device usage and a numerical model are verified. Added APIs, limitations and migration-free usage are documented. Required regression/compile/import checks and package-artifact inspection have recorded results. Final architecture describes implemented behavior and release acceptance references actual evidence. |

## User journeys

1. Existing user imports the same APIs, constructs parameters/devices, reads or
   writes values and runs existing experiments with unchanged observable behavior.
2. New client obtains a portable description, discovers supported optional
   behavior, explicitly performs an operation/read and handles its stated outcome.
3. Resource owner prepares and uses an eligible device, then releases owned
   resources after success or failure without disrupting borrowed resources.
4. Model user describes/configures a numerical model and chooses explicit error
   reporting while existing callers retain their current evaluation behavior.

## Compatibility and non-functional requirements

- Preserve existing parameter return types, initialization, permissions,
  validation, codecs, hook ordering, proxies, snapshot semantics, command side
  effects and model/mapping behavior unless a separate breaking change is approved.
- Keep existing five-element module responsibilities and public import paths.
  Production changes are confined to `tu`; inspect and test callers without
  editing `huo` or `mu` production behavior.
- Do not add runtime dependencies or change supported Python versions within this
  scope. Record actual local and CI environments; do not infer version coverage.
- Description-only operations perform no hardware access. Routine verification
  uses synthetic data, temporary files, mocks and VISA simulation; no real hardware.
- Preserve lightweight virtual-device use. No performance guarantee is invented;
  new inspection must not require acquisition or opening a connection.
- Public additions have English docstrings describing arguments, returns, errors
  and side effects; documentation distinguishes opt-in behavior from legacy paths.

## Edge cases and errors

Acceptance must address denied reads/writes, failed validation/codecs/hooks,
unsupported metadata, arbitrary nonserializable values, nested device lookup,
repeated cleanup, failed initialization, borrowed/shared handles, communication
timeouts, unsupported capabilities and model evaluation/shape errors. Existing
defects are recorded with explicit disposition and never silently promoted into
permanent desired behavior. No claim that cancelling software stops equipment.

## Priorities and exclusions

Must-have: TU-001 establishes the compatibility baseline before extension work;
TU-002–TU-007 add the compatible contracts; TU-008 verifies their combined usage.
Should-have work may refine documentation or diagnostics within those criteria,
but must not expand the approved scope. Nice-to-have remote access, domain-specific
taxonomies, streaming frameworks and universal transport support are deferred.
`mu` services, scheduling/execution pools/retries, persistence mechanisms,
calibration orchestration and UI are excluded. UI gates are not applicable.

## Acceptance status and evidence requirements

Status: requirements recorded; implementation and release acceptance **pending**.
Each task requires the documented serial test/design/review/testing/principle
gates before integration into `dev`, including CI for the candidate commit.
TU-001 specifically passes only when its matrix and characterization coverage
are independently reviewed, required checks have actual results, production
files remain unchanged and baseline defects/limitations are explicit.
Release PASS requires evidence for every criterion above; this document grants
no unrun gate a passing status and does not itself authorize promotion to `main`.
