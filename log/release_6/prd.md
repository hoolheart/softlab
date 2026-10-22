# PRD — M01-W03: explainable stored result

Owner: sw-camille (Product Owner). Date: 2026-10-22 Asia/Shanghai.
Status: **ACTIVATED — implementation authorized; execution gates pending**.
Baseline: dev `410bc16`; setup branch: `codex/m01-w03-setup`.

## Goal, authorization and scope

The user authorized starting and finishing the third release of the first
milestone. This activates **M01-W03** from
[the milestone plan](../../plan/milestone-01.md), using unused process namespace
**release_6**, not historical Release 3 or a package version. W02 is
[product accepted](../release_5/acceptance.md), with final records integrated at
dev410bc16 and actual main promotion9656b27. Earlier records describing promotion
as pending are historical; this work does not reopen that accepted release.

A researcher reopening a completed thermal experiment in another Python process
must recover enough typed context to explain its observations and repeat its
declared configuration. Deliver a bounded, explicitly versioned reference
manifest for the existing [example](../../examples/thermal_experiment.py) and
[guide](../../docs/user-guide/thermal-experiment.md), using existing persistence
primitives. Model/procedure configuration, units, source/software identity,
clock/delay interpretation, outcome and run identity are required. The Architect
reviews backend behavior before choosing an explicit JSON-text metadata envelope
or separate artifact; this PRD does not select an implementation or API surface.

Work remains serial under [AGENTS.md](../../AGENTS.md) and
[principles.md](../../principles.md): requirements → Tester plan/Developer review
→ Designer design/Architect review → implementation → independent review →
Tester verification → principles → exact candidate CI → dev integration.
Release PO acceptance precedes main promotion. Architect owns decomposition.
No future gate is certified by requirements activation.

## Required manifest meaning and supported values

The manifest must identify the thermal model/procedure and record their complete
declared configuration: initial/ambient temperature, heat capacity, conductance,
step interval, heater command and requested point count, together with units and
the educational explicit-Euler model's stated validity limits. Identify ordered
record columns/units and explain acquisition time, separate wall timestamp,
coordinator boundary, observation source boundary and previous-boundary delay.
Preserve the W01 numerical fixture and W02 lifecycle contracts.

Use a closed, documented schema with an explicit schema identifier/version.
Supported values are JSON-compatible null, booleans, strings, integers, finite
real numbers, lists and objects with string keys, only where declared by that
schema. Distinguish booleans from integer point counts; decode declared fields
to their documented Python scalar/container types without arbitrary object
construction. No NaN/infinity, arbitrary Python objects, nonstring object keys
or implicit stringification of unsupported values. Exact schema keys, nesting,
encoding and optional fields belong to reviewed detailed design, not an
arbitrary user-metadata extension point.

Source/software identity must identify softlab version, Python version and the
reference source identity. If Git revision and worktree state can be established,
record their truthful values; a dirty source cannot claim a clean revision as its
complete identity. Git absence/unavailable identity must be explicitly represented
as unknown/unavailable with reason or equivalent documented meaning, without
inventing a commit or failing ordinary installed-source use solely for missing
Git. Any source digest must name what bytes it covers. Do not imply environment
locking, physical calibration, or complete dependency reproducibility.

Stored outcome is a declared snapshot at a named lifecycle stage, not a prediction
of later operations. It must describe completed acquisition, actual requested/
completed counts and final committed boundary. If saved before reload, plot or
cleanup, it must not claim those later operations succeeded; current in-memory
RunResult remains authoritative for those outcomes. The guide must explain that
verified stored acquisition may coexist with later unsuccessful overall execution.
W02 cancelled/failed acquisition remains in-memory only with not-attempted partial
persistence/plot; no new durable failure/cancellation manifest or resume is required.

## Acceptance criteria

All criteria are Must. Independent Tester owns executable assertions/results;
Developer reviews implementability, Designer owns design/code review, Architect
reviews design. Only the owning reviewer/Tester closes their findings.

| ID | User need / behavior | Verifiable acceptance condition |
| --- | --- | --- |
| AC-01 | Explain the declared experiment. | A successful stored run includes every required model/procedure/configuration/unit/source/software/timing/outcome/run-ID field above, with schema identity/version and documented types/meaning. No required context depends solely on live globals or an original in-memory group. |
| AC-02 | Reload typed context independently. | A newly started Python process opens delivered output through the documented inspection path and reconstructs every required field with documented types and values, without the original model/process/result. It also reads the associated numeric record. |
| AC-03 | Trust relationships to stored data. | Reload verifies manifest run ID equals stored DataGroup identity; requested/completed counts, final boundary, ordered column names/units and timing interpretation agree with actual samples. Mismatched identity/count/schema/timing evidence is visibly rejected, not silently accepted or repaired. |
| AC-04 | Distinguish changed configurations and runs. | Two fresh default runs have distinct run IDs with equal declared configurations/numeric traces; a valid changed configuration is recovered distinctly and agrees with its own stored trace. Wall time/run identity are excluded from deterministic numeric comparison. |
| AC-05 | Reject unsupported or malformed metadata visibly. | Unsupported values/types, nonfinite numbers, malformed encoded content and invalid/missing required fields fail through documented diagnostics rather than stringification, object construction, fallback defaults or successful completion. Validate supplied manifest/configuration inputs before persistence when possible; never overwrite existing output. |
| AC-06 | Handle absent and incompatible versions honestly. | Documentation and executed assertions distinguish a legacy run with no W03 manifest from corrupt present metadata and unsupported schema/version. Legacy numeric data remains inspectable with explicitly unavailable context; malformed/present-invalid or unsupported manifests cannot be presented as fully understood W03 results. No automatic migration is implied. |
| AC-07 | Report truthful source identity. | Clean/dirty/Git-unavailable scenarios have deterministic tested dispositions. Captured available revision/state or source digest reflects its declared source; unavailable identity is explicit, not fabricated. Fresh-process inspection preserves the originally captured identity rather than substituting the reader's checkout/environment. |
| AC-08 | Preserve lifecycle snapshot meaning. | Normal and injected persistence/reload/plot/cleanup failures keep W02 stage outcomes truthful. Stored metadata never asserts success for unexecuted later stages. A later plot/cleanup failure can leave verified acquisition data, while overall execution remains unsuccessful and diagnostics identify that distinction. |
| AC-09 | Preserve interruption, repetition and compatibility. | W01 successful run/schema/delay/UUID/plot/collision and W02 early/mid/late stop, model/sample failure, partial rows, acknowledgment, cleanup, explicit reset and fresh-repeat assertions remain green. Legacy HDF5 data/metadata round trips remain unchanged; no library/backend format contract is silently altered. |
| AC-10 | Keep delivered artifacts mutually understandable and safe. | Fresh-path delivery/reload checks expose missing, mismatched or incomplete chosen envelope/artifact and cannot announce a fully verified result incorrectly. Fault injection covers the selected representation's write/reload failure paths. Existing source files/data and borrowed resources remain unchanged; owned temporary/backend/figure resources receive cleanup with visible obstruction diagnostics. Atomic multi-artifact durability is not inferred. |
| AC-11 | Reproduce inspection from the guide. | One documented root headless command produces the complete result; independent Tester follows a fresh-process inspection command showing typed manifest and samples, changed configuration and missing/version policy. Guide explains scope, source uncertainty, snapshot timing, W02 partials and legacy/backend limitations. |
| AC-12 | Finish the bounded release with evidence. | Approved plans/design, owned issue/failure closure, full unittest/compile/import checks, actual architecture/guide updates, all-eleven principle inspections and exact configured Python3.9/3.13 candidate CI precede dev integration; PO accepts the mapped evidence before main promotion. Record actual environment/warnings/skips/limits and clean scope/diff evidence. |

## Constraints, debt and exclusions

Preserve successful `run` return and existing example inspection compatibility
where feasible; characterize callers before any intentional example-local
extension. Legacy no-manifest loading must not silently acquire W03 context from
the reader's current defaults. Do not reinterpret arbitrary Parameter values as
JSON-only. The new bounded encoding applies only to the explicit manifest.

Carry forward unfixed R4-BASE-SCHED-01, R4-BASE-META-01 and R4-BASE-HDF5-01 with
their characterized private executor, explicit text hydration and copy-only
loading measures. OBS-004/005/006 and DEFECT-2 remain tracked historical debt.
Preserve source-integrity checks and synthetic temporary fixtures. Any newly
demonstrated prerequisite needs Architect/PO scope review before expansion;
existing debt and CI platform notices cannot disappear through a zero-warning
claim. Named warning-as-error checks do not imply unconfigured lint or all-platform
certification. Reviewer/Tester issue ownership remains unchanged.

Excluded: W04 adapters/physical mapping, reusable M03 provenance framework,
graph/model/arbitrary object serializer, backend-wide metadata repair or migration,
SQLite parity, durable partial persistence/recovery/resume, automatic retry,
blocking-call preemption or physical stop, real instruments/firmware, UI/Figma,
new dependencies, supported-Python changes and package version/tag/publication.
No hardware is needed or authorized. Weekly W03 acceptance does not accept W04
or the entire M01 milestone.

Every completed process update is immediately committed/pushed with `docs(log):`
before handoff to the user-trusted origin. No fixed calendar deadline is promised.
