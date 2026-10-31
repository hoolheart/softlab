# M03 — Proposed bounded reproducible run record

Status: **PROPOSED — planning only**. M02 is CLOSED in the authoritative local
`main` record at commit `e827e7f` (`git show e827e7f:log/release_11/closure.md`);
this plan does not implement or accept M03. Four serial increments are planning
envelopes, not calendar promises or package publications. Assign an unused
`log/release_x/` namespace and agree a bounded PRD at each kickoff. If capacity,
compatibility or evidence is insufficient, narrow scope through Product and
Architect review or move the exit date.

## Product outcome and existing evidence

A maintainer can reopen a supported synthetic result after its original session,
understand the recorded procedure, setup/model declaration, units, software and
outcome snapshot, and compare a clean-session rerun to the original samples.
The outcome is explainability and a bounded reproducibility check, not a claim
that recorded versions recreate an environment, identify every influence, prove
physical accuracy or reproduce arbitrary experiments. Use the thermal reference
and a distinct bounded count client as evidence before proposing any shared
record API; a contract double alone does not prove reusable product value. The
current signal-monitor client is in-memory only and does not persist or
reconstruct its setup. W01 must identify who constructs its stored record and
rerun inputs, selected backend, field origins and an independent sample oracle
before W03/W04 acceptance; none of those capabilities is assumed today. W01
inventories reproducible inputs and observable fields for each client; each later
PRD/design freezes independent expected sample/timing relationships, tolerances,
excluded fields and a separate-process rerun procedure before implementation.
Do not substitute unqualified self-comparison for an independent oracle.

M02 provides a serial count runner with owned cleanup and in-memory run UUID;
the thermal group UUID and its stored manifest identity are distinct. Existing
thermal v1/v2 JSON manifests are example-specific: strict validation covers
configuration, model, timing, columns, source and a completed-acquisition
snapshot **before** persistence; storage, plotting, cleanup and overall outcome
are then not yet established. `DataGroup` metadata uses existing backend
semantics and HDF5 string storage, not lossless arbitrary Python values. The
constructor metadata defect R4-BASE-META-01, the `set_meta(None)` implementation/documentation mismatch, and destructive legacy HDF5 reader
R4-BASE-HDF5-01 remain tracked; current example uses explicit metadata hydration
and disposable-copy reads. Current SQLite storage does not establish arbitrary manifest persistence; do not treat the two backends as equivalent. `Station`/`Device`/`Parameter` descriptions are structural and do not supply simulation graphs or runtime values. Characterize actual HDF5/SQLite callers, versions,
missing metadata and clean-process inspection before choosing a persistence
boundary. W01 must use disposable HDF5 copies and independently inspect on-disk
metadata, not rely solely on a returned `DataGroup`; if no safe read path exists,
record a blocker and make a scoped decision before claiming support. Do not silently change existing thermal v1/v2 interpretation or
`Parameter` value types.

## Product authority and scoped decisions

The roadmap calls for a versioned bounded manifest connecting procedure
configuration, setup/model declarations, units, software/environment identity,
outcome and samples. W01 must determine which fields the two actual clients can
truthfully provide and which data can be reopened safely, then compare extending
the existing example envelope, an additive shared text envelope, and retaining
separate client-specific records. If demonstrated use does not justify a common
schema, Product Owner and Architect must revise W02–W04 deliverables and the
milestone exit before W02 starts, explicitly identifying any separate supported
records and comparisons; rejection is not an automatic acceptance or permission
to proceed with contradictory common-schema requirements. Any chosen format must state its namespace, supported value types,
size/depth limits if needed, required/optional/unknown-field policy, version and
compatibility behavior, missing versus explicit null semantics, unsupported
value errors, and serialization/reload semantics before implementation.

Record identity and stage precisely: distinguish in-memory execution UUID from
persistent group UUID, acquisition snapshot from later persistence/plot/cleanup
outcomes, and unavailable source/environment facts from verified values. Never
record future success at a pre-persistence snapshot. Decide explicitly whether
and how later outcome updates are supported; atomic publication, audit logs,
partial-result persistence and crash recovery are not assumed. W01 and W03 design
must enumerate supported stages, unavailable reasons and whether later outcomes
are absent or updated. Before implementation define and test serialization, save,
close/disconnect and malformed/unsupported reload failures: specify which original
bytes remain intact and how independently observable primary and cleanup
diagnostics survive. This does not require persisting failed runs. No automatic
collection of secrets, host inventories or dependency lock is implicit.

Assess deferred simulation-description needs against actual clients: existing
station descriptions omit simulation nodes. Document the minimum declarative
facts needed for this specific record, or defer with rationale; no model graph
serialization, arbitrary callable/object serialization or hardware identity is
implied. Preserve original stored data on all routine reads using temporary
synthetic data and currently required safe copies. A storage/backend repair or
schema migration requires its own scoped decision and compatibility evidence.

## Proposed serial increments

Each sub-release has its own bounded PRD, evidence and unused log namespace;
close it under the project release checklist before starting the next. One active
task at a time. W04 additionally accepts M03. Dates and boundaries may be
revised through product review if safe implementation cannot be split.

| Increment | Deliverable and decision | Acceptance and dependency |
| --- | --- | --- |
| M03-W01 — Characterize and decide | Map current thermal v1/v2, DataGroup and actual backend callers, sample/schema relations, lifecycle stages, source availability and a distinct count client. Create assertion-based legacy and fault characterization; select or reject a minimal common record boundary with ownership and compatibility rationale. Assess baseline debt and simulation-description need. | Independent review accepts source/round-trip evidence and the supported or explicitly deferred design; no unsupported shared API is assumed. Depends on M02 closure. |
| M03-W02 — Bounded schema and encoding | Implement only the accepted smallest schema/codec and construction boundary for the supported clients, including explicit required and missing-field policies, identity, units, source availability and stage-qualified outcome. Preserve existing APIs and formats unless an expressly reviewed compatibility decision states otherwise. | Independent tests cover valid values, deterministic round-trip, missing/null/unknown fields, duplicate keys, nonfinite or unsupported values, version rejection and side-effect-free failure. Existing stored v1/v2 cases remain readable as specified. Depends on W01 decision. |
| M03-W03 — Persistence and clean-session inspection | Integrate the accepted envelope with the chosen existing storage path(s), retain sample/column/identity relationships, expose an inspectable stored outcome snapshot and document ownership and failure behavior. Do not broaden backend support or claim atomicity without evidence. | Synthetic temporary-file round-trip and fresh-process inspection verify records, schema, units, identity and samples; malformed/unsupported context fails visibly and legacy absence is distinct. Faults preserve originals and diagnostics; unsupported backend behavior is explicit. Depends on W02. |
| M03-W04 — Reference comparison and acceptance | Integrate both thermal modes and the second client only within the approved boundary; update guide and API documentation. Independently repeat the documented workflow in a fresh environment and a clean process, compare recorded configuration and deterministic sample relationships, and complete M03 acceptance. | Existing v1/v2 and legacy tests pass, both supported clients demonstrate the selected scope, independent reruns match specified numeric tolerances excluding timestamps/UUIDs, and required review, candidate CI, promotion and closure gates pass. Depends on W03. |

## Required evidence and compatibility matrix

1. **Stored context:** establish explicit field-by-field origin, units, type,
   required/optional/unknown handling, size policy and availability reasons.
   Assert sample order, columns, counts, acquisition boundary and persistent
   identity relationships. An unavailable value must not masquerade as an
   empty, zero or successful value. Keep actual outcome stage visible.
2. **Codec and versioning:** use an independently reviewed allowlist of bounded
   values and reject unsupported objects without `str`/`repr` fallback. Test
   booleans versus numbers, finite numeric values, duplicate keys, malformed
   input, missing required fields, explicit nulls, extra fields and unsupported
   versions. Define whether unknown fields are rejected or preserved; do not
   infer cross-version migration or silent data loss.
3. **Legacy and storage:** characterize and test current thermal v1 coordinated,
   v2 direct, and absent-legacy context; invalid present metadata must not be
   mistaken for legacy absence. Specify exactly which backend(s) are supported;
   tests of one do not certify another. Use safe copied inputs, temporary files
   and synthetic values; verify bytes/metadata needed to show original data was
   not modified. Existing defects remain tracked unless separately scoped.
4. **Lifecycle and failure:** preserve primary and independent cleanup
   diagnostics and distinguish completed, failed and cancelled work where the
   supported format honestly records it. A pre-save snapshot cannot certify save,
   plotting or cleanup. If failure/partial persistence is not supported, reject or
   document it and leave durable recovery to M07. Do not claim software stop
   proves physical termination, rollback or successful cleanup of unobserved work.
5. **Clean-session comparison:** record setup commands, interpreter and dependency
   versions actually observed, warnings and limitations. Save, reopen and inspect
   in a separate process; independently rerun with the same supported inputs and
   compare declared numeric values and sample/timing relations under explicit
   tolerances. Different UUIDs and wall timestamps are expected. Environment
   metadata is evidence of what was observed, not a lock or a promise of replay.

## Debt, exclusions and finish line

R4-BASE-SCHED-01/F-01 and generic timeout F-02 remain avoided by the accepted
M02 seam; R4-BASE-META-01, R4-BASE-HDF5-01, OBS-004/005/006 and DEFECT-2 retain
their tracked dispositions. Assess whether each affects the chosen path; a
blocking defect needs a separately reviewed disposition rather than a silent
waiver or opportunistic repair. Record actual pandas, GitHub Actions runtime,
Matplotlib/fontconfig and environment warnings only where observed.

No hardware/VISA qualification, device access, new dependency, generalized
provenance platform, secret capture, arbitrary serialization, broad backend
rewrite, forced termination, automatic retry, durable partial recovery or
unrequested migration is authorized by this planning document. M04 owns physical
adapter contracts and access decisions, M05 scan extensions and M07 recovery.

At kickoff define role ownership and finish conditions using the
[release checklist](../log/templates/release-checklist.md): independently
reviewed behavioral changes and tester-owned failure closure, product acceptance,
applicable principle inspection, full relevant unittest/compile/import checks,
frozen exact-candidate CI before `dev`, accepted promotion to `main`, verified
promoted-state checks and clean worktree. State actual environment and warnings;
reuse evidence only for unchanged content. One authoritative closure summary
links each increment, revisions/CI URLs, debt and limitations. If a mandatory gate
fails or is unavailable, leave the affected release and M03 OPEN; planning or
local tests alone do not constitute acceptance. No implementation is authorized
by this document.
