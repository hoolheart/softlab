# Release 12 (M03 Phase 1) — Joint boundary decision

Owner: Software Architect (sw-jerry), with Product Owner (sw-camille)
concurrence | 2026-11-01 Asia/Shanghai
Branch: `codex/m03-p1-characterize`
Status: **JOINT DECISION RECORDED — PRODUCT OWNER CONCURRENCE RECORDED BELOW;
PHASE 2 REQUIRES ITS OWN REVIEWED DETAILED DESIGN AND TEST GATES**

Inputs: [PRD](../prd.md), [M03 plan](../../../plan/milestone-03.md),
[executed characterization results](../test/characterization-results.md),
[architecture assessment](../reviews/p1-architecture-assessment.md)
(commit `d2180a0`), and [product assessment](../reviews/p1-product-assessment.md)
(commit `18f9c1e`).

This document records the formal joint decision boundary for M03-AC01. The two
independent assessments selected the same option, but the Product Owner must
explicitly countersign this document before it becomes the approved Phase 2
input. It is not a Phase 2 detailed-design approval, storage implementation
authorization, or release acceptance. Terms below describe semantic fields;
literal JSON keys, the new namespace string, and numeric limits remain Phase 2
schema decisions unless an existing literal value is quoted.

## 1. Decision

**Select option (a): one minimal common record envelope, implemented in later
phases as an additive, namespaced JSON-text record on HDF5, with closed
client-owned payloads.** The common record supplements the existing stored
`DataGroup`/`DataRecord`; it does not replace samples, reinterpret thermal
manifests, or establish a general provenance framework.

New thermal runs within the supported workflow will eventually dual-write the
unchanged legacy manifest and this additive record. Existing thermal files
remain legacy records under their current readers. The signal-monitor client
will use the same common outer contract only after its client-owned persistence
and inspection path exists; this decision does not claim that it persists today.

### 1.1 Exact common-envelope boundary

The shared envelope owns only the following common semantics:

| Area | Included in the common envelope | Boundary and required relation |
| --- | --- | --- |
| Namespace, profile, and version | A new additive namespace, its independent format version, and one recognized client-profile identifier for thermal or signal monitor. | The namespace and version are independent of `softlab.thermal-reference` and thermal manifest versions 1/2. The current common version and each client payload are closed: unknown fields, profiles, and unsupported versions are errors rather than silently ignored extensions. |
| Execution identity | The bounded execution UUID, separately captured from `CountResult.run_id` for a new common record. | It must not be copied from, substituted for, or used to reconstruct the persistent group UUID. Thermal must retain the value currently visible at its `after_count` seam; signal must retain the result it already returns. Legacy records must not acquire a fabricated execution UUID. |
| Persistent group identity | The canonical stored group UUID and its relation to the stored record. | It must equal `DataGroup.id`, the HDF5 group name, and the on-disk `id` attribute. Existing thermal manifest `run_id` is this **group** identity, not the bounded execution identity, and must not be relabelled. |
| Client declaration | A bounded, closed payload identified by the common client profile. | The common layer validates the payload boundary and profile dispatch; the named client owns and validates its internal semantics. It is not an arbitrary metadata bag. |
| Sample schema and ordering | Ordered stored-column declarations, a truthful unit declaration for every stored column, row-count relations, and the association between those declarations and the group's ordered `DataRecord` samples. | Envelope order must match stored column order and sample row order. Declared/requested/completed counts must agree with the acquisition snapshot and actual stored rows as applicable. Unitless values must be declared truthfully, not given invented units or blank success-like placeholders. Samples remain in record datasets and are not duplicated into an unbounded JSON blob. |
| Software, source, and environment availability | Observed softlab/Python identity and bounded source identity/availability facts, with explicit unavailable state and a bounded reason where applicable. Additional environment observations are allowed only where the reviewed profile defines them. | Record only facts actually captured for that client and run. Uncaptured facts are explicitly unavailable, never copied from another client or inferred as success. No dependency lock, environment replay guarantee, secret capture, or host inventory is implied. |
| Acquisition outcome | Exactly the `completed_acquisition_before_persistence` snapshot with acquisition `completed`, plus the requested/completed row and validated acquisition-boundary evidence needed by the client profile. | At this snapshot, `persistence`, `plotting`, `cleanup`, and `overall` are `not_yet_established`. The record cannot claim their later success, contain a later outcome history, or represent a failed, cancelled, or incomplete acquisition as a supported completed run. |

The common envelope also owns the shared required/optional, absent/null,
unavailable, malformed, duplicate, nonfinite, unknown-field, and version-error
policies specified in section 6. Explicit null has only a meaning assigned by
the Phase 2 contract; it is not a universal unknown marker.

### 1.2 Closed client-owned payloads

**Thermal client.** Its payload owns exact `ThermalConfig` declarations and
their units, thermal procedure, model/equation/method/validity semantics, the
ordered six-column thermal semantics, coordinated/direct clock and delay policy
(including the v1/v2 source-tick distinction), thermal source observations, and
the inputs needed for independent Euler comparison. The additive payload does
not weaken or replace the existing closed eleven-field v1/v2 manifest or its
validation. Legacy thermal interpretation and absent/invalid behavior remain
unchanged.

**Signal-monitor client.** Its payload owns `points`, the read-policy offsets 5
and 2, non-consumptive `tick + offset` read semantics, scheduler/loop ownership,
truthful source-column units/timing, and the declaration that
`difference = signal - reference` is caller-derived. Stored source columns are
`timestamp`, `signal`, and `reference`; wall-clock time is not relabelled as
deterministic simulation time, and genuinely unitless channels remain unitless.
`difference` must not be described as an acquired or existing stored source
column. If a later phase stores it, it must carry explicit derived semantics;
otherwise it is independently recomputed from the stored source samples.

No station/simulation graph, arbitrary callable or object, arbitrary experiment
description, generic extension map, or unbounded provenance payload belongs to
either profile.

## 2. Supported scope statement

### Supported clients

1. Thermal coordinated acquisition, preserving legacy manifest v1 behavior.
2. Thermal direct acquisition, preserving legacy manifest v2 behavior.
3. `examples/signal_monitor.py` as the second client, including its canonical
   `points=3` comparison case, once its owned stored-record construction and
   inspection workflow is implemented and reviewed.

### Supported backend and ownership

**HDF5 is the only supported backend.** The envelope is encoded and validated
as strict JSON text before persistence, stored under one new shared metadata
key, and kept alongside the existing record datasets. Already-encoded text is
preserved by the characterized HDF5 writer; this is not a claim that arbitrary
Python HDF5 metadata round-trips losslessly.

The supported write path uses an owned `HDF5DataBackend`, not generic `io.py`
helpers. It constructs the record only after the completed-acquisition gate,
adds string-keyed text with `set_meta`, and does not use `DataGroup(meta=...)`
or `set_meta(key, None)` for envelope absence/null. Connect/save status and
backend errors must be checked, owned resources disconnected, and primary and
cleanup diagnostics retained independently. Only a new run record within the
reviewed output-ownership scope is supported; same-ID deletion/recreation is
not atomic publication.

The supported read/inspection path must preserve the original and use this
discipline:

1. Hash and byte-copy the source to owned scratch space; all legacy backend
   reads occur only on that disposable copy.
2. Use `h5py.File(copy, 'r')` to enumerate groups when needed, preflight
   reserved attributes and shared-attribute presence/type, and capture required
   text attributes before any legacy load.
3. Perform at most one legacy backend load on that copy, using `str(group_id)`;
   explicitly hydrate the shared key and applicable client-legacy keys, then
   decode, validate, and cross-check identity, ordered schema/units, row counts,
   and samples.
4. Disconnect and clean up scratch resources with separate diagnostics, and
   verify that original bytes remain unchanged.

An unmodified thermal loader, `io.reload_group`, and `io.load_groups` are not
supported substitutes. In particular, thermal's existing hydration seam does
not retain arbitrary custom keys, so the additive key must be carried explicitly
by the supported Phase 3 inspection path.

### Explicit NON-support

This decision does **not** support or imply:

- SQLite record persistence unless future evidence and a separately reviewed
  storage/schema decision establish it;
- generic `io.py` reload/list certification, arbitrary `DataGroup` metadata
  round-trip, or plain/repeated HDF5 loads of an original file;
- atomic publication, in-place mutation, rollback, retry, or transactional
  guarantees;
- durable failed, cancelled, or partial-run persistence;
- later-outcome updates, outcome audit history, or rewriting the acquisition
  snapshot after save/plot/cleanup;
- partial recovery, crash recovery, or automatic resume;
- legacy migration, backend parity, environment recreation, real-instrument
  qualification, or repairs outside the explicitly reviewed seams.

Selecting any excluded path requires a new scoped decision; it is not an
implementation detail within this boundary.

## 3. Evidence basis

This decision rests on the executed evidence in
[characterization-results.md](../test/characterization-results.md), whose
execution record reports 405 passing tests plus passing compile/import checks.
No runtime experiment was repeated for this decision document.

| Characterized fact | Results area | Consequence for this decision |
| --- | --- | --- |
| Thermal v1/v2 manifests are closed and strictly validated; malformed, duplicate, nonfinite, unsupported-version, identity, schema, timing, and outcome drift fail visibly. Invalid-present is distinct from absent. | C1.1–C1.3 | Preserve the legacy codec and reuse its demonstrated strict-validation techniques without weakening its closed contract. |
| The only supported stored outcome is a completed-acquisition snapshot before persistence; later stages are not yet established. | C1.4 | Common outcome semantics must remain acquisition-stage-qualified and immutable in M03. |
| Execution UUID and persistent group UUID are distinct; the example-local thermal result does not retain the former. | C1.5, C5a | New common records require separate identity capture; legacy group identity cannot be relabelled or backfilled as execution identity. |
| HDF5 backend metadata values become text, while already-encoded JSON text and non-ASCII text survive; group/sample/schema facts are independently inspectable on disk. | C2.3, C3 | One pre-encoded JSON-text attribute plus existing datasets is sufficient; arbitrary Python metadata persistence is neither needed nor claimed. |
| Safe read-only inspection succeeded on disposable byte-copies with original-byte hashes preserved; plain HDF5 loading is destructive. | C2.3, C3, C6 | HDF5 is feasible only with owned writes and copy/preflight/hydration reads. |
| SQLite lacks arbitrary envelope storage and loses reconstructed metadata through the current constructor path; generic io reload/list defects affect both backends. | C2.4, C4 | Exclude SQLite and generic io helpers rather than inferring backend parity or adding an unreviewed repair. |
| Thermal truthfully supplies bounded configuration/model/procedure, ordered units/schema, source/software availability, and an independent Euler oracle. | C5a | These facts support a common outer record with thermal semantics kept in a closed payload. |
| Signal monitor is currently in-memory only, has deterministic bounded inputs and an independent tick/offset oracle, and distinguishes stored source columns from caller-derived `difference`. | C5b | A second real client justifies the common contract, but requires future client-owned persistence and truthful signal-specific declarations. |
| Backend ownership outside the example is limited to `io.py` and tests; synthetic fixtures, resource cleanup, and original-byte protection were verified. | C4, C6 | A narrow owned HDF5 workflow is justified without a broad backend rewrite. |

### Rejected alternatives

- **Extend the example envelope as-is.** Rejected because
  `softlab.thermal-reference` is deliberately closed around eleven thermal
  fields, thermal model/procedure semantics, six ordered columns, and
  adapter-specific v1/v2 timing. Adding signal data would either change legacy
  validation or invent thermal semantics for a different client. The additive
  record preserves compatibility instead.
- **Separate client records only.** Rejected as unnecessary for these two
  clients, not as technically impossible. Both demonstrate the same identity,
  ordered-sample, availability, stage, strict-codec, and inspection needs;
  separate outer codecs would duplicate them. Closed client payloads retain the
  actual differences. If Phase 2 cannot keep the common outer contract small,
  this remains a fallback requiring a revised joint decision, not silent scope
  drift.
- **Defer for more evidence.** Not selected: C1–C6 characterized every planned
  area, a safe HDF5 inspection route exists, both independent oracles are
  available, and negative SQLite/io evidence is sufficient to exclude those
  paths from this narrow decision.

## 4. Debt and defect dispositions

None of the following is repaired or waived by this decision. A disposition is
non-blocking only while implementation remains inside the stated boundary.

| ID | Disposition | Conditions imposed on later phases |
| --- | --- | --- |
| **D1 — `DataGroup(meta=...)` mutates caller input and crashes for non-string keys** | **Tracked debt; non-blocking.** | Phase 2 construction must create an empty group and add only string-keyed encoded text through `set_meta`; it must not route envelope data through the defective constructor or mutate caller declarations. Hydration must likewise bypass constructor metadata. |
| **D2 — plain SQLite load produces empty metadata** | **Tracked debt and backend exclusion; non-blocking for HDF5-only scope.** | SQLite remains unsupported. Selecting it makes this and the missing envelope storage blocking and requires separately reviewed schema/compatibility evidence. |
| **F-A / R4-BASE-HDF5-01 — plain HDF5 load removes reserved attributes and a second load fails** | **Tracked debt with a mandatory safety constraint; non-blocking only on the disposable-copy path.** | Phase 2 codec/construction must not depend on plain original-file loads. Phase 3 must preflight/capture on a copy, perform at most one legacy load per copy, hydrate explicitly, and prove original-byte preservation and safe repeated inspection using fresh copies/read-only disk access. Plain original loads or repeated loads of one copy are unsupported and block the affected workflow. |
| **F-B — thermal hydration drops custom keys** | **Open finding and required integration work; not a Phase 1 blocker and not a backend repair.** | Phase 2 must keep the additive key and captured-text construction boundary explicit. Phase 3 must capture and restore that key for each supported client; it must not promise arbitrary custom-key hydration or impose thermal's three legacy keys on signal. Without this seam, the common record is not implemented. |
| **F-C — `io.reload_group` fails for UUID IDs on both backends and swallows errors** | **Tracked debt; non-blocking because generic reload is excluded.** | The supported path uses an owned backend and `str(group_id)`. A `None` helper result is not evidence of absence or successful inspection. Any future generic-reload dependency requires a separately scoped repair and review. |
| **F-D — SQLite `load_groups` returns `[]`; HDF5 listing is unsafe after mutation** | **Tracked debt; non-blocking because generic listing is excluded.** | Phase 3 must discover groups through read-only HDF5 keys on a disposable copy with explicit identity validation. Empty generic-helper output must not be interpreted as an empty store. |

The `set_meta(None)` documentation/implementation mismatch also remains tracked.
JSON null stays inside encoded text and must not be implemented through that
method. Existing unrelated debt dispositions remain unchanged.

## 5. Product Owner concurrence

The independent architecture and product assessments provide the basis for this
joint decision, but they do not substitute for explicit concurrence with this
exact boundary.

| Role | Record | State |
| --- | --- | --- |
| Software Architect | sw-jerry; architecture assessment `d2180a0`; this decision drafted 2026-11-01 | **CONCUR — option (a), subject only to the boundary stated here** |
| Product Owner | sw-camille; product assessment `18f9c1e`; countersignature recorded below on 2026-11-01 Asia/Shanghai | **CONCUR — option (a), subject to the boundary and binding Phase 2 constraints recorded here** |

Product Owner concurrence record:

> **RECORDED.** sw-camille countersigned this joint boundary decision on
> 2026-11-01 Asia/Shanghai; see the "Product Owner concurrence" section below.

With the countersignature recorded and pushed, the Phase 2 schema/codec gate is
open **only** to the extent stated here: Phase 2 implementation additionally
requires its own reviewed detailed design and applicable test gates. This
concurrence does not constitute Phase 2 design approval, Phase 3/4 approval, or
M03 release acceptance.

## 6. Binding Phase 2 input

The Phase 2 schema/codec detailed design and implementation **MUST** honor all
of the following:

1. Implement only the additive common JSON-text codec, the two named closed
   client payload validators/construction seams, and the justified bounded
   group-construction seam. Freeze literal namespace/key/profile names, exact
   required/optional fields, conservative encoded size/depth limits, identity
   cross-checks, and unavailable-reason rules in the reviewed Phase 2 design.
2. Use a bounded value allowlist: JSON objects with string keys, arrays,
   strings, booleans, exact integers, finite real numbers, and explicit null
   only where assigned a meaning. Reject arbitrary Python objects and NumPy
   values unless a client constructor explicitly normalizes them to an allowed
   value. Keep samples in datasets, outside the bounded metadata blob.
3. Apply **no implicit stringification**: no `str`/`repr` fallback for
   unsupported values, no silent coercion of booleans/numbers, and no HDF5
   attribute write before successful validation and JSON encoding.
4. Reject visibly and without persistence side effects: malformed JSON,
   duplicate object keys, unsupported values/types, nonfinite numbers (including
   `NaN` and infinities), missing required fields, unknown fields, unsupported
   client profiles, and unsupported common or client-payload versions. Tests
   must cover these negatives as well as deterministic valid round-trips and
   boolean-versus-number behavior.
5. Enforce the section 1 identity, ordered sample-schema/units/row-count, source
   versus derived, and acquisition-only outcome relations. Never populate one
   UUID from the other or claim later-stage success at the stored snapshot.
6. Define required, optional, absent, explicit-null, unavailable, and
   invalid-present behavior explicitly. Optional absence never implies success,
   zero, empty, or not-applicable; unavailable facts require explicit state and
   bounded reasons where applicable. Absence of the whole additive key means
   only “no additive common record”; a present non-text, malformed, invalid, or
   unsupported-version value is an error, never legacy absence.
7. Preserve thermal manifest v1 coordinated and v2 direct readability,
   validation, timing meanings, import paths, and existing APIs. Do not migrate
   old files, change the eleven-field legacy contract, fabricate missing
   execution UUIDs, or treat the additive format version as a thermal v3.
8. Keep the new additive key explicit for the Phase 3 safe
   copy/preflight/hydration path; do not couple codec validity to generic
   `io.py`, original-file plain HDF5 loads, SQLite, or arbitrary metadata
   hydration.
9. Add no dependency or general extension/provenance framework. Use the
   demonstrated standard-library strict JSON techniques and stop for a revised
   joint decision if the two-client common contract cannot remain this small.

These constraints bind Phase 2 only after the section 5 concurrence gate opens;
they do not authorize schema implementation by themselves.

## Product Owner concurrence

**CONCUR — option (a), subject to the boundary and binding Phase 2 constraints
recorded in this decision. No additional product conditions are imposed.**

Recorded by sw-camille, Product Owner, on 2026-11-01 Asia/Shanghai after review
against the product assessment (commit `18f9c1e`) and the PRD's M03-AC01 and
Limits/debt requirements.

The decision preserves the approved product boundary:

- one minimal common envelope with closed thermal and signal-monitor
  client-owned payloads, without establishing a general provenance framework;
- HDF5-only persistence, with thermal coordinated/v1, thermal direct/v2, and
  the signal monitor as the supported clients; signal-monitor support remains
  conditional on its future owned persistence and inspection path and is not a
  claim that signal persistence exists today;
- only the `completed_acquisition_before_persistence` snapshot with acquisition
  `completed`, while persistence, plotting, cleanup, and overall remain
  `not_yet_established`; no later-outcome updates or outcome history, and no
  durable failed, cancelled, or partial-run persistence;
- SQLite, atomic publication or transactional guarantees, recovery/resume,
  generic metadata/`io.py` guarantees, and the other listed exclusions remain
  unsupported;
- D1, D2, F-A, F-B, F-C, and F-D retain the stated tracked-debt, open-finding,
  or scoped-exclusion dispositions and mandatory path constraints; none is
  repaired or waived;
- the binding Phase 2 input preserves strict closed validation, separate
  execution and persistent-group identities, ordered sample/schema/unit and
  row-count relations, acquisition-only outcome semantics, thermal v1/v2
  compatibility, and the prohibition on implicit scope broadening.

This is the explicit Product Owner countersignature for the Phase 1 joint
boundary only. It does not constitute Phase 2 detailed-design approval or,
by itself, authorization to implement; Phase 2 still requires its own reviewed
detailed design and applicable test gates. It also does not approve Phase 3 or
Phase 4 and is not M03 release acceptance. Any implementation drift beyond the
recorded boundary requires a revised joint decision.
