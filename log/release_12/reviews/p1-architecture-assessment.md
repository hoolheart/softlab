# Release 12 (M03 Phase 1) — Architecture boundary assessment

Owner: Software Architect (sw-jerry) | 2026-11-01 Asia/Shanghai
Status: **ARCHITECTURE POSITION — FEASIBLE; Product Owner concurrence still required**
Inputs: [PRD](../prd.md), [M03 plan](../../../plan/milestone-03.md),
[characterization plan](../test/characterization-test-plan.md), and
[executed characterization results](../test/characterization-results.md).

This is the architecture side of the Phase 1 evidence decision. It is not a
Phase 2 detailed design, a backend repair authorization, or a claim of release
acceptance. The assessment relies on the recorded 405-test execution and source
inspection; no runtime experiment was repeated for this document.

## 1. Feasibility verdict

**Position: (a) — a minimal COMMON record envelope across the thermal and
signal-monitor clients is justified by the evidence.** The supported form
should be an **additive, namespaced JSON-text envelope on HDF5**, with a small
common outer contract and explicitly client-specific declarations. It must not
replace or reinterpret the existing thermal v1/v2 manifest.

This boundary is feasible because both actual clients already have the same
essential record semantics: a bounded `run_count` acquisition, a `DataGroup`
containing an ordered `DataRecord`, declared column names/units, requested and
completed row counts, a persistent group UUID distinct from the bounded
execution UUID, and a truthful completed-acquisition snapshot before
persistence. Their independent oracles are also real and different: the Euler
recurrence for thermal and `tick + offset` arithmetic for signal monitor.

The commonality is therefore at the record-contract level, not at the level of
thermal configuration or model semantics. The signal client still needs an
owned persistence/inspection path and must expose or retain its existing group
for that path; the assessment does not claim that it persists anything today.

### Comparison of the three mandated options

| Option | Assessment against characterized behavior | Decision |
| --- | --- | --- |
| **Extend the existing example envelope** | `softlab.thermal-reference` is deliberately closed: its eleven top-level fields, thermal configuration/model/procedure, six-column schema, and v1/v2 timing rules are strictly validated. Versions 1 and 2 specifically encode coordinated versus direct delay behavior. Signal monitor has different inputs, columns, model declarations, and a caller-derived `difference`. Making it fit would either invent thermal semantics or weaken legacy validation. Adding fields to the existing closed object also changes the compatibility contract rather than adding an independent capability. | **Reject as the common boundary.** Preserve legacy v1/v2 readers and writers within their reviewed scope. |
| **Additive shared text envelope** | A separate metadata key can hold strict JSON text while HDF5 continues to store samples and column definitions. The common layer can own format/version, client identity, execution/group identity relations, ordered sample-schema relations, source/software availability, and the pre-persistence outcome stage. Client-owned sections can truthfully describe thermal Euler inputs versus signal offsets without forcing them into one model. Existing thermal runs can dual-write the legacy manifest and the additive record; old files remain legacy. | **Recommended.** This is the smallest reusable boundary supported by both clients and the existing HDF5 writer. |
| **Separate client-specific records only** | This would be honest and technically possible, but the evidence does not show a conflict in the common identity, schema, stage, codec, or storage semantics. Separate codecs would duplicate the same strict JSON, versioning, identity, and safe-inspection logic and would make the requested two-client comparison less consistent. Client-specific payload validation is still required under the recommended option. | **Not necessary for these two clients.** Retain this as the fallback if the Phase 2 design cannot keep the common outer contract small. |

This is **not verdict (c)**. No additional evidence is needed for the narrow
HDF5/additive-envelope decision: C1–C6 are characterized, safe on-disk HDF5
inspection succeeded, both client field inventories and independent oracles
are present, and negative SQLite/io evidence is itself sufficient to exclude
those paths. Evidence would be insufficient only if the scope were expanded to
claim generic backend parity or safe generic `io.py` reloads; those claims are
not part of this recommendation.

### Minimal supported record boundary

The common outer contract should be limited to the following semantics; exact
field names and limits remain a Phase 2 design responsibility:

- namespace, independent format version, and one of the two supported client
  identifiers;
- canonical persistent group UUID and separately captured execution UUID;
- bounded client-specific procedure/configuration/setup/model declarations,
  including unavailable-source reasons rather than invented values;
- ordered record/column definitions, units, row count, and sample relationships;
- observed softlab/Python identity and bounded source identity/availability;
- exactly the completed-acquisition-before-persistence outcome snapshot.

Thermal-specific equations and timing policies, and signal-specific offsets and
derived-difference policy, belong in client-specific sections. No station graph,
callable serialization, arbitrary experiment description, or general provenance
model is justified.

## 2. Storage path feasibility

### Supported backend: HDF5 only

HDF5 can truthfully persist this record today **as one already-encoded JSON
text attribute plus the existing `DataRecord` datasets**. `save_impl` applies
`str(value)` to every metadata value (`softlab/shui/data/hdf5.py:81-82`), so it
is lossy for arbitrary Python metadata but preserves a value that is already
text. Types inside the envelope are recovered by the strict JSON codec, not by
claiming that HDF5 metadata is lossless. C2/C3 observed the text behavior,
including non-ASCII text, and verified group/sample/schema relations on disk.

SQLite is not a supported persistence path for this record. It has no storage
for an arbitrary envelope key, persists only the intended `sample`/`run_id`
fields, and D2 observed that even that reconstruction becomes empty metadata
through the current constructor path. Supporting SQLite would require a
separately reviewed schema and compatibility decision, not an assumption of
backend parity.

### Safe write path

The supported integration should follow the thermal example's owned-backend
pattern rather than generic `io.py` helpers:

1. Construct the record only after the allowed completed-acquisition gate.
2. Validate and encode the complete envelope to JSON text before persistence.
3. Put that text on the group with `set_meta(<shared-key>, text)`; do not use
   `DataGroup(meta=...)` and do not encode null by calling `set_meta(key, None)`.
4. Use an owned `HDF5DataBackend`, check connect/save status and errors, and
   disconnect with independent cleanup diagnostics. Save only within the
   reviewed synthetic output ownership rules.

The HDF5 writer replaces a same-ID group by deletion and creation. Therefore
this boundary supports writing a new run record, not atomic publication,
in-place outcome updates, rollback, or durable partial recovery.

### Safe read and inspection path

The legacy plain HDF5 loader is not a safe routine reader of the original file.
The supported reader must retain the characterized disposable-copy discipline:

1. Hash and byte-copy the source to owned scratch space; all subsequent legacy
   backend reads operate on the copy.
2. On the copy, use `h5py.File(..., 'r')` to enumerate groups when needed,
   preflight reserved attributes and the shared attribute's presence/type, and
   capture the text attributes before calling the legacy loader. This also
   supplies independent on-disk inspection evidence.
3. Call the backend load at most once for that copy with `str(group_id)`, then
   explicitly hydrate the shared text key and any applicable client-legacy keys
   through `set_meta`. Decode and validate the envelope and cross-check group
   identity, columns, row counts, and samples before returning it.
4. Disconnect and remove scratch resources while retaining primary and cleanup
   diagnostics. Tests must verify that the delivered original bytes remain
   unchanged.

`io.reload_group`, `io.load_groups`, and an unmodified thermal `_load_group`
are not supported substitutes. In particular, the thermal helper currently
hydrates only its three legacy keys plus `thermal_manifest`; Phase 3 must
explicitly carry the additive key through the hydration seam.

**No blocking backend repair is required before this exact record can be
supported.** The required Phase 3 hydration and signal-client persistence work
are scoped feature implementation, not a silent repair of HDF5, `DataGroup`,
or `io.py`. If Product instead requires SQLite, generic `io.py` reload/list
support, or plain loads of original HDF5 files, the associated defects become
blocking and require their own reviewed repair scope.

## 3. Codec, identity, versioning, and absence semantics

These implications are sufficient to judge feasibility; they do not freeze the
Phase 2 schema.

- **Bounded value allowlist:** use JSON objects with string keys, arrays,
  strings, booleans, exact integers, finite real numbers, and explicit null.
  Reject arbitrary Python objects, NumPy values not explicitly normalized by a
  client constructor, nonfinite numbers, duplicate keys, malformed JSON, and
  any implicit `str`/`repr` fallback. The existing standard-library JSON
  patterns in the thermal example demonstrate feasibility without a new
  dependency. Samples stay in record datasets rather than being duplicated
  into an unbounded metadata blob; Phase 2 should set conservative encoded
  size/depth limits for the declarations.
- **Identity:** persistent group UUID and execution UUID are different fields
  with different owners. The group UUID must equal `DataGroup.id`, the HDF5
  group name, and the disk `id` attribute. For new common records, capture the
  execution UUID from `CountResult.run_id`; signal already returns that result,
  while thermal must retain the value currently observable in its `after_count`
  callback. Never use one identity in place of the other. Existing thermal
  v1/v2 files cannot retroactively gain an execution UUID and remain legacy
  records rather than being silently migrated.
- **Versioning:** the additive envelope has its own namespace and format
  version, independent of thermal manifest versions 1/2. Reject unsupported
  versions and unknown fields for the current closed version; do not infer a
  migration or silently discard extensions. A future version requires an
  explicit compatibility decision.
- **Stage:** initially support only
  `completed_acquisition_before_persistence` with completed acquisition and
  validated row/boundary evidence. Persistence, plotting, later cleanup, and
  overall outcome remain `not_yet_established` in the stored snapshot. No later
  outcome update is supported in M03, so a stored snapshot can never claim
  subsequent save/plot/cleanup success. Failed/cancelled durable run records
  are outside this boundary.
- **Missing, null, unknown, and absent:** missing required fields and unknown
  fields are errors. An optional unavailable fact uses JSON null together with
  a bounded, nonempty unavailable reason where applicable; it must not become
  an empty string, zero, empty collection, or success value. Absence of the
  whole additive key means only “no additive common record” and may select a
  documented legacy path. A present non-text, malformed, invalid, or
  unsupported-version value is an error, never legacy absence. JSON null is
  inside the encoded text and is unrelated to the observed `set_meta(None)`
  mismatch.

This closed common envelope with closed per-client payloads is enough to make
option (a) safe; it does not require a general extensibility framework.

## 4. Disposition of characterization deviations and findings

| Item | Disposition for the recommended boundary | Reason and required constraint |
| --- | --- | --- |
| **D1 — constructor mutation/crash with non-string keys** | **Tracked debt; non-blocking.** | Constructors for the new path create an empty `DataGroup` and add string-keyed JSON text with `set_meta`. Hydration bypasses the broken constructor metadata route. D1 strengthens, rather than changes, the reason not to use `DataGroup(meta=...)`. |
| **D2 — SQLite plain-load metadata is empty** | **Tracked debt and explicit backend exclusion; non-blocking for HDF5-only scope.** | SQLite cannot preserve the envelope today. It becomes blocking only if SQLite is selected, in which case a separately reviewed storage/schema repair is mandatory. |
| **F-A / R4-BASE-HDF5-01 — plain HDF5 load destroys reserved attributes and a second load fails** | **Tracked debt with a mandatory safety constraint; non-blocking only on the disposable-copy path.** | One legacy load may occur only on an owned byte-copy after disk attributes are captured. Plain load of an original or reliance on repeated plain loads is unsupported and would be blocking. Original-byte preservation remains a Phase 3 test requirement. |
| **F-B — thermal hydration drops custom keys** | **Required Phase 3 integration work; not a Phase 1 blocker and not a backend repair.** | The unmodified helper cannot return the additive key. The supported hydration seam must explicitly capture and restore that key; otherwise the recommended record is not considered implemented. Legacy keys and `thermal_manifest` retain their behavior. |
| **F-C — `io.reload_group` fails with UUID IDs on both backends** | **Tracked debt; non-blocking because `io.reload_group` is excluded.** | The supported HDF5 helper uses an owned backend and `str(group_id)`. Selecting generic reload without a scoped repair would be blocking, and swallowed errors must not be presented as successful inspection. |
| **F-D — `io.load_groups` is broken for SQLite and unsafe after HDF5 plain loads** | **Tracked debt; non-blocking because `io.load_groups` is excluded.** | Group discovery for the supported inspector uses read-only HDF5 keys on a disposable copy and explicit identity validation. Generic list/reload support is not implied. |

The `set_meta(None)` documentation/implementation mismatch also remains
tracked and is avoided by encoding null inside JSON. None of these items is
silently fixed or waived; the recommendation works around them through a
narrow, testable ownership boundary.

## 5. Phase 2–4 technical consequences

### Phase 2 — schema and encoding

- Specify and implement only the additive common JSON-text codec and the two
  named client payload validators/construction seams.
- Freeze the key namespace, exact required/optional fields, conservative
  size/depth limits, identity cross-checks, unavailable reasons, closed unknown
  policy, and independent version behavior.
- Reuse the characterized strict JSON techniques for duplicate and nonfinite
  rejection; test valid round-trips and all malformed/unsupported cases before
  storage integration.
- Leave `softlab.thermal-reference` v1/v2 validation and interpretation
  unchanged. Do not migrate old files or fabricate missing execution IDs.

### Phase 3 — storage and inspection

- Support HDF5 only, using the owned write path and disposable-copy,
  preflight/hydration read path above. Extend the hydration seam explicitly for
  the additive key; do not broaden it to arbitrary metadata or impose thermal's
  legacy `_META_KEYS` on signal-monitor records.
- Provide separate-process inspection that distinguishes additive absence from
  present-but-invalid data and verifies group/sample/schema/units/identity
  relations and original-byte preservation.
- Test serialization, connect/save, close/disconnect, reload failures, and
  independently observable primary/cleanup diagnostics. No atomicity or
  failed-run persistence claim follows.

### Phase 4 — client integration and comparison

- New thermal coordinated/direct runs dual-write the unchanged legacy manifest
  and the additive common record; retain the execution UUID for the latter.
  Legacy thermal files remain readable under their existing rules.
- The signal-monitor example owns its stored-record construction and rerun
  declaration. Its integration must retain/expose the existing group, capture
  `points`, offsets 5/2, bounded execution identity, ordered columns, and the
  completed-acquisition gate before saving.
- Keep signal `timestamp/signal/reference` as the stored columns. Declare
  `difference = signal - reference` as a derived client policy and validate it
  independently; do not pretend that `difference` is already a stored column.
- Compare thermal samples to the independent Euler oracle and signal samples
  to the independent tick/offset oracle with declared tolerances. Exclude wall
  timestamps and newly generated UUIDs from equality expectations.

### Explicitly excluded

SQLite support or migration; generic `io.py` certification; repairs to
`DataGroup`, HDF5, or io defects outside the scoped seam; arbitrary metadata
round-tripping; new dependencies; a general provenance framework; dependency
locks or environment recreation; secret capture; arbitrary object/callable or
simulation-graph serialization; real-instrument qualification; durable
failed/partial runs; later outcome mutation; atomic publication, rollback,
retry, and crash recovery.

## Architecture decision statement

The architecture-side Phase 1 gate is **PASS FOR THE RECOMMENDED BOUNDARY**:
option **(a)**, implemented in later phases only as an additive shared JSON-text
record on HDF5 with safe copy/hydration and client-specific declarations. No
blocking repair is required for that boundary. Product Owner agreement and a
reviewed Phase 2 detailed design are still required before schema
implementation; this document does not authorize code changes beyond that gate.
