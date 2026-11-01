# Release 12 (M03 Phase 1) — Product Boundary Assessment

Owner: Product Owner (sw-camille) | 2026-11-01 Asia/Shanghai  
Branch: `codex/m03-p1-characterize`  
Inputs: [PRD](../prd.md), [M03 plan](../../../plan/milestone-03.md),
[characterization plan](../test/characterization-test-plan.md),
[executed results](../test/characterization-results.md), and
[developer plan review](p1-test-plan-developer-review.md).

## Verdict

**PRODUCT DECISION: ACCEPT option (a) — a minimal COMMON record envelope across
the thermal client and the signal-monitor client.**

The two actual clients share a demonstrated maintainer need — identify what
ran, its bounded rerun declarations, units, observed software/source identity,
acquisition stage, samples and persistent identity — while their client-specific
semantics remain different enough that a union schema would overclaim. The
shared value is one bounded inspection, validation and outcome contract with
closed client-owned payloads, not a general provenance framework.

The product side of the M03-AC01 evidence decision is therefore **PASS**. The
Phase 1 gate still requires the Architect's independent agreement before any
schema implementation. This is neither Phase 2 design approval nor M03 release
acceptance. The assessment relies on the reported executed evidence (all C1–C6
areas characterized, 405 tests passing, compile/import checks passing, original
HDF5 bytes preserved, and developer findings F1–F4 closed); it does not claim a
separate Product test execution.

## 1. Supported boundary recommendation

### Boundary

Support **two client profiles in one small common envelope**, persisted only
through the evidenced **HDF5** route:

- The common contract owns format/profile identification and versioning,
  separate execution/persistent-group identity and sample relations, ordered
  sample-schema relations, availability-aware software/source identity, common
  outcome-stage semantics, and strict missing/invalid/unsupported behavior.
- Each client owns a closed declaration payload containing only the facts it can
  truthfully supply. The common envelope must not force thermal-only fields on
  the signal monitor or reduce the thermal model to an untyped bag of values.
- Sample rows remain associated with the stored `DataGroup`; the envelope
  describes their ordered schema, count, units and identity relations rather
  than replacing the sample store with a second unverified copy.
- Existing thermal v1 coordinated and v2 direct manifests remain readable with
  their current meanings. A common format must not relabel those versions,
  silently migrate stored files, or change existing APIs. Phase 2 must define
  additive coexistence and any mapping explicitly.
- Supported inspection must use the safe, original-preserving HDF5 path
  evidenced in C3 and the thermal example's copy/hydration convention. SQLite,
  arbitrary `DataGroup` metadata round-trip, and the generic `io.reload_group`
  and `io.load_groups` helpers are not certified by this decision.

### Why the evidence is sufficient

| Product fact | Thermal client today | Signal-monitor client today | Boundary consequence |
| --- | --- | --- | --- |
| Construction | Stored, self-validating v1/v2 manifests exist for both adapters. | Only example-local in-memory `DataGroup`/`DataRecord` construction exists; no writer, manifest or backend is selected. | Reuse the common contract, but require a future signal-client-owned stored-record construction and inspection path in Phases 2–3. Do not describe that path as existing today. |
| Procedure, setup and model | Exact `ThermalConfig`, adapter/delay behavior, thermal procedure, Euler model and validity declarations are stored. | Deterministic rerun inputs are evidenced: `points`, offsets 5/2, non-consumptive `tick + offset` reads, and fresh scheduler/loop ownership. | These are meaningful closed client payloads, not fields to generalize into one physical-model schema. |
| Units and timing | Configuration/model units, six ordered columns and adapter-specific clock relations are stored and checked. | `timestamp`, `signal` and `reference` originate in record columns; `difference` is caller-derived. Wall-clock timestamps exist, but no stored units/timing envelope exists yet. | Phase 2 must declare signal units/timing truthfully, including genuinely unitless values; it must not invent physical units or treat wall time as deterministic simulation time. |
| Software, source and environment | softlab/Python versions and source path/digest plus git revision or an unavailable reason are observed. Broader environment/dependency locks are absent. | Nothing is persisted today. Observed identity can only be claimed after the future construction path captures it. | Common availability semantics are useful; any uncaptured signal fact must be explicitly unavailable, not copied from thermal or implied. |
| Outcome | A completed-acquisition-before-persistence snapshot is stored; all later stages are not yet established. | Only in-memory acquisition exists; there is no stored outcome today. | Both can support the same acquisition-stage vocabulary once signal persistence exists; neither may claim later success at that snapshot. |
| Samples and identity | Ordered stored samples, row count and persistent group UUID are related to the manifest; the execution UUID is distinct and is not retained by example-local `RunResult`. | Ordered in-memory source samples and deterministic derived differences exist. A persistent group/sample relation still must be constructed. | New common records require persistent group identity, separately captured execution identity and sample relations in both profiles. Neither UUID may be populated from the other; legacy records must not gain a fabricated execution UUID. |

Option (b) would duplicate the common identity, availability, stage and strict
inspection behavior despite two real clients demonstrating its value. Option
(c) is not warranted: C3 found a safe read-only inspection route on disposable
copies, both clients have deterministic inputs and independent oracles, and the
observed defects can be excluded without changing their contracts. This is not
a claim that a generic schema will fit future clients.

## 2. Supported clients, stages and outcome semantics

The supported scenarios for subsequent phases are:

1. thermal coordinated acquisition (legacy manifest v1 compatibility), and
2. thermal direct acquisition (legacy manifest v2 compatibility), and
3. `examples/signal_monitor.py` acquisition, including its canonical
   `points=3` comparison case, once its client-owned stored record exists.

M03 supports one stored snapshot meaning:
**`completed_acquisition_before_persistence` with acquisition completed**. An
incomplete, failed or cancelled acquisition is not a supported successful run
record and must not be represented as one. Durable failed/partial-run
persistence is not added by this decision.

At that snapshot, persistence, plotting, cleanup and overall outcome are
**`not_yet_established`**, never successful. M03 does **not** support rewriting
the stored acquisition snapshot with later outcomes. Apart from those explicit
stage markers, later observed outcomes are absent from this record; they may be
reported separately as runtime or inspection diagnostics. In particular, a
later successful reopen proves that inspection succeeded then — it does not
retroactively prove what persistence, plotting or cleanup did at acquisition
time. No update history, atomic publication or crash-recovery semantics are
implied.

## 3. Field ownership, product policies and independent oracles

### Ownership

- **Common record contract:** owns the envelope/version and client-profile
  identity, common availability and error semantics, separate execution and
  persistent-group identity, sample relations, ordered sample-schema
  requirements and stage vocabulary.
- **Thermal client:** owns its configuration, procedure, model/units, ordered
  six-column schema, v1/v2 clock/delay semantics, source observations, row-count
  relations and Euler comparison inputs. It also owns preservation of legacy
  thermal interpretation.
- **Signal-monitor client:** owns future stored-record construction and
  inspection, `points`, offsets 5/2, non-consumptive read-policy declarations,
  scheduler/loop ownership, ordered source-column declarations and their
  units/timing. It owns the declaration that `difference = signal - reference`
  is derived by the caller, not an existing stored source column. Phase 3 may
  persist that derived value only with explicit derived semantics, or Phase 4
  may independently recompute it from persisted source samples; it must not be
  relabelled as an acquired channel.
- **Storage layer:** stores the selected group, samples and bounded text record.
  It does not infer client meaning, manufacture unavailable values, or provide
  arbitrary Python metadata persistence.

### Required, optional, absent/null and unknown policy

- Every new common record requires a recognized format and client profile,
  persistent group UUID, separately captured execution UUID, sample identity
  relations, sufficient client declarations for the bounded rerun, ordered
  sample schema/count and units, an acquisition-stage-qualified outcome, and
  explicit software/source availability. Existing legacy records retain their
  reviewed identity fields and are not migrated to fabricate missing identity.
- Client-specific required fields are closed by that client's profile. Thermal
  source digest and timing relations remain thermal requirements; they are not
  silently imposed as identical signal fields.
- Additional source/environment observations and derived-value payloads are
  optional unless a client profile makes them required. Optional absence means
  not supplied under the declared profile; it never means success, zero, empty
  or not applicable by implication.
- Unknown or unavailable values require an explicit availability state and,
  where observable, a reason. They must not masquerade as `None`, `''`, `0`, an
  empty collection or a successful outcome. `set_meta(None)` must not be used to
  imply removal or absence.
- Explicit null is permitted only where the Phase 2 contract assigns it a
  specific meaning; it is not a universal unknown marker. Missing required,
  malformed present and unsupported-version records fail visibly. Invalid
  present metadata is never legacy absence.
- Unknown fields and unsupported client profiles/versions are rejected visibly
  for this bounded envelope; they are not silently dropped, preserved as an
  unvalidated extension, or accepted as another profile. A future extension
  requires a reviewed version/profile contract. No implicit stringification,
  nonfinite numeric value or arbitrary object is supported.

### Phase 4 oracle acceptance

**Both independent oracles are accepted**, with the following limits:

| Client | Accepted independent expectation | Required comparison discipline |
| --- | --- | --- |
| Thermal | Re-derived Euler recurrence from declared configuration and procedure, not from stored output. | For both adapters, temperature uses `atol=1e-10`; `tick`, `measurement_source_tick`, `sim_time_s` and `commanded_power_w` match exactly. Preserve the v1/v2 source-tick and delay distinction. |
| Signal monitor | For `points=3`, independent `tick + offset` evaluation gives signal `[6, 7, 8]`, reference `[3, 4, 5]` and differences `[3, 3, 3]`. | Values, row order and source/derived relations match exactly. The M02 test-double values `[9, 11, 13]` are not this client's oracle. |

Phase 4 must predeclare inputs, expected values, timing relations, tolerances
and exclusions before comparison. UUIDs and wall timestamps are excluded from
cross-run value equality, but their types, ordering where applicable, and
identity relations still require validation. Expected values must be computed
independently of the stored object and of the rerun being checked. Matching two
runs only to each other is not sufficient, and these synthetic oracles prove
neither physical accuracy nor arbitrary environment reproduction.

## 4. Disposition of evidence surprises

**None blocks the recommended HDF5-only, explicitly inspected envelope. All
remain open debt/findings or scoped exclusions; none is repaired or waived by
this assessment.** If implementation cannot avoid an affected path, work must
stop for a separately reviewed repair/scope decision rather than broadening this
boundary.

| Evidence | Product disposition for this boundary |
| --- | --- |
| **D1 — constructor mutation/crash** | Additional evidence for **R4-BASE-META-01**. Carry as tracked debt. Supported construction must not depend on `DataGroup(meta=...)` and tests must show caller inputs are not mutated. Non-string-key failure remains visible; it is not converted into accepted coercion behavior. |
| **D2 — SQLite plain-load meta is empty** | Carry as observed SQLite capability plus the effect of **R4-BASE-META-01**. Non-blocking because SQLite and arbitrary metadata reload are unsupported. Do not claim that `sample`/`run_id` metadata survives a plain SQLite load. |
| **F-A — destructive HDF5 load and failing second load** | End-to-end evidence for **R4-BASE-HDF5-01**. Carry as tracked debt. Plain HDF5 loads are outside the supported inspection path. Phase 3 must demonstrate safe repeated inspection on disposable copies/read-only disk access and original-byte preservation for both clients. If no safe user workflow results, Phase 3 is blocked and any reader repair needs separate scope. |
| **F-B — example hydration drops custom keys** | Carry as an open Phase 1 finding and an explicit limitation; no new defect ID is invented here. Do not promise arbitrary custom-key hydration. Each supported client inspection path must explicitly recover its declared envelope and must not rely on thermal's three-key hydration for signal data. |
| **F-C — `io.reload_group` returns `None` on both backends** | Carry as an open observed defect. Exclude this helper from supported reopen/inspection flows; `None` from it is not evidence of legacy absence. Any future dependency on the helper requires a separately scoped defect decision. |
| **F-D — SQLite `load_groups` returns `[]`; HDF5 listing is unsafe after mutation** | Carry as an open observed defect. No supported list/reload guarantee is made through `io.load_groups`. Phase 3 must provide an explicit, tested file/group inspection workflow without treating an empty helper result as an empty database. |

The separately tracked `set_meta(None)` documentation/implementation mismatch
also remains unchanged: the common codec owns absent/null semantics, not that
method. Existing R4-BASE-SCHED-01/F-01, F-02, OBS-004/005/006 and DEFECT-2
dispositions remain unchanged by this evidence.

## 5. Scope limits and requirements for later product acceptance

### Still out of scope

No real instrument/VISA access or qualification, physical accuracy or physical
termination guarantee, secret/host collection, dependency lock or environment
replay guarantee, arbitrary object/callable/model-graph serialization, general
provenance platform, SQLite/backend parity or rewrite, generic metadata
round-trip, legacy migration, automatic retry/rollback, atomic publication,
outcome audit history, durable failed/partial-run recovery, or unrequested
repair of the findings above is authorized. M04 retains physical adapter
decisions, M05 scan extensions and M07 recovery.

### What Phases 2–4 must deliver

- **Phase 2 — contract and encoding:** jointly reviewed minimal common contract
  and two closed client profiles; explicit namespaces, versions, bounded value
  types and size/depth policy; required/optional/absent/null/unavailable and
  unknown-field behavior; identity, units and snapshot semantics; visible
  duplicate/malformed/nonfinite/unsupported/version failures with no implicit
  stringification or side effects. Provide the justified signal-client
  construction seam without using the defective metadata constructor, preserve
  thermal v1/v2 and existing import/API behavior, and prove deterministic valid
  round-trips plus the required negative cases.
- **Phase 3 — HDF5 storage and clean-session inspection:** client-owned signal
  writing and safe inspection; stored group/sample/schema/units/identity and
  row-count relations for both clients; explicit handling of source versus
  derived signal values; original-byte preservation and safe repeated reads in
  fresh processes; distinct legacy-absent versus malformed/unsupported results;
  tested serialization/save/close/disconnect/reload failures with truthful
  primary and cleanup diagnostics. Document supported file/group selection and
  unavailable reasons without using excluded io helpers or claiming SQLite,
  atomicity or failed-run persistence.
- **Phase 4 — independent comparison and release evidence:** integrate both
  thermal adapters and the signal monitor within this boundary; publish accurate
  user guidance for the supported workflow and exclusions; execute documented
  fresh-environment and clean-process reruns with actual commands, versions,
  warnings and limitations; satisfy the accepted independent oracles and
  identity/timing relations; and retain legacy compatibility. Complete
  independent review, tester-owned failure closure, full relevant regression,
  compile/import checks and applicable principle inspection. Final M03
  acceptance additionally requires the exact-candidate CI, promotion and
  promoted-state checks, authoritative debt/limitations summary and clean
  worktree required by the PRD.

Product acceptance will be withheld if any supported record claims unavailable
facts, later-stage success, backend parity, generic metadata safety or
reproducibility beyond this bounded synthetic scope.
