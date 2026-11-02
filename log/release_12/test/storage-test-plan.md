# Release 12 (M03 Phase 3) — Storage and Inspection Test Plan

Owner: Tester (sw-mike) | 2026-11-01 Asia/Shanghai
Status: **DRAFT — revised per developer review (commit `6c842fd`,
[p3-test-plan-developer-review.md](../reviews/p3-test-plan-developer-review.md));
all five findings closed in plan text; awaiting developer re-review,
then detailed design freeze; no test code and no source changes before
approval**

> **ORDERING DELTA applied per approved design §3.5.1 (b52e034/11018b2) —
> pending developer re-review.** A1.2 (4th bullet), A2.5, and A3.1 below
> carry the §3.5.1 frozen ordering rule (envelope sequence canonical;
> HDF5 enumeration lexicographic/non-semantic; by-name disk cross-check;
> samples by row index) in place of the superseded literal wording.

Branch: `codex/m03-p3-storage`
PRD: [log/release_12/prd.md](../prd.md) (Phase 3 row, **M03-AC03**,
**M03-AC04**)
Plan: [plan/milestone-03.md](../../../plan/milestone-03.md) (Phase 3 row;
evidence-matrix items **3** (legacy and storage) and **4** (lifecycle and
failure))
Binding inputs:
- [p1-boundary-decision.md](../design/p1-boundary-decision.md) — option (a);
  §1.1 exact common-envelope boundary; §2 supported backend and ownership
  (HDF5-only, owned writes, disposable-copy preflight/hydration reads,
  acquisition-snapshot-only outcome); §4 dispositions **D1, D2, F-A, F-B,
  F-C, F-D** with their mandatory path constraints; §6 binding constraints
  items 5–8.
- [p2-schema-design.md](../design/p2-schema-design.md) — frozen codec
  contract (§1.1–§1.7 literals, §2 limits, §3 error taxonomy, §4
  `build_run_group` seam, §8 freeze table); **§9 deferral list** — this plan
  picks up every §9 item: owned HDF5 save, safe read path, artifact
  cross-checks, storage-level absence-vs-invalid, signal-monitor owned
  persistence (conditional), and example dual-write wiring.
- [characterization-results.md](characterization-results.md) — evidenced
  current backend behavior (F-A destructive plain load, F-B hydration key
  loss, F-C/F-D io helper breakage, D2 SQLite empty meta) and the Phase 1
  CI corrections (the 3.9 `autospec`+`wraps` lesson; conditional warning
  expectations).
- [schema-test-plan.md](schema-test-plan.md) /
  [schema-test-results.md](schema-test-results.md) — Phase 2 conventions and
  the executed codec contract (459-test baseline: 405 pre-existing + 54
  Phase 2).

Format conventions follow the Phase 1/2 plans: assertion-based, one defect
per negative test, synthetic temporary data only, literals quote-checked
against the frozen detailed design, discrepancies flagged never guessed.

## Scope and ground rules

This is a **planning document only**. No test code is written and no source
under `softlab/` or `examples/` is modified until this plan passes
independent review. Phase 3 test code targets **the Phase 3 storage and
inspection seam only** — the owned write path (persisting the in-memory
`DataGroup` produced by the Phase 2 `build_run_group` seam through an owned
`HDF5DataBackend`), the safe read/inspection path (disposable byte-copy,
hashing, `h5py` preflight, at-most-one legacy load, explicit `run_record`
hydration), and the example-level integration that dual-writes the additive
record alongside the unchanged legacy thermal manifest. Codec re-testing is
**not** repeated here — Phase 2 results (`schema-test-results.md`, 54/54
PASS) are reused unchanged per the reuse rule; only integration seams and
storage-layer behavior are newly tested.

**Gating seam (decision §2, design §9):** the record is constructed and
saved only **after** the completed-acquisition gate at the call site;
connect/save status and backend errors are checked; owned resources are
disconnected; primary and cleanup diagnostics are retained **independently**.
Same-ID deletion/recreation is not atomic publication and is not tested as
one.

**Owned backend only:** the supported write path uses an owned
`HDF5DataBackend`, never the generic `softlab/shui/data/io.py` helpers and
never `DataGroup(meta=...)` (tracked defect D1) or `set_meta(key, None)`.
The supported read path never performs a plain original-file backend load
(F-A mandatory constraint): all legacy reads occur on a disposable,
sha256-verified byte copy, at most one legacy load per copy, with the
`run_record` key hydrated explicitly (F-B condition).

### Phase 2 literals reused verbatim (frozen, quote-checked)

These are already frozen and Phase 3 tests import and assert them from the
module (never re-declare test-local copies): `NAMESPACE =
'softlab.run-record'`; `METADATA_KEY = 'run_record'`; `FORMAT_VERSION = 1`;
`PROFILE_THERMAL = 'thermal'`; `PROFILE_SIGNAL = 'signal'`;
`OUTCOME_SNAPSHOT = 'completed_acquisition_before_persistence'`;
`ACQUISITION_COMPLETED = 'completed'`;
`STAGE_NOT_YET_ESTABLISHED = 'not_yet_established'`;
`STATE_AVAILABLE = 'available'`; `STATE_UNAVAILABLE = 'unavailable'`;
`UNAVAILABLE_REASONS = ('capture_failed', 'source_mismatch')`; the seven
`MAX_*` limits (read from `softlab.shui.run_record` constants per Phase 2
S4.4); error leaf classes `RecordDecodeError`/`RecordValidationError`; the
group seam `build_run_group` (`softlab/shui/run_group.py`).

### Phase 3 placeholders — literals the detailed design must freeze

Phase 3 has no detailed design yet. The gate order requires this plan's
review first, then the reviewed detailed design freezes the literals below;
this plan is then aligned by direct quote-check (as Phase 2 did against its
design) before test code is written. Every placeholder is tagged
**[P3P-nn]** wherever it appears and is listed in the "Placeholder list"
section at the end.

- **[P3P-01]** The Phase 3 module path(s) and public entry points: the owned
  save function, the safe inspection function, and their exact names and
  signatures. Extensions per the developer review: (a) the sample/record
  attachment mechanism (the plan's branch (a) uses the existing public
  `DataGroup.add_record(...)`; branch (b) freezes a sample-attachment
  argument/seam in the owned save signature — one must be confirmed);
  (b) the per-profile hydration key set A2.4 asserts — thermal: the three
  `_META_KEYS` plus `thermal_manifest`; signal: the empty set, which is
  a frozen literal too; (c) what "legacy context still available" means
  for A3.3's absence outcome at the chosen seam placement (raw manifest
  text vs decoded legacy envelope — manifest decoding currently lives
  only in the example layer, so this interacts with **[P3P-06]**).
- **[P3P-02]** The storage-layer error taxonomy: exception class names,
  inheritance, and frozen message templates for save/connect/disconnect/
  inspection failures (the Phase 2 codec taxonomy is frozen; the storage
  layer's is not yet defined).
- **[P3P-03]** The primary-vs-cleanup diagnostic container: its type, shape,
  and how independence is observable (e.g. named fields/attributes holding
  each diagnostic, present even when the sibling is absent).
- **[P3P-04]** The fresh-process inspection entry point: how a clean
  subprocess performs inspection (bootstrap module, CLI surface, exit codes,
  JSON payload schema) — modeled on the Phase 1
  `tests/thermal_cli_bootstrap.py` convention.
- **[P3P-05]** Real-seam fault-injection points: the exact supported
  mechanism by which each A4 fault is injected without mocks that skip the
  real code path (e.g. read-only target directory, pre-removed scratch file,
  corrupt-bytes fixture written through `h5py` itself). Extension per the
  developer review: every frozen mechanism must be **empirically verified
  to fail at the named stage on both supported interpreters (3.9 and
  3.13)** before design freeze — a seam failing at the wrong stage
  reclassifies its row (e.g. to connect-class A4.9); a seam that does not
  fail is vacuous and invalidates the row.
- **[P3P-06]** Whether the Phase 3 write/read seam lives in `softlab/shui/`
  or in the example layer, and the export-surface consequence for
  `softlab/shui/__init__.py` (Phase 2 froze the §8 table; any Phase 3
  addition must be an explicit design freeze, not drift).
- **[P3P-07]** The unavailable-reason policy for storage-level facts the
  envelope cannot capture structurally (e.g. cleanup-disconnect outcome):
  whether the frozen vocabulary `('capture_failed', 'source_mismatch')` is
  reused, extended by design revision, or these facts remain outside the
  record.

## A1 — Owned write path

**Requirement basis:** decision §2 supported backend and ownership; §6
items 1, 3; §4 D1 conditions; PRD M03-AC03 ("supported stored records
preserve declared procedure/setup/model, units, observed software/
environment availability, sample ordering/schema and identity relations"),
M03-AC04 ("chosen backend and storage ownership are explicit"); plan
evidence-matrix item 3; design §4, §9 (first deferral item).

**Record source of record (datasets input — developer review finding 1).**
`build_run_group` returns an **empty** `DataGroup` — the Phase 2 seam
constructs no `DataRecord` and no rows (verified against
`softlab/shui/run_group.py` this revision). A1 fixtures therefore
attach the record datasets themselves via the existing public
`DataGroup.add_record(record: DataRecord)` API (`softlab/shui/data/
base.py:577`, verified — "Add or update a record", raises `ValueError`
for a non-`DataRecord` argument): one `DataRecord` per envelope
`record` block, columns taken from the envelope's `record.columns`,
rows filled with synthetic fixture values known at construction time
(the A1.2 oracle). This branch (a) is implementable today and keeps
the Phase 3 save-path scope clean; branch (b) — freezing a
sample-attachment argument/seam in the owned save entry signature —
is the alternative under the **[P3P-01]** extension. One of the two
must hold before test code is written.

1. **End-to-end owned save on synthetic temp files.** Build a run group via
   the Phase 2 seam (`build_run_group`, envelope from the design §8.6
   canonical fixtures with synthetic UUIDs), attach the record fixture
   per the source-of-record paragraph above, then persist it through the
   Phase 3 owned save entry point **[P3P-01]** backed by an owned
   `HDF5DataBackend` rooted in a `tempfile.TemporaryDirectory` with the
   `softlab-m03-p3-*` prefix. Assert: the save entry point rejects
   failed/cancelled envelopes before connect (A5.2 reuse) — the
   completed-acquisition gate itself is a **call-site** property and is
   asserted at the example layer (A1.6), not here, because a `shui`-level
   save function cannot observe it; the file appears at the declared
   path; the backend is connected during save and disconnected
   afterwards (owned resource lifecycle asserted, not assumed).
2. **Verification from the stored artifact, not in-memory objects.** Every
   relation below is asserted by opening the stored file **directly with
   `h5py.File(path, 'r')`** (on a disposable byte copy per A2 — the write
   test itself may read the delivered original only via the safe path):
   - the HDF5 group name equals `str(group_id)` and the on-disk `id`
     attribute equals the envelope's `group_id` (persistent group identity,
     decision §1.1 identity row);
   - the envelope's `execution_id` appears **nowhere** in the stored group
     UUID slots — execution UUID and persistent group UUID are not
     conflated (decision §6 item 5);
   - `METADATA_KEY == 'run_record'` is present as a text attribute; its
     bytes decode via the Phase 2 codec `decode_envelope` to an envelope
     deep-equal to the input;
   - the record dataset(s) exist with the declared columns and truthful
     units; per design §3.5.1 the envelope's declared column sequence is
     the canonical stored semantic order (enforced order-sensitively
     against the in-memory record before any write), while HDF5 dataset
     enumeration is lexicographic and non-semantic — so the disk
     cross-check is **by name**: for each declared column, in envelope
     order, the same-named dataset exists with its four attributes
     matching exactly and the column-name sets are equal; samples are
     compared by row index (never via enumeration order). This
     supersedes this bullet's previous literal "stored column order
     equals envelope `record.columns` order (decision §1.1
     sample-schema row)" wording (design §3.5.1 follow-up delta
     list); the thermal dual-write fixture is unaffected in practice
     because the thermal schema is already lexicographic;
   - stored row count equals `outcome.completed_rows` (decision §6 item 5
     relation; design §9 cross-check deferral);
   - sample values in the stored dataset match an independently constructed
     expectation (synthetic fixture values known at construction time —
     full oracle comparison is Phase 4 scope, but Phase 3 asserts stored
     samples are exactly what the seam was handed);
   - the thermal legacy manifest (`thermal_manifest`) coexists on the same
     group for the thermal dual-write case (legacy record supplemented, not
     replaced — decision §1 opening).
3. **Writer status checks.** The owned save checks connect status and save
   status; a save attempted without a successful prior connect fails
   visibly (never writes). Assert the error type per the frozen taxonomy
   **[P3P-02]**.
4. **Construction discipline regression guards (D1).** Recorder-double
   assertions (plain `wraps` or hand-held recorder — **never**
   `autospec=True` combined with `wraps`, the Phase 1 3.9 lesson): the
   write path never constructs `DataGroup(meta=...)`, never calls
   `set_meta(key, None)`, and performs the encode before any HDF5 attribute
   write (decision §6 items 3–4; "no HDF5 attribute write before successful
   validation and JSON encoding").
5. **In-memory vs stored equality is not the claim.** Tests assert facts
   read back from disk; agreement with the in-memory object is incidental
   and never the sole evidence (decision §3 evidence row: "group/sample/
   schema facts are independently inspectable on disk").
6. **Example dual-write integration (developer review finding 4 — planned,
   not deferred).** Run `thermal_experiment.execute()` on a synthetic
   temp-dir configuration (both adapters, synthetic UUIDs), then inspect
   the stored artifact **via the A2 safe path** and assert:
   - the stored group carries **both** `thermal_manifest` and
     `run_record` — the legacy record is supplemented, not replaced;
   - the legacy manifest bytes, version literals (1/2), timing meanings,
     and the eleven-field closed contract are **unchanged** (equality
     against a legacy-only reference run on an identical synthetic
     config);
   - the additive envelope decodes via the Phase 2 codec and cross-checks
     against the stored artifact;
   - **call-site gate:** the record is constructed and saved only after
     `result.acquisition == 'completed'` (decision §2) — asserted here
     at the example layer, the only place the gate is observable;
   - the original legacy file's bytes are unchanged (sha256 before/
     after).
   This row is the plan's evidence for the scope paragraph's dual-write
   claim and for design §9's example-wiring item; it is **not** deferred
   to Phase 4. If the reviewed detailed design nonetheless assigns all
   example wiring to Phase 4, this row is deferred verbatim at design
   freeze and the scope paragraph narrows to match — the choice is
   recorded explicitly, never left ambiguous.

## A2 — Safe read path (disposable-copy preflight and hydration)

**Requirement basis:** decision §2 supported read/inspection discipline (four steps,
verbatim); §4 F-A mandatory safety constraint and F-B required integration
work; PRD M03-AC04 ("original data is protected"; "distinguish legacy
absence from corrupt/unsupported metadata"); plan evidence-matrix item 3
("verify bytes/metadata needed to show original data was not modified");
design §9 (second and fourth deferral items).

1. **Byte-copy and hash discipline.** For every inspection: record
   `hashlib.sha256` of the original before, `shutil.copy2` to owned scratch
   space inside the temp root, inspect only the copy, record sha256 after,
   assert byte equality. This is asserted as part of every A2/A3 test, not
   relegated to a hygiene-only check (F-A condition: "prove original-byte
   preservation and safe repeated inspection using fresh copies/read-only
   disk access").
2. **h5py-level on-disk inspection independent of `DataGroup`.** The
   inspection path enumerates groups via `h5py.File(copy, 'r')` keys,
   prefights reserved attributes (`id`, `name`, `timestamp`, `backend`)
   presence/type, and captures required text attributes **before** any
   legacy backend load (decision §2 step 2). Tests assert the inspection
   result carries the group identity, the envelope, and sample/schema facts
   obtainable without ever constructing a `DataGroup` from the original —
   on-disk facts are verified independently of any returned object.
3. **At most one legacy load per copy.** Call inventory (recorder double on
   the owned backend's `load_group`, per the Phase 1 recording-spy
   technique) asserts exactly one legacy load per inspection, invoked with
   `str(group_id)` (decision §2 step 3; F-C condition). A second inspection
   of the same original uses a **fresh copy**, and both succeed — repeated
   loads of one copy are unsupported and must never occur in the supported
   path (F-A condition).
4. **Additive-key hydration preserves `run_record` alongside legacy keys
   (F-B regression).** After the single legacy load, the inspection path
   explicitly hydrates `METADATA_KEY` and the applicable client-legacy keys
   and asserts:
   - the returned record carries the decoded envelope under the additive
     key **and** the legacy keys the client owns (thermal: its three
     `_META_KEYS` plus manifest) — the `run_record` key survives hydration
     verbatim (the F-B defect drops it today; without this seam the common
     record is not implemented);
   - hydration does **not** promise arbitrary custom-key restoration and
     does not impose thermal's legacy keys on the signal profile (F-B
     condition; signal's supported payload keys only);
   - absent additive key on a legacy-only file is reported as legacy
     absence (see A3), never as a hydration success.
5. **Decode, validate, and cross-check on the copy.** The hydrated text is
   decoded with the Phase 2 codec; identity (`group_id` vs stored group
   name/`id` attribute), schema/units, row counts, and samples are
   cross-checked against the stored artifact (decision §2 step 3) under
   the §3.5.1 read-side rule (design §3.5.1 follow-up delta list): the
   cross-check associates columns **by name** — declared order is the
   envelope's declared sequence, physical HDF5 enumeration is
   lexicographic/non-semantic — and per-column sample arrays are
   compared element-wise **by row index** against the expectation,
   never via enumeration order; a cross-check mismatch fails visibly.
6. **F-A regression guard — plain destructive load is never the supported
   path.** Negative/exclusion assertions: the supported inspection entry
   point never opens the original file with the legacy backend loader; no
   Phase 3 test performs a plain `HDF5DataBackend.load_group` on an
   original file as a supported operation (the destructive chain is
   exercised only on synthetic copies, exactly as Phase 1 did). The
   original file's sha256 is unchanged after every supported inspection —
   including inspections that fail.
7. **Scratch cleanup with independent diagnostics.** Scratch resources are
   disconnected and cleaned up with diagnostics separate from the primary
   result (decision §2 step 4); the original remains byte-identical even
   when scratch cleanup itself fails (A4 row 4 interplay asserted here for
   the success path).
8. **Signal-profile positive fixture (developer review finding 5 —
   conditional per decision §1).** Construct a run group via the Phase 2
   seam with `PROFILE_SIGNAL`, attach a synthetic record per the A1
   source-of-record paragraph, persist through the owned save path, and
   inspect via the safe path. Assert: the decoded envelope carries
   `profile == 'signal'`; hydration carries `run_record` under the
   additive key **and no legacy keys** — the signal profile's supported
   hydration key set is the **empty set**, itself a frozen literal
   (per-profile key sets per the **[P3P-01]** extension); no thermal
   `_META_KEYS` or `thermal_manifest` is imposed on the signal record
   (the A2.4 absence-assertion applied positively, against a real
   fixture). Applicability is stated plainly: decision §1 makes signal
   support conditional on its owned persistence path existing, so if the
   design freeze keeps signal-monitor owned persistence deferred (design
   §9 conditional item), **this row is deferred to Phase 4 verbatim**
   with decision §1's conditional clause cited in the results file; the
   applicability decision is recorded at design freeze either way.

## A3 — Fresh-process / separate-process inspection

**Requirement basis:** decision §2 steps 1–4 (clean scratch, no in-session
context); PRD M03-AC04 ("independent fresh-process inspection");
plan evidence-matrix item 5 ("Save, reopen and inspect in a separate
process"); design §9.

1. **Write in one process, inspect in a clean subprocess.** A parent test
   writes a synthetic record via the owned path (A1), then spawns a fresh
   interpreter (subprocess with the project venv, a dedicated bootstrap
   module per **[P3P-04]** modeled on `tests/thermal_cli_bootstrap.py`,
   `-W error` where warnings must surface) that performs the full safe
   inspection and emits a JSON payload. Assert: exit code 0; the payload
   carries the decoded envelope, identity, row counts, and sample facts
   equal to what the parent stored; per the design §4.3 payload rule
   (design §3.5.1 follow-up delta list), the payload's
   `records[*].columns` is asserted in h5py enumeration order
   (lexicographic — as physically stored, non-semantic), while the
   declared order is carried by the envelope; reconstruction in
   envelope (declared) order by same-named dataset/column association,
   comparing sample arrays by row index, is the Phase 4 by-name oracle
   rule frozen at design §3.5.1 item 5 — stated here as a
   **forward-reference only, not a Phase 3 assertion**; the parent
   then re-hashes the original — unchanged.
2. **No legacy context in the child.** The child process holds no thermal
   example state, no prior `DataGroup`, and no in-memory envelope; every
   fact in its payload comes from the stored artifact via the safe path
   (asserted by construction — the child only receives the file path and
   the group id).
3. **Legacy absence vs corrupt/unsupported metadata — three-way
   distinction** (decision §6 item 6; §4 F-A/F-B conditions; design §9
   fourth deferral item):
   - **Absent additive key** (a genuine legacy-only thermal v1/v2 file):
     the inspection reports legacy absence — a truthful "no additive common
     record" outcome with the legacy context still available, never an
     error, never a fabricated envelope.
   - **Present non-text value** (e.g. an int written via raw `h5py` on a
     synthetic copy fixture): a visible failure of the frozen type
     **[P3P-02]**, never treated as legacy absence.
   - **Malformed / wrong-namespace / unsupported-version / over-size text**
     (each constructed as a separate one-defect fixture written through raw
     `h5py`): visible failure with the codec's frozen diagnostics
     (`RecordDecodeError`/`RecordValidationError` templates from Phase 2
     §8.4) surfaced by the inspection path — **never** silent, never
     absence.
4. **Multi-group file.** A file with two stored groups inspected with an
   explicit `group_id` succeeds; without one it fails visibly (exactly-one
   rule, matching the characterized Phase 1 behavior). Group discovery is
   via read-only HDF5 keys on the copy with explicit identity validation —
   empty or `None` generic-helper output is never interpreted as an empty
   store (F-D condition).

## A4 — Fault matrix

**Requirement basis:** decision §2 (connect/save status checks; primary and
cleanup diagnostics retained independently); §4 F-A condition; PRD M03-AC04
("serialization, save, disconnect and reload faults have tested, truthful
primary and cleanup evidence"); PRD phase 3 row ("Test serialization, save,
close/disconnect and invalid reload failures, original-byte preservation and
separately observable primary/cleanup diagnostics. No atomicity or
failed-run persistence is implied."); plan evidence-matrix item 4 (lifecycle
and failure); plan §"Product authority" ("define and test serialization,
save, close/disconnect and malformed/unsupported reload failures: specify
which original bytes remain intact and how independently observable primary
and cleanup diagnostics survive").

**Injection discipline (binding):** every fault is injected at its **real
seam** with the mechanism frozen by the design **[P3P-05]** — no mocks that
skip or replace the real code path. Call instrumentation, where needed for
observability, uses the Phase 1 recording-spy technique (plain functions
appending `mock.call(...)` and delegating to the original), patched without
`autospec=True`, because 3.9 autospec does not delegate to `wraps`
(CPython gh-75988, fixed 3.11.9+/3.12/3.13 only) — a 3.13-only pass is not
evidence (Phase 1 correction record).

**Failure-stage verification mandate (binding — developer review finding
2):** the example parentheticals in the A4.2/A4.3 rows below are
*unverified candidates* against the real backend. Verified source facts
this revision (`softlab/shui/data/hdf5.py`, `backend.py`): a target path
that is an existing directory raises at **connect** (`h5py.File(path,
'a')`, `hdf5.py:39`), not at save; a parent directory removed after a
successful connect does **not** fail the save on POSIX — h5py writes
through the open descriptor into the unlinked inode, so the save can
succeed silently (vacuous probe); h5py `File.close()` is idempotent, so
an externally closed handle or a removed scratch directory does not
necessarily fail disconnect either. **[P3P-05]** therefore mandates that
every frozen fault mechanism is **empirically verified to fail at the
named stage on both supported interpreters (Python 3.9 and 3.13)**
before design freeze and before any test freezes expectations: a seam
failing at the wrong stage **reclassifies its row** (e.g. a connect-time
candidate moves to the connect-class row A4.9); a seam that does not fail
at all is vacuous and invalidates universal assertion (a). Each A4 row
below asserts the stage actually observed for its frozen mechanism, not
the stage the row name suggests. One verified save-failure seam exists
today for reference: connect to a valid path, externally `close()` the
h5py handle, then call the real save — `save_impl` (`hdf5.py:65`)
operates on a closed file and fails through `catch_error`, with the
error recorded in `backend.errors` (`backend.py:169-188`) — exactly the
observable universal assertion (a) requires.

**Universal assertions for every row:** (a) the frozen error type
**[P3P-02]** is raised/surfaced (never logged-and-swallowed, never a
`None`-means-absence result — the F-C lesson: a `None` helper result is not
evidence of absence or successful inspection); (b) the original file's bytes
remain intact (sha256 before/after). The "no artifact appears at the target
path" claim applies **only to pre-connect faults (A4.1)** — `connect_impl`
opens `'a'` mode and creates the target at connect (`hdf5.py:39`), so any
fault after a successful connect (A4.2 save, A4.3 disconnect, A4.9's
candidates) leaves an artifact at the target path. For every post-connect
row the artifact state is **pinned per row** by the frozen design
**[P3P-03]/[P3P-05]** and asserted exactly — one of, e.g., "an empty
connect-time file remains containing exactly the connect-time superblock
and no group/dataset/record content" or "removed by cleanup" — whichever
the design freezes, never left unasserted (developer review finding 3);
(c) **primary and cleanup diagnostics are
separately observable** — each retrievable independently of the other via
the frozen container **[P3P-03]**, including the row where the cleanup
diagnostic itself reports a failure; (d) no partial state is presented as
success (no atomicity is claimed; absence of atomicity must not be hidden
either — the observable outcome states exactly what is and is not
guaranteed).

| # | Fault | Injection seam (real path) | Expected behavior |
| --- | --- | --- | --- |
| A4.1 | **Serialization failure** (envelope invalid at the call-site gate — e.g. a count drift the Phase 2 codec rejects) | Before any backend connect: the owned save's encode step | `RecordValidationError` (frozen Phase 2 type) propagates; **zero** HDF5 attribute or dataset writes occurred (recorder double + file non-existence/byte-equality at target); no backend resource remains connected. |
| A4.2 | **Save failure** | Owned backend save against a failing real target — candidate mechanisms (existing-directory target, removed parent directory, externally closed handle) are **unverified candidates** pending the [P3P-05] empirical stage verification | Frozen storage error **[P3P-02]** surfaced at the **actually observed stage** (mandate above): if the frozen mechanism fails at save, save-stage error asserted; a candidate verified to fail at connect is reclassified to A4.9 and never asserted here. Pre-existing target (if any) bytes unchanged; post-connect artifact state pinned and asserted per-row [P3P-03]/[P3P-05]; connect and disconnect state checked and reported; cleanup diagnostic present and truthful. |
| A4.3 | **Close/disconnect failure** | Real seam where disconnect can genuinely fail — candidate mechanisms (externally closed handle, scratch removed before disconnect) are **unverified candidates**: h5py `File.close()` is idempotent, so neither candidate is guaranteed to fail (verified fact, mandate above); mechanism frozen per [P3P-05] after empirical stage verification | Primary result (inspection outcome) still delivered; cleanup failure reported in its **own** diagnostic slot **[P3P-03]** — neither diagnostic masks the other; original bytes unchanged; post-connect artifact state pinned and asserted per-row [P3P-03]/[P3P-05]. If no real disconnect-failure seam verifies on both interpreters, this row is recorded as BLOCKED with the evidence, not faked with a mock. |
| A4.4 | **Invalid reload — corrupt JSON** | Synthetic copy whose `run_record` text is replaced with `'not json'` via raw `h5py` on the copy | `RecordDecodeError` with the frozen `run_record: malformed JSON: ...` template; visible failure, never legacy absence; original untouched. |
| A4.5 | **Invalid reload — wrong namespace** | Text carrying a foreign/`softlab.thermal-reference` namespace where the common envelope is expected | Frozen validation failure **[P3P-02]**/codec error per the frozen contract; never silently reinterpreted as a legacy record. |
| A4.6 | **Invalid reload — unsupported version** | Text with `version` 2 under the common namespace | `RecordValidationError`, frozen `version: unsupported version (supported: 1)` template; never treated as legacy absence. |
| A4.7 | **Invalid reload — over-size** | Text exceeding `MAX_ENCODED_BYTES` on disk | `RecordDecodeError`, frozen `run_record: encoded size exceeds limit 131072 bytes` template; rejected before full parse; original untouched. |
| A4.8 | **Preflight failure** (reserved attribute missing/non-text on a malformed synthetic file) | Hand-built h5py fixture missing `id`/`backend` attrs | Frozen preflight error **[P3P-02]** raised **before** any legacy load (call inventory asserts zero loads); failure is visible and specific. |
| A4.9 | **Connect failure** (lifecycle stage preceding save; the matrix's connect-class row — developer review finding 2) | Target path failing at connect per the mechanism frozen by **[P3P-05]** (verified candidate: existing directory at the target path — `h5py.File(path, 'a')` raises at connect, `hdf5.py:39`; any save-class candidate verified to fail at connect is reclassified here) | Frozen storage error **[P3P-02]** surfaced at the **connect** stage; save never attempted (recorder inventory: zero save calls); no artifact at the target path or artifact state pinned per-row [P3P-03]/[P3P-05]; connect/disconnect state checked and reported; primary and cleanup diagnostics independently observable [P3P-03]. |

**Not claimed (recorded in results, not tested as a promise):** durability of
failed/cancelled/partial runs, rollback, retry, atomic publication,
crash recovery (decision §2 NON-support). The fault matrix asserts
truthful failure evidence only.

## A5 — Stage honesty

**Requirement basis:** decision §1.1 acquisition-outcome row and §6 item 5;
PRD M03-AC03 ("An acquisition snapshot cannot claim later persistence,
plotting or cleanup success"); plan §"Product authority" ("A pre-save
snapshot cannot certify save, plotting or cleanup"); design §1.5.

1. **Stored snapshot literals.** Every stored envelope asserted from disk
   carries `outcome.snapshot == 'completed_acquisition_before_persistence'`
   (frozen), `outcome.acquisition == 'completed'`, and
   `persistence`/`plotting`/`cleanup`/`overall` all
   `'not_yet_established'` — asserted from the **stored artifact**, not the
   in-memory envelope.
2. **Negative probes.** A synthetic envelope claiming any later-stage
   success (or a failed/cancelled acquisition) is rejected before save
   (A4.1 reuse of the Phase 2 frozen codec behavior at the storage gate);
   the write path contains no code path that rewrites the outcome after
   save/plot/cleanup (recorder double: no `set_meta` call on **any**
   key after the save's attribute write — the outcome lives inside the
   single `run_record` text; there is no separate outcome metadata key,
   so an "outcome key" assertion would be unimplementable).
3. **Post-persistence observation is outside the record.** Test comments
   and results explicitly record that save success proven by these tests is
   a test observation, not a claim stored inside the record; the record
   never acquires a later outcome history (decision §2 NON-support).
4. **Unavailable-fact honesty (structural side).** Where the stored
   envelope carries unavailable software/source facts, tests assert the
   frozen `state: 'unavailable'`/`value: null`/bounded-`reason` structure;
   capture-truthfulness of available facts remains a Phase 4 wiring concern
   per Phase 2 S1.7/F15, not re-litigated here.

## A6 — Unsupported paths

**Requirement basis:** decision §2 NON-support and §4 D2/F-C/F-D
dispositions; PRD M03-AC04 ("Unsupported backend and partial-result cases
are explicit"); plan evidence-matrix item 3 ("tests of one do not certify
another"); design §9 (permanently out of scope list).

1. **SQLite explicitly rejected.** Attempting record persistence/inspection
   through `Sqlite3DataBackend` fails with a truthful, explicit
   unsupported error **[P3P-02]** — not a silent no-op, not an empty-meta
   "success" (D2: SQLite's `{'sample', 'run_id'}`-only meta and empty
   reconstructed meta are recorded debt; selecting SQLite would make D2
   blocking and requires a separately reviewed decision). The error names
   HDF5 as the only supported backend.
2. **Generic io helpers are not the supported path.** The Phase 3
   save/inspection entries never route through `io.save_group`,
   `io.reload_group`, or `io.load_groups`:
   - **F-C regression guard:** a probe exercising `io.reload_group` on a
     record with populated valid `group.backend` still returns `None`
     (characterized defect, errors swallowed) — recorded as evidence that
     the supported path cannot depend on it; the supported path's
     call inventory (A2.3) shows zero generic-helper calls.
   - **F-D regression guard:** `io.load_groups('sqlite3', ...)` returning
     `[]` on a non-empty database is re-recorded as the reason empty
     generic-helper output is never interpreted as an empty store; the
     supported discovery path uses read-only HDF5 keys on a copy (A3.4).
   - These two guards are **characterization re-assertions** on unchanged
     production code (defects remain tracked, not repaired — no source fix
     is tested or implied).
3. **Same-ID deletion/recreation** is not atomic publication: a probe that
   deletes and recreates a same-ID group observes and records the actual
   (non-atomic) behavior without claiming it as supported publication.
4. **Partial-result / failed-run durability** is explicitly unsupported:
   no test stores a failed or cancelled acquisition, and the results file
   records this exclusion verbatim (decision §2 NON-support).

## A7 — Legacy compatibility

**Requirement basis:** decision §6 items 7–8; §4 F-B/F-C/F-D conditions;
PRD M03-AC05 legacy half ("Existing v1 coordinated/v2 direct and legacy
behavior remain compatible within reviewed scope"); plan evidence-matrix
item 3; Phase 2 S6 conventions.

1. **Existing thermal suites pass unmodified.** `test_thermal_manifest.py`,
   `test_thermal_example.py`, `test_thermal_execution.py`,
   `test_thermal_adapters.py`, `test_thermal_cli_warnings.py`,
   `test_thermal_metadata_contracts.py` keep passing as-is against Phase 3
   code; dual-write must not alter the legacy manifest bytes, versions
   (1/2), timing meanings, or the eleven-field closed contract.
2. **Stored v1/v2 files remain legacy records.** Synthetic legacy files
   produced before the Phase 3 change are read after the change with the
   legacy readers and asserted content-equal; a legacy file **without** the
   additive key behaves exactly as before (legacy absence semantics
   unchanged — Phase 2 S6.5 carried into the storage layer via A3.3).
3. **Full suite green.** `python -m unittest discover -s tests -p
   'test_*.py'` passes with the Phase 3 suite added: the 459-test baseline
   (405 pre-existing + 54 Phase 2) grows only by Phase 3 additions; zero
   pre-existing tests removed, skipped, or weakened.
4. **Import paths and export surface.** Existing public import paths
   resolve identically; any Phase 3 export addition is exactly the frozen
   design table **[P3P-06]** — no wildcard imports, no drift beyond it.
5. **Phase 2 suite unchanged and reused.** `tests/test_m03_p2_schema.py`
   passes unmodified (codec contract untouched by Phase 3 integration).

## A8 — Hygiene and portability

**Requirement basis:** Phase 1 C6; Phase 2 S7; the Phase 1 CI corrections;
AGENTS.md verification conventions; PRD M03-AC06 environment-recording half.

1. **Synthetic data only.** All fixtures programmatically constructed;
   UUIDs/digests/versions captured or generated at test time; no real
   experiment data, no VISA/hardware, no notebooks executed.
2. **Temp dirs only.** All file I/O under
   `tempfile.TemporaryDirectory(prefix='softlab-m03-p3-')`; cleanup asserted
   (temp root gone after each test); produced artifacts are exactly the
   declared synthetic outputs; nothing generated is committed.
3. **Connections released.** Every owned `HDF5DataBackend` created by the
   tests is tracked and disconnected at cleanup (harness double per Phase
   1); the full run emits zero `ResourceWarning`s — matching the
   zero-warning baseline (459-test record).
4. **Version-portable assertions (3.9 + 3.13).** No `autospec=True` with
   `wraps` anywhere (recording spies instead); no assertion hardcodes a
   version string as universal; warning expectations gated on the actual
   emitting version (e.g. the 3.13-only `sqlite3.version` deprecation
   pattern); subprocess probes run on both interpreters where behavior
   differs by interpreter. The suite must be checked on **both** Python
   3.9 and 3.13 before handoff — a 3.13-only pass is not evidence (Phase 1
   correction record; local 3.9 via scratch env per that record, or the
   candidate commit's CI matrix result recorded).
5. **Environment actually observed.** Each recorded run states Python,
   softlab, numpy, pandas, h5py (+ libhdf5), sqlite3 versions actually
   observed on each interpreter; no version asserted as a pass condition.
6. **Warnings verbatim.** Any warning emitted is recorded verbatim in the
   results file; none suppressed or filtered inside test code; deliberate
   failure-probe stderr lines (e.g. backend `_logger.error` output, as in
   Phase 1's C4 probe) recorded and identified as expected output.
7. **No global state leakage.** No `sys.set_int_max_str_digits`; no
   scheduler singleton mutation; no module-global default backend created;
   each test owns its resources (Phase 2 S5.5 conventions carried over).
8. **Verification commands** recorded per run with actual results:
   `python -m unittest discover -s tests -p 'test_*.py'`,
   `python -m compileall -q softlab`,
   `python -c "import softlab; print(softlab.__version__)"`, plus
   `python -W once -m unittest discover ...` for the warnings scan.

## A9 — Results recording

- Executable tests (after review approval and design freeze, in a later
  step) will live in `tests/` following the established convention
  (`test_m03_p1_characterization.py`, `test_m03_p2_schema.py`):
  synthetic fixtures, assertion-based, one defect per negative test.
- Execution results will be recorded in
  **log/release_12/test/storage-test-results.md** with one clearly headed
  section per plan area (A1–A8 above), each ending with a per-area
  **verdict**: `PASS` (all planned assertions executed and passing),
  `FAIL` (with bug report reference in
  [bugs.md](bugs.md) reported to sw-tom), or `BLOCKED` (with reproduction
  evidence and the scoped decision input it feeds). A blocked area is never
  reported as a pass.
- Every recorded run includes: exact commands, Python/dependency versions
  actually observed on **each** Python version run (3.9 and 3.13 at
  minimum), all warnings emitted (none waived, verbatim), pre/post
  full-suite test counts (baseline 459), original-byte sha256 evidence for
  every inspection, and applicability notes for reused Phase 1/Phase 2
  evidence.
- Reused evidence rule: Phase 2 codec results (54/54 PASS) and Phase 1
  characterization facts are reused only where content is unchanged,
  stated explicitly per item; anything affected by Phase 3 code
  (backend interaction, example integration, stored artifacts) is
  re-executed.
- Traceability: the results file maps each verdict back to the mandating
  decision section (§1.1 rows, §2 ownership/NON-support, §4 D1/D2/F-A/
  F-B/F-C/F-D conditions) / PRD criterion (**M03-AC03**, **M03-AC04**,
  plus M03-AC05 legacy half and M03-AC06 environment half where
  applicable) / plan matrix items **3** and **4** — and, after the Phase 3
  detailed design is frozen, to its sections, so the acceptance review can
  audit coverage without re-reading test code.

## Placeholder list

Literals the Phase 3 detailed design must freeze before test code is
written (each tagged **[P3P-nn]** in the body):

| ID | Literal(s) to freeze |
| --- | --- |
| P3P-01 | Phase 3 module path(s) and public entry points: owned save function, safe inspection function — exact names and signatures; plus (a) the sample/record attachment mechanism (`DataGroup.add_record(...)` today vs a frozen save-signature seam), (b) the per-profile hydration key set (thermal: three `_META_KEYS` + `thermal_manifest`; signal: empty set — a frozen literal), (c) the meaning of "legacy context still available" for A3.3 per seam placement (raw manifest text vs decoded legacy envelope; interacts with P3P-06). |
| P3P-02 | Storage-layer error taxonomy: exception class names, inheritance, frozen message templates for save/connect/disconnect/preflight/inspection failures (distinct from the frozen Phase 2 codec taxonomy). |
| P3P-03 | Primary-vs-cleanup diagnostic container: type, shape, and the observable independence contract (each retrievable when the other is absent or itself failed); also the pinned per-row post-connect artifact state for A4.2/A4.3/A4.9 (empty connect-time file vs removed-by-cleanup — one must be chosen and asserted per row). |
| P3P-04 | Fresh-process inspection entry point: subprocess bootstrap module, CLI surface, exit codes, JSON payload schema (modeled on `tests/thermal_cli_bootstrap.py`). |
| P3P-05 | Real-seam fault-injection mechanism per A4 row (save-failure target, disconnect-failure seam, corrupt-fixture construction rule) — no mock that skips the real code path; **plus the mandatory empirical failure-stage verification on both interpreters (3.9 and 3.13) with the wrong-stage reclassification rule** (finding 2). |
| P3P-06 | Seam placement (`softlab/shui/` vs example layer) and the exact export-surface delta for `softlab/shui/__init__.py`. |
| P3P-07 | Unavailable-reason policy for storage-level facts not capturable by the frozen envelope vocabulary. |

## Gate linkage

Per-area verdicts in `storage-test-results.md` are the test-side evidence
for **M03-AC03** and **M03-AC04**, for plan evidence-matrix items **3**
(legacy and storage) and **4** (lifecycle and failure), and for the §4
disposition conditions F-A/F-B/F-C/F-D/D1/D2 on the supported path. Phase 3
does **not** claim M03-AC05 oracle evidence (Phase 4) and does not alter the
Phase 1 decision or the Phase 2 codec contract. All non-placeholder
assertions are anchored in frozen literals (Phase 2 §8 table, decision
text); placeholder assertions are aligned by direct quote-check once the
Phase 3 detailed design is reviewed and frozen, then this plan is
re-reviewed if any placeholder's freeze differs from the expectations
stated here.

(End of plan.)
