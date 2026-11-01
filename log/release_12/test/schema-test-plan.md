# Release 12 (M03 Phase 2) — Schema/Codec Test Plan

Owner: Tester (sw-mike) | 2026-11-01 Asia/Shanghai
Status: **DRAFT — awaiting developer re-review, then architect/design review
per the project gate order; no test code and no source changes before approval**
Branch: `codex/m03-p2-schema`
PRD: [log/release_12/prd.md](../prd.md) (Phase 2 row, **M03-AC02**)
Plan: [plan/milestone-03.md](../../../plan/milestone-03.md)
(Phase 2 row; codec/versioning evidence matrix item 2)
Binding input: [p1-boundary-decision.md](../design/p1-boundary-decision.md)
(option (a), section 1.1 exact boundary, **section 6 binding constraints**)
Format conventions follow
[characterization-test-plan.md](characterization-test-plan.md) and its executed
results in [characterization-results.md](characterization-results.md).

## Scope and ground rules

This is a **planning document only**. No test code is written and no source
under `softlab/` is modified until this plan passes independent review. Phase 2
test code targets **the Phase 2 schema/codec module only** — strict JSON-text
encode/decode/validate of the additive common envelope and the two closed
client payload validators/construction seams — plus a legacy-preservation
regression layer over the unchanged thermal v1/v2 machinery. Persistence
integration (HDF5 `set_meta` write path, copy/preflight/hydration inspection)
is **Phase 3 scope** and is deliberately excluded here except where noted as
legacy readability checks (S6).

Test names, module paths, literal key/namespace strings, exact field lists and
numeric limits below are written against the reviewed Phase 2 detailed design
once it exists; this plan pins the **testable requirements and their evidence**
and must be updated by direct diff check if the design freezes different
literal values. Anything this plan asserts about literals is provisional until
design freeze and is marked as such.

### Placeholders pending design freeze

The following are Phase 2 design outputs; tests reference them symbolically
until frozen, then assert the frozen literals verbatim:

- the additive namespace string and shared HDF5 metadata key;
- the common format version value and each client payload version value;
- the recognized client-profile identifiers (`thermal`, `signal`);
- the exact required/optional field lists of the common envelope and of both
  client payloads;
- conservative encoded size and nesting-depth limits;
- the exact exception types raised per failure class;
- the availability/unavailable state literal, the bounded
  unavailable-reason string vocabulary, and the per-field reason bound —
  these are owned by the detailed design (decision §6 item 1 requires the
  design to freeze "unavailable-reason rules"), and this plan asserts
  whatever the frozen design states for them.

## S1 — Valid round-trips

**Requirement basis:** decision §1.1 availability row, §6 items 1, 2, 5;
PRD M03-AC02 ("valid supported values round-trip"); plan evidence matrix
item 2.

1. **Minimal valid envelope, thermal profile.** Encode the smallest envelope
   the design marks valid (all required common fields, thermal payload with
   all its required fields, optional fields absent where permitted). Decode
   the result and assert **deep equality** with the original value: no field
   added, dropped, reordered-into-different-semantics, or coerced. Assert the
   decode of the encoded text is idempotent (second decode of the same bytes
   is equal to the first).
2. **Minimal valid envelope, signal profile.** Same as S1.1 for the signal
   client payload, including truthful unitless declarations for genuinely
   unitless channels and the source-column declarations (`timestamp`,
   `signal`, `reference`) with `difference` **absent** from stored source
   columns (decision §1.2).
3. **Deterministic byte-level stability.** For each valid envelope in S1.1 and
   S1.2, encode twice from independently constructed but equal inputs and
   assert **byte-identical** encoded text (no hash-set iteration order, no
   timestamp, no UUID dependence inside the codec — identity fields are
   caller-supplied data, not codec-generated). Assert canonical key ordering
   if the design mandates it; assert encoded text is valid strict JSON
   (`json.loads` round-trips it) as an independent cross-check.
4. **Identity fields preserved distinctly.** Envelopes carry (a) the bounded
   execution UUID and (b) the persistent group UUID as **separate fields**;
   tests feed two provably different UUIDs and assert both survive
   encode→decode with their roles unchanged, and that the codec never
   populates one from the other (decision §6 item 5). A synthetic envelope
   where both UUIDs are equal must fail the identity cross-check, not pass as
   a degenerate success, if the design mandates distinctness at validation.
   **Freeze note:** this is provisional only until design freeze — at
   freeze it becomes a **hard assertion** (a hard rejection assertion if
   the frozen design mandates distinctness, or an explicitly recorded
   "not mandated" statement if it does not), never a conditional in the
   executable suite.
5. **Sample schema/ordering/units/counts relations.** Valid thermal and
   signal envelopes declare ordered column schemas with a truthful unit for
   every stored column, plus requested/completed row counts. Tests assert:
   envelope column order is preserved exactly through the round-trip; every
   declared column has an explicit unit entry (or an explicit unitless
   declaration per profile semantics, never an invented or blank
   success-like unit); declared counts are exact integers and the
   completed ≤ requested relation holds where the design states it. Samples
   themselves are **not** in the envelope (decision §6 item 2); a probe
   envelope smuggling sample arrays into the metadata blob is rejected (S2).
6. **Acquisition-snapshot-only outcome.** Valid envelopes carry
   `snapshot == 'completed_acquisition_before_persistence'` (or the frozen
   literal) with acquisition `completed` and `persistence`, `plotting`,
   `cleanup`, `overall` all `not_yet_established`. Round-trip preserves
   these verbatim; an envelope claiming any later-stage success is rejected
   (S2).
7. **Availability facts truthful and explicit.** Valid envelopes carry the
   observed software/source/environment availability facts decided in
   §1.1. Tests assert the frozen semantics verbatim: a captured fact
   carries its observed value; an **uncaptured** fact is **explicitly
   unavailable** — it carries the frozen unavailable-state literal plus a
   reason string from the frozen bounded vocabulary — never a value copied
   from another client and never a success-like stand-in (decision §1.1
   availability row; §6 items 1 and 5). Both states round-trip verbatim
   through encode→decode, and the reason string stays inside the frozen
   per-field bound (its rejection side is F16; its count bound is S4.3).

## S2 — Failure matrix

**Requirement basis:** decision §1.1 availability row, §6 items 1, 2, 3, 4,
5, 6; PRD M03-AC02 ("Tests
reject duplicate keys, unsupported values and nonfinite numbers without
implicit stringification"); plan evidence matrix item 2.

Each row is a separate test feeding **one defect** into an otherwise valid
envelope (or into the raw decode input). For every case the test asserts:
(a) an exception is **raised** (not logged-and-continued, not a None/error
result); (b) the exception **type** matches the design's specified type for
that class; (c) the message **discipline** — mentions the offending field/
key and the defect class, and does not silently continue; (d) **no
persistence side effects** occurred (S5).

| # | Defect | Expected behavior |
| --- | --- | --- |
| F1 | Malformed JSON text at decode | Decode raises the design's malformed-input error; message identifies malformed JSON, not a generic failure. |
| F2 | Wrong root type (top-level array, string, number, bool, null instead of object) | Rejected: root must be a JSON object. |
| F3 | Duplicate object keys in the raw JSON text | Rejected via strict duplicate detection (the design's `object_pairs_hook`-class technique); the duplicate key name appears in the error. Nested duplicates (inside client payload) rejected equally. |
| F4 | Unsupported value types: Python object, set, bytes, complex, NumPy scalar/array, callable, arbitrary class instance | Encode rejects **without** `str`/`repr` fallback (decision §6 item 3); the error names the field and the unsupported type. NumPy values are rejected unless the client constructor normalized them first — test the un-normalized path reaching the codec directly. |
| F5 | Nonfinite numbers: `NaN`, `+Inf`, `-Inf` at any nesting depth, including inside nested arrays/objects inside the client payload | Rejected on encode, and rejected on decode of text containing `NaN`/`Infinity` tokens. **Decode requires more than token rejection:** `parse_constant` alone does not fire on overflow literals — `json.loads('1e999')` yields `inf` through ordinary float parsing — so the design must mandate (and tests must exercise) a **recursive post-parse finiteness sweep** over the decoded value at any nesting depth, in addition to the token-level `parse_constant` rejection. Tests probe both classes: token probes (`NaN`, `Infinity`, `-Infinity`) and `1e999`-class overflow literals, each at top level and inside a *nested container three levels down* (shallow-only checks are a known gap class). The encode side is covered by the NaN/±Inf encode probes in this row plus the design's finite check before `allow_nan=False` serialization. |
| F6 | Missing required common field (each required field, one test or parametrized equivalent) | Rejected; error names the missing field. Absence of an optional field is **not** an error (S3). |
| F7 | Missing required client-payload field (per profile, per the frozen required list) | Rejected by the owning profile's validator; common layer's dispatch is exercised so the error attributes the payload to the profile. |
| F8 | Unsupported common-format version (e.g. version+1, version 0, version as string) | Rejected as unsupported-version error; message states the supported version. |
| F9 | Unsupported client-payload version (per profile) | Rejected likewise; version fields are independent of thermal manifest versions 1/2 and the additive format version is never treated as a thermal v3 (decision §6 item 7). |
| F10 | Unsupported/unrecognized client-profile identifier | Rejected as unknown-profile error; no silent fallback to a default profile. |
| F11 | **Unknown/extra fields — policy per design.** The decision mandates **closed** records: "unknown fields ... are errors rather than silently ignored extensions" (§1.1 namespace row, §6 item 4). Tests therefore assert **rejection** of any key not in the frozen allowlist, at the common layer and inside each client payload, including keys that differ only by case or by a trailing/leading underscore. For traceability, the results file also records the **rejected alternative** (unknown-fields-preserved) and cites the mandating decision text, so a future boundary change is an explicit decision revision, not a silent test relaxation. |
| F12 | Type errors on present fields (string where integer required, float where exact integer required, object where array required) | Rejected per field with the field named; `1.0` where an exact integer is required is a type error, not a success. |
| F13 | Profile-specific semantic violations at the client validator (thermal: six-column ordering, clock/delay policy, v1/v2 source-tick distinction; signal: declared source columns, derived `difference` smuggled into stored columns) | Rejected by the owning client validator with profile-attributed errors; the common layer does not reinterpret payload internals. |
| F14 | Present `null` on a field to which the frozen schema assigns **no null meaning** (one test per such field, or parametrized equivalent) | Rejected per field, with the field named; distinct from absent (F6) and from an invalid present value (F12) — the frozen policy pins exactly which fields, if any, accept explicit `null` (decision §6 item 6). Fields whose schema assigns a null meaning are **not** fed through this row; their acceptance side is asserted in S3.5. |
| F15 | Uncaptured software/source/environment availability fact presented as a success-like value (e.g. an invented version string, copied value from another client, or empty/zero stand-in for an observation that was not captured) | Rejected by the owning validator: uncaptured facts must be explicitly unavailable, never copied from another client or inferred as success (decision §1.1 availability row; §6 items 1 and 5). |
| F16 | Availability reason string outside the frozen bounded vocabulary, or missing where the frozen schema requires one with the unavailable state (and vice versa: reason present without the unavailable state) | Rejected with the offending field named; the unavailable state and its reason are bound together per the frozen unavailable-reason rules (decision §6 items 1 and 6). The reason string exceeding the frozen per-field bound is rejected at the count-boundary side (S4.3). |

## S3 — Boolean-vs-number and type strictness

**Requirement basis:** decision §6 item 3 ("no silent coercion of
booleans/numbers"); plan evidence matrix item 2 ("Test booleans versus
numbers").

1. `True` / `1` confusion: an envelope field whose schema type is integer
   fed JSON `true` (and an encode of Python `True` into an integer-typed
   field) is **rejected**, even though Python `True == 1`. Symmetric case for
   `False` / `0`.
2. Boolean field fed `1`/`0` (number, not boolean token) is rejected; a
   number field fed `true`/`false` is rejected.
3. **No implicit stringification anywhere:** no field accepts an unsupported
   value via `str()`/`repr()` coercion; a probe object whose `__str__` is
   benign still fails encode at F4. Strings that *look* numeric (`"3"`) fed
   to numeric fields are rejected (no implicit `int()`/`float()` parse either
   direction).
4. JSON distinguishes `1` (int) from `1.0` (real) only per the design's
   exact-integer rule; where the design allows finite reals, `1.0` is valid;
   where it requires exact integers, `1.0` is rejected (F12) — both variants
   pinned by the frozen schema and asserted, not assumed.

### S3.5 — Null versus absent (frozen policy)

**Requirement basis:** PRD M03-AC02 ("absent versus null"); decision §6
item 6 ("Define required, optional, absent, explicit-null, unavailable, and
invalid-present behavior explicitly. Optional absence never implies success,
zero, empty, or not-applicable"). The frozen schema pins exactly which
fields, if any, assign a meaning to explicit `null`; these checks assert the
frozen policy verbatim, one defect per negative test, distinct from F6
(absent), F12 (invalid present value) and F14 (present null where no null
meaning is assigned).

1. **Rejection side.** For every field the frozen schema classifies as
   no-null-meaning, a present `null` is rejected with the field named (F14).
   The set of such fields is taken from the frozen schema, not assumed.
2. **Acceptance side.** For every field the frozen schema **assigns** a
   null meaning (e.g. availability facts mirroring the legacy manifest's
   `digest: None` + bounded reason pattern, per decision §1.1), an explicit
   `null` is **accepted** and round-trips verbatim through encode→decode,
   preserving its assigned semantics.
3. **Distinctness.** For an optional field with an assigned null meaning,
   the three states — absent, present-null, present-valid-value — are
   asserted to decode to three semantically distinct results; in
   particular, absent never collapses to null, and neither absent nor
   null is ever reported as success, zero, empty, or not-applicable
   (decision §6 item 6 verbatim).
4. **Whole-record edge.** Absence of the whole additive key means only "no
   additive common record"; a present non-text, malformed, invalid, or
   unsupported-version value is an error, never legacy absence (decision
   §6 item 6; the legacy-absence side is covered in S6.5).

## S4 — Size/depth limits

**Requirement basis:** decision §6 item 1 ("conservative encoded size/depth
limits ... frozen in the reviewed Phase 2 design").

1. **Depth:** nesting deeper than the frozen maximum depth is rejected with a
   depth/overflow-class error; exactly-at-limit valid input is accepted
   (boundary test both sides).
2. **Size:** encoded text exceeding the frozen byte limit is rejected; the
   largest accepted input is asserted to be exactly at the limit (boundary
   test). Tests construct the oversize input programmatically so no large
   literal is committed.
3. **Count bounds:** arrays/objects whose element counts exceed frozen
   bounds (e.g. column-declaration arrays, availability reason strings) are
   rejected; at-boundary counts are accepted. The reason-strings bound
   applies to the availability/unavailable-reason facts of S1.7/F16: its
   numeric value and the reason vocabulary it bounds are **owned by the
   detailed design** (decision §6 item 1 requires the design to freeze
   "unavailable-reason rules"), and the test asserts whatever bound the
   frozen design states — read from the module's exported constant where
   one exists (S4.4).
4. Limits are asserted to be **the design's frozen values**, not test-local
   guesses: the test reads the limit from the module's own exported constant
   (if the design exports one) and asserts both sides of it, so a design
   revision that changes limits is detected by the test suite rather than
   silently passing.

## S5 — Side-effect-free failure

**Requirement basis:** decision §6 items 3, 4 ("Reject visibly and without
persistence side effects"; "no HDF5 attribute write before successful
validation and JSON encoding"); §4 debt D1 conditions (no caller-input
mutation).

1. **Failed decode leaves input untouched.** Decode receives text (immutable)
   — assert no module-level or codec-internal cache/state was mutated by the
   failure: a subsequent valid decode in the same process behaves identically
   to one in a fresh process (no failure contamination).
2. **Failed encode leaves caller input untouched.** The caller's envelope
   dict is deep-copied at the test boundary and asserted byte/deep-equal
   after a failed encode; encode must never mutate the caller's declarations
   (D1 condition). This includes failure paths that walk nested client
   payloads.
3. **Errors raised, not logged-and-continued.** Each S2 case asserts the
   exception propagates to the caller. Where the codec internally logs for
   diagnostics, that is additional to — never instead of — the raise; tests
   capture logs during one representative case and assert the raise still
   occurred.
4. **No write-before-validate.** At Phase 2 scope there is no persistence
   call in the codec itself; the constraint is tested by contract: the codec
   API has no parameter that accepts a backend/file handle, asserted by
   signature inspection (guards against scope creep coupling codec to
   persistence). The Phase 3 plan must then assert no HDF5 write occurs on
   any rejected envelope.
5. **No global state leakage between tests.** The suite neither sets nor
   depends on module-global defaults; each test constructs its own codec
   inputs. Scheduler/backend/VISA resources are irrelevant here (codec is
   pure) and the suite must not open any.

## S6 — Legacy preservation

**Requirement basis:** decision §6 items 7, 8; Phase 2 plan row ("Existing
stored v1/v2 cases remain readable as specified"); M03-AC05.

1. **Existing thermal manifests readable unchanged.** The existing suites
   `test_thermal_manifest.py`, `test_thermal_example.py`,
   `test_thermal_execution.py`, `test_thermal_adapters.py`,
   `test_thermal_cli_warnings.py` and `test_thermal_metadata_contracts.py`
   keep passing **unmodified** against the Phase 2 code; this plan asserts
   running them as-is, not updated expectations.
2. **Stored v1/v2 synthetic files remain readable.** Synthetic legacy files
   produced by the current example (coordinated → manifest v1, direct →
   manifest v2) before any Phase 2 change are decoded after the change with
   the legacy readers and asserted content-equal (manifest fields, versions
   1/2, outcome stages, group identity). Fixture generation is done by the
   suite itself running the current example against a temp dir, so no binary
   artifact is committed.
3. **Import paths unchanged.** All existing public import paths used by the
   suites in S6.1 (including `softlab.thermal`-adjacent example modules and
   `softlab.shui` exports) resolve identically; a probe imports the legacy
   manifest machinery by its current names and asserts presence.
4. **Full suite green.** `python -m unittest discover -s tests -p 'test_*.py'`
   passes with the Phase 2 test suite added — the 405-test baseline count
   grows only by Phase 2 additions; zero pre-existing tests are removed,
   skipped, or weakened. (Baseline count recorded in
   characterization-results.md: 405.)
5. **Legacy absence semantics unchanged.** Files with no additive key behave
   exactly as before (legacy absence); Phase 2 adds no code that changes the
   legacy read path's behavior on files lacking the new record.
6. **Additive key not coupled to legacy hydration.** Phase 2 codec tests do
   not depend on thermal's hydration seam retaining custom keys (finding F-B);
   that integration constraint belongs to the Phase 3 plan.

## S7 — Environment and hygiene

**Requirement basis:** characterization-test-plan.md C6; the executed CI
corrections in characterization-results.md ("Corrections — CI (Python 3.9)
failure closure"); AGENTS.md verification conventions.

1. **Synthetic data only.** All fixtures are programmatically constructed;
   no real experiment data, no VISA/hardware, no notebooks are executed.
2. **Temp dirs only.** Any file I/O (e.g. cross-checking encoded text on
   disk is not required by the codec, but if a test does so) uses
   `tempfile.TemporaryDirectory`; no writes outside the temp root; nothing
   generated is committed.
3. **Version-portable assertions (Phase 1 lesson).** This suite **must pass
   identically on Python 3.9 and 3.13**; it is checked on both before
   handoff:
   - **No `autospec=True` combined with `wraps=`.** Phase 1 found that on
     3.9 an autospec'd mock does not delegate to `wraps` (CPython gh-75988,
     fixed in 3.11.9+/3.12/3.13, never backported), producing false
     passes on 3.13 and false failures on 3.9. Any call-instrumentation in
     this suite uses plain `wraps` patching without autospec, or a hand-held
     recorder double.
   - **Conditional warning/version expectations.** Warning and deprecation
     expectations (if any) are gated on the Python/dependency version that
     actually emits them, as Phase 1 did for the `sqlite3.version`
     deprecation (3.13-only). No assertion hardcodes a version string as a
     universal fact.
   - **No hardcoded environment facts.** No test asserts specific installed
     versions of numpy/pandas/h5py or platform facts; version observations
     are recorded in the results file, never asserted as pass conditions.
   - **`Decimal`/float formatting and dict-order assumptions:** assertions
     compare parsed JSON values, not formatted text, except in S1.3 where
     determinism is the property under test.
4. **Warnings discipline.** The suite runs warning-clean on both supported
   Pythons; any warning observed is recorded verbatim in the results file
   and none is suppressed or filtered inside test code.
5. **Resource hygiene.** The codec suite opens no schedulers, backends,
   database connections or VISA resources; no global default instances are
   touched, so no cross-test state leakage is possible by construction.
6. **Verification commands** (recorded per run with actual results):
   `python -m unittest discover -s tests -p 'test_*.py'`,
   `python -m compileall -q softlab`, and the import smoke check
   `python -c "import softlab; print(softlab.__version__)"` — each run on
   every Python version targeted, from the repository root with the project
   venv.
7. **3.9 check before handoff.** Because local development is on 3.13 while
   CI gates on 3.9 and 3.13, the tester must reproduce the 3.9 run locally
   (scratch 3.9 env, per the Phase 1 correction record) or obtain the CI
   result for the candidate commit and record it; a 3.13-only pass is not
   evidence of portability.

## S8 — Results recording

- Executable tests (after review approval, in a later step) will live in
  `tests/` following the established convention
  (`test_m03_p1_characterization.py`): synthetic fixtures, assertion-based,
  one defect per negative test.
- Execution results will be recorded in
  **`log/release_12/test/schema-test-results.md`** with one clearly headed
  section per plan area (S1–S7 above), each ending with a per-area
  **verdict**: `PASS` (all planned assertions executed and passing),
  `FAIL` (with bug report reference in
  [bugs.md](bugs.md) reported to sw-tom), or `BLOCKED` (with reproduction
  evidence and the scoped decision input it feeds). A blocked area is never
  reported as a pass.
- Every recorded run includes: exact commands, Python/dependency versions
  actually observed on **each** Python version run (3.9 and 3.13 at minimum),
  all warnings emitted (none waived), the pre/post full-suite test counts
  (baseline 405), and applicability notes for any reused Phase 1 evidence.
- Reused evidence rule: Phase 1 characterization evidence is reused only
  where content is unchanged (e.g. legacy readability facts), stated
  explicitly per item; anything affected by Phase 2 code is re-executed.
- Traceability: the results file maps each verdict back to the mandating
  decision §6 item / PRD criterion (M03-AC02) / plan matrix item 2, so the
  acceptance review can audit coverage without re-reading test code.

## Gate linkage

Per-area verdicts in `schema-test-results.md` are the test-side evidence for
**M03-AC02** and for plan evidence-matrix item 2. Phase 2 **does not** claim
any M03-AC03/AC04 evidence (persistence and inspection integration are
Phase 3) and does not alter the Phase 1 decision. If the reviewed Phase 2
detailed design cannot freeze the placeholders above (namespace, versions,
required lists, limits, error types), this plan is updated by direct diff
check and re-reviewed; the executable tests are not written against
unreviewed literals.
