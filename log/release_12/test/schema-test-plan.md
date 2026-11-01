# Release 12 (M03 Phase 2) — Schema/Codec Test Plan

Owner: Tester (sw-mike) | 2026-11-01 Asia/Shanghai
Status: **DELTA-REVISED per design `4b4b6db` §8.8 — pending developer
re-review**; supersedes the fc42bf9 alignment (§8.7 revisions 1–12
applied — awaiting developer re-review, then architect/design
re-review per the project gate order); no test code and no source changes
before approval**
Branch: `codex/m03-p2-schema`
PRD: [log/release_12/prd.md](../prd.md) (Phase 2 row, **M03-AC02**)
Plan: [plan/milestone-03.md](../../../plan/milestone-03.md)
(Phase 2 row; codec/versioning evidence matrix item 2)
Binding input: [p1-boundary-decision.md](../design/p1-boundary-decision.md)
(option (a), section 1.1 exact boundary, **section 6 binding constraints**)
Design basis: [p2-schema-design.md](../design/p2-schema-design.md)
(second revision per architecture re-review, commit `4b4b6db`; all
literals below are frozen there — §1.1, §1.4–§1.7, §2.2–§2.3, §3, §4,
§7.2, §8). Every literal in this plan is quote-checked against that
design; discrepancies are flagged, never guessed. The §8.8 delta table
(6 items) is applied by direct diff in this revision.
Format conventions follow
[characterization-test-plan.md](characterization-test-plan.md) and its executed
results in [characterization-results.md](characterization-results.md).

## Scope and ground rules

This is a **planning document only**. No test code is written and no source
under `softlab/` is modified until this plan passes independent review. Phase 2
test code targets **the Phase 2 schema/codec modules only** —
`softlab/shui/run_record.py` (strict JSON-text encode/decode/validate of the
additive common envelope, the two closed client payload validators, the pure
dictionary builders, and the codec convenience functions) and
`softlab/shui/run_group.py` (the bounded **in-memory group-construction seam**
`build_run_group`, design §4, [R1]) — plus a legacy-preservation regression
layer over the unchanged thermal v1/v2 machinery.

Phase 2 scope **includes** the in-memory group construction: `build_run_group`
attaches successfully encoded text to an **empty** `DataGroup` via
`set_meta(METADATA_KEY, text)` and nothing more (design §4, §9).
Persistence integration — HDF5 save through an owned `HDF5DataBackend`,
backend/file I/O, copy/preflight/hydration inspection, the storage-level
absent-vs-invalid distinction — is **Phase 3 scope** and is deliberately
excluded here except where noted as legacy readability checks (S6).

### Frozen literals (design §8 freeze table)

All placeholders from the previous draft are now frozen; tests assert these
verbatim, reading numeric limits from the module's exported constants where
the design exports them (S4.4):

- `NAMESPACE = 'softlab.run-record'`; `METADATA_KEY = 'run_record'`;
  `FORMAT_VERSION = 1` (exact int); `THERMAL_PAYLOAD_VERSION = 1`;
  `SIGNAL_PAYLOAD_VERSION = 1` (design §1.1, §8.1).
- `PROFILE_THERMAL = 'thermal'`; `PROFILE_SIGNAL = 'signal'` (design §1.1).
- `OUTCOME_SNAPSHOT = 'completed_acquisition_before_persistence'`;
  `ACQUISITION_COMPLETED = 'completed'`;
  `STAGE_NOT_YET_ESTABLISHED = 'not_yet_established'` (design §1.5, §8.1).
- `STATE_AVAILABLE = 'available'`; `STATE_UNAVAILABLE = 'unavailable'`;
  `UNAVAILABLE_REASONS = ('capture_failed', 'source_mismatch')`;
  `MAX_UNAVAILABLE_REASON_LENGTH = 64`; availability-fact objects have
  exactly the keys `{state, value, reason}` with `value` null iff unavailable
  and `reason` null iff available (design §1.4, §8.2).
- Limits (design §2.3, §8.3): `MAX_ENCODED_BYTES = 131072` (128 KiB),
  `MAX_DEPTH = 8` (root object = depth 1, container-only accounting; scalars
  add none), `MAX_ARRAY_LENGTH = 64`, `MAX_OBJECT_ENTRIES = 64`,
  `MAX_STRING_LENGTH = 1024`, `MAX_UNAVAILABLE_REASON_LENGTH = 64`,
  `MAX_INTEGER_DIGITS = 1024`.
- Error taxonomy (design §3, §8.4): `RunRecordError` (base, never raised
  directly), `RecordDecodeError` (text/parse stage, decode only),
  `RecordValidationError` (contract stage, encode and decode); all subclass
  `ValueError`. `TypeError` (builtin) for a non-`str` `decode` argument or
  a non-`dict` `encode` argument (design §2.1, [R7]). Unpaired-surrogate
  input text at the decode size pre-check ⇒ `RecordDecodeError`,
  `run_record: input text is not encodable as UTF-8 (unpaired surrogate)`,
  identical on Python 3.9 and 3.13 (design §2.2 step 2, §8.4; S2 F17).
- Module placement: codec at `softlab/shui/run_record.py` (standard library
  only), group seam at `softlab/shui/run_group.py` (design "Module
  placement"). Export surface per S6.3 (design §8).

The common envelope root is a JSON object with **exactly ten keys**, all
required, no optional fields anywhere in the contract; the only fields with
an assigned explicit-`null` meaning are availability-fact `value` and
`reason` (design §1.2, §2.4, §8.5). The full field table is design §1.2;
column declarations are design §1.3; thermal payload is design §1.6 (with
frozen `THERMAL_MODEL`, including the exact equation string
`'T_next = T + dt * (P - G * (T - T_ambient)) / C'`, and the six ordered
columns); signal payload is design §1.7 (with frozen
`SIGNAL_OFFSETS = {'signal': 5, 'reference': 2}` and the three ordered
columns). Tests build fixtures programmatically from design §8.6's canonical
minimal envelopes.

## S1 — Valid round-trips

**Requirement basis:** decision §1.1 availability row, §6 items 1, 2, 5;
PRD M03-AC02 ("valid supported values round-trip"); plan evidence matrix
item 2; design §1, §2.2, §8.6.

1. **Minimal valid envelope, thermal profile.** Encode the design's canonical
   minimal thermal envelope (§8.6, coordinated variant; the `direct` variant
   differs exactly by `adapter: 'direct'`,
   `measurement_delay_ticks: 0`, `source_tick: 'tick'` — both variants are
   exercised). Decode the result and assert **deep equality** with the
   original value: no field added, dropped, reordered-into-different-semantics,
   or coerced (finite-real fields normalize to `float`, design §2.2). Assert
   the decode of the encoded text is idempotent (second decode of the same
   bytes is equal to the first).
2. **Minimal valid envelope, signal profile.** Same as S1.1 for the design's
   canonical signal envelope (§8.6): truthful unitless declarations use the
   explicit `'1'` unit (thermal convention) or per-profile declared units
   (`timestamp`/`s`, `signal`/`V`, `reference`/`V`); the source columns are
   exactly `timestamp`, `signal`, `reference` with `difference` **absent**
   from stored source columns (decision §1.2, design §1.7).
3. **Deterministic byte-level stability.** For each valid envelope in S1.1 and
   S1.2, encode twice from independently constructed but equal inputs and
   assert **byte-identical** encoded text — the design freezes canonical
   serialization `json.dumps(..., sort_keys=True, allow_nan=False,
   ensure_ascii=True, separators=(',', ':'))` (design §2.2 step 4, §8.6), so
   key ordering and separators are asserted, and no hash-set iteration order,
   timestamp, or UUID dependence exists inside the codec — identity fields
   are caller-supplied data, not codec-generated. Assert encoded text is
   valid strict JSON (`json.loads` round-trips it) as an independent
   cross-check.
4. **Identity fields preserved distinctly.** Envelopes carry (a) the bounded
   execution UUID (`execution_id`) and (b) the persistent group UUID
   (`group_id`) as **separate fields** (design §1.2 identity fields,
   decision §6 item 5). Tests feed two provably different UUIDs and assert
   both survive encode→decode with their roles unchanged, and that the codec
   never populates one from the other. **Hard rejection assertion (frozen,
   design §1.2, §8.4):** a synthetic envelope with `execution_id == group_id`
   is rejected with `RecordValidationError` carrying
   `execution_id: must differ from group_id`. **Canonical-form assertion
   (design §7.3):** non-canonical UUID text (uppercase, missing hyphens,
   brace-wrapped) is rejected with `RecordValidationError`
   (`<path>: expected canonical UUID`).
5. **Sample schema/ordering/units/counts relations.** Valid thermal and
   signal envelopes declare ordered column schemas with a truthful unit for
   every stored column, plus requested/completed row counts. Tests assert:
   envelope column order is preserved exactly through the round-trip; every
   declared column has an explicit unit entry (unitless columns declare the
   literal `'1'`, never an invented or blank success-like unit — empty-string
   `unit` is rejected); declared counts are exact integers and the frozen
   cross-layer relations hold: `outcome.requested_rows ==
   outcome.completed_rows == configuration.points ==
   procedure.requested_points == boundary.final_tick` (thermal, design
   §1.6) and `payload.points == outcome.requested_rows` (signal, design
   §1.7). Samples themselves are **not** in the envelope (decision §6 item
   2); a probe envelope smuggling sample arrays into the metadata blob is
   rejected (S2, F11).
6. **Acquisition-snapshot-only outcome.** Valid envelopes carry
   `snapshot == 'completed_acquisition_before_persistence'` with acquisition
   `'completed'` and `persistence`, `plotting`, `cleanup`, `overall` all
   `'not_yet_established'` (frozen literals, design §1.5). Round-trip
   preserves these verbatim; an envelope claiming any later-stage success,
   or a failed/cancelled/incomplete acquisition, is rejected (S2, §8.4
   profile-literal rows).
7. **Availability facts explicit in structure.** Valid envelopes carry the
   observed software/source facts decided in §1.1. Tests assert the frozen
   semantics verbatim (design §1.4): a captured fact carries
   `state == 'available'`, its observed value valid for its type (64-lowercase-hex
   `digest`; 40- or 64-lowercase-hex `git_revision`; boolean `git_dirty`),
   and `reason: null`; an **uncaptured** fact carries
   `state == 'unavailable'`, `value: null`, and a `reason` from the frozen
   bounded vocabulary `('capture_failed', 'source_mismatch')` — never a
   success-like stand-in value. Both states round-trip verbatim through
   encode→decode. **Responsibility split (frozen, design §1.4 [R6]):** the
   codec asserts only the *structural* side — state/value/reason bindings,
   value formats, vocabulary membership, and the reason length bound. Whether
   a well-formed value was **actually captured for this run** (versus
   invented, copied from another client, or assumed) is **capture-truthfulness**,
   which a pure structural codec cannot verify; those checks are assigned to
   the client capture/construction integration seam (the Phase 3/4 caller of
   `build_run_group`, wired where capture evidence exists) and are **out of
   the codec suite's claim**. The reason string's vocabulary rejection side
   is F16; its count bound is S4.3.
8. **Group-construction seam (new, design §4, §8.7 revision 2).**
   `build_run_group(name, envelope)` (in `softlab/shui/run_group.py`) is
   asserted as follows, using a hand-held recorder double for `DataGroup`
   construction/`set_meta` calls (S7.3 — plain `wraps` patching or a recorder,
   never `autospec=True` combined with `wraps`):
   - **Encoded-then-constructed order:** the seam calls
     `encode_envelope(envelope)` **before** any `DataGroup` is constructed;
     a recorder double asserts no group-construction call precedes the
     encode call, and the attached text equals the encode output exactly.
   - **Empty group with envelope-derived id:** the constructed group is an
     empty `DataGroup` (no records) whose id comes from the validated
     envelope: `str(group.id) == envelope['group_id']` holds by construction
     (design §4 step 2).
   - **Exactly one metadata write:** `set_meta` is called exactly once, with
     `(METADATA_KEY, text)` where `METADATA_KEY == 'run_record'` and `text`
     is the encoded JSON string; the group's meta dict contains only that
     one key. The seam never passes `meta=` to the `DataGroup` constructor
     (tracked defect D1 path) and never calls `set_meta(key, None)`
     (assertable via the recorder double and the resulting meta dict).
   - **No I/O:** no backend, file handle, or save/connect/disconnect call
     occurs anywhere in the seam (recorder double asserts absence; design
     §4 boundary).
   - **Failure atomicity:** an invalid envelope raises
     `TypeError`/`RecordValidationError` from the encode step **before** any
     group is constructed — no partially populated group can escape; the
     recorder double asserts zero group-construction calls on the failure
     path.
   - **Name-contract probes (frozen, design §4 step 0, [R10], §8.8
     delta 6):** the seam enforces an exact nonempty `str` name itself,
     first — it never relies on `DataGroup`'s inherited `str()` coercion
     (`DataGroup.name`'s setter stringifies a non-text name instead of
     rejecting it). A non-`str` name (an integer name is the probe —
     rejected, never stringified) ⇒ builtin `TypeError`,
     `run_group: name must be an exact str`; an empty name ⇒ builtin
     `ValueError`, `run_group: name must be nonempty`. Both are raised
     by the seam's own step-0 pre-check **before** any `DataGroup`
     construction, and the recorder double asserts **zero**
     constructor calls on both failure paths. These argument failures
     are builtin `TypeError`/`ValueError` — matching `DataGroup`'s own
     convention — not `RunRecordError` subclasses (the group name is a
     `DataGroup` construction argument, not part of the record
      contract).

## S2 — Failure matrix

**Requirement basis:** decision §1.1 availability row, §6 items 1, 2, 3, 4,
5, 6; PRD M03-AC02 ("Tests
reject duplicate keys, unsupported values and nonfinite numbers without
implicit stringification"); plan evidence matrix item 2; design §2.2, §3,
§8.4.

Each row is a separate test feeding **one defect** into an otherwise valid
envelope (or into the raw decode input). For every case the test asserts:
(a) an exception is **raised** (not logged-and-continued, not a None/error
result); (b) the exception **type** matches the design's frozen type for
that class (design §8.4); (c) the message **discipline** — the frozen
template `'<path>: <defect description>'` for contract-stage errors and
`'run_record: ...'` for parse-stage errors, naming the offending field/key
and the defect class (design §3); (d) **no persistence side effects**
occurred (S5).

| # | Defect | Expected behavior (frozen per design §8.4) |
| --- | --- | --- |
| F1 | Malformed JSON text at decode | `RecordDecodeError`, `run_record: malformed JSON: <parser detail>`. |
| F2 | Wrong root type (top-level array, string, number, bool, null instead of object) | `RecordDecodeError`, `run_record: root must be an object`. |
| F3 | Duplicate object keys in the raw JSON text | `RecordDecodeError` via the `object_pairs_hook` technique, `run_record: duplicate object key '<key>'`. Nested duplicates (inside client payload) rejected equally. |
| F4 | Unsupported value types: Python object, set, bytes, complex, NumPy scalar/array, callable, arbitrary class instance | Encode rejects **without** `str`/`repr` fallback (decision §6 item 3); `RecordValidationError`, `<path>: unsupported value type '<typename>'`. NumPy values are rejected unless the client constructor normalized them first — test the un-normalized path reaching the codec directly. |
| F5 | Nonfinite numbers: `NaN`, `+Inf`, `-Inf` at any nesting depth, including inside nested arrays/objects inside the client payload | Decode of `NaN`/`Infinity`/`-Infinity` tokens: `RecordDecodeError`, `run_record: nonfinite JSON token '<token>'` (via `parse_constant`). **Overflow literals:** `parse_constant` does not fire on `1e999`-class text (`json.loads('1e999')` yields `inf` through ordinary float parsing), so the recursive post-parse finiteness sweep must reject it: `RecordDecodeError`, `<path>: nonfinite number` — probed at top level and inside a *nested container three levels down* (shallow-only checks are a known gap class). Encode side: `RecordValidationError`, `<path>: nonfinite number` (the sweep's finite check runs before `allow_nan=False` serialization, which is then defense-in-depth). |
| F6 | Missing required common field (each of the ten root keys, one test or parametrized equivalent) | `RecordValidationError`, `<path>: missing required field '<key>'`. There are **no optional fields** (design §1.2), so there is no absence-success side. |
| F7 | Missing required client-payload field (per profile, per the frozen required lists of design §1.6/§1.7) | `RecordValidationError` by the owning profile's validator; the common layer's dispatch is exercised so the error attributes the payload to the profile. |
| F8 | Unsupported common-format version (`2`, `0`, `'1'`, `1.0`, `True`) | `RecordValidationError`, `version: unsupported version (supported: 1)`. |
| F9 | Unsupported client-payload version (per profile) | `RecordValidationError`, `payload.version: unsupported version (supported: 1)`; payload versions are independent of thermal manifest versions 1/2 and the additive format version is never treated as a thermal v3 (decision §6 item 7). |
| F10 | Unsupported/unrecognized client-profile identifier | `RecordValidationError`, `profile: unsupported client profile`; no silent fallback to a default profile. |
| F11 | **Unknown/extra fields — closed policy.** The decision mandates **closed** records (§1.1 namespace row, §6 item 4; design §1.2–§1.7 exact-key objects). Any key not in the frozen allowlist is rejected at the common layer, inside `record` and column declarations, inside `source` and availability-fact objects, and inside each client payload: `RecordValidationError`, `<path>: unknown field '<key>'` — including keys that differ only by case or by a trailing/leading underscore. For traceability, the results file also records the **rejected alternative** (unknown-fields-preserved) and cites the mandating decision text, so a future boundary change is an explicit decision revision, not a silent test relaxation. |
| F12 | Type errors on present fields (string where integer required, `1.0` where exact integer required, object where array required, `0`/`1` where boolean required) | `RecordValidationError`, `<path>: expected <exact integer \| finite real \| boolean \| text \| object \| array>`; `1.0` where an exact integer is required is a type error, not a success. Exact-integer rule: `type(v) is int` only (`True`, `1.0`, `'3'` all rejected); boolean rule: `type(v) is bool` only; text rule: `type(v) is str` only (design §2.2 strict type rules). |
| F13 | Profile-specific semantic violations at the client validator, **including frozen-model/offsets drift probes (design §1.6, §1.7, §8.7 revision 3)**: thermal six-column ordering, clock/delay policy (coordinated ⇒ delay 1 + `'tick-1'`; direct ⇒ delay 0 + `'tick'`), v1/v2 source-tick distinction, configuration physical validity (`heat_capacity`, `conductance`, `dt` > 0 and `0 < dt*conductance/heat_capacity <= 1`), boundary cross-checks (`final_tick == completed_rows`; `\|final_sim_time - points*dt\| <= 1e-10`); signal declared source columns (exact three, no fourth column, no rename; derived `difference` smuggled into stored columns rejected); **drift probes:** any alteration of the `model` object versus the frozen `THERMAL_MODEL` — a different equation, method, or validity string, any altered or renamed unit — is `RecordValidationError`, `payload.model: unsupported model semantics or units`; any alteration of `read_policy.offsets` versus the frozen `SIGNAL_OFFSETS = {'signal': 5, 'reference': 2}` — changed values, zero/negative offsets, extra keys, renamed channels — is `RecordValidationError`, `payload.read_policy.offsets: unsupported read offsets (supported: {'signal': 5, 'reference': 2})`. Also asserted: thermal `record.name == 'thermal_trace'` / signal `record.name == 'signal'` (frozen record names, design §1.6/§1.7). | `RecordValidationError` with the profile-attributed frozen messages of design §8.4 F13 row; the common layer does not reinterpret payload internals. |
| F14 | Present `null` on a field to which the frozen schema assigns **no null meaning** (one test per such field class, parametrized) | `RecordValidationError`, `<path>: explicit null has no assigned meaning`. Only availability-fact `value` (iff unavailable) and `reason` (iff available) carry an assigned null meaning (design §1.4, §2.4, §8.5); those two are **not** fed through this row — their acceptance side is S3.5.2. |
| F15 | **REVISED (design §1.4 [R6], §8.7 revision 4): structurally** invalid availability representation — malformed `digest`/`git_revision` (non-hex, wrong length, uppercase), wrong value type for the fact, empty text where nonempty is required, invalid state literal (not `'available'`/`'unavailable'`), and any state/value/reason binding violation | `RecordValidationError`, `<path>: invalid available value` (design §8.4 F15 row). **Structural validation only:** the codec does **not** — and cannot — reject a well-formed but invented, copied-from-another-client, or assumed observation; a well-formed fabricated digest is indistinguishable from a well-formed observed one. Capture-truthfulness (facts actually captured for this run) is a **client capture/construction integration-seam responsibility** (the Phase 3/4 caller of `build_run_group`, wired where capture evidence exists), explicitly **not** claimed by this codec suite. The previous draft's expectation that the codec rejects "invented/copied/assumed" well-formed values is removed. |
| F16 | Availability reason string outside the frozen bounded vocabulary `('capture_failed', 'source_mismatch')`, or missing where the unavailable state requires one (and vice versa: reason present without the unavailable state); also the git binding (`git_dirty.state != git_revision.state`) | `RecordValidationError` with the frozen templates `<path>: reason outside bounded vocabulary` / `<path>: unavailable state requires null value and bounded reason` / `<path>: available state requires value and null reason` (design §8.4 F16 row); the unavailable state and its reason are bound together per the frozen unavailable-reason rules (decision §6 items 1 and 6). The reason string exceeding the frozen bound `MAX_UNAVAILABLE_REASON_LENGTH = 64` is rejected at the count-boundary side (S4.3, contract layer per §8.8 delta 3). |
| F17 | **NEW (§8.8 delta 5, design §2.2 step 2, §3, §8.4):** unpaired-surrogate decode input — a Python `str` carrying an unpaired surrogate, which the decode size pre-check's UTF-8 length computation cannot encode (the builtin `UnicodeEncodeError` is mapped, never escapes the frozen taxonomy) | `RecordDecodeError`, `run_record: input text is not encodable as UTF-8 (unpaired surrogate)` — asserted **identical on Python 3.9 and 3.13** (both reject unpaired surrogates at UTF-8 encoding). |

## S3 — Boolean-vs-number, type strictness, container types, huge integers

**Requirement basis:** decision §6 item 3 ("no silent coercion of
booleans/numbers"); plan evidence matrix item 2 ("Test booleans versus
numbers"); design §2.1 [R7], §2.2 [R2], §8.7 revisions 6–7.

1. `True` / `1` confusion: an envelope field whose schema type is integer
   fed JSON `true` (and an encode of Python `True` into an integer-typed
   field) is **rejected**, even though Python `True == 1`. Symmetric case for
   `False` / `0` (`RecordValidationError`, F12 templates).
2. Boolean field fed `1`/`0` (number, not boolean token) is rejected; a
   number field fed `true`/`false` is rejected.
3. **No implicit stringification anywhere:** no field accepts an unsupported
   value via `str()`/`repr()` coercion; a probe object whose `__str__` is
   benign still fails encode at F4. Strings that *look* numeric (`"3"`) fed
   to numeric fields are rejected (no implicit `int()`/`float()` parse either
   direction).
4. JSON distinguishes `1` (int) from `1.0` (real) per the design's
   exact-integer rule: where the design allows finite reals, `1.0` is valid
   and normalizes to `float`; where it requires exact integers, `1.0` is
   rejected (F12). Negative zero is valid and round-trips exactly (design
   §7.2 edge cases).
5. **Container-type probes (frozen, [R7], §8.7 revision 6).** The public API
   accepts **only the built-in `dict` and `list` containers**; a dict/list
   subclass or any other `Mapping`/`Sequence` implementation is **not
   normalized** (design §2.1):
   - Root argument: `encode()` fed a `collections.OrderedDict` (a dict
     subclass), a `types.MappingProxyType`, a list, a tuple, or any non-dict
     ⇒ builtin `TypeError`, `run_record: expected dict`.
   - Nested containers: a dict subclass / `MappingProxyType` in place of any
     nested object (e.g. `payload.configuration`), and a tuple in place of
     any array (e.g. `record.columns`) ⇒ `RecordValidationError`,
     `<path>: unsupported value type '<typename>'`; the input is never
     copied-normalized.
   - Signatures annotate `Dict[str, Any]` / `List[...]` (never abstract
     `Mapping`/`Sequence`), asserted by inspection so the annotation never
     promises an input the sweep rejects.
6. **Huge-integer probes (frozen, [R2], §8.7 revision 7; mechanism and
    sign rule corrected per §8.8 delta 1) — identical on Python 3.9 and
    3.13, with no `autospec=True` combined with `wraps` anywhere in
    these tests (S7.3).** The contract freezes
    `MAX_INTEGER_DIGITS = 1024` decimal digits and the derived private
    bound `_MAX_INTEGER_ABS = 10 ** MAX_INTEGER_DIGITS` (a module
    constant computed once at import; pure integer comparison, no string
    or float conversion, so the check itself cannot trip the 3.13
    interpreter guard). One magnitude policy applies identically on
    encode and decode: an exact integer is accepted **iff**
    `abs(v) < 10 ** MAX_INTEGER_DIGITS`, i.e. at most 1024 decimal
    digits; a leading `-` is a sign, never counted as a digit, so
    positive and negative values behave identically (design §2.2,
    §7.2):
   - **Encode, exact-integer field at/over the bound:** pinned to the
     **signal** profile per §8.8 delta 1 — an exact-integer field
     (`outcome.requested_rows`, with matching `completed_rows` and
     `payload.points`, pure integer equality) with **1024 decimal
     digits is accepted** and encodes/decodes; **1025 digits is
     rejected** with `RecordValidationError`,
     `<path>: integer digit count exceeds limit 1024`. (Thermal counts
     at that magnitude are rejected by the `final_sim_time` float
     cross-relation, design §1.6, so the at-limit valid-envelope probe
     cannot use the thermal profile.) Acceptance includes `10**1023`
     (the smallest 1024-digit positive value — which the first
     revision's `_MAX_INTEGER_BITS = 3398` mechanism wrongly rejected
     on encode while decode accepted it; that mechanism is withdrawn).
     Values are constructed programmatically (no committed large
     literal).
   - **Decode, long integer token at/over the bound:** raw text whose
     integer token has 1024 digits round-trips; a 1025-digit token is
     rejected by the `parse_int` bound **before any conversion** with
     `RecordDecodeError`, `run_record: integer digit count exceeds limit
     1024`. The optional leading `-` is **stripped before the digit
     count** (design §2.2 second revision), so a `-` plus 1024 digits
     is **accepted** by the bound — matching encode exactly. JSON
     integer tokens have no leading zeros, so the token digit count
     equals the value's decimal digit count: the decode accepted set is
     precisely the encode accepted set, and a decoded 1024-digit value
     re-encodes without error (the round-trip boundary holds).
   - **Finite-real field fed a huge int:** an `int` whose `float()`
     conversion raises `OverflowError` (beyond float range) fed to a
     finite-real field is mapped to `RecordValidationError`,
     `<path>: expected finite real` — the builtin `OverflowError` never
     escapes (design §2.2 legacy `_manifest_real` guard, [R2]).
   - **Guard-safety proof probe:** after the digit-bound sweep, no
     serialization-time builtin exception can escape; `json.dumps` never
     meets an over-limit integer, so its `int→str` conversion is guard-safe
     on 3.13 (1024 < 4300 default guard) — acceptance/rejection is
     identical on both supported interpreters.

### S3.5 — Null versus absent (frozen policy)

**Requirement basis:** PRD M03-AC02 ("absent versus null"); decision §6
item 6; design §2.4, §8.5. The frozen schema pins exactly which fields, if
any, assign a meaning to explicit `null`; these checks assert the frozen
policy verbatim, one defect per negative test, distinct from F6 (absent),
F12 (invalid present value) and F14 (present null where no null meaning is
assigned).

1. **Rejection side.** **No optional fields exist** anywhere in the contract
   (design §1.2, §8.5); absent ⇒ F6 rejection for every one of the ten root
   keys and every nested required key. Present `null` is rejected
   (`RecordValidationError`, `<path>: explicit null has no assigned
   meaning`, F14) for every field **except** the two fact slots of §3.5.2.
   One probe per field class suffices with parametrization; the field set is
   taken from the frozen schema (design §1.2–§1.7), not assumed.
2. **Acceptance side.** Explicit `null` is **accepted**, exactly per design
   §1.4/§8.5, **only** for availability-fact `value` (iff
   `state == 'unavailable'`) and `reason` (iff `state == 'available'`); both
   round-trip verbatim through encode→decode, preserving their assigned
   semantics. Acceptance **and** rejection sides of both slots are probed
   (value-null with available state rejected; reason-null with unavailable
   state rejected — F16).
3. **Distinctness.** The three states — absent, present-null, present-valid-value —
   are asserted to be three semantically distinct results for the two fact
   slots: absent ⇒ F6 rejection; present-null ⇒ accepted only in the bound
   state; present-value ⇒ normal. Neither absent nor null is ever reported
   as success, zero, empty, or not-applicable (decision §6 item 6 verbatim).
   The previous draft's "optional field with an assigned null meaning" case
   is **vacuous by design** (no optional fields exist) and is recorded as
   such in the results file (design §8.5).
4. **Whole-record edge.** Absence of the whole `run_record` metadata key on
   disk means only "no additive common record" (legacy absence); a present
   non-text, malformed, invalid, or unsupported-version value is an error,
   never legacy absence (decision §6 item 6; design §2.4). The key-level
   distinction is made by the Phase 3 inspection path — the codec only ever
   sees present text (design §9) — so the storage-level side of this edge is
   a Phase 3 assertion; the codec-side contribution is that decode of
   non-`str` raises builtin `TypeError` (`run_record: expected JSON text`).
   The legacy-absence side is covered in S6.5.

## S4 — Size/depth/count limits

**Requirement basis:** decision §6 item 1 ("conservative encoded size/depth
limits ... frozen in the reviewed Phase 2 design"); design §2.3 frozen
limits and the frozen **per-limit reachability table** (§8.7 revisions
8–11). The blanket at-limit acceptance rule of the previous draft is
**replaced**: per design §2.3, limit+1 is always rejected at the public
boundary, and exactly-at-limit acceptance is asserted **at the layer where
it is reachable** — because the closed schema cannot produce a *valid*
envelope at most generic sweep limits, the at-limit side is proven by
layered probes (an at-limit structure passes the sweep and then fails at
the contract layer, proving the sweep accepted it).

1. **Depth (`MAX_DEPTH = 8`, container-only accounting: root object = depth
   1, each nested container +1; scalars add none and are never
   depth-checked — design §2.3, §7.2):**
   - **Layered depth-8 acceptance probe:** an exactly-8-deep container placed
     at an extra/unknown root key passes the sweep and then fails at the
     contract layer with `RecordValidationError` (`<path>: unknown field
     '<key>'`) — proving the sweep accepted depth 8.
   - **Depth-9 rejection probe:** a 9-deep container fails at the sweep with
     the depth error (`run_record: nesting depth exceeds limit 8` decode /
     `RecordValidationError` encode).
   - **Scalar-at-max-container-depth acceptance ([R4] boundary case):** a
     scalar child of a container at exactly depth 8 is accepted by the sweep
     (it then fails at the contract layer for the unknown key — the sweep
     acceptance is what is asserted).
   - **Parser recursion probe ([R4]):** raw text nesting far deeper than the
     interpreter recursion limit (but within `MAX_ENCODED_BYTES`) is rejected
     deterministically on **both** 3.9 and 3.13 with `RecordDecodeError`,
     `run_record: nesting exceeds parser recursion limit` — the builtin
     `RecursionError` raised inside `json.loads` is mapped, never escapes,
     and is interpreter-independent. Sweep recursion safety is a design
     property (depth check fires at latest on entry at depth 9, so sweep
     recursion never exceeds ~10 frames); tests assert the public behavior
     only.
2. **Size (`MAX_ENCODED_BYTES = 131072`; encode: ASCII bytes; decode: UTF-8
    bytes — design §2.3 [R5]; bound, fixture, and probe corrected per
    §8.8 delta 2):** the previous draft's "largest accepted input is
    exactly at the limit" assertion is **unsatisfiable** under the closed
    schema (the derived worst-case legitimate envelope is ≤ 66,472 bytes
    — the derivation now includes the omitted `payload.timing.dt`, the
    eighth finite real, design §2.3) and is replaced by the frozen
    boundary pair:
   - **Decode pre-check boundary pair:** exactly **131,072 bytes** of
     input **passes the size gate** — probe: syntactically valid but
     contract-invalid text (`'{}'` plus whitespace padding) —
     `'{}'` **is** an object, so it passes the root-type check and
     fails later at contract validation with `RecordValidationError`,
     `<path>: missing required field ...`, proving the gate accepted
     the size (the first revision's expectation of `RecordDecodeError`
     / `run_record: root must be an object` is withdrawn); **131,073
     bytes** is rejected by the gate with `RecordDecodeError`,
     `run_record: encoded size exceeds limit 131072 bytes`. Inputs are
     constructed programmatically (no committed large literals).
   - **Maximal-valid-envelope assertion:** the constructible maximal
     valid envelope of design §2.3 (the first revision's
     all-variable-fields-at-cap fixture was impossible — 1,024-digit
     integers in finite-real fields overflow the required float
     normalization, and thermal's huge-count/`final_sim_time`
     cross-relation cannot be satisfied at that magnitude): the four
     free-text fields at the 1,024-code-point cap with worst-case
     non-BMP escaping (`software.softlab_version`,
     `software.python_version`, `source.reference`,
     `source.digest_scope`), signal-profile counts at the 1,024-digit
     cap (`requested_rows`, `completed_rows`, `payload.points`), and
     finite reals at ordinary float values. Such an envelope encodes
     well under the **66,472-byte** ceiling and round-trips — proving
     **≈ 1.97× headroom** (131,072 / 66,472 ≈ 1.97, just under 2×) of
     the limit over the largest legitimate envelope (design §2.3
     derivation). The design's derivation is a deliberately loose
     sweep-level ceiling; the attainable maximum lies far below it.
   - **Encode-side size check:** defense-in-depth, provably unreachable for
     valid envelopes; asserted by the maximal-envelope bound above, not by a
     dedicated oversize-encode probe.
3. **Count/length bounds (`MAX_ARRAY_LENGTH = 64`, `MAX_OBJECT_ENTRIES =
   64`, `MAX_STRING_LENGTH = 1024`, `MAX_UNAVAILABLE_REASON_LENGTH = 64`),
   layered per the reachability table:**
   - **Array (64/65):** a 64-element array at an extra/unknown root key
     passes the sweep then fails at the contract layer (proving sweep
     acceptance); 65 elements fail at the sweep (`<path>: array length
     exceeds limit 64`).
   - **Object entries (64/65):** a 64-entry object at an extra/unknown root
     key passes the sweep then fails at the contract layer; 65 entries fail
     at the sweep (`<path>: object entry count exceeds limit 64`). (The
     largest legitimate object is the ten-key root.)
   - **String length (1024/1025):** at-limit acceptance exists here —
     a **valid** envelope with a 1024-code-point `digest_scope` (or
     `reference`, `software.*`) is **accepted**; 1025 is rejected by the
     sweep (`<path>: string length exceeds limit 1024`).
   - **Unavailable-reason length (64/65), probed at the proper field,
     both checks at the contract layer ([R5], §8.8 delta 3 — the
     path-specific reason-length check is **not** part of the generic
     §7.2 sweep):** a 64-character reason passes the length gate and
     then fails **vocabulary** validation (`RecordValidationError`,
     `<path>: reason outside bounded vocabulary`) — proving the length
     gate accepted 64; 65 characters fail the contract-layer length
     check with `RecordValidationError` (`<path>: reason length exceeds
     limit 64`) on **both** encode and decode. The numeric value is
     read from the module's exported constant (S4.4).
   - **Integer digits (1024/1025):** pinned to the **signal** profile
     per §8.8 delta 1 — probed at the public valid-envelope boundary
     per S3.6 (a 1024-digit `requested_rows` with matching
     `completed_rows` and `payload.points` encodes/decodes; 1025 digits
     rejected — encode `RecordValidationError`, decode
     `RecordDecodeError`), identical on 3.9 and 3.13.
4. **Constant-sourced limits (S4.4, updated per §8.7 revision 11).** Limits
   are asserted to be **the design's frozen values**, not test-local guesses:
   the test reads each limit from the module's own exported constant and
   asserts both sides of it, so a design revision that changes limits is
   detected by the test suite rather than silently passing. The constant set
   is exactly: `MAX_ENCODED_BYTES = 131072`, `MAX_DEPTH = 8`,
   `MAX_ARRAY_LENGTH = 64`, `MAX_OBJECT_ENTRIES = 64`,
    `MAX_STRING_LENGTH = 1024`, `MAX_UNAVAILABLE_REASON_LENGTH = 64`,
    `MAX_INTEGER_DIGITS = 1024`. All seven numeric limits are importable
    from `softlab.shui.run_record` **only** (§8.8 delta 4, [R8]: the
    first revision's dual-package import assertion is withdrawn — S4.4
    requires the limits readable from the owning module's exported
    constants, never package-level names); the module-only import
    assertion is part of S6.3.

## S5 — Side-effect-free failure

**Requirement basis:** decision §6 items 3, 4 ("Reject visibly and without
persistence side effects"; "no HDF5 attribute write before successful
validation and JSON encoding"); §4 debt D1 conditions (no caller-input
mutation); design §3 side-effect-free-failure guarantee.

1. **Failed decode leaves input untouched.** Decode receives text (immutable)
   — assert no module-level or codec-internal cache/state was mutated by the
   failure: a subsequent valid decode in the same process behaves identically
   to one in a fresh process (no failure contamination). The codec is
   stateless (design §2.2), so the module-level convenience instance shares
   no cross-test state (S5.5).
2. **Failed encode leaves caller input untouched.** The caller's envelope
   dict is deep-copied at the test boundary and asserted byte/deep-equal
   after a failed encode; encode must never mutate the caller's declarations
   (D1 condition; validated output is freshly built, design §2.2 step 2).
   This includes failure paths that walk nested client payloads.
3. **Errors raised, not logged-and-continued.** Each S2 case asserts the
   exception propagates to the caller. The design freezes: there is no
   log-and-continue path anywhere in the module — every defect propagates as
   an exception (design §3). Tests capture logs during one representative
   case and assert the raise still occurred.
4. **No write-before-validate.** At Phase 2 scope there is no persistence
   call in the codec itself; the constraint is tested by contract: the codec
   API has no parameter that accepts a backend/file handle, asserted by
   signature inspection (guards against scope creep coupling codec to
   persistence). Additionally, `build_run_group` performs no backend/file
   I/O and no save/connect/disconnect (S1.8 recorder-double assertion,
   design §4). The Phase 3 plan must then assert no HDF5 write occurs on any
   rejected envelope.
5. **No global state leakage between tests.** The suite neither sets nor
   depends on module-global defaults (including **never** calling
   `sys.set_int_max_str_digits` — the integer bound is enforced by the
   module, not by mutating interpreter state, design "Implementation
   Notes"); each test constructs its own codec inputs. Scheduler/backend/
   VISA resources are irrelevant here (codec is pure) and the suite must not
   open any.

## S6 — Legacy preservation and export surface

**Requirement basis:** decision §6 items 7, 8; Phase 2 plan row ("Existing
stored v1/v2 cases remain readable as specified"); M03-AC05; design §8
export table ([R8], §8.7 revision 12).

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
3. **Import paths and export surface unchanged ([R8]).** All existing
   public import paths used by the suites in S6.1 (including
   `softlab.thermal`-adjacent example modules and `softlab.shui` exports)
   resolve identically; a probe imports the legacy manifest machinery by its
   current names and asserts presence. **Export-surface assertion (updated
   per §8.8 delta 4, [R8]):** the package-level re-export list of
   `softlab/shui/__init__.py` for this feature is **exactly** the design §8
   frozen table — no additional names from this feature, no wildcard
   imports: `RunRecordCodec`, `JsonRunRecordCodec`, `encode_envelope`,
   `decode_envelope`, `build_envelope`, `build_thermal_payload`,
   `build_signal_payload`, `build_run_group`, `RunRecordError`,
   `RecordDecodeError`, `RecordValidationError`. **No numeric limit is
   re-exported at package level:** the seven `MAX_*` names
   (`MAX_ENCODED_BYTES`, `MAX_DEPTH`, `MAX_ARRAY_LENGTH`,
   `MAX_OBJECT_ENTRIES`, `MAX_STRING_LENGTH`,
   `MAX_UNAVAILABLE_REASON_LENGTH`, `MAX_INTEGER_DIGITS`) are asserted to
   be **absent** from `softlab.shui` — importing each from the package
   raises `ImportError` — and importable from `softlab.shui.run_record`
   directly (the S4.4 constant source). Everything else —
   `NAMESPACE`, `METADATA_KEY`, `FORMAT_VERSION`, profile identifiers,
   payload versions, outcome/availability literals, `UNAVAILABLE_REASONS`,
   `THERMAL_MODEL`, `SIGNAL_OFFSETS` — stays on `softlab.shui.run_record`
   only and is imported from the module directly by tests that need it.
   Assert both directions: every listed name is importable from
   `softlab.shui`; no feature name outside the reduced §8 table (the
   seven `MAX_*` names included) leaks into `softlab.shui`. The
   pre-existing `softlab.shui` exports
   (`DatabaseBackend`, `catch_error`, `data`, `profile`) are unchanged.
   Package-import and direct-module import smoke checks both apply.
4. **Full suite green.** `python -m unittest discover -s tests -p 'test_*.py'`
   passes with the Phase 2 test suite added — the 405-test baseline count
   grows only by Phase 2 additions; zero pre-existing tests are removed,
   skipped, or weakened. (Baseline count recorded in
   characterization-results.md: 405.)
5. **Legacy absence semantics unchanged.** Files with no additive key behave
   exactly as before (legacy absence); Phase 2 adds no code that changes the
   legacy read path's behavior on files lacking the new record.
6. **Additive key not coupled to legacy hydration.** Phase 2 codec tests do
   not depend on thermal's hydration seam retaining custom keys (finding F-B;
   design §9 — the additive key must be carried explicitly by Phase 3, never
   promised as arbitrary custom-key hydration); that integration constraint
   belongs to the Phase 3 plan.

## S7 — Environment and hygiene

**Requirement basis:** characterization-test-plan.md C6; the executed CI
corrections in characterization-results.md ("Corrections — CI (Python 3.9)
failure closure"); AGENTS.md verification conventions; design §2.2 [R2]/[R4]
(portable integer and recursion behavior).

1. **Synthetic data only.** All fixtures are programmatically constructed
   (including §8.6 canonical envelopes: UUIDs, `softlab_version`,
   `python_version`, and digest values are captured or generated at test
   time, never hardcoded); no real experiment data, no VISA/hardware, no
   notebooks are executed.
2. **Temp dirs only.** Any file I/O (e.g. cross-checking encoded text on
   disk is not required by the codec, but if a test does so) uses
   `tempfile.TemporaryDirectory`; no writes outside the temp root; nothing
   generated is committed.
3. **Version-portable assertions (Phase 1 lesson).** This suite **must pass
   identically on Python 3.9 and 3.13**; it is checked on both before
   handoff:
   - **No `autospec=True` combined with `wraps`.** Phase 1 found that on
     3.9 an autospec'd mock does not delegate to `wraps` (CPython gh-75988,
     fixed in 3.11.9+/3.12/3.13, never backported), producing false
     passes on 3.13 and false failures on 3.9. Any call-instrumentation in
     this suite (including the S1.8 group-seam recorder double) uses plain
     `wraps` patching without autospec, or a hand-held recorder double.
   - **Conditional warning/version expectations.** Warning and deprecation
     expectations (if any) are gated on the Python/dependency version that
     actually emits them, as Phase 1 did for the `sqlite3.version`
     deprecation (3.13-only). No assertion hardcodes a version string as a
     universal fact.
   - **No hardcoded environment facts.** No test asserts specific installed
     versions of numpy/pandas/h5py or platform facts; version observations
     are recorded in the results file, never asserted as pass conditions.
     The integer-bound and parser-recursion probes (S3.6, S4.1) are the
     explicit portability assertions: integer acceptance is identical on
     3.9 and 3.13 because the bound (1024) is far below the 3.13
     interpreter guard (4300) and the checks use no string conversion on
     unbounded values; the `RecursionError` mapping makes deep-nesting
     rejection deterministic on both interpreters (design §2.2 [R2]/[R4]).
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
  **log/release_12/test/schema-test-results.md** with one clearly headed
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
  decision §6 item / PRD criterion (M03-AC02) / plan matrix item 2 **and to
  the revised detailed design section** (§1.x/§2.x/§3/§4/§8.4/§8.6), so the
  acceptance review can audit coverage without re-reading test code.

## Gate linkage

Per-area verdicts in `schema-test-results.md` are the test-side evidence for
**M03-AC02** and for plan evidence-matrix item 2. Phase 2 **does not** claim
any M03-AC03/AC04 evidence (persistence and inspection integration are
Phase 3) and does not alter the Phase 1 decision. This revision applies
the second-round delta (design §8.8, six items, commit `4b4b6db`) on top
of the fc42bf9 alignment (§8.7 revisions 1–12); all literals are frozen
design values, quote-checked against the design text. If the design is
revised again before freeze, this plan is updated by direct diff check
and re-reviewed; the executable tests are not written against unreviewed
literals.
