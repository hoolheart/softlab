# Release 12 (M03 Phase 2) — Schema/Codec Architecture Review

## Review Information

- **Reviewer**: sw-jerry (Software Architect)
- **Date**: 2026-11-01 Asia/Shanghai
- **Branch**: `codex/m03-p2-schema`
- **Reviewed design**: `log/release_12/design/p2-schema-design.md` at commit
  `5b16836`
- **Binding inputs**:
  - `log/release_12/design/p1-boundary-decision.md`, especially §1 and §6;
  - `log/release_12/test/schema-test-plan.md`, approved through the developer
    re-review recorded in `p2-test-plan-developer-review.md`;
  - `log/release_12/prd.md`, Phase 2 and M03-AC02;
  - `plan/milestone-03.md`, Phase 2 and evidence-matrix item 2.
- **Source checked**: `softlab/shui/__init__.py`, the `softlab/shui/` and
  `softlab/shui/data/` package boundaries, `DataRecord.prepare_columns`,
  `DataGroup` metadata behavior, `CountResult`, the thermal manifest/model/
  acquisition code, and `examples/signal_monitor.py`.
- **Method**: static architecture, contract, source-fidelity, and Python
  3.9/3.13 portability review. No implementation or design files were changed,
  and no runtime tests were executed for this review.

## Verdict

**CHANGES REQUESTED.**

The pure codec is placed in the correct architectural layer and the design
preserves the central additive-record boundary. However, the design omits a
construction seam explicitly required by the joint decision, broadens two
client-owned contracts beyond their authorized semantics, and does not yet
provide a portable, internally consistent failure/limit contract. The approved
test plan also contains acceptance probes that cannot be satisfied by the
frozen public schema. Implementation should not begin until the required fixes
below are incorporated and the affected design/test-plan diffs are re-reviewed.

**Findings: 8 total — High: 3, Medium: 3, Low: 2.**

## Findings

### 1. High — The mandated Phase 2 bounded group-construction seam is deferred, not designed

**Locations**: design §4 (especially lines 488–494 and 542–548), §9 lines
940–946; boundary decision §6 item 1 and §4 D1.

The boundary decision requires Phase 2 to implement "the justified bounded
group-construction seam." D1 further requires Phase 2 construction to create an
empty group and add only string-keyed encoded text through `set_meta`, avoiding
`DataGroup(meta=...)`. The design labels its three dictionary builders as the
"Phase 2 portion" of that surface, but none constructs a group. It then
explicitly defers creating the group and calling `set_meta` to Phase 3.
`build_envelope()` is an envelope-construction seam, not the required bounded
group-construction seam.

This is not resolved by the approved test plan's silence: the plan does not
reassign or remove the final joint decision's requirement. The current design
therefore does not cover the entire binding Phase 2 surface.

**Required fix**: either:

1. design and test the narrow **in-memory** group-construction seam in Phase
   2—empty `DataGroup`, only successfully validated/encoded text attached with
   `set_meta(METADATA_KEY, text)`, no constructor-metadata path, no
   `set_meta(key, None)`, and no backend/file I/O—while leaving HDF5 save,
   copy/preflight/hydration, and inspection in Phase 3; or
2. obtain an explicit revision of the joint boundary decision that moves this
   seam to Phase 3, then reconcile the PRD/plan/test-plan references through
   their applicable review gates.

Do not merely rename `build_envelope()` as the group-construction seam.

### 2. High — Arbitrary-precision integer handling is not portable and escapes the frozen error taxonomy

**Locations**: design §2.2 finite-real rule and encode/decode algorithms,
§2.3, §3, §7.2 line 688, §8.4; thermal `_manifest_real` at
`examples/thermal_experiment.py:365-375`.

The design says exact integers have arbitrary precision and are bounded only by
encoded size, while finite reals are checked with
`math.isfinite(float(v))`. Three failure paths follow:

1. Converting a sufficiently large Python `int` to `float` raises
   `OverflowError`; the design does not map it to `RecordValidationError`.
   The legacy `_manifest_real` technique being reused explicitly catches this
   case, but the new design omits that part of the technique.
2. CPython's integer/string digit guard differs between the supported
   interpreters. Python 3.9 has no `sys.int_max_str_digits` guard, whereas
   Python 3.13 does. A sufficiently long integer token can therefore make
   `json.loads` raise a plain `ValueError` on 3.13 while parsing on 3.9.
   Encoding a correspondingly large integer can likewise raise during
   `json.dumps` before the design's post-serialization size check.
3. The decode algorithm wraps only `json.JSONDecodeError`; the encode algorithm
   has no conversion/serialization error mapping. These paths violate the
   claim that every non-`TypeError` failure is a `RunRecordError` and
   `ValueError`, and make accepted values interpreter-dependent.

The 64 KiB check does not solve this: it runs after `dumps` on encode, and a
sub-64 KiB integer token can already exceed the 3.13 conversion guard.

**Required fix**: freeze a conservative, interpreter-independent integer
magnitude or decimal-digit policy and enforce it **before** float conversion or
JSON serialization/parsing conversion. Map finite-real overflow to
`RecordValidationError`, and specify deterministic decode handling (for
example, a bounded `parse_int` contract or equivalent wrapping) that raises
`RecordDecodeError` without swallowing the duplicate-key/nonfinite hook errors.
Remove or qualify the arbitrary-precision claim. Add identical 3.9/3.13 tests
for huge integers in exact-integer and finite-real fields and for encode/decode
behavior immediately around the chosen numeric bound.

### 3. High — The profile validators accept semantics beyond the two authorized clients

**Locations**: design §1.6 `model`, §1.7 `read_policy.offsets`; boundary
decision §1.2 and §6 item 9; thermal `_check_manifest` model check at
`examples/thermal_experiment.py:413-421`; signal reads at
`examples/signal_monitor.py:29-33`.

Two validations are weaker than the boundary they implement:

- The decision fixes the signal read-policy offsets to **5 and 2**, and the
  independent oracle is based on those exact values. The design accepts any
  exact integers, including negative or unrelated offsets, thereby supporting
  an unreviewed family of signal acquisitions.
- The legacy thermal validator requires `model == _MODEL`, including its exact
  units. The new design checks only nonempty text and closed keys. It would
  accept a different equation, method, validity claim, or unit declaration
  while still freezing Euler procedure/timing semantics and claiming to carry
  inputs for independent Euler comparison. That permits internally
  contradictory records and broadens the authorized thermal client into an
  arbitrary model-description surface.

Closed keys alone do not make these client semantics closed.

**Required fix**: for the currently authorized clients, require signal offsets
exactly `{'signal': 5, 'reference': 2}` and require the thermal model and its
units to equal the legacy `_MODEL` declaration. The builders may still accept
client-supplied declarations as evidence, but validation must reject drift.
Add negative tests for altered offsets, equation/method/validity, and units.
Supporting variation instead requires a revised joint decision and a revised
oracle/scope, not a validator relaxation.

### 4. Medium — Depth enforcement is inconsistent with its own definition and does not contain parser recursion failures

**Locations**: design §2.2 decode steps 3–4, §2.3 depth definition, §3 error
taxonomy, §7.2 sweep pseudocode.

There are two independent depth defects:

1. The contract says container nesting increases depth and "scalars add none."
   The pseudocode nevertheless calls every child with `depth + 1` and checks
   `depth > MAX_DEPTH` before determining whether the child is a container.
   Consequently, a scalar child of a container at exactly depth 8 is visited at
   depth 9 and rejected, contradicting both the definition and the
   exactly-at-limit acceptance rule.
2. Decode calls `json.loads` before the depth sweep. Sufficiently nested JSON
   can raise `RecursionError` in the parser before the sweep executes. That
   exception is neither mapped to `RecordDecodeError` nor covered by the frozen
   taxonomy. A 64 KiB input is still ample to exceed the interpreter recursion
   limit, and parser recursion behavior must not become an accidental
   Python-version-dependent depth policy.

**Required fix**: define depth increments/checks only for containers, with a
scalar-at-maximum-container-depth boundary test. Also specify how excessive
parser nesting is rejected as `RecordDecodeError`—by pre-parse containment or
by carefully mapped parser `RecursionError`—while preserving hook exceptions.
Test both `MAX_DEPTH + 1` and nesting beyond the interpreter parser recursion
threshold on Python 3.9 and 3.13.

### 5. Medium — The exactly-at-limit acceptance requirements are unreachable through the closed public schema

**Locations**: design §2.3 boundary rule and §8.3; approved test plan S4.1–S4.4;
closed field tables in design §1.

The design and plan require exactly-at-limit valid inputs to be accepted, but
the frozen closed schema provides no valid values at most of those limits:

- deepest valid container nesting is stated as 4, not `MAX_DEPTH == 8`;
- the only arrays are the 6-column thermal and 3-column signal declarations,
  not arrays of length 64;
- the largest closed object has 10 entries, not 64;
- both permitted reason strings are shorter than 64, and a 64-character reason
  is necessarily outside the vocabulary;
- the closed fields and 1024-character per-string cap do not provide enough
  variable content for a valid envelope to reach the stated 65536-byte maximum.

Thus an input that isolates a generic sweep boundary is subsequently rejected
by closed-contract validation; it cannot satisfy the plan's public-codec
"accepted at the limit" assertion. S4.2's largest-accepted-encoded-input probe
is particularly undefined under this schema.

**Required fix**: revise the design and approved test plan together so each
boundary assertion is reachable and names the layer at which it applies.
Either choose contract-reachable limits or explicitly separate white-box
sweep-limit tests from end-to-end valid-envelope tests and remove acceptance
claims that cannot hold at the public codec boundary. If an exact largest
valid encoded size is required, derive/freeze it from the complete closed field
and string constraints rather than asserting that it equals the defensive
upper bound. Re-review the resulting direct plan diff.

### 6. Medium — F15 demands provenance detection that a pure structural codec cannot perform

**Locations**: design §1.4 lines 190–196, §2.4 lines 443–444, §8.4 F15;
approved test plan F15.

The design correctly states in §1.4 that an invented or copied but well-formed
value is a **client truthfulness violation**, and that the validator can enforce
only structure. Section 2.4 then claims a "success-like stand-in for an
uncaptured fact" is rejected, and the approved F15 requires rejection of
invented version strings or copied observations.

A pure codec cannot distinguish an observed valid SHA-256 digest from an
invented valid digest, an observed nonempty version string from a fabricated
one, or a measured `git_dirty == false` from an assumed false. Accepting the
caller-supplied fact object provides no evidence about how it was captured.
The current design therefore both disclaims and promises the same check.

**Required fix**: state the responsibility split precisely and reconcile F15
by direct reviewed diff. The codec should reject structurally invalid
availability representations (malformed digest/revision, invalid state/value/
reason bindings, empty values where nonempty text is required). Actual
capture-vs-copy/invention assertions belong at the client capture/construction
integration seam where capture evidence exists, not at the pure JSON validator.
Do not claim rejection of arbitrary well-formed false observations unless a
separately designed capture-evidence API makes that possible.

### 7. Low — Public container-type signatures promise inputs that the exact-type sweep rejects

**Locations**: design §2.1 and §4 signatures versus §7.2 exact `dict`/`list`
sweep.

`encode()` and the builders accept `Mapping`, and `build_envelope()` declares
`columns: Sequence[Mapping]`. The encode pre-check raises `TypeError` only for a
non-`Mapping`, but the recursive sweep accepts only `type(value) is dict` and
`type(value) is list`. A custom `Mapping`, a dict subclass, or tuple-based
`columns` therefore satisfies the public annotation/pre-check but is rejected
as an unsupported value. The design does not say whether builders normalize
these abstract container types before validation.

**Required fix**: either narrow the signatures and prose to the exact accepted
built-in containers, or specify side-effect-free normalization of the declared
`Mapping`/`Sequence` inputs to fresh dict/list structures before the shared
validation path. Freeze the resulting exception behavior and add one
representative test for each advertised abstract input category.

### 8. Low — Re-exporting every constant from `softlab.shui` is broader than necessary

**Locations**: design §8 lines 725–727; current `softlab/shui/__init__.py`.

The test plan requires numeric limits to be readable from the module's exported
constants; it does not require every schema literal and limit to become a
top-level `softlab.shui` public name. Re-exporting all constants plus the codec,
builders, convenience functions, and exceptions unnecessarily enlarges the
package's compatibility surface for a deliberately narrow additive feature.

**Required fix**: enumerate the exact intended public exports. Keep detailed
schema constants on `softlab.shui.run_record` unless a concrete client need
justifies package-level re-export, and limit `softlab.shui` re-exports to the
reviewed client-facing seam. Avoid wildcard exports and retain direct-module
and package-import smoke checks.

## Confirmed Sound Aspects

Subject to the required fixes above, the following architectural choices are
sound and should be retained:

1. **Codec home and decoupling**: `softlab/shui/run_record.py` is the correct
   五行 home for the persistence-format codec. A standard-library-only codec
   directly under `shui/`, rather than under `shui/data/`, avoids importing the
   defective `DataGroup(meta=...)` path, backends, HDF5, or generic `io.py` and
   introduces no circular-import dependency of its own. Adding the required
   group-construction seam must not silently destroy this codec decoupling.
2. **DIP seam**: `RunRecordCodec` gives the future persistence consumer an
   abstraction while `JsonRunRecordCodec` owns JSON validation/serialization.
   Clients supply captured facts and the Phase 2 API exposes no backend or file
   handle. One concrete implementation is sufficient; no framework or new
   dependency is needed.
3. **Core boundary fidelity**: the namespace/version independence, closed
   field lists, exact boolean/integer distinction, rejection of implicit
   `str`/`repr` fallback, separate canonical execution/group UUIDs with hard
   distinctness rejection, ordered column declarations and truthful unitless
   `'1'`, source-versus-derived signal columns, and acquisition-only outcome
   literals all follow the decision.
4. **Legacy compatibility and phase isolation**: the design leaves thermal
   manifest v1/v2 code, import paths, APIs, files, and timing interpretations
   unchanged; performs no migration; and keeps HDF5 writes, backend ownership,
   copy/preflight/hydration, stored-artifact cross-checks, and inspection out of
   the pure codec. No Phase 3 persistence/hydration implementation is smuggled
   into the codec. Finding 1 concerns the opposite problem—a required Phase 2
   construction seam being deferred.
5. **Literal freeze completeness**: the approved plan's namespace, key,
   versions, profiles, availability literals/vocabulary, limits, and exception
   placeholders all have concrete values. The remaining problems are the
   semantic and reachability contradictions identified above, not unfrozen
   placeholders.

## Approval Condition

Re-review is required after findings 1–8 are resolved. Any resulting change to
the already approved test plan must be an explicit, traceable diff with the
applicable tester/developer review; it must not silently weaken the joint
boundary. This review is not implementation authorization, test evidence, or
M03 acceptance.

## Re-review — revised design at `c15f9e8`

- **Reviewer**: sw-jerry (Software Architect)
- **Date**: 2026-11-01 Asia/Shanghai
- **Reviewed design**: `log/release_12/design/p2-schema-design.md` at
  `c15f9e8`
- **Associated test-plan diff reviewed**: `schema-test-plan.md` at
  `fc42bf9`; its own header still awaits developer re-review, which remains
  a separate required gate.
- **Method**: static contract, boundary, source-fidelity, arithmetic, and
  Python 3.9/3.13 portability review. No implementation files were changed
  and no runtime tests were executed.

### Re-review verdict

**CHANGES REQUESTED.** Original findings 1, 3, 4, 6, and 7 are closed in
substance. Findings 2, 5, and 8 are not correctly closed, and two new
contract defects are recorded below. The group seam remains within the
joint decision, the pinned client declarations match their legacy sources,
and the F15 responsibility split is now correct. Approval is nevertheless
blocked by the inconsistent integer boundary, an incomplete size
derivation/probe contract, and the still-broad package exports. This
re-review does not waive the test plan's pending developer re-review and is
not implementation authorization.

### Per-finding closure

#### 1. High — CLOSED: bounded in-memory group construction is now designed

Design §4 now provides `build_run_group` in the separate
`softlab/shui/run_group.py` module. It encodes and validates before
construction, creates `DataGroup(name=name, id=...)` without `meta=`, and
calls only `set_meta(METADATA_KEY, text)` with successfully encoded text.
It has no backend/file handle or save/connect/disconnect operation and
leaves persistence, hydration, and inspection in Phase 3. This satisfies
boundary decision §6 item 1 and the D1 construction conditions without
coupling the pure codec to `DataGroup`. The revised plan's S1.8 covers the
same order and boundary. The unrelated name-contract inaccuracy is new
finding 10, not a reopening of this core seam finding.

#### 2. High — OPEN: the 1024-digit contract and the bit-length guard disagree

The finite-real overflow guard and pre-conversion decode token check are
correct additions, and bounding tokens below 4300 digits avoids the
Python-3.13 interpreter guard. However, `_MAX_INTEGER_BITS = 3398` does not
implement the stated 1024-decimal-digit encode contract:

- every accepted value has `bit_length() <= 3398`, hence is `< 2**3398`
  and has at most 1023 decimal digits;
- `10**1023`, the smallest 1024-digit positive integer, has bit length
  3399 and is therefore rejected by the encode sweep;
- decode accepts a positive 1024-digit token through `_bounded_int`, so a
  signal envelope with matching 1024-digit counts can decode successfully
  and then fail when the normalized envelope is encoded again.

Thus design §2.3/§7.2 and revised plan S3.6/S4.3 cannot satisfy their claim
that a 1024-digit `requested_rows` encodes and round-trips. The asymmetry
would be reproducible on both interpreters; guard safety alone does not
make the public encode/decode contracts identical.

**Required correction**: freeze one magnitude policy and apply it
consistently on encode and decode. For a true decimal-digit contract, an
integer comparison against a precomputed power-of-ten bound avoids string
conversion; alternatively, explicitly freeze a bit-length contract and use
it after the safe token conversion on decode as well. Correct the constant
names/messages, reachability table, and plan probes to the actual boundary,
including positive and negative values and decode→encode round-trips on
Python 3.9 and 3.13.

#### 3. High — CLOSED: thermal model and signal offsets are pinned to legacy semantics

Design §1.6's `THERMAL_MODEL` is value-for-value identical to legacy
`_MODEL`, including equation, method, validity, and all seven units. Design
§1.7 freezes exactly `{'signal': 5, 'reference': 2}`, matching
`examples/signal_monitor.py`. Both builders treat caller declarations as
evidence and reject drift. Revised plan F13 now includes the required
model/unit and offset/key negative probes. This stays within decision §1.2
and §6 item 9.

#### 4. Medium — CLOSED: depth accounting and parser recursion failure are contained

Design §7.2 now checks depth only on container entry; scalar children at a
container depth of 8 are not rejected as depth 9. The sweep stops at depth
9, and `json.loads` parser `RecursionError` is mapped to
`RecordDecodeError` while hook exceptions and `JSONDecodeError` retain
their own channels. Revised plan S4.1 includes the scalar-at-limit,
depth-9, and parser-recursion probes on both supported interpreters.

#### 5. Medium — OPEN: the layered approach is sound, but the frozen limits/probes are not yet valid

The per-limit reachability table and the direct plan revision correctly
replace blanket valid-envelope acceptance at unreachable generic limits.
The following defects prevent closure:

1. The integer row is unreachable as written for the reason in finding 2.
2. The size derivation omits serialized `payload.timing.dt`, a finite real
   equal to `configuration.dt`. The table accounts for only seven reals;
   the thermal envelope contains eight. Using the document's own
   conservative 1024-byte-per-real accounting, including the omitted field
   changes the subtotal from 65,448 to 66,472 bytes, so the asserted
   65,448-byte bound and at-least-2× headroom under 131,072 are not
   established by this derivation.
3. The proposed "all-variable-fields-at-cap valid envelope" is not a valid
   fixture: 1024-digit integers in finite-real fields overflow the required
   float normalization, the encode bit guard rejects 1024-digit integers,
   and thermal's huge-count/final-simulation-time cross-relations cannot be
   satisfied by that construction. A conservative bound is acceptable, but
   the plan must use an actually constructible valid envelope rather than
   calling this impossible fixture valid.
4. Revised plan S4.2 says `'{}'` plus whitespace at exactly 131,072 bytes
   fails with `RecordDecodeError`/`root must be an object`. `{}` is an
   object and passes the root check; it must fail contract validation with
   `RecordValidationError` for missing required fields, as the design's
   decode stages require.
5. The design and plan say a 65-character unavailable reason fails in the
   generic sweep, but §7.2 implements only the generic 1024-code-point
   string check. Either specify the path-specific 64-character check in
   that sweep or assign the 64/65 pair to the contract-validation layer in
   the required order.

**Required correction**: derive the defensive byte bound from every
serialized variable field, recompute or rejustify `MAX_ENCODED_BYTES`, use
reachable valid fixtures, and make the design/plan exception and layer
assertions identical. Then re-review the direct diffs.

#### 6. Medium — CLOSED: F15 now separates structural validity from capture truthfulness

Design §§1.4/2.4/8.4 now limit the codec to malformed values, invalid state
literals, and state/value/reason binding violations. They expressly assign
invented/copied/assumed but well-formed observations to the client capture
integration seam. The direct plan revision at `fc42bf9` makes the same
correction in S1.7 and F15 and removes the impossible codec rejection
claim. The plan's pending developer re-review remains required before
implementation, but the architectural contradiction itself is resolved.

#### 7. Low — CLOSED: public signatures match exact built-in container behavior

The codec and builders now use `Dict`/`List`, root arguments must be exact
`dict`, nested dict/list subclasses and other `Mapping`/`Sequence` inputs
are rejected rather than implicitly normalized, and the exception split is
explicit. Revised plan S3 item 5 covers representative root and nested
cases. The advertised annotations and sweep semantics now agree.

#### 8. Low — OPEN: all numeric limits are still unnecessarily re-exported from `softlab.shui`

The explicit table and prohibition on wildcard imports are improvements,
but the table still re-exports all seven `MAX_*` constants from
`softlab.shui`. Its rationale repeats the misunderstanding corrected by the
original finding: S4.4 requires constants readable from the owning module,
not package-level names. No concrete client need justifies seven new
package compatibility commitments.

**Required correction**: keep the numeric limits on
`softlab.shui.run_record`; limit package-level exports to the reviewed
codec/builders/group seam and exceptions. Reconcile design §8, §8.7 item
11, and revised plan S4.4/S6.3 by removing the dual-package import
assertions while retaining direct-module constant checks and both import
smoke checks.

### New findings

#### 9. Medium — Decode size pre-check can leak `UnicodeEncodeError` outside the frozen taxonomy

**Location**: design §2.2 decode step 2 and §3 "No escaping builtins".

`len(text.encode('utf-8'))` is evaluated outside the parse `try` block. A
Python `str` containing an unpaired surrogate raises `UnicodeEncodeError`
at that pre-check. That exception is a builtin `UnicodeError`/`ValueError`,
not a `RecordDecodeError` or other `RunRecordError`, contrary to the frozen
public failure contract.

**Required fix**: map UTF-8 encoding failure at the decode boundary to
`RecordDecodeError` with the `run_record:` message discipline, without
catching unrelated contract exceptions. Add the same unpaired-surrogate
probe on Python 3.9 and 3.13.

#### 10. Low — `build_run_group` overstates `DataGroup`'s name validation

**Location**: design §4 `build_run_group` docstring;
`softlab/shui/data/base.py:449-454`.

The design says `DataGroup` raises `ValueError` for an empty/**non-text**
name. The actual setter first performs `name = str(name)` and rejects only
an empty converted string; non-text values such as integers are accepted
and converted. The documented exception behavior is therefore not
source-faithful and introduces an unreviewed implicit-coercion path.

**Required fix**: preferably enforce exact nonempty `str` at the narrow
seam before constructing the group and freeze its exception type/message;
otherwise document the inherited coercion explicitly and justify it. Add
the corresponding empty/non-text name probes to the plan.

### Re-review approval condition

Resolve original findings 2, 5, and 8 and new findings 9–10 in the design
and affected test plan, then complete the applicable developer review and
architecture re-review of those direct diffs. Findings 1, 3, 4, 6, and 7
need no further architectural rework unless those corrections change their
contracts. No implementation or test execution is authorized by this
review.
