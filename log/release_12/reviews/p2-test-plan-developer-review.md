# Phase 2 Schema/Codec Test Plan — Developer Review (implementability)

Reviewer: sw-tom (Developer) | 2026-11-01 Asia/Shanghai
Branch: `codex/m03-p2-schema`
Input: [log/release_12/test/schema-test-plan.md](../test/schema-test-plan.md)
against [log/release_12/design/p1-boundary-decision.md](../design/p1-boundary-decision.md)
(option (a), §1.1, **§6 binding constraints**),
[log/release_12/prd.md](../prd.md) (Phase 2 row, **M03-AC02**), and
[plan/milestone-03.md](../../../plan/milestone-03.md) (evidence matrix item 2).
Method: source-level inspection only — no test code written, no source
modified. Codec-relevant source skimmed to confirm placeholder expectations
are implementable without touching legacy behavior:
`examples/thermal_experiment.py` (manifest encode/decode/validate,
lines 341–532), `softlab/shui/data/hdf5.py` (metadata write path),
`softlab/shui/data/base.py` (`set_meta`/`get_meta`). Baseline re-verified on
this branch: `python -m unittest discover -s tests -p 'test_*.py'` →
**405 tests, OK** (Python 3.13, project venv), matching the plan's recorded
baseline in characterization-results.md.

## Verdict

**CHANGES REQUESTED** — the plan's scope discipline, placeholder policy,
failure-matrix structure, side-effect-free layer, legacy-preservation layer
and gate linkage are all sound and implementable against the decided
envelope **without any legacy change** (positive confirmations below).
However, three coverage gaps must be closed in plan text before the plan is
executable: explicit **null-vs-absent** checks (required verbatim by
M03-AC02 and decision §6 item 6), **availability/unavailable-reason**
semantics (required by decision §1.1 and §6 item 1), and a decode-side
finite-sweep requirement so **overflow literals** cannot smuggle nonfinite
values past token-level rejection (F5). All three fixes are additive
plan-text clarifications; no re-planning and no scope change is needed.

## Findings

### Required fixes

1. **Missing explicit null-vs-absent coverage (PRD M03-AC02 "absent versus
   null"; decision §6 item 6).** M03-AC02 lists "absent versus null" as a
   stated property of the accepted format, and §6 item 6 mandates that
   required/optional/absent/explicit-null/unavailable/invalid-present
   behavior be defined explicitly, with "optional absence never implies
   success, zero, empty, or not-applicable". The plan tests absence well
   (S1 minimal envelopes with optionals absent; F6 missing required fields)
   and wrong types (F12), but present-null is covered only incidentally —
   F12 rejects null only if the frozen schema classifies it as a type error,
   which is exactly the semantic the decision says must be pinned. No
   planned check asserts the three distinct facts the decision demands:
   (a) present-null on a field with **no assigned null meaning** is
   rejected, named per field; (b) present-null on a field where the design
   **assigns** a meaning (e.g. availability facts mirroring the legacy
   manifest's `digest: None` + bounded reason pattern) is accepted and is
   semantically distinct from absent; (c) absent ≠ null ≠ success/zero/empty
   for optional fields. Fix: add an explicit null-vs-absent section (or
   extend S2/S3) pinning both rejection and acceptance sides per the frozen
   schema, one defect per negative test, with traceability to §6 item 6.

2. **Availability / unavailable-reason semantics unpinned (decision §1.1
   availability row; §6 item 1).** The decision requires the Phase 2 design
   to freeze "unavailable-reason rules" and states: uncaptured facts are
   explicitly unavailable with a bounded reason, never copied from another
   client, never inferred as success; unitless channels declared truthfully.
   The plan pins truthful unitless declarations and outcome stages (S1.2,
   S1.6) but contains **no planned check** for the envelope's
   software/source/environment availability facts: no check that an
   unavailable fact carries explicit unavailable state plus a bounded reason
   string, and no probe asserting an unavailable fact presented as a
   success-like value is rejected. This is also where S4.3's "reason
   strings" count bound currently dangles — no S-area states what the reason
   strings bound applies to. Fix: add valid-envelope checks (unavailable
   fact = explicit state + bounded reason round-trips verbatim) and negative
   probes (success-like value for an uncaptured fact rejected; reason string
   exceeding the frozen bound rejected), traced to §1.1 and §6 items 1 and 5.

3. **F5 decode side needs a recursive finite sweep; token rejection alone
   leaks overflow literals.** The plan's decode requirement ("rejected on
   decode of text containing `NaN`/`Infinity` tokens") is satisfied by the
   `parse_constant` technique the legacy example demonstrates
   (`_decode_manifest`, thermal_experiment.py:528–532) — but that technique
   only fires on the literal tokens `NaN`, `Infinity`, `-Infinity`.
   `json.loads('1e999')` returns `inf` through ordinary float parsing
   **without** `parse_constant` firing, so a strict codec whose decode relies
   solely on token rejection accepts a finite-looking literal that decodes
   nonfinite, at any depth. Fix: F5 should require (and test) a recursive
   post-parse finiteness sweep over the decoded value at any nesting depth,
   with explicit `1e999`-class probes (including nested), in addition to the
   token probes. The encode side is correspondingly fine only if the design
   mandates a finite check before `allow_nan=False` serialization; the plan
   already covers NaN/±Inf encode probes, so one clause extending the same
   requirement to decode closes the asymmetry.

### Minor observations (non-blocking)

4. **S1.4 conditional distinctness check.** "must fail the identity
   cross-check … *if* the design mandates distinctness at validation" is
   acceptable under the plan's own placeholder policy, but the executable
   suite must not ship with a conditional: at design freeze this becomes
   either a hard rejection assertion or an explicitly recorded
   not-mandated statement. The plan's diff-check rule already covers this;
   noted so it is not forgotten.

5. **S6.2 fixture strategy verified implementable.** The six legacy suites
   named in S6.1 all exist under `tests/`; re-running the full suite on this
   branch confirms 405 passing tests on Python 3.13, so the baseline count
   the plan will diff against is accurate. Self-generated v1/v2 fixtures are
   text-manifest based (`hdf5.py:81-82` stores `str(value)` attributes;
   `examples/thermal_experiment.py:250-252` already writes
   `json.dumps(..., sort_keys=True, allow_nan=False)` via `set_meta`), so no
   binary artifact needs to be committed and legacy readers remain
   sufficient. Sound as planned.

6. **Version-portability rules (S7) are sound.** The autospec/`wraps`
   prohibition (CPython gh-75988, 3.9 false failures) is correctly
   characterized and matches the Phase 1 correction record. The strict-JSON
   techniques this suite needs (`object_pairs_hook`, `parse_constant`,
   `allow_nan=False`, `sort_keys` determinism) have stable behavior across
   Python 3.9–3.13; no portability hazard identified in S1–S6. The dual-version
   execution requirement (S7.7) and the no-hardcoded-environment-facts rule
   are both necessary and correctly scoped.

7. **No Phase 3 leakage found.** S1–S5 exercise only the pure codec and
   client validator/construction seams; S5.4's signature-inspection contract
   ("codec API accepts no backend/file handle") is a legitimate Phase-2
   proxy for "no HDF5 write before validate" and correctly defers the real
   persistence assertion to Phase 3; S6.5/S6.6 keep the additive key
   decoupled from legacy hydration per finding F-B's disposition. Nothing
   in the plan reaches into HDF5 `set_meta` write paths, copy/preflight, or
   hydration inspection.

## Verified as implementable without legacy changes (positive confirmation)

1. **All S1–S5 techniques exist in-tree today as reusable patterns.**
   Closed-record validation with type-identity checks (`type(x) is not int`,
   rejecting `True`/`1.0` where exact integers are required) at
   thermal_experiment.py:352–508; duplicate-key rejection via
   `object_pairs_hook` at 520–526; nonfinite-token rejection via
   `parse_constant` at 528–530; `allow_nan=False` + `sort_keys` encode at
   510–512. A new Phase 2 codec module can apply the same standard-library
   techniques to the additive envelope with zero modification to the legacy
   v1/v2 machinery, exactly as decision §6 items 7–9 require.
2. **Every failure-matrix row maps to an enforceable mechanism.** F1/F2
   (input type + root-type gate), F3 (pairs hook, all depths), F4 (encode
   allowlist walk naming field and type), F5 (encode finite check + decode
   sweep per finding 3), F6–F10 (closed-schema required lists, version
   allowlists, profile dispatch), F11 (frozen allowlists at both layers,
   case/underscore near-miss probes), F12 (type-identity rules), F13
   (profile-owned validators) — all are implementable against the decided
   envelope as pure functions of the frozen design literals.
3. **F11 traceability practice is good.** Recording the rejected
   unknown-fields-preserved alternative with its mandating decision text
   makes any future boundary relaxation an explicit decision revision —
   consistent with the project's closure and decision-revision discipline.

## Closure conditions

This review closes when the plan text addresses findings 1–3 (additive
edits), after which no further developer input is anticipated for findings
4–7. The executable suite remains gated on the reviewed Phase 2 detailed
design freezing the placeholders the plan correctly lists.

## Re-review (2026-11-01, after sw-mike revision 21cf8f4)

Method: per-finding verification of the revised plan text
(`log/release_12/test/schema-test-plan.md` @ 21cf8f4) against this review's
findings and the binding inputs (decision §6 quoted verbatim below for
consistency; PRD M03-AC02). Source-level inspection only.

1. **Null-vs-absent coverage (required) — CLOSED.** New S3.5 covers all
   three mandated facts: (a) rejection side — S3.5.1 with per-field
   parametrization, cross-referenced to failure-matrix row F14, field set
   taken from the frozen schema, not assumed; (b) acceptance side — S3.5.2
   for fields where the design assigns null meaning, with verbatim
   round-trip; (c) distinctness — S3.5.3 pins absent/present-null/
   present-valid-value as three semantically distinct decodes, with the
   decision §6 item 6 "never implies success, zero, empty, or
   not-applicable" clause asserted verbatim. S3.5.4 additionally pins the
   whole-record edge exactly as decision §6 item 6 states it ("Absence of
   the whole additive key means only 'no additive common record' …"). The
   F14 row correctly partitions fields from the S3.5 acceptance side, so no
   field is both fed to F14 and asserted accepted in S3.5. Requirement basis
   cites both M03-AC02 ("absent versus null") and decision §6 item 6
   verbatim. Consistent; complete.
2. **Availability / unavailable-reason semantics (required) — CLOSED.**
   S1.7 pins the valid-side semantics (captured fact = observed value;
   uncaptured fact = frozen unavailable-state literal + reason from the
   frozen bounded vocabulary; never copied from another client; never a
   success-like stand-in) with verbatim round-trip, traced to decision §1.1
   availability row and §6 items 1 and 5. F15 adds the negative probe for
   success-like presentation of uncaptured facts; F16 adds the
   vocabulary/binding probes (out-of-vocabulary reason, reason missing
   where required, reason present without the unavailable state). The
   vocabulary is explicitly design-owned: the placeholder list now freezes
   "the availability/unavailable state literal, the bounded
   unavailable-reason string vocabulary, and the per-field reason bound" as
   detailed-design outputs, and S4.3 anchors the previously dangling
   reason-strings count bound to the S1.7/F16 facts, read from the module's
   exported constant where one exists. Semantics pinned; literals correctly
   deferred to the design freeze.
3. **F5 recursive decode-side finite sweep (required) — CLOSED.** F5 now
   mandates a recursive post-parse finiteness sweep over the decoded value
   at any nesting depth, explicitly noting that `parse_constant` alone does
   not fire on overflow literals (`json.loads('1e999')` → `inf` through
   ordinary float parsing). Probe classes are complete: token probes
   (`NaN`, `Infinity`, `-Infinity`) and `1e999`-class overflow literals,
   each at top level and inside a nested container three levels down — a
   superset of the review's minimum ask. Encode side retains the NaN/±Inf
   probes plus the design's finite check before `allow_nan=False`
   serialization; the decode/encode asymmetry is closed.
4. **S1.4 conditional distinctness check (minor) — CLOSED.** The freeze
   note is added: provisional until design freeze, becoming either a hard
   rejection assertion or an explicitly recorded "not mandated" statement,
   never a conditional in the executable suite — exactly the closure
   condition recorded.

**Minor observation (non-blocking, no finding):** S4.3 houses the per-field
reason-string length bound under "Count bounds"; the heading is element-count
oriented but the section title "Size/depth limits" covers it, the text states
the binding explicitly, and S4.4's read-from-exported-constant rule applies.
No action needed; noted for the executable-suite step.

**Overall verdict: APPROVED.** All 3 required fixes and the minor item are
closed in plan text; no new findings. The plan remains gated on the reviewed
Phase 2 detailed design freezing the listed placeholders (namespace, versions,
required lists, limits, error types, availability literals/vocabulary) per the
plan's own gate-linkage section.

---

## Re-Review — Final (delta revisions applied, design APPROVED)

Reviewer: sw-tom (Developer) | 2026-11-01 Asia/Shanghai
Input: [schema-test-plan.md](../test/schema-test-plan.md) at `523fd8e`
(delta-revised per design §8.8) against
[p2-schema-design.md](../design/p2-schema-design.md) at `4b4b6db`
(second revision; architecture review findings all CLOSED, approval
recorded at `c058ec1`).
Method: source-level inspection only — no test code written, no source
modified. Every frozen literal, boundary pair, error type/message, and
layering assertion in the plan was quote-checked against the approved
design text; the §8.7 revision table (12 items) and §8.8 delta table
(6 items) were each traced to a concrete plan location.

### Per-change verification

**§8.7 revisions 1–12 (fc42bf9 alignment) — all applied and correct:**

1. Scope paragraph includes the in-memory group seam `build_run_group`
   while excluding HDF5/backend/inspection (Phase 3) — lines 33–43. ✓
2. S1.8 group-construction assertions: encoded-before-constructed order,
   empty group with `str(group.id) == envelope['group_id']`, exactly one
   `set_meta('run_record', text)`, no `meta=`/`set_meta(key, None)`, no
   I/O, failure atomicity, hand-held recorder double (never
   `autospec=True` + `wraps`) — matches design §4 and §8.7 item 2's
   assertability correction. ✓
3. F13 drift probes: thermal `THERMAL_MODEL` and signal `SIGNAL_OFFSETS`
   exact-equality negatives with frozen messages, plus frozen record
   names. ✓
4. F15 revised to structural-only validation with the [R6]
   capture-truthfulness responsibility split (Phase 3/4 seam), removing
   the codec-rejects-fabricated claim. ✓
5. S1.7 carries the same [R6] responsibility split on the positive side. ✓
6. S3.5 container-type probes: root `TypeError` (`run_record: expected
   dict`), nested `RecordValidationError`, no normalization, annotation
   inspection. ✓
7. S3.6 huge-integer probes present (see delta 1 below for corrections). ✓
8. S4.1 layered depth formulation, scalar-at-depth-8 acceptance, and
   parser-recursion probe with the `RecursionError` mapping. ✓
9. S4.2 replaced by the boundary pair + maximal-envelope assertion (see
   delta 2 below for corrections). ✓
10. S4.3 layered count/length formulation per the reachability table. ✓
11. S4.4 mechanism and constant set correct. ✓
12. S6.3 export-surface assertion present (see delta 4 below). ✓

**§8.8 deltas 1–6 (523fd8e) — all applied and correct:**

1. S3.6 now freezes `MAX_INTEGER_DIGITS = 1024` with the pure-integer
   `_MAX_INTEGER_ABS = 10 ** MAX_INTEGER_DIGITS` comparison, the
   sign-stripped decode token rule (`-` + 1024 digits accepted, matching
   encode), and the signal-profile pinning of the at-limit valid-envelope
   probe (`10**1023` acceptance, 1024/1025 boundary, round-trip boundary
   argument). The withdrawn `_MAX_INTEGER_BITS = 3398` mechanism appears
   only as an explicitly recorded withdrawal note — not as an asserted
   value. ✓
2. S4.2 uses the corrected ≤ 66,472-byte derivation (eighth finite real
   `payload.timing.dt` included), the ≈ 1.97× headroom claim, the
   constructible maximal fixture (four free-text fields at cap with
   worst-case non-BMP escaping, signal-profile counts at the digit cap,
   ordinary floats), and the corrected 131,072-byte probe expectation
   (`'{}'` + padding passes the root-type check and fails contract
   validation with `RecordValidationError`, not `RecordDecodeError`).
   The stale 65,448 figure appears nowhere. ✓
3. S4.3's reason-length 64/65 pair is assigned to the contract layer
   (64 → vocabulary failure proving the length gate accepted; 65 →
   `<path>: reason length exceeds limit 64` on both encode and decode),
   matching design §7.2's "reason length is contract-layer, not sweep". ✓
4. S4.4/S6.3: module-only constant imports; the seven `MAX_*` names are
   asserted **absent** from `softlab.shui` (ImportError) and the package
   re-export list is exactly the design §8 table's eleven names. The
   withdrawn dual-package import assertion appears only as a withdrawal
   note. ✓
5. F17 (unpaired surrogate) with the frozen
   `run_record: input text is not encodable as UTF-8 (unpaired
   surrogate)` message, mapped at the decode size pre-check, asserted
   identical on 3.9 and 3.13; mirrored in the frozen-literals bullet. ✓
6. S1.8 name-contract probes: integer name → builtin `TypeError`
   (`run_group: name must be an exact str`), empty name → builtin
   `ValueError` (`run_group: name must be nonempty`), both at the seam's
   step-0 pre-check with zero `DataGroup` constructor calls asserted by
   the recorder double — matches design §4 step 0 / [R10]. ✓

**Literal spot-checks beyond the delta tables:** namespace/key/version
constants, profile identifiers, outcome/availability literals,
`UNAVAILABLE_REASONS`, the seven limits, the error taxonomy, the F1–F16
message templates, the `execution_id == group_id` hard rejection, the
canonical-UUID rule, the THERMAL_MODEL equation string, SIGNAL_OFFSETS,
the cross-layer count relations (thermal and signal), and the §8.6
canonical envelopes (coordinated/direct distinction, signal three-column
schema with `difference` absent) are all verbatim consistent with the
approved design. The ten-key closed root, no-optional-fields policy, and
null-only-in-fact-slots policy match §1.2/§2.4/§8.5.

**Version portability (review question 3):** no assertion found that
could pass on 3.13 and fail on 3.9 or vice versa. The integer probes use
pure integer comparison / token-length checks with no `str()`/`float()`
on unbounded values (accepted maximum 1024 digits is far below the 3.13
default guard of 4300); the `RecursionError` mapping makes deep-nesting
rejection interpreter-independent; F17's `UnicodeEncodeError` path is
identical on both interpreters; `json` overflow literals (`1e999` →
`inf`) behave identically; S1.3's byte-identical determinism follows
from frozen `sort_keys`/`separators`/`ensure_ascii` and contains no
hash-order, timestamp, or UUID-generation dependence. S7.3 retains the
Phase 1 lessons (no `autospec=True` + `wraps`, no hardcoded environment
facts, warnings discipline, 3.9 run before handoff).

**Scope (review question 4):** no Phase 3 leakage — persistence, backend
I/O, copy/preflight/hydration, storage-level absent-vs-invalid, and
capture-truthfulness are all explicitly deferred, with the codec-side
contributions (S3.5.4, S6.5/S6.6) correctly bounded. The group seam and
the three builders are correctly in scope as Phase 2 deliverables.

### Findings

**Required fix (under-scope against the design's freeze table):**

1. **Missing negative probes for the design §8.4 "Counts relation" and
   "Cross-layer count drift" rows.** The design's test freeze table
   (§8, "Concrete literals the approved test plan MUST assert verbatim")
   freezes two message templates that no plan case asserts:
   - `outcome.completed_rows: differs from outcome.requested_rows`
     (§1.5 freezes `completed_rows == requested_rows`; plan S1.5 asserts
     the relation only on the positive side).
   - `<path>: inconsistent with outcome.requested_rows` (§1.6/§1.7
     freeze the cross-layer count equalities; plan F13's negative probes
     cover boundary cross-checks — `final_tick == completed_rows`,
     `|final_sim_time - points*dt| <= 1e-10` — but not the counts
     themselves, e.g. thermal `configuration.points ≠
     outcome.requested_rows` or signal `payload.points ≠
     outcome.requested_rows`).

   The fix is additive plan text only: extend F13 (or add the probes
   beside S1.5) to feed one-defect count mismatches — requested ≠
   completed, and per-profile points ≠ requested_rows — and assert the
   two frozen messages verbatim. This closes the last gap between the
   design's §8.4 freeze table and the plan's failure matrix; everything
   else in §8.4 maps to a named plan case.

**Observations (non-blocking, no finding):**

- The frozen-literals bullet phrases the taxonomy as "all subclass
  `ValueError`". The design's class table gives `RunRecordError` the base
  `Exception` (only the two leaf classes mix in `ValueError`); the
  wording mirrors design §8.4's closing sentence and is acceptable in
  plan text, but the executable suite must assert `isinstance(exc,
  ValueError)` only for `RecordDecodeError`/`RecordValidationError`,
  never for `RunRecordError` itself. No plan change required — record
  for the implementation step.
- Portability nit: the 3.13 guard assumption is the interpreter default
  (4300); the plan correctly forbids mutating it (S5.5) and no test
  depends on a non-default guard value. No action.

### Overall verdict

**CHANGES REQUESTED** — for finding 1 only. The two design revisions
(§8.7 × 12, §8.8 × 6) are applied exactly, every checked literal is
verbatim consistent with the approved design, no stale first-round value
is asserted, portability and scope are clean. The single missing case is
a small additive extension of F13/S1.5; once the counts-relation and
cross-layer count-drift negatives are added with their frozen messages,
this plan is executable as written and I expect to approve without
further iterations.

---

## Closure (2026-11-01, after sw-mike fix `25a0c2e`)

Method: quote-check of the revised plan text
(`log/release_12/test/schema-test-plan.md` @ `25a0c2e`) against the
approved design (`p2-schema-design.md` @ `c058ec1`) §8.4 freeze table and
§3 class table. Source-level inspection only.

**Finding 1 (count-mismatch negative probes) — CLOSED:**

1. **F13 count-mismatch probes (a/b/c) — correct and verbatim.** All
   three probes assert the frozen messages character-for-character
   against design §8.4:
   - (a) `outcome.completed_rows != outcome.requested_rows` ⇒
     `RecordValidationError`,
     `outcome.completed_rows: differs from outcome.requested_rows`
     — verbatim the §8.4 "Counts relation" row template.
   - (b) thermal `configuration.points != outcome.requested_rows` ⇒
     `configuration.points: inconsistent with outcome.requested_rows`
     — the §8.4 "Cross-layer count drift" template with `<path>`
     correctly instantiated to the thermal layer, matching the §1.6
     frozen equality `configuration.points == outcome.requested_rows`.
   - (c) signal `payload.points != outcome.requested_rows` ⇒
     `payload.points: inconsistent with outcome.requested_rows`
     — same template instantiated to the signal layer, matching §1.7's
     `payload.points == outcome.requested_rows`.
   Each probe is one-defect with all other counts left consistent, per
   the finding's fix instruction; the "each frozen message asserted
   verbatim" clause is stated in the row text. The positive-side
   relations in S1.5 are untouched and remain asserted.
2. **S1.5 negative-side pointer — correct.** The S1.5 bullet now carries
   the negative side explicitly, names F13's count-mismatch probes as
   the asserting location, quotes both frozen messages verbatim, and is
   traced to the §8.4 "Counts relation" / "Cross-layer count drift"
   rows. No duplication, no divergence from the F13 row text.
3. **Leaf-class `isinstance` note — correct and complete.** The
   executable-suite note added to the frozen-literals bullet records
   exactly the disposition of this review's observation: assert
   `isinstance(exc, ValueError)` only for the leaf classes
   `RecordDecodeError`/`RecordValidationError`; never for
   `RunRecordError` itself, whose class-table base (design §3) is
   `Exception` only. The note also explains that the plan's "all
   subclass `ValueError`" phrasing mirrors design §8.4's closing
   sentence and applies to the leaf classes — consistent with the
   approved design text.

No other plan text changed in `25a0c2e` (diff reviewed: +18/−2, single
file). All literals previously verified at the final re-review remain
verbatim consistent; no new findings.

**Overall verdict: APPROVED.** The plan is executable as written,
gated on the reviewed Phase 2 detailed design freezing the listed
placeholders per the plan's own gate-linkage section.
