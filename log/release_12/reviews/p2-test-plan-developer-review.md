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
