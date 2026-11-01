# Code Review Report — M03 Phase 2: Bounded run-record schema/codec and construction seam

## Review Information

- **Reviewer**: sw-celeste
- **Date**: 2026-11-01 Asia/Shanghai
- **Branch**: `codex/m03-p2-schema`
- **Commit**: `a437dc3` (`feat(shui): add bounded run-record codec and group seam`)
- **Files reviewed**: `softlab/shui/run_record.py` (new, 1173 lines),
  `softlab/shui/run_group.py` (new, 68 lines),
  `softlab/shui/__init__.py` (+17 lines)
- **Reviewed against**:
  - Design: [p2-schema-design.md](../design/p2-schema-design.md)
    (APPROVED basis, commit `4b4b6db`)
  - Test plan: [schema-test-plan.md](../test/schema-test-plan.md)
    (delta-revised, commit `25a0c2e`)
  - Architecture: [arch.md](../../../arch.md); binding boundary:
    [p1-boundary-decision.md](../design/p1-boundary-decision.md) (option (a),
    §6 binding constraints)
- **Environment**: local Python 3.13.15 (project `.venv`); 3.9 runtime
  evidence remains a CI/tester gate per plan S7.7 (grammar-level 3.9
  compatibility verified here via `ast.parse(..., feature_version=(3, 9))`).

## Summary

- **Status**: **APPROVED**
- **Total Issues**: 0 defects
- **Critical**: 0
- **High**: 0
- **Medium**: 0
- **Low**: 0
- **Informational observations**: 3 (none blocking; see §"Observations")

The implementation was verified against every frozen element of the design —
constants, literals, message templates, limits, layering rules, error
taxonomy, export table, D1-safe group seam, and the no-I/O guarantee — with
a 190+ assertion adversarial probe suite executed against the built package.
Every probe passed. The full pre-existing suite (405 tests) passes
unmodified.

## Design Compliance

- [x] Implementation matches detailed design
- [x] All interfaces match design signatures (`RunRecordCodec` abstract with
      `encode`/`decode`; `JsonRunRecordCodec`; `build_thermal_payload`,
      `build_signal_payload`, `build_envelope` with the exact keyword-only
      signatures of design §4; `build_run_group(name, envelope)`)
- [x] Data structures match design (closed key sets, frozen column schemas,
      `THERMAL_MODEL`, `SIGNAL_OFFSETS` — value-compared verbatim)
- [x] No unauthorized design deviations

### Frozen constants and literals (design §1.1, §1.4–§1.7, §8.1–§8.3)

Verified by direct assertion: `NAMESPACE`, `METADATA_KEY`, `FORMAT_VERSION`,
`PROFILE_THERMAL`/`PROFILE_SIGNAL`, both payload versions, all outcome and
availability literals, `UNAVAILABLE_REASONS`, all seven numeric limits
(`131072 / 8 / 64 / 64 / 1024 / 64 / 1024`), `THERMAL_MODEL` (including the
exact equation and validity strings and all seven units), and
`SIGNAL_OFFSETS = {'signal': 5, 'reference': 2}` — all exact.

### Message templates (design §3, §8.4)

Every frozen template reproduced verbatim against the live codec, including:
`run_record: malformed JSON: ...`, `run_record: root must be an object`,
`run_record: duplicate object key '<key>'` (root and nested),
`<path>: unsupported value type '<typename>'`,
`run_record: nonfinite JSON token '<token>'`, `<path>: nonfinite number`
(top-level and three levels nested),
`<path>: missing required field '<key>'` (all ten root keys),
`version: unsupported version (supported: 1)`,
`payload.version: unsupported version (supported: 1)`,
`profile: unsupported client profile`, `<path>: unknown field '<key>'`
(including case-differing keys), the F12 expected-type vocabulary,
`<path>: explicit null has no assigned meaning`,
`<path>: invalid available value`, the three F16 binding templates,
`execution_id: must differ from group_id`, `<path>: expected canonical UUID`,
all sweep-limit templates, `<path>: reason length exceeds limit 64`,
`run_record: input text is not encodable as UTF-8 (unpaired surrogate)`,
both integer-digit templates, `<path>: expected finite real`,
`run_record: nesting exceeds parser recursion limit`,
`run_record: expected dict` / `run_record: expected JSON text`,
`outcome.completed_rows: differs from outcome.requested_rows`,
`configuration.points: inconsistent with outcome.requested_rows`,
`payload.points: inconsistent with outcome.requested_rows`,
`payload.model: unsupported model semantics or units`,
`payload.read_policy.offsets: unsupported read offsets (supported:
{'signal': 5, 'reference': 2})`, and both `run_group:` name-contract
messages. All exact-string equality matches.

### Limits and layering (design §2.3, §7.2)

- Depth: container-only accounting confirmed — an exactly-8-deep container
  at an extra key passes the sweep and fails at the contract layer
  (`unknown field`); a 9-deep container fails the sweep; a scalar child of a
  depth-8 container is accepted by the sweep. Encode raises
  `RecordValidationError`, decode `RecordDecodeError`, both with
  `run_record: nesting depth exceeds limit 8`.
- Array/object/string: 64/65 and 1024/1025 boundary pairs behave exactly per
  the reachability table (layered probes verified both sides).
- Reason length: contract-layer on both paths — 64 chars passes the length
  gate then fails vocabulary; 65 chars fails the length check, with
  `RecordValidationError` on **both** encode and decode ([R5] second
  revision, plan delta 3). Verified.
- Integer digits: signal-profile 1024-digit `requested_rows` (with matching
  counts) encodes, decodes, and re-encodes byte-identically; 1025 digits
  rejected — encode `RecordValidationError`, decode `RecordDecodeError`.
  `10**1023` (smallest 1024-digit positive) accepted on both paths; the
  decode sign rule verified (`-` stripped before digit count; `-` + 1024
  digits accepted by the parse bound, `-` + 1025 rejected at parse).
- Encoded size: 131,072-byte input (`'{}'` + padding) passes the gate and
  fails later with `RecordValidationError` (`missing required field
  'namespace'`); 131,073 bytes rejected with `RecordDecodeError`
  (`run_record: encoded size exceeds limit 131072 bytes`). Maximal
  constructible valid envelope (four 1024-code-point non-BMP free-text
  fields + 1024-digit signal counts) encodes to **29,008 bytes** and
  round-trips — well under the 66,472-byte derived ceiling, confirming the
  ≈ 1.97× headroom claim of design §2.3.
- Finite-real overflow guard: a 1024-digit int fed to a real field maps to
  `RecordValidationError` (`expected finite real`) on encode **and** decode;
  the thermal `points * dt` cross-check overflow maps to the frozen
  boundary message. No builtin `OverflowError` escapes.

### Error taxonomy (design §3)

`RunRecordError(Exception)` (not a `ValueError`, never raised directly);
both leaves subclass `RunRecordError` and `ValueError`. Builtin `TypeError`
for non-`dict` encode / non-`str` decode with the frozen messages. No
escaping builtins observed on any probed path, including the
unpaired-surrogate UTF-8 pre-check ([R9]) and the parser-recursion mapping
([R4]).

### Export table (design §8, [R8] second revision)

`softlab.shui` exposes exactly the eleven frozen names and nothing else from
this feature; all seven `MAX_*` limits are absent at package level and
importable from `softlab.shui.run_record`; `NAMESPACE`, `THERMAL_MODEL`,
etc. stay module-only; pre-existing exports (`DatabaseBackend`,
`catch_error`, `data`, `profile`) unchanged. Verified by attribute
assertions in both directions.

### D1-safe group seam (design §4, [R1], [R10])

Verified with a hand-held recorder double patching
`softlab.shui.run_group.DataGroup` / `encode_envelope` (both patchable
module-level names — the recorder-double mechanism of plan S1.8/S7.3 is
fully observable):

- Call order is exactly `encode → construct → set_meta`; zero constructions
  on any failure path (invalid envelope, non-`str` name, empty name).
- Constructor receives exactly `name=` and `id=` (a `uuid.UUID` equal to
  `envelope['group_id']`) — never `meta=`.
- Exactly one `set_meta('run_record', text)` call; `text` is byte-identical
  to the `encode_envelope` output; the resulting meta dict holds only that
  one key; `set_meta` is never called with `None`.
- Step-0 name contract enforced by the seam itself: integer name ⇒ builtin
  `TypeError` (`run_group: name must be an exact str`), empty name ⇒
  builtin `ValueError` (`run_group: name must be nonempty`), both before any
  construction (an integer name is never stringified by `DataGroup`'s
  inherited coercion).
- End-to-end with the real `DataGroup`: empty group (`records == []`),
  `str(group.id) == envelope['group_id']`, one metadata entry that decodes
  back to the original envelope.

### No-I/O guarantee

`run_record.py` imports only `abc`, `json`, `math`, `re`, `uuid`, `typing`
(AST-verified). No public codec/seam signature accepts a backend, file
handle, or path (signature inspection). `run_group.py` contains no
save/connect/disconnect/open calls (AST-verified). The codec is stateless;
failed encode leaves caller input byte-identical (deep-copy compared,
including a nested-payload failure path); decode failure leaves no
cross-call contamination.

## Test-Plan Satisfiability (S1–S7 + F1–F17 observability)

Every plan assertion was checked for implementability against the actual
code:

- S1.1–S1.7, S2 F1–F17, S3 (incl. container-type probes [R7] and
  huge-integer probes [R2]), S3.5, S4.1–S4.4, S5.1–S5.5, S6.3–S6.4: all
  directly reproducible; the probes above mirror each plan row and pass.
- S1.8 recorder-double assertions (constructor call arguments, exactly-one
  `set_meta`, failure atomicity, zero-construction failure paths) are fully
  observable via `mock.patch`/recorder on `run_group.DataGroup` and
  `run_group.encode_envelope` — no `autospec`+`wraps` needed.
- S6.1/S6.4: the 405-test baseline passes unmodified (verified locally,
  Python 3.13.15; 3.9 is a CI gate per plan S7.7).

**Observability guidance for sw-mike (not a defect):** the S4.1
parser-recursion probe must use an input deep enough to trip the parser
itself on **both** interpreters. On Python 3.13.15 the C JSON scanner
tolerates ≳ 5,000 nesting levels (the codec's own depth sweep rejects those
first with `run_record: nesting depth exceeds limit 8`); the
`RecursionError` mapping path is exercised at ≥ ~20,000 levels. A probe of
60,000 levels (120,000 bytes, within `MAX_ENCODED_BYTES`) was verified here
to produce `RecordDecodeError: run_record: nesting exceeds parser recursion
limit`; on 3.9 the parser trips at a much lower depth, so the same probe
shape works there. Both messages are `RecordDecodeError`, so the taxonomy
is interpreter-independent even where the intermediate message differs.

## Architecture Compliance

- [x] Follows arch.md and the Phase 1 binding decision (option (a))
- [x] Uses defined interfaces (DIP seam `RunRecordCodec`; single justified
      implementation)
- [x] Proper error handling (no swallowed errors; every defect raises;
      builtin interpreter exceptions mapped at their call sites)
- [x] No code duplication (one shared `_sweep_value` parameterized by error
      class/build flag; one shared `_validate_envelope` code path for
      builders, encode, and decode)
- [x] Module placement per design: codec stdlib-only and decoupled from
      `shui/data/`; `run_group.py` is the only new importer of
      `shui.data.base` — no import cycle
- [x] Legacy untouched: no modification outside the three files; legacy
      manifest machinery, examples, and `shui/data/` implementations
      unchanged (commit diff confirms)

## Simplicity Audit

- [x] No unnecessary third-party dependencies — **zero new dependencies**;
      standard library only in the codec; the seam adds only the mandated
      `DataGroup` import
- [x] No over-engineering or premature generalization — one codec
      implementation behind the Phase-3-justified abstraction; helpers are
      private; no speculative configurability
- [x] Code is as simple as it can be while meeting requirements — the
      parameterized sweep avoids a duplicated encode/decode traversal; the
      closed-key-set tables keep validation declarative
- [x] Data structures are minimal and appropriate (tuples of key names,
      frozen literal dicts, no new classes beyond the frozen taxonomy)

## Quality Checks

- [x] No compiler errors (`python -m compileall -q softlab` — clean)
- [x] No compiler warnings
- [x] No lint-level issues found (≤ 80-column lines throughout; no
      `Mapping`/`Sequence` annotation drift — signatures annotate
      `Dict`/`List` per [R7]; `typing` style consistent with repo `py39`
      target; no 3.10+ grammar — `ast.parse(feature_version=(3, 9))` passes
      on both new modules)
- [x] Import warning-clean under `-W error::DeprecationWarning -W
      error::FutureWarning`
- [x] Tests pass (405/405 locally on 3.13.15; independent execution by
      sw-mike pending — this review verifies satisfiability, not test
      results)
- [x] Docstrings: English, complete, on all public API and private helpers
- [x] `git diff --check` clean; worktree clean; only the three intended
      files in the commit

## Observations (informational, no change requested)

1. **`build_thermal_payload` docstring** states "Raises ``TypeError`` for
   non-dict container arguments" but the builder also raises `TypeError`
   for a non-`str` `adapter`. Behavior is correct and probed; the docstring
   could mention it. Cosmetic; not a gate item.
2. **Root-level missing-field messages** render as bare
   `missing required field '<key>'` (empty `<path>` prefix via `_at`),
   documented in the helper. Nested paths render normally. This matches the
   plan's `<path>: ...` template with an empty root path; recorded for
   tester awareness when writing exact-message assertions at the root.
3. **Recursion-probe depth selection** guidance for sw-mike — see the
   observability paragraph above.

## Reproduction Record

Commands actually run (repo root, project `.venv`, Python 3.13.15):

- `python -m unittest discover -s tests -p 'test_*.py'` → **Ran 405 tests,
  OK** (baseline intact; zero pre-existing tests weakened)
- `python -m compileall -q softlab` → clean
- `python -c "import softlab; print(softlab.__version__)"` → `0.3.0`
- Adversarial probe suite (190+ assertions, this review's own script held
  outside the repo): constants, taxonomy, export surface, py39 grammar,
  valid round-trips (thermal coordinated/direct, signal), byte-level
  determinism, decode idempotence, re-encode stability, finite-real
  normalization, identity distinctness/canonical UUID, full F1–F17 matrix
  with exact frozen messages, container-type probes, huge-integer
  encode/decode boundaries with sign rule, all limit boundary pairs per the
  reachability table, maximal-envelope construction (29,008 bytes),
  side-effect-free failure (input untouched after failed encode), builders
  vs canonical fixtures, recorder-double seam assertions (call order,
  constructor args, exactly-one `set_meta`, failure atomicity, name
  contract), real-`DataGroup` end-to-end, stdlib-only import audit, no-I/O
  call audit. **All passed.**

## Approval

- [x] All issues resolved (none found)
- [x] Design faithfully implemented
- [x] Code meets quality and simplicity standards
- [x] **Approved for testing by sw-mike**

— sw-celeste, 2026-11-01
