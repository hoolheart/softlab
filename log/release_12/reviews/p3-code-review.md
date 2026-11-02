# Code Review Report — M03 Phase 3: Storage integration and safe inspection

## Review Information

- **Reviewer**: sw-celeste
- **Date**: 2026-11-03 (Asia/Shanghai)
- **Branch**: `codex/m03-p3-storage`
- **Commit under review**: `0217593` (`feat(shui): add owned run-record
  storage and inspection seam`)
- **Design authority**: `log/release_12/design/p3-storage-design.md`
  (`b52e034`, APPROVED, third revision)
- **Test plan authority**: `log/release_12/test/storage-test-plan.md`
  (`d7d7169`, APPROVED, ordering deltas applied)

## Summary

- **Status**: APPROVED
- **Total Issues**: 0 blocking; 2 non-blocking observations
- **Critical**: 0
- **High**: 0
- **Medium**: 0
- **Low**: 0 (two observations recorded below with explicit
  no-fix-required rationale)

## Design Compliance

- [x] Implementation matches the detailed design — verified section by
  section against `b52e034`
- [x] All interfaces match design signatures (`save_run_group(group,
  path, *, backend=None)`, `inspect_run_group(path, group_id=None, *,
  legacy_keys=(), backend=None)`)
- [x] Data structures match design (frozen `StorageReport` /
  `StoredRecordFacts` / `InspectionResult`; field sets exact)
- [x] No unauthorized design deviations

Point-by-point conformance, all verified by reading **and** by
execution (probe evidence listed under "Reproduction" below):

| Design clause | Implementation | Result |
| --- | --- | --- |
| §2.2 save algorithm steps 0–5, frozen order, report-at-entry | `run_storage.py:420-485` | Conformant; step 2 codec errors propagate unwrapped with additive `.report` attachment (R3); step 4 captures `BaseException` so *every* escape — including `KeyboardInterrupt` — carries the populated report, which is exactly what the §2.2 docstring contract ("EVERY exception that leaves this function carries `.report`") requires and what the re-wired m02_w04 test relies on |
| §2.5 error taxonomy | `run_storage.py:179-202` | Conformant: base subclasses `Exception`; the four leaves multiply-inherit `ValueError`; `isinstance(exc, ValueError)` and `isinstance(exc, RunStorageError)` both verified at runtime |
| §2.5 frozen message templates | — | **All 20 templates verified character-exact at runtime** (see probe list): `backend: unsupported backend type 'sqlite3' (supported: 'hdf5')`, `run_record: additive key missing from group metadata`, `group_id: group identity '<a>' differs from envelope group_id '<b>'`, `record: missing declared record '<name>'`, `record.columns: differ from envelope declaration`, `record.rows: stored row count <n> differs from outcome.completed_rows <m>`, connect/save prefixes, `read: original artifact unreadable: <exc>`, both `group_id:` resolution templates, `preflight:` templates, `run_record: present value is not text`, `<key>: present value is not text`, `load:` prefix, all three `cross-check:` templates, both `safety:` templates |
| §2.6 frozen cleanup templates, verbatim, frozen append order | `run_storage.py:234-260` vs legacy `_disconnect` (`examples/thermal_experiment.py:333-352`) | Conformant and byte-identical literals: `Backend still connected`, `Backend disconnect left its file handle open`, `Raw handle close failed: <exc>`, `Scratch cleanup at <dir>: <exc>`; append order (exception text → `backend.errors` delta → still-connected → open-handle → raw-close failure) confirmed live: an injected disconnect fault produced `('writer cleanup fault', 'Backend still connected', 'Backend disconnect left its file handle open')` |
| §3.1.1 inspection ordering, R1–R6 | `run_storage.py:529-629` | Conformant: single `try/finally` from `TemporaryDirectory` creation; only step 0 (backend resolution) and step 1 (baseline hash) are pre-resource (R4(a)); the `finally` never raises (disconnect and scratch removal guarded, re-hash guarded — R6); safety failure dominates at finalize with `__cause__` chaining and two-entry `primary` on coincidence (R2 coincident rule) — all reproduced live |
| §3.5.1 by-name / row-index ordering | `run_storage.py:344-380` | Conformant: pre-save column check is order-sensitive against the in-memory record; disk cross-check is by-name with equal name sets; `StoredRecordFacts.columns` is h5py enumeration order (verified lexicographic on a live artifact) |
| §4 fresh-process protocol | `tests/run_record_inspect_bootstrap.py` | Conformant: `redirect_stdout(sys.stderr)` around the inspection call (stdout carries only the JSON payload — `json.loads(proc.stdout)` succeeds raw); exit codes 0/1/2 reproduced; JSON schema matches §4.3 exactly (success and failure shapes); warning policy mirrored **verbatim** from `tests/thermal_cli_bootstrap.py`, including the corrected pandas "before the concat operation" text (diff-verified); lazy `softlab` import after policy installation is a correct refinement for the 3.9 pyparsing allowance path |
| §9 / [P3P-06] nine-name export delta | `softlab/shui/__init__.py:30-40` | Exactly the nine frozen names (`save_run_group`, `inspect_run_group`, four error leaves + base, `StorageReport`, `InspectionResult`); `StoredRecordFacts` and the hydration constants correctly stay module-level; no wildcard; import placed after `run_group` — no cycle |
| §6.3 artifact pins | — | Reproduced: A4.1 no artifact; A4.9 directory byte-untouched; A4.2 empty connect-time file with zero groups; A4.3 (both variants) primary delivered with `cleanup_ok=False` |
| §7 no-repair discipline | `git diff 0217593^..0217593` | Conformant: `hdf5.py`, `backend.py`, `base.py`, `io.py` untouched; F-A/F-B/F-C/F-D/D1/D2 remain routed around, not repaired |

## Test-Plan Satisfiability (A1–A9)

Every plan area is satisfiable as written against this implementation:

- **A1**: the seam, the pre-connect rejection, the lifecycle recorder
  seam (`backend=` injection), and the `StorageReport` return all exist
  and behave as frozen. A1.2's by-name disk rule is implementable
  (enumeration order observed lexicographic).
- **A2**: sha256 before/after observable (`original_sha256` exposed);
  exactly-one-load and fresh-copy disciplines hold structurally;
  hydration sets are the frozen literals (`THERMAL_HYDRATION_KEYS`
  4-tuple, `SIGNAL_HYDRATION_KEYS == ()`, both mapped).
- **A3**: the bootstrap is spawnable today — I ran it as a fresh
  process and asserted exit 0/1/2 and both payload schemas.
- **A4**: every fault seam is real and reproducible — I executed A4.1
  (codec rejection, zero artifact), A4.2 (externally closed handle,
  empty-artifact pin), A4.3 **both variants** (save: cleanup-only
  diagnostics on success; inspection: primary delivered,
  `cleanup_ok=False`, frozen `Failed to disconnect:` entry, original
  bytes intact, scratch removed, zero `$TMPDIR` leftovers), A4.4's
  mechanism (raw-h5py attribute mutation → unwrapped
  `RecordDecodeError` with post-`finally` `.report`), A4.8 (missing
  reserved attrs, verbatim template), A4.9 (existing directory,
  verbatim prefix, untouched directory). The R6 rows (byte
  modification mid-inspection; re-hash failure via mid-inspection
  deletion; coincident safety+body with `__cause__` and two-entry
  `primary`) were additionally probed and behave exactly as frozen.
- **A5–A9**: no post-save metadata writes exist in the seam (read-only
  over `group.meta`); SQLite rejection is step-0 and pre-I/O
  (reproduced); scratch prefix is the frozen literal; the nine-name
  delta is exact.

## Legacy Preservation

- [x] `thermal_experiment.py` changes are purely additive:
  `RunResult.execution_id` field, `after_count` retention line,
  `_source_facts()` (new function), the envelope attach block, the
  `_load_group` additive hydration block
- [x] Legacy failure surface character-for-character: the writer block
  maps `StorageConnectError`/`StorageSaveError` to the legacy
  `RuntimeError` literals verbatim; the cleanup-only path raises
  `RuntimeError('; '.join(report.cleanup))` which reproduces the legacy
  `_disconnect` raise message exactly (the library cleanup entries are
  the legacy `_disconnect` diagnostics verbatim, same order); the
  `Writer cleanup: <exc>` wrapper at `:962` and the
  `_diagnostic`-to-stderr surface are unchanged (verified live — see
  w04 adjudication)
- [x] `_load_group`: the additive block acts only when `METADATA_KEY`
  is present in the pre-captured attrs; legacy files take the
  pre-change path byte-for-byte
- [x] No import-path changes; existing `softlab.shui` exports untouched
- [x] 459-test baseline green (reproduced independently, below)

## M02 Legacy-Test Adjudication (the flagged changes)

### `tests/test_m02_w01_characterization.py` — reload-appends 9 → 11 (2 tests)

**Verdict: LEGITIMATE alignment. No restoration required.**

Mechanism verified in source: `DataGroup.record()` returns
`DataRecord.copy()` (`base.py:570-573` → `base.py:200-203`), and
`copy()` replays `add_rows(self.table)` — so each read-only `record()`
call on a non-acquisition record produces exactly one event in the
instrumented `add_rows` trace. The approved wiring adds exactly two
such calls, both read-only copies:

1. `original.record('thermal_trace').columns` — the §2.4 envelope
   attach in `execute()`;
2. `group.record(record_name)` — the §2.2 step 3 pre-save cross-check
   in `save_run_group`.

9 + 2 = 11, matching the observed delta with no unexplained residue.
The characterization intent is untouched: the clean-trace ordering
assertions, table value assertions, and pure-observe counts are
unchanged; the updated comments name the cause with design references.
The count was always an "observed, separate" characterization value —
it now characterizes the wired reality.

### `tests/test_m02_w04_integration.py` — writer-cleanup interruption test (1 test)

**Verdict: LEGITIMATE alignment. No restoration required.**

The old injection seam **ceased to exist by approved design**: §2.4
routes the writer block through `save_run_group`, so
`thermal._disconnect` is no longer called by the writer block, and the
old duck-typed `Writer` fake (with `file = object()`, `connected =
True`) could never pass the seam's `isinstance(backend,
HDF5DataBackend)` step-0 guard. The rewrite is therefore forced, and
its choices are the right ones:

- A **real `HDF5DataBackend` subclass** replaces the duck-typed fake —
  strictly higher fidelity (real connect against a real file; the plan
  A4 injection discipline favors real seams).
- The cleanup fault is injected at the seam's **real** verified
  disconnect (class-level `disconnect` patch), not at a bypassed mock.
- The characterization intent — *cleanup failure must not mask the
  persistence interruption* — is preserved exactly:
  `assertIs(caught.exception, interruption)` still heads the
  assertions, and I reproduced the flow live: calls inventory is
  `['connect', 'save', 'disconnect', 'disconnect']` (the seam's
  verified disconnect records the fault; the example's `finally`
  retries under its unchanged writer-reference discipline).
- The new report assertions (`operation == 'save'`,
  `primary == ('save interrupted',)`, `cleanup_ok == False`, fault text
  in `report.cleanup`) assert the frozen Phase 3 observable — stronger
  than the replaced stderr string check.

The one dropped assertion (`'writer cleanup fault' in stderr`) does
**not** correspond to lost behavior: I reproduced the scenario and
confirmed the example layer still emits `Writer cleanup: writer
cleanup fault` through `_diagnostic(result)` on stderr. The fault text
is now asserted in the structured report instead; the legacy stderr
surface is intact but unasserted. Optional (not required): the stderr
assertion could be re-added alongside the report assertions.

## Issues Found

None requiring change. Two observations recorded for completeness:

### Observation 1 (Low, no fix required) — unenforced column-count agreement invariant

- **Location**: `softlab/shui/run_storage.py:330-340` (`_preflight`)
- **Description**: `StoredRecordFacts.row_count` takes the first
  enumerated column dataset's `shape[0]`; the documented invariant "all
  column datasets of one record must agree" (design §3.2, mirrored in
  the docstring) is not itself verified.
- **Impact**: None on writer-produced artifacts (all columns come from
  one table). A hand-corrupted fixture with disagreeing column lengths
  fails truthfully downstream at the legacy load (pandas `DataFrame`
  construction), so no silent-pass path exists. The §3.1.1 frozen
  algorithm contains no agreement-check step, so the implementation is
  conformant as written.
- **Status**: RECORDED — no change required; Phase 4 test authors
  should not construct fixtures that rely on per-column disagreement
  being caught at preflight.

### Observation 2 (Low, no fix required) — unreachable `report=None` fallback in the bootstrap

- **Location**: `tests/run_record_inspect_bootstrap.py:133-134`
- **Description**: the exit-1 payload serializes
  `report if report is not None else None`, while design §4.3 freezes
  `report` as always present. Rules R1–R6 make the `None` branch
  unreachable for any `Exception` escaping `inspect_run_group`
  (verified across 23 probe scenarios, including codec errors and
  safety failures); the branch is harmless defense — any test asserting
  the schema would fail on `null`, so nothing is masked.
- **Status**: RECORDED — no change required.

## Architecture Compliance

- [x] Follows `arch.md` and the 五行 placement (`shui` owns data
  persistence; module placement mirrors the Phase 2 precedent)
- [x] Uses defined interfaces only (`HDF5DataBackend` public surface;
  no `group._records` reach-through — the `DataGroup.record()` copy
  semantics are respected per the design's implementation note)
- [x] Proper error handling — no swallowed errors anywhere; the
  `finally` blocks append diagnostics instead of raising (R6)
- [x] No code duplication — the example's writer discipline is lifted
  into the seam exactly once; legacy `_disconnect` remains for the
  example's own reader/finally paths

## Simplicity Audit

- [x] **No new third-party dependencies** — `h5py` (existing project
  dependency, decision-§2-mandated preflight) plus stdlib only
  (`dataclasses`, `hashlib`, `os`, `shutil`, `tempfile`, `typing`)
- [x] No over-engineering: five exception classes, three frozen
  containers, two entry points, three hydration constants, three small
  internal helpers — exactly the design's inventory, nothing more
- [x] No premature abstraction: no registry, no plugin hooks, no
  generic backend abstraction beyond the frozen `isinstance` guard
- [x] No backend repairs smuggled in (diff confirms `softlab/shui/data/`
  untouched)
- [x] The bootstrap is a test instrument, not production surface —
  matching the design's simplicity ruling (one consumer, no general
  CLI contract)

## Quality Checks

- [x] No compiler errors — `python -m compileall -q softlab
  tests/run_record_inspect_bootstrap.py examples/thermal_experiment.py`
  clean
- [x] No compiler/import warnings — `python -W error -c "import
  softlab; import softlab.shui.run_storage"` clean; full save→inspect
  round trip under `-W error` clean (3.13.15)
- [x] py39 compatibility — `ast.parse(feature_version=(3, 9))` passes
  on all touched files; `typing.Dict/List/Optional/Tuple` style
  throughout; no 3.10+ syntax
- [x] Docstrings in English, typed signatures, repo style
- [x] Tests: full suite **459/459 OK** reproduced independently
  (`python -m unittest discover -s tests -p 'test_*.py'`, Python
  3.13.15, `.venv`)
- [x] Bootstrap exercised as a fresh process (exit 0/1/2, payload
  schemas, traceback on stderr, stdout pure JSON)

## Reproduction Log (what I ran before claiming anything)

1. Full suite: `.venv/bin/python -m unittest discover -s tests -p
   'test_*.py'` → **Ran 459 tests, OK** (60.8 s).
2. 22-scenario adversarial probe script (scratch, outside the repo)
   against both entry points: taxonomy bases; verbatim frozen messages
   (all 20 templates); R4(a)/R4(b) empty-cleanup semantics; R2
   coincident chaining (`__cause__`, two-entry `primary`); R6 safety
   dominance (mid-inspection byte modification; mid-inspection
   deletion → re-hash failure template); A4.1/A4.2/A4.3-save/A4.8/A4.9
   with artifact pins; legacy absence hydration; signal-vs-thermal key
   sets; save/inspect round trip with sha256 byte preservation —
   **22/22 pass**.
3. A4.3 inspection variant (frozen row): handle-invalidation spy on
   `load_group` → primary delivered, `cleanup_ok=False` with frozen
   `Failed to disconnect:` diagnostic, original sha256 unchanged,
   scratch removed, zero `$TMPDIR` leftovers.
4. w04 scenario reproduction outside the test: `KeyboardInterrupt`
   identity preserved, calls inventory `['connect','save','disconnect',
   'disconnect']`, frozen cleanup tuple `('writer cleanup fault',
   'Backend still connected', 'Backend disconnect left its file handle
   open')`, and the legacy stderr surface (`Writer cleanup: writer
   cleanup fault`) still emitted.
5. Bootstrap CLI as a fresh subprocess: exit 0 with schema-exact
   success payload (including `original_sha256` equality and
   lexicographic `records[*].columns`); exit 1 with schema-exact error
   payload + stderr traceback; exit 2 on missing `--profile`; legacy
   absence payload (`envelope_present: false`, raw `legacy_meta`).
6. `diff` of the bootstrap warning policy against
   `tests/thermal_cli_bootstrap.py` — verbatim, including the corrected
   pandas text.
7. `git diff 0217593` (post-state vs commit) — empty; no stray changes.

Not reproduced: Python 3.9.23 execution (the developer's commit message
reports both-interpreter round trips; the design's §6.2 seam evidence
is both-interpreter). py39 validation here is syntax-level plus the
absence of 3.10+ constructs; CI matrix re-confirmation remains the
both-interpreter gate.

## Approval

- [x] All issues resolved (none blocking; two observations recorded
  with rationale)
- [x] Design faithfully implemented — algorithms, templates, ordering,
  containers, taxonomy, export delta all conformant
- [x] Code meets quality and simplicity standards
- [x] The two flagged M02 legacy-test alignments are legitimate
  consequences of the approved wiring; characterization intent
  preserved; no restoration required
- [x] **Approved for testing by sw-mike** (Phase 3 test implementation
  against the approved plan may proceed)
