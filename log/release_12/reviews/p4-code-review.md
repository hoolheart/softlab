# Code Review Report — M03 Phase 4: Signal run-record persistence wiring

## Review Information

- **Reviewer**: sw-celeste
- **Date**: 2026-11-04
- **Branch**: `codex/m03-p4-acceptance`
- **Commits under review**:
  - `db007bc` — `feat(examples): add signal run-record persistence wiring`
    (`examples/signal_monitor.py`, `tests/test_m03_p1_characterization.py`)
  - `98b8569` — `docs(guide): add run-record guide` (`docs/run-record.md`)
- **Reviewed against**: `log/release_12/design/p4-oracle-design.md`
  (approved freeze, `63ed461`, esp. §5.1/§5.2), `p1-boundary-decision.md`,
  `p2-schema-design.md`, `p3-storage-design.md`,
  `log/release_12/test/characterization-results.md` (C5b), `arch.md`.

## Summary

- **Status**: CHANGES REQUESTED
- **Total Issues**: 1 (plus 1 non-blocking hardening observation)
- **Critical**: 0
- **High**: 0
- **Medium**: 0
- **Low**: 1

The implementation is a faithful, minimal realization of the approved
oracle freeze §5.1 wiring: all six required inputs are correctly provided,
no `softlab/` library code is touched, `acquire()` is untouched, and the
guide is accurate in every command and claim I reproduced. One trivial
dead-code defect (an unused import) must be removed before approval; per
the zero-tolerance policy no defect is waived regardless of size.

## Design Compliance

- [x] Implementation matches the approved freeze §5.1 (all six inputs):
  1. **Acquisition-success gate** — `persist()` checks
     `result.overall_success` before any envelope construction or save;
     verified by reproduction (failed `CountResult` raises `RuntimeError`,
     no artifact created). The explicit
     `completed_rows != requested_rows` second gate is a truthful
     supplement within the freeze's "or the explicitly equivalent
     acquisition/... checks" allowance.
  2. **Identity inputs** — `execution_id = str(result.run_id)`;
     `group_id = str(uuid.uuid4())`, fresh and distinct per run
     (verified distinct across two runs and from each other).
  3. **Group construction and attachment** — `build_run_group(group_id,
     envelope)` then public `DataGroup.add_record(record)` before
     `save_run_group`; matches p3-storage-design §2.1–§2.2 and the seam's
     frozen contract (id taken from the validated envelope; `meta=` never
     routed through the defective `DataGroup(meta=...)` constructor — D1
     respected).
  4. **Envelope inputs** — `build_signal_payload(points=
     result.requested_rows, offsets=SIGNAL_OFFSETS)` and
     `build_envelope(profile=PROFILE_SIGNAL, ...)` with declared-order
     columns `['timestamp', 'signal', 'reference']` taken from the
     acquired record (non-lexicographic declared order verified stored
     verbatim in the envelope).
  5. **Software/source ownership** — signal-owned `_capture_source_identity`
     / `_source_facts` (exact file-bytes sha256 plus best-effort git
     revision/dirty); thermal's private helper is not reused, matching the
     boundary decision (F-B: thermal's legacy keys are not imposed on
     signal). Captured digest reproduced exactly against the file bytes;
     `git_dirty.state == git_revision.state` holds by construction.
  6. **Save and cleanup** — `StorageReport.primary_ok` and `cleanup_ok`
     both asserted; the seam owns the verified disconnect. Storage errors
     are re-raised as owned `RuntimeError`s with the original chained.
- [x] `SavedRun` shape adequate for the §5.2 rerun procedure: it carries
  exactly what step 2 requires to be recorded (`execution_id`, `group_id`,
  artifact `path`, `report`) — nothing more, per the simplicity principle.
- [x] No legacy behavior change: `acquire()` is byte-identical to the
  Phase 1 baseline (diff confirms additions only).
- [x] No `softlab/` library changes in either commit (`git show --stat`
  confirms: `examples/`, `tests/`, `docs/` only) — the Phase 2/3 seams
  sufficed, as the freeze predicted.
- [x] Acquisition-snapshot-only honesty: stored outcome verified as
  `acquisition: completed`, `requested == completed == 3`, and
  `persistence`/`plotting`/`cleanup`/`overall` all
  `not_yet_established` in the inspected envelope.

## Issues Found

### [Low] Issue 1: Unused import `re` in `examples/signal_monitor.py`

- **Location**: `examples/signal_monitor.py`, line 12 (`import re`)
- **Description**: The module imports `re` but never uses it. AST plus
  word-boundary occurrence analysis confirms exactly one occurrence of
  the name in the file — the import line itself. All other imports are
  used.
- **Impact**: Dead code; violates the simplicity principle ("if removal
  breaks nothing, it does not belong") and the zero-tolerance quality
  bar. It produces no runtime warning (the project configures Black/isort
  only, no lint gate), which is precisely why human review must catch it.
- **Recommendation**: Delete line 12. No other change is required; the
  suite, compileall, and the `-W error` import smoke all remain valid.
- **Status**: OPEN

## Flagged Characterization-Test Update — Adjudication

**Verdict: legitimate truth update for an authorized Phase 4 change.
Strictness is preserved; the historical results document remains valid as
dated evidence.**

Reasoning, mirroring the M02 adjudication standard:

1. **Authorized change.** The approved freeze §5.1 itself states plainly
   that the signal wiring "does not exist today" and **must** be added as
   the Phase 4 prerequisite. Updating the inventory test from asserting
   absence to asserting the new truthful state is the designed consequence
   of an approved design, not a test weakened to accommodate unreviewed
   code.
2. **Equivalent strictness.** The replaced test is not a relaxation:
   - Static absence checks survive where still truthful and now target the
     real invariant — the example performs **no direct backend I/O of its
     own** (`import h5py`, `Sqlite`, `sqlite3` all absent). The old
     blanket absence of the substrings `backend`/`HDF5`/`manifest`/`save`
     is *necessarily* retired: the freeze-mandated wiring legitimately
     contains `save_run_group` and HDF5 wording in docstrings/messages.
     Keeping those checks would assert a falsehood, not a strictness.
   - Positive seam-usage assertions are added (`persist`, `build_run_group`,
     `build_envelope`, `build_signal_payload`, `save_run_group`,
     `add_record` all present), pinning the wiring to the shared seams.
   - The defined-name inventory remains **exact and exhaustive** — now
     enumerating all six module-defined callables (`SavedRun`,
     `_capture_source_identity`, `_git`, `_source_facts`, `acquire`,
     `persist`). Any future module-level growth fails this test.
3. **Strictness added, not just preserved.** The new
   `test_persist_gate_refuses_unsuccessful_acquisition` is a behavioral
   regression test of freeze §5.1 input 1: a failed acquisition raises
   before the storage seam and no artifact is created. I reproduced this
   independently (gate raises `RuntimeError`; `never.h5` absent).
4. **Historical evidence intact.** `characterization-results.md` C5b is
   phrased as dated fact ("in-memory only **today**"; "Ownership of any
   future stored-record construction is a Phase 1 design input") and the
   freeze §8.2 records the same gap at freeze time. Neither was edited;
   the Phase 1 results remain valid evidence of the Phase 1 state. No
   silent rewrite of history occurred.

**Non-blocking hardening observation (not a required fix):** the updated
absence tuple could additionally include `HDF5DataBackend`/`DataBackend`
so a hypothetical direct-backend path inside `persist()` would also fail
the static check, fully mirroring the old test's lowercase `backend`
absence intent. The exact defined-name inventory already bounds
module-level growth, so this is defense-in-depth only; sw-tom may apply
it together with the Issue 1 fix at negligible cost.

## Guide Accuracy Audit (`docs/run-record.md`, W7 pre-check)

Every command and claim spot-checked by execution or source inspection:

- [x] §2.1 thermal CLI command runs **verbatim**
  (`MPLBACKEND=Agg python examples/thermal_experiment.py --output-dir ...
  --adapter coordinated`); artifact is `<output-dir>/thermal.h5` (plus
  `thermal.png`) as claimed; refusal of non-completed acquisition
  confirmed in source (`thermal_experiment.py:319`).
- [x] §2.2 signal recipe runs **verbatim** (acquire → persist → prints
  `execution_id`, `group_id`, `path`); all six wiring-contract steps
  match the implementation exactly.
- [x] §3 bootstrap CLI commands work verbatim for **both** profiles
  (`--profile signal` and `--profile thermal`, exit 0, single JSON
  payload on stdout); the exactly-one-group rule without `--group-id`
  and the fresh-copy/never-load-original discipline match
  `inspect_run_group`'s documented four-step discipline and the F-A/F-B
  dispositions in the boundary decision.
- [x] §4 envelope contents match the inspected artifact field-for-field
  (profile, identities, ordered declaration with truthful units
  `s`/`V`/`V`, software versions, source availability facts with bounded
  reasons, acquisition-only outcome, signal payload literals including
  `run_count_owned` and `caller_derived_not_stored`).
- [x] §5 identity semantics verified on disk: HDF5 `id` attribute and
  group name equal `group_id`; `execution_id` appears only as JSON text
  in the envelope, in no stored UUID slot; the two are distinct per run.
- [x] §6 debt/limitation claims (D1, D2, F-A…F-D, no atomicity, no
  failed-run persistence, no later-outcome updates, lexicographic
  non-semantic on-disk column order) all match the boundary decision
  dispositions and observed on-disk layout (`['reference', 'signal',
  'timestamp']` on disk vs declared `['timestamp', 'signal',
  'reference']` in the envelope).
- [x] No overclaiming found: scope honesty (HDF5-only, snapshot-only,
  unsupported paths) is stated as limitation, not capability. Nothing
  false for W7 to trip over.

Minor nuance (informational, no change required): §3's "exits 0 on
success" is the supported-artifact contract; the bootstrap also exits 0
for truthful legacy-absence and cleanup-only-failure reports per its own
docstring. Within the guide's scope this is accurate.

## Architecture Compliance

- [x] Follows `arch.md` and the five-element module responsibilities —
  the example stays a client; all storage behavior routes through
  `shui` seams.
- [x] Uses defined interfaces with exact signatures (`build_run_group`,
  `build_envelope`, `build_signal_payload`, `save_run_group` all invoked
  per their frozen contracts).
- [x] Proper error handling: owned `TypeError`/`ValueError`/`RuntimeError`
  gates with informative messages; storage errors chained (`from exc`);
  no swallowed errors; `capture_failed`/`source_mismatch` bounded reasons
  match the closed codec vocabulary (`UNAVAILABLE_REASONS`).
- [x] No code duplication: source capture is signal-owned per the
  boundary decision, not a copy of thermal's helper (different reference
  file, bounded reason mapping, `git_dirty` state coupling per schema).

## Simplicity Audit

- [x] No new third-party dependencies (stdlib only: `dataclasses`,
  `hashlib`, `pathlib`, `platform`, `subprocess`, `uuid`).
- [x] No over-engineering: `SavedRun` is a frozen four-field dataclass;
  capture helpers are two small functions plus one bounded `_git`
  wrapper (5 s timeout, `check=True`, cwd beside the reference file);
  no abstractions beyond the freeze's requirements.
- [ ] Issue 1: one unused import (`re`) — the single simplicity violation
  found. Required fix above.
- [x] Data structures minimal: plain dicts mapped onto the closed schema;
  no new types beyond `SavedRun`.

## Quality Checks

- [x] No compiler errors — `python -m compileall -q softlab` (and the
  example/test files compile) clean.
- [x] No runtime warnings — import smoke under `-W error` passes
  (`import softlab` → `0.3.0`).
- [x] Full suite green — `python -m unittest discover -s tests -p
  'test_*.py'`: **Ran 508 tests, OK** (Python 3.13.15, `.venv`).
- [x] End-to-end smoke reproduced personally (see below).
- [x] py39 compatibility: no post-3.9 syntax (no walrus/match/PEP 604
  unions); stdlib usage all 3.9-safe.
- [ ] Lint cleanliness: one dead import (Issue 1).

## Reproduction Evidence (performed by reviewer, 2026-11-04)

Environment: project `.venv`, Python 3.13.15, softlab 0.3.0, from the
repository root at `98b8569`.

1. `python -m unittest discover -s tests -p 'test_*.py'` → `Ran 508
   tests … OK`.
2. `python -m compileall -q softlab` → clean; `python -W error -c
   "import softlab; print(softlab.__version__)"` → `0.3.0`.
3. Guide §2.2 recipe verbatim: `acquire(points=3)` →
   `persist(..., '<tmp>/signal.h5')` → differences `[3, 3, 3]`,
   `primary_ok/cleanup_ok` both `True`; second run produced distinct
   `execution_id` and `group_id`.
4. Bootstrap inspection (separate process) on the produced artifact →
   exit 0; envelope profile `signal`, declared columns `['timestamp',
   'signal', 'reference']`, outcome `completed` with all later stages
   `not_yet_established`, payload literals as frozen, source facts all
   `available`, software `{softlab 0.3.0, python 3.13.15}`.
5. Disposable-copy h5py read: stored samples `signal [[6],[7],[8]]`,
   `reference [[3],[4],[5]]` — the §2 oracle values round-tripped through
   `save_run_group`; on-disk column enumeration lexicographic; stored
   `source.digest.value` equals `sha256(examples/signal_monitor.py)`
   exactly (`b3f1db71…`); `source.git_revision.value` equals HEAD
   (`98b8569`), `git_dirty` `False` on a clean tree.
6. Gate probes: failed-acquisition `CountResult` → `RuntimeError`, no
   artifact; non-`CountResult`/non-`DataRecord` → `TypeError`; wrong
   record name → `ValueError`.
7. Guide §2.1/§3 commands verbatim: thermal CLI produced
   `<output-dir>/thermal.h5`; bootstrap `--profile thermal` inspection
   exit 0, payload `profile: thermal`, manifest v1. (Temp artifacts
   cleaned up.)
8. Unused-import analysis (AST + word-boundary scan) of
   `examples/signal_monitor.py`: `re` occurs once (its own import line);
   all other imports used.

## Approval

- [ ] All issues resolved — **Issue 1 (unused `import re`) OPEN**
- [x] Design faithfully implemented (freeze §5.1 all six inputs)
- [x] Flagged characterization-test update adjudicated: legitimate truth
  update, strictness preserved, historical evidence intact
- [x] Guide accurate; nothing false for W7
- [ ] Approved for testing by sw-mike — **blocked on the Issue 1 fix**

**Required before re-review:** remove `import re` from
`examples/signal_monitor.py` (optionally apply the hardening
observation to the inventory test's absence tuple). Re-review scope will
be limited to the diff of that fix; all other evidence above stands.
