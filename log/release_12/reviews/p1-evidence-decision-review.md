# Release 12 (M03 Phase 1) — Independent Evidence-Decision Review

## Review Information
- **Reviewer**: sw-celeste (Software Designer / independent reviewer)
- **Date**: 2026-11-01 Asia/Shanghai
- **Branch**: `codex/m03-p1-characterize`
- **Reviewed artifacts**:
  - `log/release_12/test/characterization-results.md` + `tests/test_m03_p1_characterization.py`
  - `log/release_12/reviews/p1-product-assessment.md` (`18f9c1e`)
  - `log/release_12/reviews/p1-architecture-assessment.md` (`d2180a0`)
  - `log/release_12/design/p1-boundary-decision.md` (`3e12df6` + concurrence `8b76701`)
  - `log/release_12/prd.md` (M03-AC01) and `plan/milestone-03.md`
- **Gate**: M03-AC01 — independent review must accept the source/round-trip
  evidence and the supported (or explicitly deferred) design before Phase 2.

## Summary

- **Verdict**: **ACCEPTED**
- **Total issues**: 1
- **Critical**: 0 — **High**: 0 — **Medium**: 0 — **Low**: 1 (resolved in this commit)

The executed evidence supports every claim the joint boundary decision rests
on. The decision stays inside the PRD boundary (acquisition-snapshot-only, no
later-outcome updates, no failed-run persistence, HDF5-only, thermal v1/v2
compatibility preserved) and the defect/debt dispositions are honest. The
Phase 2 binding constraints are complete enough for detailed design without
guessing.

## What I independently reproduced

All commands run from the repository root in the project venv
(`conda activate "$PWD/.venv"`, Python 3.13.15, softlab 0.3.0).

1. **Full suite rerun**: `python -m unittest tests.test_m03_p1_characterization -v`
   → **Ran 47 tests, OK**. Full regression
   `python -m unittest discover -s tests -p 'test_*.py'` → **Ran 405 tests,
   OK** (matches the results document exactly).
   `python -m compileall -q softlab` exit 0; import smoke prints `0.3.0`.
2. **HDF5 disposable-copy byte preservation (C3/C6)**: wrote a synthetic group
   with `HDF5DataBackend`, recorded sha256, performed all reads on a
   `shutil.copy2` copy with `h5py.File(copy, 'r')`; original sha256 unchanged.
3. **Text-attr round-trip (C2.3)**: non-ASCII text (`héllo`) and pre-encoded
   JSON text survive verbatim; every on-disk attr is `str`; disk `id` attr ==
   group directory name == `str(DataGroup.id)` (C3 identity relation).
4. **F-A end to end**: first plain `load_group` returns the group with
   `meta == {}` and the reserved `id` attr is removed from the file on close;
   a second plain load of the same file returns `None` with
   `Failed to load group: AttributeError("'UUID' object has no attribute 'replace'")`
   in `backend.errors` — verbatim match with the results document.
5. **D1**: `DataGroup('probe', meta={'a': 1})` → `group.meta == {}`, caller
   dict visibly unchanged for all-str keys (in-place write of the same key, as
   the results document states). With `{2: 'x'}` construction raises
   `RuntimeError: dictionary changed size during iteration` and the caller's
   dict is left holding **both** `{2: 'x', '2': 'x'}` — reproduced verbatim.
6. **C2.2**: `set_meta('k', None)` keeps the key with value `None`; on an
   absent key it **creates** the key — matching the docstring mismatch claim.
7. **D2**: plain `Sqlite3DataBackend.load_group` reconstructs the group with
   `meta == {}` (not `{'sample', 'run_id'}`), confirming R4-BASE-META-01 drops
   the reconstruction dict.
8. **F-C**: after `io.save_group(grp, 'hdf5', ...)` writes
   `group.backend = {'type': 'hdf5', ...}`, `io.reload_group(grp)` returns
   `None`; the identical underlying load with `str(group.id)` succeeds — id
   type is the sole cause.
9. **F-D**: `io.load_groups('sqlite3', ...)` returns `[]` on a non-empty
   database.
10. **C1.5 execution vs group UUID**: wrapped the example's module-global
    `run_count`; captured `CountResult.run_id` is a `uuid.UUID`, distinct from
    `result.group.id`, and the example-local `RunResult` has no `run_id`
    attribute (fields enumerated via `dataclasses.fields`).
11. **C1.4 outcome snapshot from disk bytes**: `inspect_stored` on a
    disposable copy yields
    `snapshot == 'completed_acquisition_before_persistence'`,
    `acquisition == 'completed'`, and `persistence`/`plotting`/`cleanup`/
    `overall` all `'not_yet_established'` — reproduced verbatim.
12. **Warnings claim**: the full 405-test run emits no Python warnings beyond
    backend "Connected to ..." stdout lines; no warning suppression observed.

Claims I did **not** re-derive independently but verified by reading the
executed suite and its passing tests: the C1.2 strict-validation diagnostic
matrix, F-B (example hydration retains only the three `_META_KEYS` + manifest;
`test_example_hydration_path_restores_only_legacy_keys` passes), the C5a Euler
oracle (`test_stored_samples_match_independent_euler_oracle` passes), and the
C5b signal-monitor inventory (all three inventory tests pass). These rest on
asserting tests inside the rerun suite, not on unexecuted prose.

## Findings

### [Low] Finding 1: Stale "concurrence pending" status lines in the decision document
- **Location**: `log/release_12/design/p1-boundary-decision.md`, header status
  block and section 5 concurrence table/record.
- **Description**: Commit `8b76701` appended the Product Owner countersignature
  ("Product Owner concurrence" section) but left the header status as
  "PRODUCT OWNER CONCURRENCE PENDING; PHASE 2 SCHEMA IMPLEMENTATION BLOCKED"
  and section 5 as "PENDING EXPLICIT CONCURRENCE" / "**PENDING.**" — the
  document contradicted itself.
- **Impact**: Administrative only; no semantic change to the decision, the
  boundary, or the binding constraints. A stale PENDING marker could have been
  misread as an open gate.
- **Recommendation**: Update the stale lines to reflect the recorded
  countersignature.
- **Status**: **RESOLVED** in this commit (factual status correction; content of
  the decision and the concurrence record untouched).

## Review focus answers

1. **Evidence supports the claims.** Every load-bearing claim in section 3 of
   the decision (thermal manifest strictness, acquisition-only snapshot,
   execution/group UUID distinctness, HDF5 text-attr behavior, safe
   disposable-copy inspection, SQLite/io negative evidence, two-client
   inventories and oracles, backend ownership map) is backed either by my
   independent reproduction above or by asserting tests in the rerun 47-test
   suite. No claim rests on unexecuted prose.
2. **Within the PRD boundary.** The decision supports only the
   `completed_acquisition_before_persistence` snapshot with later stages
   `not_yet_established`; explicitly excludes later-outcome updates, failed/
   cancelled/partial-run persistence, SQLite, generic `io.py` helpers, atomic
   publication, recovery/resume, migration, and a general provenance framework;
   preserves thermal v1/v2 semantics and existing APIs. This matches PRD
   M03-AC01 and the Limits/debt section. No scope creep found: the decision
   document repeatedly and explicitly disclaims being a Phase 2 design
   approval or release acceptance, and the signal-monitor client is honestly
   described as not persisting today.
3. **Debt dispositions are honest.** D1 and D2 are recorded as observed and
   linked to R4-BASE-META-01; F-A to R4-BASE-HDF5-01; F-B, F-C, F-D are new
   findings recorded as-is with explicit non-blocking conditions tied to the
   supported-path exclusions. Nothing is waived or silently repaired; the
   `set_meta(None)` mismatch is called out as separately tracked per the PRD.
   Each disposition states exactly which later-phase condition would make it
   blocking.
4. **Phase 2 binding constraints are sufficient.** Section 6 of the decision
   enumerates: implementation surface (codec + two closed payload validators +
   bounded construction seam), bounded value allowlist, no implicit
   stringification, the negative-case list, identity/row-count/unit relations,
   required/optional/absent/null/unavailable/invalid-present semantics, v1/v2
   preservation, hydration-seam boundary, and the no-new-dependency rule with
   a stop condition if the common contract cannot stay small. Literal names and
   limits are deliberately deferred to the Phase 2 detailed design, which is
   the correct division of labor; a detailed design can proceed without
   guessing the semantics.

## Approval

- [x] Source/round-trip evidence accepted (independently reproduced sample)
- [x] Supported design (option a, additive HDF5 JSON-text envelope) accepted
- [x] Defect/debt dispositions verified honest and tracked
- [x] Phase 2 binding constraints verified complete
- [x] **Phase 1 gate M03-AC01: PASS — Phase 2 detailed design may proceed**
