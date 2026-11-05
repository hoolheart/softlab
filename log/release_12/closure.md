# M03 release 12 closure

Status: **CLOSED — M03 (Release 12)**
Date: 2026-11-05 Asia/Shanghai. This is the authoritative closure summary for
release 12 and M03. The coordinator verifies final worktree cleanliness after
committing and pushing this factual closure record.

A maintainer can reopen a supported synthetic result (thermal coordinated/v1,
thermal direct/v2, signal monitor) in a clean session, understand its bounded
procedure/setup/model declarations, units, observed software/environment
identity, acquisition-outcome stage and sample relations, and compare a
deterministic rerun with independent expected values — within the recorded
scope: HDF5-only storage, acquisition-snapshot-only outcome, no environment
lock, no physical validation, no atomicity or failed-run persistence.

| Final fact | Evidence |
| --- | --- |
| Phases 1–4 and acceptance | Phase gates: P1 characterization + boundary decision (PO concurrence, independent review ACCEPTED); P2 schema/codec (design APPROVED after 2 rounds, code review zero defects); P3 storage/inspection (design APPROVED after 3 rounds, code review zero blocking findings); P4 oracle freeze + walkthroughs W1–W7 (code review APPROVED). Final product acceptance **PASS — ACCEPTED** at `9913905` (`acceptance.md`; issues 1–4 all closed). Principle inspections 1–4 all PASS (`principle_compliance_report.md`). |
| Independent testing | 521-test suite green on Python 3.13.15 (project venv) and Python 3.9.23 (scratch env); fresh-environment full discovery 521 tests OK in an isolated venv with verbatim command transcript (six A/B driver + twelve bootstrap inspection commands); W1–W3 original-vs-rerun comparisons against independently recomputed frozen oracles (`abs_tol=1e-10` Euler temperatures); compileall/import checks clean; zero undispositioned warnings. Results: `test/characterization-results.md`, `test/schema-test-results.md`, `test/storage-test-results.md`, `test/walkthrough-results.md`. |
| Integrated candidate and CI | Final candidate `622dc5edd8…` (acceptance verdict commit); [candidate CI](https://github.com/hoolheart/softlab/actions/runs/42051004463) passed Python 3.9 and 3.13 before integration. `dev` merge `70842f7`. |
| Promoted revision and verification | `main` promotion `5a0e41634f8b8c291d88cae81bbdfddc59feb195` matches `origin/main` and contains the candidate. [Promoted-state CI](https://github.com/hoolheart/softlab/actions/runs/42052175557) passed both Python jobs. |
| Deliverables | `softlab/shui/run_record.py` (bounded `softlab.run-record` v1 codec), `run_group.py` (`build_run_group` D1-safe seam), `run_storage.py` (`save_run_group`/`inspect_run_group`), package exports; thermal example dual-write + additive hydration; signal example owned persistence (`persist()`); `tests/run_record_inspect_bootstrap.py` fresh-process inspection CLI; maintainer guide `docs/run-record.md`; `arch.md` M03 record. |
| Remaining debt and limits | R4-BASE-SCHED-01/F-01/F-02 avoided by M02 seam; R4-BASE-META-01/D1 (constructor metadata), R4-BASE-HDF5-01/F-A (destructive plain load), D2 (SQLite empty meta), F-B (custom-key loss), F-C/F-D (`io.reload_group`/`load_groups`), OBS-004/005/006, DEFECT-2 retain tracked dispositions — none silently repaired. `set_meta(None)` mismatch tracked separately, avoided by design. SQLite record persistence unsupported; no atomicity, rollback, later-outcome updates, or failed-run persistence. Same-ID failed save can lose prior group content (documented in guide/design). No Python 3.9 host interpreter (CI + scratch env covered); pandas-2.x `FutureWarning` baseline on 3.9 recorded; GitHub Actions Node.js 20 deprecation annotation is a baseline runner warning. |
| Worktree and scope | Clean-worktree verification follows this closure commit and push. Only release-12 scope delivered; no backend rewrite, no new dependencies, no real-hardware qualification. |

The [release checklist](../templates/release-checklist.md) governed final
verification. One release, four serial gated phases, one acceptance, one
promotion, one closure — as authorized at kickoff.
