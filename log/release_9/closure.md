# M02-W02 release closure

Status: **CLOSED**  
Date: 2026-10-30 Asia/Shanghai. This is the authoritative closure summary for
release 9. The coordinator verifies final worktree cleanliness after committing and pushing
this factual closure record; this administrative update does not reopen the release.

The release delivers an additive private-loop serial scheduler and bounded count
runner, a distinct synthetic signal-monitor client and reference timing-policy
fixtures. Acquisition, records and model policy remain caller-owned. The
shipped thermal CLI migration remains W04 scope.

| Final fact | Evidence |
| --- | --- |
| Acceptance and independent gates | [Product acceptance](acceptance.md) PASS for the bounded outcome; [design review](reviews/design-review.md) APPROVED; [code review](reviews/code-review.md) APPROVED with CR-W02-01/02 closed; [test results](test/execution-results.md) report 328 local tests, compile and import passing; [principle inspection](principle_compliance_report.md) PASS for applicable local gates. |
| Integrated candidate and CI | Candidate `1757f9f7fc66ef0fc689a25e9f3969407d54388c`; [candidate CI](https://github.com/hoolheart/softlab/actions/runs/41077062869) passed Python 3.9 and 3.13 before integration. `dev` merge `14d25d8`. |
| Promoted revision and verification | `main` promotion `b21f521` matches `origin/main`; the candidate is an ancestor. The [promoted-state CI](https://github.com/hoolheart/softlab/actions/runs/41077341440) passed both Python jobs. The remaining `dev`/`main` differences are prior release 8 closure records. |
| Remaining debt and limits | R4-BASE-SCHED-01, generic control-point timeout F-02, R4-BASE-META-01, R4-BASE-HDF5-01, OBS-004/005/006 and DEFECT-2 retain their tracked dispositions. GitHub Actions reported a Node 20 deprecation annotation; it remains a baseline warning. Local initial Matplotlib/fontconfig cache warnings and the successful temporary-cache rerun are recorded in test evidence. No hardware qualification, physical termination, forced-process cleanup, rollback, automatic retry or durable recovery is claimed. |
| Worktree and scope | Clean on `main` before this record; final worktree cleanliness is checked by the coordinator after this record is committed and pushed. |

W03 retains lifecycle refinement and W04 retains thermal CLI migration and
milestone acceptance. The [release checklist](../templates/release-checklist.md)
governs the final verification and closure.
