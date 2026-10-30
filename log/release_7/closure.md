# M01-W04 and milestone closure

Status: **CLOSED**. Date: 2026-10-30. This is the authoritative closure summary
for M01-W04 and the first milestone. The four weekly product outcomes and M01
exit criteria are accepted in the [milestone matrix](milestone-acceptance.md).
The bounded W04 [product acceptance](acceptance.md), independent [code review](reviews/code-review.md),
[tests](test/results.md), and [principle inspection](reviews/principle-inspection.md)
record the applicable evidence and limitations.

- Final integration candidate and `dev`: `59c75a93d39c9cdf781e972f17cc9e4a7a049f89`;
  [candidate CI](https://github.com/hoolheart/softlab/actions/runs/41023664054)
  passed before integration.
- Promoted `main`: `991f306fd666c3a21a597004f47da5646129f49a`;
  [promoted-state CI](https://github.com/hoolheart/softlab/actions/runs/41023849470)
  passed. The promoted and candidate trees match.
- Local independent regression passed 278 tests under Python 3.13.15; six focused
  adapter checks, compile and import passed. A fresh isolated Python 3.9.6
  environment executed, saved, reloaded and plotted the reference experiment;
  see [fresh-environment evidence](test/fresh-environment.md) and the independent
  repeat in test results. Initial network resolution failed, and an isolated pip
  upgrade was needed for installation. Pandas concatenation FutureWarnings were
  observed on that walkthrough; this is not a universal warning-free claim.
- R4-BASE-SCHED-01, R4-BASE-META-01, R4-BASE-HDF5-01, OBS-004/005/006 and
  DEFECT-2 retain their existing dispositions. The [follow-on backlog](follow-on-backlog.md)
  is proposed, not authorized implementation. No real hardware or simulated VISA
  qualification, package publication, dependency lock, universal adapter contract
  or full local Python 3.9 regression is claimed.

The worktree was clean before this factual closure reconciliation. These
administrative records do not alter the accepted runtime or reopen the release;
final handoff verifies their diff and resulting worktree status.
