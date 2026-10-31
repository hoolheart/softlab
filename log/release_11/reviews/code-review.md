# M02-W04 independent code review

Reviewer: Designer | Date: 2026-10-31 Asia/Shanghai
Verdict: **CHANGES REQUESTED**. Reviewed the thermal migration and guidance
against the [approved design](../design/thermal-migration.md), PRD and W03
bounded runner, including stop, result, lifecycle and failure mapping.

**CR-W04-01 — later interruption replaces the acquisition primary cause.**
When `run_count` raises an interruption after an earlier ordinary acquisition
failure, its attached `CountResult` retains the original failure as `cause`.
`_acquire` correctly maps that result, but `execute` then unconditionally sets
`result.cause` to the interruption. This loses the primary failure evidence,
contrary to the W03 contract and W04 design. Preserve the mapped primary cause
while re-raising the identical interruption after cleanup; independently test
an ordinary primary followed by interruption, including partial result and
cleanup diagnostics. Retain the fallback behavior where no attached result is
available without inventing evidence.

Test adaptations and full regression remain in progress. This review does not
claim execution success, acceptance or release closure. Reviewer reinspection is
required after the correction.

## Reviewer reinspection

The revised migration retains the mapped acquisition primary while re-raising
the first interruption, guards diagnostic output, and independently attempts
boundary/row inspection and writer cleanup without allowing later failures to
mask that interruption. It preserves the last observed runner boundary when
available. The reviewer closes **CR-W04-01** after code reinspection; the
independent test owner must verify these fault paths and remaining regression
and fixture adaptations. No runtime or acceptance pass is claimed here.

Verdict: **APPROVED**, subject to the pending independent test gate and review
of any subsequent behavior changes.
