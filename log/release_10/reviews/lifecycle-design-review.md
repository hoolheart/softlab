# M02-W03 lifecycle design architecture review

Reviewer: Architect | Date: 2026-10-30 Asia/Shanghai
Verdict: **CHANGES REQUESTED**. Reviewed the [design](../design/lifecycle-design.md)
against the [PRD](../prd.md), M02 plan, W02 accepted boundary and existing
bounded runner and Counter callers.

1. **AR-W03-01 — reuse guard and preflight ordering.** A private claim detects
   prior `run_count` calls but cannot prove a Counter has never been used through
   legacy execution, especially if its record was cleared. Define the actual
   enforceable guarantee and explicitly state the caller responsibility and
   compatibility effect. Check an existing claim before invoking the
   caller-supplied `row_count`, whose side effects are expressly possible;
   retain the rule that failed initial validation does not claim a fresh Counter.
   Tests must isolate these cases without changing legacy reset or execution.
2. **AR-W03-02 — inspection precedence not fully specified.** Separate row and
   boundary observations are required after failure, but the design must also
   define what happens when normal observation itself fails: the first error is
   primary, the other reader is still attempted, a second error is retained,
   and successful values remain truthful. State how a `BaseException` during
   either inspection interacts with a prior ordinary failure or interruption,
   cleanup and re-raising. Test both reader orders and observable values.

The proposed additive UUID, independent diagnostics, owned-resource boundary
and caller-owned reset direction are otherwise consistent with the scoped PRD.
Reinspection is required; no implementation, runtime result or acceptance is
claimed by this review.

## Architect re-review

**APPROVED.** The revised design states that only previous bounded-run claims
are detectable, makes fresh Counter use the caller's responsibility, checks a
claim before the side-effectful reader and leaves failed preflight unclaimed.
It defines independent row and boundary observation in normal and failure
paths, first-error and interruption precedence, and continued cleanup.
AR-W03-01 and AR-W03-02 are closed by the Architect. The new bounded-path reuse
restriction is an explicit lifecycle change; legacy `Process.reset()` and
legacy callers remain unchanged. Tests must verify the stated limits rather
than claim detection of undetectable legacy reuse.

This design approval does not assert runtime correctness, test completion,
product acceptance, candidate CI or release closure.
