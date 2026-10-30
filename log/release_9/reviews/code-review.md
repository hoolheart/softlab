# Independent code review — M02-W02

Reviewer: Designer | Date: 2026-10-30 Asia/Shanghai  
Verdict: **CHANGES REQUESTED**. Reviewed the new bounded runner, serial
scheduler, public exports, synthetic client, and tests against the
[design](../design/bounded-count-design.md), W01 decision and current callers.

1. **CR-W02-01 — action TimeoutError is reported as a deadline timeout.**
   `SerialScheduler.wait_point` catches `asyncio.TimeoutError` regardless of
   its origin, including when `timeout=None`. An action that raises this error
   is an action failure and must return `False` while retaining the original
   exception; `None` is reserved for the scheduler's bounded deadline. This
   also affects runner failure truthfulness. Distinguish the two cases and
   assert both action-raised and actual bounded-timeout outcomes.
2. **CR-W02-02 — interruption can suppress remaining cleanup and mask the
   primary interruption.** `close()` catches only `Exception` around stop,
   drain and close. If any step raises `BaseException`, later cleanup is not
   attempted; `run_count` likewise only catches `Exception` from `close`, so
   such an interruption can replace a previously captured primary interruption.
   Preserve the primary while independently attempting later cleanup and
   reporting the cleanup failure. Add a fault-injection regression establishing
   cause identity and remaining cleanup attempts.

The review does not claim test execution, product acceptance or release closure.
Reviewer reinspection is required after fixes.

## Reviewer reinspection

The action dispatch now distinguishes an exception raised by an action from
an expired wait, retaining the action exception. Cleanup independently attempts
stop, drain and close even on interruption; the first cleanup interruption is
re-raised unless an earlier acquisition interruption takes precedence. The
runner attaches terminal evidence and preserves the earlier interruption. The
reviewer closes **CR-W02-01** and **CR-W02-02** based on the corrected code;
targeted regression execution and independent tester-owned results remain a
separate mandatory gate. No runtime pass is claimed by this reinspection.

Verdict: **APPROVED**, subject to the pending independent test gate and review
of any subsequent behavior changes.
