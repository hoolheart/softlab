# M02-W02 test plan developer review

Owner: Developer | Status: APPROVED | 2026-10-30 Asia/Shanghai

Reviewed [execution plan](../test/execution-plan.md) against the
[proposed design](../design/bounded-count-design.md), the W01 decision,
`softlab.huo.process` and the thermal executor. The plan's ordered traces,
serial scheduler rejection and timeout tests, stop boundaries, incomplete row
checks, and owned cleanup fault injection are implementable with synthetic
callbacks and a test-local instrumented scheduler/loop. Existing legacy tests
should remain unchanged. Real coroutine timeout is testable; a synchronous
blocking body cannot be preempted by `asyncio.wait_for`.

## Required resolution

- **TP-01 — caller setup failure boundary.** E08 requires control-write failure
  to appear as terminal primary evidence, but the proposed design places setup
  and control writes with the caller before runner invocation. As written, the
  runner cannot observe such a failure. Clarify whether the caller-owned setup is
  passed as an optional callback executed inside the runner's protected lifecycle,
  or whether a reference integration fixture owns and verifies that failure and
  its cleanup separately. Do not claim the shared runner handles it otherwise.
- **TP-02 — preflight reader invocation.** Initial row-count validation requires
  calling the caller's reader, so a blanket assertion that all invalid inputs
  are rejected before *any* callback cannot hold. Define validation order and
  require no acquisition/control/action side effects; explicitly instrument the
  inspection callback and distinguish it from acquisition callbacks. A reader
  that itself performs side effects remains caller responsibility.
- **TP-03 — concrete interruption and fault injection.** The design must define
  a Python 3.9-compatible way to retain cleanup diagnostics with the same
  reraised `BaseException`, and an injectable or observable ownership seam for
  scheduling and cleanup failures. Tests should use narrowly scoped patching or
  test doubles, without making private implementation fields a public promise.
  Define public result/stop types and methods before converting this plan to
  executable tests.

Architect review of the design is pending; the plan is ready for implementation
only after the above items are resolved by the respective owners. This review
is design inspection, not execution or test evidence.

## Developer re-review and issue closure

The updated test plan resolves TP-01 by separately testing a caller setup
failure before runner construction and an action-contained write failure;
TP-02 by identifying the preflight row-count inspection and its ordering; and
TP-03 by specifying Python 3.9-compatible exception attachment/fallback and
scoped test injection without exposing internals as public API. The Developer
closes TP-01, TP-02 and TP-03 after inspecting the revised test plan and design.
The plan is implementable subject to independent architect approval of the
design. This is review evidence, not test execution.
