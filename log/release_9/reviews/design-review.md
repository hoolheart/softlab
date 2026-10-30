# Architect design review — M02-W02

Reviewer: Architect | Date: 2026-10-30 Asia/Shanghai  
Verdict: **CHANGES REQUESTED**. Design: [bounded count execution](../design/bounded-count-design.md).

The proposed separation of private serial dispatch, caller-owned acquisition
policy and owned cleanup is consistent with the accepted W01 boundary and W02
plan. Existing `Scheduler`, `Process` and `SimpleProcess` sources and the thermal
prototype were inspected. The design is not yet implementation-ready:

1. **AR-W02-01 — public contract unresolved.** The design defers names,
   signatures, export locations, stop-object protocol and interruption
   diagnostics to implementation. Specify the API and types, request and
   acknowledgment operations, result/diagnostic representation, cleanup
   ownership and exception/interrupt behavior now so Developer and Tester can
   verify the same contract. State which unsupported resource parameters are
   absent or rejected and how rejection occurs.
2. **AR-W02-02 — supported process and preflight boundary unclear.** `Process`
   permits composite implementations that can commit multiple or chained
   actions, while the scheduler supports one unchained action. Define the
   supported process contract and rejection/progress behavior, including how a
   pending or chained process is detected. Reading caller-provided row and
   boundary callbacks is executable user code: distinguish validating callable
   shape from invoking readers, document their expected side effects and failure
   behavior, and avoid promising side-effect-free validation that cannot be
   guaranteed for arbitrary callbacks.
3. **AR-W02-03 — timeout limitations.** `asyncio.wait_for` cannot interrupt a
   synchronous blocking action running on the event-loop thread. Specify what
   the three-valued timeout means for awaitable/cooperative actions and that
   synchronous blocking work can exceed the timeout; provide tests that do not
   imply preemption or physical termination. Define repeat-wait behavior after
   timeout and how owned pending work is handled.

These findings do not require widening scope or changing legacy behavior.
Designer should revise and request re-review; acceptance, code review, testing
and release gates remain separate and pending.

## Architect re-review

Verdict: **APPROVED** for the bounded W02 design, subject to the stated
acceptance and independent verification gates. The revised design names module
exports and signatures, defines the stop/result and cleanup contract, limits
input to the existing Counter, distinguishes reader execution and caller setup,
and states Python 3.9-compatible interruption diagnostics. It explicitly
rejects chained actions before execution and describes non-preemptible
synchronous bodies, cached timeout and standalone scheduler cleanup. Secondary
inspection diagnostics affect overall success. These revisions address
AR-W02-01, AR-W02-02 and AR-W02-03; the Architect closes all three findings.

This approval does not assert that the implementation or tests exist, that any
runtime behavior has passed, or that product acceptance and release closure
have occurred. Changes discovered during implementation require review in
proportion to their effect on this boundary.
