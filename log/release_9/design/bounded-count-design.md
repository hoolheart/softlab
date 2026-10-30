# M02-W02 detailed design: bounded count execution

Owner: Software Designer | Status: REVISED — architect re-review required

Inputs: [W01 decision](../../release_8/design/evidence-decision.md),
[M02 plan](../../../plan/milestone-02.md), `examples/thermal_experiment.py`
(`_CountScheduler`, `_acquire`, `execute`), `softlab.huo.process` and
`softlab.huo.scheduler`. This design is additive; W01's accepted constraints
and the W02 PRD/test plan govern any discrepancies.

## Boundary and responsibility

Implement `softlab.huo.scheduler.serial.SerialScheduler`, exported from
`softlab.huo.scheduler`, and `softlab.huo.process.bounded` with public exports
`StopRequest`, `CountResult` and `run_count` from `softlab.huo.process`.
Do not import the example in library code. The synchronous entry point is:

```python
run_count(process: Counter, total: int, row_count: Callable[[], int],
          boundary: Callable[[], Any], *, stop: Optional[StopRequest] = None,
          after_row: Optional[Callable[[CountResult], None]] = None) -> CountResult
```

`Counter` is the existing concrete count process returned by `count()`, not an
arbitrary `Process`; its single serial action per cycle and complete-row append
are the supported contract. The caller binds the record and ensures the reader
reports that very record. The runner creates and owns a new serial scheduler
and its loop; there is no borrowed scheduler/loop argument. `StopRequest` has
boolean `requested` and `acknowledged` fields, initially false, and `request()`
sets only `requested`. The caller owns the request object and may request from
`after_row`; this API does not promise thread-safe cross-thread signalling.
`CountResult` has `acquisition`, `requested_rows`, `completed_rows`, `boundary`,
`stop`, `cause`, `cleanup_errors`, `work_drained`, `loop_closed`, `terminal`,
and an `overall_success` property requiring completed acquisition and clean,
drained, closed resources. `cleanup_errors` is an ordered list of diagnostic
strings. The callback receives the mutable live result; it may request stop but
must not mutate other state. Snapshot values are supplied by the caller.

The caller constructs and configures the count process and its record, performs
setup and control writes, owns model advance and read policies, defines the
meaning of the boundary, and owns persistence, plotting and model cleanup. The
existing count's before-get hook advances; its getters run in their existing
order and its record append is the commit of a complete row. The generic runner
must never add a row or perform an additional measurement. Coordinated versus
direct timing and any extra pure observation stay in their adapters. A second
synthetic signal-monitor client must use the same runner, with its distinct
paired consumptive reads, and a bounded test fixture must exercise both thermal
policies. The shipped CLI stays unchanged until W04.

## Execution and result contract

Preflight first validates the process type, total (nonnegative integer excluding
bool), callable readers and callback, stop type, pending status, and absence of
a running loop. It then invokes `row_count` once and requires the exact integer
zero, excluding bool. These checks occur before creating owned resources or
calling acquisition/control actions. Reader invocation is itself caller code and
may have side effects; a reader exception propagates before resource creation.
The caller supplies a fresh, nonpending count process and record. Invalid input
raises before acquiring resources. The process is reset once before the loop, retaining legacy reset
behavior; the runner does not reset a model or record. No automatic retry.

Result fields: acquisition (`completed`, `cancelled`, `failed`), requested total,
completed row count, last successfully observed boundary (arbitrary caller
value), stop request and acknowledgment, primary exception or `None`, ordered
cleanup diagnostics, and factual work-drained and loop-closed flags. Retain the
original exception object, not just a formatted message. An acquisition success
with a cleanup failure is still an unsuccessful overall operation; do not
relabeled it as an acquisition failure. A snapshot is not a durable record or a
claim of physical termination.

After scheduler creation/start and process reset, repeatedly:

1. Read row count and boundary. If row count equals the requested total, mark
   completed before checking the stop request; therefore late stop does not
   retroactively cancel completion.
2. Otherwise, if requested, acknowledge and mark cancelled; start no new work.
3. Commit one process action and join it. A rejected commit or non-true join is
   failure, preserving the scheduler's original exception where available.
4. Re-read row count and boundary, including on failure: if advance succeeded
   but a getter failed, the boundary may have advanced without a committed row.
   Require exactly one additional complete row on successful action; a progress
   violation fails rather than spinning. Never fabricate or roll back rows.
5. After a successful row call `after_row`, allowing it to request stop. A stop
   observed at the next iteration is acknowledged only if the target is not
   already complete. Callback exceptions are primary failures.

Capture errors from count/boundary inspection without overwriting a prior primary
failure; report secondary inspection failures in diagnostics. On ordinary
operational exceptions return terminal evidence after cleanup. If interrupted by
`BaseException`, perform all cleanup and re-raise the original interruption.
Attach the terminal `CountResult` as `count_result` on that exception; if
attribute assignment fails, emit the cleanup diagnostics to standard error and
re-raise the original interruption unchanged. Test both normal attachment and
fallback without depending on Python 3.11 exception notes. No
claim is made that synchronous blocking or external physical work is preempted.

## Owned scheduler and cleanup

Implement the existing `Scheduler` interface using the characterized thermal
serial dispatch: `start`, `stop`, `is_running`, `acquire_point`, `commit_action`,
`wait_point`, `clear_done_points`, and `snapshot`. A point accepts only one
unchained, nonthreaded `Action`; invalid/not-started/duplicate commits return
`False` before executing its body. Unknown points return `False`; success returns
`True`, ordinary action failure returns `False` with exception identity and
diagnostic retained, and a bounded timeout returns `None`. Dispatch sync bodies
and await returned awaitables on the privately created loop. Never route through
or mutate the module-global scheduler. A timeout can cancel an awaitable at an asynchronous suspension point; a
synchronous blocking body prevents the event loop from enforcing the deadline
until it returns, and may complete successfully after it. No preemption or
physical termination is promised. The serial scheduler has no reset API;
legacy `Process.reset()` and append-on-repeat semantics remain untouched.

On completion, cancellation, setup/dispatch failure, and interruption, attempt
scheduler stop, cancellation/drain of pending owned loop tasks, and loop close
independently in that order. Record each failure and continue later steps. Set
flags only on observed successful drain/closure. Failure to construct an owned
resource must still release any resource already created. Never dispose a
borrowed/global scheduler or thread event loop. The synchronous runner's
preflight avoids nested event loops; the scheduler must reject unsupported
chained and threaded actions before body side effects. Cleanup can only claim
what was actually observed. Scheduler lifecycle and result ownership must be
explicit in public docstrings; no caller-side access to private loop attributes
should be necessary.

## Verification and compatibility

Independent tests must establish operation traces for both thermal policies and
the signal monitor; stateful read counts must reveal extra observations. Cover
zero and after-row stops, late stop, failure before advance and read failure
after advance, commit/join rejection, success, no post-acknowledgment work,
partial rows and boundary, cleanup on every terminal path and primary plus
cleanup failures, including a cleanup step that fails without suppressing later
steps. Test preflight and unsupported actions before side effects; verify the
borrowed/global scheduler is unaffected. Preserve existing count/scan and W01
characterization tests, and run the required full discovery, compile and import
checks. W03 adds lifecycle fault depth and reset/repeat evidence; none of the
basic cleanup or truthful failure obligations above can be deferred there.

Control writes performed before `run_count` are caller setup: their failure is
reported by the reference fixture, and no runner resource has yet been created.
If a caller puts a control write inside a scheduled count action, that exception
is a runner-captured primary failure. Tests must cover both without claiming the
runner observed a pre-invocation failure. Fault injection may patch the serial
scheduler factory or methods in tests; these are not additional public APIs.

No real instruments, persistence format changes, generic scheduler repair,
new dependencies, automatic retry, durable resume, or scan extension are part
of this design. Architect re-review is required.

## Clarifications for review

A timed-out point caches `None`; repeated waits return `None` without rerunning
the action or causing further action side effects. Reject a chained Counter
child whose action has a nonempty begin point before executing that action.
Secondary inspection failures are recorded in diagnostics and make
`overall_success` false even if acquisition otherwise completed.

For standalone use, `SerialScheduler.stop()` disables further commits and
clears action references; `close()` additionally attempts to drain owned tasks
and close its loop, reporting cleanup failures and observed drain/close flags.
It is safe to call `close()` after `stop()`; callers must close their scheduler
on every terminal path. The runner invokes this owned cleanup automatically.
