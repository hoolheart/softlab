# Bounded count execution

`run_count` executes a fresh `Counter` using a serial scheduler and event loop it
owns. The caller configures the process, its record, model and control writes;
the count hooks and getters determine advance and measurement order. Supply a
reader for the actual record length and a reader for the current acquisition
boundary. Preflight requires a nonpending `Counter`, an empty record and a
nonnegative integer target. Reader callbacks are caller code and may have their
own side effects. The synchronous runner cannot be called from a running event
loop; it does not accept borrowed execution resources.

From the repository root in an environment with softlab installed, try the
synthetic paired-signal client:

```python
from examples.signal_monitor import acquire

result, record, differences, reads = acquire(3)
assert result.overall_success
assert result.completed_rows == result.boundary == 3
assert differences == [3, 3, 3]
assert reads == [(channel, tick) for tick in (1, 2, 3)
                 for channel in ('signal', 'reference')]
```

The example advances once per row, reads each channel once and computes a
caller-owned difference. It creates no files or instrument connections. The
record and derived values are in memory.

A caller can pass `stop=StopRequest()` and call `stop.request()` before the run
or from `after_row`. The runner acknowledges a request at the next boundary,
without starting another cycle. A request made after the final row does not
change a completed result. This is cooperative software cancellation: it cannot
preempt blocking work or prove that physical work stopped.

Inspect `result.acquisition`, `completed_rows`, `boundary`, `cause`,
`cleanup_errors`, `work_drained`, `loop_closed` and `terminal`. A read can fail
after the model advances, so the boundary may exceed the number of rows; no
rollback or retry is implied. `overall_success` also requires successful owned
cleanup. Operational exceptions return a failed result. Preflight errors raise
before the scheduler is created; interruptions are re-raised after cleanup with
`count_result` attached where possible, otherwise cleanup diagnostics go to
standard error. Caller setup failures before invocation are caller-owned.

Standalone `SerialScheduler` users must call `close()` on every terminal path.
Only unchained, nonthreaded actions are supported. A timeout can interrupt a
cooperative awaitable, but cannot preempt a blocking synchronous action.
Legacy `count` and `scan` APIs retain their existing behavior. Both thermal
adapter modes use this execution seam; see the [thermal guide](thermal-experiment.md).

Each accepted bounded run has an in-memory `result.run_id` UUID. Use a fresh
Counter, record, model and stop request for each run. A Counter already used by
`run_count` is rejected before its row-count reader is called; a Counter used
only through legacy APIs cannot be identified reliably, so supplying a fresh
one remains the caller's responsibility. There is no automatic retry or resume.
Legacy `Process.reset()` semantics are unchanged.

`result.boundary_observed` distinguishes no successful boundary observation
from a successfully observed value of `None`. The last observed boundary can be
stale after a later reader failure. `result.secondary_errors` retains ordered
`(stage, exception)` pairs alongside the existing string diagnostics; a primary
failure remains in `result.cause`.

Use `reset_after_count(result, reset_callback)` to invoke a caller-owned model
reset only after terminal state, owned-work drain and loop closure. A reset
failure propagates unchanged and does not rewrite the prior result or rerun the
acquisition. This helper does not make the old Counter eligible for reuse.
