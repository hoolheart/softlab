# M02-W03 detailed lifecycle design

Owner: Software Designer | Status: PROPOSED — architect review required

Inputs: [PRD](../prd.md), [W02 design](../../release_9/design/bounded-count-design.md),
`softlab/huo/process/bounded.py`, `softlab/huo/scheduler/serial.py`,
`examples/signal_monitor.py`, `examples/thermal_experiment.py`, and the
[principles](../../../principles.md). The W02 `run_count` signature, existing
`CountResult` fields, `StopRequest`, and `SerialScheduler` methods remain
compatible. Thermal migration stays in W04.

## Fresh run and identity

Append `run_id: uuid.UUID` to `CountResult` with a `uuid.uuid4` default factory;
keep existing positional constructor arguments and properties. Every call that
passes preflight and creates a result receives a distinct identity; no ID is
created for a preflight rejection. The UUID is in-memory run evidence, not a
persisted provenance schema. Fresh repeat means fresh `Counter`, record, model,
stop request, scheduler and private event loop; deterministic traces and numeric
rows match excluding timestamps and UUIDs. No automatic retry, replay, resume or
rollback is performed.

`run_count` supports a caller-supplied fresh Counter with a nonpending process
and an initially empty record. It can detect previous bounded-run claims only;
a Counter previously used through legacy APIs and subsequently cleared cannot
be reliably identified, and is unsupported rather than claimed to be detected.
Check an existing bounded claim before calling the row-count reader; an unclaimed
reader failure must not set the claim. After remaining preflight validation succeeds,
claim that Counter for this bounded-run path before creating the scheduler or
calling reset; a subsequent `run_count` with the same Counter raises
`RuntimeError` before execution side effects, even if the first run failed or
was cancelled. Store the private claim on the Counter, without modifying legacy
`Process.reset()` or preventing legacy callers from using that process. Do not
clear the claim on failed setup or reset: eligibility cannot safely be inferred
from a partly executed callback. Calls failing preflight before the claim do not
consume the Counter. The claim guards supported sequential use, not simultaneous
cross-thread invocation; cross-thread use is unsupported.

The runner's existing `process.reset()` is initialization of a fresh Counter,
not a reusable-execution reset. Its failure is the primary operational cause,
no acquisition occurs, and owned cleanup still runs. Do not repeat reset
implicitly. No new scheduler reset API is introduced.

## Caller-owned model reset

Export `reset_after_count(result: CountResult, reset: Callable[[], None]) -> None`
from `softlab.huo.process`. Validate types and call the caller's synchronous
reset exactly once only when `result.terminal`, `result.work_drained`, and
`result.loop_closed` are true; otherwise raise before invoking it. This is a
caller-owned model operation, not reuse of a Counter or scheduler. Its failure
propagates unchanged, does not alter the prior terminal result, and never starts
acquisition. The caller remains responsible for fresh model/process/record on a
new run. No physical completion is inferred from drain and closure. Since
`CountResult` is mutable for W02 compatibility, callers must not mutate terminal
evidence; the helper does not mutate it. A successful reset does not modify or
replace the result, and no helper-level automatic retry is provided.

## State, failures and observation

Retain W02's `acquisition` values and `cause` exception identity. Add a
`secondary_errors` list of `(stage: str, exception: BaseException)` pairs to
`CountResult`, default empty, preserving ordered exception identity alongside
existing string `cleanup_errors`. Keep `cleanup_errors` and its existing
`overall_success` behavior for compatible callers; add a corresponding string
entry for each secondary error so the existing success property remains false.
Record secondary reader errors and each applicable stop, drain and close failure
without overwriting the primary cause. `SerialScheduler` likewise exposes
structured secondary failures for the runner to copy while retaining existing
string diagnostics and flags. Avoid duplicating a single cleanup failure when
`close()` re-raises an interruption already recorded by the scheduler.

The primary cause is the first operational failure in execution order, including
scheduler creation/start, reset, commit/join, acquisition, row-count or boundary
inspection, or after-row callback. An absent scheduler after constructor failure
means there is no owned scheduler to drain/close and both flags remain false;
any resource allocated during construction must be released by the constructor
before it raises. Failure of preflight validation or its initial row-count
reader propagates without a result or owned resources.

Inspect row count and boundary separately on every observation, in that order:
one reader's exception must not prevent the other from reporting a newer truthful
value. With no prior cause, the first inspection error becomes primary; later
inspection failures are secondary. An inspection `BaseException` is still
reraised after cleanup even if an earlier ordinary failure remains the primary
cause; the first interruption retains precedence over later interruptions. Update each field only after its reader succeeds. Preserve the last
successful value when later inspection fails and record the failure with its
stage. Add `boundary_observed: bool` default false, set true only after a
successful boundary reader; `boundary=None` is otherwise ambiguous because
`None` may itself be a valid boundary value. A boundary may exceed row count
when sampling fails after advance. Invalid row count is an inspection failure;
never manufacture rows or roll back a caller-owned model.

On every path where an owned scheduler exists, independently attempt stop,
drain, and close, retaining ordered diagnostics and truthful observed flags. An
ordinary primary exception remains `cause`; later cleanup/inspection exceptions
are secondary. A `BaseException` interruption is re-raised after all cleanup:
the first interruption takes precedence over later interruptions and retains
its identity; attach terminal result as `count_result` where possible, otherwise
emit diagnostics to stderr without replacing the original. If an ordinary
primary error precedes an interruption, retain it as `cause` and re-raise the
interruption with attached evidence. If interruption is first and later ordinary
errors occur, it remains the primary cause. Continue independent cleanup after
all failures. Thread event loop and global scheduler remain untouched; borrowed
resources are not accepted as inputs and no ownership transfer is implied.

Stop request is checked only after completion test and before committing the
next action. Acknowledgment prevents later cycles; a late request does not
relabel completion. Neither cooperative cancellation nor task drain proves
physical work has stopped or preempts synchronous blocking work.

## Verification and compatibility

Test using independent synthetic fault injection: each scheduler creation,
start, commit/join, advance, sample, reset, row-count, boundary, stop, drain and
close failure, including pairs of primary and secondary failures. Verify exact
exception identities and cleanup order/flags, an interruption with attachment
and fallback, and continued cleanup after interruption. Cover missing boundary
and `None` as a valid observed boundary, and show a successful boundary read
still occurs when row-count inspection fails. Verify zero/after-row/late stop,
no work after acknowledgment, and untouched global/thread loops. Independently
assert fresh deterministic runs and UUIDs, original terminal evidence after
successful and failed model reset, reject ineligible reset and repeated Counter
use before effects, and no implicit retry. Run W01/W02 characterization and
full discovery, compile, and import checks. Existing `count`, `scan`, legacy
`Process.reset()` and the existing thermal example are not altered by the new
bounded-path guard. Review all public exports and docstrings, update the bounded
usage guide, and record limitations and actual warnings.
