"""Bounded count execution with caller-owned acquisition policy."""
import asyncio
import inspect
import sys
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional

from softlab.huo.scheduler import SerialScheduler
from softlab.huo.scheduler.serial import _SchedulerInitFailure
from .common import Counter


@dataclass
class StopRequest:
    """Cooperative request, acknowledged only before a subsequent cycle."""
    requested: bool = False
    acknowledged: bool = False

    def request(self) -> None:
        """Request cancellation at the next acquisition boundary."""
        self.requested = True


@dataclass
class CountResult:
    """Terminal acquisition and independent owned-cleanup evidence."""
    requested_rows: int
    stop: StopRequest
    acquisition: str = 'not_started'
    completed_rows: int = 0
    boundary: Any = None
    cause: Optional[BaseException] = None
    cleanup_errors: List[str] = field(default_factory=list)
    work_drained: bool = False
    loop_closed: bool = False
    terminal: bool = False
    run_id: uuid.UUID = field(default_factory=uuid.uuid4)
    boundary_observed: bool = False
    secondary_errors: List[tuple] = field(default_factory=list)

    @property
    def overall_success(self) -> bool:
        """Whether acquisition completed and owned cleanup succeeded."""
        return (self.acquisition == 'completed' and not self.cleanup_errors
                and self.work_drained and self.loop_closed and self.terminal)


def reset_after_count(result: CountResult, reset: Callable[[], None]) -> None:
    """Reset caller-owned state once after a terminal, drained, closed run.

    Reset errors propagate unchanged; the result and process are not reset.
    """
    if not isinstance(result, CountResult):
        raise TypeError('result must be a CountResult')
    if not callable(reset) or inspect.iscoroutinefunction(reset):
        raise TypeError('reset must be synchronous and callable')
    if not (result.terminal and result.work_drained and result.loop_closed):
        raise RuntimeError('Reset requires terminal, drained and closed run')
    value = reset()
    if inspect.isawaitable(value):
        if inspect.iscoroutine(value):
            value.close()
        raise TypeError('reset must not return an awaitable')


def run_count(process: Counter, total: int, row_count: Callable[[], int],
              boundary: Callable[[], Any], *, stop: Optional[StopRequest] = None,
              after_row: Optional[Callable[[CountResult], None]] = None
              ) -> CountResult:
    """Run a fresh count process using owned serial execution.

    Readers inspect caller-owned record and boundary. Configuration, control
    writes, persistence and model cleanup remain caller-owned. Preflight errors
    raise; ordinary operational failures return a result retaining their cause.
    Interruptions are reraised after cleanup with ``count_result`` attached when
    possible, otherwise cleanup diagnostics are printed to stderr.
    """
    if not isinstance(process, Counter):
        raise TypeError('process must be a Counter')
    if type(total) is not int or total < 0:
        raise ValueError('total must be a nonnegative integer')
    if not callable(row_count) or not callable(boundary):
        raise TypeError('row_count and boundary must be callable')
    if after_row is not None and not callable(after_row):
        raise TypeError('after_row must be callable')
    if stop is not None and not isinstance(stop, StopRequest):
        raise TypeError('stop must be a StopRequest')
    if process.is_pending():
        raise ValueError('process must not be pending')
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError('run_count cannot run inside an event loop')
    if getattr(process, '_bounded_run_claimed', False):
        raise RuntimeError('Counter already used for bounded execution')
    initial = row_count()
    if type(initial) is not int or initial != 0:
        raise ValueError('initial row count must be zero')
    process._bounded_run_claimed = True
    stop = stop if stop is not None else StopRequest()
    result = CountResult(total, stop)
    scheduler = None
    interruption = None

    def secondary(stage, error):
        result.secondary_errors.append((stage, error))
        result.cleanup_errors.append(f'{stage}: {error}')

    def inspect_state(primary=None):
        errors = []
        try:
            count = row_count()
            if type(count) is not int or not 0 <= count <= total:
                raise ValueError('invalid row count')
            result.completed_rows = count
        except BaseException as exc:
            errors.append(('Row count inspection', exc))
        try:
            value = boundary()
            result.boundary = value
            result.boundary_observed = True
        except BaseException as exc:
            errors.append(('Boundary inspection', exc))
        if not errors:
            return None
        if primary is None:
            result.cause = errors[0][1]
            for stage, exc in errors[1:]:
                secondary(stage, exc)
            first_interrupt = next((exc for _, exc in errors
                                    if not isinstance(exc, Exception)), None)
            raise first_interrupt or errors[0][1]
        for stage, exc in errors:
            secondary(stage, exc)
        return next((exc for _, exc in errors
                     if not isinstance(exc, Exception)), None)

    try:
        scheduler = SerialScheduler(_capture_failures=True)
        if not scheduler.start():
            raise RuntimeError('Scheduler did not start')
        process.reset()
        while True:
            inspect_state()
            if result.completed_rows == total:
                result.acquisition = 'completed'
                break
            if stop.requested:
                stop.acknowledged = True
                result.acquisition = 'cancelled'
                break
            previous = result.completed_rows
            if not process.commit(scheduler) or process.join(scheduler) is not True:
                raise (scheduler.last_exception or RuntimeError(
                    scheduler.last_error or 'Count execution failed'))
            inspect_state()
            if result.completed_rows != previous + 1:
                raise RuntimeError('Count action did not commit exactly one row')
            if after_row is not None:
                after_row(result)
    except BaseException as exc:
        result.acquisition = 'failed'
        constructor_cleanup = None
        if isinstance(exc, _SchedulerInitFailure):
            constructor_cleanup = exc.cleanup
            exc = exc.primary
        if result.cause is None:
            result.cause = exc
        if constructor_cleanup is not None:
            secondary('Owned loop close', constructor_cleanup)
            if not isinstance(constructor_cleanup, Exception):
                interruption = constructor_cleanup
        if not isinstance(exc, Exception):
            interruption = exc
        observed_interrupt = inspect_state(primary=result.cause)
        if interruption is None:
            interruption = observed_interrupt
    finally:
        if scheduler is not None:
            try:
                scheduler.close()
            except BaseException as exc:
                if not any(error is exc for _, error in scheduler.secondary_errors):
                    secondary('Scheduler cleanup', exc)
                if interruption is None and not isinstance(exc, Exception):
                    interruption = exc
            result.cleanup_errors.extend(scheduler.cleanup_errors)
            result.secondary_errors.extend(scheduler.secondary_errors)
            result.work_drained = scheduler.work_drained
            result.loop_closed = scheduler.loop_closed
        result.terminal = True
    if interruption is not None:
        try:
            interruption.count_result = result
        except BaseException:
            try:
                print('; '.join(result.cleanup_errors), file=sys.stderr)
            except BaseException:
                pass
        raise interruption
    return result
