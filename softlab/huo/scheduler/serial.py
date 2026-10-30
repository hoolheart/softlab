"""Private-loop serial scheduler for bounded, unchained actions."""
import asyncio
import inspect
import uuid
from typing import Any, Dict, Optional

from .scheduler import Action, Scheduler


class _ActionTimeout(Exception):
    """Distinguish an action-raised TimeoutError from a wait deadline."""

    def __init__(self, cause):
        self.cause = cause


class SerialScheduler(Scheduler):
    """Dispatch one unchained action per point on an owned event loop.

    Call ``close`` on every terminal path. Timeouts can interrupt cooperative
    awaitables but cannot preempt a synchronous blocking action.
    """

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._running = False
        self._points = {}
        self._done = {}
        self.last_error = ''
        self.last_exception = None
        self.cleanup_errors = []
        self.work_drained = False
        self.loop_closed = False

    def start(self) -> bool:
        if self._loop.is_closed():
            return False
        self._running = True
        return True

    def stop(self) -> None:
        self._running = False
        self._points.clear()
        self._done.clear()

    def close(self) -> None:
        """Stop, drain and close owned resources, recording each failure."""
        if self._loop.is_closed():
            self.loop_closed = True
            return
        interruption = None
        try:
            self.stop()
        except BaseException as exc:
            self.cleanup_errors.append(f'Scheduler stop: {exc}')
            if not isinstance(exc, Exception):
                interruption = exc
        try:
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            if pending:
                self._loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True))
            self.work_drained = True
        except BaseException as exc:
            self.cleanup_errors.append(f'Owned loop drain: {exc}')
            if interruption is None and not isinstance(exc, Exception):
                interruption = exc
        try:
            self._loop.close()
            self.loop_closed = self._loop.is_closed()
        except BaseException as exc:
            self.cleanup_errors.append(f'Owned loop close: {exc}')
            if interruption is None and not isinstance(exc, Exception):
                interruption = exc
        if interruption is not None:
            raise interruption

    def is_running(self) -> bool:
        return self._running

    def acquire_point(self) -> str:
        point = str(uuid.uuid4())
        self._points[point] = None
        return point

    def commit_action(self, action: Action) -> bool:
        if (not self._running or not isinstance(action, Action)
                or action.begin_point or action.to_thread
                or action.end_point not in self._points
                or self._points[action.end_point] is not None):
            return False
        self._points[action.end_point] = action
        return True

    def wait_point(self, point: str,
                   timeout: Optional[float] = None) -> Optional[bool]:
        if point in self._done:
            return self._done[point]
        action = self._points.get(point)
        if not self._running or action is None:
            return False

        async def dispatch():
            func, args, kwargs = action.body
            try:
                result = func(*args, **kwargs)
                if inspect.isawaitable(result):
                    await result
            except asyncio.TimeoutError as exc:
                raise _ActionTimeout(exc) from exc

        try:
            coro = dispatch()
            if timeout is not None:
                coro = asyncio.wait_for(coro, timeout)
            self._loop.run_until_complete(coro)
            result = True
        except _ActionTimeout as exc:
            self.last_exception = exc.cause
            self.last_error = f'{type(exc.cause).__name__}: {exc.cause}'
            result = False
        except asyncio.TimeoutError as exc:
            self.last_exception = exc
            self.last_error = str(exc) or 'Action timed out'
            result = None
        except Exception as exc:
            self.last_exception = exc
            self.last_error = f'{type(exc).__name__}: {exc}'
            result = False
        self._done[point] = result
        return result

    def clear_done_points(self) -> None:
        for point in self._done:
            self._points.pop(point, None)
        self._done.clear()

    def snapshot(self) -> Dict[str, Any]:
        return {'type': 'SerialScheduler', 'running': self._running,
                'points': len(self._points), 'completed': len(self._done)}
