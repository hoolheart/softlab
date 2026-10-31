"""Test-owned M01 serial scheduler characterization fixture."""
import asyncio
import inspect
import uuid
from typing import Any, Dict, Optional
from softlab.huo.scheduler import Action, Scheduler


class _CountScheduler(Scheduler):
    """Owned serial Action executor for this count; no chains or threads."""

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._running = False
        self._points: Dict[str, Optional[Action]] = {}
        self._done: Dict[str, Optional[bool]] = {}
        self.last_error = ''
        self.last_exception: Optional[Exception] = None

    def start(self) -> bool:
        """Enable commits without changing the thread's event loop."""
        self._running = True
        return True

    def stop(self) -> None:
        """Disable commits and discard owned action references."""
        self._running = False
        self._points.clear()
        self._done.clear()

    def is_running(self) -> bool:
        """Return whether commits are enabled."""
        return self._running

    def acquire_point(self) -> str:
        """Allocate a fresh serial completion key."""
        point = str(uuid.uuid4())
        self._points[point] = None
        return point

    def commit_action(self, action: Action) -> bool:
        """Accept one unchained, nonthreaded action for an unused endpoint."""
        if (not self._running or not isinstance(action, Action)
                or action.begin_point or action.to_thread
                or action.end_point not in self._points
                or self._points[action.end_point] is not None):
            return False
        self._points[action.end_point] = action
        return True

    def wait_point(self, point: str,
                   timeout: Optional[float] = None) -> Optional[bool]:
        """Run once; return false on failure or None on timeout."""
        if point in self._done:
            return self._done[point]
        action = self._points.get(point)
        if not self._running or action is None:
            return False

        async def dispatch() -> None:
            func, args, kwargs = action.body
            result = func(*args, **kwargs)
            if inspect.isawaitable(result):
                await result

        try:
            coroutine = dispatch()
            if timeout is not None:
                coroutine = asyncio.wait_for(coroutine, timeout)
            self._loop.run_until_complete(coroutine)
            result = True
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
        """Discard completed endpoints without executing anything."""
        for point in self._done:
            self._points.pop(point, None)
        self._done.clear()

    def snapshot(self) -> Dict[str, Any]:
        """Describe this bounded executor's current state."""
        return {'type': '_CountScheduler', 'running': self._running,
                'points': len(self._points), 'completed': len(self._done)}


def _make_scheduler() -> _CountScheduler:
    """Create a private executor with a separately owned event loop."""
    return _CountScheduler()
