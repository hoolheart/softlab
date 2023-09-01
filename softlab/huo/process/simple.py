"""Simple implement of process, all actions are wrapped into single body"""

from abc import abstractmethod
from typing import (
    Optional,
    Callable,
)
from softlab.huo.scheduler import (
    Action,
    Scheduler,
)
from softlab.huo.process.process import Process

class SimpleProcess(Process):
    """
    Base of simple processes which only concern simple actions

    The derived processes only need to implement asynchronised ``body``
    method to achieve process action. If a simple process is aborted before
    finish (i.e. ``reset`` is called), the aborting signal can be obtained
    by ``aborting`` porperty.
    """

    def __init__(self, name: Optional[str] = None, **kwargs):
        super().__init__(name, **kwargs)
        self._begin_point = ''
        self._end_point = ''
        self._finished = False
        self._running = False
        self._aborting = False

    @property
    def begin_point(self) -> str:
        """Get begin point of process"""
        return self._begin_point

    @begin_point.setter
    def begin_point(self, point: str) -> None:
        """Set begin point, can't be called while pending"""
        if self._running:
            raise RuntimeError('Can\'t set begin point while pending')
        self._begin_point = str(point)

    @property
    def end_point(self) -> str:
        """
        Get end point of process

        Notice that end point is generated when ``commit`` is called, it
        can't be set arbitrarily.
        """
        return self._end_point

    @property
    def aborting(self) -> bool:
        """Aborting signal"""
        return self._aborting

    @abstractmethod
    async def body(self) -> None:
        """Process action body, need implementation"""
        pass

    async def _run(self) -> None:
        if not self._finished:
            try:
                await self.body()
                self._finished = not self._aborting
            finally:
                self._begin_point = ''
                self._running = False
                self._aborting = False

    def reset(self) -> None:
        if self._running:
            self._aborting = True
        else:
            self._finished = False

    def commit(self, scheduler: Scheduler) -> bool:
        if not self._finished and not self._running:
            point = scheduler.acquire_point()
            rst = scheduler.commit_action(Action(
                self.begin_point, point, func=self._run
            ))
            if rst:
                self._running = True
                self._aborting = False
                self._end_point = point
            return rst
        return False

    def is_pending(self) -> bool:
        return self._running

    def join(self, scheduler: Scheduler) -> bool:
        if len(self._end_point) > 0:
            return scheduler.wait_point(self._end_point)
        return False

    def has_more(self) -> bool:
        return not self._finished

class SimpleProcessWrapper(SimpleProcess):
    """
    Wrapped process of any callable object
    """

    def __init__(self, proc: Callable, name: Optional[str] = None, **kwargs):
        super().__init__(name, **kwargs)
        if not isinstance(proc, Callable):
            raise TypeError(f'Invalid process {type(proc)}')
        self._proc = proc

    async def body(self) -> None:
        if asyncio.iscoroutinefunction(self._proc):
            await self._proc()
        else:
            self._proc()

if __name__ == '__main__':
    import asyncio
    import time
    from softlab.jin.validator import ValString
    from softlab.huo.scheduler import get_scheduler
    from softlab.huo.process.process import run_process

    class SaluteProcess(SimpleProcess):
        def __init__(self, name: Optional[str] = None, **kwargs):
            super().__init__(name, **kwargs)
            self.add_attribute('subject', ValString(1), 'World')

        async def body(self) -> None:
            print(f'Hello {self.subject()}')
            await asyncio.sleep(1.0)

    def hello_func():
        print('hello func')
        time.sleep(1.0)

    async def hello_func_async():
        print('hello func async')
        await asyncio.sleep(1.0)

    sch = get_scheduler()
    sch.start()
    print(sch.snapshot())
    proc = SaluteProcess('salute')
    run_process(proc, sch)
    proc.subject('Beijing')
    run_process(proc, sch)
    run_process(SimpleProcessWrapper(hello_func, 'wrap_func'), sch)
    run_process(SimpleProcessWrapper(hello_func_async, 'wrap_sync'), sch)
