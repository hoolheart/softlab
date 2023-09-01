"""Common implementations to treat composition of multiple processes"""

from typing import (
    Any,
    Sequence,
    Dict,
    Callable,
    Optional,
)
import logging
from softlab.shui.data import DataGroup
from softlab.huo.scheduler import (
    Scheduler,
    get_scheduler,
)
from softlab.huo.process.process import Process

_logger = logging.getLogger(__name__)

class CompositeProcess(Process):
    """
    Process to hold a sequence of subprocesses

    This is super class of all composited process, it is designed to be an
    iterable class.
    """

    def __init__(self, processes: Sequence[Process] = [],
                 name: Optional[str] = None) -> None:
        super().__init__(name)
        self._children: list[Process] = []
        self._iter_index = 0
        if isinstance(processes, Sequence):
            for proc in processes:
                self.add(proc)

    def set_data_group(self, group: DataGroup) -> None:
        """Override data_group setting to synchronize with subprocesses"""
        super().set_data_group(group)
        for child in self._children:
            child.set_data_group(group)

    def add(self, child: Process) -> None:
        """Add a new subprocess"""
        if not isinstance(child, Process):
            raise TypeError(f'Invalid process type {type(child)}')
        self._children.append(child)
        if isinstance(self.data_group, DataGroup):
            child.set_data_group(self.data_group)

    def clear(self) -> None:
        """Clear all subprocesses"""
        self._children = []
        self._iter_index = 0

    def __len__(self) -> int:
        """Get count of subprocesses"""
        return len(self._children)

    def __getitem__(self, index: int) -> Process:
        """Get subprocess at given index"""
        return self._children[index]

    def __iter__(self) -> Any:
        """Start iteration"""
        self._iter_index = 0
        return self

    def __next__(self) -> Process:
        """Iterate sequence"""
        if self._iter_index < len(self._children):
            self._iter_index += 1
            return self._children[self._iter_index - 1]
        else:
            raise StopIteration

    def snapshot(self) -> Dict[str, Any]:
        snapshot = super().snapshot()
        snapshot['children'] = [
            child.snapshot()
            for child in self._children
        ]
        return snapshot

class SeriesProcess(CompositeProcess):
    """
    Implementation of composited process, the subprocesses are run sequentially
    """

    def __init__(self, processes: Sequence[Process] = [],
                 name: Optional[str] = None) -> None:
        super().__init__(processes, name)
        self._index = 0

    def commit(self, scheduler: Scheduler) -> bool:
        while self._index < len(self):
            proc: Process = self[self._index]
            if proc.is_pending():
                _logger.warn(f'Child {proc.name} of {self.name} is pending')
                return False
            elif not proc.has_more():
                self._index += 1
                continue
            else:
                return proc.commit(scheduler)
        _logger.warn(f'All children of {self.name} has done')
        return False

    def is_pending(self) -> bool:
        if self._index < len(self):
            return self[self._index].is_pending()
        return False

    def join(self, scheduler: Scheduler) -> bool:
        if self._index < len(self):
            proc: Process = self[self._index]
            if proc.is_pending():
                return proc.join(scheduler)
        return True

    def has_more(self) -> bool:
        if self._index < len(self):
            proc: Process = self[self._index]
            if proc.is_pending() or proc.has_more():
                return True
            else:
                return self._index < len(self) - 1
        return False

    def reset(self) -> None:
        for child in self:
            child.reset()
        self._index = 0

class ParallelProcess(CompositeProcess):
    """
    Implementation of composited process, the subprocesses are run concurrently
    """

    def __init__(self, processes: Sequence[Process] = [],
                 name: Optional[str] = None) -> None:
        super().__init__(processes, name)

    def commit(self, scheduler: Scheduler) -> bool:
        rst: bool = False
        for child in self:
            if child.is_pending() or not child.has_more():
                continue
            if child.commit(scheduler):
                rst = True
        return rst

    def is_pending(self) -> bool:
        for child in self:
            if child.is_pending():
                return True
        return False

    def join(self, scheduler: Scheduler) -> bool:
        rst = False
        for child in self:
            if child.is_pending():
                if child.join(scheduler):
                    rst = True
        return rst

    def has_more(self) -> bool:
        for child in self:
            if child.is_pending() or child.has_more():
                return True
        return False

    def reset(self) -> None:
        for child in self:
            child.reset()

class SwitchProcess(CompositeProcess):
    """
    Implementaion of composited process, only run one subprocess selected by
    a given switcher
    """

    def __init__(self, switcher: Optional[Callable] = None,
                 processes: Sequence[Process] = [],
                 name: Optional[str] = None) -> None:
        super().__init__(processes, name)
        self._switcher: Optional[Callable] = None
        self.switcher = switcher
        self._current: Optional[Process] = None
        self._decided = False

    @property
    def switcher(self) -> Optional[Callable]:
        return self._switcher

    @switcher.setter
    def switcher(self, sw: Callable) -> None:
        if not isinstance(sw, Callable):
            raise TypeError(f'Switcher needs to be callable: {type(sw)}')
        self._switcher = sw

    def commit(self, scheduler: Scheduler) -> bool:
        if self._current is None and isinstance(self._switcher, Callable) \
                and not self._decided:
            self._decided = True
            try:
                index = int(self._switcher(self.data_group))
                assert(index >= 0 and index < len(self))
            except Exception as e:
                _logger.warn(f'Failed to get valid index by switcher: {e}')
                return False
            self._current = self[index]
            return self._current.commit(scheduler)
        return False

    def is_pending(self) -> bool:
        if isinstance(self._current, Process):
            return self._current.is_pending()
        return False

    def join(self, scheduler: Scheduler) -> bool:
        if isinstance(self._current, Process):
            return self._current.join(scheduler)
        return False

    def has_more(self) -> bool:
        if isinstance(self._current, Process):
            return self._current.has_more()
        return not self._decided

    def reset(self) -> None:
        for child in self:
            child.reset()
        self._current = None
        self._decided = False

class SweepProcess(Process):
    """
    A sweep loop with a loop body and a callable sweeper.

    The sweeper is used in every beginning of loop, it takes two arguments:
    1. the corresponding data group;
    2. the body process, in order to adjust the body for next loop.

    The sweeper returns a bool value to decide whether to continue loop,
    True means continue, False means finished.
    """

    def __init__(self, sweeper: Callable, body: Process,
                 name: Optional[str] = None) -> None:
        super().__init__(name)
        if not isinstance(body, Process):
            raise TypeError(f'Sweep body must be a process: {type(body)}')
        self._body = body
        if not isinstance(sweeper, Callable):
            raise TypeError(f'Sweeper must be callable: {type(sweeper)}')
        self._sweeper = sweeper
        self._in_loop = False
        self._finished = False

    def set_data_group(self, group: DataGroup) -> None:
        """Override to synchronize with sweep body"""
        super().set_data_group(group)
        self._body.set_data_group(group)

    def commit(self, scheduler: Scheduler) -> bool:
        if self._in_loop:
            return self._body.commit(scheduler)
        elif not self._finished:
            try:
                decision = bool(self._sweeper(self.data_group, self._body))
            except Exception as e:
                _logger.critical(f'Failed to call sweeper: {e}')
                return False
            if decision:
                self._in_loop = True
                self._body.reset()
                return self._body.commit(scheduler)
            else:
                self._finished = True
        return False

    def is_pending(self) -> bool:
        if self._in_loop:
            return self._body.is_pending()
        return False

    def join(self, scheduler: Scheduler) -> bool:
        if self._in_loop:
            rst = self._body.join(scheduler)
            if not self._body.has_more():
                self._in_loop = False
            return rst
        return self._finished

    def has_more(self) -> bool:
        return not self._finished

    def reset(self) -> None:
        self._body.reset()
        self._in_loop = False
        self._finished = False

if __name__ == '__main__':
    import asyncio
    from softlab.jin.validator import ValString
    from softlab.huo.process.process import run_process
    from softlab.huo.process.simple import SimpleProcess

    class SaluteProcess(SimpleProcess):
        def __init__(self, name: Optional[str] = None, **kwargs):
            super().__init__(name, **kwargs)
            self.add_attribute('subject', ValString(1), 'World')

        async def body(self) -> None:
            print(f'Hello {self.subject()}')
            await asyncio.sleep(1.0)

    class SubjectSweeper:
        def __init__(self, subjects: Sequence[str] = []) -> None:
            self._subjects = list(subjects)
            self._index = 0

        def reset(self):
            self._index = 0

        def __call__(self, _: DataGroup, proc: SaluteProcess) -> bool:
            if self._index < len(self._subjects):
                proc.subject(self._subjects[self._index])
                self._index += 1
                return True
            else:
                return False

    sch = get_scheduler()
    sch.start()
    names = ['aaa', 'bbb', 'ccc', 'Suzhou',
             'China', 'Asia', 'Earth', 'universe']
    print(names)

    processes = [SaluteProcess(f'sep{i}') for i in range(len(names))]
    for i in range(len(names)):
        processes[i].subject(names[i])
    series = SeriesProcess(processes, 'series')
    print(f'Create series process: {series.snapshot()}')
    run_process(series, sch)

    print('Use parallel of separated processes')
    parallel = ParallelProcess(processes, 'parallel')
    print(f'Create parallel process: {parallel.snapshot()}')
    run_process(parallel, sch)

    print('Use switch process')
    sw = SwitchProcess(lambda x: 5, processes, 'switch')
    print(f'Create switch process: {sw.snapshot()}')
    run_process(sw, sch)

    print('Use sweep process')
    sweep = SweepProcess(
        SubjectSweeper(names), SaluteProcess('salute'), 'sweep')
    print(f'Create sweep process: {sweep.snapshot()}')
    run_process(sweep, sch)
