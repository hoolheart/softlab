"""Interface of dynamic process"""
from abc import abstractmethod
from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
)
import time
from softlab.jin.misc import (
    Delegated,
    LimitedAttribute,
)
from softlab.jin.validator import Validator
from softlab.shui.data import DataGroup
from softlab.huo.scheduler import (
    Scheduler,
    get_scheduler,
)

class Process(Delegated):
    """
    Interface of any dynamic process

    Properties:
    - name -- name of process, only can be given in creation
    - data_group -- binding data group

    As a subclass of `DelegateAttributes`, this interface supports customized
    attributes. Each attribute is an instance of ``LimitedAttribute``.
    ``add_attribute`` method is used to add such attribute by given unique key,
    validator and initial value.

    The methods need be implemented in derived classes:
    - commit -- commit necessary actions into scheduler
    - is_pending -- whether there are committed by unfinished actions
    - join -- wait until all committed actions finish
    - has_more -- whether there are more actions to run
    - reset -- reset to initial state

    Usage:
    1. create and configure a process
    2. commit actions into scheduler
    3. wait until all committed actions finish
    4. check whether there are more actions, if so, back to step 2
    """

    def __init__(self, name: Optional[str] = None):
        self._name = '' if name is None else str(name)
        self._attributes: Dict[str, LimitedAttribute] = {}
        self.add_delegate_attr_dict('_attributes')
        self._group: Optional[DataGroup] = None

    @property
    def name(self) -> str:
        """Get name of process"""
        return self._name

    @property
    def data_group(self) -> Optional[DataGroup]:
        """Get binding data group"""
        return self._group

    @data_group.setter
    def data_group(self, group: Optional[DataGroup]) -> None:
        """Bind with given data group"""
        self.set_data_group(group)

    @abstractmethod
    def set_data_group(self, group: Optional[DataGroup]) -> None:
        """Bind with given data group"""
        if self.is_pending():
            raise RuntimeError(f'Can\'t change data group while pending')
        if group is not None and not isinstance(group, DataGroup):
            raise TypeError(f'Invalid data group type: {type(group)}')
        self._group = group

    def add_attribute(self, key: str,
                      vals: Validator, initial_value: Any) -> None:
        """
        Add an attribute

        Args:
        - key -- the key of attribute, should be unique in one process
        - vals -- the validator of attribute,
        - initial_value -- the initial value of attribute
        """
        if key in self._attributes:
            raise ValueError(f'Already has the attribute with key "{key}"')
        self._attributes[key] = LimitedAttribute(vals, initial_value)

    @abstractmethod
    def commit(self, scheduler: Scheduler) -> bool:
        """Commit actions into scheduler"""
        raise NotImplementedError

    @abstractmethod
    def is_pending(self) -> bool:
        """Whether there are committed-but-unfinished actions"""
        raise NotImplementedError

    @abstractmethod
    def join(self, scheduler: Scheduler) -> bool:
        """Wait until committed actions finish"""
        raise NotImplementedError

    @abstractmethod
    def has_more(self) -> bool:
        """Whether there are more actions to run"""
        raise NotImplementedError

    @abstractmethod
    def reset(self) -> None:
        """Reset to initial state"""
        raise NotImplementedError

    def snapshot(self) -> Dict[str, Any]:
        return {
            'class': self.__class__,
            'name': self.name,
            'pending': self.is_pending(),
            'more': self.has_more(),
        }

def run_process(process: Process, scheduler: Optional[Scheduler] = None,
                verbose: bool = True) -> Tuple[bool, float]:
    """
    Run a process

    Arguments:
    - process -- the process to run
    - scheduler -- the scheduler to perform running, use ``get_scheduler`` if
                   None is given
    - verbose -- verbose flag

    Returns a tuple of result and cost time
    """
    if not isinstance(process, Process):
        raise TypeError(f'Invalid process type {type(process)}')
    if not isinstance(scheduler, Scheduler):
        scheduler = get_scheduler()
    verbose = bool(verbose)
    if verbose:
        print('---- Run Process ----')
    process.reset()
    if verbose:
        print('Reset process first')
        for key, value in process.snapshot().items():
            print(f'{key}: {value}')
        print()
    t0 = time.perf_counter()
    rst = True
    ticks = 0
    while process.has_more():
        process.commit(scheduler)
        if not process.join(scheduler):
            if verbose:
                print(f'Failed to join after {ticks}th commit')
            rst = False
            break
        ticks += 1
    used = time.perf_counter() - t0
    if verbose:
        print('Succeed' if rst else 'Failed')
        print(f'Commit {ticks} Times')
        print(f'Used {used} s')
        print('---- The End ----')
        print()
    return rst, used

if __name__ == '__main__':
    import asyncio
    from typing import Sequence
    from softlab.huo.scheduler import Action

    class SequenceSaluteProcess(Process):
        def __init__(self, subjects: Sequence[str],
                     name: Optional[str] = None, **kwargs):
            super().__init__(name, **kwargs)
            self._subjects: list[str] = []
            self._index = 0
            self._point = ''
            for subject in subjects:
                self.add(subject)

        def add(self, subject: str) -> None:
            self._subjects.append(str(subject))

        async def salute(self) -> None:
            if self._index >= 0 and self._index < len(self._subjects):
                print(f'Hello {self._subjects[self._index]}')
                await asyncio.sleep(1.0)
                self._index += 1

        def reset(self) -> None:
            self._index = 0
            self._point = ''

        def commit(self, scheduler: Scheduler) -> bool:
            if self._index == 0 and len(self._subjects) > 0 \
                    and len(self._point) == 0:
                points = scheduler.acquire_points(len(self._subjects))
                assert(len(points) == len(self._subjects))
                for i in range(len(points)):
                    if not scheduler.commit_action(Action(
                        '' if i == 0 else points[i-1], points[i],
                        func=self.salute,
                    )):
                        return False
                self._point = points[-1]
                return True
            return False

        def is_pending(self) -> bool:
            return len(self._point) > 0

        def join(self, scheduler: Scheduler) -> bool:
            if len(self._point) > 0:
                rst = scheduler.wait_point(self._point)
                self._point = ''
                return rst
            return False

        def has_more(self) -> bool:
            return self._index < len(self._subjects)

    sch = get_scheduler()
    sch.start()
    print(sch.snapshot())
    names = ['aaa', 'bbb', 'ccc', 'Suzhou',
             'China', 'Asia', 'Earth', 'universe']
    print(names)
    proc = SequenceSaluteProcess(names, 'single_seq')
    print(f'Create process: {proc.snapshot()}')
    run_process(proc, sch)
