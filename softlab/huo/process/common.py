from abc import abstractmethod
from typing import (
    Any,
    Dict,
    Optional,
    Callable,
    Tuple,
    Optional,
    Sequence,
    Union,
)
import asyncio
from datetime import datetime
from softlab.huo.process.process import Process
from softlab.huo.process.simple import SimpleProcess
from softlab.huo.process.composite import (
    SweepProcess,
    WrappedSweeper,
)
from softlab.shui.data import (
    DataGroup,
    DataRecord,
)
from softlab.tu.station import Parameter
from softlab.jin.validator import (
    ValType,
    ValInt,
    ValNumber,
)
from softlab.jin.misc import LimitedAttribute

ParameterSet = Union[Sequence[Parameter],
                     Dict[str, Parameter],
                     Sequence[Tuple[str, Parameter]]]
"""Possible type union to define a parameter set"""

SetterSeq = Sequence[Tuple[str, Parameter, Any]]
"""
Sequence type of setter info, each info is a tuple with 3 elements:
- key, corresponding column name in DataRecord
- parameter to set
- value to set to the parameter
"""

GetterSeq = Sequence[Tuple[str, Parameter]]
"""
Sequence type of getter info, each info is a tuple with 2 element:
- key, corresponding column name in DataRecord
- parameter to get
"""

ValueDict = Dict[str, Any]
"""Dictionary type of setter key and setter value"""


def parse_getters(getters: ParameterSet) -> GetterSeq:
    """Parse any possible parameter set into a getter info sequence"""
    if isinstance(getters, Sequence) and len(getters) > 0:
        if isinstance(getters[0], Tuple):
            return getters
        return list(map(lambda para: (para.name, para), getters))
    elif isinstance(getters, Dict):
        return getters.items()
    return []


def parse_setters(setters: ParameterSet) -> SetterSeq:
    """Parse any possible parameter set into a setter info sequence"""
    if isinstance(setters, Sequence) and len(setters) > 0:
        if isinstance(setters[0], Tuple):
            return list(map(
                lambda pair: (pair[0], pair[1], pair[1]()), setters))
        return list(map(
            lambda para: (para.name, para, para()), setters))
    elif isinstance(setters, Dict):
        return list(map(
            lambda pair: (pair[0], pair[1], pair[1]()), setters.items()))
    return []


def parse_setters_with_values(
        setters: ParameterSet, values: ValueDict) -> SetterSeq:
    """
    Parse any possible parameter set and given dictionary of values
    into a setter info sequence
    """
    if isinstance(setters, Sequence) and len(setters) > 0:
        if isinstance(setters[0], Tuple):
            return list(map(
                lambda pair: (
                    pair[0], pair[1],
                    values[pair[0]] if pair[0] in values else pair[1]()),
                setters))
        return list(map(
            lambda para: (
                para.name,
                para,
                values[para.name] if para.name in values else para()),
            setters))
    elif isinstance(setters, Dict):
        return list(map(
            lambda pair: (pair[0],
                          pair[1],
                          values[pair[0]] if pair[0] in values else pair[1]()),
            setters.items()))
    return []


class AtomJob(SimpleProcess):
    """
    Atom job with one time of setting and getting of given parameters.

    The job is defined by sequence of setter info tuples and getter info tuples,
    they must be given at initialziation.

    Each setter corresponds an attribute of job. Such attribute uses key of
    setter as name and shares the same validator of parameter of setter. The
    value of such attribute will be given to the setting parameter at the
    setting phase.

    Three delays can be controlled in initialization and
    treat as attributes as well:
    - delay_begin --- delay before all setting and getting
    - delay_after_set --- delay after setting action, ignored if no setting
    - delay_end --- delay after all setting and getting

    Every atom job can be given a DataRecord or use `prepare_record` method
    to generate such DataRecord to record actions. The DataRecord is composed
    by three parts:
    - timestamp of action, after delay of beginning
    - values to set, column names are keys of setters
    - values got, column names are keys of getters

    `t0` attribute is used to control timestamp of record, which will be
    calculated by utc time minus t0 value.

    Units of t0, record timestamps and all delays are second.

    Three important properties:
    - setters --- dictionary of key and parameter of setters, read-only
    - getters --- dictionary of key and parameter of getters, read-only
    - record --- DataRecord to record data

    Users can also define hooks before or after setting and getting. The hook
    functions are given at initialization.

    Another important attribute is `is_dryrun`. When it is True, the job
    only performs begin and end delays without any setting, getting or record.
    """

    def __init__(self,
                 setters: SetterSeq = [],
                 getters: GetterSeq = [],
                 delay_begin: float = 0.0,
                 delay_after_set: float = 0.0,
                 delay_end: float = 0.0,
                 hook_before_set: Optional[Callable] = None,
                 hook_after_set: Optional[Callable] = None,
                 hook_before_get: Optional[Callable] = None,
                 hook_after_get: Optional[Callable] = None,
                 name: Optional[str] = None) -> None:
        super().__init__(name)
        self.add_attribute('t0', ValNumber(), 0.0)
        if not isinstance(delay_begin, float) or delay_begin < 0.0:
            delay_begin = 0.0
        self.add_attribute('delay_begin', ValNumber(0.0), delay_begin)
        if not isinstance(delay_after_set, float) or delay_after_set < 0.0:
            delay_after_set = 0.0
        self.add_attribute('delay_after_set', ValNumber(0.0), delay_after_set)
        if not isinstance(delay_end, float) or delay_end < 0.0:
            delay_end = 0.0
        self.add_attribute('delay_end', ValNumber(0.0), delay_end)
        self.add_attribute('is_dryrun', ValType(bool), False)
        self._columns = [{'name': 'timestamp',
                          'unit': 's', 'dependent': False}]
        self._setters: Dict[str, Parameter] = {}
        if isinstance(setters, Sequence):
            for setter in setters:
                if isinstance(setter, Tuple) and len(setter) == 3:
                    key = str(setter[0])
                    para = setter[1]
                    value = setter[2]
                    if len(key) > 0 and isinstance(para, Parameter):
                        self._setters[key] = para
                        self.add_attribute(key, para.validator, value)
                        self._columns.append({
                            'name': key,
                            'dependent': False,
                        })
        self._getters: Dict[str, Parameter] = {}
        if isinstance(getters, Sequence):
            for getter in getters:
                if isinstance(getter, Tuple) and len(getter) == 2:
                    key = str(getter[0])
                    para = getter[1]
                    if len(key) > 0 and isinstance(para, Parameter):
                        self._getters[key] = para
                        self._columns.append({
                            'name': key,
                            'dependent': True,
                        })
        self._record: Optional[DataRecord] = None
        self._hook_before_set = hook_before_set \
            if isinstance(hook_before_set, Callable) else None
        self._hook_after_set = hook_after_set \
            if isinstance(hook_after_set, Callable) else None
        self._hook_before_get = hook_before_get \
            if isinstance(hook_before_get, Callable) else None
        self._hook_after_get = hook_after_get \
            if isinstance(hook_after_get, Callable) else None

    @property
    def setters(self) -> Dict[str, Parameter]:
        """Dictionary of key and parameter of setters, read-only"""
        return self._setters

    @property
    def getters(self) -> Dict[str, Parameter]:
        """Dictionary of key and parameter of getters, read-only"""
        return self._getters

    @property
    def record(self) -> Optional[DataRecord]:
        """DataRecord to record data"""
        return self._record

    @record.setter
    def record(self, record: DataRecord) -> None:
        if not isinstance(record, DataRecord):
            raise TypeError(f'Invalid DataRecord: {type(record)}')
        for col in self._columns:
            key = col['name']
            if not record.has_column(key):
                raise ValueError(f'Record {record.name} has no column {key}')
        self._record = record

    def prepare_record(self, record_name: str, rebuild: bool = False) -> None:
        """
        Prepare record with given name

        New record is built only when current record is None or "rebuild" flag
        is True, if the data group has been given, the new generated record
        will be add to group automatically.
        """
        if self._record is None or rebuild:
            self._record = DataRecord(record_name, self._columns)
            if isinstance(self.data_group, DataGroup):
                self.data_group.add_record(self._record)

    async def body(self) -> None:
        delay = self.delay_begin()
        if delay > 0.0:
            await asyncio.sleep(delay)
        values = {'timestamp': datetime.now().timestamp() - self.t0()}
        dryrun = self.is_dryrun()
        if len(self._setters) > 0 and not dryrun:
            if isinstance(self._hook_before_set, Callable):
                self._hook_before_set()
            for key, para in self._setters.items():
                attr = self.attribute(key)
                para(attr())
                values[key] = attr()
            if isinstance(self._hook_after_set, Callable):
                self._hook_after_set()
            delay = self.delay_after_set()
            if delay > 0.0:
                await asyncio.sleep(delay)
        if len(self._getters) > 0 and not dryrun:
            if isinstance(self._hook_before_get, Callable):
                self._hook_before_get()
            for key, para in self._getters.items():
                values[key] = para()
            if isinstance(self._hook_after_get, Callable):
                self._hook_after_get()
        if isinstance(self._record, DataRecord) and not dryrun:
            self._record.add_rows(values)
        delay = self.delay_end()
        if delay > 0.0:
            await asyncio.sleep(delay)


class AtomJobSweeper(SweepProcess):
    """
    Abstract sweeper of an atom job

    The parameters to initialize an atom job and to control sweeping process
    are given at initialization:
    - setters --- sequence of setter info
    - getters --- sequence of getter info
    - delay_begin --- delay time before the whole process
    - delay_after_set --- delay time after each setting, ignored if no setting
    - delay_gap --- delay time between two executions of job
    - delay_end --- delay time after the whole process
    - hook_before_set --- hook function before setting, ignored if no setting
    - hook_after_set --- hook function after setting, ignored if no setting
    - hook_before_get --- hook function before getting, ignored if no getting
    - hook_after_get --- hook function after getting, ignored if no getting
    - name --- optional process name

    Note that, "delay_begin" and "delay_end" don't corresponds to the delays
    with same names in atom job. They, along with "delay_gap", are used to
    control the whole sweep.

    The sweeping process is controlled by ``reset_sweep`` and ``adapt`` methods,
    which must be implemented in derived classes.

    The atom job can be accessed by ``child`` property and its ``setters``,
    ``getters`` and ``record`` properites can also be directly accessed via
    sweeper itself.

    Another property is the boolean variable ``sweeping`` indicating
    whether the sweeping procedure is ongoing.
    """

    def __init__(self,
                 setters: SetterSeq = [],
                 getters: GetterSeq = [],
                 delay_begin: float = 0.0,
                 delay_after_set: float = 0.0,
                 delay_gap: float = 0.0,
                 delay_end: float = 0.0,
                 hook_before_set: Optional[Callable] = None,
                 hook_after_set: Optional[Callable] = None,
                 hook_before_get: Optional[Callable] = None,
                 hook_after_get: Optional[Callable] = None,
                 name: Optional[str] = None) -> None:
        job_name: Optional[str] = None
        if isinstance(name, str) and len(name) > 0:
            job_name = f'{name}_job'
        super().__init__(
            WrappedSweeper(self._reset_sweep, self._perform_sweep),
            AtomJob(setters,
                    getters,
                    delay_after_set=delay_after_set,
                    hook_before_set=hook_before_set,
                    hook_after_set=hook_after_set,
                    hook_before_get=hook_before_get,
                    hook_after_get=hook_after_get,
                    name=job_name),
            name,
        )
        self.add_attribute('t0', ValNumber(), 0.0)
        if not isinstance(delay_begin, float) or delay_begin < 0.0:
            delay_begin = 0.0
        self.add_attribute('delay_begin', ValNumber(0.0), delay_begin)
        if not isinstance(delay_after_set, float) or delay_after_set < 0.0:
            delay_after_set = 0.0
        self.add_attribute('delay_after_set', ValNumber(0.0), delay_after_set)
        if not isinstance(delay_gap, float) or delay_gap < 0.0:
            delay_gap = 0.0
        self.add_attribute('delay_gap', ValNumber(0.0), delay_gap)
        if not isinstance(delay_end, float) or delay_end < 0.0:
            delay_end = 0.0
        self.add_attribute('delay_end', ValNumber(0.0), delay_end)
        self._sweeping = False

    @property
    def sweeping(self) -> bool:
        """Whether the sweeping procedure is ongoing"""
        return self._sweeping

    @property
    def setters(self) -> Dict[str, Parameter]:
        """Dictionary of key and parameter of setters, read-only"""
        return self.child._setters

    @property
    def getters(self) -> Dict[str, Parameter]:
        """Dictionary of key and parameter of getters, read-only"""
        return self.child._getters

    @property
    def record(self) -> Optional[DataRecord]:
        """DataRecord to record data"""
        return self.child._record

    @record.setter
    def record(self, record: DataRecord) -> None:
        self.child._record = record

    def prepare_record(self, record_name: str, rebuild: bool = False) -> None:
        """
        Prepare record with given name

        New record is built only when current record is None or "rebuild" flag
        is True, if the data group has been given, the new generated record
        will be add to group automatically.
        """
        self.child.prepare_record(record_name, rebuild)

    @abstractmethod
    def reset_sweep(self) -> None:
        """Reset sweeping, need implementation"""
        raise NotImplementedError

    @abstractmethod
    def adapt(self, record: Optional[DataRecord]) -> ValueDict:
        """
        Adapt values of setters, return empty dictionary if sweeping ends,
        need implementation
        """
        raise NotImplementedError

    def _reset_sweep(self) -> None:
        self.child.t0(self.t0())
        self.child.delay_begin(self.delay_begin())
        self.child.delay_after_set(self.delay_after_set())
        self.child.delay_end(0.0)
        self.child.is_dryrun(False)
        self._sweeping = False
        self.reset_sweep()

    def _perform_sweep(self, job: AtomJob) -> bool:
        if job.is_dryrun():
            self._sweeping = False
            return False
        values = self.adapt(job.record)
        if len(values) > 0:
            for key, value in values.items():
                attr = job.attribute(key)
                if isinstance(attr, LimitedAttribute):
                    attr(value)
            if self._sweeping:
                job.delay_begin(self.delay_gap())
            else:
                self._sweeping = True
        else:
            job.delay_begin(0.0)
            job.delay_end(self.delay_end())
            job.is_dryrun(True)
        return True


class Counter(AtomJobSweeper):
    """
    A counter measures given parameters given times and records result
    if record property is set.

    Arguments:
    - getters --- sequence of getter info, non-empty
    - times --- measure times, at least 1
    - delay_begin --- delay time before the whole process
    - delay_gap --- delay time between two measurements
    - delay_end --- delay time after the whole process
    - hook_before_get --- hook function before getting
    - hook_after_get --- hook function after getting
    - name --- optional process name

    ``times`` can also be accessed and modified after initialization as an
    attribute of counter. The property ``index`` indicates current index of
    counting, 0 (not begin) ~ times (last one has been triggered).
    """

    def __init__(self,
                 getters: ParameterSet,
                 times: int = 1,
                 delay_begin: float = 0,
                 delay_gap: float = 0,
                 delay_end: float = 0,
                 hook_before_get: Optional[Callable] = None,
                 hook_after_get: Optional[Callable] = None,
                 name: Optional[str] = None) -> None:
        super().__init__(getters=parse_getters(getters),
                         delay_begin=delay_begin,
                         delay_gap=delay_gap,
                         delay_end=delay_end,
                         hook_before_get=hook_before_get,
                         hook_after_get=hook_after_get,
                         name=name)
        if len(self.child.getters) < 1:
            raise ValueError('No getter in count process')
        if not isinstance(times, int):
            raise TypeError(f'Invalid times type: {type(times)}')
        elif times < 1:
            raise ValueError(f'Invalid times {times}')
        self.add_attribute('times', ValInt(1), times)
        self._index = 0

    @property
    def index(self) -> int:
        """Current index of counting"""
        return self._index

    def reset_sweep(self) -> None:
        self._index = 0

    def adapt(self, _: Optional[DataRecord]) -> ValueDict:
        if self._index < self.times():
            self._index += 1
            return {'running': True}
        else:
            return {}


def count(name: Optional[str] = None,
          group: Optional[DataGroup] = None,
          record: Optional[DataRecord] = None,
          *args, **kwargs) -> Process:
    """
    Convinent function to generate a counter process

    Arguments are split into 3 parts:
    - Optional process name, using data group and data record, if record is
      None, the function will let the process to prepare itself.
    - Non-keyword arguments to put any parameters to count, following
      key-parameter pattern, where key can be ignored, such as
      <key1>, <parameter1>, <parameter2>, ...
    - Keyword arguments to pass other necessary parameter of ``Counter``, such
      as "times", "delay_begin", "delay_gap", "delay_end", "hook_before_get" and
      "hook_after_get".
    """
    if 'getters' in kwargs:
        proc = Counter(name=name, **kwargs)
    else:
        getters: list = []
        key: str = ''
        for arg in args:
            if isinstance(arg, Parameter):
                if len(key) > 0:
                    getters.append((key, arg))
                else:
                    getters.append((arg.name, arg))
                key = ''
            elif isinstance(arg, str) and len(key) == 0:
                key = arg
            else:
                raise ValueError(f'Invalid argument: {arg}')
        proc = Counter(name=name, getters=getters, **kwargs)
    _assign_group_and_record(
        proc, group, record,
        name if isinstance(name, str) and len(name) > 0 else 'count')
    return proc


class Scanner(AtomJobSweeper):
    """
    A scanner sweeps setters through given values and measures getters after
    each setting, recording both setting and getting if record property is set.

    Arguments:
    - setters --- sequence of setter info, non-empty
    - values --- sequence of values to sweep, matching setters, non-empty
    - getters --- sequence of getter info
    - delay_begin --- delay time before the whole process
    - delay_after_set --- delay time after each setting
    - delay_gap --- delay time between two executions of job
    - delay_end --- delay time after the whole process
    - hook_before_set --- hook function before setting
    - hook_after_set --- hook function after setting
    - hook_before_get --- hook function before getting, ignored if no getting
    - hook_after_get --- hook function after getting, ignored if no getting
    - name --- optional process name

    ``values`` can also be accessed and modified after initialization as a
    property. The total times of sweeping can be got as length of scanner and
    the property ``index`` indicates current index of scanning.
    """

    def __init__(self,
                 setters: ParameterSet,
                 values: Sequence[ValueDict],
                 getters: ParameterSet = [],
                 delay_begin: float = 0,
                 delay_after_set: float = 0,
                 delay_gap: float = 0,
                 delay_end: float = 0,
                 hook_before_set: Optional[Callable] = None,
                 hook_after_set: Optional[Callable] = None,
                 hook_before_get: Optional[Callable] = None,
                 hook_after_get: Optional[Callable] = None,
                 name: Optional[str] = None) -> None:
        super().__init__(parse_setters_with_values(setters, values[0]),
                         parse_getters(getters),
                         delay_begin,
                         delay_after_set,
                         delay_gap,
                         delay_end,
                         hook_before_set,
                         hook_after_set,
                         hook_before_get,
                         hook_after_get,
                         name)
        self._values = values
        self._index = 0

    @property
    def values(self) -> Sequence[ValueDict]:
        """Sequence of values to sweep, matching setters, non-empty"""
        return self._values

    @values.setter
    def values(self, value_seq: Sequence[ValueDict]) -> None:
        if not self.sweeping and isinstance(value_seq, Sequence) and \
                len(value_seq) > 0:
            self._values = value_seq
            self._index = 0

    @property
    def index(self) -> int:
        """Current index of scanning"""
        return self._index

    def __len__(self) -> int:
        """Total times to scan"""
        return len(self._values)

    def reset_sweep(self) -> None:
        self._index = 0

    def adapt(self, _: Optional[DataRecord]) -> ValueDict:
        if self._index < len(self._values):
            self._index += 1
            return self._values[self._index - 1]
        else:
            return {}


def scan(name: Optional[str] = None,
         getters: ParameterSet = [],
         group: Optional[DataGroup] = None,
         record: Optional[DataRecord] = None,
         *args, **kwargs) -> Process:
    """
    Convinent function to generate a scanner process

    Arguments are split into 3 parts:
    - Optional process name, sequence of getter info, using data group and
      data record, if record is None, the function will let the process to
      prepare itself.
    - Non-keyword arguments to put any parameters to scan, following
      key-parameter-values pattern, where key can be ignored, such as
      <key1>, <parameter1>, <values1>, <parameter2>, <values2> ... Note that
      every "values" is a non-empty sequence and all values must have the
      same size.
    - Keyword arguments to pass other necessary parameter of ``Scanner``, such
      as "delay_begin", "delay_after_set", "delay_gap", "delay_end",
      "hook_before_set", "hook_after_set", "hook_before_get" and
      "hook_after_get".
    """
    if 'setters' in kwargs and 'values' in kwargs:
        proc = Scanner(name=name, getters=getters, **kwargs)
    else:
        key: str = ''
        para: Optional[Parameter] = None
        setters: list = []
        values: list = []
        count = 0
        for arg in args:
            if isinstance(arg, str) and len(key) == 0:
                key = arg
            elif isinstance(arg, Parameter) and para is None:
                para = arg
                if len(key) == 0:
                    key = arg.name
            elif isinstance(arg, Sequence) and isinstance(para, Parameter):
                if len(arg) == 0:
                    raise ValueError(f'Empty value sequence for {key}')
                elif count > 0 and len(arg) != count:
                    raise ValueError(
                        f'Value count {arg} of {key} doesn\'t match {count}')
                if count == 0:
                    count = len(arg)
                    values = [{} for _ in range(count)]
                setters.append((key, para))
                for i in range(count):
                    values[i][key] = arg[i]
                key = ''
                para = None
            else:
                raise ValueError('Invalid setting of setters')
        if count == 0:
            raise ValueError('No setter info')
        proc = Scanner(setters, values, getters, name=name, **kwargs)
    _assign_group_and_record(
        proc, group, record,
        name if isinstance(name, str) and len(name) > 0 else 'scan')
    return proc


class GridScanner(AtomJobSweeper):
    """
    A grid scanner sweeps setters through any combination of given values
    and measures getters after each setting, recording both setting and getting
    if record property is set.

    Arguments:
    - setters --- sequence of setter info, non-empty
    - grid --- sequence of key-values tuples, matching setters, non-empty
    - getters --- sequence of getter info
    - delay_begin --- delay time before the whole process
    - delay_after_set --- delay time after each setting
    - delay_gap --- delay time between two executions of job
    - delay_end --- delay time after the whole process
    - hook_before_set --- hook function before setting
    - hook_after_set --- hook function after setting
    - hook_before_get --- hook function before getting, ignored if no getting
    - hook_after_get --- hook function after getting, ignored if no getting
    - name --- optional process name

    The value sequence of each setter can has different but non-zero size.
    The whole scanning can be measured as a tuple of sizes of all values.
    Such tuple can be accessed by read-only property ``shape`` and current
    scanning step is indicated by read-only property ``index``, which is also a
    tuple with the same size of ``shape``.

    ``grid`` can also be accessed and modified after initialization as a
    property.
    """

    def __init__(self,
                 setters: ParameterSet,
                 grid: Sequence[Tuple[str, Sequence[Any]]],
                 getters: ParameterSet = [],
                 delay_begin: float = 0,
                 delay_after_set: float = 0,
                 delay_gap: float = 0,
                 delay_end: float = 0,
                 hook_before_set: Optional[Callable] = None,
                 hook_after_set: Optional[Callable] = None,
                 hook_before_get: Optional[Callable] = None,
                 hook_after_get: Optional[Callable] = None,
                 name: Optional[str] = None) -> None:
        super().__init__(
            parse_setters_with_values(setters, dict(map(
                lambda pair: (pair[0], pair[1][0]), grid
            ))),
            parse_getters(getters),
            delay_begin,
            delay_after_set,
            delay_gap,
            delay_end,
            hook_before_set,
            hook_after_set,
            hook_before_get,
            hook_after_get,
            name)
        if not GridScanner._check_grid(grid):
            raise ValueError('Empty value grid to scan')
        self._grid = grid
        self._shape = tuple(map(lambda pair: len(pair[1]), grid))
        self._index = [0] * (len(grid) + 1)

    @property
    def grid(self) -> Sequence[Tuple[str, Sequence[Any]]]:
        """Sequence of key-values tuples, matching setters, non-empty"""
        return self._grid

    @grid.setter
    def grid(self, values: Sequence[Tuple[str, Sequence[Any]]]) -> None:
        if not self.sweeping and GridScanner._check_grid(values):
            self._grid = values
            self._shape = tuple(map(lambda pair: len(pair[1]), values))
            self._index = [0] * (len(values) + 1)

    @property
    def shape(self) -> Tuple[int]:
        """Tuple of sizes of all values"""
        return self._shape

    @property
    def index(self) -> Tuple[int]:
        """Current scanning step, tuple with the same size of `shape`"""
        return tuple(self._index[:-1])

    def reset_sweep(self) -> None:
        for i in range(len(self._index)):
            self._index[i] = 0

    def adapt(self, _: Optional[DataRecord]) -> ValueDict:
        if self._index[-1] == 0:
            values: ValueDict = {}
            for i in range(len(self._grid)):
                values[self._grid[i][0]] = self.grid[i][1][self._index[i]]
            for i in range(len(self._index)):
                self._index[i] += 1
                if i >= len(self._shape):
                    break
                elif self._index[i] < self._shape[i]:
                    break
                self._index[i] = 0
            return values
        else:
            return {}

    def _check_grid(values: Sequence[Tuple[str, Sequence[Any]]]) -> bool:
        if not isinstance(values, Sequence) or len(values) == 0:
            return False
        else:
            for e in values:
                if not isinstance(e, Tuple) or len(e) != 2 or \
                   not isinstance(e[0], str) or len(e[0]) == 0 or \
                   not isinstance(e[1], Sequence) or len(e[1]) == 0:
                    return False
        return True


def grid_scan(name: Optional[str] = None,
              getters: ParameterSet = [],
              group: Optional[DataGroup] = None,
              record: Optional[DataRecord] = None,
              *args, **kwargs) -> Process:
    """
    Convinent function to generate a grid scanner process

    Arguments are split into 3 parts:
    - Optional process name, sequence of getter info, using data group and
      data record, if record is None, the function will let the process to
      prepare itself.
    - Non-keyword arguments to put any parameters to scan, following
      key-parameter-values pattern, where key can be ignored, such as
      <key1>, <parameter1>, <values1>, <parameter2>, <values2> ... Note that
      every "values" is a non-empty sequence and they can have different sizes.
    - Keyword arguments to pass other necessary parameter of ``GridScanner``,
      such as "delay_begin", "delay_after_set", "delay_gap", "delay_end",
      "hook_before_set", "hook_after_set", "hook_before_get" and
      "hook_after_get".

    Note that "values" in non-keyword arguments should be Sequence type, if the
    users create them by using numpy ndarrays, should transform them into lists
    first (call ``tolist()``).
    """
    key: str = ''
    para: Optional[Parameter] = None
    setters: list = []
    values: list = []
    for arg in args:
        if isinstance(arg, str) and len(key) == 0:
            key = arg
        elif isinstance(arg, Parameter) and para is None:
            para = arg
            if len(key) == 0:
                key = arg.name
        elif isinstance(arg, Sequence) and isinstance(para, Parameter):
            if len(arg) == 0:
                raise ValueError(f'Empty value sequence for {key}')
            setters.append((key, para))
            values.append((key, arg))
            key = ''
            para = None
        else:
            raise ValueError('Invalid setting of setters')
    if len(setters) == 0:
        raise ValueError('No setter info')
    proc = GridScanner(setters, values, getters, name=name, **kwargs)
    _assign_group_and_record(
        proc, group, record,
        name if isinstance(name, str) and len(name) > 0 else 'grid_scan')
    return proc


def _assign_group_and_record(proc: AtomJobSweeper,
                             group: Optional[DataGroup] = None,
                             record: Optional[DataRecord] = None,
                             record_name: str = 'sweeper'):
    if isinstance(group, DataGroup):
        proc.data_group = group
    if isinstance(record, DataRecord):
        proc.record = record
    else:
        proc.prepare_record(record_name)
