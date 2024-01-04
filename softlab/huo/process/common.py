from abc import abstractmethod
from typing import (
    Any,
    Dict,
    Optional,
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
    ProcessSweeper,
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

SetterSeq = Sequence[Tuple[str, Parameter, Any]]

GetterSeq = Sequence[Tuple[str, Parameter]]

ValueDict = Dict[str, Any]

class AtomJob(SimpleProcess):

    def __init__(self,
                 setters: SetterSeq = [],
                 getters: GetterSeq = [],
                 delay_begin: float = 0.0,
                 delay_after_set: float = 0.0,
                 delay_end: float = 0.0,
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
        self._columns = [{'name': 'timestamp', 'unit': 's', 'dependent': False}]
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
                    para = setter[1]
                    if len(key) > 0 and isinstance(para, Parameter):
                        self._getters[key] = para
                        self._columns.append({
                            'name': key,
                            'dependent': True,
                        })
        self._record: Optional[DataRecord] = None

    @property
    def setters(self) -> Dict[str, Parameter]:
        return self._setters

    @property
    def getters(self) -> Dict[str, Parameter]:
        return self._getters

    @property
    def record(self) -> Optional[DataRecord]:
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
            for key, para in self._setters.items():
                attr = self.attribute(key)
                para(attr())
                values[key] = attr()
            delay = self.delay_after_set()
            if delay > 0.0:
                await asyncio.sleep(delay)
        if len(self._getters) > 0 and not dryrun:
            for key, para in self._getters.items():
                values[key] = para()
        if isinstance(self._record, DataRecord) and not dryrun:
            self._record.add_rows(values)
        delay = self.delay_end()
        if delay > 0.0:
            await asyncio.sleep(delay)

class AtomJobSweeper(SweepProcess):

    def __init__(self,
                 setters: SetterSeq = [],
                 getters: GetterSeq = [],
                 delay_begin: float = 0.0,
                 delay_after_set: float = 0.0,
                 delay_gap: float = 0.0,
                 delay_end: float = 0.0,
                 name: Optional[str] = None) -> None:
        super().__init__(
            WrappedSweeper(self.reset_sweep, self.perform_sweep),
            AtomJob(setters,
                    getters,
                    delay_after_set=delay_after_set,
                    name=f'{name}_job' if isinstance(name, str) and \
                        len(name) > 0 else None),
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
        return self._sweeping

    @property
    def record(self) -> Optional[DataRecord]:
        return self.child._record

    @record.setter
    def record(self, record: DataRecord) -> None:
        self.child._record = record

    def prepare_record(self, record_name: str, rebuild: bool = False) -> None:
        self.child.prepare_record(record_name, rebuild)

    @abstractmethod
    def reset(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def adaptive(self, record: Optional[DataRecord]) -> ValueDict:
        raise NotImplementedError

    def reset_sweep(self) -> None:
        self.child.t0(self.t0())
        self.child.delay_begin(self.delay_begin())
        self.child.delay_after_set(self.delay_after_set())
        self.child.delay_end(0.0)
        self.child.is_dryrun(False)
        self._sweeping = False
        self.reset()

    def perform_sweep(self, job: AtomJob) -> bool:
        if job.is_dryrun():
            return False
        values = self.adaptive(job.record)
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

def parse_getters(getters: ParameterSet) -> GetterSeq:
    if isinstance(getters, Sequence) and len(getters) > 0:
        if isinstance(getters[0], Tuple):
            return getters
        return list(map(lambda para: (para.name, para), getters))
    elif isinstance(getters, Dict):
        return getters.items()
    return []

def parse_setters(setters: ParameterSet) -> SetterSeq:
    if isinstance(setters, Sequence) and len(setters) > 0:
        if isinstance(setters[0], Tuple):
            return list(map(lambda key, para: (key, para, para()), setters))
        return list(map(lambda para: (para.name, para, para()), setters))
    elif isinstance(setters, Dict):
        return list(map(lambda key, para: (key, para, para()), setters.items()))
    return []

def parse_setters_with_values(
        setters: ParameterSet, values: ValueDict) -> SetterSeq:
    if isinstance(setters, Sequence) and len(setters) > 0:
        if isinstance(setters[0], Tuple):
            return list(map(
                lambda key, para: (key,
                                   para,
                                   values[key] if key in values else para()),
                setters))
        return list(map(
            lambda para: (
                para.name,
                para,
                values[para.name] if para.name in values else para()),
            setters))
    elif isinstance(setters, Dict):
        return list(map(
            lambda key, para: (key,
                               para,
                               values[key] if key in values else para()),
            setters.items()))
    return []

class Counter(AtomJobSweeper):

    def __init__(self,
                 getters: ParameterSet,
                 times: int = 1,
                 delay_begin: float = 0,
                 delay_gap: float = 0,
                 delay_end: float = 0,
                 name: Optional[str] = None) -> None:
        super().__init__(getters=parse_getters(getters),
                         delay_begin=delay_begin,
                         delay_gap=delay_gap,
                         delay_end=delay_end,
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
        return self._index

    def reset(self) -> None:
        self._index = 0

    def adaptive(self, _: Optional[DataRecord]) -> ValueDict:
        if self._index < self.times():
            self._index += 1
            return {'running': True}
        else:
            return {}

class Scanner(AtomJobSweeper):

    def __init__(self,
                 setters: ParameterSet,
                 values: Sequence[ValueDict],
                 getters: ParameterSet = [],
                 delay_begin: float = 0,
                 delay_after_set: float = 0,
                 delay_gap: float = 0,
                 delay_end: float = 0,
                 name: Optional[str] = None) -> None:
        super().__init__(parse_setters_with_values(setters, values[0]),
                         parse_getters(getters),
                         delay_begin,
                         delay_after_set,
                         delay_gap,
                         delay_end,
                         name)
        self._values = values
        self._index = 0

    @property
    def values(self) -> Sequence[ValueDict]:
        return self._values

    @values.setter
    def values(self, value_seq: Sequence[ValueDict]) -> None:
        if not self.sweeping and isinstance(value_seq, Sequence) and \
                len(value_seq) > 0:
            self._values = value_seq
            self._index = 0

    @property
    def index(self) -> int:
        return self._index

    def __len__(self) -> int:
        return len(self._values)

    def reset(self) -> None:
        self._index = 0

    def adaptive(self, _: Optional[DataRecord]) -> ValueDict:
        if self._index < len(self._values):
            self._index += 1
            return self._values[self._index - 1]
        else:
            return {}

class GridScanner(AtomJobSweeper):

    def __init__(self,
                 setters: ParameterSet,
                 values: Sequence[Tuple[str, Sequence[Any]]],
                 getters: ParameterSet = [],
                 delay_begin: float = 0,
                 delay_after_set: float = 0,
                 delay_gap: float = 0,
                 delay_end: float = 0,
                 name: Optional[str] = None) -> None:
        super().__init__(
            parse_setters_with_values(setters, dict(map(
                lambda key, seq: (key, seq[0]), values
            ))),
            parse_getters(getters),
            delay_begin,
            delay_after_set,
            delay_gap,
            delay_end,
            name)
        if not GridScanner._check_grid(values):
            raise ValueError('Empty value grid to scan')
        self._grid = values
        self._shape = tuple(map(lambda _, seq: len(seq), values))
        self._index = [0] * (len(values) + 1)

    @property
    def grid(self) -> Sequence[Tuple[str, Sequence[Any]]]:
        return self._grid

    @grid.setter
    def grid(self, values: Sequence[Tuple[str, Sequence[Any]]]) -> None:
        if not self.sweeping and GridScanner._check_grid(values):
            self._grid = values
            self._shape = tuple(map(lambda _, seq: len(seq), values))
            self._index = [0] * (len(values) + 1)

    @property
    def shape(self) -> Tuple[int]:
        return self._shape

    @property
    def index(self) -> Tuple[int]:
        return tuple(self._index[:-1])

    def reset(self) -> None:
        for i in range(len(self._index)):
            self._index[i] = 0

    def adaptive(self, _: Optional[DataRecord]) -> ValueDict:
        if self._index[-1] == 0:
            values: ValueDict = {}
            for i in range(len(self._grid)):
                values[self._grid[i][0]] = self.grid[i][1][self._index[i]]
            for i in range(len(self._index)):
                self._index[i] += 1
                if self._index[i] < self._shape[i]:
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
