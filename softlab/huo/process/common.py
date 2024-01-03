from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
    Optional,
    Sequence,
)
import asyncio
from datetime import datetime
from softlab.huo.process.simple import SimpleProcess
from softlab.huo.process.composite import SweepProcess
from softlab.shui.data import (
    DataGroup,
    DataRecord,
)
from softlab.tu.station import Parameter
from softlab.jin.validator import (
    ValInt,
    ValNumber,
)

class AtomJob(SimpleProcess):

    def __init__(self, name: Optional[str] = None,
                 setters: Sequence[Tuple[str, Parameter, Any]] = [],
                 getters: Sequence[Tuple[str, Parameter]] = [],
                 delay_begin: float = 0.0,
                 delay_after_set: float = 0.0,
                 delay_end: float = 0.0):
        super().__init__(name)
        self.add_attribute('t0', ValNumber(), 0.0)
        if not isinstance(delay_begin) or delay_begin < 0.0:
            delay_begin = 0.0
        self.add_attribute('delay_begin', ValNumber(0.0), delay_begin)
        if not isinstance(delay_after_set) or delay_after_set < 0.0:
            delay_after_set = 0.0
        self.add_attribute('delay_after_set', ValNumber(0.0), delay_after_set)
        if not isinstance(delay_end) or delay_end < 0.0:
            delay_end = 0.0
        self.add_attribute('delay_end', ValNumber(0.0), delay_end)
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

    async def body(self) -> None:
        delay = self.delay_begin()
        if delay > 0.0:
            await asyncio.sleep(delay)
        values = {'timestamp': datetime.now().timestamp() - self.t0()}
        if len(self._setters) > 0:
            for key, para in self._setters.items():
                attr = getattr(self, key)
                para(attr())
                values[key] = attr()
            delay = self.delay_after_set()
            if delay > 0.0:
                await asyncio.sleep(delay)
        if len(self._getters) > 0:
            for key, para in self._getters.items():
                values[key] = para()
        if isinstance(self._record, DataRecord):
            self._record.add_rows(values)
        delay = self.delay_end()
        if delay > 0.0:
            await asyncio.sleep(delay)
