"""Parameter interface"""

from abc import abstractmethod
from typing import (
    Any,
    Optional,
    Callable,
)
import warnings
from softlab.jin.validator import (
    Validator,
    ValNumber,
    ValAnything,
)
import math


class Parameter():
    """
    Parameter base class

    A parameter represents a single degree of freedom, it can be an attribute
    of a device, a specific measurement or a result of an analysis task.

    There are 5 public properties:
    - name --- non-empty string representing the parameter
    - validator --- description of the validator that guards the input of
                    parameter, read-only
    - settable --- whether the parameter can be set, read-only
    - gettable --- whether the parameter can be get, read-only
    - owner --- the owner object of parameter, e.g. a device or a task

    Public methods:
    - snapshot --- get the snapshot dict of parameter
    - set --- set parameter value
    - get --- get parameter value

    Parameter is callable object, calling without parameter means getting,
    and calling with parameters means setting (only first parameter is used).

    Stored value can be different with accessed one if parsers are given
    during initialization:
    - decoder --- parse setting value into stored value
    - encoder --- parse stored value into output value

    User can also define three hook functions:
    - before_set --- hook function before setting, take two parameters:
                     previous value and next value respectively
    - after_set --- hook function after setting with new value as parameter
    - before_get --- hook function to alter stored value before getting,
                     stored value will be given to such function as parameter
                     and changed due to function's return

    The process of setting:
    - check ``settable`` property, throw RuntimeError if not settable
    - use validator to check input value
    - if decoder is given, decode value
    - call before-setting hook function if exist
    - change stored value
    - call after-setting hook function if exist

    The process of getting:
    - check ``gettable`` property, throw RuntimeError if not gettable
    - if before-getting hook function is given, use it to alter store value
    - if encoder is given, return encoded value, otherwise return stored one
    """

    def __init__(self,
                 name: str,
                 validator: Validator = ValAnything(),
                 settable: bool = True,
                 gettable: bool = True,
                 init_value: Optional[Any] = None,
                 owner: Optional[Any] = None,
                 decoder: Optional[Callable[[Any], Any]] = None,
                 encoder: Optional[Callable[[Any], Any]] = None,
                 before_set: Optional[Callable[[Any, Any], None]] = None,
                 after_set: Optional[Callable[[Any], None]] = None,
                 before_get: Optional[Callable[[Any], Any]] = None) -> None:
        """
        Initialize parameter

        Args:
        - name --- parameter name, non-empty string
        - validator --- validator for inner value, ``Validator`` instance
        - settable --- whether the parameter can be set
        - gettable --- whether the parameter can be get
        - init_value --- initial value, optional
        - owner --- parameter owner, optional
        - decoder --- callable to parse setting value into the stored one
        - encoder --- callable to parse stored value into output one
        - before_set --- hook function before setting, [prev, next] -> None
        - after_set --- hook function after setting
        - before_get --- hook function to alter stored value before getting

        Note: warning if settable and gettable are both False
        """
        self._name = str(name)  # parameter name
        if len(self._name) == 0:
            raise ValueError('Given name is empty')
        if not isinstance(validator, Validator):  # validator
            raise TypeError(f'Invalid validator {type(validator)} for {name}')
        self._validator = validator
        self._settable = settable if isinstance(
            settable, bool) else True  # access
        self._gettable = gettable if isinstance(gettable, bool) else True
        if not self._settable and not self._gettable:
            warnings.warn(
                f'Parameter {self.name} is neither settable and gettable')
        self._value: Any = None  # stored value
        self.owner = owner  # owner
        self._decoder: Optional[Callable] = decoder if isinstance(
            decoder, Callable) else None
        self._encoder: Optional[Callable] = encoder if isinstance(
            encoder, Callable) else None
        self._before_set: Optional[Callable] = before_set if isinstance(
            before_set, Callable) else None
        self._after_set: Optional[Callable] = after_set if isinstance(
            after_set, Callable) else None
        self._before_get: Optional[Callable] = before_get if isinstance(
            before_get, Callable) else None
        if init_value is not None:  # initial value
            if self.settable:
                self.set(init_value)
            else:
                self._validator.validate(init_value)
                self._value = init_value

    @property
    def name(self) -> str:
        """Get parameter name"""
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        """Set parameter name"""
        name = str(name)
        if len(name) > 0:
            self._name = name
        else:
            raise ValueError(f'New name is empty (current name: {self._name})')

    @property
    def validator(self) -> Validator:
        """Get description of validator"""
        return self._validator

    @property
    def settable(self) -> bool:
        """Get whether the paremter can be set"""
        return self._settable

    @property
    def gettable(self) -> bool:
        """Get whether the paremter can be get"""
        return self._gettable

    def __repr__(self) -> str:
        return f'{type(self)}/{self.name}'

    def snapshot(self) -> dict:
        """Get parameter snapshot"""
        return {
            'name': self.name,
            'type': type(self),
            'settable': self.settable,
            'gettable': self.gettable,
            'validator': repr(self.validator),
            'owner': str(self.owner),
        }

    def set(self, value: Any) -> None:
        """Set parameter value"""
        if not self.settable:
            raise RuntimeError(f'Parameter {self.name} is not settable')
        self._validator.validate(value, repr(self))
        if isinstance(self._decoder, Callable):
            value = self._decoder(value)
        if isinstance(self._before_set, Callable):
            self._before_set(self._value, value)
        self._value = value
        if isinstance(self._after_set, Callable):
            self._after_set(self._value)

    def get(self) -> Any:
        """Get parameter value"""
        if not self.gettable:
            raise RuntimeError(f'Parameter {self.name} is not gettable')
        if isinstance(self._before_get, Callable):
            self._value = self._before_get(self._value)
        if isinstance(self._encoder, Callable):
            return self._encoder(self._value)
        return self._value

    def __call__(self, *args: Any) -> Any:
        """Get or set parameter value, makes parameter callable"""
        if len(args) == 0:
            return self.get()  # get value
        else:
            self.set(args[0])  # set value


class QuantizedParameter(Parameter):
    """
    Parameter with quantized inner data

    Additional properties:
    - lsb --- least sigificant bit, aka the step of quantization
    - mode --- quantization mode, round, floor or ceil
    """

    def __init__(self, name: str,
                 settable: bool = True, gettable: bool = True,
                 min: float = 0.0, max: float = 100.0, lsb: float = 1.0,
                 mode: str = 'round',
                 init_value: Optional[float] = None,
                 owner: Optional[Any] = None) -> None:
        """
        Initialization

        Arguments not in super initialization:
        - min --- minimal value
        - max --- maximal value
        - lsb --- least sigificant bit
        - mode --- quantization mode, round, floor or ceil
        """
        super().__init__(name, ValNumber(min, max),
                         settable, gettable, init_value, owner,
                         decoder=self._parse, encoder=self._interprete)
        if not isinstance(min, float) or not isinstance(max, float) or \
                not isinstance(lsb, float):
            raise TypeError(
                f'Invalid type: {type(min)}, {type(max)}, {type(lsb)}')
        if lsb > 0 and max > min and (max-min) > lsb:
            self._lsb = lsb
        else:
            raise ValueError(f'Invalid value: {min}, {max}, {lsb}')
        self._quantizer = round
        if mode == 'floor':
            self._quantizer = math.floor
        elif mode == 'ceil':
            self._quantizer = math.ceil

    @property
    def lsb(self) -> float:
        """Get least sigificant bit"""
        return self._lsb

    @property
    def mode(self) -> str:
        """Get quantization mode"""
        if self._quantizer == math.floor:
            return 'floor'
        elif self._quantizer == math.ceil:
            return 'ceil'
        return 'round'

    def snapshot(self) -> dict:
        s = super().snapshot()
        s['lsb'] = self.lsb
        s['mode'] = self.mode
        return s

    def _parse(self, value: float) -> int:
        return self._quantizer(value / self._lsb)

    def _interprete(self, value: int) -> float:
        return self._lsb * value


class ProxyParameter(Parameter):

    def __init__(self, name: str, obj: Parameter,
                 owner: Optional[Any] = None) -> None:
        if not isinstance(obj, Parameter):
            raise TypeError(f'Invalid paramter to proxy: {type(obj)}')
        super().__init__(name,
                         obj._validator, obj.settable, obj.gettable,
                         owner=owner)
        self._obj = obj

    def set(self, value: Any) -> None:
        self._obj.set(value)

    def get(self) -> Any:
        return self._obj.get()


if __name__ == '__main__':
    from softlab.jin.validator import (
        ValType,
        ValInt,
        ValAnything,
        ValNothing,
        ValPattern,
    )
    for para, val in [
        (Parameter('demo', ValAnything(), init_value=42), 'lab'),
        (Parameter('email1', ValPattern('\w+(\.\w+)*@\w+(\.\w+)+')), 'a@b.com'),
        (Parameter('email2', ValPattern('\w+(\.\w+)*@\w+(\.\w+)+')), 'a_b.com'),
        (Parameter('int', ValInt(0, 100), settable=False, init_value=61), 73),
        (Parameter('percentage', ValInt(0, 100), gettable=False), 73),
        (Parameter('noaccess', ValNothing('test'), False, False), 0),
        (Parameter('bool', ValType(bool), owner='pk'), False),
        (QuantizedParameter('adc', False, lsb=100.0/256, init_value=18), 13.2),
        (QuantizedParameter('dac', gettable=False, lsb=150.0/65536), 89.2),
        (QuantizedParameter('quantizer', min=-20.0, max=20.0, lsb=0.01,
                            owner='pk'), -10.328977345),
        (ProxyParameter('proxy1',
                        Parameter('int1', ValInt(0, 100), gettable=False)), 73),
        (ProxyParameter('proxy2',
                        Parameter('int2', ValInt(0, 100), init_value=50)), 103),
    ]:
        print(f'-------- {para} --------')
        print(para.snapshot())
        print(f'Try set value: {val}')
        try:
            para(val)
        except Exception as e:
            print(e)
        try:
            print(f'Try get value: {para()}')
        except Exception as e:
            print(e)
