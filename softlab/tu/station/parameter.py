"""Parameter interface"""

from abc import abstractmethod
from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Callable,
    Set,
)
import time
import warnings
from softlab.jin.validator import (
    Validator,
    ValNumber,
    ValAnything,
)
import math


def _validate_metadata(metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validate and recursively copy user metadata for descriptions.

    Only exact built-in ``dict`` (top level), ``dict``, ``list``, ``str``,
    ``int``, ``float``, ``bool`` and None values are accepted, and dict
    keys must be exact ``str``. Boolean is checked before integer since
    ``bool`` subclasses ``int``. Floats must be finite.

    Args:
    - metadata --- user metadata mapping, None means an empty mapping

    Returns:
    - a detached dict, recursively copied so later mutation of the
      caller's metadata cannot affect the returned description

    Errors:
    - TypeError --- if ``metadata`` or a nested container has an
      unsupported type, or a dict key is not an exact ``str``
    - ValueError --- if a float is NaN or infinite, or a container
      refers to itself on the active recursion path

    Side-effects: none; no ``str``, ``repr`` or JSON fallback of any
    user value is invoked.
    """

    def copy(value: Any, path: str, active: Set[int]) -> Any:
        # immutable scalars; check bool before int (bool subclasses int)
        if type(value) is bool or value is None or type(value) is str:
            return value
        if type(value) is int:
            return value
        if type(value) is float:
            if not math.isfinite(value):
                raise ValueError(
                    f'Non-finite float in metadata at {path}')
            return value
        if type(value) is dict:
            if id(value) in active:
                raise ValueError(
                    f'Cyclic metadata container at {path}')
            active.add(id(value))
            result = {}
            for key, item in value.items():
                if type(key) is not str:
                    raise TypeError(
                        f'Non-string metadata key at {path}')
                result[key] = copy(item, f'{path}.{key}', active)
            active.discard(id(value))
            return result
        if type(value) is list:
            if id(value) in active:
                raise ValueError(
                    f'Cyclic metadata container at {path}')
            active.add(id(value))
            result = [
                copy(item, f'{path}[{index}]', active)
                for index, item in enumerate(value)
            ]
            active.discard(id(value))
            return result
        raise TypeError(
            f'Unsupported metadata type {type(value).__name__} '
            f'at {path}')

    if metadata is None:
        return {}
    if type(metadata) is not dict:
        raise TypeError(
            f'Metadata must be a dict, got {type(metadata).__name__}')
    return copy(metadata, 'metadata', set())


@dataclass(frozen=True)
class Reading():
    """
    Immutable record of the outcome of one ``Parameter.read()`` call.

    A reading is a frozen value object: attribute rebinding raises
    ``dataclasses.FrozenInstanceError``. Field values are not copied or
    deeply frozen --- ``value`` is the identical object the acquisition
    chain returned, echoed by reference.

    ``quality`` is a closed vocabulary: exactly ``'ok'`` (acquisition
    chain completed) or ``'failed'`` (acquisition chain raised). No
    other value is defined.

    On a failed read, ``value`` is ``None`` (no acquisition completed),
    ``acquired_at`` is ``None`` (no completion time exists), ``error``
    holds the original exception object (identity preserved, never
    wrapped), and the declared ``uncertainty``/``calibration`` are still
    populated from the parameter's constructor declarations.

    Serialization limit: this class defines no ``__iter__``, no
    ``to_json`` and no JSON fallback of any kind. ``json.dumps`` of a
    reading raises ``TypeError`` under the standard encoder --- an
    explicit, loud outcome, never a restriction on what may be stored.
    Note that the generated dataclass ``__repr__`` and ``__hash__``
    touch field values, so ``repr(reading)`` may raise for opaque values
    and hashability is value-dependent.

    Fields:
    - value --- the exact object ``get()`` returned (identity, never
      copied); ``None`` on a failed read
    - acquired_at --- epoch seconds of acquisition-chain completion,
      stamped after the chain returns; ``None`` on a failed read
    - quality --- ``'ok'`` or ``'failed'`` (closed vocabulary)
    - error --- the original exception object on failure, else ``None``
    - uncertainty --- declared uncertainty of the parameter, else
      ``None``
    - calibration --- declared calibration reference of the parameter,
      else ``None``
    """

    value: Any
    acquired_at: Optional[float]
    quality: str
    error: Optional[Exception]
    uncertainty: Optional[Any]
    calibration: Optional[Any]


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
    - read --- opt-in measurement read returning a ``Reading`` with
               acquisition time, quality and the original failure
               object, distinct from the legacy value-only ``get()``/
               ``__call__`` path
    - describe_reading --- opt-in versioned description of the declared
                           measurement metadata, in its own schema
                           namespace, never touching the stored value

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
                 before_get: Optional[Callable[[Any], Any]] = None,
                 unit: Optional[str] = None,
                 value_type: Optional[str] = None,
                 shape: Optional[List[int]] = None,
                 channel: Optional[str] = None,
                 uncertainty: Optional[Any] = None,
                 calibration: Optional[Any] = None) -> None:
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
        - unit --- physical unit declaration, optional, default None
        - value_type --- value type declaration, optional, default None
        - shape --- value shape declaration, optional, default None
        - channel --- channel declaration, optional, default None
        - uncertainty --- uncertainty declaration, optional, default None
        - calibration --- calibration reference declaration, optional,
          default None

        The six measurement keywords (``unit``, ``value_type``,
        ``shape``, ``channel``, ``uncertainty``, ``calibration``) are
        descriptive declarations only: they are stored verbatim, with no
        validation and no copying, and are echoed by reference in
        ``describe_reading()`` and on ``Reading.uncertainty`` /
        ``Reading.calibration``. Callers must treat them as read-only.

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
        # declared measurement metadata, stored verbatim, no validation,
        # no copying; echoed by reference and documented as read-only
        self._unit: Optional[str] = unit
        self._value_type: Optional[str] = value_type
        self._shape: Optional[List[int]] = shape
        self._channel: Optional[str] = channel
        self._uncertainty: Optional[Any] = uncertainty
        self._calibration: Optional[Any] = calibration

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

    def describe(self,
                 metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get a portable, JSON-compatible description of this parameter.

        The description contains only structure and access permissions:
        ``schema_version``, ``name``, ``type`` (qualified type string),
        ``metadata``, ``settable`` and ``gettable``. No runtime value,
        validator representation or owner object is included.

        Args:
        - metadata --- optional user metadata mapping attached to this
          node, validated and recursively copied

        Returns:
        - a detached version-1 description dict

        Errors:
        - TypeError --- invalid metadata type or non-string metadata key
        - ValueError --- non-finite float or cyclic metadata container

        Side-effects: none; never reads the stored value and never calls
        ``get``, ``set``, ``snapshot``, validators, codecs or hooks.
        """
        cls = type(self)
        return {
            'schema_version': 1,
            'name': self._name,
            'type': cls.__module__ + '.' + cls.__qualname__,
            'metadata': _validate_metadata(metadata),
            'settable': self._settable,
            'gettable': self._gettable,
        }

    def describe_reading(self) -> Dict[str, Any]:
        """
        Get a versioned description of the declared measurement metadata.

        The result lives in its own schema namespace (``schema_version``
        1, independent of the ``describe()`` v1 and
        ``describe_operation()`` v1 schemas) and is a fresh dict literal
        on every call. The four optional fields ``unit``,
        ``value_type``, ``shape`` and ``channel`` are ``None`` when
        undeclared. Declared values are echoed verbatim by reference
        (``shape`` in particular) and must be treated as read-only.

        Returns:
        - a fresh version-1 reading-description dict

        Side-effects: zero device I/O; never reads the stored value;
        never invokes validators, codecs, hooks, or ``str``/``repr`` on
        any runtime value.
        """
        return {
            'schema_version': 1,
            'name': self._name,
            'unit': self._unit,
            'value_type': self._value_type,
            'shape': self._shape,
            'channel': self._channel,
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

    def read(self) -> Reading:
        """
        Opt-in richer read, returning a ``Reading`` of one acquisition.

        Performs exactly one acquisition through the legacy ``get()``
        chain (permission check, ``before_get`` hook, encoder), so
        acquisition counts, hook ordering and encoder behavior are
        identical by construction; ``acquired_at`` is stamped with
        ``time.time()`` after the acquisition chain completes. No
        caching is performed: each call acquires afresh.

        Raises ``RuntimeError`` when the parameter is not gettable,
        exactly like ``get()``, before any acquisition is attempted;
        permission denial is a programming error, not an acquisition
        outcome, and never produces a ``Reading``.

        If the acquisition chain raises an ``Exception`` subclass, the
        original exception object is returned on ``reading.error`` with
        ``quality == 'failed'`` instead of propagating; ``BaseException``
        subclasses (e.g. ``KeyboardInterrupt``) propagate unchanged.
        Legacy ``get()`` always propagates the identical exception
        object.

        Returns:
        - a ``Reading`` with the acquired value, completion timestamp,
          quality and the original failure object (on failure)

        Side-effects: exactly those of one legacy ``get()`` call (hook
        invocation, device I/O); none on the permission-denied path.
        """
        if not self.gettable:
            raise RuntimeError(f'Parameter {self.name} is not gettable')
        try:
            value = self.get()
        except Exception as error:
            return Reading(value=None, acquired_at=None, quality='failed',
                           error=error, uncertainty=self._uncertainty,
                           calibration=self._calibration)
        return Reading(value=value, acquired_at=time.time(), quality='ok',
                       error=None, uncertainty=self._uncertainty,
                       calibration=self._calibration)

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

    def read(self) -> Reading:
        """
        Forward the opt-in rich read to the target parameter.

        Returns:
        - the target's own ``Reading`` object, giving value/quality/
          error parity with a direct read of the target

        Errors: permission is enforced on the target side --- the
        target's ``read()`` performs its own gettable pre-check, so a
        denied target raises the target's ``RuntimeError`` through this
        proxy with zero proxy-side acquisition.
        """
        return self._obj.read()


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
