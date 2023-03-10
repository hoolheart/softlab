"""Validators for different types of value"""

from typing import (
    Any,
    Sequence,
    Set,
    Union,
)
import numpy as np
import re

class Validator():
    """
    Base class for all validators
    
    Every validator should implement ``validate`` method,
    which checks value validation and raises error if invalid

    Another implementable method is ``__repr__`` which should return
    specific description of validator
    """

    def validate(self, value: Any, context: str = '') -> None:
        raise NotImplementedError
    
    def __repr__(self) -> str:
        return f'{type(self)}'
    
def validate_value(value: Any, validator: Validator, context: str = '') -> bool:
    """Function to validate value by given validator"""
    try:
        validator.validate(value, context)
    except Exception as e:
        print(e)
        return False
    return True
    
class ValidatorAll(Validator):
    """
    Validator requires value satisfying all sub validators

    Args:
        validators --- sequence of sub validators
    """

    def __init__(self, validators: Sequence[Validator]) -> None:
        if not isinstance(validators, Sequence):
            raise TypeError(f'sub validators should be a sequence')
        self._children = tuple(filter(
            lambda v: isinstance(v, Validator), validators
        ))
        if len(self._children) == 0:
            raise ValueError(f'No valid sub validator')
        
    def validate(self, value: Any, context: str = '') -> None:
        for child in self._children:
            child.validate(value, context)

    def __repr__(self) -> str:
        return super().__repr__() + ' ({})'.format(', '.join(map(
            lambda child: repr(child), self._children
        )))
    
class ValidatorAny(Validator):
    """
    Validator requires value satisfying at least one sub validators

    Args:
        validators --- sequence of sub validators
    """

    def __init__(self, validators: Sequence[Validator]) -> None:
        if not isinstance(validators, Sequence):
            raise TypeError(f'sub validators should be a sequence')
        self._children = tuple(filter(
            lambda v: isinstance(v, Validator), validators
        ))
        if len(self._children) == 0:
            raise ValueError(f'No valid sub validator')
        
    def validate(self, value: Any, context: str = '') -> None:
        succ = False
        for child in self._children:
            try:
                child.validate(value, context)
            except Exception:
                continue
            succ = True
            break
        if not succ:
            raise ValueError(
                f'{value} is not accepted by any validators in {context}')

    def __repr__(self) -> str:
        return super().__repr__() + ' ({})'.format(', '.join(map(
            lambda child: repr(child), self._children
        )))
    
class ValAnything(Validator):
    """Validator allows all kind of values"""
    
    def validate(self, value: Any, context: str = '') -> None:
        pass

    def __repr__(self) -> str:
        return super().__repr__() + ' allows any value'
    
class ValNothing(Validator):
    """Validator denies any value with given reason"""

    def __init__(self, reason: str) -> None:
        self._reason = str(reason)

    @property
    def reason(self) -> str:
        return self._reason
    
    @reason.setter
    def reason(self, reason: str) -> None:
        self._reason = str(reason)

    def validate(self, value: Any, context: str = '') -> None:
        raise RuntimeError(f'{self._reason}; {context}')
    
    def __repr__(self) -> str:
        return super().__repr__() + f'({self._reason})'

class ValType(Validator):
    """Validator only accepts given type"""

    def __init__(self, T: Union[type, Sequence[type]]) -> None:
        if T is None:
            raise RuntimeError('A type must be given')
        if isinstance(T, type):
            self._T = T
        elif isinstance(T, Sequence):
            self._T = tuple(map(
                lambda t: t if isinstance(t, type) else type(t),
                T,
            ))
        else:
            self._T = type(T)

    @property
    def valid_type(self) -> type:
        return self._T
    
    def validate(self, value: Any, context: str = '') -> None:
        if not isinstance(value, self._T):
            raise TypeError(
                f'{self._T} value required but {type(value)} in {context}')
        
    def __repr__(self) -> str:
        return f'Validator for {self._T}'
    
class ValString(ValType):
    """
    Validator only accepts string with valid length
    
    Initialization arguments:
        min_length --- minimal limit of string length, default is 0
        max_length --- maximal limit of string length, default is -1 (no limit)

    Raises:
            TypeError --- min_length and/or max_length are not intergers
            ValueError --- max_length is non-negative but < min_length
    
    Properties:
        min_length --- minimal limit of string length
        max_length --- maximal limit of string length
    """

    def __init__(self, min_length: int = 0, max_length: int = -1) -> None:
        super().__init__(str)
        self.set_length_range(min_length, max_length)
        

    @property
    def min_length(self) -> int:
        """Get minimal limit of string length"""
        return self._min
    
    @property
    def max_length(self) -> int:
        """Get maximal limit of string length"""
        return self._max
    
    def set_length_range(
            self, min_length: int = 0, max_length: int = -1) -> None:
        """Set constraints on string length"""
        if not isinstance(min_length, int) or not isinstance(max_length, int):
            raise TypeError(
                f'Not intergers: {type(min_length)} {type(max_length)}')
        self._min = int(min_length)
        self._max = int(max_length)
        if self._min < 0:
            self._min = 0
        if self._max < 0:
            self._max = -1
        elif self._max < self._min:
            raise ValueError(f'max {max_length} < min {min_length}')

    def validate(self, value: Any, context: str = '') -> None:
        super().validate(value, context)
        l = len(value)
        if self._min > 0 and l < self._min:
            raise ValueError(
                f'Require min length {self._min} but {l} in {context}')
        elif self._max >= 0 and l > self._max:
            raise ValueError(
                f'Require max length {self._max} but {l} in {context}')
        
    def __repr__(self) -> str:
        return super().__repr__() + f' ({self._min} ~ {self._max})'
    
class ValPattern(ValType):
    """
    Validator for strings with given pattern

    Args:
        pattern --- the regular expression to define string pattern
    """

    def __init__(self, pattern: str) -> None:
        super().__init__(str)
        if not isinstance(pattern, str) or len(pattern) < 1:
            raise ValueError(f'Invalid pattern {pattern}')
        self._pattern = pattern

    @property
    def pattern(self) -> str:
        return self._pattern
    
    def validate(self, value: Any, context: str = '') -> None:
        super().validate(value, context)
        match = re.match(self._pattern, value)
        if match is None or match.group() != value:
            raise ValueError(
                f'"{value}" dismatches pattern "{self._pattern}" in {context}')

    def __repr__(self) -> str:
        return super().__repr__() + f' (pattern: {self._pattern})'
    
class ValInt(ValType):
    """
    Validator accepts int or np.integer in given range

    Args:
        min --- minimal value
        max --- maximal value
    """

    _BIGGEST = int(0x7fffffff)

    def __init__(self, min: int = -_BIGGEST-1, max: int = _BIGGEST) -> None:
        super().__init__((int, np.integer))
        if not isinstance(min, int) or not isinstance(max, int):
            raise TypeError(
                f'invalid type of boundaries: {type(min)} {type(max)}')
        if min > max:
            raise ValueError(f'Invalid range: {min} ~ {max}')
        self._min = min
        self._max = max

    @property
    def min(self) -> int:
        return self._min
    
    @property
    def max(self) -> int:
        return self._max
    
    def validate(self, value: Any, context: str = '') -> None:
        super().validate(value, context)
        if value < self._min or value > self._max:
            raise ValueError(
                f'{value} out of range [{self._min}, {self._max}] in {context}')
        
    def __repr__(self) -> str:
        return super().__repr__() + f' ({self._min} ~ {self._max})'
    
class ValNumber(ValType):
    """
    Validator accepts all kinds of number value in given range

    Args:
        min --- minimal value
        max --- maximal value
    """

    def __init__(self, 
                 min: float = -float('inf'), 
                 max: float = float('inf')) -> None:
        super().__init__((int, float, np.integer, np.floating))
        if not isinstance(min, float) or not isinstance(max, float):
            raise TypeError(
                f'invalid type of boundaries: {type(min)} {type(max)}')
        if min > max:
            raise ValueError(f'Invalid range: {min} ~ {max}')
        self._min = min
        self._max = max

    @property
    def min(self) -> int:
        return self._min
    
    @property
    def max(self) -> int:
        return self._max
    
    def validate(self, value: Any, context: str = '') -> None:
        super().validate(value, context)
        if value < self._min or value > self._max:
            raise ValueError(
                f'{value} out of range [{self._min}, {self._max}] in {context}')
        
    def __repr__(self) -> str:
        return super().__repr__() + f' ({self._min} ~ {self._max})'
    
class ValEnum(Validator):
    """Validator allows only one of given candidates"""
    
    def __init__(self, candidates: Sequence) -> None:
        if not isinstance(candidates, Sequence):
            raise TypeError(f'candidates must be a sequence')
        self._candidates = set(candidates)

    @property
    def candidates(self) -> Set:
        return self._candidates
    
    def validate(self, value: Any, context: str = '') -> None:
        if not value in self._candidates:
            raise ValueError(f'{value} is not a candidate in {context}')
        
    def __repr__(self) -> str:
        return super().__repr__() + ' ({})'.format(
            ', '.join(map(str, self._candidates)))

if __name__ == '__main__':
    for value, validator in [
        ('5', ValType(str)),
        ('5', ValType(int)),
        (103, ValInt(max=100)),
        ('prettyage.new@gmail.com', ValPattern('\w+@\w+(\.\w+)+')),
        ('prettyage.new@gmail.com', ValPattern('\w+(\.\w+)*@\w+(\.\w+)+')),
        (12.3, ValNumber(0.0, 100.0)),
        ('on', ValEnum(('on', 'off'))),
        ('job', ValEnum(('on', 'off'))),
    ]:
        rst = validate_value(value, validator, 'demo')
        print(f'validate {value} by {validator}: {rst}')
