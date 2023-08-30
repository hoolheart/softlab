"""Abstract definition of validator interface"""
from typing import Any

class Validator():
    """
    Abstract interface for all validators

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
