"""Abstract interface for any physics model"""

from typing import (
    Any,
    Dict,
    Optional,
    Callable,
)
from softlab.jin.validator import Validator
from softlab.jin.misc import (
    Delegated,
    LimitedAttribute,
)

class PhysicsModel(Delegated):
    """
    Abstract interface of any physics model

    Inherited from ``Delegated`` and making dict of ``LimitedAttribute``
    as deleagted attribute dict, which means any attribute added by
    ``add_attribute`` can be called by its name, a.k.a. ``<obj>.<attr_name>()``
    for reading and ``<obj>.<attr_name>(value)`` for writting.

    Optional name can be given at initialization.

    The ``features`` porperty is used to get any calculated model features,
    the calculation should be implemented in ``calculate_features`` method.

    Physics model can produce any calllable calculator in method
    ``get_calculator`` which should be implemented in derived classes.
    """

    def __init__(self, name: Optional[str] = None) -> None:
        super().__init__()
        self._name = name if isinstance(name, str) else ''
        self._attributes = {}
        self.add_delegate_attr_dict('_attributes')

    @property
    def name(self) -> str:
        """Name of model, given at initialization"""
        return self._name

    @property
    def features(self) -> Dict[str, Any]:
        """Calculated features due to attributes"""
        try:
            return self.calculate_features()
        except:
            return {}

    def add_attribute(self, key: str,
                      vals: Validator, initial_value: Any) -> None:
        """
        Add an attribute to the model, usually called at initialization of
        derived classes

        Args:
            - key, the key of attribute, should be unique in one model
            - vals, the validator of attribute,
            - initial_value, the initial value of attribute
        """
        if key in self._attributes:
            raise ValueError(f'Already has the attribute with key "{key}"')
        self._attributes[key] = LimitedAttribute(vals, initial_value)

    def __repr__(self) -> str:
        prefix = f'"{self.name}"' if len(self.name) > 0 else ''
        return f'{prefix}{self.__class__}'

    def get_calculator(self,
                       type: str,
                       conditions: Dict[str, Any] = {}) -> Callable:
        """
        Get any callable calculator due to ``type`` and optional ``conditions``
        """
        raise NotImplementedError(f'Not implementation for type "{type}"')

    def calculate_features(self) -> Dict[str, Any]:
        """Calculate model features due to attribute setting"""
        raise NotImplementedError('Should be implemented in derived class')

if __name__ == '__main__':
    from softlab.jin.validator import ValNumber
    import numpy as np

    class Motion1D(PhysicsModel):

        def __init__(self,
                     name: Optional[str] = None,
                     mass: float = 1.0) -> None:
            super().__init__(name)
            if not isinstance(mass, float) or mass < 1e-18:
                mass = 1.0
            self.add_attribute('mass', ValNumber(1e-18), mass)

        def get_calculator(self,
                           type: str,
                           conditions: Dict[str, Any] = {}) -> Callable:
            if type == 'diff':
                return lambda x: self._diff(x, conditions['force'])
            return super().get_calculator(type, conditions)

        def calculate_features(self) -> Dict[str, Any]:
            return {'weight': self.mass() * 9.8}

        def _diff(self, cur: np.ndarray, force: float) -> np.ndarray:
            if isinstance(cur, np.ndarray) and cur.shape == (2, 1):
                return np.array([cur[1, 0], force / self.mass()]).reshape(2, 1)
            return np.ndarray()

    m = Motion1D('test', 0.1)
    print(f'Create 1D motion model {m}')
    print(f'Mass: {m.mass()}')
    print(f'Features: {m.features}')
    calc = m.get_calculator('diff', {'force': 1.0})
    x0 = np.zeros((2, 1))
    x0[1, 0] = -10.0
    print(f'Initial state: {x0}')
    print(f'Diff: {calc(x0)}')
    m.mass(1.0)
    print(f'Change mass to {m.mass()} and diff becomes: {calc(x0)}')
