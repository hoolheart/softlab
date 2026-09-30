"""Abstract interface for any theoretical model"""

import collections.abc
from typing import (
    Any,
    Dict,
    Optional,
    Callable,
    Tuple,
)
from softlab.jin.validator import Validator
from softlab.jin.misc import (
    Delegated,
    LimitedAttribute,
)
from softlab.tu.theory.mapping import Mapping

class TheoryModel(Delegated):
    """
    Abstract interface of any theoretical model

    Inherited from ``Delegated`` and making dict of ``LimitedAttribute``
    as deleagted attribute dict, which means any attribute added by
    ``add_attribute`` can be called by its name, a.k.a. ``<obj>.<attr_name>()``
    for reading and ``<obj>.<attr_name>(value)`` for writting.

    Optional name can be given at initialization.

    The ``features`` porperty is used to get any calculated model features,
    the calculation should be implemented in ``calculate_features`` method.

    Theoretical model can produce any mapping in method
    ``get_mapping`` which should be implemented in derived classes.

    The method names ``describe``, ``supported_configuration``,
    ``configuration``, ``configure`` and ``evaluate_features`` are
    reserved: they shadow any delegated attribute key of the same name,
    because delegated lookup (``Delegated.__getattr__``) only fires when
    normal attribute lookup fails. If a model registers an attribute
    under one of these names, dotted-call access reaches the method, not
    the attribute; explicit lookup through ``model._attributes[name]()``
    remains available as the escape hatch (the OBS-006 convention). No
    in-repo model uses any of these names as attribute keys.
    """

    def __init__(self, name: Optional[str] = None) -> None:
        super().__init__()
        self._name = name if isinstance(name, str) else ''
        self._attributes = {}
        self._attr_descriptions = {}
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
                      vals: Validator, initial_value: Any,
                      description: str = '') -> None:
        """
        Add an attribute to the model, usually called at initialization of
        derived classes

        Args:
            - key, the key of attribute, should be unique in one model
            - vals, the validator of attribute,
            - initial_value, the initial value of attribute
            - description, optional semantic description of the attribute,
              default ``''``

        ``description`` (keyword-only in intent, positional-compatible by
        default): optional semantic description of the attribute, default
        ``''``. It is recorded only after the duplicate-key check and the
        initial-value validation both succeed, so a rejected attribute
        never leaves an orphan description. Existing three-argument calls
        are unchanged.
        """
        if key in self._attributes:
            raise ValueError(f'Already has the attribute with key "{key}"')
        self._attributes[key] = LimitedAttribute(vals, initial_value)
        self._attr_descriptions[key] = description

    def describe(self) -> Dict[str, Any]:
        """
        Returns the versioned semantic description of this model
        (``schema_version`` 1): the model ``name``, the model kind as the
        class's ``__qualname__`` (JSON-safe), and one entry per attribute
        carrying its semantic ``description`` (``''`` when none was
        given). Performs no evaluation: ``calculate_features`` is never
        called, so a model whose evaluation raises is still fully
        describable. The returned dict is freshly built on each call;
        mutating it does not affect the model.

        Returns:
            description dict with keys ``schema_version`` (``int``, 1),
            ``name`` (``str``, possibly empty), ``model`` (``str``,
            class ``__qualname__``) and ``attributes`` (``dict`` mapping
            each attribute key to a ``dict`` carrying its ``description``)
        """
        return {
            'schema_version': 1,
            'name': self.name,
            'model': type(self).__qualname__,
            'attributes': {
                key: {'description': self._attr_descriptions[key]}
                for key in self._attributes
            },
        }

    def supported_configuration(self) -> Tuple[str, ...]:
        """
        Returns the supported configuration keys — exactly the
        registered attribute keys, in registration order. The declaration
        is explicit and independent of current attribute values: it does
        not change when values change. The keys are strings and JSON-safe.

        Returns:
            tuple of the registered attribute keys, in registration order
        """
        return tuple(self._attributes.keys())

    def configuration(self) -> Dict[str, Any]:
        """
        Returns the current configuration as a fresh dict mapping each
        supported key to its attribute's current value, read through the
        existing ``LimitedAttribute.get()``. Values are JSON-serializable
        exactly when the model's attribute values are: JSON safety of
        values is the model author's responsibility and is not enforced —
        an attribute holding a non-serializable value (e.g. an ndarray)
        is still configurable, but the returned dict will not dump to
        JSON. Performs no evaluation and no I/O.

        Returns:
            fresh dict mapping each supported configuration key to its
            attribute's current value
        """
        return {key: attr.get() for key, attr in self._attributes.items()}

    def configure(self,
                  cfg: collections.abc.Mapping[str, Any]) -> None:
        """
        Applies a configuration mapping. Three phases, each completing
        fully before the next: (1) a non-``collections.abc.Mapping``
        argument raises ``TypeError`` naming the received type; (2) any
        key not in ``supported_configuration()`` raises ``KeyError``
        naming the key; (3) every value is pre-validated against its
        attribute's existing ``Validator`` before any value is applied.
        Rejection at any phase leaves the previous configuration fully
        intact — no partial application for any rejection cause,
        including multi-key validation failure. Application writes each
        value through the existing ``LimitedAttribute.set()``
        (validate-then-assign), reusing the existing validation chain
        rather than duplicating it. An empty mapping is a no-op.
        Re-applying a dict read from ``configuration()`` is an identity.

        Args:
            - cfg, the configuration mapping to apply

        Errors:
            - ``TypeError``, if ``cfg`` is not a
              ``collections.abc.Mapping``
            - ``KeyError``, if any key of ``cfg`` is not a registered
              attribute key
            - the attribute ``Validator``'s own exception, if any value
              fails pre-validation
        """
        if not isinstance(cfg, collections.abc.Mapping):
            raise TypeError(
                f'Configuration should be a mapping, '
                f'not {type(cfg).__name__}')
        for key in cfg:
            if key not in self._attributes:
                raise KeyError(key)
        for key, value in cfg.items():
            self._attributes[key]._vals.validate(value)
        for key, value in cfg.items():
            self._attributes[key].set(value)

    def evaluate_features(self, strict: bool = False) -> Dict[str, Any]:
        """
        Evaluates model features by calling ``calculate_features()``
        directly — never through the ``features`` property (the property
        swallows all exceptions, so a strict path routed through it could
        never propagate anything). With ``strict=True`` the original
        exception object from ``calculate_features`` propagates unchanged
        (no wrapping, no ``raise ... from``). With ``strict=False`` (the
        default) the legacy lenient semantics are preserved: any
        ``Exception`` raised by evaluation returns ``{}`` (the OBS-005
        fallback), while ``BaseException``-only process-control
        exceptions (``KeyboardInterrupt``, ``SystemExit``,
        ``GeneratorExit``) deliberately propagate — the swallow is not
        extended into new code. The legacy ``features`` property is a
        separate, unchanged code path.

        Args:
            - strict, whether to propagate the original evaluation error
              (``True``) or fall back to an empty dict (``False``,
              default)

        Returns:
            calculated feature dict, or ``{}`` on evaluation failure in
            lenient mode
        """
        if strict:
            return self.calculate_features()
        try:
            return self.calculate_features()
        except Exception:
            return {}

    def __repr__(self) -> str:
        prefix = f'"{self.name}"' if len(self.name) > 0 else ''
        return f'{prefix}{self.__class__}'

    def get_mapping(self,
                    type: str,
                    conditions: Dict[str, Any] = {}) -> Mapping:
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

    class Motion1D(TheoryModel):

        def __init__(self,
                     name: Optional[str] = None,
                     mass: float = 1.0) -> None:
            super().__init__(name)
            if not isinstance(mass, float) or mass < 1e-18:
                mass = 1.0
            self.add_attribute('mass', ValNumber(1e-18), mass)

        def get_mapping(self,
                        type: str,
                        conditions: Dict[str, Any] = {}) -> Callable:
            if type == 'diff':
                return Mapping((2,1), (2,1),
                               lambda x: self._diff(x, conditions['force']))
            return super().get_mapping(type, conditions)

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
    calc = m.get_mapping('diff', {'force': 1.0})
    x0 = np.zeros((2, 1))
    x0[1, 0] = -10.0
    print(f'Initial state: {x0}')
    print(f'Diff: {calc(x0)}')
    m.mass(1.0)
    print(f'Change mass to {m.mass()} and diff becomes: {calc(x0)}')
