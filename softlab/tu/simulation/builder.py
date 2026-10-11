"""Object builder interface and model-keyed object-builder registry.

This module is the object-side analog of
``softlab.tu.station.device.DeviceBuilder`` and its device-builder
registry: an ``ObjectBuilder`` carries a ``model`` identity and
``build(name, **kwargs)`` constructs a ``SimulatedObject``, while the
module-global ``_object_builders`` registry supports discovery and
station-level construction. The two registries are fully distinct.
"""

from typing import (
    Any,
    Dict,
    Optional,
)
from softlab.tu.simulation.object import SimulatedObject


class ObjectBuilder():
    """
    Builder interface to generate specific simulated object.

    Every available subclass must implement ``build`` function.
    Different builders differ due to their different ``model`` properties.
    """

    def __init__(self, model: str) -> None:
        """
        Initialization

        Args:
        - model --- builder model
        """
        model = str(model)
        if len(model) == 0:
            raise ValueError('Empty object builder model')
        self._model = model

    @property
    def model(self) -> str:
        """Get builder model"""
        return self._model

    def __repr__(self) -> str:
        return f'<ObjectBuilder>{self.model}'

    def build(self, name: str, **kwargs: Any) -> SimulatedObject:
        """
        Generate a simulated object, implemented in subclasses.

        Args:
        - name --- object name; should become the constructed object's
          name, since the station inserts members under ``obj.name``
        - kwargs --- key specific arguments to create object

        Returns: a fresh simulated object corresponding to such builder.

        Contract for subclasses:
        - MUST return a ``SimulatedObject``.
        - MUST return a fresh instance per call, constructed within the
          call; MUST NOT return an instance already returned by an
          earlier ``build`` call or otherwise owned (e.g. already a
          station member). Station insertion rejects an already-owned
          result via the duplicate-name check, so non-conforming
          builders fail loudly on the second insertion rather than
          aliasing mutable state.
        - MAY raise any exception; the exception object propagates
          through ``Station.build_object`` unchanged and station
          membership is left untouched.

        Errors: the base implementation raises ``NotImplementedError``.
        """
        raise NotImplementedError


_object_builders: Dict[str, ObjectBuilder] = {}
"""Global dictionary of object builders"""


def register_object_builder(builder: ObjectBuilder) -> None:
    """Register object builder

    Stores the builder under its model identity and calls nothing on it
    (registration constructs nothing).

    Errors:
    - TypeError --- ``builder`` is not an ``ObjectBuilder``
    - ValueError --- another builder is already registered under the
      same model
    """
    if not isinstance(builder, ObjectBuilder):
        raise TypeError(f'Invalid object builder type {type(builder)}')
    if builder.model in _object_builders:
        raise ValueError(
            f'Object builder with model {builder.model} has exist')
    _object_builders[builder.model] = builder


def get_object_builder(model: str) -> Optional[ObjectBuilder]:
    """Get object builder with given ``model``, return None if non-exist"""
    return _object_builders.get(model, None)
