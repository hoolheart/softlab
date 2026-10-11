"""Define ``Station`` class and its corresponding functions"""

from typing import(
    Any,
    Dict,
    Sequence,
    Optional,
)
from datetime import datetime
from softlab.tu.station.device import (
    Device,
    DeviceBuilder,
    get_device_builder,
    _describe_device_node,
)
from softlab.tu.station.parameter import _validate_metadata
from softlab.tu.simulation import (
    SimulatedObject,
    ObjectBuilder,
    get_object_builder,
)
from softlab.jin.misc import Delegated

class Station(Delegated):
    """
    Station is an ensemble of devices to perform experiment.

    By inheriting ``Delegated``, any device  can be accessed directly as
    an attribute of station.

    Station membership uses one shared namespace: a name identifies at
    most one member across devices and simulated objects, so neither
    kind can silently shadow the other. Device members live in
    ``devices`` and are looked up via ``device``; object members live in
    ``objects`` and are looked up via ``object`` (the explicit escape
    hatch for names colliding with station methods or properties).

    Properties:
    - name --- station name, given at initialization
    - created_at --- datetime when station is created
    - devices --- sequence of containing device names
    - objects --- sequence of containing object names

    Public methods:
    - add_device --- add a device into station
    - rm_device --- remove a device from station
    - device --- access a device with given name, none if non-exist
    - build_device --- build a device by using device builder
    - add_object --- add a simulated object into station
    - rm_object --- remove a simulated object from station
    - object --- access a simulated object with given name, none if
      non-exist
    - build_object --- build a simulated object by using object builder
    - snapshot --- return snapshot of station information
    """

    def __init__(self, name: str) -> None:
        """Initialization, ``name`` must be non-empty string"""
        name = str(name)
        if len(name) == 0:
            raise ValueError('Empty station name')
        self._name = name
        self._devices: Dict[str, Device] = {}
        self.add_delegate_attr_dict('_devices')
        self._objects: Dict[str, SimulatedObject] = {}
        self.add_delegate_attr_dict('_objects')
        self._created_at: datetime = datetime.now()

    @property
    def name(self) -> str:
        """Get name of station"""
        return self._name

    @property
    def created_at(self) -> datetime:
        """Get created timestamp"""
        return self._created_at

    @property
    def devices(self) -> Sequence[str]:
        """Get sequence of device names"""
        return self._devices.keys()

    @property
    def objects(self) -> Sequence[str]:
        """Get sequence of object names"""
        return self._objects.keys()

    def __repr__(self) -> str:
        return f'{type(self)}/{self.name}'

    def snapshot(self) -> Dict[str, Any]:
        """Get snapshot of station information"""
        return {
            'name': self.name,
            'created_at': self._created_at.strftime('%Y-%m-%dT%H:%M:%S'),
            'devices': dict(map(
                lambda key: (key, self._devices[key].snapshot()),
                self._devices,
            ))
        }

    def describe(self,
                 metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get a portable, JSON-compatible description of this station.

        The description contains only structure: common fields plus a
        ``devices`` dict keyed by lookup key, where each device's
        current name is serialized separately. Dictionary iteration
        order is preserved. No runtime parameter value, creation time
        or owner object is included.

        Args:
        - metadata --- optional user metadata mapping attached to this
          node, validated and recursively copied; descendants receive
          empty metadata

        Returns:
        - a detached version-1 description dict

        Errors:
        - TypeError --- invalid metadata type or non-string metadata key
        - ValueError --- non-finite float, cyclic metadata container, or
          a device cycle inside the device hierarchy

        Side-effects: none; never reads parameter values and never calls
        ``get``, ``set``, ``snapshot``, validators, codecs, hooks or VISA
        handles during traversal.
        """
        cls = type(self)
        return {
            'schema_version': 1,
            'name': self._name,
            'type': cls.__module__ + '.' + cls.__qualname__,
            'metadata': _validate_metadata(metadata),
            'devices': {
                key: _describe_device_node(device, {}, set(), key)
                for key, device in self._devices.items()
            },
        }

    def add_device(self, device: Device) -> None:
        """Add ``device`` into station"""
        if not isinstance(device, Device):
            raise TypeError(f'Invalid device type {type(device)}')
        if device.name in self._objects:
            raise ValueError(f'Device name {device.name} collides with an '
                             f'existing object')
        if device.name in self._devices:
            raise ValueError(f'Device with name {device.name} has exist')
        self._devices[device.name] = device

    def rm_device(self, device_name: str) -> Optional[Device]:
        """Remove a device with name ``device_name`` from station"""
        return self._devices.pop(str(device_name), None)

    def device(self, device_name: str) -> Optional[Device]:
        """Access device with name ``device_name``"""
        return self._devices.get(str(device_name), None)

    def build_device(self, model: str, name: str, **kwargs: Any) -> None:
        """Build a device and add into station

        Args:
        - model --- builder model
        - name --- device name
        - kwargs --- arguments to build device
        """
        builder = get_device_builder(model) # get builder
        if not isinstance(builder, DeviceBuilder):
            raise RuntimeError(f'Failed to get builder with model {model}')
        self.add_device(builder.build(name, **kwargs))

    def add_object(self, obj: SimulatedObject) -> None:
        """Add ``obj`` into station

        The object is inserted under its own (read-only) name; because
        ``SimulatedObject.name`` has no setter, the membership key always
        equals the object's current name for its whole membership.

        Args:
        - obj --- the simulated object to add

        Errors:
        - TypeError --- ``obj`` is not a ``SimulatedObject``
        - ValueError --- ``obj.name`` is already held by an object member
        - ValueError --- ``obj.name`` is already held by a device member

        Side-effects: on success only, one insertion into the object
        membership; on any error, membership is unchanged.
        """
        if not isinstance(obj, SimulatedObject):
            raise TypeError(f'Invalid object type {type(obj)}')
        if obj.name in self._objects:
            raise ValueError(f'Object with name {obj.name} has exist')
        if obj.name in self._devices:
            raise ValueError(f'Object name {obj.name} collides with an '
                             f'existing device')
        self._objects[obj.name] = obj

    def rm_object(self, object_name: str) -> Optional[SimulatedObject]:
        """Remove an object with name ``object_name`` from station

        Removal performs no cleanup of the removed object. Note that
        removal of an object enrolled in an active coordinated
        simulation will be rejected by the future ownership layer
        (R3-003 scope), not by this method.
        """
        return self._objects.pop(str(object_name), None)

    def object(self, object_name: str) -> Optional[SimulatedObject]:
        """Access object with name ``object_name``"""
        return self._objects.get(str(object_name), None)

    def build_object(self, model: str, name: str, **kwargs: Any) -> None:
        """Build a simulated object and add into station

        Steps, in order: validate ``name``, look up the object builder
        registered under ``model``, build the object, then insert it via
        ``add_object``. Every failure leaves station membership —
        devices and objects — unchanged; nothing is partially registered
        or inserted.

        Args:
        - model --- builder model key in the object-builder registry
        - name --- object name for the constructed member; must be a
          non-empty ``str``, validated before any registry access or
          construction
        - kwargs --- arguments to build object

        Errors:
        - ValueError --- ``name`` is not a ``str`` or is empty
        - RuntimeError --- no object builder registered under ``model``
          (``Failed to get object builder with model ...``)
        - any exception raised by the builder's ``build`` propagates as
          the identical exception object, never caught, wrapped or
          replaced
        - TypeError/ValueError --- the built object fails membership
          validation in ``add_object``

        Side-effects: on success only, the constructed object is
        inserted under ``obj.name`` and becomes retrievable via
        ``object()`` and attribute delegation. Exactly one ``build``
        call per ``build_object`` call. A builder returning an
        already-owned instance is rejected by the duplicate-name check.
        """
        if type(name) is not str or len(name) == 0:
            raise ValueError('Object name must be a non-empty str, got '
                             + repr(name))
        builder = get_object_builder(model) # get builder
        if not isinstance(builder, ObjectBuilder):
            raise RuntimeError(
                f'Failed to get object builder with model {model}')
        self.add_object(builder.build(name, **kwargs))

_default_station: Station = Station('default')
"""Default station"""

def default_station() -> Station:
    """Get default station"""
    return _default_station

def set_default_station(station: Station) -> None:
    """Set default station"""
    global _default_station
    if not isinstance(station, Station):
        raise TypeError(f'Invalid station type {type(station)}')
    _default_station = station

if __name__ == '__main__':
    from pprint import pprint
    from softlab.tu.station.parameter import Parameter
    from softlab.tu.station.device import register_device_builder
    from softlab.jin.validator import ValNumber
    station = default_station()
    print(f'Get default station {station}')
    for i in range(3):
        station.add_device(Device(f'device{i}'))
    station.device('device1').add_parameter(
        Parameter('para', ValNumber(0.0, 1.0), init_value=0.1))
    pprint(station.snapshot())
    print(f'Initial para: {station.device1.para()}')
    station.device1.para(0.5)
    print(f'After setting: {station.device1.para()}')

    set_default_station(Station('demo'))
    print(f'Change default station {default_station()}')
    station = default_station()

    class _BuilderDemo(DeviceBuilder):
        def __init__(self) -> None:
            super().__init__('demo')

        def build(self, name: str, **_) -> Device:
            dev = Device(name)
            dev.add_parameter(
                Parameter('percentage', ValNumber(0.0, 100.0), init_value=3.14))
            return dev
    register_device_builder(_BuilderDemo())

    station.build_device('demo', 'demo_device')
    pprint(station.snapshot())
