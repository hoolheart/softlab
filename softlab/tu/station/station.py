"""Define ``Station`` class and its corresponding functions"""

import numbers
import math
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
# Intra-package reuse of the documented simulation-interval rule
# (design D6, steps 1-4); shared with Coordinator.prepare re-validation.
from softlab.tu.simulation.coordination import _validate_interval
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

    A station may optionally participate in a coordinated simulation
    (R3-003): ``sim_dt`` holds the shared simulation interval and a
    ``Coordinator`` (constructed explicitly as ``Coordinator(station)``)
    registers itself in ``_coordinator``. Both slots are inert when
    unused — ordinary station/device work never requires them — and
    objects enrolled in an active coordinated simulation cannot be
    removed until the coordinator releases them.
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
        # optional coordinated-simulation composition (R3-003); both
        # inert when unused (None), never created lazily
        self._sim_dt: Optional[float] = None
        self._coordinator: Any = None

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

    @property
    def sim_dt(self) -> Optional[float]:
        """
        Shared simulation interval of an optional coordinated
        simulation (R3-003); None until set.

        Ordinary station/device use never requires it. While a
        coordinator is prepared (RUNNING) or mid-operation, changes are
        rejected by the setter until a coordinated reset.

        Returns: the stored interval, or None.

        Errors: none.

        Side-effects: none.
        """
        return self._sim_dt

    @sim_dt.setter
    def sim_dt(self, value: Any) -> None:
        """
        Set the shared simulation interval (documented rule, design D6).

        **Coercion/representability rule:** non-bool ``numbers.Real``
        values are coerced to the IEEE-754 double nearest the original
        value via ``float()`` — the stored interval is always a plain
        ``float``. ``Decimal`` is rejected (it is not registered as
        ``numbers.Real``); NumPy real scalars and ``Fraction`` are
        accepted through coercion. Booleans are rejected explicitly
        (``bool`` subclasses ``int`` and registers as
        ``numbers.Real``, so the bool check precedes the real check).

        Validation order: bool -> TypeError; non-real -> TypeError;
        coerce to float; non-finite -> ValueError; non-positive ->
        ValueError; coordinator prepared or mid-operation (``_busy``)
        -> RuntimeError. The prepared/busy rejection closes the
        callback-reachable window during ``prepare()`` and
        STOPPED-state ``reset()``, so no interval change can land
        mid-operation and the interval observed during a run is
        constant (the coordinator reads it live; no caching exists).

        Every rejection leaves the stored interval unchanged and the
        station fully usable.

        Args:
        - value --- candidate interval

        Returns: None.

        Errors:
        - TypeError --- bool or non-real value
        - ValueError --- NaN, ±inf, zero or negative value
        - RuntimeError --- an associated coordinator is prepared
          (RUNNING) or mid-operation; reset first

        Side-effects: on success only, replaces the stored interval.
        """
        if type(value) is bool:
            raise TypeError(
                'sim_dt must be a positive finite real number; '
                'booleans are rejected')
        if not isinstance(value, numbers.Real):
            raise TypeError(
                f'sim_dt must be a positive finite real number, got '
                f'{type(value).__name__}')
        if not math.isfinite(float(value)):
            raise ValueError(f'sim_dt must be finite, got {value!r}')
        if float(value) <= 0.0:
            raise ValueError(f'sim_dt must be positive, got {value!r}')
        coordinator = self._coordinator
        if coordinator is not None and \
                (coordinator.prepared or coordinator._busy):
            raise RuntimeError(
                'sim_dt is fixed while the coordinated simulation is '
                'prepared or mid-operation; reset it first')
        self._sim_dt = _validate_interval(value)

    @property
    def coordinator(self) -> Optional[Any]:
        """
        The coordinator associated with this station, None if none.

        Read-only view of the composition slot: never creates a
        coordinator (inspection purity — reading station state never
        invokes simulation callbacks or prepares anything).

        Returns: the registered coordinator or None.

        Errors: none.

        Side-effects: none.
        """
        return self._coordinator

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

        Removal performs no cleanup of the removed object (membership
        bookkeeping only). An object enrolled in an active coordinated
        simulation (its ownership slot is claimed by a coordinator) is
        rejected with RuntimeError naming the active coordination:
        detach it first via the coordinator's safe-detach sequence
        (disconnect -> withdraw), then remove it. Missing names keep
        the lenient None semantics; unclaimed objects pop exactly as
        before.

        Args:
        - object_name --- name of the object member to remove

        Returns: the removed object, or None if no such member.

        Errors:
        - RuntimeError --- the object is enrolled in an active
          coordinated simulation

        Side-effects: on success only, one removal from the object
        membership; no hardware or simulation cleanup is attempted.
        """
        name = str(object_name)
        obj = self._objects.get(name)
        if obj is not None and \
                getattr(obj, '_active_coordinator', None) is not None:
            raise RuntimeError(
                f'Object {name!r} is enrolled in an active '
                'coordinated simulation; detach it before removal')
        return self._objects.pop(name, None)

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
