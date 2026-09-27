"""
A ``Device`` is an abstraction of any kind of laborary equipment, or anything
can be treated like an equipment, e.g. a vitual analyzer.

The data of device takes form of parameters, therefore a device can be treated
as a container of parameters. Note that the devices are not only form of
parameter containers.

A device instance can have child devices. For instance, a oscilloscope contains
more than one channels generally, and each channel can abstracted into a child
device of oscilloscope.
"""

from typing import (
    Any,
    Dict,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)
from softlab.jin.misc import Delegated
from softlab.tu.station.parameter import (
    Parameter,
    _validate_metadata,
)


def _describe_device_node(
    device: "Device",
    metadata: Dict[str, Any],
    active: Set[int],
    path: str,
) -> Dict[str, Any]:
    """
    Build a detached version-1 description node of ``device``.

    Stored collections ``_parameters`` and ``_devices`` are read
    directly (no delegated attribute lookup), preserving dictionary
    iteration order and serializing the lookup key separately from the
    contained object's current name. Nested parameter and device nodes
    always carry empty metadata and are constructed from stored fields;
    a subclass ``describe`` override is never invoked.

    Args:
    - device --- device instance to inspect
    - metadata --- already validated, detached metadata for this node
    - active --- identities of devices on the active ancestry path
    - path --- dotted lookup path of ``device``, used in error messages

    Returns:
    - a detached version-1 description dict with ``parameters`` and
      ``children`` keyed by lookup key

    Errors:
    - ValueError --- if ``device`` is already on the active ancestry
      path (a cycle)

    Side-effects: none; never calls ``get``, ``set``, ``snapshot``,
    validators, codecs, hooks or VISA handles.
    """
    if id(device) in active:
        raise ValueError(f'Cyclic device reference at {path}')
    active.add(id(device))
    parameters = {}
    for key, para in device._parameters.items():
        cls = type(para)
        parameters[key] = {
            'schema_version': 1,
            'name': para._name,
            'type': cls.__module__ + '.' + cls.__qualname__,
            'metadata': {},
            'settable': para._settable,
            'gettable': para._gettable,
        }
    children = {}
    for key, child in device._devices.items():
        children[key] = _describe_device_node(
            child, {}, active, f'{path}.{key}')
    active.discard(id(device))
    cls = type(device)
    return {
        'schema_version': 1,
        'name': device._name,
        'type': cls.__module__ + '.' + cls.__qualname__,
        'metadata': metadata,
        'parameters': parameters,
        'children': children,
    }


class Device(Delegated):
    """
    Device base class

    A device has a non-empty name, and contains any count of parameters and
    child devices. By inheriting ``Delegated``, any parameter and child device
    can be accessed directly as an attribute of device.

    Public properties:
    - name --- device name, should be non-empty and unique in station
    - parent --- parent device, used for sub-devices, optional
    - initialized --- readiness flag, True only between a successful
      ``prepare()`` and the next ``cleanup()``

    Public methods:
    - snapshot --- get the snapshot dict of device
    - parameter --- get parameter with given name, none if non-exist
    - child --- get subdevice with given name, none if non-exist
    - add_parameter --- add a new parameter
    - rm_parameter --- remove parameter with given name
    - add_child --- add a new subdevice
    - rm_child --- remove subdevice with given name
    - set_parameters --- batch setting of multiple parameters
    - supports --- side-effect-free capability detection
    - prepare --- explicit preparation, opt-in for subclasses
    - cleanup --- explicit, idempotent release of owned resources only

    Note: parameters of children can be accessed by
          "<child_name>.<parameter_name>" or even
          "<child_name>.<child_child_name>.<parameter_name>"

    Lifecycle notes:
    - The lifecycle contract (``supports``, ``prepare``, ``initialized``,
      ``cleanup``) is single-threaded: no locking is performed and no
      atomicity across threads is guaranteed; device authors needing
      concurrent access must serialize externally.
    - The lifecycle names above are real members of ``Device``, so they
      take precedence over delegated parameter/child attribute access
      (OBS-006 category): a parameter or child named e.g. ``"prepare"``
      is shadowed for attribute-style access, while explicit lookup via
      ``parameter("prepare")`` / ``child("prepare")`` remains available.
    """

    def __init__(self, name: str) -> None:
        """
        Initialization

        Args:
        - name --- device name, non-empty string, should be unique in station
        """
        name = str(name)
        if len(name) == 0:
            raise ValueError('Empty device name')
        self._name = name
        self._parameters: Dict[str, Parameter] = {}
        self.add_delegate_attr_dict('_parameters')
        self._devices: Dict[str, Device] = {}
        self.add_delegate_attr_dict('_devices')
        self._parent: Optional[Device] = None
        self._initialized: bool = False
        self._needs_cleanup: bool = False

    @property
    def name(self) -> str:
        """Get device name"""
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        """Change device name"""
        name = str(name)
        if len(name) == 0:
            raise ValueError('Empty device name')
        self._name = name

    @property
    def parent(self) -> Optional["Device"]:
        """Get parent device"""
        return self._parent

    @parent.setter
    def parent(self, parent: Optional["Device"]) -> None:
        """Set parent device, only valid for a different device or None"""
        if parent is None or isinstance(parent, Device):
            if parent == self:
                raise ValueError(f'A device {self} can not be its own parent')
            self._parent = parent
        else:
            raise ValueError(f'Invalid parent device {parent}')

    def __repr__(self) -> str:
        return f'{type(self)}/{self.name}'

    def supports(self, capability: str) -> bool:
        """
        Detect whether a capability is supported, side-effect-free

        Base vocabulary: ``'prepare'`` and ``'cleanup'`` are supported,
        ``'connection'`` is not (a base device is virtual and has no
        connection to open), and any other string — including the empty
        string — is an unknown capability and is not supported. Detection
        is a pure string-membership test: it never performs I/O, never
        touches a resource manager, and never raises for unknown names.

        Args:
        - capability --- capability name to test, arbitrary string

        Returns:
        - True if the capability is supported, False otherwise

        Extension rule for subclasses: a subclass may override this
        method to advertise additional capabilities, but it must remain
        side-effect-free and must never raise for unknown capability
        strings (unknown names return False). The recommended form is
        additive over the base result, e.g.
        ``return capability in ('trigger',) or super().supports(capability)``.
        A subclass that drops ``'prepare'``/``'cleanup'`` from its result
        while still inheriting the base hooks is out of contract.

        Side-effects: none.
        """
        return capability in ('prepare', 'cleanup')

    def prepare(self) -> None:
        """
        Explicit preparation (opt-in lifecycle)

        Runs the subclass acquisition hook ``_prepare_impl()`` and, on
        success, marks the device initialized. For the base virtual
        device the hook is a no-op, so ``prepare()`` acquires and opens
        nothing: no resource manager, no connection, zero I/O.

        Idempotence: if the device is already initialized, this is a
        no-op — the hook is not re-run, no exception is raised, and
        readiness stays True. Re-preparation after a ``cleanup()`` runs
        the hook again: the lifecycle is a repeatable cycle, not a
        one-shot.

        Failure: the release path is armed before the hook runs, so if
        the hook raises, the device is not initialized but holds a
        partial acquisition that the first subsequent ``cleanup()``
        releases exactly once. The original exception object propagates
        unchanged — no wrapping, no chaining, no implicit cleanup.

        After a failed ``prepare()`` (the hook raised), the device holds
        a pending partial acquisition: ``cleanup()`` MUST be called
        before calling ``prepare()`` again. Re-preparation without an
        intervening ``cleanup()`` is outside the lifecycle contract and
        its behavior is not guaranteed.

        This contract is single-threaded: no locking is performed and
        concurrent calls are outside the contract.

        Returns: None

        Errors:
        - any exception raised by ``_prepare_impl()`` propagates
          unchanged; the device is left not initialized

        Side-effects: for the base class, state fields only; subclasses
        may perform arbitrary acquisition in ``_prepare_impl()``.
        """
        if self._initialized:
            return
        self._needs_cleanup = True
        self._prepare_impl()
        self._initialized = True

    @property
    def initialized(self) -> bool:
        """
        Readiness flag of the device (read-only)

        Transitions: False on a freshly constructed device; False to
        True only on a successful ``prepare()``; True to False on
        ``cleanup()`` of an initialized device; stays False through a
        failed ``prepare()``. Outside those transitions the value is
        unchanged.

        Returns:
        - True if the device has been successfully prepared and not
          cleaned up since, False otherwise

        Side-effects: none.
        """
        return self._initialized

    def cleanup(self) -> None:
        """
        Explicit, idempotent release of owned resources only

        Safe to call at any point. If no acquisition has been attempted
        (the device is a non-owner), this is a no-op: the subclass hook
        is not invoked, so borrowed (externally owned) resources are
        never released by a non-owner. Otherwise the release is
        discharged exactly once per acquisition attempt, including a
        failed preparation: the pending state is cleared before the hook
        runs, so a repeated ``cleanup()`` is a no-op even if the hook
        raises, and the release is never implicitly retried.

        If the subclass release hook ``_cleanup_impl()`` raises, the
        exception propagates unchanged; the pending state is already
        cleared, so a failing release is never retried implicitly.

        This contract is single-threaded: no locking is performed and
        concurrent calls are outside the contract.

        Returns: None

        Errors:
        - any exception raised by ``_cleanup_impl()`` propagates
          unchanged

        Side-effects: for the base class, state fields only; subclasses
        may release owned resources in ``_cleanup_impl()``.
        """
        if not self._needs_cleanup:
            return
        self._needs_cleanup = False
        self._initialized = False
        self._cleanup_impl()

    def _prepare_impl(self) -> None:
        """
        Acquisition hook, invoked by ``prepare()`` — base no-op

        The base implementation acquires nothing, which is why a plain
        ``Device`` performs zero I/O at every point. Subclasses opting
        into the lifecycle override this hook to perform actual
        acquisition; raising signals failure with a partial acquisition
        permitted (releasable by the first subsequent ``cleanup()``).
        Subclasses express acquisition here only and must release only
        what this hook acquired, in ``_cleanup_impl()``. Subclasses must
        not override ``prepare()`` itself.

        Returns: None

        Side-effects: none for the base implementation.
        """
        pass

    def _cleanup_impl(self) -> None:
        """
        Release hook, invoked by ``cleanup()`` — base no-op

        The base implementation releases nothing. Subclasses opting into
        the lifecycle override this hook to release owned resources;
        borrowed (externally owned) resources must not be released here
        — the base gating guarantees the hook never runs for a device
        that never attempted acquisition. The hook is invoked at most
        once per acquisition attempt (successful or failed), and never
        when no attempt was made. If it raises, the exception propagates
        unchanged and is never implicitly retried. Subclasses must not
        override ``cleanup()`` itself.

        Returns: None

        Side-effects: none for the base implementation.
        """
        pass

    def snapshot(self) -> Dict[str, Any]:
        """Get snapshot of device information"""
        return {
            'name': self.name,
            'parameters': dict(map(
                lambda key: (key, self._parameters[key].snapshot()),
                self._parameters,
            )),
            'children': dict(map(
                lambda key: (key, self._devices[key].snapshot()),
                self._devices,
            ))
        }

    def describe(self,
                 metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get a portable, JSON-compatible description of this device.

        The description contains only structure: common fields plus
        ``parameters`` and ``children`` dicts keyed by lookup key, where
        each contained object's current name is serialized separately.
        Dictionary iteration order is preserved. No runtime parameter
        value, validator representation or owner object is included.

        Args:
        - metadata --- optional user metadata mapping attached to this
          node, validated and recursively copied; descendants receive
          empty metadata

        Returns:
        - a detached version-1 description dict

        Errors:
        - TypeError --- invalid metadata type or non-string metadata key
        - ValueError --- non-finite float, cyclic metadata container, or
          a device cycle on the active ancestry path

        Side-effects: none; never reads parameter values and never calls
        ``get``, ``set``, ``snapshot``, validators, codecs, hooks or
        subclass ``describe`` overrides during traversal.
        """
        return _describe_device_node(
            self, _validate_metadata(metadata), set(), self._name)

    def parameter(self, key: str) -> Optional[Parameter]:
        """
        Get parameter with given key

        Args:
        - key --- parameter key

        Returns:
        - the parameter instance, None if non-exist

        Note:
            user can use '.' to link child name,
            e.g. <child_name>.<parameter_name> or even
            <child_name>.<child_child_name>.<parameter_name>
        """
        if key in self._parameters:
            return self._parameters[key]
        parts = key.split('.')
        if len(parts) > 1:
            cur_dev = self
            for child_name in parts[:-1]:
                cur_dev = cur_dev.child(child_name)
                if cur_dev is None:
                    break
            if isinstance(cur_dev, Device):
                return cur_dev.parameter(parts[-1])
        return None

    def child(self, key: str) -> Optional["Device"]:
        """
        Get child device with given key

        Args:
        - key --- child device key

        Returns: the device instance, None if non-exist
        """
        if key in self._devices:
            return self._devices[key]
        return None

    def add_parameter(self, para: Parameter, visible: bool = True) -> None:
        """
        Add a parameter into device

        Args:
        - para --- parameter instance, its name as its key should not be used
                   in existing parameter dict and child device dict
        - visible --- whether the parameter is visible from outside, if not,
                      its key put into omitting attribute names
        """
        if not isinstance(para, Parameter):  # check
            raise TypeError(f'Invalid parameter type {type(para)}')
        key = para.name  # use parameter name as key
        if key in self._parameters or key in self._devices:
            raise ValueError(f'Parameter with key {key} has exist')
        self._parameters[key] = para  # update
        para.owner = self
        if not visible:
            self.add_omit_delegate_attrs(key)  # invisible parameter

    def add_child(self, child: "Device", visible: bool = True) -> None:
        """
        Add a child device into device

        Args:
        - child --- child device instance, its name as its key should not be
                    used in existing parameter dict and child device dict
        - visible --- whether the child is visible from outside, if not,
                      its key put into omitting attribute names
        """
        if not isinstance(child, Device):  # check
            raise TypeError(f'Invalid child device type {type(child)}')
        key = child.name  # use device name as key
        if key in self._devices or key in self._parameters:
            raise ValueError(f'Child device with key {key} has exist')
        self._devices[key] = child  # update
        child.parent = self
        if not visible:
            self.add_omit_delegate_attrs(key)  # invisible child device

    def rm_parameter(self, key: str) -> Optional[Parameter]:
        """Remove parameter from device and return it (None if non-exist)"""
        para = self._parameters.pop(key, None)
        if isinstance(para, Parameter):
            para.owner = None
        return para

    def rm_child(self, key: str) -> Optional["Device"]:
        """Remove child from device and return it (None if non-exist)"""
        child = self._devices.pop(key, None)
        if isinstance(child, Device):
            child.parent = None
        return child

    def set_parameters(self, settings: Union[Sequence[Tuple[str, Any]],
                                             Dict[str, Any]]) -> None:
        """
        Set any number of parameters

        Args:
        - settings --- setting information, either be the sequence of
                       key-value pairs or dictionary of keys and values,
                       note that the sequence form can control the setting
                       order
        """
        if isinstance(settings, Dict):
            settings = settings.items()
        for key, value in settings:
            para = self.parameter(key)
            if isinstance(para, Parameter):
                para(value)
            else:
                raise KeyError(f'Invalid parameter key {key}')


class DeviceBuilder():
    """
    Builder interface to gerenate specific device.

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
            raise ValueError('Empty device builder model')
        self._model = model

    @property
    def model(self) -> str:
        """Get builder model"""
        return self._model

    def __repr__(self) -> str:
        return f'<DeviceBuilder>{self.model}'

    def build(self, name: str, **kwargs: Any) -> Device:
        """
        Generate a device, implemented in subclasses

        Args:
        - name --- device name
        - kwargs --- key specific arguments to create device

        Returns: a device corresponding to such builder
        """
        raise NotImplementedError


_device_builders: Dict[str, DeviceBuilder] = {}
"""Global dictionary of device builders"""


def register_device_builder(builder: DeviceBuilder) -> None:
    """Register device builder"""
    if not isinstance(builder, DeviceBuilder):
        raise TypeError(f'Invalid device builder type {type(builder)}')
    if builder.model in _device_builders:
        raise ValueError(
            f'Device builder with model {builder.model} has exist')
    _device_builders[builder.model] = builder


def get_device_builder(model: str) -> Optional[DeviceBuilder]:
    """Get device builder with given ``model``, return None if non-exist"""
    return _device_builders.get(model, None)


if __name__ == '__main__':
    import pprint
    from softlab.jin.validator import ValNumber
    dev = Device('demo')
    for para in [
        Parameter('para0', ValNumber(0.0, 10.0), init_value=3.14),
        Parameter('para1', ValNumber(-10.0, 10.0), init_value=3.14),
        Parameter('para2', ValNumber(-5.0, 5.0), init_value=3.14),
    ]:
        dev.add_parameter(para)
    for child in [
        Device('child0'), Device('child1')
    ]:
        dev.add_child(child)
    dev.child0.add_parameter(Parameter('para3', ValNumber(0.0, 1.0)))
    pprint.pprint(dev.snapshot())
    print(f'Parameter values: {dev.para0()}, {dev.para1()}, '
          f'{dev.para2()}, {dev.child0.para3()}')
    dev.para0(7.8)
    dev.para1(-7.8)
    dev.para2(-0.123344)
    dev.child0.para3(0.5)
    print('After setting')
    print(f'Parameter values: {dev.para0()}, {dev.para1()}, '
          f'{dev.para2()}, {dev.child0.para3()}')
    dev.set_parameters({'para0': 5.0, 'para1': 0.1, 'child0.para3': 0.97})
    print('After batch setting')
    print(f'Parameter values: {dev.para0()}, {dev.para1()}, '
          f'{dev.para2()}, {dev.child0.para3()}')

    class _BuilderDemo(DeviceBuilder):
        def __init__(self) -> None:
            super().__init__('demo')

        def build(self, name: str, **_) -> Device:
            dev = Device(name)
            dev.add_parameter(
                Parameter('percentage', ValNumber(0.0, 100.0), init_value=3.14))
            return dev

    builder = _BuilderDemo()  # create a demo builder
    print(f'Create demo builder {builder}')
    dev2 = builder.build('built')
    print(f'Use builder to generate a device')
    pprint.pprint(dev2.snapshot())
    print('Register the builder')
    register_device_builder(builder)
    print(f'Get builder with model {builder.model}: '
          f'{get_device_builder(builder.model)}')
    print(f'Get builder with model 123: {get_device_builder("123")}')
