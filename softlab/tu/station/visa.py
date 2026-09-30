"""Drivers for VISA-based devices"""

from typing import (
    Any,
    Dict,
    Optional,
    Callable,
)
from softlab.tu.station.parameter import Parameter
import pyvisa as visa
import logging

_logger = logging.getLogger(__name__)


class VisaHandle():
    """
    Simple handle of VISA connection

    ``VisaHandle`` mirrors the TU-004 lifecycle outcomes (``supports``,
    ``initialized``, ``prepare``, ``cleanup`` and resource ownership)
    without inheriting ``Device``: a ``VisaHandle`` is a connection
    handle, not a device container.

    Properties:
    - address --- address of visa device, read-only
    - timeout --- time-out time, forwarded raw to the PyVISA resource;
      the legacy docstring described the unit as seconds while the
      value was forwarded unchanged (OBS-001), resolved by the explicit
      ``timeout_seconds`` property
    - timeout_seconds --- time-out time in seconds, converting to/from
      the PyVISA millisecond convention (x1000) in both directions
    - read_termination --- termination in reading
    - write_termination --- termination in writing
    - initialized --- whether the handle currently holds a valid resource
    - stopped --- device-confirmed stop latch (see ``confirm_stop``)

    Public Methods:
    - supports --- detect lifecycle capabilities, no I/O
    - open --- open device
    - prepare --- re-open the resource after cleanup (repeatable cycle)
    - close --- close device (legacy alias of ``cleanup``)
    - cleanup --- release the owned resource, idempotent
    - borrow --- adopt an externally owned resource (classmethod)
    - clear --- clear visa buffer
    - read --- read as string
    - read_raw --- read as raw bytes
    - write --- write as string
    - write_raw --- write as raw bytes
    - query --- query as string
    - abort --- request interruption of the in-flight operation
    - confirm_stop --- ask the device for a completion confirmation
    """

    def __init__(self,
                 address: str, visalib: Optional[str] = None,
                 timeout: Optional[float] = None,
                 timeout_seconds: Optional[float] = 5.0,
                 read_termination: Optional[str] = '\n',
                 write_termination: Optional[str] = '\n',
                 device_clear: bool = True) -> None:
        """
        Initialization

        Args:
        - address --- address of visa device
        - visalib --- specified visa lib, use default (ni) if None
        - timeout --- time-out time forwarded raw to the PyVISA
          resource; ``timeout=None`` (the default) means "not
          specified": the documented seconds default applies and the
          resource receives ``timeout_seconds * 1000`` milliseconds
          (5000 ms by default). An explicitly supplied ``timeout`` is
          forwarded to the PyVISA resource unchanged and is never
          rescaled; when both ``timeout`` and ``timeout_seconds`` are
          supplied, the explicit raw ``timeout`` wins and
          ``timeout_seconds`` is ignored. Note the legacy docstring
          described ``timeout``'s unit as seconds while the value was
          forwarded raw; that unit discrepancy is OBS-001, resolved by
          this sentinel contract and the explicit ``timeout_seconds``
          property
        - timeout_seconds --- time-out time in seconds, applied as
          milliseconds on the resource when ``timeout`` is not given;
          ``None`` disables the timeout (the resource receives ``None``)
        - read_termination --- termination in reading
        - write_termination --- termination in writing
        - device_clear --- whether to clear device buffer

        If anything after resource acquisition fails during construction
        (device clear or a timeout/termination assignment), the acquired
        resource is closed exactly once and the original exception
        object propagates unchanged; no open resource is leaked.
        """
        self._backend: str = ''
        self._lib = visalib if isinstance(visalib, str) else None
        self._resource: Optional[visa.resources.MessageBasedResource] = None
        self._manager: Optional[visa.ResourceManager] = None
        self._initialized: bool = False
        self._owns_resource: bool = True
        self._stopped: bool = False
        self._device_clear: bool = bool(device_clear)
        self._read_termination: Optional[str] = read_termination
        self._write_termination: Optional[str] = write_termination
        self._timeout_raw: Optional[float] = timeout
        self._timeout_seconds: Optional[float] = timeout_seconds

        address = str(address)
        if len(address) > 0 and '@' in address:
            address, lib_in_addr = address.split('@')
            if self._lib is None:
                self._lib = '@' + lib_in_addr
        self._address = address

        try:
            if self._lib:
                _logger.info(f'Opening PyVISA resource with {self._lib}')
                self._manager = visa.ResourceManager(self._lib)
                self._backend = self._lib.split('@')[1]
            else:
                _logger.info('Opening PyVISA resource with default backend')
                self._manager = visa.ResourceManager()
                self._backend = 'ni'
            self._acquire()
        except Exception as e:
            _logger.info(f'Failed to connect {address}')
            raise e

    def _acquire(self) -> None:
        """
        Acquire the resource: open, type-check, configure

        Single acquisition site shared by ``__init__`` and
        ``prepare()``: opens the resource through the retained resource
        manager, rejects non-message-based resources (closing them once,
        the legacy path, outside the failed-init guard), then replays
        the stored construction configuration (device clear flag,
        timeout, both terminations).

        If anything after resource acquisition fails, the acquired
        resource is closed exactly once (a failing close is logged and
        suppressed, never masking the original error) and the original
        exception object propagates unchanged (OBS-003).

        Args:
        - None

        Returns:
        None.

        Errors:
        - ``TypeError`` --- the opened resource is not a
          ``MessageBasedResource`` (legacy rejection path)
        - any configuration failure propagates unchanged after the
          acquired resource is closed exactly once

        Side-effects:
        Sets ``self._resource`` and ``self._initialized`` on success;
        performs device I/O (open, optional clear, assignments).
        """
        _logger.info(f'Opening PyVISA resource at {self._address}')
        resource = self._manager.open_resource(self._address)
        if not isinstance(resource, visa.resources.MessageBasedResource):
            resource.close()
            raise TypeError(
                f'{__class__} only support MessageBasedResource '
                f'instead of {type(resource)}')
        try:
            if self._device_clear:
                resource.clear()
            if self._timeout_raw is not None:
                resource.timeout = self._timeout_raw
            else:
                seconds = self._timeout_seconds
                resource.timeout = None if seconds is None \
                    else seconds * 1000
            resource.read_termination = self._read_termination
            resource.write_termination = self._write_termination
        except Exception:
            try:
                resource.close()
            except Exception:
                _logger.warning(
                    'Failed to close resource after initialization failure',
                    exc_info=True)
            raise
        self._resource = resource
        self._initialized = True

    @property
    def address(self) -> str:
        """Get device address"""
        return self._address

    @property
    def initialized(self) -> bool:
        """
        Whether the handle currently holds a valid, ready resource

        ``True`` after a successful eager open (or ``borrow``), ``False``
        after ``cleanup()`` until a successful ``prepare()``.

        Args:
        - None

        Returns:
        The readiness flag.

        Errors:
        None.

        Side-effects:
        None --- performs no I/O.
        """
        return self._initialized

    def supports(self, capability: str) -> bool:
        """
        Detect whether a lifecycle capability is supported, without I/O

        The capability vocabulary is ``('connection', 'prepare',
        'cleanup')`` and all three report ``True``; ``'connection'`` is
        ``True`` because a ``VisaHandle`` *is* a connection (a deliberate
        mirror-divergence from the ``Device`` base vocabulary, which
        answers ``False`` for ``'connection'``). Unknown names ---
        including the empty string --- report ``False``. Detection is
        case-sensitive, performs no I/O and never raises.

        Args:
        - capability --- capability name, case-sensitive

        Returns:
        ``True`` for ``'connection'``/``'prepare'``/``'cleanup'``,
        ``False`` otherwise.

        Errors:
        None.

        Side-effects:
        None --- never touches the resource.
        """
        return capability in ('connection', 'prepare', 'cleanup')

    def prepare(self) -> None:
        """
        Prepare the handle: re-open the resource after ``cleanup()``

        A no-op while the handle is already initialized. Otherwise the
        handle re-runs acquisition through the retained resource manager
        with the construction-time configuration re-applied (address,
        device clear flag, timeout rule and both terminations), making
        the lifecycle a plain repeatable cycle. If the re-open fails,
        the original exception object propagates unchanged, any resource
        acquired before the failure is closed exactly once by the
        acquisition guard, ``initialized`` stays ``False`` and a later
        ``prepare()`` retries --- there is no failed-pending state.

        On a borrowed handle there is nothing to re-open (no address or
        manager was retained), so ``prepare()`` raises
        ``RuntimeError('Cannot re-open a borrowed visa resource')``.

        Lifecycle methods (``prepare``, ``cleanup``, ``supports``,
        ``initialized``, ``stopped``) are single-threaded in the TU-004
        sense: calling ``cleanup()`` while an operation is in flight is
        outside the contract. Operation serialization applies to the
        five I/O operations and ``confirm_stop()`` only.

        Args:
        - None

        Returns:
        None.

        Errors:
        - ``RuntimeError`` --- borrowed handle, nothing to re-open
        - any acquisition failure propagates unchanged (see
          ``_acquire()``)

        Side-effects:
        Re-opens and re-configures the resource on success.
        """
        if self._initialized:
            return
        if not self._owns_resource:
            raise RuntimeError('Cannot re-open a borrowed visa resource')
        self._acquire()

    def cleanup(self) -> None:
        """
        Release the resource and discharge the lifecycle state

        Idempotent: the release runs at most once per acquisition cycle.
        An owned resource is closed exactly once; a borrowed resource is
        never closed by the non-owner. State is discharged before the
        release, so a failing ``resource.close()`` propagates unchanged
        and is never implicitly retried. ``stopped`` is reset for the
        next acquisition cycle.

        Lifecycle methods (``prepare``, ``cleanup``, ``supports``,
        ``initialized``, ``stopped``) are single-threaded in the TU-004
        sense: calling ``cleanup()`` while an operation is in flight is
        outside the contract.

        Args:
        - None

        Returns:
        None.

        Errors:
        A failing resource ``close()`` propagates unchanged; the
        already-discharged state is not re-armed.

        Side-effects:
        Sets ``_resource`` to ``None`` and ``initialized``/``stopped``
        to ``False``; closes the owned resource exactly once.
        """
        resource = self._resource
        self._resource = None
        self._initialized = False
        self._stopped = False
        if resource is not None and self._owns_resource:
            resource.close()

    @classmethod
    def borrow(cls, resource: visa.resources.MessageBasedResource
               ) -> 'VisaHandle':
        """
        Adopt an externally owned, already-open message-based resource

        Adoption is non-intrusive: no I/O, no ``clear()``, no
        configuration changes --- the borrower uses the resource exactly
        as the owner configured it. The new handle is a non-owner:
        ``cleanup()``/``close()`` never close the borrowed resource, and
        ``prepare()`` is unavailable (it raises ``RuntimeError``, as
        there is nothing to re-open). ``initialized`` is ``True`` from
        adoption. ``address`` is informational only (the resource's
        ``resource_name`` when present, else ``''``).

        Args:
        - resource --- an open ``MessageBasedResource`` owned elsewhere

        Returns:
        A non-owning ``VisaHandle`` wrapping ``resource``.

        Errors:
        - ``TypeError`` --- ``resource`` is not a
          ``MessageBasedResource`` (mirrors the constructor policy)

        Side-effects:
        None --- performs no device I/O and no configuration writes.
        """
        if not isinstance(resource, visa.resources.MessageBasedResource):
            raise TypeError(
                f'{cls} only support MessageBasedResource '
                f'instead of {type(resource)}')
        handle = cls.__new__(cls)
        handle._lib = None
        handle._backend = ''
        handle._manager = None
        handle._resource = resource
        handle._address = getattr(resource, 'resource_name', '') or ''
        handle._initialized = True
        handle._owns_resource = False
        handle._stopped = False
        handle._device_clear = False
        handle._read_termination = None
        handle._write_termination = None
        handle._timeout_raw = None
        handle._timeout_seconds = 5.0
        return handle

    @property
    def timeout(self) -> Optional[float]:
        """Get timeout in seconds"""
        if self._resource:
            return self._resource.timeout
        return None

    @timeout.setter
    def timeout(self, timeout: Optional[float]) -> None:
        """Set timeout in seconds"""
        if self._resource:
            self._resource.timeout = timeout

    @property
    def timeout_seconds(self) -> Optional[float]:
        """
        Get timeout in seconds, converted from the PyVISA millisecond
        convention (resource value / 1000)

        ``None`` maps to ``None`` (disabled timeout) and a handle
        without a resource reads ``None``, mirroring the legacy
        ``timeout`` getter posture. No rounding, no clamping, no
        locking --- the timeout properties are direct pass-throughs,
        not serialized operations.

        Args:
        - None

        Returns:
        The resource timeout in seconds, or ``None``.

        Errors:
        None.

        Side-effects:
        None --- performs no I/O beyond the attribute read.
        """
        if self._resource:
            value = self._resource.timeout
            return None if value is None else value / 1000
        return None

    @timeout_seconds.setter
    def timeout_seconds(self, timeout: Optional[float]) -> None:
        """
        Set timeout in seconds, converted to the PyVISA millisecond
        convention (value x 1000)

        ``None`` disables the timeout (the resource receives ``None``).
        No rounding, no clamping, no locking --- the timeout properties
        are direct pass-throughs, not serialized operations.

        Args:
        - timeout --- timeout in seconds, or ``None`` to disable

        Returns:
        None.

        Errors:
        None.

        Side-effects:
        Writes the resource timeout in milliseconds.
        """
        if self._resource:
            self._resource.timeout = None if timeout is None \
                else timeout * 1000

    @property
    def read_termination(self) -> Optional[str]:
        """Get reading termination"""
        if self._resource:
            return self._resource.read_termination

    @read_termination.setter
    def read_termination(self, termination: Optional[str]) -> None:
        """Set reading termination"""
        if self._resource:
            self._resource.read_termination = termination

    @property
    def write_termination(self) -> Optional[str]:
        """Get writing termination"""
        if self._resource:
            return self._resource.write_termination

    @write_termination.setter
    def write_termination(self, termination: Optional[str]) -> None:
        """Set writing termination"""
        if self._resource:
            self._resource.write_termination = termination

    def open(self) -> None:
        """Open device"""
        if self._resource:
            self._resource.open()

    def clear(self) -> None:
        """Clear buffer"""
        if self._resource:
            self._resource.clear()

    def close(self) -> None:
        """
        Close device

        ``close()`` is the legacy alias of ``cleanup()``: it is
        idempotent and ownership-aware. This differs from the pre-TU-006
        behavior, where a second ``close()`` invoked the resource's
        ``close()`` again; a second ``close()`` is now a no-op. No
        production caller depended on the old double-close behavior.

        Args:
        - None

        Returns:
        None.

        Errors:
        Same as ``cleanup()``.

        Side-effects:
        Same as ``cleanup()``: the owned resource is closed exactly
        once; a borrowed resource is never closed.
        """
        self.cleanup()

    def read(self, encoding: Optional[str] = None) -> str:
        """Read as string"""
        if self._resource:
            return self._resource.read(encoding=encoding)
        raise RuntimeError('Invalid visa resource')

    def read_raw(self, size: Optional[int] = None) -> bytes:
        """Read as raw bytes"""
        if self._resource:
            return self._resource.read_raw(size=size)
        raise RuntimeError('Invalid visa resource')

    def write(self, message: str, encoding: Optional[str] = None) -> int:
        """Write as string"""
        if self._resource:
            return self._resource.write(message=message, encoding=encoding)
        raise RuntimeError('Invalid visa resource')

    def write_raw(self, message: bytes) -> int:
        """Write as raw bytes"""
        if self._resource:
            return self._resource.write(message=message)
        raise RuntimeError('Invalid visa resource')

    def query(self, command: str, delay: Optional[float] = None) -> str:
        """Query as string"""
        if self._resource:
            return self._resource.query(command, delay)
        raise RuntimeError('Invalid visa resource')


class VisaParameter(Parameter):
    """
    Simple parameter in visa device, the value can be set and/or get by
    using simple commands.

    The stored value of a parameter in VISA device is generally a string.
    So ``decoder`` and/or ``encoder`` are needed to parse between stored string
    and meaningful accessed value.
    """

    def __init__(self, name: str, handle: VisaHandle,
                 get_cmd: str = '', set_cmd: str = '',
                 read_after_setting: bool = False,
                 encoding: Optional[str] = None,
                 query_delay: Optional[float] = None,
                 pre_cmd: str = '', post_cmd: str = '',
                 decoder: Optional[Callable] = None,
                 encoder: Optional[Callable] = None,
                 **kwargs) -> None:
        """
        Initialization

        Args:
        - name --- parameter name
        - handle --- visa device handle
        - get_cmd --- command to get value, use buffered value if empty
        - set_cmd --- command to set value, no actual setting if empty
        - read_after_setting --- whether to perform a reading after setting
        - encoding --- encoding in read and write operations, optional
        - query_delay --- delay when querying value from device
        - decoder --- function to parse value to device readable string
        - encoder --- function to parse got string into meaningful value
        """
        super().__init__(name,
                         decoder=decoder,
                         encoder=encoder,
                         before_set=self.before_set,
                         before_get=self.before_get,
                         **kwargs)
        if not isinstance(handle, VisaHandle):
            raise TypeError(f'Invalid visa handle {type(handle)}')
        self._handle = handle
        self._get_cmd = str(get_cmd)
        self._set_cmd = str(set_cmd)
        self._read_after_setting = read_after_setting
        self._encoding = encoding
        self._delay = query_delay
        self._pre_cmd = str(pre_cmd)
        self._post_cmd = str(post_cmd)

    @property
    def handle(self) -> VisaHandle:
        return self._handle

    def before_set(self, current: Any, next: Any) -> None:
        if len(self._set_cmd) > 0:
            if len(self._pre_cmd) > 0:
                self._handle.write(self._pre_cmd)
                if self._read_after_setting:
                    self._handle.read()
            self._handle.write(self._set_cmd.format(next), self._encoding)
            if self._read_after_setting:
                self._handle.read()
            if len(self._post_cmd) > 0:
                self._handle.write(self._post_cmd)
                if self._read_after_setting:
                    self._handle.read()

    def before_get(self, value: Any) -> Any:
        if len(self._get_cmd) > 0:
            if len(self._pre_cmd) > 0:
                self._handle.write(self._pre_cmd)
                if self._read_after_setting:
                    self._handle.read()
            value = self._handle.query(self._get_cmd, self._delay)
            if len(self._post_cmd) > 0:
                self._handle.write(self._post_cmd)
                if self._read_after_setting:
                    self._handle.read()
            return value
        return value

    def snapshot(self) -> dict:
        return {
            **super().snapshot(),
            'get_cmd': self._get_cmd,
            'set_cmd': self._set_cmd,
        }


class VisaCommand(Parameter):
    """
    Simple visa command as a read-only command

    Public Methods:
    - execute --- perform the command's write explicitly (TU-003)
    - describe_operation --- report side-effect semantics without I/O (TU-003)
    """

    def __init__(self, name: str,
                 handle: VisaHandle, cmd: str,
                 **kwargs) -> None:
        """
        Initialization

        Args:
        - name --- parameter name
        - handle --- visa device handle
        - cmd --- command string, non-empty
        """
        super().__init__(name,
                         settable=False,
                         before_get=self.before_get,
                         init_value=True,
                         **kwargs)
        if not isinstance(handle, VisaHandle):
            raise TypeError(f'Invalid visa handle {type(handle)}')
        self._handle = handle
        cmd = str(cmd)
        if len(cmd) == 0:
            raise ValueError('Empty command')
        self._cmd = cmd

    @property
    def handle(self) -> VisaHandle:
        return self._handle

    def before_get(self, value: Any) -> Any:
        self._handle.write(self._cmd)
        return value

    def execute(self) -> Any:
        """
        Perform the command's write explicitly and report the stored value

        The explicit operation path is not gated by the legacy gettable /
        settable permissions of ``Parameter``: executing a ``VisaCommand``
        is always permitted, because the command's whole purpose is to be
        executed. The write is performed directly and does not invoke
        ``before_get``, ``get()`` or ``__call__``.

        Args:
        - None

        Returns:
        The stored value (``self._value``) verbatim. No encoder or decoder
        is applied; for the standard configuration the stored value is the
        construction-time ``init_value`` (``True``) and never changes.

        Errors:
        Any exception raised by the handle's write propagates unchanged,
        with the original exception object preserved (no wrapping). This
        includes transport failures (e.g. ``pyvisa.errors.VisaIOError``)
        and the ``RuntimeError('Invalid visa resource')`` raised when the
        handle's resource is unavailable; in both cases exactly one write
        was attempted before the exception.

        Side-effects:
        Exactly one command write per call (``self._handle.write(
        self._cmd)``, positional), no reads, no other device I/O. Two
        calls write exactly twice.
        """
        self._handle.write(self._cmd)
        return self._value

    def describe_operation(self) -> Dict[str, Any]:
        """
        Describe the operation's side-effect semantics without executing it

        Returns a fresh, detached, JSON-compatible dict on every call, in
        a schema namespace separate from TU-002 ``describe()`` (version 1
        of the operation-description schema, not the structure-description
        schema). Only four fields exist: ``schema_version`` (int, 1),
        ``name`` (the command's parameter name), ``effect`` (``'write'``,
        the side-effect kind of one invocation), and ``executions_per_call``
        (int, 1, the number of command writes performed by one ``execute()``
        call).

        Args:
        - None

        Returns:
        Fresh version-1 operation description dict.

        Errors:
        None.

        Side-effects:
        None --- performs no device I/O, never touches the handle, and never
        includes the command string or the resource address in the result.
        """
        return {
            'schema_version': 1,
            'name': self._name,
            'effect': 'write',
            'executions_per_call': 1,
        }

    def snapshot(self) -> dict:
        return {
            **super().snapshot(),
            'cmd': self._cmd,
        }


class VisaIDN(VisaParameter):

    def __init__(self, name: str, handle: VisaHandle, **kwargs) -> None:
        super().__init__(name, handle,
                         get_cmd='*IDN?',
                         encoder=self.interprete,
                         settable=False,
                         **kwargs)

    def interprete(self, value: str) -> Dict[str, str]:
        parts = value.split(',')
        info = {}
        others = ''
        for idx, val in enumerate(parts):
            if idx == 0:
                info['vendor'] = val.strip()
            elif idx == 1:
                info['model'] = val.strip()
            elif idx == 2:
                info['serial'] = val.strip()
            elif idx == 3:
                info['revision'] = val.strip()
            else:
                if len(others) == 0:
                    others = val.strip()
                else:
                    others = f'{others} {val.strip()}'
        if len(others) > 0:
            info['others'] = others
        return info
