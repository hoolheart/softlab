"""Acceptance tests for opt-in device lifecycle and capabilities (TU-004).

Proposed contract under test (test surface for developer/designer review;
no production implementation is asserted to exist):

- ``Device.supports(capability) -> bool`` --- explicit, side-effect-free
  capability detection. The base device is virtual: it reports ``False``
  for ``'connection'`` and any unknown capability, and ``True`` for the
  lifecycle capabilities ``'prepare'`` and ``'cleanup'``.
- ``Device.prepare() -> None`` --- explicit preparation. For a virtual
  device it is a no-op that acquires and opens nothing (no PyVISA resource
  manager, no connection); subclasses opt in by overriding the protected
  ``_prepare_impl()`` hook.
- ``Device.initialized -> bool`` --- explicit readiness. ``False`` on a
  fresh device, ``True`` only after a successful ``prepare()``, and
  ``False`` again after ``cleanup()`` or when preparation failed.
- ``Device.cleanup() -> None`` --- explicit, idempotent release of
  *owned* resources only, via the protected ``_cleanup_impl()`` hook.
  Cleanup is safe before any successful preparation (a non-owner releases
  nothing, so borrowed resources are never released by a non-owner), safe
  after a failed preparation (partial acquisition is released exactly
  once), and repeated cleanup after success invokes the release hook at
  most once. A failed ``_prepare_impl()`` propagates its original
  exception and leaves the device not initialized.
- Legacy behavior is unchanged: construction, parameter/child management,
  delegated access and snapshots remain exactly as before, and a plain
  ``Device`` performs no connection operation at any point.
"""

import unittest
from unittest.mock import Mock, patch

from softlab.tu.station import Device, Parameter


class _OwningDevice(Device):
    """Test device that acquires a mock resource during preparation."""

    def __init__(self, name, resource):
        super().__init__(name)
        self._resource = resource
        self.cleanup_calls = 0

    def _prepare_impl(self):
        self._resource.acquire()

    def _cleanup_impl(self):
        self.cleanup_calls += 1
        self._resource.release()


class _FailingDevice(Device):
    """Test device whose preparation fails after acquiring a resource."""

    def __init__(self, name, resource, error):
        super().__init__(name)
        self._resource = resource
        self._error = error
        self.cleanup_calls = 0

    def _prepare_impl(self):
        self._resource.acquire()
        raise self._error

    def _cleanup_impl(self):
        self.cleanup_calls += 1
        self._resource.release()


class _BorrowingDevice(Device):
    """Test device holding an externally-owned (borrowed) resource."""

    def __init__(self, name, borrowed):
        super().__init__(name)
        self._borrowed = borrowed
        self.cleanup_calls = 0

    def _cleanup_impl(self):
        self.cleanup_calls += 1
        self._borrowed.close()


class LifecycleTests(unittest.TestCase):
    def test_virtual_device_prepare_requires_no_connection(self):
        device = Device("virtual")
        # Resolve API before error assertions, so missing API is the red cause.
        supports = device.supports
        prepare = device.prepare
        self.assertFalse(supports("connection"))
        with patch(
            "softlab.tu.station.visa.visa.ResourceManager"
        ) as manager:
            prepare()
        manager.assert_not_called()  # no meaningless connection operation
        # Virtual device remains fully usable without any connection.
        self.assertEqual(device.snapshot()["name"], "virtual")

    def test_readiness_is_explicit_across_lifecycle(self):
        device = Device("virtual")
        initialized = device.initialized
        self.assertFalse(initialized)  # not ready before preparation
        device.prepare()
        self.assertTrue(device.initialized)
        device.cleanup()
        self.assertFalse(device.initialized)

    def test_unsupported_capability_detectable_without_side_effects(self):
        device = Device("virtual")
        for capability in ("connection", "trigger", "raw-bus", ""):
            self.assertFalse(device.supports(capability))
        self.assertTrue(device.supports("prepare"))
        self.assertTrue(device.supports("cleanup"))
        with patch(
            "softlab.tu.station.visa.visa.ResourceManager"
        ) as manager:
            device.supports("connection")
        manager.assert_not_called()  # detection performs no I/O

    def test_repeated_cleanup_is_idempotent_after_success(self):
        resource = Mock()
        device = _OwningDevice("owned", resource)
        device.prepare()
        self.assertTrue(device.initialized)
        device.cleanup()
        device.cleanup()
        self.assertEqual(device.cleanup_calls, 1)
        self.assertEqual(resource.release.call_count, 1)

    def test_cleanup_before_successful_prepare_is_safe_noop(self):
        resource = Mock()
        device = _OwningDevice("owned", resource)
        device.cleanup()  # never prepared: non-owner releases nothing
        self.assertEqual(device.cleanup_calls, 0)
        resource.release.assert_not_called()

    def test_failed_preparation_propagates_and_stays_not_ready(self):
        error = RuntimeError("prepare failed")
        device = _FailingDevice("failing", Mock(), error)
        with self.assertRaises(RuntimeError) as raised:
            device.prepare()
        self.assertIs(raised.exception, error)  # original cause preserved
        self.assertFalse(device.initialized)

    def test_failed_preparation_cleanup_releases_partial_acquisition_once(self):
        resource = Mock()
        device = _FailingDevice("failing", resource, RuntimeError("boom"))
        with self.assertRaises(RuntimeError):
            device.prepare()
        resource.acquire.assert_called_once_with()
        device.cleanup()
        self.assertEqual(device.cleanup_calls, 1)
        self.assertEqual(resource.release.call_count, 1)
        device.cleanup()  # still idempotent after failure
        self.assertEqual(device.cleanup_calls, 1)

    def test_borrowed_resource_not_released_by_non_owner(self):
        borrowed = Mock()  # owned by an external party, never acquired here
        device = _BorrowingDevice("borrower", borrowed)
        device.cleanup()  # non-owner cleanup must not release
        self.assertEqual(device.cleanup_calls, 0)
        borrowed.close.assert_not_called()

    def test_legacy_device_behavior_unchanged(self):
        # Compatibility guard: passes against unchanged legacy code.
        with patch(
            "softlab.tu.station.visa.visa.ResourceManager"
        ) as manager:
            device = Device("legacy")
            device.add_parameter(Parameter("gain"))
            child = Device("channel")
            child.add_parameter(Parameter("offset"))
            device.add_child(child)
        manager.assert_not_called()  # construction never opened a connection
        self.assertIs(device.parameter("gain").owner, device)
        self.assertIs(device.child("channel"), child)
        self.assertEqual(device.channel.offset(), None)
        snapshot = device.snapshot()
        self.assertEqual(snapshot["name"], "legacy")
        self.assertEqual(list(snapshot["parameters"]), ["gain"])
        self.assertEqual(list(snapshot["children"]), ["channel"])
        device.set_parameters({"gain": 3})
        self.assertEqual(device.gain(), 3)


if __name__ == "__main__":
    unittest.main()
