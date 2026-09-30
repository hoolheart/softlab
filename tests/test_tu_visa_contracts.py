"""Acceptance tests for VISA resource and operation outcome contracts (TU-006).

Proposed contract under test (test surface for developer/designer review; no
production implementation is asserted to exist). Legacy ``VisaHandle``
construction and usage stay compatible: address-based construction remains
eager open+clear with ``device_clear`` honored, legacy ``timeout`` raw
numeric forwarding is unchanged (OBS-001 stays a documented legacy path),
transport errors already propagate as identical objects, and legacy
``close()`` remains available.

Group A --- lifecycle adoption and ownership (closes OBS-003):
- ``VisaHandle`` adopts the approved TU-004 lifecycle contract compatibly:
  ``supports(capability)`` reports ``'connection'``/``'prepare'``/
  ``'cleanup'`` as ``True`` and answers unknown names (including ``''``)
  with ``False`` without performing any I/O; ``initialized`` is ``True``
  after the unchanged eager successful open; ``cleanup()`` releases the
  owned resource exactly once and is idempotent; ``prepare()`` is a no-op
  while initialized and re-opens the resource after ``cleanup()`` so the
  lifecycle is a repeatable cycle.
- Failed initialization cleans up: if anything after resource acquisition
  fails during construction (``clear()`` or a timeout/termination
  assignment), the acquired resource is closed exactly once before the
  original exception object propagates (no leaked open resource).
- Ownership: ``VisaHandle.borrow(resource)`` adopts an externally owned,
  already-open message-based resource. Borrowing performs no device I/O
  and no configuration changes (the borrower uses the resource exactly as
  the owner configured it), and a non-owner ``cleanup()``/``close()`` never
  closes the borrowed resource. Default address-based construction owns
  its resource and closes it.

Group B --- timeout units and defaults (resolves OBS-001 additively):
- Construction contract (developer-recommended resolution of the
  default-construction contradiction, review issue 1): the legacy
  ``timeout`` parameter becomes ``Optional[float] = None`` where ``None``
  is the sentinel meaning "not specified"; the seconds default is
  ``timeout_seconds: float = 5.0``, a new constructor keyword. When
  ``timeout`` is ``None`` the seconds default applies (resource value
  5000 ms); when ``timeout`` is given, the legacy raw path forwards it
  untouched (an explicitly supplied raw ``timeout`` is never rescaled,
  per OBS-001 "do not silently rescale existing callers"). Guard case 1
  pins raw forwarding of an explicitly supplied ``timeout=5.0``; guard
  case 6 constructs its first block with an explicit ``timeout=5.0``
  (mirroring case 1) to pin explicit-value raw forwarding, then pins
  raw forwarding of set/get on the legacy ``timeout`` property.
- Additive, explicit ``timeout_seconds`` property expresses the timeout in
  seconds and converts to the PyVISA millisecond convention (x1000) on the
  resource, in both directions; ``None`` disables the timeout. The
  documented default is 5.0 seconds (resource value 5000 ms), applied on
  default construction via the ``timeout_seconds`` keyword default.
- An operation whose completion exceeds the configured timeout surfaces
  the original ``VisaIOError`` (e.g. ``VI_ERROR_TMO``) object unchanged.

Group C --- blocking, concurrency, interruption:
- Operations block until completion or timeout (timed with mocks; the
  ``@sim`` backend has no response-delay support).
- Concurrent operations are serialized by an internal guard: two threads
  must not interleave a single resource call.
- ``abort()`` requests interruption of the in-flight operation. The
  aborted call terminates promptly and propagates its underlying error
  object unchanged. The abort report separates the software abort from a
  confirmed physical stop: ``report.aborted`` is ``True`` while
  ``report.stopped`` and the handle's ``stopped`` flag remain ``False`` ---
  an abort request never claims the equipment stopped and issues no device
  writes to force a stop. Aborting with nothing in flight is a quiet
  no-op.
- ``confirm_stop()`` is the only path that may report a physical stop: it
  performs exactly one completion-status query (``'*OPC?'``), returns
  ``True`` only when the device confirms completion, and propagates
  transport errors unchanged.

Group D --- error causes and real resource outcomes:
- Transport errors propagate as the identical exception object (guarded).
- Operations on a cleaned-up handle raise ``RuntimeError`` matching
  ``'Invalid visa resource'`` with zero resource I/O; ``prepare()``
  recovers the handle.
- OBS-002 disposition (focused regression, group D): ``write_raw`` must
  target the resource's raw ``write_raw`` with the identical bytes object
  --- current production forwards the bytes to the string ``write``
  (``resource.write(message=bytes)``), which is the recorded defect this
  case pins and turns red on until fixed.
- End-to-end ``@sim`` verification of resource outcomes: real simulator
  round trips, real timeout-unit conversion on a real resource, non-owner
  close behaviour on a real resource, and device-confirmed stop against
  the simulated device.

Designer may refine naming (``borrow``, ``timeout_seconds``, the abort
report type, ``confirm_stop``) through an explicit test-plan revision; the
acceptance semantics above are the contract under test. All resources in
this file are mocks or the ``@sim`` simulator; no real hardware is
accessed.
"""

import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pyvisa

from softlab.tu.station import VisaHandle

SIMULATOR = str(Path(__file__).parent / "visa_sim.yaml") + "@sim"


def make_resource(testcase, address="TEST", **kwargs):
    """Mock an owned handle: patched resource manager returning a mock
    message-based resource, and construct the handle."""
    resource = Mock(spec=pyvisa.resources.MessageBasedResource)
    manager = Mock()
    manager.open_resource.return_value = resource
    patcher = patch(
        "softlab.tu.station.visa.visa.ResourceManager", return_value=manager)
    factory = patcher.start()
    testcase.addCleanup(patcher.stop)
    handle = VisaHandle(address, **kwargs)
    return handle, resource, manager, factory


class VisaLifecycleTests(unittest.TestCase):
    """Group A: lifecycle adoption, failed-init cleanup, ownership."""

    def test_legacy_eager_open_and_close_unchanged(self):
        # Compatibility guard: passes against unchanged production code.
        # Pins raw forwarding of an EXPLICIT legacy timeout=5.0 (review
        # issue 1): an explicitly supplied raw timeout must forward
        # unchanged (OBS-001: no silent rescaling of existing callers).
        handle, resource, manager, factory = make_resource(
            self, "TEST@sim", timeout=5.0)
        factory.assert_called_once_with("@sim")
        manager.open_resource.assert_called_once_with("TEST")
        resource.clear.assert_called_once_with()
        self.assertEqual(handle.address, "TEST")
        self.assertEqual(resource.timeout, 5.0)  # raw legacy forwarding
        handle.close()
        resource.close.assert_called_once_with()

    def test_capability_detection_and_readiness_without_io(self):
        handle, resource, _, _ = make_resource(self, "TEST@sim")
        supports = handle.supports
        self.assertTrue(supports("connection"))
        self.assertTrue(supports("prepare"))
        self.assertTrue(supports("cleanup"))
        for unknown in ("trigger", "", "Connection", "CLEANUP"):
            self.assertFalse(supports(unknown))
        initialized = handle.initialized
        self.assertTrue(initialized)
        # Capability detection performs no device I/O at any point.
        resource.reset_mock()
        self.assertFalse(supports(""))
        self.assertTrue(supports("connection"))
        resource.assert_not_called()

    def test_cleanup_idempotent_and_prepare_reopens(self):
        handle, resource, manager, _ = make_resource(self, "TEST@sim")
        prepare, cleanup = handle.prepare, handle.cleanup
        self.assertTrue(handle.initialized)
        prepare()  # no-op while initialized: no re-open, no extra clear
        manager.open_resource.assert_called_once_with("TEST")
        resource.clear.assert_called_once_with()
        cleanup()
        self.assertFalse(handle.initialized)
        resource.close.assert_called_once_with()
        cleanup()  # idempotent: release hook runs at most once
        resource.close.assert_called_once_with()
        prepare()  # repeatable cycle re-opens the resource
        self.assertTrue(handle.initialized)
        self.assertEqual(manager.open_resource.call_count, 2)

    def test_failed_initialization_closes_resource_before_propagating(self):
        # OBS-003: clear failure after acquisition must not leak the
        # resource; the original exception object propagates unchanged.
        resource = Mock(spec=pyvisa.resources.MessageBasedResource)
        manager = Mock()
        manager.open_resource.return_value = resource
        patcher = patch(
            "softlab.tu.station.visa.visa.ResourceManager",
            return_value=manager)
        self.addCleanup(patcher.stop)
        patcher.start()
        error = pyvisa.errors.VisaIOError(
            pyvisa.constants.StatusCode.error_timeout)
        resource.clear.side_effect = error
        with self.assertRaises(pyvisa.errors.VisaIOError) as raised:
            VisaHandle("TEST@sim")
        self.assertIs(raised.exception, error)
        resource.close.assert_called_once_with()

        # A failing post-acquisition assignment cleans up the same way.
        class _FailingResource(Mock):
            pass

        failing = _FailingResource(spec=pyvisa.resources.MessageBasedResource)
        setter_error = pyvisa.errors.VisaIOError(
            pyvisa.constants.StatusCode.error_system_error)

        def _raise_on_set(self, value):
            raise setter_error

        _FailingResource.timeout = property(
            lambda self: 0.0, _raise_on_set)
        manager.open_resource.return_value = failing
        with self.assertRaises(pyvisa.errors.VisaIOError) as raised:
            VisaHandle("BROKEN@sim")
        self.assertIs(raised.exception, setter_error)
        failing.close.assert_called_once_with()

    def test_borrowed_resource_is_never_closed_by_non_owner(self):
        resource = Mock(spec=pyvisa.resources.MessageBasedResource)
        borrow = VisaHandle.borrow
        handle = borrow(resource)
        self.assertTrue(handle.initialized)
        # Adoption is non-intrusive: no clear, no configuration writes.
        resource.clear.assert_not_called()
        handle.cleanup()
        resource.close.assert_not_called()
        handle.cleanup()  # idempotent, still silent
        resource.close.assert_not_called()
        # The true owner keeps full use of the borrowed resource.
        resource.query.return_value = "alive"
        self.assertEqual(resource.query("*IDN?", None), "alive")


class VisaTimeoutTests(unittest.TestCase):
    """Group B: explicit timeout units, defaults, timeout error path."""

    def test_legacy_timeout_raw_forwarding_unchanged(self):
        # Compatibility guard: passes against unchanged production code.
        # Pins raw forwarding of set/get on the legacy ``timeout``
        # property: the first block constructs with an EXPLICIT
        # ``timeout=5.0`` (mirroring guard case 1) so it pins
        # explicit-value raw forwarding and stays green under the
        # ``timeout=None`` sentinel semantics, where default construction
        # instead applies the seconds default (5000 ms on the resource,
        # pinned by case 7).
        handle, resource, manager, _ = make_resource(
            self, "TEST@sim", timeout=5.0)
        self.assertEqual(resource.timeout, 5.0)
        other = VisaHandle("CUSTOM@sim", timeout=12.5)
        self.addCleanup(other.close)
        self.assertEqual(resource.timeout, 12.5)
        other.timeout = 25.0
        self.assertEqual(resource.timeout, 25.0)
        resource.timeout = 37.5
        self.assertEqual(other.timeout, 37.5)
        other.timeout = None
        self.assertIsNone(resource.timeout)

    def test_timeout_seconds_converts_to_visa_milliseconds(self):
        handle, resource, _, _ = make_resource(self, "TEST@sim")
        timeout_seconds = handle.timeout_seconds
        # Default construction takes the seconds path (review issue 1):
        # no explicit raw timeout was supplied, so the 5.0 s default is
        # applied as 5000 ms on the resource and reads back as 5.0 s.
        self.assertEqual(resource.timeout, 5000)
        self.assertEqual(timeout_seconds, 5.0)
        handle.timeout_seconds = 2.5
        self.assertEqual(resource.timeout, 2500)
        self.assertEqual(handle.timeout_seconds, 2.5)
        resource.timeout = 4000  # owner-side change reads back in seconds
        self.assertEqual(handle.timeout_seconds, 4.0)
        handle.timeout_seconds = None
        self.assertIsNone(resource.timeout)
        self.assertIsNone(handle.timeout_seconds)
        # The explicit API does not disturb the legacy raw path.
        handle.timeout = 12.5
        self.assertEqual(resource.timeout, 12.5)

    def test_timeout_seconds_default_and_constructor_keyword(self):
        handle, resource, _, _ = make_resource(self, "TEST@sim")
        self.assertEqual(handle.timeout_seconds, 5.0)
        self.assertEqual(resource.timeout, 5000)
        quick, quick_resource, _, _ = make_resource(
            self, "FAST@sim", timeout_seconds=1.5)
        self.assertEqual(quick.timeout_seconds, 1.5)
        self.assertEqual(quick_resource.timeout, 1500)

    def test_timeout_error_propagates_original_cause(self):
        handle, resource, _, _ = make_resource(self, "TEST@sim")
        error = pyvisa.errors.VisaIOError(
            pyvisa.constants.StatusCode.error_timeout)
        resource.read.side_effect = error
        with self.assertRaises(pyvisa.errors.VisaIOError) as raised:
            handle.read()
        self.assertIs(raised.exception, error)
        resource.read.assert_called_once()
        # @sim: an explicit timeout does not disturb real outcomes.
        # Developer-escalated fix (verified root cause): make_resource's
        # patcher above replaces pyvisa.ResourceManager (a module-global
        # attribute) and is still active here, so a "real" sim handle
        # would wrap the mock resource and ``"*IDN?" in <Mock>`` would
        # raise ``TypeError: argument of type 'Mock' is not iterable``.
        # Stop all active patches before constructing the real handle;
        # the patcher's own stop at cleanup is a safe double-stop on
        # Python 3.13.
        patch.stopall()
        sim = VisaHandle("GPIB::1::INSTR", visalib=SIMULATOR,
                         read_termination="\r", write_termination="\r",
                         device_clear=False)
        try:
            self.assertEqual(sim.timeout_seconds, 5.0)
            self.assertIn("Simulated", sim.query("*IDN?"))
        finally:
            sim.close()


class VisaBlockingConcurrencyTests(unittest.TestCase):
    """Group C: blocking, serialization guard, abort/confirmed stop."""

    def test_operations_block_until_completion(self):
        handle, resource, _, _ = make_resource(self, "TEST@sim")

        def slow_read(*args, **kwargs):
            time.sleep(0.15)
            return "42"

        resource.read.side_effect = slow_read
        before = time.monotonic()
        result = handle.read()
        elapsed = time.monotonic() - before
        self.assertEqual(result, "42")
        self.assertGreaterEqual(elapsed, 0.15)  # blocked for the full call
        resource.read.assert_called_once_with(encoding=None)

    def test_concurrent_operations_are_serialized(self):
        handle, resource, _, _ = make_resource(self, "TEST@sim")
        events = []

        def tracked_query(command, delay=None):
            events.append(("start", command))
            time.sleep(0.1)
            events.append(("end", command))
            return command

        resource.query.side_effect = tracked_query
        barrier = threading.Barrier(3)
        results = {}

        def worker(name):
            barrier.wait()
            results[name] = handle.query(name, None)

        threads = [threading.Thread(target=worker, args=(name,),
                                    daemon=True)
                   for name in ("A", "B")]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(timeout=10)
        self.assertEqual(results, {"A": "A", "B": "B"})
        # Strictly serialized: start/end pairs never interleave.
        self.assertEqual([event[0] for event in events],
                         ["start", "end", "start", "end"])

    def test_abort_requests_interruption_without_claiming_stop(self):
        handle, resource, _, _ = make_resource(self, "TEST@sim")
        entered = threading.Event()
        release = threading.Event()
        error = pyvisa.errors.VisaIOError(pyvisa.constants.VI_ERROR_ABORT)

        def blocked_read(encoding=None):
            entered.set()
            if not release.wait(timeout=10):
                raise AssertionError("abort never released the read")
            raise error

        resource.read.side_effect = blocked_read
        outcome = {}

        def worker():
            try:
                handle.read()
            except Exception as exc:  # record the propagating object
                outcome["error"] = exc

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        self.assertTrue(entered.wait(timeout=10))
        try:
            report = handle.abort()
        finally:
            release.set()  # model a backend honouring the abort request
        thread.join(timeout=10)
        self.assertFalse(thread.is_alive())
        # The aborted call surfaces its underlying error object unchanged.
        self.assertIs(outcome.get("error"), error)
        # A request is not a confirmed physical stop: nothing claims the
        # equipment stopped, and no stop command is written to the device.
        self.assertTrue(report.aborted)
        self.assertFalse(report.stopped)
        self.assertFalse(handle.stopped)
        resource.write.assert_not_called()
        # Aborting with nothing in flight is a quiet no-op.
        idle = handle.abort()
        self.assertFalse(idle.aborted)
        self.assertFalse(idle.stopped)
        resource.write.assert_not_called()

    def test_confirmed_stop_requires_device_confirmation(self):
        handle, resource, _, _ = make_resource(self, "TEST@sim")
        confirm_stop = handle.confirm_stop
        self.assertFalse(handle.stopped)  # mere handle: no stop claimed
        resource.query.return_value = "0"
        self.assertFalse(confirm_stop())  # device reports still running
        self.assertFalse(handle.stopped)
        resource.query.assert_called_once_with("*OPC?", None)
        resource.query.return_value = "1"
        self.assertTrue(confirm_stop())  # device confirms completion
        self.assertTrue(handle.stopped)
        # Transport errors propagate unchanged from the confirmation probe.
        resource.reset_mock()
        error = pyvisa.errors.VisaIOError(
            pyvisa.constants.StatusCode.error_timeout)
        resource.query.side_effect = error
        with self.assertRaises(pyvisa.errors.VisaIOError) as raised:
            handle.confirm_stop()
        self.assertIs(raised.exception, error)


class VisaErrorCauseTests(unittest.TestCase):
    """Group D: original causes, defined post-cleanup errors, @sim."""

    def test_transport_errors_propagate_as_identical_objects(self):
        # Compatibility guard: passes against unchanged production code.
        handle, resource, _, _ = make_resource(self, "TEST@sim")
        error = pyvisa.errors.VisaIOError(
            pyvisa.constants.StatusCode.error_timeout)
        resource.query.side_effect = error
        with self.assertRaises(pyvisa.errors.VisaIOError) as raised:
            handle.query("READ?", 0.1)
        self.assertIs(raised.exception, error)
        resource.query.assert_called_once_with("READ?", 0.1)

    def test_cleaned_up_handle_rejects_operations_without_io(self):
        handle, resource, _, _ = make_resource(self, "TEST@sim")
        handle.cleanup()
        resource.reset_mock()
        for operation in (lambda: handle.read(),
                          lambda: handle.read_raw(),
                          lambda: handle.write("V 1"),
                          lambda: handle.write_raw(b"V 1"),
                          lambda: handle.query("V?", None)):
            with self.assertRaisesRegex(RuntimeError, "Invalid visa resource"):
                operation()
        resource.assert_not_called()  # defined error, zero device I/O
        handle.prepare()  # recovery is a supported repeatable cycle
        self.assertTrue(handle.initialized)
        resource.query.return_value = "3.5"
        self.assertEqual(handle.query("V?", None), "3.5")

    def test_write_raw_targets_raw_resource_write(self):
        # OBS-002 disposition (compatibility.md: "Resolve explicitly in
        # TU-006 with a focused regression and compatible error handling").
        # Recorded defect (visa.py at gate time): handle.write_raw forwards
        # the bytes to the resource's string write --- resource.write(
        # message=bytes) --- instead of the resource's write_raw. The
        # explicit behavior decision is the FIX direction: write_raw must
        # target resource.write_raw with the identical bytes object, the
        # return value is forwarded unchanged, and transport errors
        # propagate as the identical object. This case is RED against the
        # current defect (mirroring how TU-001 recorded defects separately
        # from intended contracts) and turns green with the fix; the
        # post-cleanup zero-I/O policy of case 15 composes with it
        # (write_raw on a cleaned-up handle already raises RuntimeError
        # before any resource call).
        handle, resource, _, _ = make_resource(self, "TEST@sim")
        message = b"V 1"
        resource.write_raw.return_value = 3
        self.assertEqual(handle.write_raw(message), 3)
        resource.write_raw.assert_called_once_with(message)
        self.assertIs(resource.write_raw.call_args.args[0], message)
        resource.write.assert_not_called()
        resource.reset_mock()
        error = pyvisa.errors.VisaIOError(
            pyvisa.constants.StatusCode.error_io)
        resource.write_raw.side_effect = error
        with self.assertRaises(pyvisa.errors.VisaIOError) as raised:
            handle.write_raw(b"FAIL")
        self.assertIs(raised.exception, error)
        resource.write.assert_not_called()

    def test_sim_end_to_end_resource_outcomes(self):
        # Borrowed resource: adopted as-is, never closed by the non-owner.
        manager = pyvisa.ResourceManager(SIMULATOR)
        resource = manager.open_resource("GPIB::1::INSTR")
        resource.read_termination = "\r"
        resource.write_termination = "\r"
        try:
            handle = VisaHandle.borrow(resource)
            self.assertTrue(handle.initialized)
            self.assertIn("Simulated", handle.query("*IDN?"))
            self.assertFalse(handle.stopped)  # no stop claimed so far
            self.assertTrue(handle.confirm_stop())  # sim answers *OPC? -> 1
            self.assertTrue(handle.stopped)
            handle.cleanup()
            handle.cleanup()
            self.assertIsNotNone(resource.session)  # owner resource intact
            self.assertIn("Simulated", resource.query("*IDN?"))
        finally:
            resource.close()
        # Owned handle against the simulator: defaults, errors, recovery.
        owned = VisaHandle("GPIB0::26::INSTR", visalib=SIMULATOR,
                           read_termination="\r", write_termination="\r",
                           device_clear=False)
        self.assertTrue(owned.supports("connection"))
        # Default construction applies the seconds default as real 5000 ms
        # on the resource (review issue 1: timeout=None sentinel path).
        self.assertEqual(owned.timeout, 5000)
        self.assertEqual(owned.timeout_seconds, 5.0)  # 5000 ms on resource
        self.assertIn("Keithley", owned.query("*IDN?"))
        owned.cleanup()
        with self.assertRaisesRegex(RuntimeError, "Invalid visa resource"):
            owned.query("*IDN?")
        owned.prepare()
        self.assertIn("Keithley", owned.query("*IDN?"))
        owned.cleanup()


if __name__ == "__main__":
    unittest.main()
