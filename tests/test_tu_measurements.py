"""Acceptance tests for opt-in measurement semantics (TU-005).

Proposed contract under test (test surface for developer/designer review;
no production implementation is asserted to exist):

- ``Parameter.read() -> Reading`` --- opt-in richer read. Performs the same
  acquisition path as ``get()`` (permission check, ``before_get`` hook,
  encoder) and returns a ``Reading`` result exposing ``value`` (equal to
  what ``get()`` returns), ``acquired_at`` (epoch seconds of the
  acquisition) and ``quality`` (``'ok'`` on success). Legacy ``get()`` and
  ``__call__`` remain value-only and unchanged.
- Failed acquisition: when the acquisition step raises, ``read()`` returns
  a ``Reading`` with ``quality == 'failed'`` and the original exception
  preserved on ``reading.error`` instead of silently swallowing it.
  Legacy ``get()`` still propagates the identical exception object.
- ``Reading.uncertainty`` / ``Reading.calibration`` --- optional
  uncertainty and calibration reference, ``None`` unless the parameter
  declares them.
- ``Parameter.describe_reading() -> Dict[str, Any]`` --- opt-in versioned
  description in its own schema namespace (``schema_version`` 1), like
  TU-003 ``describe_operation()``; it never reinterprets the TU-002
  version-1 ``describe()`` schema. Optional fields ``unit``,
  ``value_type``, ``shape`` and ``channel`` are ``None`` when undeclared.
  It performs no device I/O and never reads the stored value.
- Serialization limits are explicit: arbitrary nonserializable values stay
  readable through both ``get()`` and ``read().value`` (identical object);
  dumping a ``Reading`` that holds such a value to JSON raises
  ``TypeError`` rather than coercing the value or restricting what may be
  stored.
"""

import json
import time
import unittest
from unittest.mock import Mock

import pyvisa

from softlab.tu.station import (
    Parameter,
    ProxyParameter,
    VisaHandle,
    VisaParameter,
)


class MeasurementTests(unittest.TestCase):
    def assert_json(self, value):
        self.assertEqual(
            json.loads(json.dumps(value, allow_nan=False)), value)

    def test_rich_read_exposes_acquisition_time_and_quality(self):
        backing = Parameter("backing", init_value=2.0)
        measured = Parameter(
            "reading", settable=False,
            before_get=lambda _: backing() * 3)
        read = measured.read  # resolve API first: missing API is the red cause
        before = time.time()
        result = read()
        after = time.time()
        self.assertEqual(result.value, 6.0)
        self.assertEqual(result.quality, "ok")
        self.assertIsInstance(result.acquired_at, (int, float))
        self.assertGreaterEqual(result.acquired_at, before)
        self.assertLessEqual(result.acquired_at, after)
        # Legacy value-only access is untouched by the opt-in read.
        self.assertEqual(measured.get(), 6.0)
        self.assertNotIsInstance(measured.get(), type(result))
        self.assertEqual(measured(), 6.0)

    def test_legacy_value_only_reads_unchanged(self):
        measured = Parameter("reading", init_value=1.5)
        self.assertEqual(measured.get(), 1.5)
        self.assertEqual(measured(), 1.5)
        measured(2.5)
        self.assertEqual(measured(), 2.5)
        self.assertEqual(measured.snapshot()["name"], "reading")

    def test_reading_description_optional_fields_and_versioning(self):
        plain = Parameter("voltage", settable=False)
        describe_reading = plain.describe_reading
        info = describe_reading()
        self.assertEqual(info["schema_version"], 1)
        self.assertEqual(info["name"], "voltage")
        for field in ("unit", "value_type", "shape", "channel"):
            self.assertIsNone(info[field])
        self.assert_json(info)
        described = Parameter(
            "current", settable=False,
            unit="A", value_type="float", shape=[3], channel="1")
        rich = described.describe_reading()
        self.assertEqual(rich["unit"], "A")
        self.assertEqual(rich["value_type"], "float")
        self.assertEqual(rich["shape"], [3])
        self.assertEqual(rich["channel"], "1")
        self.assert_json(rich)

    def test_describe_v1_schema_not_reinterpreted(self):
        measured = Parameter("voltage", settable=False)
        description = measured.describe()
        self.assertEqual(description["schema_version"], 1)
        self.assertEqual(
            set(description),
            {"schema_version", "name", "type", "metadata",
             "settable", "gettable"})
        self.assert_json(description)

    def test_optional_uncertainty_and_calibration_reference(self):
        plain = Parameter("reading", init_value=1.0)
        read_plain = plain.read
        self.assertIsNone(read_plain().uncertainty)
        self.assertIsNone(read_plain().calibration)
        calibrated = Parameter(
            "reading", init_value=1.0,
            uncertainty=0.02, calibration="cal-2026-09")
        result = calibrated.read()
        self.assertEqual(result.value, 1.0)
        self.assertEqual(result.quality, "ok")
        self.assertEqual(result.uncertainty, 0.02)
        self.assertEqual(result.calibration, "cal-2026-09")

    def test_arbitrary_nonserializable_value_remains_readable(self):
        class Opaque:
            def __repr__(self):
                raise AssertionError("Value must remain opaque")

            __str__ = __repr__

        value = Opaque()
        measured = Parameter("opaque", init_value=value)
        read = measured.read
        self.assertIs(measured.get(), value)
        result = read()
        self.assertIs(result.value, value)
        self.assertEqual(result.quality, "ok")
        self.assert_json(measured.describe_reading())
        # Explicit serialization limit: the reading is not coercible and
        # the stored value is not restricted.
        with self.assertRaises(TypeError):
            json.dumps({"reading": result})

    def test_failed_acquisition_reports_quality_and_preserves_error(self):
        handle = Mock(spec=VisaHandle)
        error = pyvisa.errors.VisaIOError(
            pyvisa.constants.StatusCode.error_timeout)
        handle.query.side_effect = error
        measured = VisaParameter(
            "signal", handle, get_cmd="MEAS?",
            encoder=float, settable=False)
        read = measured.read
        result = read()
        self.assertEqual(result.quality, "failed")
        self.assertIs(result.error, error)
        handle.query.assert_called_once_with("MEAS?", None)
        # Legacy path still propagates the identical exception object.
        with self.assertRaises(pyvisa.errors.VisaIOError) as raised:
            measured.get()
        self.assertIs(raised.exception, error)

    def test_rich_read_performs_single_acquisition_on_visa_parameter(self):
        handle = Mock(spec=VisaHandle)
        handle.query.return_value = "3.5"
        measured = VisaParameter(
            "signal", handle, get_cmd="MEAS?",
            encoder=float, settable=False)
        read = measured.read
        before = time.time()
        result = read()
        after = time.time()
        handle.query.assert_called_once_with("MEAS?", None)
        self.assertEqual(result.value, 3.5)
        self.assertEqual(result.quality, "ok")
        self.assertGreaterEqual(result.acquired_at, before)
        self.assertLessEqual(result.acquired_at, after)
        handle.query.reset_mock()
        self.assertEqual(measured.get(), 3.5)
        handle.query.assert_called_once_with("MEAS?", None)

    def test_proxy_parameter_forwards_rich_read(self):
        target = Parameter("inner", init_value=7)
        proxy = ProxyParameter("outer", target)
        read = proxy.read
        result = read()
        self.assertEqual(result.value, 7)
        self.assertEqual(result.quality, "ok")
        self.assertGreaterEqual(result.acquired_at, 0.0)
        self.assertEqual(proxy.get(), 7)

    def test_denied_read_matches_legacy_permission_error(self):
        write_only = Parameter("write_only", gettable=False)
        read = write_only.read
        with self.assertRaises(RuntimeError):
            write_only.get()
        with self.assertRaises(RuntimeError):
            read()


if __name__ == "__main__":
    unittest.main()
