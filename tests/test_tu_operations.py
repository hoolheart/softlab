"""Acceptance tests for explicit command operations (TU-003)."""

import json
import unittest
from unittest.mock import Mock, patch

import pyvisa

from softlab.tu.station import VisaCommand, VisaHandle


class OperationTests(unittest.TestCase):
    def setUp(self):
        self.resource = Mock(spec=pyvisa.resources.MessageBasedResource)
        self.manager = Mock()
        self.manager.open_resource.return_value = self.resource
        patcher = patch(
            "softlab.tu.station.visa.visa.ResourceManager",
            return_value=self.manager,
        )
        self.factory = patcher.start()
        self.addCleanup(patcher.stop)
        self.handle = VisaHandle("TEST@sim")
        self.addCleanup(self.handle.close)

    def assert_json(self, value):
        self.assertEqual(
            json.loads(json.dumps(value, allow_nan=False)), value)

    def test_explicit_operation_path_available_and_callable(self):
        command = VisaCommand("reset", self.handle, "*RST")
        # Resolve API before error assertions, so missing API is the red cause.
        execute = command.execute
        self.resource.reset_mock()
        self.assertTrue(callable(execute))
        self.assertIs(execute(), True)
        self.resource.write.assert_called_once_with(
            message="*RST", encoding=None)
        execute()
        self.assertEqual(self.resource.write.call_count, 2)

    def test_side_effect_semantics_discoverable_without_execution(self):
        command = VisaCommand("reset", self.handle, "*RST")
        describe_operation = command.describe_operation
        self.resource.reset_mock()
        info = describe_operation()
        self.assert_json(info)
        self.assertEqual(info["schema_version"], 1)
        self.assertEqual(info["name"], "reset")
        self.assertEqual(info["effect"], "write")
        self.assertEqual(info["executions_per_call"], 1)
        self.assertNotIn("*RST", json.dumps(info))
        self.assertEqual(self.resource.mock_calls, [])

    def test_legacy_invocation_permissions_and_execution_counts_unchanged(self):
        command = VisaCommand("reset", self.handle, "*RST")
        self.resource.reset_mock()
        command.snapshot()
        self.resource.write.assert_not_called()
        self.assertIs(command(), True)
        self.assertIs(command.get(), True)
        self.assertEqual(self.resource.write.call_count, 2)
        with self.assertRaises(RuntimeError):
            command(False)
        self.assertEqual(self.resource.write.call_count, 2)

    def test_description_lookup_executes_nothing_and_hides_command(self):
        command = VisaCommand("reset", self.handle, "*RST")
        self.resource.reset_mock()
        description = command.describe()
        self.assert_json(description)
        self.assertNotIn("*RST", json.dumps(description))
        self.assertEqual(self.resource.mock_calls, [])
        # Legacy invocation still performs exactly one write afterwards.
        self.assertIs(command(), True)
        self.resource.write.assert_called_once_with(
            message="*RST", encoding=None)

    def test_denied_set_and_failed_operation_propagate_errors(self):
        command = VisaCommand("reset", self.handle, "*RST")
        execute = command.execute
        with self.assertRaises(RuntimeError):
            command(False)  # denied write performs no execution
        self.resource.write.assert_not_called()
        error = pyvisa.errors.VisaIOError(
            pyvisa.constants.StatusCode.error_timeout)
        self.resource.write.side_effect = error
        with self.assertRaises(pyvisa.errors.VisaIOError) as raised:
            execute()
        self.assertIs(raised.exception, error)  # original cause preserved

    def test_execute_on_unavailable_resource_raises(self):
        handle = Mock(spec=VisaHandle)
        handle.write.side_effect = RuntimeError("Invalid visa resource")
        command = VisaCommand("reset", handle, "*RST")
        execute = command.execute
        with self.assertRaises(RuntimeError):
            execute()
        handle.write.assert_called_once_with("*RST")


if __name__ == "__main__":
    unittest.main()
