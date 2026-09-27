"""Acceptance tests for opt-in portable setup descriptions (TU-002)."""

import json
import unittest
from unittest.mock import Mock, patch

from softlab.tu.station import (
    Device, Parameter, Station, VisaCommand, VisaHandle,
)


class DescriptionTests(unittest.TestCase):
    def assert_json(self, description):
        self.assertEqual(
            json.loads(json.dumps(description, allow_nan=False)), description
        )

    def test_parameter_schema_and_access_flags(self):
        p = Parameter("reading", settable=False)
        result = p.describe()
        self.assertEqual(result["schema_version"], 1)
        self.assertEqual(result["name"], "reading")
        self.assertEqual(result["type"], "softlab.tu.station.parameter.Parameter")
        self.assertFalse(result["settable"])
        self.assertTrue(result["gettable"])
        self.assertNotIn("value", result)
        self.assert_json(result)

    def test_description_avoids_hooks_and_legacy_snapshot_overrides(self):
        p = Parameter("reading", before_get=Mock(side_effect=AssertionError))
        with patch.object(p, "snapshot", side_effect=AssertionError("snapshot")):
            self.assert_json(p.describe())
        p._before_get.assert_not_called()

    def test_arbitrary_values_remain_opaque_and_snapshot_unchanged(self):
        class Opaque:
            def __repr__(self):
                raise AssertionError("Value must remain opaque")

            __str__ = __repr__

        value = Opaque()
        p = Parameter("opaque", init_value=value)
        before = p.snapshot()
        self.assert_json(p.describe())
        self.assertEqual(p.snapshot(), before)
        self.assertIs(p.snapshot()["type"], Parameter)
        self.assertIs(p(), value)

    def test_device_nesting_retains_lookup_keys_after_rename(self):
        device, child = Device("instrument"), Device("channel")
        p = Parameter("voltage")
        child.add_parameter(p)
        device.add_child(child)
        p.name = "renamed"
        result = device.describe()
        self.assertEqual(result["schema_version"], 1)
        leaf = result["children"]["channel"]["parameters"]["voltage"]
        self.assertEqual(leaf["name"], "renamed")
        self.assertIs(device.parameter("channel.voltage"), p)
        self.assert_json(result)

    def test_station_recursive_description_preserves_snapshot(self):
        station, device = Station("lab"), Device("instrument")
        device.add_parameter(Parameter("value", init_value=object()))
        station.add_device(device)
        before = station.snapshot()
        result = station.describe()
        self.assertEqual(result["schema_version"], 1)
        self.assertEqual(result["name"], "lab")
        self.assertEqual(result["devices"]["instrument"]["name"], "instrument")
        self.assert_json(result)
        self.assertEqual(station.snapshot(), before)

    def test_visa_command_description_performs_no_io(self):
        handle = Mock(spec=VisaHandle)
        command = VisaCommand("reset", handle, "*RST")
        self.assert_json(command.describe())
        self.assertEqual(handle.mock_calls, [])
        self.assertIs(command(), True)
        handle.write.assert_called_once_with("*RST")

    def test_json_metadata_is_copied_and_unknown_keys_preserved(self):
        p = Parameter("reading")
        metadata = {"lab_custom": {"tags": ["a", None, True, 1, 2.5]}}
        result = p.describe(metadata=metadata)
        self.assertEqual(result["metadata"], metadata)
        self.assert_json(result)
        result["metadata"]["lab_custom"]["tags"].append("changed")
        self.assertEqual(metadata["lab_custom"]["tags"], ["a", None, True, 1, 2.5])

    def test_indirect_device_cycle_is_rejected(self):
        root, child = Device("root"), Device("child")
        root.add_child(child)
        child.add_child(root)
        describe = root.describe
        with self.assertRaises(ValueError):
            describe()

    def test_shared_device_is_not_a_cycle_and_results_are_independent(self):
        root, left, right = Device("root"), Device("left"), Device("right")
        shared = Device("shared")
        shared.add_parameter(Parameter("reading"))
        root.add_child(left)
        root.add_child(right)
        left.add_child(shared)
        right.add_child(shared)
        result = root.describe()
        a = result["children"]["left"]["children"]["shared"]
        b = result["children"]["right"]["children"]["shared"]
        self.assertEqual(a, b)
        self.assertIsNot(a, b)
        a["parameters"]["reading"]["metadata"]["local"] = True
        self.assertEqual(b["parameters"]["reading"]["metadata"], {})
        self.assert_json(result)

    def test_metadata_cycle_is_rejected(self):
        p = Parameter("reading")
        describe = p.describe
        metadata = {"items": []}
        metadata["items"].append(metadata)
        with self.assertRaises(ValueError):
            describe(metadata=metadata)

    def test_unsupported_metadata_is_rejected_without_coercion(self):
        p = Parameter("reading")
        # Resolve API before error assertions, so missing API is the red cause.
        describe = p.describe
        for metadata in ({"bad": object()}, {1: "key"}, {"bad": float("nan")}, {"bad": float("inf")}):
            with self.subTest(metadata_type=type(metadata)):
                with self.assertRaises((TypeError, ValueError)):
                    describe(metadata=metadata)


if __name__ == "__main__":
    unittest.main()
