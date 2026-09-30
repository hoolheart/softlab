"""Compatibility contracts for tu; all VISA resources are mocks."""

import importlib
import unittest
from unittest.mock import Mock, call, patch

import numpy as np
from numpy.testing import assert_array_equal
import pyvisa

from softlab.jin.validator import Validator, ValInt
from softlab.tu.station import (
    Device, DeviceBuilder, Parameter, ProxyParameter, QuantizedParameter,
    Station, VisaCommand, VisaHandle, VisaIDN, VisaParameter,
    default_station, get_device_builder, register_device_builder,
    set_default_station,
)
from softlab.tu.theory import Mapping, TheoryModel, batch_mapping


class ParameterContracts(unittest.TestCase):
    def test_validation_codecs_and_hooks_have_ordered_values(self):
        events = []

        class Guard(Validator):
            def validate(self, value, context=""):
                events.append(("validate", value))
                if value < 0:
                    raise ValueError("negative")

        def decode(value):
            events.append(("decode", value))
            return value * 10

        def refresh(value):
            events.append(("get", value))
            return value + 1

        def encode(value):
            events.append(("encode", value))
            return str(value)

        p = Parameter(
            "p", Guard(), decoder=decode, encoder=encode,
            before_set=lambda old, new: events.append(("before", old, new)),
            after_set=lambda value: events.append(("after", value)),
            before_get=refresh,
        )
        self.assertIsNone(p(2, "ignored"))
        self.assertEqual(p(), "21")
        self.assertEqual(events, [
            ("validate", 2), ("decode", 2), ("before", None, 20),
            ("after", 20), ("get", 20), ("encode", 21),
        ])
        events.clear()
        with self.assertRaises(ValueError):
            p(-1)
        self.assertEqual(events, [("validate", -1)])
        self.assertEqual(p(), "22")

    def test_initialization_and_permissions(self):
        hook = Mock()
        p = Parameter("p", ValInt(), init_value=3, after_set=hook)
        hook.assert_called_once_with(3)
        decoder = Mock(return_value=100)
        readonly = Parameter(
            "ro", ValInt(), settable=False, init_value=4,
            decoder=decoder, after_set=hook,
        )
        self.assertEqual(readonly(), 4)
        decoder.assert_not_called()
        with self.assertRaises(RuntimeError):
            readonly(5)
        with self.assertRaises(TypeError):
            Parameter("bad", ValInt(), settable=False, init_value="bad")
        getter = Mock()
        writeonly = Parameter("wo", gettable=False, before_get=getter)
        writeonly(6)
        with self.assertRaises(RuntimeError):
            writeonly()
        getter.assert_not_called()
        self.assertEqual(p(), 3)

    def test_failed_decoder_or_before_hook_preserves_value(self):
        error = ValueError("failed")
        for field in ("decoder", "before_set"):
            with self.subTest(field=field):
                hook = Mock(side_effect=[5, error])
                p = Parameter("p", **{field: hook})
                p(5)
                with self.assertRaises(ValueError) as raised:
                    p(6)
                self.assertIs(raised.exception, error)
                self.assertEqual(p(), 5)

    def test_arbitrary_value_and_snapshot_do_not_read(self):
        value = object()
        getter = Mock(return_value=value)
        p = Parameter("p", init_value=value, before_get=getter)
        snapshot = p.snapshot()
        getter.assert_not_called()
        self.assertIs(snapshot["type"], Parameter)
        self.assertNotIn("value", snapshot)
        self.assertEqual(snapshot["name"], "p")
        self.assertIs(p(), value)

    def test_proxy_forwards_validation_permissions_and_hooks(self):
        hook = Mock()
        target = Parameter("original", ValInt(0, 10), after_set=hook)
        proxy = ProxyParameter("alias", target)
        proxy(4)
        self.assertEqual(target(), 4)
        self.assertEqual(proxy(), 4)
        hook.assert_called_once_with(4)
        with self.assertRaises(ValueError):
            proxy(11)
        ro = ProxyParameter("ro", Parameter("source", settable=False))
        with self.assertRaises(RuntimeError):
            ro(1)

    def test_quantization_modes_and_bounds(self):
        for mode, expected in (("round", 1.5), ("floor", 1.0), ("ceil", 1.5)):
            with self.subTest(mode=mode):
                p = QuantizedParameter("q", lsb=0.5, mode=mode)
                p(1.3)
                self.assertEqual(p(), expected)
                self.assertEqual(p.snapshot()["mode"], mode)
                with self.assertRaises(ValueError):
                    p(-1.0)


class DeviceContracts(unittest.TestCase):
    def test_nested_paths_delegation_ownership_and_removal(self):
        root, child, leaf = Device("root"), Device("child"), Device("leaf")
        p = Parameter("value")
        root.add_child(child)
        child.add_child(leaf)
        leaf.add_parameter(p)
        self.assertIs(root.child("child").leaf.value, p)
        self.assertIs(root.parameter("child.leaf.value"), p)
        self.assertIs(p.owner, leaf)
        self.assertIs(leaf.parent, child)
        self.assertIsNone(root.parameter("missing.value"))
        root.set_parameters([("child.leaf.value", 2)])
        self.assertEqual(p(), 2)
        with self.assertRaises(KeyError):
            root.set_parameters({"missing": 3})
        with self.assertRaises(ValueError):
            leaf.add_parameter(Parameter("value"))
        self.assertIs(leaf.rm_parameter("value"), p)
        self.assertIsNone(p.owner)
        self.assertIs(child.rm_child("leaf"), leaf)
        self.assertIsNone(leaf.parent)

    def test_batch_settings_preserve_sequence_order(self):
        events = []
        device = Device("device")
        device.add_parameter(Parameter(
            "first", after_set=lambda value: events.append(("first", value)),
        ))
        device.add_parameter(Parameter(
            "second", after_set=lambda value: events.append(("second", value)),
        ))
        device.set_parameters([("second", 2), ("first", 1)])
        self.assertEqual(events, [("second", 2), ("first", 1)])
        self.assertEqual(device.first(), 1)
        self.assertEqual(device.second(), 2)

    def test_station_builder_registry_and_default_are_restored(self):
        module = importlib.import_module("softlab.tu.station.device")

        class Builder(DeviceBuilder):
            def build(self, name, **kwargs):
                result = Device(name)
                result.add_parameter(Parameter("value", init_value=kwargs["value"]))
                return result

        original = default_station()
        self.addCleanup(set_default_station, original)
        with patch.dict(module._device_builders, {}, clear=True):
            builder = Builder("test")
            register_device_builder(builder)
            self.assertIs(get_device_builder("test"), builder)
            with self.assertRaises(ValueError):
                register_device_builder(builder)
            station = Station("test")
            set_default_station(station)
            self.assertIs(default_station(), station)
            station.build_device("test", "device", value=9)
            self.assertEqual(station.device("device").value(), 9)
            self.assertEqual(station.snapshot()["devices"]["device"]["name"], "device")
            self.assertIsNotNone(station.rm_device("device"))
            with self.assertRaises(RuntimeError):
                station.build_device("missing", "none")


class VisaContracts(unittest.TestCase):
    def setUp(self):
        self.resource = Mock(spec=pyvisa.resources.MessageBasedResource)
        self.manager = Mock()
        self.manager.open_resource.return_value = self.resource
        patcher = patch("softlab.tu.station.visa.visa.ResourceManager", return_value=self.manager)
        self.factory = patcher.start()
        self.addCleanup(patcher.stop)
        self.handle = VisaHandle("TEST@sim")
        self.addCleanup(self.handle.close)

    def test_eager_open_clear_and_transport_errors(self):
        self.factory.assert_called_once_with("@sim")
        self.manager.open_resource.assert_called_once_with("TEST")
        self.resource.clear.assert_called_once_with()
        self.assertEqual(self.handle.address, "TEST")
        error = pyvisa.errors.VisaIOError(pyvisa.constants.StatusCode.error_timeout)
        self.resource.query.side_effect = error
        with self.assertRaises(pyvisa.errors.VisaIOError) as raised:
            self.handle.query("READ?", 0.1)
        self.assertIs(raised.exception, error)
        self.resource.query.assert_called_once_with("READ?", 0.1)
        self.handle.close()
        self.resource.close.assert_called_once_with()

    def test_timeout_values_are_forwarded_without_conversion(self):
        # TU-006 sentinel supersession (design handoff 2, step-B
        # co-requisite): default construction (``timeout=None`` sentinel)
        # now applies the ``timeout_seconds`` default — 5.0 s, forwarded
        # to the resource as 5000 ms. This replaces the TU-001-era pin of
        # the implicit legacy raw 5.0. The assertions below pin explicit
        # raw set/get forwarding, which OBS-001 still tracks.
        self.assertEqual(self.resource.timeout, 5000)
        handle = VisaHandle("CUSTOM@sim", timeout=12.5)
        self.addCleanup(handle.close)
        self.assertEqual(self.resource.timeout, 12.5)
        handle.timeout = 25.0
        self.assertEqual(self.resource.timeout, 25.0)
        self.resource.timeout = 37.5
        self.assertEqual(handle.timeout, 37.5)
        handle.timeout = None
        self.assertIsNone(self.resource.timeout)

    def test_parameter_commands_codecs_and_snapshot(self):
        p = VisaParameter("voltage", self.handle, get_cmd="V?", set_cmd="V {}", decoder=str, encoder=float)
        self.resource.reset_mock()
        p.snapshot()
        self.assertEqual(self.resource.mock_calls, [])
        p(2.5)
        self.resource.write.assert_called_once_with(message="V 2.5", encoding=None)
        self.resource.query.return_value = "3.5"
        self.assertEqual(p(), 3.5)
        self.resource.query.assert_called_once_with("V?", None)

    def test_legacy_command_reads_execute_once_and_writes_denied(self):
        command = VisaCommand("reset", self.handle, "*RST")
        self.resource.reset_mock()
        command.snapshot()
        self.resource.write.assert_not_called()
        self.assertIs(command(), True)
        self.assertIs(command.get(), True)
        self.assertEqual(self.resource.write.call_args_list, [
            call(message="*RST", encoding=None), call(message="*RST", encoding=None),
        ])
        with self.assertRaises(RuntimeError):
            command(False)
        self.assertEqual(self.resource.write.call_count, 2)

    def test_non_message_resource_is_closed_on_rejection(self):
        invalid = Mock()
        self.manager.open_resource.return_value = invalid
        with self.assertRaises(TypeError):
            VisaHandle("INVALID", visalib="@sim")
        invalid.close.assert_called_once_with()


class TheoryContracts(unittest.TestCase):
    def test_model_attributes_features_and_legacy_failure(self):
        class Model(TheoryModel):
            def calculate_features(self):
                return {"double": self.value() * 2}

        m = Model("model")
        m.add_attribute("value", ValInt(0, 10), 2)
        self.assertEqual(m.features, {"double": 4})
        m.value(3)
        self.assertEqual(m.features, {"double": 6})
        with self.assertRaises(ValueError):
            m.value(11)
        with self.assertRaises(ValueError):
            m.add_attribute("value", ValInt(), 1)
        with patch.object(m, "calculate_features", side_effect=ValueError("bad")):
            self.assertEqual(m.features, {})
        with self.assertRaises(NotImplementedError):
            m.get_mapping("missing")

    def test_mapping_shapes_types_and_batch(self):
        m = Mapping((2, 1), (2, 1), lambda x: x * 2, {"unit": "V"})
        data = np.arange(8).reshape(4, 2)
        assert_array_equal(batch_mapping(m, data), data * 2)
        self.assertEqual(m.metadata, {"unit": "V"})
        with self.assertRaises(TypeError):
            m([[1], [2]])
        with self.assertRaises(ValueError):
            m(np.zeros((1, 2)))
        with self.assertRaises(RuntimeError):
            m()
        with self.assertRaises(RuntimeError):
            Mapping((1,), (1,), lambda x: "bad")(np.ones(1))
        with self.assertRaises(ValueError):
            batch_mapping(m, np.ones(4))

    def test_public_exports_retain_class_identity(self):
        import softlab.tu as tu
        self.assertIs(tu.station.Parameter, Parameter)
        self.assertIs(tu.station.VisaIDN, VisaIDN)
        self.assertIs(tu.theory.Mapping, Mapping)
        self.assertIs(tu.theory.TheoryModel, TheoryModel)


if __name__ == "__main__":
    unittest.main()
