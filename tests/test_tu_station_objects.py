"""R3-002 acceptance cases: station object membership and object builders.

Implements R3-TC-04a--04k and 04m--04r of
``log/release_3/test/r3-002-test-plan.md`` (Revision 2) against
``softlab.tu.simulation`` (``ObjectBuilder`` and its model-keyed registry)
and ``softlab.tu.station.Station`` object membership. All fixtures are
synthetic; stdlib + NumPy only; no scheduler, no hardware, no connected
simulation.
"""

import unittest
from unittest.mock import patch

from softlab.tu.simulation import (
    ObjectBuilder,
    SimulatedObject,
    get_object_builder,
    register_object_builder,
)
from softlab.tu.simulation import builder as builder_module
import softlab.tu.station.device as device_module
from softlab.tu.station import (
    Device,
    DeviceBuilder,
    Station,
    get_device_builder,
    register_device_builder,
)


def make_accumulator_object(name='acc'):
    """Scalar accumulator ``x' = x + u`` with observation ``y = 2x``."""
    return SimulatedObject(
        name,
        {'u': 0.0}, {'x': 0.0}, ['y'],
        evolve=lambda u, x: {'x': x['x'] + u['u']},
        observe=lambda x: {'y': 2.0 * x['x']},
    )


class AccumulatorBuilder(ObjectBuilder):
    """Object builder constructing fresh accumulator objects per call."""

    def __init__(self, model='accumulator') -> None:
        super().__init__(model)
        self.build_count = 0

    def build(self, name: str, **kwargs) -> SimulatedObject:
        self.build_count += 1
        return make_accumulator_object(name)


class SameInstanceBuilder(ObjectBuilder):
    """Misbehaving builder returning one pre-owned instance every call."""

    def __init__(self, model='same-instance') -> None:
        super().__init__(model)
        self.instance = make_accumulator_object('alias')

    def build(self, name: str, **kwargs) -> SimulatedObject:
        return self.instance


def membership_snapshot(station):
    """Map of member names to member identities for both namespaces."""
    return {
        'devices': {name: station.device(name) for name in station.devices},
        'objects': {name: station.object(name) for name in station.objects},
    }


class StationObjectMembership(unittest.TestCase):
    """R3-TC-04a--04g, 04i: membership, namespaces, naming."""

    def test_04a_add_and_look_up_object(self):
        station = Station('station')
        obj = make_accumulator_object('acc')
        station.add_object(obj)
        self.assertIs(station.object('acc'), obj)
        self.assertIs(station.acc, obj)  # attribute delegation
        self.assertEqual(list(station.objects), ['acc'])
        self.assertEqual(list(station.devices), [])

    def test_04b_membership_namespaces_are_independent(self):
        station = Station('station')
        device = Device('d')
        obj = make_accumulator_object('o')
        station.add_device(device)
        station.add_object(obj)
        self.assertIs(station.device('d'), device)
        self.assertIsNone(station.object('d'))
        self.assertIs(station.object('o'), obj)
        self.assertIsNone(station.device('o'))
        # companion ruling to R3-TC-04b: the device-side insertion guard
        # rejects a device name already held by an object member
        with self.assertRaises(ValueError):
            station.add_device(Device('o'))
        self.assertIsNone(station.device('o'))
        self.assertIs(station.object('o'), obj)

    def test_04c_remove_object_is_lenient(self):
        station = Station('station')
        obj = make_accumulator_object('acc')
        station.add_object(obj)
        self.assertIs(station.rm_object('acc'), obj)
        self.assertIsNone(station.object('acc'))
        self.assertIsNone(station.rm_object('acc'))
        self.assertIsNone(station.rm_object('missing'))

    def test_04d_explicit_lookup_survives_method_name_collision(self):
        for name in ('snapshot', 'add_device', 'devices'):
            station = Station('station')
            obj = make_accumulator_object(name)
            station.add_object(obj)
            # real members win over delegated names, same as devices
            self.assertTrue(callable(station.snapshot))
            self.assertTrue(callable(station.add_device))
            self.assertIsInstance(station.devices, type(station.objects))
            # explicit lookup is the OBS-006-style escape hatch
            self.assertIs(station.object(name), obj)

    def test_04e_rejects_non_object(self):
        station = Station('station')
        for invalid in (Device('dev'), 'plain-string', None):
            with self.assertRaises(TypeError):
                station.add_object(invalid)
        self.assertEqual(list(station.objects), [])
        self.assertEqual(list(station.devices), [])

    def test_04f_rejects_object_shadowing_device(self):
        station = Station('station')
        device = Device('acc')
        station.add_device(device)
        with self.assertRaises(ValueError):
            station.add_object(make_accumulator_object('acc'))
        self.assertIs(station.device('acc'), device)
        self.assertIsNone(station.object('acc'))
        self.assertEqual(list(station.objects), [])

    def test_04g_rejects_duplicate_object_name(self):
        station = Station('station')
        first = make_accumulator_object('acc')
        station.add_object(first)
        with self.assertRaises(ValueError):
            station.add_object(make_accumulator_object('acc'))
        self.assertIs(station.object('acc'), first)
        self.assertEqual(list(station.objects), ['acc'])

    def test_04i_object_names_are_immutable_for_members(self):
        station = Station('station')
        obj = make_accumulator_object('n')
        station.add_object(obj)
        # no rename path exists: name is a read-only property
        with self.assertRaises(AttributeError):
            obj.name = 'other'
        self.assertIs(station.object('n'), obj)
        self.assertIsNone(station.object('other'))


class ObjectBuilderRegistry(unittest.TestCase):
    """R3-TC-04j, 04k: builder convention and registry semantics."""

    def test_04j_builder_convention_mirrors_device_builder(self):
        builder = AccumulatorBuilder('accumulator')
        self.assertEqual(builder.model, 'accumulator')
        self.assertEqual(repr(builder), '<ObjectBuilder>accumulator')
        self.assertEqual(
            type(builder).build.__qualname__,
            'AccumulatorBuilder.build')
        with self.assertRaises(NotImplementedError):
            ObjectBuilder('base').build('name')
        with patch.dict(builder_module._object_builders, {}, clear=True):
            register_object_builder(builder)
            self.assertIs(get_object_builder('accumulator'), builder)
            self.assertIsNone(get_object_builder('missing'))

    def test_04k_registry_semantics(self):
        with patch.dict(builder_module._object_builders, {}, clear=True):
            module = builder_module
            # (a) duplicate model registration is rejected
            builder = AccumulatorBuilder('shared')
            register_object_builder(builder)
            with self.assertRaises(ValueError):
                register_object_builder(AccumulatorBuilder('shared'))
            # (b) registries are distinct: same model string coexists
            device_builder = DeviceBuilder('shared')
            with patch.dict(
                    device_module._device_builders, {}, clear=True):
                register_device_builder(device_builder)
                self.assertIs(get_device_builder('shared'), device_builder)
                self.assertIs(get_object_builder('shared'), builder)
            # (c) registration constructs nothing
            counter = AccumulatorBuilder('counted')
            register_object_builder(counter)
            self.assertEqual(counter.build_count, 0)
            # non-builder registration is rejected
            with self.assertRaises(TypeError):
                module.register_object_builder(object())
            # empty model is rejected
            with self.assertRaises(ValueError):
                ObjectBuilder('')


class StationBuildObject(unittest.TestCase):
    """R3-TC-04h, 04m--04r: station construction entry semantics."""

    def setUp(self):
        self._patchers = []
        for target in (builder_module._object_builders,):
            patcher = patch.dict(target, {}, clear=True)
            patcher.start()
            self._patchers.append(patcher)
        self.addCleanup(self._stop_patchers)

    def _stop_patchers(self):
        for patcher in self._patchers:
            patcher.stop()

    def test_04h_invalid_name_rejected_before_mutation(self):
        station = Station('station')
        register_object_builder(AccumulatorBuilder())
        before = membership_snapshot(station)
        for invalid in ('', 123, None):
            with self.assertRaises(ValueError):
                station.build_object('accumulator', invalid)
        # invalid name wins over unknown model (gate order contract)
        with self.assertRaises(ValueError):
            station.build_object('missing', '')
        self.assertEqual(membership_snapshot(station), before)

    def test_04m_build_object_inserts_member(self):
        station = Station('station')
        register_object_builder(AccumulatorBuilder())
        station.build_object('accumulator', 'acc', ignored=True)
        obj = station.object('acc')
        self.assertIsInstance(obj, SimulatedObject)
        self.assertEqual(list(station.objects), ['acc'])
        self.assertEqual(list(station.devices), [])

    def test_04n_builder_exception_propagates_unchanged(self):
        station = Station('station')
        station.add_device(Device('dev'))
        station.add_object(make_accumulator_object('keep'))

        error = RuntimeError('construction failed')

        class FailingBuilder(ObjectBuilder):
            def build(self, name: str, **kwargs) -> SimulatedObject:
                raise error

        register_object_builder(FailingBuilder())
        before = membership_snapshot(station)
        with self.assertRaises(RuntimeError) as caught:
            station.build_object('failing', 'new')
        self.assertIs(caught.exception, error)
        after = membership_snapshot(station)
        self.assertEqual(after, before)
        for name, member in before['objects'].items():
            self.assertIs(after['objects'][name], member)
        for name, member in before['devices'].items():
            self.assertIs(after['devices'][name], member)

    def test_04o_unknown_model_is_documented_runtime_error(self):
        station = Station('station')
        before = membership_snapshot(station)
        with self.assertRaises(RuntimeError):
            station.build_object('missing', 'x')
        self.assertEqual(membership_snapshot(station), before)
        # CHK-04-1: the unknown-model error is documented in the docstring
        self.assertIn(
            'RuntimeError', Station.build_object.__doc__)

    def test_04p_membership_validation_failure_after_build(self):
        station = Station('station')
        device = Device('acc')
        station.add_device(device)
        register_object_builder(AccumulatorBuilder())
        before = membership_snapshot(station)
        with self.assertRaises(ValueError):
            station.build_object('accumulator', 'acc')
        self.assertEqual(membership_snapshot(station), before)
        self.assertIs(station.device('acc'), device)
        self.assertIsNone(station.object('acc'))

    def test_04q_built_instances_have_independent_state(self):
        station = Station('station')
        register_object_builder(AccumulatorBuilder())
        station.build_object('accumulator', 'a')
        station.build_object('accumulator', 'b')
        first = station.object('a')
        second = station.object('b')
        self.assertIsNot(first, second)
        first.set_input('u', 1.0)
        first.evolve_once()
        self.assertEqual(first.get_state('x'), 1.0)
        self.assertEqual(second.get_state('x'), 0.0)

    def test_04r_already_owned_instance_rejected(self):
        station = Station('station')
        register_object_builder(SameInstanceBuilder())
        station.build_object('same-instance', 'a')
        first = station.object('a')
        with self.assertRaises(ValueError):
            station.build_object('same-instance', 'b')
        self.assertIs(station.object('a'), first)
        self.assertIsNone(station.object('b'))
        self.assertEqual(list(station.objects), ['a'])


if __name__ == '__main__':
    unittest.main()
