"""Coordinated-simulation acceptance cases for R3-003.

Implements the automated cases R3-TC-01a--01e, 02a--02f, 03a--03f,
04a--04f, 05a--05g, 06a--06e and 08a--08f of
``log/release_3/test/r3-003-test-plan.md`` (Revision 2) against the
approved detailed design ``log/release_3/design/r3-003-detailed-design.md``
(modules ``softlab.tu.simulation.coordination`` /
``softlab.tu.simulation.mock``, the ``SimulatedObject`` candidate/commit
protocol, and the ``Station`` ``sim_dt``/``coordinator`` additions).

All fixtures are synthetic; stdlib + NumPy only; fresh stations per case;
no scheduler, no hardware, no wall-clock access.
"""

import compileall
import importlib
import io
import sys
import unittest
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

import numpy as np

from softlab.tu.simulation import (
    Coordinator,
    Endpoint,
    MockControl,
    MockMeasurement,
    SimulatedObject,
)
from softlab.tu.station import (
    Device,
    Parameter,
    Station,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
TESTS_DIR = Path(__file__).resolve().parent


def make_accumulator(name, x0, u0=0.0):
    """Scalar accumulator ``x' = x + u``, ``y = x``, with evolve/observe
    call counters and a log of the ``(u, x_previous)`` pairs received."""
    calls = {'evolve': 0, 'observe': 0}
    seen = []

    def evolve(u, x):
        calls['evolve'] += 1
        seen.append((u['u'], x['x']))
        return {'x': x['x'] + u['u']}

    def observe(x):
        calls['observe'] += 1
        return {'y': x['x']}

    obj = SimulatedObject(
        name, {'u': u0}, {'x': x0}, ['y'], evolve, observe)
    return obj, calls, seen


def build_chain(order=('A', 'B', 'C'), sim_dt=0.5):
    """Three-object accumulator chain A->B->C on a fresh station.

    Returns ``(station, coordinator, objects, counters, logs, initials)``.
    """
    station = Station('chain-station')
    station.sim_dt = sim_dt
    initials = {'A': 1.0, 'B': 10.0, 'C': 100.0}
    objs, counters, logs = {}, {}, {}
    for name in order:
        obj, calls, seen = make_accumulator(name, initials[name])
        objs[name] = obj
        counters[name] = calls
        logs[name] = seen
        station.add_object(obj)
    coord = Coordinator(station)
    for name in order:
        coord.enroll(name)
    coord.connect(Endpoint('A', 'y'), Endpoint('B', 'u'))
    coord.connect(Endpoint('B', 'y'), Endpoint('C', 'u'))
    return station, coord, objs, counters, logs, initials


class DelayedNetworkTests(unittest.TestCase):
    """R3-TC-01: every edge delivers the previous-boundary value."""

    def test_r3_tc_01a_chain_advances_at_most_one_edge(self):
        station, coord, objs, counters, logs, initials = build_chain()
        coord.prepare()
        self.assertEqual(coord.tick, 0)
        self.assertEqual(coord.sim_time, 0.0)
        coord.step()  # transition 0->1
        # B consumes A's tick-0 output; C consumes B's tick-0 output
        # (NOT B's newly computed 11.0)
        self.assertEqual(logs['B'][-1][0], 1.0)
        self.assertEqual(logs['C'][-1][0], 10.0)
        self.assertEqual(objs['A'].get_state('x'), 1.0)
        self.assertEqual(objs['B'].get_state('x'), 11.0)
        self.assertEqual(objs['C'].get_state('x'), 110.0)
        coord.step()  # transition 1->2
        # A's new output reaches C no earlier than this transition
        self.assertEqual(logs['C'][-1][0], 11.0)
        self.assertEqual(objs['C'].get_state('x'), 121.0)

    def test_r3_tc_01b_fan_out_delivers_identical_value(self):
        station = Station('fanout')
        station.sim_dt = 0.5
        src, _, _ = make_accumulator('S', 5.0)
        d1, _, log1 = make_accumulator('D1', 0.0)
        d2, _, log2 = make_accumulator('D2', 0.0)
        for obj in (src, d1, d2):
            station.add_object(obj)
        coord = Coordinator(station)
        for name in ('S', 'D1', 'D2'):
            coord.enroll(name)
        coord.connect(Endpoint('S', 'y'), Endpoint('D1', 'u'))
        coord.connect(Endpoint('S', 'y'), Endpoint('D2', 'u'))
        coord.prepare()
        coord.step()
        self.assertEqual(log1[-1][0], 5.0)
        self.assertEqual(log2[-1][0], 5.0)
        self.assertEqual(d1.get_input('u'), 5.0)
        self.assertEqual(d2.get_input('u'), 5.0)

    def test_r3_tc_01c_feedback_uses_initialized_previous_value(self):
        station, coord, objs, counters, logs, initials = build_chain()
        coord.connect(Endpoint('C', 'y'), Endpoint('A', 'u'))
        coord.prepare()
        coord.step()  # 0->1
        # A consumed C's committed tick-0 output (100.0), not a same-tick
        # value; cycles need no solver and no rejection
        self.assertEqual(logs['A'][-1][0], 100.0)
        self.assertEqual(objs['A'].get_state('x'), 101.0)

    def test_r3_tc_01d_determinism_independent_of_registration_order(self):
        def build(object_order, endpoint_order):
            station = Station('det')
            station.sim_dt = 0.25
            initials = {'A': 1.0, 'B': 10.0, 'C': 100.0}
            objs = {}
            for name in object_order:
                obj, _, _ = make_accumulator(name, initials[name])
                objs[name] = obj
                station.add_object(obj)
            list(station.objects)  # inspection interleaving
            coord = Coordinator(station)
            for name in object_order:
                coord.enroll(name)
            list(coord.participants)
            ctrl = MockControl('k', 2.0)
            meas = MockMeasurement('m', -1.0)
            for kind in endpoint_order:
                if kind == 'control':
                    coord.add_control(ctrl)
                else:
                    coord.add_measurement(meas)
            coord.connect(Endpoint('k'), Endpoint('A', 'u'))
            coord.connect(Endpoint('A', 'y'), Endpoint('B', 'u'))
            coord.connect(Endpoint('B', 'y'), Endpoint('C', 'u'))
            coord.connect(Endpoint('C', 'y'), Endpoint('m'))
            list(coord.connections)
            return coord, objs, meas

        def run(coord, objs, meas):
            trace = []
            coord.prepare()
            for _ in range(3):
                coord.step()
                trace.append((
                    coord.tick,
                    coord.sim_time,
                    tuple(objs[name].get_state('x')
                          for name in ('A', 'B', 'C')),
                    meas.read(),
                ))
            return trace

        coord1, objs1, meas1 = build(('A', 'B', 'C'), ('control',
                                                        'measurement'))
        coord2, objs2, meas2 = build(('C', 'A', 'B'), ('measurement',
                                                        'control'))
        trace1 = run(coord1, objs1, meas1)
        trace2 = run(coord2, objs2, meas2)
        self.assertEqual(trace1, trace2)

    def test_r3_tc_01e_no_edge_uses_same_tick_source_golden_trace(self):
        station, coord, objs, counters, logs, initials = build_chain()
        coord.prepare()
        delivered_b, delivered_c = [], []
        committed = {name: [initials[name]] for name in ('A', 'B', 'C')}
        for _ in range(3):
            coord.step()
            delivered_b.append(logs['B'][-1][0])
            delivered_c.append(logs['C'][-1][0])
            for name in ('A', 'B', 'C'):
                committed[name].append(objs[name].get_state('x'))
        # y == x, so committed outputs equal committed states
        for k in range(3):
            self.assertEqual(delivered_b[k], committed['A'][k])
            self.assertEqual(delivered_c[k], committed['B'][k])
        # explicit golden trace from the approved design derivation
        self.assertEqual(delivered_b, [1.0, 1.0, 1.0])
        self.assertEqual(delivered_c, [10.0, 11.0, 12.0])
        self.assertEqual(committed['C'][1:], [110.0, 121.0, 133.0])


class InitialStateTimingTests(unittest.TestCase):
    """R3-TC-02: tick-zero preparation, mock staging, no implicit tick."""

    def test_r3_tc_02a_inspection_never_prepares_then_explicit_prepare(self):
        station, coord, objs, counters, logs, initials = build_chain()
        # inspection before any preparation
        self.assertIsNotNone(station.object('A'))
        list(station.objects)
        station.snapshot()
        station.describe()
        for name in ('A', 'B', 'C'):
            self.assertEqual(counters[name]['evolve'], 0)
            self.assertEqual(counters[name]['observe'], 0)
        self.assertFalse(coord.prepared)
        # explicit preparation computes tick-zero outputs from initials
        coord.prepare()
        self.assertTrue(coord.prepared)
        self.assertEqual(coord.tick, 0)
        self.assertEqual(coord.sim_time, 0.0)
        for name in ('A', 'B', 'C'):
            self.assertEqual(counters[name]['evolve'], 0)
            self.assertEqual(counters[name]['observe'], 1)

    def test_r3_tc_02b_failed_tick_zero_leaves_unstarted(self):
        station = Station('failprep')
        station.sim_dt = 0.5
        sentinel = RuntimeError('tick-zero boom')
        armed = {'flag': True}

        def observe(x):
            if armed['flag']:
                raise sentinel
            return {'y': x['x']}

        obj = SimulatedObject(
            'A', {'u': 0.0}, {'x': 1.0}, ['y'],
            evolve=lambda u, x: {'x': x['x'] + u['u']}, observe=observe)
        station.add_object(obj)
        coord = Coordinator(station)
        coord.enroll('A')
        with self.assertRaises(RuntimeError) as caught:
            coord.prepare()
        self.assertIs(caught.exception, sentinel)
        self.assertFalse(coord.prepared)
        with self.assertRaises(RuntimeError):
            coord.tick
        with self.assertRaises(RuntimeError):
            coord.sim_time
        self.assertEqual(obj.get_state('x'), 1.0)  # nothing committed
        # the ownership claim pre-exists enrollment and is untouched by
        # the failed preparation (D4: claim spans the whole enrollment)
        self.assertIs(obj._active_coordinator, coord)
        # later healthy prepare works from untouched declarations
        armed['flag'] = False
        coord.prepare()
        self.assertEqual(coord.tick, 0)
        coord.step()
        self.assertEqual(coord.tick, 1)

    def test_r3_tc_02c_control_write_stages_and_readback_shows_pending(self):
        station = Station('stage')
        station.sim_dt = 0.5
        obj, calls, seen = make_accumulator('A', 1.0, u0=1.5)
        station.add_object(obj)
        coord = Coordinator(station)
        coord.enroll('A')
        ctrl = MockControl('k', 1.5)
        coord.add_control(ctrl)
        coord.connect(Endpoint('k'), Endpoint('A', 'u'))
        coord.prepare()
        ctrl.write(3.0)
        self.assertEqual(ctrl.read(), 3.0)  # readback shows pending
        self.assertEqual(obj.get_input('u'), 1.5)  # unchanged downstream
        self.assertEqual(coord.tick, 0)
        self.assertEqual(calls['evolve'], 0)
        coord.step()
        self.assertEqual(seen[-1][0], 3.0)  # delivered next boundary
        self.assertEqual(obj.get_input('u'), 3.0)
        self.assertEqual(coord.tick, 1)

    def test_r3_tc_02d_measurement_tick_zero_sample_is_declared_initial(self):
        station, coord, objs, counters, logs, initials = build_chain()
        meas = MockMeasurement('m', -1.0)  # distinct from C's tick-0 output
        coord.add_measurement(meas)
        coord.connect(Endpoint('C', 'y'), Endpoint('m'))
        coord.prepare()
        self.assertEqual(meas.read(), -1.0)  # tick-zero sample, not 100.0
        tick, sim_time = coord.tick, coord.sim_time
        self.assertEqual(meas.read(), -1.0)  # reads never fetch the source
        self.assertEqual(coord.tick, tick)  # reads never advance time
        coord.step()  # 0->1
        self.assertEqual(meas.read(), 100.0)  # C's committed tick-0 output
        coord.step()  # 1->2
        self.assertEqual(meas.read(), 110.0)

    def test_r3_tc_02e_last_write_wins_before_tick(self):
        station = Station('lww')
        station.sim_dt = 0.5
        obj, calls, seen = make_accumulator('A', 1.0)
        station.add_object(obj)
        coord = Coordinator(station)
        coord.enroll('A')
        ctrl = MockControl('k', 0.0)
        coord.add_control(ctrl)
        coord.connect(Endpoint('k'), Endpoint('A', 'u'))
        coord.prepare()
        ctrl.write(1.0)
        ctrl.write(2.0)
        coord.step()
        self.assertEqual(seen[-1][0], 2.0)
        self.assertTrue(all(entry[0] != 1.0 for entry in seen))

    def test_r3_tc_02f_reads_writes_inspection_never_tick(self):
        station, coord, objs, counters, logs, initials = build_chain()
        extra, _, _ = make_accumulator('D', 0.0)
        station.add_object(extra)
        coord.enroll('D')
        ctrl = MockControl('k', 2.0)
        meas = MockMeasurement('m', -1.0)
        coord.add_control(ctrl)
        coord.add_measurement(meas)
        coord.connect(Endpoint('k'), Endpoint('A', 'u'))
        coord.connect(Endpoint('C', 'y'), Endpoint('m'))
        coord.prepare()

        def snapshot_counters():
            return tuple(
                (counters[name]['evolve'], counters[name]['observe'])
                for name in ('A', 'B', 'C'))

        tick, sim_time = coord.tick, coord.sim_time
        ctrl.read()
        ctrl.write(3.0)
        meas.read()
        station.snapshot()
        station.describe()
        station.object('A')
        list(coord.participants)
        list(coord.connections)
        extra.set_input('u', 9.0)  # unconnected-input write stages
        self.assertEqual(coord.tick, tick)
        self.assertEqual(coord.sim_time, sim_time)
        self.assertEqual(snapshot_counters(), ((0, 1), (0, 1), (0, 1)))
        self.assertEqual(extra.get_input('u'), 9.0)


class SharedClockTests(unittest.TestCase):
    """R3-TC-03: station-owned shared interval, clock policy, binding."""

    def test_r3_tc_03a_valid_interval_shared_by_all_participants(self):
        station = Station('plain')  # no simulation argument
        device = Device('dev')
        station.add_device(device)
        self.assertIsNone(station.sim_dt)
        station.sim_dt = 0.5
        self.assertEqual(station.sim_dt, 0.5)
        a, _, _ = make_accumulator('A', 1.0)
        b, _, _ = make_accumulator('B', 10.0)
        station.add_object(a)
        station.add_object(b)
        coord = Coordinator(station)
        coord.enroll('A')
        coord.enroll('B')
        coord.connect(Endpoint('A', 'y'), Endpoint('B', 'u'))
        coord.prepare()
        for _ in range(3):
            coord.step()
        self.assertEqual(coord.tick, 3)
        self.assertEqual(coord.sim_time, 3 * 0.5)
        # B consumed A's committed output (1.0) at every transition
        self.assertEqual(b.get_state('x'), 13.0)

    def test_r3_tc_03b_invalid_interval_values_rejected(self):
        # first data block: invalid values are explicitly rejected
        cases = [
            (0, ValueError),
            (-1.0, ValueError),
            (float('nan'), ValueError),
            (float('inf'), ValueError),
            (float('-inf'), ValueError),
            (True, TypeError),
            (False, TypeError),
            ('0.5', TypeError),
            (None, TypeError),
            (0.5 + 0j, TypeError),
        ]
        for value, error_type in cases:
            with self.subTest(value=value):
                station = Station('reject')
                with self.assertRaises(error_type):
                    station.sim_dt = value
                self.assertIsNone(station.sim_dt)  # stored value unchanged
                station.sim_dt = 0.5  # station fully usable afterwards
                self.assertEqual(station.sim_dt, 0.5)
        # second data block (03b cont.): the documented coercion rule
        # for other real-valued scalars (Decimal, NumPy, Fraction)
        station = Station('coerce')
        with self.assertRaises(TypeError):
            station.sim_dt = Decimal('0.5')  # Decimal is not numbers.Real
        scalar = np.float64(0.5)  # NumPy real scalar is numbers.Real
        station.sim_dt = scalar
        self.assertEqual(station.sim_dt, 0.5)
        self.assertIs(type(station.sim_dt), float)
        station2 = Station('fraction')
        station2.sim_dt = Fraction(1, 2)  # Fraction is numbers.Real
        self.assertEqual(station2.sim_dt, 0.5)

    def test_r3_tc_03c_failed_tick_does_not_advance_clock(self):
        station = Station('clockfail')
        station.sim_dt = 0.5
        sentinel = RuntimeError('evolve boom')
        armed = {'flag': False}
        calls = {'evolve': 0}

        def evolve(u, x):
            calls['evolve'] += 1
            if armed['flag']:
                raise sentinel
            return {'x': x['x'] + u['u']}

        obj = SimulatedObject(
            'A', {'u': 0.0}, {'x': 1.0}, ['y'], evolve,
            observe=lambda x: {'y': x['x']})
        station.add_object(obj)
        coord = Coordinator(station)
        coord.enroll('A')
        coord.prepare()
        coord.step()
        self.assertEqual(coord.tick, 1)
        armed['flag'] = True
        with self.assertRaises(RuntimeError) as caught:
            coord.step()
        self.assertIs(caught.exception, sentinel)
        self.assertEqual(coord.tick, 1)  # clock identical before/after
        self.assertEqual(coord.sim_time, 0.5)
        self.assertEqual(obj.get_state('x'), 1.0)
        armed['flag'] = False
        coord.step()
        self.assertEqual(coord.tick, 2)

    def test_r3_tc_03d_mid_run_interval_change_rejected_until_reset(self):
        station, coord, objs, counters, logs, initials = build_chain()
        coord.prepare()
        coord.step()
        with self.assertRaises(RuntimeError):
            station.sim_dt = 0.25
        self.assertEqual(station.sim_dt, 0.5)  # unchanged
        self.assertEqual(coord.tick, 1)
        coord.step()  # still governed by the original interval
        self.assertEqual(coord.sim_time, 1.0)
        coord.reset()
        station.sim_dt = 0.25  # succeeds once stopped
        coord.prepare()
        coord.step()
        self.assertEqual(coord.sim_time, 0.25)  # new interval governs

    def test_r3_tc_03e_model_dt_binding_explicit_validated_single_driver(self):
        station = Station('bind')
        station.sim_dt = 0.5
        obj = SimulatedObject(
            'integrator',
            {'dt': 0.0, 'u': 0.0},
            {'t': 0.0},
            ['time'],
            evolve=lambda u, x: {'t': x['t'] + u['dt']},
            observe=lambda x: {'time': x['t']},
        )
        # a dt-named input WITHOUT binding receives no special treatment
        clockless = SimulatedObject(
            'clockless',
            {'dt': 0.0},
            {'t': 0.0},
            ['time'],
            evolve=lambda u, x: {'t': x['t'] + u['dt']},
            observe=lambda x: {'time': x['t']},
        )
        station.add_object(obj)
        station.add_object(clockless)
        coord = Coordinator(station)
        coord.enroll('integrator')
        coord.enroll('clockless')
        src, _, _ = make_accumulator('src', 1.0)
        station.add_object(src)
        coord.enroll('src')
        int_obj = SimulatedObject(
            'intmodel', {'n': 0}, {'x': 0.0}, ['y'],
            evolve=lambda u, x: {'x': x['x']},
            observe=lambda x: {'y': x['x']})
        station.add_object(int_obj)
        coord.enroll('intmodel')
        coord.bind_interval('integrator', 'dt')
        coord.prepare()
        for _ in range(3):
            coord.step()
        self.assertEqual(obj.get_state('t'), 1.5)  # k * sim_dt
        self.assertEqual(clockless.get_state('t'), 0.0)  # never inferred
        self.assertEqual(clockless.get_input('dt'), 0.0)
        # contradictory second drivers on the bound input are rejected
        # (graph edits require the stopped state: reset first)
        coord.reset()
        with self.assertRaises(ValueError):
            coord.bind_interval('integrator', 'dt')
        with self.assertRaises(ValueError):
            coord.connect(Endpoint('src', 'y'), Endpoint('integrator', 'dt'))
        with self.assertRaises(RuntimeError):
            obj.set_input('dt', 0.1)
        # an int-declared input cannot be interval-bound
        with self.assertRaises(TypeError):
            coord.bind_interval('intmodel', 'n')
        self.assertEqual(obj.get_input('dt'), 0.0)  # restored to initial
        coord.unbind_interval('integrator', 'dt')
        with self.assertRaises(KeyError):
            coord.unbind_interval('integrator', 'dt')

    def test_r3_tc_03f_numeric_clock_limits(self):
        import sys
        # (a) smallest subnormal and largest finite double are accepted
        station = Station('tiny')
        station.sim_dt = 5e-324
        self.assertEqual(station.sim_dt, 5e-324)
        obj, _, _ = make_accumulator('A', 1.0)
        station.add_object(obj)
        coord = Coordinator(station)
        coord.enroll('A')
        coord.prepare()
        coord.step()
        self.assertEqual(coord.sim_time, float(1) * 5e-324)
        # (c) the largest finite interval admits exactly one tick
        big = Station('big')
        big.sim_dt = sys.float_info.max
        b_obj, _, _ = make_accumulator('A', 1.0)
        big.add_object(b_obj)
        big_coord = Coordinator(big)
        big_coord.enroll('A')
        big_coord.prepare()
        big_coord.step()
        self.assertEqual(big_coord.tick, 1)
        self.assertEqual(big_coord.sim_time, float(1) * sys.float_info.max)
        with self.assertRaises(OverflowError):
            big_coord.step()  # float(2) * max overflows: failed tick
        self.assertEqual(big_coord.tick, 1)  # no silent inf clock
        self.assertEqual(
            big_coord.sim_time, float(1) * sys.float_info.max)
        self.assertEqual(b_obj.get_state('x'), 1.0)
        # (b) accumulated time matches the multiplication policy exactly
        many = Station('many')
        many.sim_dt = 1e-9
        m_obj, _, _ = make_accumulator('A', 1.0)
        many.add_object(m_obj)
        many_coord = Coordinator(many)
        many_coord.enroll('A')
        many_coord.prepare()
        ticks = 100_000
        for _ in range(ticks):
            many_coord.step()
        self.assertEqual(many_coord.tick, ticks)
        self.assertEqual(many_coord.sim_time, float(ticks) * 1e-9)


class OwnershipTests(unittest.TestCase):
    """R3-TC-04: ownership, standalone-operation guards, reentrancy."""

    def test_r3_tc_04a_connected_removal_rejected(self):
        station, coord, objs, counters, logs, initials = build_chain()
        coord.prepare()
        coord.step()
        committed = {name: objs[name].get_state('x')
                     for name in ('A', 'B', 'C')}
        with self.assertRaises(RuntimeError):
            station.rm_object('B')
        self.assertIsNotNone(station.object('B'))  # still a member
        for name in ('A', 'B', 'C'):
            self.assertEqual(objs[name].get_state('x'), committed[name])

    def test_r3_tc_04b_removal_succeeds_after_safe_detach(self):
        station, coord, objs, counters, logs, initials = build_chain()
        coord.prepare()
        coord.step()
        coord.reset()  # stopped/reset boundary: graph editable
        coord.disconnect(Endpoint('B', 'u'))
        coord.disconnect(Endpoint('C', 'u'))
        coord.withdraw('B')
        removed = station.rm_object('B')
        self.assertIs(removed, objs['B'])
        self.assertIsNone(station.object('B'))
        # standalone operations are available again
        objs['B'].set_input('u', 1.0)
        objs['B'].evolve_once()
        self.assertEqual(objs['B'].get_state('x'), 11.0)
        # with no edges remaining, withdrawing A succeeds as well
        coord.withdraw('A')
        self.assertIsNone(objs['A']._active_coordinator)

    def test_r3_tc_04c_shared_active_ownership_conflict_rejected(self):
        obj, _, _ = make_accumulator('shared', 1.0)
        station1 = Station('s1')
        station1.add_object(obj)
        coord1 = Coordinator(station1)
        coord1.enroll('shared')
        # one coordinator per station, permanent
        with self.assertRaises(RuntimeError):
            Coordinator(station1)
        station2 = Station('s2')
        station2.add_object(obj)
        coord2 = Coordinator(station2)
        with self.assertRaises(RuntimeError):
            coord2.enroll('shared')
        # duplicate enrollment and unknown names are rejected too
        with self.assertRaises(ValueError):
            coord1.enroll('shared')
        with self.assertRaises(KeyError):
            coord1.enroll('missing')
        # S1's simulation remains coherent and fully functional
        station1.sim_dt = 0.5
        coord1.prepare()
        coord1.step()
        self.assertEqual(coord1.tick, 1)

    def test_r3_tc_04d_standalone_operations_prevented_while_coordinated(self):
        station, coord, objs, counters, logs, initials = build_chain()
        d_obj, _, _ = make_accumulator('D', 0.0)
        station.add_object(d_obj)
        coord.enroll('D')
        coord.prepare()
        with self.assertRaises(RuntimeError):
            objs['A'].evolve_once()
        with self.assertRaises(RuntimeError):
            objs['A'].reset()
        with self.assertRaises(RuntimeError):
            objs['B'].set_input('u', 1.0)  # driven input
        # unconnected-input writes still stage through the boundary
        d_obj.set_input('u', 5.0)
        self.assertEqual(d_obj.get_input('u'), 5.0)
        # read access stays safe
        self.assertEqual(objs['A'].get_state('x'), 1.0)
        objs['A'].observe_outputs()
        # after detach, standalone operations work again
        coord.reset()
        coord.disconnect(Endpoint('B', 'u'))
        coord.disconnect(Endpoint('C', 'u'))
        coord.withdraw('A')
        self.assertIsNone(objs['A']._active_coordinator)
        objs['A'].evolve_once()
        expected = 1.0 + objs['A'].get_input('u')
        self.assertEqual(objs['A'].get_state('x'), expected)

    def test_r3_tc_04e_reentrancy_guard_mid_operation(self):
        station = Station('reent')
        station.sim_dt = 0.5
        ref = {}
        outcome = {}

        def evolve(u, x):
            try:
                ref['coord'].step()
                outcome['step'] = None
            except RuntimeError as exc:
                outcome['step'] = exc
            try:
                ref['ctrl'].write(9.0)
                outcome['write'] = None
            except RuntimeError as exc:
                outcome['write'] = exc
            return {'x': x['x'] + u['u']}

        obj = SimulatedObject(
            'A', {'u': 1.5}, {'x': 1.0}, ['y'], evolve,
            observe=lambda x: {'y': x['x']})
        station.add_object(obj)
        coord = Coordinator(station)
        ref['coord'] = coord
        coord.enroll('A')
        ctrl = MockControl('k', 1.5)
        ref['ctrl'] = ctrl
        coord.add_control(ctrl)
        coord.connect(Endpoint('k'), Endpoint('A', 'u'))
        coord.prepare()
        coord.step()  # nested step and nested write both rejected
        self.assertIsInstance(outcome['step'], RuntimeError)
        self.assertIsInstance(outcome['write'], RuntimeError)
        self.assertEqual(coord.tick, 1)  # exactly one transition committed
        self.assertEqual(obj.get_state('x'), 2.5)
        self.assertEqual(ctrl.read(), 1.5)  # rejected write left no trace

        # a callback that propagates the reentrancy failure fails the
        # tick atomically, original exception identity preserved
        # (fresh network; enrollment needs STOPPED)
        station2 = Station('reent2')
        station2.sim_dt = 0.5
        ref2 = {}

        def evolve_nested(u, x):
            try:
                ref2['coord'].step()  # rejected: mid-operation
            except RuntimeError as exc:
                ref2['nested'] = exc
                raise  # the callback propagates the reentrancy failure

        obj2 = SimulatedObject(
            'B', {'u': 0.0}, {'x': 7.0}, ['y'], evolve_nested,
            observe=lambda x: {'y': x['x']})
        station2.add_object(obj2)
        coord2 = Coordinator(station2)
        ref2['coord'] = coord2
        coord2.enroll('B')
        coord2.prepare()
        before = obj2.get_state('x')
        with self.assertRaises(RuntimeError) as caught:
            coord2.step()
        self.assertIs(caught.exception, ref2['nested'])
        self.assertEqual(coord2.tick, 0)  # no corruption of in-flight tick
        self.assertEqual(obj2.get_state('x'), before)

    def test_r3_tc_04f_two_mock_endpoints_on_one_participant_permitted(self):
        station = Station('multi')
        station.sim_dt = 0.5
        calls = {'evolve': 0, 'observe': 0}
        seen = []

        def evolve(u, x):
            calls['evolve'] += 1
            seen.append((u['u1'], u['u2']))
            return {'x': x['x'] + u['u1'] + u['u2']}

        obj = SimulatedObject(
            'P', {'u1': 0.0, 'u2': 0.0}, {'x': 0.0}, ['y'], evolve,
            observe=lambda x: {'y': x['x']})
        station.add_object(obj)
        coord = Coordinator(station)
        coord.enroll('P')
        k1 = MockControl('k1', 0.0)
        k2 = MockControl('k2', 0.0)
        m1 = MockMeasurement('m1', -1.0)
        coord.add_control(k1)
        coord.add_control(k2)
        coord.add_measurement(m1)
        coord.connect(Endpoint('k1'), Endpoint('P', 'u1'))
        coord.connect(Endpoint('k2'), Endpoint('P', 'u2'))
        coord.connect(Endpoint('P', 'y'), Endpoint('m1'))
        coord.prepare()
        k1.write(1.0)
        k2.write(2.0)
        coord.step()
        self.assertEqual(seen[-1], (1.0, 2.0))
        self.assertEqual(k1.read(), 1.0)
        self.assertEqual(k2.read(), 2.0)
        self.assertEqual(m1.read(), 0.0)  # P's committed tick-0 output
        self.assertEqual(obj.get_state('x'), 3.0)


class ConnectionSafetyTests(unittest.TestCase):
    """R3-TC-05: direction/single-driver/value rules, copies, atomicity."""

    def _station_with_endpoints(self):
        station = Station('conn')
        station.sim_dt = 0.5
        a, _, _ = make_accumulator('A', 1.0)
        b, _, _ = make_accumulator('B', 10.0)
        station.add_object(a)
        station.add_object(b)
        coord = Coordinator(station)
        coord.enroll('A')
        coord.enroll('B')
        ctrl = MockControl('k', 0.0)
        meas = MockMeasurement('m', -1.0)
        coord.add_control(ctrl)
        coord.add_measurement(meas)
        return station, coord

    def test_r3_tc_05a_endpoint_direction_validation_atomic(self):
        station, coord = self._station_with_endpoints()
        invalid = [
            # unknown source/destination names
            (Endpoint('nope', 'y'), Endpoint('B', 'u'), KeyError),
            (Endpoint('A', 'y'), Endpoint('nope', 'u'), KeyError),
            (Endpoint('A', 'nope'), Endpoint('B', 'u'), KeyError),
            # input offered as source, output offered as destination
            (Endpoint('A', 'u'), Endpoint('B', 'u'), ValueError),
            (Endpoint('A', 'y'), Endpoint('A', 'y'), ValueError),
            # wrong-kind mock endpoints
            (Endpoint('m'), Endpoint('B', 'u'), ValueError),
            (Endpoint('A', 'y'), Endpoint('k'), ValueError),
            # unknown mock endpoint
            (Endpoint('nope'), Endpoint('B', 'u'), KeyError),
        ]
        for source, dest, error_type in invalid:
            with self.subTest(source=source, dest=dest):
                with self.assertRaises(error_type):
                    coord.connect(source, dest)
                self.assertEqual(coord.connections, ())  # graph unchanged
        with self.assertRaises(TypeError):
            coord.connect('A', Endpoint('B', 'u'))
        with self.assertRaises(TypeError):
            coord.connect(Endpoint('A', 'y'), 'B')
        self.assertEqual(coord.connections, ())
        # valid directions still work afterwards
        coord.connect(Endpoint('A', 'y'), Endpoint('B', 'u'))
        coord.connect(Endpoint('k'), Endpoint('A', 'u'))
        self.assertEqual(len(coord.connections), 2)
        # endpoint validation happens at construction, not at connect
        with self.assertRaises(ValueError):
            Endpoint('', 'y')
        with self.assertRaises(ValueError):
            Endpoint('A', '')
        with self.assertRaises(TypeError):
            Endpoint(1, 'y')
        with self.assertRaises(TypeError):
            Endpoint('A', 1)

    def test_r3_tc_05b_single_driver_per_input(self):
        station, coord = self._station_with_endpoints()
        c, _, _ = make_accumulator('C', 100.0)
        station.add_object(c)
        coord.enroll('C')
        coord.connect(Endpoint('A', 'y'), Endpoint('B', 'u'))
        with self.assertRaises(ValueError):
            coord.connect(Endpoint('C', 'y'), Endpoint('B', 'u'))
        self.assertEqual(
            coord.connections,
            ((Endpoint('A', 'y'), Endpoint('B', 'u')),))
        coord.prepare()
        coord.step()
        # the original edge remains intact and functional
        self.assertEqual(station.object('B').get_input('u'), 1.0)

    def test_r3_tc_05c_value_compatibility_at_prepare_and_tick_time(self):
        # prepare-time: scalar output into ndarray input fails preparation
        station = Station('incompat')
        station.sim_dt = 0.5
        src, _, _ = make_accumulator('A', 1.0)
        dst = SimulatedObject(
            'B', {'u': np.zeros(2)}, {'x': 0.0}, ['y'],
            evolve=lambda u, x: {'x': x['x'] + float(np.sum(u['u']))},
            observe=lambda x: {'y': x['x']})
        station.add_object(src)
        station.add_object(dst)
        coord = Coordinator(station)
        coord.enroll('A')
        coord.enroll('B')
        coord.connect(Endpoint('A', 'y'), Endpoint('B', 'u'))  # direction ok
        with self.assertRaises(TypeError) as caught:
            coord.prepare()
        message = str(caught.exception)
        self.assertIn('A', message)  # names source and destination
        self.assertIn('B', message)
        self.assertIn('u', message)
        self.assertFalse(coord.prepared)  # nothing published

        # tick-time: output shape changes between ticks; the tick fails
        # atomically with the prior network committed
        station2 = Station('ticktime')
        station2.sim_dt = 0.5
        calls = {'n': 0}

        def observe(u):
            calls['n'] += 1
            if calls['n'] >= 2:
                return {'y': np.array([1.0, 2.0, 3.0])}
            return {'y': np.array([1.0, 2.0])}

        src2 = SimulatedObject(
            'S', {}, {'x': 0.0}, ['y'],
            evolve=lambda u, x: {'x': x['x']}, observe=observe)
        dst2 = SimulatedObject(
            'D', {'u': np.zeros(2)}, {'x': 0.0}, ['y'],
            evolve=lambda u, x: {'x': x['x'] + float(np.sum(u['u']))},
            observe=lambda x: {'y': x['x']})
        station2.add_object(src2)
        station2.add_object(dst2)
        coord2 = Coordinator(station2)
        coord2.enroll('S')
        coord2.enroll('D')
        coord2.connect(Endpoint('S', 'y'), Endpoint('D', 'u'))
        coord2.prepare()  # tick-zero output shape (2,) is compatible
        # transition 0->1 still reads the committed shape-(2,) S_0 and
        # succeeds, publishing the shape-(3,) output into S_1
        coord2.step()
        self.assertEqual(coord2.tick, 1)
        self.assertEqual(dst2.get_state('x'), 3.0)
        # transition 1->2 reads S_1: the shape-(3,) value now violates
        # the destination spec — the tick fails atomically
        with self.assertRaises(ValueError):
            coord2.step()
        self.assertEqual(coord2.tick, 1)
        self.assertEqual(dst2.get_state('x'), 3.0)
        self.assertEqual(src2.get_state('x'), 0.0)

    def test_r3_tc_05d_fan_out_copies_isolate_ndarray_mutation(self):
        station = Station('alias')
        station.sim_dt = 0.5
        src = SimulatedObject(
            'S', {}, {'x': np.array([1.0, 2.0])}, ['y'],
            evolve=lambda u, x: {'x': x['x']},
            observe=lambda x: {'y': x['x']})
        d1 = SimulatedObject(
            'D1', {'u': np.zeros(2)}, {'x': np.zeros(2)}, ['y'],
            evolve=lambda u, x: {'x': x['x'] + u['u']},
            observe=lambda x: {'y': x['x']})
        d2 = SimulatedObject(
            'D2', {'u': np.zeros(2)}, {'x': np.zeros(2)}, ['y'],
            evolve=lambda u, x: {'x': x['x'] + u['u']},
            observe=lambda x: {'y': x['x']})
        for obj in (src, d1, d2):
            station.add_object(obj)
        coord = Coordinator(station)
        for name in ('S', 'D1', 'D2'):
            coord.enroll(name)
        meas = MockMeasurement('m', np.array([-1.0, -1.0]))
        coord.add_measurement(meas)
        coord.connect(Endpoint('S', 'y'), Endpoint('D1', 'u'))
        coord.connect(Endpoint('S', 'y'), Endpoint('D2', 'u'))
        coord.connect(Endpoint('S', 'y'), Endpoint('m'))
        coord.prepare()
        coord.step()
        delivered = d1.get_input('u')  # fresh copy out of the store
        delivered[0] = 999.0
        self.assertEqual(list(d2.get_input('u')), [1.0, 2.0])
        self.assertEqual(list(meas.read()), [1.0, 2.0])  # snapshot intact
        coord.step()  # source buffer still delivers the committed values
        self.assertEqual(list(d2.get_input('u')), [1.0, 2.0])
        self.assertEqual(list(d1.get_input('u')), [1.0, 2.0])

    def test_r3_tc_05e_unconnected_inputs_retain_staged_values(self):
        station = Station('unconn')
        station.sim_dt = 0.5
        src, _, _ = make_accumulator('A', 1.0)
        dst, _, dlog = make_accumulator('D', 0.0, u0=5.0)
        station.add_object(src)
        station.add_object(dst)
        coord = Coordinator(station)
        coord.enroll('A')
        coord.enroll('D')
        coord.connect(Endpoint('A', 'y'), Endpoint('D', 'u'))
        # a genuinely unconnected input on a third object; enrollment
        # requires STOPPED, so add it before prepare
        extra = SimulatedObject(
            'E', {'v': 5.0}, {'x': 0.0}, ['y'],
            evolve=lambda u, x: {'x': x['x']},
            observe=lambda x: {'y': x['x']})
        station.add_object(extra)
        coord.enroll('E')
        coord.prepare()
        # 'u' is driven by the edge: staging into it is rejected
        with self.assertRaises(RuntimeError):
            dst.set_input('u', 7.0)
        extra.set_input('v', 7.0)  # stages through the controlled boundary
        coord.step()
        coord.step()
        self.assertEqual(extra.get_input('v'), 7.0)  # never overwritten
        self.assertEqual(dlog[-1][0], 1.0)  # edge still delivered

    def test_r3_tc_05f_topology_changes_require_stopped_boundary(self):
        station, coord = self._station_with_endpoints()
        c, _, _ = make_accumulator('C', 100.0, u0=0.0)
        station.add_object(c)
        coord.enroll('C')
        coord.bind_interval('C', 'u')
        coord.connect(Endpoint('A', 'y'), Endpoint('B', 'u'))
        coord.prepare()
        coord.step()
        new_ctrl = MockControl('k2', 1.0)
        new_meas = MockMeasurement('m2', -1.0)
        with self.assertRaises(RuntimeError):
            coord.connect(Endpoint('C', 'y'), Endpoint('B', 'u'))
        with self.assertRaises(RuntimeError):
            coord.disconnect(Endpoint('B', 'u'))
        with self.assertRaises(RuntimeError):
            coord.bind_interval('C', 'u')
        with self.assertRaises(RuntimeError):
            coord.unbind_interval('C', 'u')
        with self.assertRaises(RuntimeError):
            coord.add_control(new_ctrl)
        with self.assertRaises(RuntimeError):
            coord.add_measurement(new_meas)
        with self.assertRaises(RuntimeError):
            coord.withdraw('C')
        with self.assertRaises(RuntimeError):
            coord.rm_control('k')
        e, _, _ = make_accumulator('E', 0.0)
        station.add_object(e)
        with self.assertRaises(RuntimeError):
            coord.enroll('E')  # enrollment requires STOPPED too
        # after the stopped/reset boundary the same edits succeed
        coord.reset()
        coord.disconnect(Endpoint('B', 'u'))
        coord.connect(Endpoint('C', 'y'), Endpoint('B', 'u'))
        coord.unbind_interval('C', 'u')
        coord.bind_interval('C', 'u')
        coord.add_control(new_ctrl)
        coord.add_measurement(new_meas)
        coord.rm_control('k')
        self.assertEqual(len(coord.connections), 1)

    def test_r3_tc_05g_invalid_graph_changes_individually_atomic(self):
        station, coord = self._station_with_endpoints()
        c, _, _ = make_accumulator('C', 100.0)
        station.add_object(c)
        coord.enroll('C')
        coord.connect(Endpoint('A', 'y'), Endpoint('B', 'u'))
        before = coord.connections
        # a sequence of edits: valid then invalid; after the invalid edit
        # the graph is bit-identical to the last valid state
        coord.connect(Endpoint('C', 'y'), Endpoint('A', 'u'))
        mid = coord.connections
        self.assertEqual(len(mid), 2)
        with self.assertRaises(ValueError):
            coord.connect(Endpoint('C', 'y'), Endpoint('B', 'u'))
        self.assertEqual(coord.connections, mid)
        with self.assertRaises(KeyError):
            coord.connect(Endpoint('nope', 'y'), Endpoint('B', 'u'))
        self.assertEqual(coord.connections, mid)
        with self.assertRaises(KeyError):
            coord.disconnect(Endpoint('nope', 'u'))  # no such edge
        self.assertEqual(coord.connections, mid)
        self.assertNotEqual(before, mid)
        # no batch API exists; each edit is individually atomic
        self.assertFalse(hasattr(coord, 'connect_batch'))


class AtomicTickResetTests(unittest.TestCase):
    """R3-TC-06: failed ticks/resets preserve the committed network."""

    def test_r3_tc_06a_evolve_failure_preserves_network_and_pending(self):
        station = Station('atomic')
        station.sim_dt = 0.5
        sentinel = RuntimeError('evolve boom')
        armed = {'flag': False}
        calls = {'evolve': 0}

        def evolve_a(u, x):
            calls['evolve'] += 1
            if armed['flag'] and calls['evolve'] == 2:
                raise sentinel
            return {'x': x['x'] + u['u']}

        a = SimulatedObject(
            'A', {'u': 1.5}, {'x': 1.0}, ['y'], evolve_a,
            observe=lambda x: {'y': x['x']})
        b, _, seen_b = make_accumulator('B', 10.0)
        station.add_object(a)
        station.add_object(b)
        coord = Coordinator(station)
        coord.enroll('A')
        coord.enroll('B')
        ctrl = MockControl('k', 1.5)
        coord.add_control(ctrl)
        coord.connect(Endpoint('k'), Endpoint('A', 'u'))
        coord.connect(Endpoint('A', 'y'), Endpoint('B', 'u'))
        coord.prepare()
        coord.step()  # evolve call 1 succeeds
        self.assertEqual(coord.tick, 1)
        ctrl.write(4.0)  # accepted pending write before the failing tick
        armed['flag'] = True
        with self.assertRaises(RuntimeError) as caught:
            coord.step()
        self.assertIs(caught.exception, sentinel)
        self.assertEqual(coord.tick, 1)  # clock preserved
        self.assertEqual(coord.sim_time, 0.5)
        self.assertEqual(a.get_state('x'), 2.5)  # committed states intact
        self.assertEqual(b.get_state('x'), 11.0)  # evolved in tick 1
        self.assertEqual(ctrl.read(), 4.0)  # pending write survives
        armed['flag'] = False
        coord.step()  # retry delivers the surviving pending value
        self.assertEqual(coord.tick, 2)
        self.assertEqual(seen_b[-1][0], 2.5)  # A's committed tick-1 output
        self.assertEqual(a.get_state('x'), 6.5)  # 2.5 + 4.0

    def test_r3_tc_06b_observe_failure_at_tick_time_is_atomic(self):
        station = Station('obsfail')
        station.sim_dt = 0.5
        sentinel = RuntimeError('observe boom')
        armed = {'flag': False}
        calls = {'observe': 0}

        def observe(x):
            calls['observe'] += 1
            if armed['flag']:
                raise sentinel
            return {'y': x['x']}

        a = SimulatedObject(
            'A', {'u': 0.0}, {'x': 1.0}, ['y'],
            evolve=lambda u, x: {'x': x['x'] + u['u']}, observe=observe)
        station.add_object(a)
        coord = Coordinator(station)
        coord.enroll('A')
        coord.prepare()
        coord.step()
        armed['flag'] = True
        with self.assertRaises(RuntimeError) as caught:
            coord.step()
        self.assertIs(caught.exception, sentinel)
        self.assertEqual(coord.tick, 1)
        self.assertEqual(a.get_state('x'), 1.0)  # no partial publication

    def test_r3_tc_06c_reset_factory_failure_leaves_network_usable(self):
        station = Station('resetfail')
        station.sim_dt = 0.5
        sentinel = RuntimeError('factory boom')
        armed = {'flag': False}

        def factory():
            if armed['flag']:
                raise sentinel
            return 10.0

        a, _, _ = make_accumulator('A', 1.0)
        b = SimulatedObject(
            'B', {'u': 0.0}, {'x': factory}, ['y'],
            evolve=lambda u, x: {'x': x['x'] + u['u']},
            observe=lambda x: {'y': x['x']})
        station.add_object(a)
        station.add_object(b)
        coord = Coordinator(station)
        coord.enroll('A')
        coord.enroll('B')
        coord.connect(Endpoint('A', 'y'), Endpoint('B', 'u'))
        coord.prepare()
        coord.step()
        committed_a = a.get_state('x')
        committed_b = b.get_state('x')
        armed['flag'] = True
        with self.assertRaises(RuntimeError) as caught:
            coord.reset()
        self.assertIs(caught.exception, sentinel)
        self.assertTrue(coord.prepared)  # still running, prior values
        self.assertEqual(coord.tick, 1)
        self.assertEqual(a.get_state('x'), committed_a)
        self.assertEqual(b.get_state('x'), committed_b)
        coord.step()  # behaves as if the reset was never attempted
        self.assertEqual(coord.tick, 2)
        self.assertEqual(a.get_state('x'), committed_a)
        self.assertEqual(b.get_state('x'), committed_b + committed_a)

    def test_r3_tc_06d_successful_reset_rebuilds_everything(self):
        station = Station('fullreset')
        station.sim_dt = 0.5
        a = SimulatedObject(
            'A', {'u': 1.5, 'w': 0.0}, {'x': 1.0}, ['y'],
            evolve=lambda u, x: {'x': x['x'] + u['u'] + u['w']},
            observe=lambda x: {'y': x['x']})
        b, _, _ = make_accumulator('B', 10.0)
        c, _, _ = make_accumulator('C', 100.0)
        d, _, _ = make_accumulator('D', 0.0, u0=5.0)
        for obj in (a, b, c, d):
            station.add_object(obj)
        coord = Coordinator(station)
        for name in ('A', 'B', 'C', 'D'):
            coord.enroll(name)
        ctrl = MockControl('k', 1.5)
        meas = MockMeasurement('m', -1.0)
        coord.add_control(ctrl)
        coord.add_measurement(meas)
        coord.connect(Endpoint('k'), Endpoint('A', 'u'))
        coord.connect(Endpoint('A', 'y'), Endpoint('B', 'u'))
        coord.connect(Endpoint('B', 'y'), Endpoint('C', 'u'))
        coord.connect(Endpoint('C', 'y'), Endpoint('A', 'w'))  # feedback
        coord.connect(Endpoint('C', 'y'), Endpoint('m'))
        coord.prepare()

        def run_steps(steps):
            trace = []
            for _ in range(steps):
                coord.step()
                trace.append(tuple(
                    station.object(name).get_state('x')
                    for name in ('A', 'B', 'C')))
            return trace

        ctrl.write(5.0)  # staged control write (leaks must not survive)
        d.set_input('u', 9.0)  # staged unconnected-input write
        trace = run_steps(2)
        self.assertEqual(coord.tick, 2)
        self.assertNotEqual(meas.read(), -1.0)
        coord.reset()
        # everything visible together: clock, inputs, states, buffers
        self.assertEqual(coord.tick, 0)
        self.assertEqual(coord.sim_time, 0.0)
        self.assertFalse(coord.prepared)
        self.assertEqual(a.get_state('x'), 1.0)
        self.assertEqual(b.get_state('x'), 10.0)
        self.assertEqual(c.get_state('x'), 100.0)
        self.assertEqual(a.get_input('u'), 1.5)
        self.assertEqual(a.get_input('w'), 0.0)  # feedback buffer initial
        self.assertEqual(ctrl.read(), 1.5)  # pending writes do not leak
        self.assertEqual(meas.read(), -1.0)  # tick-zero sample restored
        self.assertEqual(d.get_input('u'), 5.0)  # declared initial
        # topology retained: re-run reproduces the original trace
        ctrl.write(5.0)
        d.set_input('u', 9.0)
        coord.prepare()
        self.assertEqual(run_steps(2), trace)

    def test_r3_tc_06e_reset_failure_atomic_across_participants(self):
        station = Station('multistage')
        station.sim_dt = 0.5
        sentinel = RuntimeError('late factory boom')
        armed = {'flag': False}

        def factory():
            if armed['flag']:
                raise sentinel
            return 100.0

        a, _, _ = make_accumulator('a', 1.0)
        b, _, _ = make_accumulator('b', 10.0)
        c = SimulatedObject(
            'c', {'u': 0.0}, {'x': factory}, ['y'],
            evolve=lambda u, x: {'x': x['x'] + u['u']},
            observe=lambda x: {'y': x['x']})
        for obj in (a, b, c):
            station.add_object(obj)
        coord = Coordinator(station)
        for name in ('a', 'b', 'c'):
            coord.enroll(name)
        coord.connect(Endpoint('a', 'y'), Endpoint('b', 'u'))
        coord.connect(Endpoint('b', 'y'), Endpoint('c', 'u'))
        coord.prepare()
        coord.step()
        committed = {name: station.object(name).get_state('x')
                     for name in ('a', 'b', 'c')}
        armed['flag'] = True  # factory of the later-staged 'c' raises
        with self.assertRaises(RuntimeError) as caught:
            coord.reset()
        self.assertIs(caught.exception, sentinel)
        for name in ('a', 'b', 'c'):  # no partial reset visible
            self.assertEqual(station.object(name).get_state('x'),
                             committed[name])
        self.assertEqual(coord.tick, 1)
        coord.step()  # network fully usable
        self.assertEqual(coord.tick, 2)


class CompatibilityEvidenceTests(unittest.TestCase):
    """R3-TC-08: regression, gates, opt-in property, scope, CI."""

    _REGRESSION_MODULES = (
        'test_compatibility',
        'test_sim_user_guide',
        'test_tu_contracts',
        'test_tu_descriptions',
        'test_tu_integration',
        'test_tu_lifecycle',
        'test_tu_measurements',
        'test_tu_operations',
        'test_tu_simulation',
        'test_tu_simulation_integration',
        'test_tu_station_objects',
        'test_tu_theory',
        'test_tu_visa_contracts',
    )

    def _run_modules(self, module_names):
        if str(TESTS_DIR) not in sys.path:
            sys.path.insert(0, str(TESTS_DIR))
        loader = unittest.TestLoader()
        suite = unittest.TestSuite()
        for name in module_names:
            suite.addTests(loader.loadTestsFromName(name))
        stream = io.StringIO()
        result = unittest.TextTestRunner(stream=stream).run(suite)
        return result, stream.getvalue()

    def test_r3_tc_08a_pre_implementation_regression_baseline(self):
        result, output = self._run_modules(self._REGRESSION_MODULES)
        self.assertEqual(result.testsRun, 165,
                         'baseline suite must hold 165 tests')
        self.assertTrue(result.wasSuccessful(), output)

    def test_r3_tc_08b_post_implementation_gates(self):
        # compile gate
        self.assertTrue(compileall.compile_dir(
            str(REPO_ROOT / 'softlab'), quiet=2, maxlevels=20))
        # import smoke
        softlab = importlib.import_module('softlab')
        self.assertEqual(softlab.__version__, '0.3.0')
        for name in ('softlab.tu.simulation', 'softlab.tu.station',
                     'softlab.tu.simulation.coordination',
                     'softlab.tu.simulation.mock'):
            importlib.import_module(name)

    def test_r3_tc_08c_standalone_compatibility_beyond_new_capability(self):
        modules = (
            'test_tu_simulation',
            'test_tu_simulation_integration',
            'test_tu_station_objects',
            'test_tu_contracts',
            'test_tu_descriptions',
            'test_tu_lifecycle',
        )
        result, output = self._run_modules(modules)
        self.assertTrue(result.wasSuccessful(), output)
        # standalone probes: ordinary stations need no simulation argument
        station = Station('ordinary')
        self.assertIsNone(station.sim_dt)
        self.assertIsNone(station.coordinator)
        device = Device('dev')
        station.add_device(device)
        self.assertIn('dev', station.devices)
        # no coordinated machinery fires without explicit opt-in
        obj, calls, _ = make_accumulator('A', 1.0)
        station.add_object(obj)
        obj.evolve_once()
        obj.reset()
        self.assertEqual(calls['evolve'], 1)
        self.assertIsNone(obj._active_coordinator)

    def test_r3_tc_08d_no_automatic_device_hook_execution(self):
        station = Station('hooks')
        calls = {'set': 0, 'get': 0}
        device = Device('dev')
        para = Parameter(
            'p',
            before_set=lambda old, new: calls.__setitem__('set',
                                                          calls['set'] + 1),
            before_get=lambda stored: calls.__setitem__('get',
                                                        calls['get'] + 1))
        device.add_parameter(para)
        station.add_device(device)
        station.snapshot()
        station.describe()
        # ordinary station operations execute no arbitrary device hooks
        self.assertEqual(calls['set'], 0)
        self.assertEqual(calls['get'], 0)
        # mock participation is opt-in only: a standalone mock endpoint
        # touches no station, device or parameter
        ctrl = MockControl('k', 0.0)
        meas = MockMeasurement('m', -1.0)
        self.assertIsNone(ctrl._coordinator)
        self.assertIsNone(meas._coordinator)
        ctrl.write(1.0)
        self.assertEqual(ctrl.read(), 1.0)
        self.assertEqual(meas.read(), -1.0)
        self.assertEqual(calls['set'], 0)
        self.assertEqual(calls['get'], 0)

    def test_r3_tc_08e_no_new_dependencies_and_scope(self):
        import re
        allowed = {'numbers', 'math', 'typing', 'collections',
                   'collections.abc', 'numpy', 'np'}
        for filename in ('coordination.py', 'mock.py'):
            path = REPO_ROOT / 'softlab' / 'tu' / 'simulation' / filename
            source = path.read_text()
            for match in re.finditer(
                    r'^(?:from|import)\s+([A-Za-z_][A-Za-z0-9_.]*)',
                    source, re.MULTILINE):
                root = match.group(1).split('.')[0]
                if root in ('softlab',):
                    continue
                self.assertIn(root, allowed,
                              f'{filename} imports third-party {root}')
            # import-direction rule: no station import, no huo process
            self.assertIsNone(re.search(
                r'^\s*(?:from|import)\s+softlab\.tu\.station',
                source, re.MULTILINE),
                f'{filename} must not import station (D1)')
            self.assertIsNone(re.search(
                r'^\s*(?:from|import)\s+.*\bhuo\b',
                source, re.MULTILINE))

    def test_r3_tc_08f_candidate_ci_workflow_matrix(self):
        workflow = REPO_ROOT / '.github' / 'workflows' / 'ci.yml'
        self.assertTrue(workflow.exists())
        content = workflow.read_text()
        self.assertIn('3.9', content)
        self.assertIn('3.13', content)


if __name__ == '__main__':
    unittest.main()
