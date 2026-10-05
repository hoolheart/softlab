"""Integration acceptance cases for SIM-001 (SIM-TC-06a--06e).

Verifies the ``Device``/``Parameter`` bridge over a shared
:class:`softlab.tu.simulation.SimulatedObject` and the ``huo``
count/scan integration with explicit stepping bound to
``hook_before_get`` / ``hook_after_set`` (never ``hook_before_set``),
evolving exactly once per point with zero real sleeps. ``huo`` runs
through its public ``softlab.huo.process`` entry points only; no
production code under ``softlab/huo/`` is touched. The scheduler is
started/stopped through the ``get_scheduler()`` + ``run_process``
pattern; no global state leaks between tests. All fixtures are
synthetic; stdlib + NumPy only.
"""

import unittest

from softlab.huo.process import (
    count,
    run_process,
    scan,
)
from softlab.huo.scheduler import get_scheduler
from softlab.jin.validator import ValNumber
from softlab.tu.simulation import SimulatedObject
from softlab.tu.station import (
    Device,
    Parameter,
)


def make_accumulator():
    """Scalar accumulator ``x' = x + u`` with observation ``y = x``
    plus an evolve call counter."""
    calls = {'evolve': 0}

    def evolve(u, x):
        calls['evolve'] += 1
        return {'x': x['x'] + u['u']}

    obj = SimulatedObject(
        'plant', {'u': 0.0}, {'x': 0.0}, ['y'], evolve,
        observe=lambda x: {'y': x['x']},
    )
    return obj, calls


def make_stepper(obj, calls, log):
    """No-arg synchronous stepping closure for huo hooks: evolves the
    object exactly once and records the invocation."""

    def step():
        log.append(None)
        obj.evolve_once()

    return step


def build_bridge():
    """One simulated object shared between a control mock device
    (``drive`` setter parameter feeding the input) and an observation
    mock device (``signal`` getter parameter reading the outputs)."""
    obj, calls = make_accumulator()
    control = Device('controls')
    drive = Parameter(
        'drive', ValNumber(),
        before_set=lambda old, new: obj.set_input('u', new))
    drive(0.0)
    control.add_parameter(drive)
    observer = Device('observer')
    signal = Parameter(
        'signal', ValNumber(),
        before_get=lambda stored: obj.observe_outputs()['y'])
    observer.add_parameter(signal)
    return obj, calls, control, drive, observer, signal


class ParameterBridgeTests(unittest.TestCase):
    """SIM-TC-06a--06c: Device/Parameter bridge and shared object."""

    def test_sim_tc_06a_parameter_bridge_for_inputs(self):
        """SIM-TC-06a: setting the input parameter through its normal
        set path updates the object's committed input exactly once,
        evolves nothing, and the read-back reflects the stored
        (pending) input."""
        obj, calls = make_accumulator()
        sets = []

        def on_set(old, new):
            sets.append(new)
            obj.set_input('u', new)

        drive = Parameter('drive', ValNumber(), before_set=on_set)
        drive(0.0)
        sets.clear()
        drive(1.0)
        self.assertEqual(sets, [1.0])  # exactly one object update
        self.assertEqual(calls['evolve'], 0)  # no evolution on set
        self.assertEqual(obj.get_input('u'), 1.0)
        self.assertEqual(drive(), 1.0)  # read-back contract
        # the parameter's own validation gate stays intact and fires
        # before the bridge: a rejected value never reaches the object
        with self.assertRaises(Exception):
            drive('not-a-number')
        self.assertEqual(sets, [1.0])
        self.assertEqual(obj.get_input('u'), 1.0)

    def test_sim_tc_06b_parameter_bridge_for_observations(self):
        """SIM-TC-06b: after one explicit advancement, the measurement
        parameter getter returns ``G(x_next)``; the read itself does
        not advance state."""
        obj, calls = make_accumulator()
        signal = Parameter(
            'signal', ValNumber(),
            before_get=lambda stored: obj.observe_outputs()['y'])
        obj.set_input('u', 1.0)
        obj.evolve_once()  # the single explicit advancement
        self.assertEqual(calls['evolve'], 1)
        self.assertEqual(signal(), 1.0)  # G(x_next) with y = x
        self.assertEqual(signal(), 1.0)
        self.assertEqual(calls['evolve'], 1)  # reads never advance

    def test_sim_tc_06c_one_object_shared_between_devices(self):
        """SIM-TC-06c: a control device and an observation device
        closing over the same object share it without per-device state
        copies: the observation device sees the state produced by the
        control device's input."""
        obj, calls, control, drive, observer, signal = build_bridge()
        drive(2.0)  # set input via device A
        self.assertEqual(calls['evolve'], 0)
        obj.evolve_once()  # explicit advancement via the defined hook
        self.assertEqual(signal(), 2.0)  # read via device B
        self.assertEqual(signal(), 2.0)  # consistent, stable reads
        self.assertEqual(calls['evolve'], 1)
        # the object itself is the single source of truth
        self.assertEqual(obj.get_state('x'), 2.0)


class HuoProcessIntegrationTests(unittest.TestCase):
    """SIM-TC-06d--06e: huo count/scan with explicit stepping."""

    def setUp(self):
        self._scheduler = get_scheduler()
        self._scheduler.start()

    def tearDown(self):
        if self._scheduler.is_running:
            self._scheduler.stop()

    def test_sim_tc_06d_huo_count_integration_with_explicit_stepping(
            self):
        """SIM-TC-06d: a huo count over the measurement parameter with
        stepping bound to ``hook_before_get`` evolves exactly once per
        count point (delays default to zero: no real sleep is
        interpreted as a simulation step), and the recorded values
        equal a manual replay of the same steps."""
        obj, calls = make_accumulator()
        signal = Parameter(
            'signal', ValNumber(),
            before_get=lambda stored: obj.observe_outputs()['y'])
        obj.set_input('u', 1.0)
        steps = []
        proc = count('acquire', None, None, signal, times=5,
                     hook_before_get=make_stepper(obj, calls, steps))
        success, _ = run_process(proc, self._scheduler, verbose=False)
        self.assertTrue(success)
        # exactly one evolve per count point, stepped in the hook
        self.assertEqual(len(steps), 5)
        self.assertEqual(calls['evolve'], 5)
        recorded = proc.record.table['signal'].tolist()
        # manual replay of the identical steps on a fresh object (the
        # count run held the input at u=1.0 throughout)
        replay_obj, _ = make_accumulator()
        replay_obj.set_input('u', 1.0)
        expected = []
        for _ in range(5):
            replay_obj.evolve_once()
            expected.append(replay_obj.observe_outputs()['y'])
        self.assertEqual(recorded, expected)
        self.assertEqual(recorded, [1.0, 2.0, 3.0, 4.0, 5.0])

    def test_sim_tc_06e_huo_scan_integration_with_explicit_stepping(
            self):
        """SIM-TC-06e: a huo scan over a settable parameter driving the
        model input with stepping bound to ``hook_after_set`` evolves
        exactly once per scan point; recorded outputs match the
        analytic sequence (no ``hook_before_set`` off-by-one); two
        identical executions are deterministic."""
        values = [0.0, 1.0, 2.0, 3.0]
        expected_signal = [0.0, 1.0, 3.0, 6.0]  # cumulative x' = x + u

        def run_once():
            obj, calls = make_accumulator()
            drive = Parameter(
                'drive', ValNumber(),
                before_set=lambda old, new: obj.set_input('u', new))
            drive(0.0)
            signal = Parameter(
                'signal', ValNumber(),
                before_get=lambda stored: obj.observe_outputs()['y'])
            steps = []
            proc = scan('sweep', [signal], None, None, drive, values,
                        hook_after_set=make_stepper(obj, calls, steps))
            success, _ = run_process(
                proc, self._scheduler, verbose=False)
            self.assertTrue(success)
            return proc, steps, calls, obj

        proc, steps, calls, obj = run_once()
        # one evolution per scan point, stepped after the set commits
        self.assertEqual(len(steps), len(values))
        self.assertEqual(calls['evolve'], len(values))
        table = proc.record.table
        self.assertEqual(table['drive'].tolist(), values)
        self.assertEqual(table['signal'].tolist(), expected_signal)
        # analytic cross-check by manual replay of the identical steps
        obj.reset()
        replayed = []
        for value in values:
            obj.set_input('u', value)
            obj.evolve_once()
            replayed.append(obj.observe_outputs()['y'])
        self.assertEqual(table['signal'].tolist(), replayed)
        # determinism across two identical executions
        proc2, _, _, _ = run_once()
        self.assertEqual(
            proc2.record.table['signal'].tolist(),
            table['signal'].tolist())
        self.assertEqual(
            proc2.record.table['drive'].tolist(),
            table['drive'].tolist())


if __name__ == '__main__':
    unittest.main()
