"""Object-contract acceptance cases for SIM-001 (deterministic
simulated-object foundation).

Implements SIM-TC-01a--01e, 02a--02e, 03a--03d, 04a--04d and 05a--05i of
``log/release_2/test/sim-001-test-plan.md`` (revision 2) against
``softlab.tu.simulation.SimulatedObject``. All fixtures are synthetic;
stdlib + NumPy only; no scheduler, no hardware, no wall-clock access.
"""

import unittest
from pathlib import Path

import numpy as np

from softlab.jin.validator import ValNumber
from softlab.tu.simulation import SimulatedObject
from softlab.tu.station import (
    Device,
    Parameter,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
GUIDE_PATH = REPO_ROOT / 'docs' / 'user-guide' / 'simulation.md'


def make_accumulator():
    """Scalar accumulator ``x' = x + u`` with non-identity observation
    ``y = G(x) = 2x``, plus evolve/observe call counters and a log of
    the ``(u, x_previous)`` pairs each evolve callback received."""
    calls = {'evolve': 0, 'observe': 0}
    seen = []

    def evolve(u, x):
        calls['evolve'] += 1
        seen.append((u['u'], x['x']))
        return {'x': x['x'] + u['u']}

    def observe(x):
        calls['observe'] += 1
        return {'y': 2.0 * x['x']}

    obj = SimulatedObject(
        'acc', {'u': 0.0}, {'x': 0.0}, ['y'], evolve, observe)
    return obj, calls, seen


def make_integrator():
    """Scalar integrator with explicit ``dt`` input and ``t`` state,
    mirroring the user-guide example model."""
    return SimulatedObject(
        'integrator',
        {'u': 0.0, 'dt': 0.0},
        {'t': 0.0, 'x': 0.0},
        ['time', 'position'],
        evolve=lambda u, x: {
            't': x['t'] + u['dt'],
            'x': x['x'] + u['dt'] * u['u'],
        },
        observe=lambda x: {'time': x['t'], 'position': x['x']},
    )


def make_wired_object():
    """Simulated object wired to plain ``Device``/``Parameter`` mock
    devices: a control device whose ``drive`` parameter setter feeds
    the object input, and an observation device whose ``signal``
    parameter getter reads object outputs."""
    obj, calls, _ = make_accumulator()
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


class DeclarationContractTests(unittest.TestCase):
    """SIM-TC-01: distinct input/state/output roles and value contract."""

    def test_sim_tc_01a_scalar_declaration_contract(self):
        """SIM-TC-01a: declarations enumerate exactly the declared
        variables per role; roles are distinct namespaces; initial
        values follow the documented defaults."""
        obj = make_integrator()
        self.assertEqual(obj.name, 'integrator')
        self.assertEqual(obj.input_names, ('u', 'dt'))
        self.assertEqual(obj.state_names, ('t', 'x'))
        self.assertEqual(obj.output_names, ('time', 'position'))
        # roles are distinct namespaces with unique names overall
        all_names = obj.input_names + obj.state_names + obj.output_names
        self.assertEqual(len(all_names), 6)
        self.assertEqual(len(set(all_names)), 6)
        self.assertIn('u', obj.input_names)
        self.assertNotIn('u', obj.state_names)
        self.assertNotIn('u', obj.output_names)
        # initial values per documented defaults
        self.assertEqual(obj.get_input('u'), 0.0)
        self.assertEqual(obj.get_input('dt'), 0.0)
        self.assertEqual(obj.get_state('t'), 0.0)
        self.assertEqual(obj.get_state('x'), 0.0)
        self.assertEqual(obj.observe_outputs(),
                         {'time': 0.0, 'position': 0.0})

    def test_sim_tc_01b_ndarray_value_contract(self):
        """SIM-TC-01b: ndarray inputs are accepted; outputs keep the
        pinned dtype/shape; no object arrays appear."""
        obj = SimulatedObject(
            'vec',
            {'u': np.zeros(2), 'dt': 0.0},
            {'x': np.zeros(2)},
            ['y'],
            evolve=lambda u, x: {'x': x['x'] + u['u'] * u['dt']},
            observe=lambda x: {'y': x['x']},
        )
        obj.set_input('u', np.array([1.0, 2.0]))
        obj.set_input('dt', 0.5)
        obj.evolve_once()
        out = obj.observe_outputs()['y']
        self.assertIs(type(out), np.ndarray)
        self.assertEqual(out.dtype, np.dtype(np.float64))
        self.assertEqual(out.shape, (2,))
        self.assertTrue(np.array_equal(out, np.array([0.5, 1.0])))
        self.assertFalse(np.issubdtype(out.dtype, np.object_))

    def test_sim_tc_01c_invalid_declaration_errors(self):
        """SIM-TC-01c: every invalid declaration raises the documented
        error type with a useful message; no partially constructed
        object escapes."""
        evolve = lambda u, x: {'x': x['x']}  # noqa: E731
        observe = lambda x: {'y': x['x']}  # noqa: E731
        base = dict(name='t', inputs={'u': 0.0}, states={'x': 0.0},
                    outputs=['y'], evolve=evolve, observe=observe)

        def construct(**overrides):
            return SimulatedObject(**{**base, **overrides})

        # name must be a non-empty str
        with self.assertRaises(ValueError):
            construct(name='')
        with self.assertRaises(ValueError):
            construct(name=42)
        # inputs/states must be mappings
        with self.assertRaises(TypeError):
            construct(inputs=['u'])
        with self.assertRaises(TypeError):
            construct(states='x')
        # outputs must be a sequence of names
        with self.assertRaises(TypeError):
            construct(outputs='y')
        # callbacks must be callable
        with self.assertRaises(TypeError):
            construct(evolve=None)
        with self.assertRaises(TypeError):
            construct(observe=None)
        # duplicate name across roles is prohibited
        with self.assertRaises(ValueError) as cm:
            construct(inputs={'x': 0.0})
        self.assertIn("'x'", str(cm.exception))
        # duplicate output names are prohibited
        with self.assertRaises(ValueError) as cm:
            construct(outputs=['y', 'y'])
        self.assertIn("'y'", str(cm.exception))
        # input name colliding with an output name is prohibited
        with self.assertRaises(ValueError) as cm:
            construct(inputs={'y': 0.0})
        self.assertIn("'y'", str(cm.exception))
        # empty variable names are prohibited
        with self.assertRaises(ValueError):
            construct(inputs={'': 0.0})
        # non-numeric initial values are rejected
        with self.assertRaises(TypeError) as cm:
            construct(inputs={'u': 'bad'})
        self.assertIn("'u'", str(cm.exception))
        with self.assertRaises(ValueError):
            construct(states={'x': np.array([object()], dtype=object)})

    def test_sim_tc_01d_unknown_variable_errors(self):
        """SIM-TC-01d: reading or writing an undeclared name in any
        role raises a KeyError-family error naming the variable."""
        obj, _, _ = make_accumulator()
        for operation in (
            lambda: obj.set_input('nope', 1.0),
            lambda: obj.get_input('nope'),
            lambda: obj.get_state('nope'),
        ):
            with self.assertRaises(KeyError) as cm:
                operation()
            self.assertIn('nope', str(cm.exception))

    def test_sim_tc_01e_incompatible_value_errors(self):
        """SIM-TC-01e: incompatible values raise the documented
        validation error and leave committed state unchanged."""
        obj = SimulatedObject(
            'typed',
            {'n': 0, 'f': 0.0, 'v': np.zeros(2)},
            {'x': 0.0},
            ['y'],
            evolve=lambda u, x: {
                'x': x['x'] + float(u['n']) + u['f'] + float(np.sum(u['v'])),
            },
            observe=lambda x: {'y': x['x']},
        )
        obj.set_input('n', 3)
        obj.set_input('f', 1.0)
        obj.evolve_once()
        committed = obj.get_state('x')
        committed_out = obj.observe_outputs()

        def assert_unchanged():
            self.assertEqual(obj.get_state('x'), committed)
            self.assertEqual(obj.observe_outputs(), committed_out)
            self.assertEqual(obj.get_input('n'), 3)
            self.assertEqual(obj.get_input('f'), 1.0)

        # float to an int-pinned input
        with self.assertRaises(TypeError):
            obj.set_input('n', 1.5)
        assert_unchanged()
        # int to a float-pinned input
        with self.assertRaises(TypeError):
            obj.set_input('f', 1)
        assert_unchanged()
        # str to a numeric input
        with self.assertRaises(TypeError):
            obj.set_input('n', 'x')
        assert_unchanged()
        # ndarray shape mismatch
        with self.assertRaises(ValueError) as cm:
            obj.set_input('v', np.zeros(3))
        self.assertIn("'v'", str(cm.exception))
        assert_unchanged()
        # ndarray dtype mismatch
        with self.assertRaises(ValueError) as cm:
            obj.set_input('v', np.zeros(2, dtype=np.int64))
        self.assertIn("'v'", str(cm.exception))
        assert_unchanged()


class EvolutionSemanticsTests(unittest.TestCase):
    """SIM-TC-02: dynamic and memoryless evaluation; evolve-once per
    explicit evolution."""

    def test_sim_tc_02a_accumulating_evolve_semantics(self):
        """SIM-TC-02a: one explicit evolution invokes ``evolve``
        exactly once with the committed input and previous state; the
        state and the output reflect the resulting state."""
        obj, calls, seen = make_accumulator()
        obj.set_input('u', 1.0)
        obj.evolve_once()
        self.assertEqual(calls['evolve'], 1)
        self.assertEqual(seen, [(1.0, 0.0)])
        self.assertEqual(obj.get_state('x'), 1.0)
        self.assertEqual(obj.observe_outputs(), {'y': 2.0})

    def test_sim_tc_02b_sequence_accumulates_deterministically(self):
        """SIM-TC-02b: each further explicit evolution increments the
        state by the committed input; the evolve callback ran exactly
        once per explicit evolution."""
        obj, calls, seen = make_accumulator()
        obj.set_input('u', 1.0)
        obj.evolve_once()
        for expected in (2.0, 3.0, 4.0):
            obj.evolve_once()
            self.assertEqual(obj.get_state('x'), expected)
        self.assertEqual(calls['evolve'], 4)
        self.assertEqual(seen, [(1.0, 0.0), (1.0, 1.0),
                                (1.0, 2.0), (1.0, 3.0)])

    def test_sim_tc_02c_memoryless_model(self):
        """SIM-TC-02c: a model whose observation depends only on the
        current input reproduces outputs per input, independent of
        history."""
        obj = SimulatedObject(
            'gain', {'u': 0.0}, {'held': 0.0}, ['y'],
            evolve=lambda u, x: {'held': u['u']},
            observe=lambda x: {'y': 10.0 * x['held']},
        )
        obj.set_input('u', 1.0)
        obj.evolve_once()
        self.assertEqual(obj.observe_outputs(), {'y': 10.0})
        obj.set_input('u', 2.0)
        obj.evolve_once()
        self.assertEqual(obj.observe_outputs(), {'y': 20.0})
        obj.set_input('u', 1.0)
        obj.evolve_once()
        self.assertEqual(obj.observe_outputs(), {'y': 10.0})
        # input assignment alone does not change the outputs
        obj.set_input('u', 3.0)
        self.assertEqual(obj.observe_outputs(), {'y': 10.0})
        obj.evolve_once()
        self.assertEqual(obj.observe_outputs(), {'y': 30.0})

    def test_sim_tc_02d_dt_as_input_context(self):
        """SIM-TC-02d: ``dt`` supplied as an explicit input advances the
        model deterministically; no hidden wall-clock dt is used and
        results are byte-for-byte reproducible across runs."""
        def make_clock():
            return SimulatedObject(
                'clock', {'dt': 0.0}, {'t': 0.0}, ['time'],
                evolve=lambda u, x: {'t': x['t'] + u['dt']},
                observe=lambda x: {'time': x['t']},
            )

        def script(obj):
            obj.set_input('dt', 0.25)
            observations = []
            for _ in range(4):
                obj.evolve_once()
                observations.append(obj.observe_outputs()['time'])
            return observations

        first = script(make_clock())
        second = script(make_clock())
        self.assertEqual(first, [0.25, 0.5, 0.75, 1.0])
        self.assertEqual(first, second)

    def test_sim_tc_02e_output_from_resulting_state(self):
        """SIM-TC-02e: the observed output equals ``G(x_next)``, not
        ``G(x_previous)``, for a non-identity observation."""
        obj, _, _ = make_accumulator()  # y = G(x) = 2x
        self.assertEqual(obj.get_state('x'), 0.0)
        obj.set_input('u', 1.0)
        obj.evolve_once()
        # G(x_previous) = 0.0, G(x_next) = G(1.0) = 2.0
        self.assertEqual(obj.get_state('x'), 1.0)
        self.assertEqual(obj.observe_outputs(), {'y': 2.0})


class ReadWithoutEvolutionTests(unittest.TestCase):
    """SIM-TC-03: reads never evolve, never mutate, never touch
    hardware or a scheduler."""

    def test_sim_tc_03a_inspection_does_not_invoke_evolve(self):
        """SIM-TC-03a: reading state, outputs and declarations leaves
        the evolve call count at zero and the committed state
        unchanged."""
        obj, calls, _ = make_accumulator()
        obj.set_input('u', 1.0)
        obj.evolve_once()
        state_before = obj.get_state('x')
        evolve_count = calls['evolve']
        # declaration reads
        self.assertEqual(obj.name, 'acc')
        self.assertEqual(obj.input_names, ('u',))
        self.assertEqual(obj.state_names, ('x',))
        self.assertEqual(obj.output_names, ('y',))
        # state and output reads
        self.assertEqual(obj.get_input('u'), 1.0)
        self.assertEqual(obj.get_state('x'), state_before)
        self.assertEqual(obj.observe_outputs(), {'y': 2.0 * state_before})
        # nothing evolved, nothing changed
        self.assertEqual(calls['evolve'], evolve_count)
        self.assertEqual(obj.get_state('x'), state_before)

    def test_sim_tc_03b_repeated_observation_stability(self):
        """SIM-TC-03b: two consecutive observations are identical and
        both equal ``G(x_committed)``."""
        obj, _, _ = make_accumulator()
        obj.set_input('u', 1.0)
        obj.evolve_once()
        obj.evolve_once()
        first = obj.observe_outputs()
        second = obj.observe_outputs()
        self.assertEqual(first, second)
        self.assertEqual(first, {'y': 4.0})

    def test_sim_tc_03c_input_assignment_alone_does_not_evolve(self):
        """SIM-TC-03c: assigning an input without requesting evolution
        leaves state and outputs at the last committed evolution; the
        new input is visible as a pending (stored, unconsumed) input."""
        obj, calls, _ = make_accumulator()
        obj.set_input('u', 1.0)
        obj.evolve_once()
        self.assertEqual(calls['evolve'], 1)
        obj.set_input('u', 5.0)
        self.assertEqual(calls['evolve'], 1)
        self.assertEqual(obj.get_state('x'), 1.0)
        self.assertEqual(obj.observe_outputs(), {'y': 2.0})
        self.assertEqual(obj.get_input('u'), 5.0)

    def test_sim_tc_03d_no_hardware_scheduler_access_on_read(self):
        """SIM-TC-03d: reads through parameter getters of a mock device
        succeed with no scheduler running, no event loop and no
        evolution."""
        obj, calls, _, drive, _, signal = make_wired_object()
        evolve_count = calls['evolve']
        # plain synchronous reads; this file never starts a scheduler
        self.assertEqual(drive(), 0.0)
        self.assertEqual(signal(), 0.0)
        self.assertEqual(obj.get_state('x'), 0.0)
        self.assertEqual(obj.observe_outputs(), {'y': 0.0})
        self.assertEqual(calls['evolve'], evolve_count)


class RepeatablePreparationTests(unittest.TestCase):
    """SIM-TC-04: reset restores the initial condition; replay
    reproduces observations; independent objects share nothing."""

    def test_sim_tc_04a_reset_restores_initial_condition(self):
        """SIM-TC-04a: after evolving away (including a time state),
        reset restores documented initial inputs, states and outputs,
        matching a pristine object."""
        obj = make_integrator()
        pristine = make_integrator()
        obj.set_input('u', 2.0)
        obj.set_input('dt', 0.5)
        for _ in range(3):
            obj.evolve_once()
        self.assertEqual(obj.get_state('t'), 1.5)
        self.assertEqual(obj.get_state('x'), 3.0)
        obj.reset()
        self.assertEqual(obj.get_input('u'), 0.0)
        self.assertEqual(obj.get_input('dt'), 0.0)
        self.assertEqual(obj.get_state('t'), 0.0)
        self.assertEqual(obj.get_state('x'), 0.0)
        self.assertEqual(obj.observe_outputs(),
                         {'time': 0.0, 'position': 0.0})
        # subsequent observation matches the pristine initial object
        self.assertEqual(obj.get_input('u'), pristine.get_input('u'))
        self.assertEqual(obj.get_input('dt'), pristine.get_input('dt'))
        self.assertEqual(obj.get_state('t'), pristine.get_state('t'))
        self.assertEqual(obj.get_state('x'), pristine.get_state('x'))
        self.assertEqual(obj.observe_outputs(), pristine.observe_outputs())

    def test_sim_tc_04b_replay_reproduces_observations(self):
        """SIM-TC-04b: replaying the identical input assignment plus
        explicit evolution sequence reproduces every observation
        exactly."""
        obj, _, _ = make_accumulator()
        script = [1.0, 2.0, 0.5, 1.5]

        def run_script():
            observations = []
            for value in script:
                obj.set_input('u', value)
                obj.evolve_once()
                observations.append(obj.observe_outputs()['y'])
            return observations

        first = run_script()
        obj.reset()
        second = run_script()
        x = 0.0
        expected = []
        for value in script:
            x += value
            expected.append(2.0 * x)
        self.assertEqual(first, expected)
        self.assertEqual(second, first)

    def test_sim_tc_04c_independent_objects_do_not_share_mutable_state(
            self):
        """SIM-TC-04c: two independently constructed objects with
        ndarray state share no mutable container; evolving one and
        mutating its inspection values never affects the other."""
        def make():
            return SimulatedObject(
                'vec', {'u': np.zeros(2)},
                {'v': np.array([1.0, 0.0])},
                ['y'],
                evolve=lambda u, x: {'v': x['v'] + u['u']},
                observe=lambda x: {'y': x['v']},
            )

        obj_a, obj_b = make(), make()
        obj_a.set_input('u', np.array([1.0, 1.0]))
        obj_a.evolve_once()
        # B is untouched by A's evolution
        self.assertTrue(np.array_equal(
            obj_b.get_state('v'), np.array([1.0, 0.0])))
        self.assertTrue(np.array_equal(
            obj_b.observe_outputs()['y'], np.array([1.0, 0.0])))
        # mutating an inspection value returned from A does not touch B
        leaked = obj_a.get_state('v')
        leaked[:] = 999.0
        self.assertTrue(np.array_equal(
            obj_b.get_state('v'), np.array([1.0, 0.0])))
        # A's committed state is likewise unaffected by the mutation
        self.assertTrue(np.array_equal(
            obj_a.get_state('v'), np.array([2.0, 1.0])))
        # no shared mutable container: inspections are distinct objects
        self.assertIsNot(obj_a.get_state('v'), obj_b.get_state('v'))
        self.assertIsNot(obj_a.get_state('v'), obj_a.get_state('v'))
        self.assertIsNot(obj_a.observe_outputs()['y'],
                         obj_b.observe_outputs()['y'])

    def test_sim_tc_04d_reset_after_failed_evolution(self):
        """SIM-TC-04d: a failed evolution leaves no residue; reset then
        succeeds and restores the documented initial condition."""
        boom = RuntimeError('boom')

        def evolve(u, x):
            if u['u'] == -1.0:
                raise boom
            return {'x': x['x'] + u['u']}

        obj = SimulatedObject(
            'flaky', {'u': 0.0}, {'x': 0.0}, ['y'], evolve,
            observe=lambda x: {'y': x['x']},
        )
        obj.set_input('u', 1.0)
        obj.evolve_once()
        self.assertEqual(obj.get_state('x'), 1.0)
        obj.set_input('u', -1.0)
        with self.assertRaises(RuntimeError) as cm:
            obj.evolve_once()
        self.assertIs(cm.exception, boom)
        self.assertEqual(obj.get_state('x'), 1.0)  # no partial publish
        obj.reset()  # succeeds despite the earlier failure
        self.assertEqual(obj.get_input('u'), 0.0)
        self.assertEqual(obj.get_state('x'), 0.0)
        self.assertEqual(obj.observe_outputs(), {'y': 0.0})


class FailureAtomicityOwnershipTests(unittest.TestCase):
    """SIM-TC-05: predictable failures, atomicity, and mutable-alias
    protection."""

    def test_sim_tc_05a_failure_atomicity_input_update(self):
        """SIM-TC-05a: a rejected input update propagates the original
        error unwrapped; committed state, outputs and the previously
        valid input are unchanged."""
        obj = SimulatedObject(
            'typed', {'n': 0}, {'x': 0.0}, ['y'],
            evolve=lambda u, x: {'x': x['x'] + float(u['n'])},
            observe=lambda x: {'y': x['x']},
        )
        obj.set_input('n', 3)
        obj.evolve_once()
        committed = obj.get_state('x')
        with self.assertRaises(TypeError) as cm:
            obj.set_input('n', 1.5)  # int-pinned input rejects float
        self.assertIsNone(cm.exception.__cause__)  # never wrapped
        self.assertEqual(obj.get_input('n'), 3)
        self.assertEqual(obj.get_state('x'), committed)
        self.assertEqual(obj.observe_outputs(), {'y': committed})

    def test_sim_tc_05b_failure_atomicity_evolve(self):
        """SIM-TC-05b: a raising evolve callback propagates the exact
        original exception object; committed state and outputs remain
        bit-identical to the snapshot."""
        boom = RuntimeError('boom')

        def evolve(u, x):
            if u['u'] == -1.0:
                raise boom
            return {'x': x['x'] + u['u']}

        obj = SimulatedObject(
            'flaky', {'u': 0.0}, {'x': 0.0}, ['y'], evolve,
            observe=lambda x: {'y': 2.0 * x['x']},
        )
        obj.set_input('u', 1.0)
        obj.evolve_once()
        state_snapshot = obj.get_state('x')
        out_snapshot = obj.observe_outputs()
        obj.set_input('u', -1.0)
        with self.assertRaises(RuntimeError) as cm:
            obj.evolve_once()
        self.assertIs(cm.exception, boom)  # identity preserved
        self.assertIsNone(cm.exception.__cause__)
        self.assertEqual(obj.get_state('x'), state_snapshot)
        self.assertEqual(obj.observe_outputs(), out_snapshot)

    def test_sim_tc_05c_mutable_alias_protection_caller_supplied(self):
        """SIM-TC-05c: in-place mutation of caller-supplied containers
        (assignment values and construction-time initial values) never
        changes committed state."""
        obj = SimulatedObject(
            'vec', {'u': np.zeros(2)},
            {'v': np.array([1.0, 0.0])},
            ['y'],
            evolve=lambda u, x: {'v': x['v'] + u['u']},
            observe=lambda x: {'y': x['v']},
        )
        u = np.array([1.0, 2.0])
        obj.set_input('u', u)
        u[:] = 999.0
        self.assertTrue(np.array_equal(
            obj.get_input('u'), np.array([1.0, 2.0])))
        init = np.array([1.0, 0.0])
        other = SimulatedObject(
            'vec2', {'u': np.zeros(2)}, {'v': init}, ['y'],
            evolve=lambda u, x: {'v': x['v'] + u['u']},
            observe=lambda x: {'y': x['v']},
        )
        init[:] = 999.0
        self.assertTrue(np.array_equal(
            other.get_state('v'), np.array([1.0, 0.0])))

    def test_sim_tc_05d_mutable_alias_protection_returned_values(self):
        """SIM-TC-05d: mutating inspection return values in place never
        changes committed state; no returned object aliases committed
        storage."""
        obj = SimulatedObject(
            'vec', {'u': np.zeros(2)},
            {'v': np.array([1.0, 2.0])},
            ['y'],
            evolve=lambda u, x: {'v': x['v'] + u['u']},
            observe=lambda x: {'y': x['v']},
        )
        state = obj.get_state('v')
        state[:] = 999.0
        self.assertTrue(np.array_equal(
            obj.get_state('v'), np.array([1.0, 2.0])))
        outputs = obj.observe_outputs()
        self.assertIsNot(outputs['y'], obj.observe_outputs()['y'])
        outputs['y'][:] = 999.0
        self.assertTrue(np.array_equal(
            obj.observe_outputs()['y'], np.array([1.0, 2.0])))

    def test_sim_tc_05e_mutable_alias_protection_callback_arguments(
            self):
        """SIM-TC-05e: an adversarial evolve callback that mutates or
        retains its arguments cannot corrupt committed state: callback
        arguments are copies, and nothing handed to a callback is
        retained by the object."""
        retained = {}

        def evolve(u, x):
            retained['u'] = u['u']
            retained['x'] = x['v']
            nxt = x['v'] + np.array([1.0, 1.0])
            retained['next'] = nxt
            u['u'][:] = -999.0  # in-place mutation of the argument
            return {'v': nxt}

        obj = SimulatedObject(
            'vec', {'u': np.zeros(2)},
            {'v': np.array([1.0, 2.0])},
            ['y'],
            evolve=evolve,
            observe=lambda x: {'y': x['v']},
        )
        obj.set_input('u', np.array([5.0, 6.0]))
        obj.evolve_once()
        # in-callback mutation of the copied argument left stores intact
        self.assertTrue(np.array_equal(
            obj.get_input('u'), np.array([5.0, 6.0])))
        self.assertTrue(np.array_equal(
            obj.get_state('v'), np.array([2.0, 3.0])))
        # mutating every retained reference after the call changes
        # nothing committed
        retained['u'][:] = 777.0
        retained['x'][:] = 777.0
        retained['next'][:] = 777.0
        self.assertTrue(np.array_equal(
            obj.get_input('u'), np.array([5.0, 6.0])))
        self.assertTrue(np.array_equal(
            obj.get_state('v'), np.array([2.0, 3.0])))
        self.assertTrue(np.array_equal(
            obj.observe_outputs()['y'], np.array([2.0, 3.0])))

    def test_sim_tc_05f_unsupported_value_category_documented(self):
        """SIM-TC-05f: every deliberately unsupported value category is
        clearly rejected with the documented error naming the
        variable."""
        obj, _, _ = make_accumulator()
        with self.assertRaises(TypeError) as cm:
            obj.set_input('u', True)  # bool subclasses int, not exact
        self.assertIn("'u'", str(cm.exception))
        with self.assertRaises(TypeError) as cm:
            obj.set_input('u', 'nope')
        self.assertIn("'u'", str(cm.exception))
        with self.assertRaises(TypeError) as cm:
            obj.set_input('u', [1.0, 2.0])
        self.assertIn("'u'", str(cm.exception))
        # ndarray of any dtype against a float-pinned input: category
        # mismatch (the object-dtype ValueError path is pinned below on
        # an ndarray-pinned variable)
        with self.assertRaises(TypeError) as cm:
            obj.set_input('u', np.array([1.0, 'x'], dtype=object))
        self.assertIn("'u'", str(cm.exception))
        # object-dtype ndarray against an ndarray-pinned variable
        vec = SimulatedObject(
            'vecrej', {'v': np.zeros(2)}, {'x': 0.0}, ['y'],
            evolve=lambda u, x: {'x': x['x']},
            observe=lambda x: {'y': x['x']},
        )
        with self.assertRaises(ValueError) as cm:
            vec.set_input('v', np.array([1.0, 'x'], dtype=object))
        self.assertIn("'v'", str(cm.exception))
        with self.assertRaises(TypeError) as cm:
            SimulatedObject(
                'bad', {'u': np.float64(1.0)}, {'x': 0.0}, ['y'],
                evolve=lambda u, x: {'x': x['x']},
                observe=lambda x: {'y': x['x']})
        message = str(cm.exception)
        self.assertIn('float(v)', message)
        self.assertIn('v.item()', message)
        with self.assertRaises(TypeError) as cm:
            SimulatedObject(
                'bad', {'u': False}, {'x': 0.0}, ['y'],
                evolve=lambda u, x: {'x': x['x']},
                observe=lambda x: {'y': x['x']})
        self.assertIn('Boolean', str(cm.exception))

    def test_sim_tc_05g_external_side_effects_out_of_rollback_scope(self):
        """SIM-TC-05g: the user guide documents that external side
        effects of user callbacks are outside rollback guarantees."""
        self.assertTrue(
            GUIDE_PATH.is_file(),
            f'{GUIDE_PATH} missing: user guide is a SIM-AC-08 artifact')
        content = GUIDE_PATH.read_text(encoding='utf-8')
        self.assertIn('outside every rollback guarantee', content)

    def test_sim_tc_05h_failure_atomicity_observation_callback(self):
        """SIM-TC-05h: a raising observation callback propagates the
        exact original exception object; committed state is unchanged;
        the prior observation stands; a subsequent valid observation
        succeeds."""
        bad_obs = ValueError('bad observation')

        def observe(x):
            if x['x'] == 2.0:
                raise bad_obs
            return {'y': x['x']}

        obj = SimulatedObject(
            'obs', {'u': 0.0}, {'x': 0.0}, ['y'],
            evolve=lambda u, x: {'x': x['x'] + u['u']},
            observe=observe,
        )
        prior = obj.observe_outputs()
        self.assertEqual(prior, {'y': 0.0})
        obj.set_input('u', 2.0)
        obj.evolve_once()  # evolve into the offending state: x == 2.0
        snapshot = obj.get_state('x')
        with self.assertRaises(ValueError) as cm:
            obj.observe_outputs()
        self.assertIs(cm.exception, bad_obs)  # identity preserved
        self.assertIsNone(cm.exception.__cause__)
        self.assertEqual(obj.get_state('x'), snapshot)  # state intact
        self.assertEqual(prior, {'y': 0.0})  # prior observation stands
        obj.reset()
        self.assertEqual(obj.observe_outputs(), {'y': 0.0})

    def test_sim_tc_05i_failure_atomicity_reset(self):
        """SIM-TC-05i: a factory raising during reset propagates the
        identical exception object; committed inputs, states and
        observations equal the pre-reset snapshot exactly; the object
        remains fully usable; a later reset succeeds and restores the
        documented initial condition."""
        calls = {'count': 0, 'armed': False}
        boom = RuntimeError('reset boom')

        def factory():
            calls['count'] += 1
            if calls['armed'] and calls['count'] == 2:
                raise boom
            return 0.0

        evolve = lambda u, x: {'x': x['x'] + u['u']}  # noqa: E731
        observe = lambda x: {'y': x['x']}  # noqa: E731
        obj = SimulatedObject(
            'resetb', {'u': 0.0}, {'x': factory}, ['y'], evolve, observe)
        self.assertEqual(calls['count'], 1)  # factory ran once at build
        obj.set_input('u', 1.0)
        obj.evolve_once()
        snap_input = obj.get_input('u')
        snap_state = obj.get_state('x')
        snap_out = obj.observe_outputs()
        calls['armed'] = True
        with self.assertRaises(RuntimeError) as cm:
            obj.reset()
        self.assertIs(cm.exception, boom)  # identical object propagates
        self.assertIsNone(cm.exception.__cause__)
        self.assertEqual(calls['count'], 2)
        # no partially committed reset: exact pre-reset snapshot
        self.assertEqual(obj.get_input('u'), snap_input)
        self.assertEqual(obj.get_state('x'), snap_state)
        self.assertEqual(obj.observe_outputs(), snap_out)
        # object remains fully usable
        obj.set_input('u', 2.0)
        obj.evolve_once()
        self.assertEqual(obj.get_state('x'), snap_state + 2.0)
        self.assertEqual(obj.observe_outputs(), {'y': snap_state + 2.0})
        # disarmed factory: a second reset succeeds and restores the
        # documented initial condition, matching a pristine object
        calls['armed'] = False
        obj.reset()
        self.assertEqual(calls['count'], 3)
        self.assertEqual(obj.get_input('u'), 0.0)
        self.assertEqual(obj.get_state('x'), 0.0)
        self.assertEqual(obj.observe_outputs(), {'y': 0.0})
        pristine = SimulatedObject(
            'resetb', {'u': 0.0}, {'x': 0.0}, ['y'], evolve, observe)
        self.assertEqual(obj.get_input('u'), pristine.get_input('u'))
        self.assertEqual(obj.get_state('x'), pristine.get_state('x'))
        self.assertEqual(obj.observe_outputs(), pristine.observe_outputs())


class MalformedCallbackKeyTests(unittest.TestCase):
    """R3-TC-07e--07j: wrong/missing/extra callback keys — including
    mixed string and non-string keys — raise the documented
    ``ValueError``; non-mapping results remain ``TypeError``; failed
    evolutions keep build-then-swap atomicity."""

    def _object_with_evolve(self, evolve):
        return SimulatedObject(
            'malformed', {'u': 1.0}, {'x': 0.0}, ['y'], evolve,
            observe=lambda x: {'y': x['x']},
        )

    def test_r3_tc_07e_mixed_extra_keys_evolve_value_error(self):
        """R3-TC-07e: an ``evolve`` result mixing a string and a
        non-string extra key raises the documented ``ValueError``
        (never the incidental sorting ``TypeError``) and leaves the
        committed state unchanged."""
        obj = self._object_with_evolve(
            lambda u, x: {'x': x['x'] + u['u'], 'z': 0.0, 2: 0.0})
        snapshot = obj.get_state('x')
        with self.assertRaises(ValueError) as cm:
            obj.evolve_once()
        message = str(cm.exception)
        self.assertIn("'z'", message)
        self.assertIn('2', message)
        self.assertEqual(obj.get_state('x'), snapshot)

    def test_r3_tc_07f_mixed_extra_keys_observe_value_error(self):
        """R3-TC-07f: an ``observe`` result mixing a string and a
        non-string extra key raises the documented ``ValueError`` and
        touches no committed store."""
        obj = SimulatedObject(
            'malformed', {'u': 0.0}, {'x': 0.0}, ['y'],
            evolve=lambda u, x: {'x': x['x']},
            observe=lambda x: {'y': x['x'], 'z': 0.0, 2: 0.0},
        )
        with self.assertRaises(ValueError) as cm:
            obj.observe_outputs()
        self.assertIn("'z'", str(cm.exception))
        self.assertEqual(obj.get_state('x'), 0.0)
        self.assertEqual(obj.get_input('u'), 0.0)

    def test_r3_tc_07g_single_non_string_extra_key_named(self):
        """R3-TC-07g: a single non-string extra key still raises a
        ``ValueError`` whose message names the key, byte-identical to
        the pre-fix rendering."""
        obj = self._object_with_evolve(
            lambda u, x: {'x': x['x'] + u['u'], 2: 0.0})
        with self.assertRaises(ValueError) as cm:
            obj.evolve_once()
        self.assertIn('extra [2]', str(cm.exception))

    def test_r3_tc_07h_pure_string_key_discrepancies_named(self):
        """R3-TC-07h: pure-string missing/extra key discrepancies raise
        the documented ``ValueError`` naming the discrepancy."""
        obj = self._object_with_evolve(lambda u, x: {})
        with self.assertRaises(ValueError) as cm:
            obj.evolve_once()
        self.assertIn("'x'", str(cm.exception))
        obj = self._object_with_evolve(
            lambda u, x: {'x': x['x'] + u['u'], 'z': 0.0})
        with self.assertRaises(ValueError) as cm:
            obj.evolve_once()
        self.assertIn("'z'", str(cm.exception))

    def test_r3_tc_07i_non_mapping_results_remain_type_error(self):
        """R3-TC-07i: non-mapping callback results raise ``TypeError``
        ("must return a mapping") — checked before any key handling."""
        obj = self._object_with_evolve(lambda u, x: [('x', 1.0)])
        with self.assertRaises(TypeError) as cm:
            obj.evolve_once()
        self.assertIn('must return a mapping', str(cm.exception))
        obj = SimulatedObject(
            'malformed', {'u': 0.0}, {'x': 0.0}, ['y'],
            evolve=lambda u, x: {'x': x['x']},
            observe=lambda x: 42,
        )
        with self.assertRaises(TypeError) as cm:
            obj.observe_outputs()
        self.assertIn('must return a mapping', str(cm.exception))

    def test_r3_tc_07j_failed_evolve_atomicity_under_malformed_keys(self):
        """R3-TC-07j: the key-discrepancy ``ValueError`` raises before
        the commit, so committed state stays bit-identical and a later
        valid evolution still succeeds."""
        calls = {'count': 0}

        def evolve(u, x):
            calls['count'] += 1
            if calls['count'] == 2:
                return {'x': x['x'], 'z': 0.0, 2: 0.0}
            return {'x': x['x'] + u['u']}

        obj = self._object_with_evolve(evolve)
        obj.evolve_once()
        self.assertEqual(obj.get_state('x'), 1.0)
        snapshot = obj.get_state('x')
        with self.assertRaises(ValueError):
            obj.evolve_once()
        self.assertEqual(obj.get_state('x'), snapshot)
        obj.evolve_once()  # still fully usable
        self.assertEqual(obj.get_state('x'), 2.0)


class ResetRestorationCharacterizationTests(unittest.TestCase):
    """R3-TC-07l/07m: reset restores owned stores from their declared
    sources — re-invoking factories and tracking their current results
    — and a failed reset preserves the owned stores and the identical
    exception object."""

    def test_r3_tc_07l_reset_reinvokes_factory_and_tracks_it(self):
        """R3-TC-07l: each ``reset()`` re-invokes a factory-declared
        variable's factory exactly once; the committed value tracks the
        factory's current result — reset makes no equivalence claim to
        fresh construction."""
        calls = {'count': 0}

        def factory():
            calls['count'] += 1
            return calls['count'] * 10.0

        obj = SimulatedObject(
            'tracked', {'u': factory}, {'x': 0.0}, ['y'],
            evolve=lambda u, x: {'x': x['x'] + u['u']},
            observe=lambda x: {'y': x['x']},
        )
        self.assertEqual(calls['count'], 1)
        self.assertEqual(obj.get_input('u'), 10.0)
        obj.set_input('u', 1.0)  # move away from the factory value
        obj.evolve_once()
        obj.reset()
        self.assertEqual(calls['count'], 2)
        self.assertEqual(obj.get_input('u'), 20.0)
        obj.reset()
        self.assertEqual(calls['count'], 3)
        self.assertEqual(obj.get_input('u'), 30.0)

    def test_r3_tc_07m_failed_reset_preserves_stores_and_identity(self):
        """R3-TC-07m: a raising factory propagates the identical
        original exception object; the owned inputs and states equal
        the pre-reset snapshot exactly; a later reset succeeds."""
        calls = {'count': 0, 'armed': False}
        boom = RuntimeError('reset boom')

        def factory():
            calls['count'] += 1
            if calls['armed']:
                raise boom
            return 0.0

        obj = SimulatedObject(
            'armed', {'u': 0.0}, {'x': factory}, ['y'],
            evolve=lambda u, x: {'x': x['x'] + u['u']},
            observe=lambda x: {'y': x['x']},
        )
        snap_input = obj.get_input('u')
        snap_state = obj.get_state('x')
        calls['armed'] = True
        with self.assertRaises(RuntimeError) as cm:
            obj.reset()
        self.assertIs(cm.exception, boom)
        self.assertIsNone(cm.exception.__cause__)
        self.assertEqual(obj.get_input('u'), snap_input)
        self.assertEqual(obj.get_state('x'), snap_state)
        calls['armed'] = False
        obj.reset()
        self.assertEqual(obj.get_input('u'), 0.0)
        self.assertEqual(obj.get_state('x'), 0.0)
        self.assertEqual(obj.observe_outputs(), {'y': 0.0})


if __name__ == '__main__':
    unittest.main()
