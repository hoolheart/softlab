"""R3-004 connected-example acceptance cases (R3-TC-09a/09b).

Verifies the guide's §13 executed connected example: the recorded
output equals an independent in-test derivation of the declared
coordination semantics (R3-TC-09a), and the guide's code block
byte-reproduces the recorded output across runs (R3-TC-09b).
stdlib + the project only (no NumPy needed).
"""

import contextlib
import io
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
GUIDE_PATH = REPO_ROOT / 'docs' / 'user-guide' / 'simulation.md'

NAMES = ('A', 'B', 'C', 'D')
INITIAL_STATES = {'A': 1.0, 'B': 10.0, 'C': 100.0, 'D': 0.0}
CONTROL_INITIAL = 2.0
MEASUREMENT_INITIAL = -1.0
SIM_DT = 0.5
# (source, destination) for the five §13 connections; 'drive' is the
# mock control source and 'probe' the mock measurement sink
EDGES = (('A', 'B'), ('B', 'C'), ('C', 'A'),
         ('drive', 'D'), ('C', 'probe'))


def connected_example_and_output():
    """Extract the executed-example code block and the recorded output
    block from the guide's §13 (same convention as §11's harness)."""
    content = GUIDE_PATH.read_text(encoding='utf-8')
    section = content.split('## 13. Executed connected example')[1]
    code = section.split('```python')[1].split('```')[0]
    expected = section.split('```text')[1].split('```')[0]
    return code, expected


class _ReferenceRun:
    """Independent reference model of the §13 scenario, implementing
    the documented coordination semantics only: at transition k->k+1
    every destination consumes the committed boundary snapshot S_k
    (sources' previous-boundary values) plus the frozen control
    pending; states accumulate ``x_next = x + u`` with ``y = x``;
    the measurement receives the S_k source copy; ``sim_time`` is
    ``float(tick) * sim_dt``; reset restores constructor initials."""

    def __init__(self):
        self.states = dict(INITIAL_STATES)
        self.pending = CONTROL_INITIAL
        self.sample = MEASUREMENT_INITIAL
        self.tick = 0
        self.prepared = False
        self._snapshot = None
        self.delivered = {name: 0.0 for name in NAMES}

    @property
    def sim_time(self):
        return float(self.tick) * SIM_DT

    def prepare(self):
        # S_0 from initial states; nothing is delivered at prepare, so
        # the measurement sample keeps its constructor initial.
        self._snapshot = dict(self.states)
        self.tick = 0
        self.prepared = True

    def step(self):
        snapshot = self._snapshot
        delivered = {name: 0.0 for name in NAMES}
        for source, dest in EDGES:
            value = self.pending if source == 'drive' \
                else snapshot[source]
            if dest == 'probe':
                self.sample = value
            else:
                delivered[dest] = value
        for name in NAMES:
            self.states[name] += delivered[name]
        self.delivered = delivered
        self.tick += 1
        self._snapshot = dict(self.states)

    def write_control(self, value):
        # stages the pending value; never ticks, never touches states
        self.pending = value

    def reset(self):
        self.states = dict(INITIAL_STATES)
        self.pending = CONTROL_INITIAL
        self.sample = MEASUREMENT_INITIAL
        self.tick = 0
        self.prepared = False
        self._snapshot = None
        self.delivered = {name: 0.0 for name in NAMES}


def derive_golden_output():
    """Build the expected §13 recorded output from the reference
    model, mirroring the print templates of the guide program."""
    run = _ReferenceRun()
    lines = [
        'connected example: chain A -> B -> C, feedback C -> A, '
        'drive -> D, C.y -> probe',
        f'sim_dt = {SIM_DT:.1f}',
    ]

    def states_line():
        return '  states: ' + ' '.join(
            f'{name}.x={run.states[name]:.1f}' for name in NAMES)

    def delivered_line():
        return '  delivered: ' + ' '.join(
            f'{name}.u={run.delivered[name]:.1f}' for name in NAMES)

    run.prepare()
    lines.append(f'after prepare: tick={run.tick} '
                 f'sim_time={run.sim_time:.1f}')
    lines.append(states_line())
    lines.append(f'  drive pending={run.pending:.1f}  '
                 f'probe={run.sample:.1f}')

    run.step()
    lines.append(f'step to tick={run.tick} sim_time={run.sim_time:.1f}')
    lines.append(delivered_line())
    lines.append(states_line())
    lines.append(f'  probe={run.sample:.1f}')

    run.write_control(3.0)
    lines.append(f'write drive=3.0 between ticks: '
                 f'readback={run.pending:.1f}, tick still {run.tick}, '
                 f'D.x still {run.states["D"]:.1f}')

    run.step()
    lines.append(f'step to tick={run.tick} sim_time={run.sim_time:.1f}')
    lines.append(delivered_line())
    lines.append(states_line())
    lines.append(f'  probe={run.sample:.1f}')

    run.reset()
    lines.append(f'after reset: tick={run.tick} '
                 f'sim_time={run.sim_time:.1f} prepared={run.prepared}')
    lines.append(states_line())
    lines.append(f'  drive pending={run.pending:.1f}  '
                 f'probe={run.sample:.1f}')

    run.prepare()
    run.step()
    lines.append(f're-run after prepare + one step: tick={run.tick} '
                 f'sim_time={run.sim_time:.1f}')
    lines.append(states_line())
    lines.append(f'  probe={run.sample:.1f}')

    return '\n'.join(lines) + '\n'


class ConnectedGuideTests(unittest.TestCase):
    """R3-TC-09a/09b: executed connected example matches the derived
    golden trace and byte-reproduces the recorded output."""

    def test_r3_tc_09a_golden_trace(self):
        """R3-TC-09a: the §13 program's executed output equals an
        independent derivation of the declared coordination
        semantics (the derivation, not the implementation, is the
        expectation source)."""
        code, _ = connected_example_and_output()
        namespace = {'__name__': '__guide_example__'}
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            exec(compile(code, str(GUIDE_PATH), 'exec'), namespace)
        actual = buffer.getvalue()
        self.assertTrue(actual.endswith('\n'))
        self.assertEqual(
            actual.rstrip('\n'),
            derive_golden_output().rstrip('\n'),
            'executed output must equal the independently derived '
            'golden trace')

    def test_r3_tc_09b_byte_reproduction(self):
        """R3-TC-09b: the §13 code block executes byte-identically
        across runs and equals the guide's recorded output block
        byte-for-byte."""
        code, expected = connected_example_and_output()
        namespace = {'__name__': '__guide_example__'}

        def run_once():
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                exec(compile(code, str(GUIDE_PATH), 'exec'), namespace)
            return buffer.getvalue()

        first = run_once()
        second = run_once()
        self.assertEqual(
            first.rstrip('\n'), expected.strip('\n'),
            'example output must equal the guide recorded output')
        self.assertTrue(first.endswith('\n'))
        self.assertEqual(first, second)  # byte-identical repeat run


if __name__ == '__main__':
    unittest.main()
