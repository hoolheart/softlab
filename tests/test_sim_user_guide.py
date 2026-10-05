"""User-guide acceptance cases for SIM-001 (SIM-TC-08a--08d).

Verifies the delivery documentation of ``softlab.tu.simulation``:
the user-guide topic checklist (SIM-TC-08a), the guide's executed
deterministic example re-run against its recorded output (SIM-TC-08b),
API docstring presence/quality (SIM-TC-08c), and the assembled test
evidence record (SIM-TC-08d). stdlib + NumPy only.
"""

import contextlib
import io
import re
import unittest
from pathlib import Path

from softlab.tu.simulation import SimulatedObject

REPO_ROOT = Path(__file__).resolve().parent.parent
GUIDE_PATH = REPO_ROOT / 'docs' / 'user-guide' / 'simulation.md'
RESULTS_PATH = REPO_ROOT / 'log' / 'release_2' / 'test' / \
    'sim-001-test-results.md'

ALL_CASE_IDS = tuple(
    [f'SIM-TC-01{c}' for c in 'abcde']
    + [f'SIM-TC-02{c}' for c in 'abcde']
    + [f'SIM-TC-03{c}' for c in 'abcd']
    + [f'SIM-TC-04{c}' for c in 'abcd']
    + [f'SIM-TC-05{c}' for c in 'abcdefghi']
    + [f'SIM-TC-06{c}' for c in 'abcde']
    + [f'SIM-TC-07{c}' for c in 'abcde']
    + [f'SIM-TC-08{c}' for c in 'abcd']
)


def guide_example_and_output():
    """Extract the executed-example code block and the recorded output
    block from the guide's §11."""
    content = GUIDE_PATH.read_text(encoding='utf-8')
    section = content.split('## 11. Executed example')[1]
    code = section.split('```python')[1].split('```')[0]
    expected = section.split('```text')[1].split('```')[0]
    return code, expected


class UserGuideTests(unittest.TestCase):
    """SIM-TC-08a--08d: usable, honestly bounded delivery."""

    def test_sim_tc_08a_guide_topic_checklist(self):
        """SIM-TC-08a: the user guide exists, is English, and covers
        every required topic consistently with the implemented API."""
        self.assertTrue(
            GUIDE_PATH.is_file(),
            f'{GUIDE_PATH} missing: user guide is a SIM-AC-08 artifact')
        content = GUIDE_PATH.read_text(encoding='utf-8')
        lowered = content.lower()
        required_topics = {
            'construction': 'construction',
            'evolve contract': 'evolve',
            'observation': 'observation',
            'reset': 'reset',
            'error model': 'error',
            'ownership/aliasing rules': 'ownership/aliasing',
            'supported value contract': 'value contract',
            'optional time context': 'time context',
            'mock-device vs object distinction': 'mock device',
            'deterministic-callback contract': 'deterministic',
            'external side effects caveat':
                'outside every rollback guarantee',
            'device/parameter bridge': 'parameter',
            'limitations': 'limitation',
        }
        for topic, token in required_topics.items():
            self.assertIn(token, lowered,
                          f'user guide must cover {topic!r}')
        self.assertIn('SimulatedObject', content)
        # English-only heuristic: no CJK characters
        self.assertIsNone(
            re.search(r'[\u4e00-\u9fff]', content),
            'user guide must be English')

    def test_sim_tc_08b_executed_deterministic_example(self):
        """SIM-TC-08b: the guide's example executes deterministically;
        actual output equals the guide's recorded output byte-for-byte;
        repeated execution is byte-identical."""
        code, expected = guide_example_and_output()
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

    def test_sim_tc_08c_api_docstrings(self):
        """SIM-TC-08c: sampled public API docstrings are English with
        parameters/returns/errors per repo convention."""
        samples = {
            'SimulatedObject': SimulatedObject.__doc__,
            '__init__': SimulatedObject.__init__.__doc__,
            'evolve_once': SimulatedObject.evolve_once.__doc__,
            'observe_outputs': SimulatedObject.observe_outputs.__doc__,
            'reset': SimulatedObject.reset.__doc__,
            'set_input': SimulatedObject.set_input.__doc__,
            'get_state': SimulatedObject.get_state.__doc__,
        }
        for name, doc in samples.items():
            self.assertIsNotNone(doc, f'{name} must have a docstring')
            self.assertGreater(len(doc), 20, f'{name} docstring too short')
            self.assertIsNone(
                re.search(r'[\u4e00-\u9fff]', doc),
                f'{name} docstring must be English')
        # the class docstring carries the full constructor contract
        class_doc = samples['SimulatedObject']
        self.assertGreater(len(class_doc), 200)
        for section in ('Args', 'Errors'):
            self.assertIn(section, class_doc)
        # the main entry points document arguments and errors
        for name in ('evolve_once', 'observe_outputs',
                     'reset', 'set_input', 'get_state'):
            doc = samples[name]
            self.assertIn('Args', doc, f'{name} must document arguments')
            self.assertRegex(doc, r'(Errors|error)',
                             f'{name} must document error cases')
        self.assertIn('Returns', samples['evolve_once'])
        self.assertIn('Returns', samples['observe_outputs'])
        self.assertIn('Returns', samples['get_state'])
        # __init__ delegates to the class docstring (repo convention)
        self.assertIn('class docstring', samples['__init__'])

    def test_sim_tc_08d_evidence_assembly(self):
        """SIM-TC-08d: the test-results record assembles the full
        evidence (unittest, compile, import, warning gate, CI
        reference), records every planned case ID as PASS, and marks
        nothing passed that was not run."""
        self.assertTrue(
            RESULTS_PATH.is_file(),
            f'{RESULTS_PATH} missing: test evidence record required '
            'before integration')
        content = RESULTS_PATH.read_text(encoding='utf-8')
        lowered = content.lower()
        for marker in ('unittest', 'compileall', 'import smoke',
                       '-W error', 'warning gate', 'CHK-06-1',
                       'CHK-06-2', 'CI'):
            self.assertIn(marker.lower(), lowered,
                          f'evidence record must mention {marker!r}')
        # every planned case ID recorded with an explicit PASS verdict
        for case_id in ALL_CASE_IDS:
            self.assertRegex(
                content, rf'\|\s*{re.escape(case_id)}\s*\|\s*PASS',
                f'evidence record must list {case_id} as PASS')
        # recorded suite result is a real OK run, not a silent waiver
        self.assertRegex(content, r'Ran \d+ tests in [\d.]+s')
        self.assertRegex(content, r'(?m)^OK$')
        self.assertNotIn('WAIVER', content)


if __name__ == '__main__':
    unittest.main()
