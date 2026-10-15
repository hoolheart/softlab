"""R3-004 arch.md reference-integrity case (R3-TC-09h).

Verifies that every reference target in the root architecture
document — both Markdown link targets and inline-code spans that look
like repository paths — resolves to an existing path under the
repository root, and that the Release 3 section cites its own
evidence trail. stdlib only.
"""

import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
ARCH_PATH = REPO_ROOT / 'arch.md'

FENCE_RE = re.compile(r'```.*?```', re.DOTALL)
LINK_RE = re.compile(r'\[[^\]]*\]\(([^)\s]+)\)')
SPAN_RE = re.compile(r'`([^`]+)`')
PATH_EXT_RE = re.compile(
    r'^(?:[\w.~-]+/)+[\w.~-]+\.(?:py|md|toml|yaml|txt|rst)$')


def collect_reference_targets(content):
    """Collect repo-relative reference targets from Markdown links and
    inline-code path spans, skipping URLs, pure fragments and fenced
    code blocks; fragment suffixes are stripped before resolution."""
    body = FENCE_RE.sub('', content)
    targets = [match.group(1) for match in LINK_RE.finditer(body)]
    for match in SPAN_RE.finditer(body):
        span = match.group(1).strip()
        if '/' in span and PATH_EXT_RE.match(span):
            targets.append(span)
    resolved = []
    for target in targets:
        if target.startswith(('http://', 'https://')):
            continue
        path = target.split('#', 1)[0].strip()
        if path:
            resolved.append(path)
    return resolved


class ArchReferenceTests(unittest.TestCase):
    """R3-TC-09h: arch.md links and path spans resolve."""

    def test_r3_tc_09h_arch_reference_integrity(self):
        """Every reference target in arch.md resolves under the repo
        root; the collection is non-empty and includes at least one
        log/release_3/ target (the Release 3 record cites its own
        evidence trail)."""
        self.assertTrue(
            ARCH_PATH.is_file(),
            f'{ARCH_PATH} missing: architecture record required')
        content = ARCH_PATH.read_text(encoding='utf-8')
        targets = collect_reference_targets(content)
        self.assertTrue(targets, 'no reference targets collected')
        missing = sorted({
            target for target in targets
            if not (REPO_ROOT / target).exists()
        })
        self.assertEqual(
            missing, [],
            f'arch.md references missing paths: {missing}')
        self.assertTrue(
            any(target.startswith('log/release_3/') for target in targets),
            'the Release 3 architecture record must cite its own '
            'evidence trail under log/release_3/')


if __name__ == '__main__':
    unittest.main()
