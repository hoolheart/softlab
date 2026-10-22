"""Selectivity for the approved fresh-child dependency-warning disposition."""

import contextlib
import importlib.util
import io
from pathlib import Path
import unittest
import warnings

from pyparsing.warnings import PyparsingDeprecationWarning

SPEC = importlib.util.spec_from_file_location(
    "thermal_warning_child", Path(__file__).with_name("thermal_cli_bootstrap.py"))
child = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(child)


class ThermalChildWarningTests(unittest.TestCase):
    """No project, pandas or unreviewed dependency warning is exempted."""

    def test_each_exact_dependency_warning_is_visible_on_stderr(self):
        for message, module in child.DEPENDENCY_WARNINGS:
            with self.subTest(message=message), warnings.catch_warnings():
                child.install_warning_policy()
                stderr = io.StringIO()
                with contextlib.redirect_stderr(stderr):
                    warnings.warn_explicit(message, PyparsingDeprecationWarning,
                                           "dependency_fixture.py", 1, module=module)
                self.assertIn("PyparsingDeprecationWarning", stderr.getvalue())
                self.assertIn(message, stderr.getvalue())

    def test_changed_message_category_module_and_project_warnings_stay_fatal(self):
        message, module = child.DEPENDENCY_WARNINGS[0]
        cases = [
            (message + " unexpected", PyparsingDeprecationWarning, module),
            (message, UserWarning, module),
            (message, PyparsingDeprecationWarning, module + ".unreviewed"),
            (message, PyparsingDeprecationWarning, "softlab.warning_probe"),
            ("new project warning", UserWarning, "softlab.warning_probe"),
            ("baseline concat warning", FutureWarning, "softlab.shui.data.base"),
        ]
        for text, category, origin in cases:
            with self.subTest(category=category.__name__, origin=origin), warnings.catch_warnings():
                child.install_warning_policy()
                with self.assertRaises(category):
                    warnings.warn_explicit(text, category, "warning_fixture.py", 1,
                                           module=origin)


if __name__ == "__main__":
    unittest.main()
