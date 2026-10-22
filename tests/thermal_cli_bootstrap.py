"""Fresh CLI test child: expose four approved dependency warnings on stderr."""

import re
import runpy
import sys
import warnings

from pyparsing.warnings import PyparsingDeprecationWarning


DEPENDENCY_WARNINGS = (
    ("'oneOf' deprecated - use 'one_of'", "matplotlib._fontconfig_pattern"),
    ("'parseString' deprecated - use 'parse_string'", "matplotlib._fontconfig_pattern"),
    ("'resetCache' deprecated - use 'reset_cache'", "matplotlib._fontconfig_pattern"),
    ("'enablePackrat' deprecated - use 'enable_packrat'", "matplotlib._mathtext"),
)


def install_warning_policy() -> None:
    """Keep all warnings fatal except exact approved, visible dependency cases."""
    warnings.simplefilter("error")
    for message, module in DEPENDENCY_WARNINGS:
        warnings.filterwarnings(
            "default", message="^" + re.escape(message) + "$",
            category=PyparsingDeprecationWarning,
            module="^" + re.escape(module) + "$",
        )


if __name__ == "__main__":
    install_warning_policy()
    # Retain the actual script path and original CLI arguments in a new process.
    sys.argv = sys.argv[1:]
    runpy.run_path(sys.argv[0], run_name="__main__")
