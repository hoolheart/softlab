"""Fresh CLI test child: expose approved baseline warnings on stderr."""

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


PANDAS_BASELINE_WARNING = (
    "The behavior of DataFrame concatenation with empty or all-NA entries is "
    "deprecated. In a future version, this will no longer exclude empty or "
    "all-NA columns when determining the result dtypes. To retain the old "
    "behavior, exclude the relevant entries before the concat operation."
)


def install_warning_policy() -> None:
    """Keep all warnings fatal except exact approved, visible baseline cases."""
    warnings.simplefilter("error")
    for message, module in DEPENDENCY_WARNINGS:
        warnings.filterwarnings(
            "default", message="^" + re.escape(message) + "$",
            category=PyparsingDeprecationWarning,
            module="^" + re.escape(module) + "$",
        )
    warnings.filterwarnings(
        "default", message="^" + re.escape(PANDAS_BASELINE_WARNING) + "$",
        category=FutureWarning, module=r"^softlab\.shui\.data\.base$",
    )


if __name__ == "__main__":
    install_warning_policy()
    # Retain the actual script path and original CLI arguments in a new process.
    sys.argv = sys.argv[1:]
    runpy.run_path(sys.argv[0], run_name="__main__")
