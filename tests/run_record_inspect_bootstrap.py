"""Fresh-process run-record inspection child: JSON payload on stdout.

Test-side bootstrap modeled on ``tests/thermal_cli_bootstrap.py``: a
standalone module that installs the warning policy and runs the safe
inspection path against one stored artifact. It holds no test state —
every fact in its payload comes from the stored artifact via
:func:`softlab.shui.run_storage.inspect_run_group`. stdout carries the
single JSON payload and nothing else (the backend connect message is
redirected to stderr around the inspection call); stderr carries
operational prints, diagnostics, and tracebacks.

Usage::

    python tests/run_record_inspect_bootstrap.py <path> \
        --profile {thermal,signal} [--group-id <uuid>]

Exit codes: ``0`` = inspection completed (payload on stdout, including
legacy absence and cleanup-only failure); ``1`` = inspection failure
(error payload on stdout, traceback on stderr); ``2`` = CLI usage error
(argparse default, message on stderr).
"""

import argparse
from contextlib import redirect_stdout
import json
import re
import sys
import warnings

from pyparsing.warnings import PyparsingDeprecationWarning

# NOTE: ``softlab.shui.run_storage`` is imported lazily inside ``main``
# after ``install_warning_policy()`` — on Python 3.9 the softlab import
# chain (jin.vis -> matplotlib -> pyparsing) emits the approved baseline
# warnings, so the policy must be active before that import runs.


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


def _report_payload(report) -> dict:
    """Serialize the frozen StorageReport container one-for-one."""
    return {
        'operation': report.operation,
        'path': report.path,
        'primary_ok': report.primary_ok,
        'primary': list(report.primary),
        'cleanup_ok': report.cleanup_ok,
        'cleanup': list(report.cleanup),
    }


def _records_payload(result) -> dict:
    """Serialize per-record facts and samples of the hydrated group.

    ``columns`` is in h5py enumeration order (lexicographic, as
    physically stored); sample values come from the hydrated group's
    record tables via ``tolist()`` (JSON-native).
    """
    records = {}
    for name, facts in result.record_facts.items():
        record = result.group.record(name)
        table = record.table
        samples = {}
        for column in facts.columns:
            samples[column['name']] = table[column['name']].tolist()
        records[name] = {
            'columns': [{'name': str(column['name']),
                         'label': str(column['label']),
                         'unit': str(column['unit']),
                         'dependent': bool(column['dependent'])}
                        for column in facts.columns],
            'row_count': facts.row_count,
            'samples': samples,
        }
    return records


def main(argv=None) -> int:
    """Parse the documented command; emit the JSON payload on stdout."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('path')
    parser.add_argument('--profile', choices=('thermal', 'signal'),
                        required=True)
    parser.add_argument('--group-id')
    arguments = parser.parse_args(argv)
    install_warning_policy()
    from softlab.shui.run_storage import (
        PROFILE_HYDRATION_KEYS,
        inspect_run_group,
    )
    try:
        # stdout carries only the JSON payload; backend operational
        # prints (the HDF5 connect message) go to stderr.
        with redirect_stdout(sys.stderr):
            result = inspect_run_group(
                arguments.path, arguments.group_id,
                legacy_keys=PROFILE_HYDRATION_KEYS[arguments.profile])
    except Exception as exc:
        report = getattr(exc, 'report', None)
        print(json.dumps({
            'status': 'error',
            'error_type': type(exc).__name__,
            'error_message': str(exc),
            'report': (_report_payload(report) if report is not None
                       else None),
        }, sort_keys=True, allow_nan=False))
        raise  # traceback on stderr; exit code 1
    print(json.dumps({
        'status': 'ok',
        'group_id': str(result.group.id),
        'envelope_present': result.envelope is not None,
        'envelope': result.envelope,
        'legacy_meta': result.legacy_meta,
        'records': _records_payload(result),
        'original_sha256': result.original_sha256,
        'report': _report_payload(result.report),
    }, sort_keys=True, allow_nan=False))
    return 0


if __name__ == '__main__':
    sys.exit(main())
