# BOOT-001 developer implementability review

Owner: sw-tom. Date: 2026-09-26. Branch: `codex/workflow-preparation`.
Reviewed plan: [bootstrap validation](../tests/bootstrap_validation.md), committed
at `741e146`; task scope: [BOOT-001](../tasks.md).

## Implementability verdict: APPROVED

The tester's plan can be implemented using GitHub Actions and existing unittest
commands without changing production code or runtime dependencies. Explicitly
importing `pyvisa_sim` before discovery addresses optional simulator skips. Each
check runs separately so failure propagates. Writable runner caches address the
recorded local cache diagnostics. The matrix is a target, not passing evidence.

## Implementation notes

CI uses Ubuntu with Python 3.9/3.13, read-only repository permissions, checkout and
Python setup actions, editable installation plus the simulator, environment
logging, simulator import, regression discovery, compilation and import checks.
Push triggers cover main/dev/codex task branches; pull requests target main/dev.
AGENTS guidance preserves existing engineering rules while documenting task → dev
→ main gates. Templates are intentionally small and require evidence and ownership.

This record is a developer test-plan review and configuration implementation note,
not a designer/architect approval or independent review. Bootstrap is documentation
and CI preparation under principle 3; no production interfaces are designed here.
Independent review, final testing, principle inspection, acceptance and actual CI
remain separate gates before integration. No merge is performed by this step.

## CI configuration correction

The first pushed workflow at `6fbd1f8` completed with failure before starting
checks (run 36252511432). Job-level environment expressions cannot use the
runner context. Cache configuration now uses a shell step writing RUNNER_TEMP
paths to GITHUB_ENV before validation. Remote execution must still be verified;
the initial failed run is not a passing test result.
