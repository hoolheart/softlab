# M02-W03 independent test execution

Tester | 2026-10-30 | Verdict: **PASS for local tested scope**

The stabilized working-tree implementation and independent tests were exercised on Python 3.13.15 using the project `.venv`, synthetic inputs and temporary storage. Actual commands and results:

- `MPLCONFIGDIR=/private/tmp/softlab-mpl XDG_CACHE_HOME=/private/tmp/softlab-cache .venv/bin/python -m unittest discover -s tests -p 'test_*.py'`: **345 tests passed** in 22.222 seconds, no reported skips or warnings.
- The same environment with `-p 'test_m02_w03_lifecycle.py'`: **17 tests passed** in 0.023 seconds.
- `.venv/bin/python -m compileall -q softlab`: passed.
- `MPLCONFIGDIR=/private/tmp/softlab-mpl XDG_CACHE_HOME=/private/tmp/softlab-cache .venv/bin/python -c 'import softlab; print(softlab.__version__)'`: passed, version `0.3.0`.

The earlier red run and incremental results are recorded in [targeted results](targeted-results.md). The initial run without writable cache overrides emitted Matplotlib and Fontconfig cache warnings; these were not printed with the temporary cache settings above. Historical pandas and Actions warnings remain tracked; no claim is made about other environments. These checks are not Python 3.9 or CI evidence, nor physical instrument validation.

## Acceptance coverage

- **AC01–03:** The approved lifecycle design and tests cover preflight, scheduler construction/start/dispatch/join, reset, acquisition and independent inspection, identity and secondary errors. W02 tests assert partial rows and advance/read boundaries, including post-advance failure; W03 tests distinguish observed `None` and unavailable boundaries. Constructor dual faults include immutable primary and interruption precedence.
- **AC04–05:** W02 tests cover before/after/late stop, no further cycle after acknowledgment, interruption attachment/fallback, borrowed-loop and global scheduler preservation. W03 adds stop, drain and close failure identity and ordering, truthful flags, continued cleanup and competing interruptions. Blocking or physical work is not preempted by these tests.
- **AC06–08:** Tests cover reset eligibility, failed and successful caller-owned reset with retained terminal evidence, async rejection, process claim ordering and failed reset, independent fresh runs with distinct UUIDs, matching trace and both numeric columns, and fresh run after interruption. They assert no implicit retry in the tested failure paths. Freshness after legacy process use cannot be inferred by the bounded runner and remains caller responsibility.
- **AC09:** Full discovery includes W01/W02 legacy process, count, scheduler, scan and synthetic client regression tests. Legacy tracked defects remain separately scoped as documented in the PRD.
- **AC10–11:** Test evidence supports review and acceptance; product acceptance, exact-candidate CI, integration, promotion, promoted-state verification and clean worktree are separate outstanding gates. No UI or hardware validation applies.

The test owner closes no outstanding test failures: the red cases reflected planned behavior before implementation and the updated targeted and full runs passed. Constructor-failure tests use an injected setup failure and restore the test-owned loop after the simulated close fault; they do not assert that the failed production close succeeded.
