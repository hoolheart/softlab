# M02-W03 targeted test results

Tester | 2026-10-30 | Provisional: implementation was in progress.

Before implementation, `.venv/bin/python -m unittest discover -s tests -p 'test_m02_w03_lifecycle.py'` ran 8 tests with 4 failures and 4 errors, reflecting absent W03 functionality. The local environment emitted Matplotlib and Fontconfig cache warnings.

After implementation began and five further tests were added, `MPLCONFIGDIR=/private/tmp/softlab-mpl XDG_CACHE_HOME=/private/tmp/softlab-cache .venv/bin/python -m unittest discover -s tests -p 'test_m02_w03_lifecycle.py'` passed 13 tests in 0.016 seconds. No warnings were printed in that run. Coverage includes fresh-run IDs and traces, preflight and reset claims, inspection identity and observed boundary state, setup and join faults, cleanup fault ordering, and interruption precedence. This working-tree check is not final verification of frozen candidate content; full discovery, compilation, import and CI remain required.

Following code review, two independent regressions were added for constructor initialization and close failures together, and rejection of asynchronous and coroutine-returning resets. The developer's fixes were already present in the shared working tree, so no red run was observed for these cases. The same targeted command passed 15 tests in 0.019 seconds without printed warnings. Constructor exception identity, failed-close observation, coroutine closure and unchanged terminal evidence were checked. This remains a provisional working-tree result.

After constructor evidence transport was revised, the direct-constructor test was updated to assert exception chaining. A separate bounded-run test covers an immutable primary, ordinary primary followed by cleanup interruption, and primary interruption followed by cleanup interruption. It verifies both exception identities, first-interruption precedence, no acquisition, and false drain/closure flags when closure fails. The targeted command passed 16 tests in 0.020 seconds with no printed warnings. This is provisional working-tree evidence.
