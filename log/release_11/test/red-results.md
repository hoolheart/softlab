# M02-W04 pre-implementation red evidence

Tester | 2026-10-31 Asia/Shanghai

Before the developer began migration, `MPLCONFIGDIR=/private/tmp/softlab-mpl XDG_CACHE_HOME=/private/tmp/softlab-cache MPLBACKEND=Agg .venv/bin/python -m unittest discover -s tests -p 'test_m02_w04_integration.py'` ran four tests in 0.296 seconds and failed with six subcase failures. Both adapter modes did not call the shared runner, build/prepare/write failures reported the old drained flag, and injected shared-runner preflight was not invoked. The callback snapshot/stop identity characterization passed. Output used temporary synthetic directories; no warnings were printed. These results are deliberately red and do not claim implementation, full regression or acceptance.
