# M02-W02 test execution

Owner: Tester | 2026-10-30 Asia/Shanghai  
Status: **PASS on current working-tree candidate; candidate CI and promotion pending**

Independent final local execution on macOS arm64 and Python 3.13.15 passed all
328 discovered tests with `-W all`, including the 22-test W02 suite. The suite
covers the actual signal-monitor client and paired read counts; both boundary
policy fixtures; ordered monitor reads; completion, stop and late-stop; advance
and sample failure; action rejection and timeout; preflight; primary and cleanup
failure; cleanup-origin `KeyboardInterrupt` and `SystemExit`; preservation of a
primary interruption when cleanup also interrupts; exception-attachment
fallback; and borrowed-loop/global identity checks. See
[`test_m02_w02_bounded.py`](../../../tests/test_m02_w02_bounded.py). The two
review-finding tests and cleanup-origin follow-up were first run after their
fixes; no pre-fix red run is claimed (see [red baseline](red-baseline.md)).

| Check | Actual result |
| --- | --- |
| Full unittest discovery, `-W all` | 328 tests, OK, exit 0, 22.071 seconds |
| Targeted W02 suite, `-W all` | 22 tests, OK, exit 0 |
| `compileall -q softlab` | exit 0 |
| `import softlab` | exit 0, version `0.3.0` |
| `git diff --check` | exit 0; untracked files are not covered by this command |
| Working tree at test time | changed and untracked release artifacts; not yet clean or committed |

Full discovery captured no warnings using temporary writable Matplotlib and XDG
caches. The initial invocation without those overrides emitted Matplotlib and
fontconfig cache warnings, recorded in the red baseline; the historical pandas
and Actions warnings were not observed in this local run, not waived. Existing
baseline scheduler, metadata, HDF5, OBS-004/005/006 and DEFECT-2 dispositions
remain as recorded in W01. No test failures remain in this execution. Commands:

```sh
MPLCONFIGDIR=/private/tmp/softlab-mpl XDG_CACHE_HOME=/private/tmp/softlab-cache .venv/bin/python -W all -m unittest discover -s tests -p 'test_m02_w02_bounded.py'
MPLCONFIGDIR=/private/tmp/softlab-mpl XDG_CACHE_HOME=/private/tmp/softlab-cache .venv/bin/python -W all -m unittest discover -s tests -p 'test_*.py'
.venv/bin/python -m compileall -q softlab
MPLCONFIGDIR=/private/tmp/softlab-mpl XDG_CACHE_HOME=/private/tmp/softlab-cache .venv/bin/python -c 'import softlab; print(softlab.__version__)'
git diff --check
```

This evidence applies to the current working-tree code and tests, before final
commit. Python 3.9, exact-candidate CI, promoted state, clean worktree and product
acceptance are not claimed. No physical equipment, VISA qualification or
forced-process cleanup is inferred. Re-run affected tests if behavior changes.
