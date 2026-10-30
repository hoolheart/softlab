# Fresh-environment walkthrough

Date: 2026-10-30. Executor: implementation agent; independent product review
remains pending. Synthetic data only, macOS, Python 3.9.6, newly created isolated
`venv` in `/private/tmp`; existing project environment was not modified.

`python3 -m venv /private/tmp/softlab-m01-fresh-venv` created the environment.
The sandboxed `pip install -e .` failed to resolve PyPI DNS. A network-enabled
retry downloaded declared dependencies but failed editable installation using
bundled pip 21.2.4 with modern setuptools' develop path; setuptools emitted
license metadata deprecation warnings. Upgrading pip within this temporary venv
to 26.0.1 and repeating `pip install -e .` succeeded. No project dependency
configuration was changed. Principal installed versions included numpy 2.0.2,
pandas 2.3.3, scipy 1.13.1 and h5py 3.14.0.

With writable temporary Matplotlib/cache paths and `MPLBACKEND=Agg`, the guide's
`python examples/thermal_experiment.py --adapter direct --output-dir <fresh-path>`
completed successfully, reopened its HDF5 and wrote a 21,633-byte PNG. A separate
fresh-process `--inspect-file` recovered context available, manifest version 2
and eight rows. Pandas emitted repeated existing DataFrame concatenation
FutureWarnings from `softlab/shui/data/base.py:314` during run and inspection.
The result establishes this specific walkthrough, not full Python 3.9 regression,
locked dependencies, CI or physical/VISA qualification.

Commands executed from the repository root (the initial install used the first
command before network-enabled retry and pip upgrade):

```sh
python3 -m venv /private/tmp/softlab-m01-fresh-venv
/private/tmp/softlab-m01-fresh-venv/bin/python -m pip install -e .
/private/tmp/softlab-m01-fresh-venv/bin/python -m pip install --upgrade pip
/private/tmp/softlab-m01-fresh-venv/bin/python -m pip install -e .
MPLCONFIGDIR=/private/tmp/softlab-mpl XDG_CACHE_HOME=/private/tmp/softlab-cache MPLBACKEND=Agg /private/tmp/softlab-m01-fresh-venv/bin/python examples/thermal_experiment.py --adapter direct --output-dir /private/tmp/softlab-m01-fresh-walkthrough
MPLCONFIGDIR=/private/tmp/softlab-mpl XDG_CACHE_HOME=/private/tmp/softlab-cache /private/tmp/softlab-m01-fresh-venv/bin/python examples/thermal_experiment.py --inspect-file /private/tmp/softlab-m01-fresh-walkthrough/thermal.h5
```
