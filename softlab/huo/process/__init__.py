"""
Dynamic process interface and general implements

- ``Process``: interface of any working process
- ``SimpleProcess``: implement of simple processes, all actions are wrapped
  into single body
- ``SimpleProcessWrapper``: wrap any callable object into a ``SimpleProcess``
- ``CompositeProcess``: abstract implementations of ``Process`` to hold
  multiple subprocesses
- ``SeriesProcess``: implementations of ``CompositeProcess``, run subprocesses
  as a serial
- ``ParallelProcess``: implementations of ``CompositeProcess``, run subprocesses
  parallelly
- ``SwitchProcess``: implementations of ``CompositeProcess``, use a switcher
  to select one of subprocesses to run
- ``SweepProcess``: use a sweeper to control subprocess
"""

from softlab.huo.process.process import (
    Process,
    run_process,
)

from softlab.huo.process.simple import (
    SimpleProcess,
    SimpleProcessWrapper,
)

from softlab.huo.process.composite import (
    CompositeProcess,
    SeriesProcess,
    ParallelProcess,
    SwitchProcess,
    SweepProcess,
)

from softlab.huo.process.common import (
    AtomJob,
    AtomJobSweeper,
    Counter,
    Scanner,
    GridScanner,
    count,
    scan,
    grid_scan,
)
