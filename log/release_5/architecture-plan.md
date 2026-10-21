# M01-W02 architecture envelope — [PLANNED]

Owner: sw-jerry (Architect). 2026-10-21. PRD subject `0691bfc`.
This is a feasibility/envelope assessment, not detailed design or implemented
capability. Current arch.md remains actual W01 architecture. Verdict:
**FEASIBLE within the example boundary; characterization/design gates pending**.

## Source and caller findings

Read examples/thermal_experiment.py (_CountScheduler, _acquire, run, main),
softlab/huo/process/{process,simple,composite,common}.py, scheduler/{scheduler,
impl_scheduler}.py, simulation/{coordination,object}.py, current arch.md and W01
review/test/acceptance/debt records. Existing callers are the command and
W01 tests in tests/test_thermal_example.py; Tester must assert their contracts.
No runtime characterization was executed by Architect.

- Process has reset/commit/join/has_more, without a public cancellation operation.
  run_process unconditionally resets first, then commits/joins while has_more;
  join failure returns false. It does not expose a cooperative stop boundary.
- SimpleProcess.reset during execution sets _aborting; it does not interrupt its
  body. SweepProcess.reset resets child/sweeper bookkeeping. Neither establishes
  termination, and neither should be advertised as safe concurrent reset.
- AtomJob calls before-get hook, all getters, then add_rows. A step can therefore
  commit before a getter failure leaves no row. Preserve that observable split.
- Concrete scheduler.stop cancels owned runners/points and clears registries;
  cancellation is software/task bookkeeping. Its existing singleton completion
  callback debt remains; the W01 private serial scheduler dispatches one action
  through its separate loop. Do not infer cancel acknowledgment or drainage from
  either stop method. Test concrete/private status behavior as distinct contracts.
- Coordinator.step stages then publishes its committed boundary; reset stages
  candidate initial state and republishes zero on success. Failure preserves
  the previous publication; external callback side effects are outside atomicity.
  Reset only after terminal/drained work; no reset/step in teardown.
- Current _acquire wraps exceptions but does not expose the partial DataGroup;
  run retains acquisition data only on successful return and reports stages by
  exceptions. W02 therefore needs local retained-result/failure reporting while
  preserving successful run(config, output_dir) -> DataGroup behavior.

## Planned responsibility allocation

Keep tu responsible for simulation state and atomic boundaries, huo for existing
process/actions, shui for existing storage, jin for existing plot. Application
coordination remains in the example. Introduce no library-wide run object or
public cancellation API. The Designer must specify a local cooperative execution
boundary and retained terminal inspection path, with explicit request versus
acknowledgment, row versus committed tick, and independent acquisition,
persistence, plot and cleanup outcomes. Existing primitives allow bounded local
composition; no evidence presently requires library source changes. Exact local
interfaces and orchestration remain detailed-design decisions after test review.

Stop after K complete rows must prevent the next advance/sample cycle; already
running synchronous work may finish before acknowledgment. End-of-acquisition
late requests cannot change its completed outcome. Failed step and failed
sampling retain distinct final boundaries. Partial/empty records stay in memory;
no partial save/plot. Successful persistence remains fresh HDF5 plus verified
copy-only reopening and headless PNG; failures cannot claim atomic/no-byte save.
Separate deliberate model reset from resource release and preserve pre-reset
terminal evidence. Fresh repeats create fresh topology, process, record, stop
state and owned resources. Borrowed/default loops/schedulers/figures remain alone.

## Debt and scope disposition

Retain unfixed R4-BASE-SCHED-01 (private serial adapter), R4-BASE-META-01
(explicit set_meta/disk-text hydration), R4-BASE-HDF5-01 (copy-only read) and
OBS-004/005/006/DEFECT-2. Their existing tested local measures must regress;
this release does not close or waive library debt. A newly demonstrated
prerequisite blocks affected implementation pending Architect/PO scope review.
No current scope blocker is established by source inspection. Runtime test
prerequisites, exact loop/status behavior and cleanup obstructions remain Tester
work, not certified here. W03 manifest/versioning, W04 adaptation, retry/resume,
real instruments, dependencies, Python support, package tags and UI are excluded.
