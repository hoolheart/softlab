# M01-W01-001 architecture review of detailed design

Reviewer: sw-jerry (Architect). Date: 2026-10-18.
Reviewed design `e0df093`, activated PRD, approved tester plan and actual
Scheduler/Action, SimpleProcess/run_process, DataGroup and HDF5 callers.
Verdict: **CHANGES_REQUESTED**. No tests/code executed or reviewed here.

## Explicit architecture decisions

The tiny example-local serial `_CountScheduler(Scheduler)` is APPROVED in
principle as a bounded use of an existing public extension interface. Its public
Action.body dispatch and unsupported threaded/chained-action rejection preserve
count execution while avoiding singleton mutation. No private scheduler cloning,
global replacement or production scheduler fix is authorized. Tests must verify
actual count dispatch, false/error results, owned loop cleanup and global isolation.
This is not a reusable scheduler framework or a public scheduling guarantee.

Disk-derived hydration of the three declared string metadata keys is APPROVED
in principle: DataGroup.__init__ writes back into its `meta` argument instead of
self._meta (base.py:433–438), so set_meta of values captured from the freshly
opened file is necessary bounded compatibility glue. Never hydrate from original
in-memory metadata; independent corrupted/missing disk fields must fail. Guide
must distinguish this helper from plain backend load_group behavior.

## Required finding (reviewer closure only)

| ID | Severity | Finding | Required correction | State |
| --- | --- | --- | --- | --- |
| DR-01 | Blocking | HDF5 load_data_group_from_hdf5_group aliases group.attrs then pops name/id/timestamp/backend (hdf5.py:137–142). The proposed runner's own verification load therefore removes identity from the delivered thermal.h5 before returning success. Limiting later tests to one read or using copies afterward cannot repair that already altered artifact; independent reopened output cannot satisfy AC06/11. | Designer must specify an artifact-preserving verification/reload strategy using example-local glue without changing library source or storage format. For example, perform destructive legacy load on a private temporary copy while capturing metadata from that same copy; preserve completed thermal.h5 and disclose legacy reader limitations. Alternative strategies need equivalent failure/cleanup and identity evidence. Update tests/design to independently reopen completed outputs and assert reserved disk attributes remain intact after run. Do not weaken AC06/11 or claim plain legacy reload is nondestructive. | OPEN |

No PRD expansion is required if the example preserves output and performs
verified disk-derived loading through existing backend on an owned temporary
copy. If Designer instead proposes relaxing output identity/reload requirements,
that is a PRD contradiction requiring Product Owner review before advancement.
No implementation or executable red-test phase until this finding is reinspected
and closed by Architect; independent code-review issues remain Designer-owned.

## Baseline debt disposition

Track these as baseline defects, unfixed and excluded from this release:
R4-BASE-SCHED-01: concrete scheduler completion callback uses module singleton
(impl_scheduler.py:173–174), preventing normal independently owned completion.
R4-BASE-META-01: DataGroup constructor loses metadata as described above.
R4-BASE-HDF5-01: legacy loader destructively removes reserved attributes.
Future-release backlog candidates require separate authorization/characterization;
none is waived, fixed, or granted acceptance here. Current example workarounds
must have explicit tests and limitations. Existing OBS/DEFECT debt is untouched.

Scope otherwise remains example/tests/guide and process records only. Exact
columns, numerical recurrence, previous-boundary timing, final-state inspection,
owned resource handling and staged failure reporting align with PRD by document
inspection. Runtime feasibility/cleanup correctness remain tester/code-review
gates, not claims of this architecture decision.
