# M01-W01-001 developer test-plan review

Owner: sw-tom (Developer), 2026-10-18. Verdict: **APPROVED**.
Reviewed committed test plan and baseline at `f149039` on
`codex/m01-w01-thermal-example`. This is implementability review only;
no production code, executable tests or detailed design was created or run.

The plan maps AC01–12 and NFR01–05 to bounded assertions and independent
expected values. The baseline independently establishes the one-boundary
measurement delay and numeric oracle. Existing AtomJob before-get hook runs
before getter evaluation and row append; run_process returns an explicit
success flag, so acquisition failure checks are implementable without changing
legacy behavior. Failure injection through example-local seams and mocks is
feasible; exact names, schema and seams remain Designer-owned.

Read relevant callers/contracts: common.py AtomJob/count, SimpleProcess,
process.py run_process, concrete scheduler start/stop, backend connection/error
state, HDF5 save/load and plot_record. The following contract details must guide
design and executable assertions; they do not require a plan scope change:

- Backend disconnect and scheduler stop return None. TC05's result checks must
  respect those void contracts, inspect actual disconnected/inactive state and
  errors, and never demand a truthy return. HDF5 disconnect_impl can suppress an
  exception into False while public disconnect changes status; verify the owned
  file handle and recorded errors rather than treating status alone as proof.
- Scheduler construction may create/set an event loop or borrow a running one.
  TC11 correctly requires explicit ownership and forbids closing borrowed loops.
- HDF5 load sorts column keys and consumes reserved attributes on the file.
  The recorded fresh-reopen ownership and Designer-selected compatible schema
  are necessary; do not assert repeated load from the same persisted group or
  arbitrary insertion order as an existing contract.
- plot_record may create a figure before failing. Failure cleanup must cover
  newly created figures while preserving preexisting figures.
- Injected cleanup failures must be reported and remaining cleanup attempted;
  when a mock deliberately prevents release, assert truthful failure/resource
  evidence instead of claiming the obstructed resource was released.

No blocking implementability issue found. Baseline results are Tester evidence,
not checks rerun or independently closed by this review. Detailed design and
Architect approval, executable red tests, implementation, independent review,
testing, principle inspection and candidate CI remain pending. No UI/hardware
or publication gate applies. Next owner: sw-celeste for detailed design.
