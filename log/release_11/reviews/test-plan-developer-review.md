# M02-W04 test plan developer review

Owner: Developer | 2026-10-31 | Verdict: APPROVED

Reviewed [test plan](../test/test-plan.md) against the proposed
[migration design](../design/thermal-migration.md), W04 PRD, existing thermal
execution/manifest tests and W03 bounded runner. Numerical oracles, independent
synthetic fault injection, actual shipped entry-point tracing, repeat and
persistence/CLI verification are implementable without real instruments. This is
inspection rather than test execution.

- **TP-W04-01 — Setup failure compatibility.** Existing `execute` allocates its
  scheduler before model setup; the shared seam allocates it after setup. Obtain
  the product/architect decision on cleanup flags and reset eligibility for
  setup failures, and specify assertions for the approved behavior. Do not
  manufacture cleanup evidence or test only successful runs.
- **TP-W04-02 — Result and interruption mapping.** Include callback-time
  snapshots of `RunResult`, stop identity, runner preflight errors with no
  `CountResult`, and attached/unavailable interruption evidence, including
  independent writer cleanup. Preserve original failure identity and separate
  secondary diagnostics. The test oracle must follow the approved decision.
- **TP-W04-03 — Private historical fixtures.** Explicitly inventory affected
  `_CountScheduler`, `_make_scheduler` and `_acquire` tests and map each to
  test-owned historical characterization or an equivalent shared-seam fault
  fixture. Do not delete their behavioral assertions or require production
  to retain an unused scheduler.
- **TP-W04-04 — Fresh environment feasibility.** State the concrete clean-env
  creation/install approach and evidence of no system site packages; treat an
  actual package-index/network failure as a blocker, not a simulated pass.

Tester should revise and request developer re-review after the outstanding
architect/product decisions. No design approval, test execution or release gate
is asserted here.

## Developer re-review and issue closure

The revised plan cites the Product Owner's approved setup-failure decision and
requires after-allocation eligibility checks (TP-W04-01); specifies exact
callback-time state, preflight and interruption mapping (TP-W04-02); inventories
private fixtures and preserves their behavioral assertions (TP-W04-03); and
defines a clean virtual environment with evidence and blocker reporting
(TP-W04-04). Developer closes all four findings after inspecting the revisions.
The architect has approved the detailed design. This review is implementability
evidence, not execution or release acceptance.
