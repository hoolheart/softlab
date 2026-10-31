# M02-W04 thermal migration architecture review

Reviewer: Architect | Date: 2026-10-31 Asia/Shanghai
Verdict: **CHANGES REQUESTED**. Reviewed [migration design](../design/thermal-migration.md)
against [PRD](../prd.md), the M02 plan, W03 bounded lifecycle and the current
example's `execute`, `_acquire`, `RunResult`, stop and reset paths.

1. **AR-W04-01 — pre-run setup ownership and compatibility unresolved.** The
   prior example constructs its scheduler before model setup; after a later
   setup failure it attempts cleanup and can report drained/closed. The design
   moves scheduler creation inside `run_count`, after model setup, and leaves
   failure flags and reset eligibility to future investigation. Define exact
   truthful outcomes for failures before shared-runner ownership and map them
   to existing API and reset behavior. If the accepted compatibility contract
   cannot be retained without fabricating resource evidence or duplicating
   runner ownership, identify the specific behavioral change and obtain Product
   Owner scope decision before implementation. Test model-build, prepare and
   control-write failures separately; do not invent cleanup of an uncreated loop.
2. **AR-W04-02 — preflight, callback and interruption mappings incomplete.**
   `run_count` preflight can raise without a `CountResult`, while after-row
   callbacks consume a live nonterminal result and interruption attachment may
   fail. Specify how each maps to thermal acquisition status, stage, cause,
   partial rows, stop and cleanup evidence without inventing information, and
   how original interruption survives example cleanup. Define what callback sees
   before its invocation and test failure/fallback paths.

The separation of model policy from shared execution and preservation of group
identity, manifest versions and timing is sound in principle. Historical private
fixtures require explicit test ownership and applicability, as proposed. These
findings require design revision and reinspection; no runtime, acceptance or
release result is claimed.

## Architect re-review

**APPROVED.** The revised design incorporates the explicit
[Product Owner decision](setup-failure-product-decision.md) for failures before
runner creation, including truthful flags and reset ineligibility. It specifies
preflight exceptions, callback snapshots, interruption evidence and fallback.
Reinspection also corrected callback-time `RunResult.stage` to the existing
`setup` value: the old implementation updates a local stage during acquisition
and assigns it to the result only on termination. Independent tests must cover
these mappings and the approved setup-failure change. AR-W04-01 and AR-W04-02
are closed by the Architect. Existing private characterization evidence must be
explicitly dispositioned as described in the design.

This approval does not establish implementation, test, product acceptance or
release closure.
