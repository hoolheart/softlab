# M02-W03 test plan developer review

Owner: Developer | 2026-10-30 | Verdict: APPROVED

Reviewed [test plan](../test/test-plan.md), [PRD](../prd.md), W02 implementation
and the proposed [lifecycle design](../design/lifecycle-design.md). The listed
faults and deterministic fresh-run comparison are implementable with synthetic
Counter fixtures, injected callbacks and narrowly scoped patches. No production
infrastructure or device access is needed. Existing W01/W02 tests should remain
unchanged. Review is inspection only, not test execution.

- **TP-W03-01:** Specify coverage of independent row and boundary reads when
  either fails during an ordinary inspection and after an acquisition failure.
  Assert first error identity, secondary identities and an observed `None`
  distinguished from no successful boundary. An earlier observed boundary may
  be stale; its successful observation must not imply it is current.
- **TP-W03-02:** Explicitly verify the claim is checked before the row reader on
  bounded-run reuse, preflight reader failure does not consume a fresh claim,
  reset failure consumes the claim, and legacy-used Counter freshness cannot be
  detected. Do not require a guarantee for caller-owned legacy use the runner
  cannot observe. Test prior terminal evidence after failed reset and no retry.
- **TP-W03-03:** Include independent inspection and cleanup interruption
  precedence (ordinary primary plus later interruption and multiple
  interruptions) and stderr fallback. Constructor failure tests must avoid
  assuming access to an incompletely constructed scheduler; failure after owned
  resource allocation requires a deliberate injected constructor cleanup seam.

Tester should address these points and request re-review. Architect design
review remains pending, including exact state representation and cleanup
injection; no executable or release gate has passed by virtue of this review.

## Developer re-review and issue closure

The revised test plan explicitly covers independent inspection and `None`
boundary semantics (TP-W03-01), bounded claim ordering and its detection limits
(TP-W03-02), and interruption precedence and stderr fallback (TP-W03-03). Its
constructor fault case requires a deliberate safe post-allocation injection
seam and verifies disposal without assuming access to an incompletely created
scheduler. The Developer closes all three findings after inspecting the revised
plan. Architect design review has approved the design. This review establishes
implementability only; no test execution or release gate is claimed.
