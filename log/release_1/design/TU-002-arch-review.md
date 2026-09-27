# TU-002 architecture design review

Reviewer: sw-jerry (architect). Candidate: `3f16452` on
`codex/tu-002-descriptions`. Verdict: **CHANGES_REQUESTED**. Scope is detailed
design compliance with the Release 1 PRD, TU-002 tests, existing `tu` contracts,
and the five-module architecture; this is not a code review or test execution.

The proposed opt-in `describe(metadata=None)` API fits the compatibility goal.
It keeps snapshots and value access intact, uses existing hierarchy lookup keys,
rejects unsupported metadata without coercion, avoids I/O, and introduces no
dependency or unnecessary service layer. Active-ancestry detection is suitable
for indirect device cycles while permitting acyclic shared references. The
design also identifies focused tests for metadata cycles, device cycles, and
shared devices before implementation.

## Required correction

The design says recursive descriptions inspect stored fields directly and do
not dispatch through subclass behavior, yet also says a subclass may override
`describe()` to add behavior. In a nested `Device` or `Station` description,
that override would be skipped, making the stated extension contract false.
Revise the design to choose and document one coherent rule. The simplest
compatible choice is to define version 1 as the inherited minimal description
for nested objects and remove the promise that arbitrary subclass overrides
participate in recursion. If subclass customization is retained, specify how
recursion dispatches it while preserving cycle detection, JSON validation, and
no-I/O behavior. Keep the first implementation within TU-002 scope.

The design must also state that its built-in no-I/O guarantee applies to the
base traversal; arbitrary user overrides cannot be certified by the library.
No other architectural blocker was found. OBS-006 remains open for TU-004's
hierarchy policy; detecting a cycle during description does not change the
existing mutation contract. After correction, resubmit this design for review
before implementation.
