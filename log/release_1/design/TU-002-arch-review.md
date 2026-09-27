# TU-002 architecture design review

Reviewer: sw-jerry (architect). Initial candidate: `3f16452`; corrected
candidate: `0095fce` on `codex/tu-002-descriptions`. Final verdict:
**APPROVED**. Scope is detailed
design compliance with the Release 1 PRD, TU-002 tests, existing `tu` contracts,
and the five-module architecture; this is not a code review or test execution.

The proposed opt-in `describe(metadata=None)` API fits the compatibility goal.
It keeps snapshots and value access intact, uses existing hierarchy lookup keys,
rejects unsupported metadata without coercion, avoids I/O, and introduces no
dependency or unnecessary service layer. Active-ancestry detection is suitable
for indirect device cycles while permitting acyclic shared references. The
design also identifies focused tests for metadata cycles, device cycles, and
shared devices before implementation.

## Resolved correction

The initial design promised subclass override customization but bypassed those
overrides in nested traversal. The correction defines version 1 nested output
as inherited minimal fields, with the subclass's qualified type string, and
explicitly declines to invoke arbitrary overrides. It also confines no-I/O and
JSON guarantees to the built-in traversal. These statements resolve the
contradiction without an extension framework. The design's extra focused tests
for cyclic metadata, indirect device cycles and shared-device reuse remain
required before implementation. OBS-006 remains open for TU-004's hierarchy
mutation policy; detection during description does not change that contract.

TU-002 may proceed to implementation after its other applicable gates. This
approval does not claim code review, test pass, CI or integration.
