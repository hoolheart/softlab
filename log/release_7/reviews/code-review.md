# M01-W04 independent code review

Reviewer: software designer (independent of implementation). Date: 2026-10-30.
Verdict: **APPROVED** for the reviewed implementation and guide; test execution,
product acceptance and release gates remain separate responsibilities.

Reviewed the changed thermal example and guide against the W04 PRD, detailed
design, existing coordinator semantics and W03 manifest validation. The same
count acquisition performs one step per row; default mode retains previous-boundary
measurement and version 1. The direct adapter observes committed model output and
records zero delay, source tick and version 2 consistently. Both versions retain
closed schema and sample validation, and unsupported versions are rejected. The
existing scheduler, failure, partial-row and cleanup paths remain shared. Direct
observation's extra callback and physical limits are documented.

Findings closed by reviewer after developer corrections:

1. The manifest guide still described version 1 as the only supported version;
   corrected to distinguish versions 1 and 2 and their timing policies.
2. New example-local adapter and timing helpers lacked type annotations;
   annotations added in accordance with project conventions.

No further actionable issues identified in the reviewed changes. This review does
not assert that pending independent tests, exact-candidate CI or acceptance passed.
