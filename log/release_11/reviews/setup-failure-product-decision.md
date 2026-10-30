# W04 setup-failure compatibility decision

Owner: Product Owner | Date: 2026-10-31 Asia/Shanghai
Verdict: **APPROVED — narrowly scoped W04 behavior change**, subject to the
implementation and independent verification below. This is a requirements
decision, not W04 release or milestone acceptance.

The prior thermal example creates an owned scheduler before model construction.
If model preparation or the initial heater write later fails, it attempts to
stop, drain and close that scheduler; when these succeed, its `RunResult` may
report `work_drained=True` and `loop_closed=True`. If a coordinator is retained
and other prerequisites hold, `reset_after_run` can then be eligible. A
model-build failure has no retained coordinator and cannot be reset.

The accepted bounded runner owns its scheduler and creates it only after thermal
model preparation and initial control write. If model build, prepare or initial
write fails before runner invocation, no runner loop exists to drain or close.
The migrated example must report `work_drained=False` and `loop_closed=False`,
retain the setup failure and truthful partial context, and reject
`reset_after_run` under its existing eligibility rules. These flags record actual
observed runner work; they must not assert cleanup of an uncreated resource.
This deliberately changes the old prepare/write failure result and associated
reset eligibility. It does not promise that no caller-owned setup side effect
occurred, nor imply an automatic retry or physical termination.

Architect and Tester identified that moving setup into the first count action
changes pre-stopped and zero-row setup ordering, while creating a throwaway
scheduler only to reproduce flags would duplicate lifecycle ownership. The
truthful, narrowly scoped difference is preferable and is accepted for W04.
The normal, cancelled, failed and interrupted paths **after** shared runner
allocation retain their agreed outcome, cleanup and reset semantics; existing
CLI, manifest, numerical, timing, partial-result and storage contracts remain
subject to the PRD.

Independent tests must inject model-build, prepare and initial-write failures
separately; assert cause identity, stage, retained coordinator/partial result as
applicable, false runner cleanup flags and ineligible reset, no acquisition or
fabricated row, and no cleanup of borrowed/global resources. They must also
prove after-allocation failure and success preserve owned cleanup evidence and
reset eligibility when its actual preconditions hold. Review the design and
record any additional affected compatibility evidence before implementation.
