# M01-W03-001 independent code review — iteration 1

Reviewer: sw-celeste (Designer). Date: 2026-10-22 Asia/Shanghai.
Subject: committed1ddf625, implementationd48e925, guide1ab392a and Tester fixture
correction9f956e7 on codex/m01-w03-stored-context.
Verdict: **CHANGES_REQUESTED**.

Reviewed the example/guide, W01/W02/metadata callers and new manifest assertions
against [PRD](../prd.md), [approved design](../design/M01-W03-001-design.md),
[test plan](../test/M01-W03-001-plan.md) and Architect approval. Static review only;
no production/test edits or test execution. Developer272 full/55 thermal green
self-checks are historical reported evidence, not this independent Tester gate.

## CR-W03-01 — P2 — Validate logical boundaries exactly

Status: **OPEN**. Location: examples/thermal_experiment.py,
_validate_manifest_data, expected_columns and common np.allclose comparison.
The code compares tick and measurement_source_tick with absolute tolerance1e-10,
just like physical temperature/time values. Both are logical integer identities:
the approved design requires tick exactly1..N and source boundary exactly0..N-1.
A copied HDF5 sample tick changed from3 to3+1e-11 currently passes this branch,
even though it is fractional and contradicts the declared boundary. Source tick
has the same gap. Closed integer manifest fields cannot detect this because the
actual sample validation explicitly permits that discrepancy.

Required correction: check the two logical boundary columns against the exact
integer sequence without float tolerance (and reject fractional/nonfinite values);
retain the documented tolerance for physical simulation-time/temperature values.
Do not round/coerce away corrupt boundary evidence. Keep the correction local to
the example and preserve existing integer-valued float record representation.
Tester must add committed assertions tampering each boundary column by a small
nonzero fractional amount below the old tolerance, and verify visible rejection
through copy-only inspection with source bytes preserved. Tester owns those tests
and failure closure; Developer owns implementation; only this Reviewer closes
CR-W03-01 after inspecting the committed fix.

## Other reviewed contracts

Closed objects reject required/unknown keys and nested invalid types; counts and
flags distinguish bool/int. JSON decode rejects duplicate keys/nonfinite tokens;
availability nulls and fixed semantics remain explicit. Producer capture scopes
Git to the resolved tracked source and records failures rather than clean guesses;
script digest coverage is named, reader inspection does not recapture identity.
Current source-repository identity is best effort as documented, not an atomic
checkout/environment lock.

Optional envelope capture/hydration precedes destructive copy loading. Legacy
absence stays distinct from corrupt present/unsupported metadata. Fresh delivery
metadata comparison detects removal of its newly required envelope. Independent
Euler recurrence validates changed configurations against actual rows; other
identity/count/column/clock/legacy-metadata checks are present. CLI discovery is
read-only and selected group reload uses disposable copy; machine-readable stdout
is protected from backend connection prints. Generation mode/stop/group selection
errors are visible. Source/software metadata is captured before persistence, and
stored snapshot makes no later success claim. W02 partial/primary/structured
cleanup/reset contracts remain intact. No library/dependency/format change found.

No additional actionable issues raised. Existing R4-BASE/OBS/DEFECT debt remains
open under bounded measures. Remain Code Review until repair/Reviewer closure;
then independent Tester verification, principles and exact candidate CI remain
required. Guide and scope do not certify hardware, whole-M01 or acceptance.
Report/board commit and push immediately with docs(log) before handoff.
