# M01-W03-SETUP independent document review

Reviewer: sw-celeste (Designer). Date: 2026-10-22 Asia/Shanghai.
Subject: committed `2ccd22d`, including PRD `affe67b`, on
codex/m01-w03-setup from dev410bc16. Verdict: **APPROVED for setup documents**.

Reviewed [PRD](../prd.md), [envelope](../architecture-plan.md),
[tasks](../tasks.md), [board](../sprint_board.md) and
[start principles](../principle_compliance_report.md) against
[milestone](../../../plan/milestone-01.md), [AGENTS](../../../AGENTS.md),
[principles](../../../principles.md), current architecture and thermal example
source/callers. Checked HDF5 save/load and DataGroup constructor source for the
representation decision. Static document/source/diff review only, not executable
backend characterization, detailed design, implementation or runtime verification.

| Check | Finding |
| --- | --- |
| Milestone scope | W03 adds bounded stored context and fresh-process typed inspection to accepted W02. Configuration/units/source/software/clock/outcome/run identity are required; W04 adapters and whole-M01 acceptance remain outside scope. |
| Backend decision | Existing HDF5 save converts metadata values with str; explicitly encoded JSON text can preserve types without changing backend format. Loader pops identity attributes and constructor loses metadata, supporting retained copy-only read and explicit disk-text hydration. Passing arbitrary dict values directly is correctly rejected as a design approach. |
| Closed schema | Identifier/version and declared scalar/container fields constrain JSON use. Nonfinite values, bool-as-count, unsupported objects/keys, missing fields and malformed/present-invalid/version-unsupported content must fail visibly. Exact fields/interfaces remain Designer work after test-plan approval. |
| Legacy and data consistency | Absent legacy envelope is distinct from corrupt present data and unsupported version. Numeric legacy inspection remains possible with unavailable context; no current-default hydration. Manifest identity/count/order/units/clock must agree with actual loaded samples, including changed configuration. |
| Identity truthfulness | Producer revision/worktree state or unavailable reason is explicit; digest byte scope must be named. Git runs must refer to the source repository, dirty identity cannot masquerade as clean and fresh-process readers cannot replace captured producer identity. No dependency lock/full reproducibility claim. |
| Lifecycle and storage | Named snapshot follows completed acquisition and precedes save/reload/plot/cleanup. Later stages remain unknown in stored snapshot; RunResult governs actual later outcomes. W02 partial failure/cancellation stays in memory without new durable manifest; no atomic-save or zero-byte-failure claim. |
| Scope/debt | No library/backend repair, arbitrary serializer, new framework/dependency/Python-support change, SQLite parity, migration, UI or hardware. Existing R4-BASE and historical OBS/DEFECT debt remains open with required local-measure regressions. New prerequisites require Architect/PO disposition. |
| Gates and evidence | Only setup active; implementation/closure backlog. Source-backed decision is explicitly planned and executable characterization remains pending. Serial test/developer-review/design/Architect-review/red/code/review/test/principles/CI gates and owner closure are maintained. Exact candidate CI before dev and PO acceptance before main remain distinct. |

Issue IDs: **none**. No review issue or historical debt closed. Existing W02
acceptance/promotion is dependency evidence and is not reopened. Setup changes
exactly five release_6 documents; no library/example/test/configuration/actual
architecture change appears in the subject diff. Runtime checks and acceptance
are not inferred from document approval.

Board advances to Tester document validation. Tester scope/path/link/diff checks,
coordinator formal/start-principle verification, exact candidate Python3.9/3.13 CI
and setup dev integration remain pending. Production TDD/UI gates are inapplicable
to this documents-only setup; subsequent implementation retains them as relevant.
This report/board handoff requires immediate docs(log) commit/push and upstream
verification before progression; resulting hash is reported separately.

## Targeted language-preference review

Reviewer: sw-celeste. Date: 2026-10-22 Asia/Shanghai.
Subject: AGENTS.md addition committed `e46c914` on codex/m01-w03-setup.
Verdict: **APPROVED**; issue IDs: none.

The six-line addition requires English communication and newly created/updated
project deliverables in current and future softlab sessions, matching the user's
direct instruction for this international project. Existing Chinese instructions
remain intact unless translation is explicitly required. This preservation rule
does not weaken the English requirement for new communication/deliverables.
The committed diff changes no unrelated engineering/workflow rules. Earlier setup
document approval remains valid for unchanged documents. No source/tests/design
or runtime execution performed. Tester document validation is the next gate;
this targeted approval does not certify CI, integration or release acceptance.
