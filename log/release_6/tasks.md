# M01-W03 serial backlog

Architect: sw-jerry. 2026-10-22 Asia/Shanghai. PRD affe67b; dev410bc16.

| Order / ID | Deliverable | Branch/dependency | State |
| --- | --- | --- | --- |
| 0 / M01-W03-SETUP | Activated requirements, backend-reviewed envelope, tasks/start principles; independent document review/validation/CI | codex/m01-w03-setup from dev410bc16 | Done; final dev6d332ce; completion records/CI both versions PASS |
| 1 / M01-W03-001 | Strict stored manifest and independent typed inspection; corruption/version/source/outcome/compatibility tests and guide | codex/m01-w03-stored-context from integrated setup dev6d332ce; created/pushed | Done; finaldev20fa9a0; completion records integrated/both CI versions PASS |
| 2 / M01-W03-CLOSURE | Actual architecture, release principles, document checks and PO acceptance/reconciliation | codex/m01-w03-release-closure from verifieddev20fa9a0; created/pushed | PO accepted; runner gate resolved; final candidate CI/integration pending |

One implementation slice keeps encoding, actual stored evidence validation and
fresh-process inspection coherent across AC01–12. No deadline or overlapping task.
[Architecture envelope](architecture-plan.md) selects JSON text in group metadata
following backend source review; it does not approve detailed interfaces/code.

Setup: Architect feasibility/decomposition → Designer independent document review
and owned issue closure → Tester document/path/scope validation → coordinator
formal/principle gate → exact candidate Python3.9/3.13 CI → Developer dev merge.
Documentation-only setup needs no invented production test/design execution.

Implementation: Tester characterization and test plan → Developer implementability
review → Designer detailed design → Architect approval → Tester executable red
contracts → Developer implementation/self-check → Designer independent review/
closure → Tester complete checks/failure closure → principles → exact candidate
CI → dev integration. At most three review/fix loops before escalation. Characterize
backend text/loader loss/copy preservation and W01/W02 callers before behavioral
extensions. Unexpected library prerequisites require Architect/PO scope review.

Require fresh-process typed inspection, strict unsupported/malformed/missing/version
checks, actual data/identity/count/schema/clock consistency, clean/dirty/unavailable
producer identity, changed configuration and truthful pre-persistence snapshot,
fault injection and unchanged W01/W02 regression. Tester records actual versions,
257-test historical baseline separately from newly executed results, warnings/
skips/limits, unittest discovery, compile/import and executed guide journeys.
No historical result substitutes for current exact candidate CI.

Closure: actual architecture and all-eleven release principles → independent
Designer document review → Tester final document/evidence validation → coordinator
formal gate → PO AC01–12 acceptance → exact closure candidate CI/dev reconciliation
→ accepted main promotion. No package tag/publication or whole-M01 acceptance.
Update board per phase; every completed log update commits/pushes immediately as
docs(log) before handoff. All reviews/results identify committed subjects and
retain owner-only closure. UI/Figma/hardware N/A. Do not mark Done before dev merge
or accepted before PO verdict. Future artifacts live in release_6/test/, design/
and reviews/ with task prefixes; they are requirements, not yet-existing evidence.
