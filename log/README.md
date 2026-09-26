# Workflow records

Use `release_0/` for the authorized workflow bootstrap. A release directory is
an evidence grouping, not an assertion that a product release was accepted.
Copy the relevant files from `templates/` for future approved releases/tasks.

- `prd.md`: requirements and verifiable acceptance criteria (product owner).
- `tasks.md`: ordered backlog and dependencies (architect).
- `design/<task>.md`: detailed design (designer), architect review, and developer
  implementability review of the test plan.
- `review/<task>.md`: independent review and issue closure (designer).
- `tests/<task>.md`: test plan, actual results and failure closure (tester).
- `principle_compliance_report.md`: phase-by-phase principle inspection.
- `sprint-board.md`: one active task, phase, blockers and integration evidence.
- `acceptance.md`: explicit product-owner verdict and evidence links.

Commit completed record updates immediately with `docs(log):` and push before
handoff on the active task branch. Do not invent approvals or results; pending,
blocked and not-applicable statuses must carry reasons. Reference the revision
being inspected rather than requiring a document to contain its own commit hash.

Tasks branch from and integrate into `dev` after the agreed gates. Promote an
accepted release to `main`. The user-authorized bootstrap uses configuration and
document checks instead of production TDD; later API changes require tests first.
See [principles](../principles.md), [architecture](../arch.md), and
[repository guidance](../AGENTS.md). Templates do not authorize backlog work.
