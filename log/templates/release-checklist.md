# Release checklist — <release>

Owner: <coordinator> | Candidate: <revision> | Status: OPEN / CLOSED

## Agree the gates before implementation

- [ ] Define scope, acceptance criteria, applicable checks and the finish line.
- [ ] Identify only the roles needed for expertise or independent review.
- [ ] Behavioral changes: independent review, meaningful tests and owned issue
  closure. Configuration changes: relevant review/validation. Factual document
  corrections: direct content/link/diff checks; no full team confirmation chain.
- [ ] Record baseline debt, warnings, environment limits and their dispositions.

## Freeze and verify

- [ ] Required reviews, tests, architecture/guide updates and principle checks
  complete. Reference existing evidence; state why it applies to unchanged content.
- [ ] Product acceptance PASS; unresolved scoped failures are not waived.
- [ ] Code and required documents committed/pushed as one final candidate.
- [ ] Exact candidate CI passed before integration to `dev`.
- [ ] Promote the accepted content to `main`; verify its revision/tree and required
  promoted-state checks. Do not add intermediate reconciliation CI gates.
- [ ] Diff hygiene and worktree status checked; preserve unrelated user changes.

Rerun checks only for affected changes, failures or unresolved risks. If a frozen
candidate changes, identify which evidence remains applicable and which gates
need repeating. Do not treat an unrun check as passed or discard failed history.

## Close once

Supply final merge revisions and CI URLs in the final handoff. Do not commit just
to record the previous commit's CI result. Record final facts together in one
authoritative summary when available; its factual maintenance does not reopen
the release, require every role to confirm, or restart the gate chain.

| Final fact | Evidence |
| --- | --- |
| Acceptance and applicable independent gates | <links / verdicts> |
| Integrated candidate and CI | <revision / URL / actual result> |
| Promoted revision and verification | <revision / URL / actual result> |
| Remaining debt and limits | <tracked items / exclusions> |
| Worktree and scope | <actual result / preserved user changes> |

When all applicable finish conditions hold, set **CLOSED**, state what was
delivered and stop. Administrative corrections do not create another release
task. Investigate product/environment/reporting failures only as far as needed
for a sound decision; report meaningful changes instead of repeated waiting.
