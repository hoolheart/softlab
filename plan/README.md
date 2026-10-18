# Proposed development plan

Status: **PROPOSED — planning only; no implementation authorization or release acceptance.**
Prepared 2026-10-18. Start date: **TBD**. Planning horizon: twelve development
months, with a committed scope decision required before each implementation task.

softlab's purpose is to make experiments programmable, composable and repeatable.
The intended outcome is one procedure developed against a simulated setup,
adapted to physical equipment through explicit execution policies, with a
trustworthy record of controls, observations and outcomes. Simulation is a
preparation tool; agreement with physical reality requires separate validation.

## Read this plan

- [Roadmap](roadmap.md): twelve month-length milestones and exit gates.
- [Milestone M01](milestone-01.md): four week-length releases for the first month.
- [M01-W01 PRD](release-01-prd.md): detailed requirements for the first release.
- [Planning reviews](reviews.md): independent feasibility and product review
  records, when prepared. These are planning evidence, not execution evidence.
- [Planning validation](validation.md): document, link and diff verification
  results and the limits of the checks performed.

`M01`–`M12` are relative development months, approximately four working weeks
each. `M01-W01`–`M01-W04` are weekly acceptance increments, not promised calendar
deadlines or mandatory package publications. They do not rename historical
`log/release_1`–`release_3`, and do not imply a Python package version or tag.
At execution kickoff assign an unused process-log namespace and package version
only if a publication is explicitly planned. The twelve-month horizon assumes
one focused delivery stream with reviewer/tester availability; if capacity drops,
move dates or reduce scope instead of bypassing gates.

## Verified planning baseline

The source inspection baseline is `dev` commit `82883ad`; historical Release 3
completion is recorded in [completion_report.md](../log/release_3/completion_report.md).
Its accepted/promotion and 210-test statements are historical evidence, not checks
rerun for this plan. [arch.md](../arch.md) documents the implemented primitives.

Existing capabilities include explicit coordinated simulation in `tu`, hook-based
count/scan execution in `huo`, data records/groups and HDF5/SQLite in `shui`, and
record plotting in `jin`. There is no general experiment-run service. `AtomJob`
uses wall-clock timestamps; its delays do not advance simulation. Coordinator
edges deliver previous-boundary values; a mock measurement is not a direct object
read. Station descriptions currently omit simulation nodes. HDF5 metadata values
are written as strings, so arbitrary nested metadata is not losslessly persisted.
These constraints shape the first release and the later provenance work.

## Workflow adaptation

Use the requested [sw-prod-workflow skill](/Users/edward/.cc-switch/skills/sw-prod-workflow/SKILL.md)
flexibly through role separation, serial task execution and independent review.
The skill link is machine-specific; the invoked source is
`/Users/edward/.cc-switch/skills/sw-prod-workflow/SKILL.md`.
[AGENTS.md](../AGENTS.md) and [principles.md](../principles.md) take precedence:
`codex/<task>` → `dev` → `main`, never direct task merges to `main`. Do not replace
project principles with the skill's generic defaults.

This engagement produces planning documents only. Product Owner drafts scope;
Architect reviews feasibility/dependencies; an independent reviewer checks
acceptance and principles. Document/link/diff verification applies; production
TDD, runtime testing, UI and release promotion gates are **not applicable to this
planning change**, and cannot be claimed passed. Planning approval does not make
future code implemented or future tests green.

For each future release, reaffirm requirements → test plan and developer review
→ detailed design and architect review → implementation → independent review
→ testing → principle inspection → candidate CI → `dev` integration. Perform
release acceptance before `main` promotion. One task is active at a time; role
handoffs within that task are serial. Record completed process artifacts under
`log/` with `docs(log):` commits/pushes. Reviewers close review issues; testers
close failures. UI/Figma gates apply only if an actual UI change is introduced.

## Guardrails and replanning

No real instrument access is authorized by this plan. Routine evidence uses
synthetic data, temporary output directories, and VISA simulation where needed.
No new service framework, universal solver, broad driver catalog or GUI is a
prerequisite. Preserve public behavior and import paths; characterize callers
before changing them. Do not make arbitrary Parameter values JSON-only.

At every weekly and monthly acceptance, record actual evidence, unresolved
baseline debt, effort and user feedback. An unmet exit gate rolls the milestone
forward or triggers an explicitly reviewed scope change; calendar passage is not
acceptance. Reorder later milestones only after documenting dependencies and
updating this plan. Review the whole roadmap after M03, M06 and M09.
