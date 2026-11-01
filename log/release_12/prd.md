# PRD — M03 reproducible run record

Owner: Product Owner | 2026-10-31 Asia/Shanghai
Status: **REVIEWED — independent reviews approved; awaiting user confirmation**
Review completed: 2026-11-01 Asia/Shanghai.
Release namespace: `release_12` (one release, not a package version).

## Authorization and hold

The user authorizes starting one release for the whole M03 and explicitly asks
to pause at PRD confirmation. Prepare and independently review these requirements
and the phase plan, then present them for the user's confirmation. **Do not
create test plans or executable tests, design, implement, run runtime experiments,
or start phase 1 before that confirmation.** PRD approval by a reviewer is not
user confirmation or release acceptance. Source/document inspection and
planning-document validation are permitted during this hold.

## Product outcome and prerequisite

A maintainer can reopen a supported synthetic result in a clean session,
understand its bounded procedure, setup/model declarations, units, observed
software/environment identity, acquisition outcome stage and sample relations,
and compare a deterministic rerun with independent expected values. This is
explainability within the recorded scope, not an environment lock, physical
validation or guarantee of arbitrary experiment reproduction. M02 closed at
`e827e7f:log/release_11/closure.md`; its shared bounded execution and accepted
thermal v1/v2 behavior are the baseline. The detailed [M03 plan](../../plan/milestone-03.md)
records evidence, constraints, debt and proposed decisions.

Existing thermal manifests record a completed-acquisition snapshot before
persistence, not later persistence/plot/cleanup success; their group UUID differs
from the in-memory bounded execution UUID. The signal-monitor client currently
returns in-memory data only. HDF5 metadata is text and the legacy loader may
mutate reserved attributes; SQLite must not be assumed to persist arbitrary
metadata. `DataGroup(meta=...)` and `set_meta(None)` have tracked behavior or
implementation/documentation issues. None of these observations is a license to
silently repair or broaden the current contract.

## One release, four serial gated phases

| Phase | Bounded outcome and gate before proceeding |
| --- | --- |
| 1 — Characterize and decide | After user confirmation, create reviewed characterization plan and design, then assert legacy thermal v1/v2 and absent/invalid behavior, backend callers and metadata/read safety on synthetic temporary data. Inspect HDF5 metadata independently using disposable copies. Inventory both actual clients, including how the in-memory signal client could construct a stored record and clean-session rerun inputs, backend, field origins and independent sample oracle. Product Owner and Architect independently agree a minimal supported boundary or document a blocker. **No schema implementation begins before this evidence decision is approved.** |
| 2 — Schema and encoding | With the phase 1 decision, define reviewed bounded field/type, version, missing/null/unknown and failure contract, and implement only the justified codec/construction seam. Independent tests verify valid round-trips and malformed, duplicate, unsupported, nonfinite and version failures. Preserve legacy behavior and import paths unless an explicit reviewed change authorizes otherwise. |
| 3 — Storage and inspection | Integrate only selected, evidenced storage paths; verify group/sample/schema/units/identity relations in temporary files and separate-process inspection. Define supported snapshot stages and unavailable reasons, and whether later outcomes are absent or updated. Test serialization, save, close/disconnect and invalid reload failures, original-byte preservation and separately observable primary/cleanup diagnostics. No atomicity or failed-run persistence is implied. |
| 4 — Reference comparison and acceptance | Integrate the supported boundary with both thermal modes and the justified second client, update guidance and independently execute fresh-environment and clean-process walkthroughs. Compare predeclared numeric/timing oracles with explicit tolerances and exclusions; verify distinct identities, compatibility and remaining debt. Complete one M03 acceptance and release closure, not four separate release promotions. |

If phase 1 does not justify a common schema, Product Owner and Architect must
revise phases 2–4 and the milestone exit, explicitly identify any separate
supported records and comparisons, or stop/replan. Rejection is not automatic
acceptance or permission to build an unsupported shared API. Each phase remains
serial and uses proportionate, applicable design/test/review gates; phase evidence
can be reused where unchanged.

## Acceptance criteria

| ID | Observable criterion |
| --- | --- |
| M03-AC01 | Phase 1 supplies reviewed source/caller and runtime characterization, safe on-disk metadata evidence, backend capability decision, two-client field ownership and independent oracle inventory. Product/Architect decide common versus separate scope before schema implementation. |
| M03-AC02 | The accepted format states namespace, bounded supported values, required/optional/unknown policies, absent versus null, version/compatibility and errors. Tests reject duplicate keys, unsupported values and nonfinite numbers without implicit stringification; valid supported values round-trip. |
| M03-AC03 | Supported stored records preserve declared procedure/setup/model, units, observed software/environment availability, sample ordering/schema and identity relations. Execution UUID and stored group UUID are not conflated. An acquisition snapshot cannot claim later persistence, plotting or cleanup success. |
| M03-AC04 | Chosen backend and storage ownership are explicit. Synthetic round-trip and independent fresh-process inspection distinguish legacy absence from corrupt/unsupported metadata; original data is protected and serialization, save, disconnect and reload faults have tested, truthful primary and cleanup evidence. Unsupported backend and partial-result cases are explicit. |
| M03-AC05 | Existing v1 coordinated/v2 direct and legacy behavior remain compatible within reviewed scope. Both supported client scenarios have independently selected inputs, numeric/timing oracles, tolerances, excluded timestamps/UUIDs and rerun procedure; clean-session repeat matches those assertions, not just itself. |
| M03-AC06 | A fresh documented environment walkthrough and independent repeat complete for the approved scope, with actual commands, versions, warnings and limitations recorded. Runtime tests, unittest discovery, compile and import checks, independent code review and tester-owned failure closure pass; UI gates do not apply. |
| M03-AC07 | Product acceptance and principle inspection map all phases and debt to evidence. Freeze required code/documents, pass exact-candidate CI before `dev`, promote accepted content to `main`, verify promoted state and required checks, record revisions/CI URLs once, and verify clean worktree before marking release and milestone CLOSED. Unrun gates remain pending. |

## Limits and debt

No real instrument/VISA qualification, physical termination guarantee, secret
collection, dependency lock, arbitrary object/callable serialization, general
provenance framework, backend rewrite, automatic retry, rollback or durable
partial recovery is implied. M04 owns physical adapter decisions, M05 scan
extensions and M07 recovery. R4-BASE-SCHED-01/F-01, F-02, R4-BASE-META-01,
R4-BASE-HDF5-01, OBS-004/005/006 and DEFECT-2 retain tracked dispositions;
assess the `set_meta(None)` mismatch separately. Any blocking repair or migration
requires independently reviewed scope. Record actual historical and current
warnings in the environment observed; do not waive or suppress them.

Product Owner owns scope/acceptance; Architect owns feasibility and design
review; Developer reviews implementability and implements; independent reviewer
owns review closure; Tester owns test execution/failure closure. The
[principles](../../principles.md) and [release checklist](../templates/release-checklist.md)
govern the single final release. No test, design, implementation, runtime, CI,
promotion or acceptance result is claimed in this PRD.
