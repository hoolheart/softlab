# Planning document validation

Tester: sw-mike. Date: 2026-10-18. Verdict: **PASS for documentation
validation only**. No tester failures were found. Implementation, runtime,
numerical behavior, hardware, CI and release acceptance were not tested.

## Actual checks

- `rg --files plan`: the five input planning documents were present:
  README, roadmap, milestone-01, release-01-prd and reviews.
- Python `pathlib`/`re` inspection of Markdown link targets: **27 local links
  resolved**, including the absolute skill path. There were no fragment links
  requiring anchor validation. The absolute skill link is machine-specific;
  README explicitly identifies that portability limitation.
- Python inspection of roadmap table rows: exactly **M01–M12**, in order.
- Python inspection of milestone table rows: exactly **M01-W01–M01-W04**,
  in order, with purposes, deliverables and acceptance/dependencies.
- Python set comparison of PRD identifiers: **FR-01–12, AC-01–12 and
  NFR-01–05** present with no unexpected identifiers. Manual table inspection
  confirmed one acceptance ID per functional requirement; the test-ownership
  section maps the acceptance set to scenario groups. Future test cases are
  explicitly deferred to the preimplementation test plan.
- `git diff --check`: exit 0. Because these files were untracked, this command
  alone did not inspect them. A separate Python check of every input Markdown
  file confirmed final newlines and no trailing whitespace.
- `git status --short`: only `?? plan/` at validation time; no tracked source,
  dependency or architecture changes. No production tests apply to this pure
  planning deliverable.

## Limits and handoff

After adding this report, the same path/whitespace inspection passed for all
**six Markdown files and 29 local links**; `git diff --check` again exited 0.

This report records structural/document checks, not execution of the future
reference experiment or verification of its numerical oracle. Source feasibility
and principle findings belong to the independent [planning review](reviews.md).
The [overview](README.md) distinguishes historical evidence from new execution
evidence. No commit, push, remote CI or integration is certified here; those
remain the coordinator/developer handoff responsibility.

## Hardware planning extension — 2026-10-18

Tester verdict: **PASS for updated documentation only**. The original validation
above remains historical evidence. No tester failures were found in this update.

Actual checks used Python `pathlib`, `re`, `decimal.Decimal` and `subprocess`,
plus `git status --short`, `git diff --check` and
`git diff -- plan/release-01-prd.md plan/milestone-01.md`:

- All **eight Markdown files and 40 local links** resolved; no fragment links
  were present. Every planning file had a final newline and no trailing
  whitespace. `git diff --check` exited 0; the separate Python whitespace check
  also covered the two new untracked hardware documents.
- Exactly twelve monthly and four weekly rows remain. The first-release PRD
  and first-milestone file had no diff; FR-01–12, AC-01–12 and NFR-01–05 still
  match their expected identifier sets. Hardware stage IDs HP-01–05 are complete.
- Decimal arithmetic over the ten kit rows gives **US$26.85 observed component
  anchors + US$72.00 allowances = US$98.85**, leaving US$1.15 below US$100.
  Manual inspection confirmed this is explicitly a conditional target allowance,
  with shipping/tax contingency, unresolved landed quotation/availability and
  tools/host assumptions disclosed. It is not presented as an orderable quote
  or proof that the required protected fixture fits those allowances.
- Manual cross-document inspection confirmed M04 GO/DEFER/NO-GO and firmware
  preparation precede the conditional M05 pilot; serial capacity can move scan
  scope/dates. M10 archived measured data requires provenance and cannot qualify
  current adapters or stop paths. M12 separately states simulation software,
  named setup qualification and dataset-specific empirical model validation.
  M01 remains hardware-free.
- Status contained only changes within `plan/`; no runtime source, dependency
  or architecture change was included.

Manufacturer-source verification belongs to the independent planning reviewer
and was not duplicated by this structural check. No purchase, instrument access,
electrical design qualification, firmware execution, runtime regression, CI,
commit/push or release acceptance was performed or certified by this validation.
