# M01-W03 product acceptance

Current release state: **CLOSED**; [final promotion/CI evidence](closure.md).
The phase-time acceptance/principle evidence below retains its original scope.

Product Owner: sw-camille. Date: 2026-10-24 Asia/Shanghai.
Verdict: **PASS — bounded M01-W03 runtime and documentation accepted**.
Accepted integrated runtime: dev20fa9a0; closure evidence subjecta61b08d on
codex/m01-w03-release-closure. Tester comparisons establish runtime/tests/guide/
configuration identical to verified447ad095 and final integrated20fa9a0.

## Product outcome and review method

The [PRD](prd.md) promised an explainable stored reference result. The delivered
[guide](../../docs/user-guide/thermal-experiment.md) describes one closed version1
JSON-text thermal_manifest in the sample HDF5 group, separate-process typed
inspection, changed configuration and explicit legacy/corruption/version behavior.
Producer identity is preserved with bounded source digest/Git scope and honest
unknown/dirty values. Stored completed-acquisition context explicitly leaves
later persistence/plot/cleanup outcomes unestablished; current RunResult remains
authoritative for those operations. This meets the bounded reference-user goal.

PO inspected requirements, approved design/Architect review, implementation and
code-review closure, guide, actual architecture, integration/principle records
and independent results. PO did not execute tests, design, implement or close
technical findings. Runtime conclusions rely on [owned task results](test/M01-W03-001-results.md),
[CI investigation/closure](test/M01-W03-001-ci-failure.md) and
[final validation](test/M01-W03-CLOSURE-validation.md). Earlier phase-pending and
failed-candidate records remain historical, superseded only by later named owned
evidence; no gate is made retroactively passing.

## Acceptance matrix

| Criterion | Verdict | Accepted evidence |
| --- | --- | --- |
| AC-01 | PASS | Closed root/nested schema covers complete configuration, model/equation/validity/units, procedure, columns, clock, producer software/source, UUID and named outcome snapshot; physically stored and independently asserted. |
| AC-02 | PASS | Brand-new CLI interpreter reconstructs all declared typed fields and numeric rows without original live state; guide generation and inspection run in separate processes. |
| AC-03 | PASS | Actual UUID/count/schema/unit/clock/config and sample temperature/tick/source/time/power tampering rejected; exact logical-boundary sub-tolerance corruption regressions pass and inspection preserves source hashes. Independent delayed recurrence checks stored physical observations. |
| AC-04 | PASS | Fresh defaults recover equal configuration/traces with distinct IDs; changed dt0.5/power6/points5 recovers distinctly and matches independent delayed Euler oracle. |
| AC-05 | PASS | Wrong/unsupported types, bool counts, nonfinite values, nonstring keys, extra/missing/duplicate keys, malformed/nontext/nonfinite-token JSON visibly rejected. Encoding failure prevents writer construction and preserves cause. No arbitrary constructors/stringification. |
| AC-06 | PASS | Absent attribute yields explicit legacy_unavailable/null manifest with numeric inspection; present invalid content or unsupported schema/version fails visibly without downgrade, default repair or automatic migration. |
| AC-07 | PASS | Clean/dirty/unrelated-working-directory source fixtures, unavailable Git/digest reasons and declared exact-script SHA256 scope pass; reader preserves captured producer identity rather than recapturing local environment. |
| AC-08 | PASS | Completed acquisition snapshot labels later outcomes not_yet_established; later plot failure retains verified data while overall execution fails. W02 save/reload/cleanup faults and memory-only partials retain truthful independent outcomes. |
| AC-09 | PASS | Full regression includes W01 numeric/schema/delay/UUID/plot/collision and W02 interruption/acknowledgment/failure/reset/repeat/resources plus legacy metadata characterization. Library/backend/dependency contracts unchanged. |
| AC-10 | PASS | Missing new-delivery manifest and malformed/version/relationship copies visibly rejected; source integrity, fresh-path refusal and owned cleanup/borrowed-state failure matrix pass. Incomplete bytes are not mislabeled atomic success. |
| AC-11 | PASS | Tester executed documented default/changed generation, separate-process typed/numeric inspection and verbatim legacy/malformed/version fixtures in temporary synthetic outputs; guide explains identity uncertainty, snapshot and backend/W02 limits. |
| AC-12 | PASS for delivered product/evidence readiness | Approved plan/design/owned review and failure closure, actual architecture, guide, local full275/compile/import, all11 principles and exact implementation candidate/final-dev both-version CI are recorded. This PO acceptance precedes promotion; final closure-document CI/integration checks remain explicitly required below. |

## Owned gates and remaining integration

Designer closed CR-W03-01 in approved iteration2; Tester closed feature, fixture,
regression and scoped CI failures through actual green evidence. Independent
harness review and [closure document review](reviews/M01-W03-CLOSURE-review.md)
are APPROVED; no scoped issue remains open. Underlying warning/library debt
remains open as described below. PO does not reassign any closure authority.

Actual architecture8dde568 and release-readiness principlee28575c reconcile the
delivered feature. Final Tester validation of84df298 actually reran275 tests
under -W error, compileall and import, with no skips/unexpected local diagnostics;
links/whitespace/scope and runtime identity checks passed. Existing executed guide
evidence remains applicable because its bytes are unchanged. No remote warning-free
or unconfigured-tool claim follows from local checks.

Exact implementation candidate11f6c94
[CI39793295929](https://github.com/hoolheart/softlab/actions/runs/39793295929)
passed Python3.9/3.13 before devf7aa589 integration. Final task-record dev20fa9a0
[CI39797205010](https://github.com/hoolheart/softlab/actions/runs/39797205010)
also explicitly passed both versions. These do not certify this acceptance commit's
CI. Coordinator supplied all-eleven formal release READINESS PASS at clean
HEAD/upstreama61b08d; delegated recording appears in principle_compliance_report.md.

This PASS accepts the delivered runtime/documentation. Exact final closure candidate
CI, closure dev reconciliation, target-state checks and authorized main promotion
remain required and are not claimed executed here. Reinspect affected acceptance
if requirements/runtime/guide scope changes. Weekly W03 acceptance does not accept
W04 or the complete first milestone and does not authorize version/tag/publication.

## Explicit warning/debt disposition and limits

Retain the previously Architect/PO-approved bounded test-child policy: four exact
parser message/category/module rules and one exact pandas-concat FutureWarning
rule use visible action-default stderr; every other/new warning stays fatal.
Parent/production policy and dependencies/configuration are unchanged. Actual
reproduced failures preceded separate scope approval; Designer reviewed harness
changes and Tester closed scoped CI failures after exact both-version success.
Underlying parser/pandas warning debt remains open. Python3.9 CI still reports
known warnings; no zero-warning CI claim or silent waiver is made. Changed or
unknown cases require review rather than widening filters. CI Node20/forced24/
Ubuntu26 notices retain separate disclosed platform disposition.

R4-BASE-SCHED-01/META-01/HDF5-01 remain unfixed under the tested private executor,
explicit text hydration and copy-only loading/source-integrity measures.
OBS-004/005/006/DEFECT-2 remain historical out-of-scope debt; no broad repair or
debt closure is inferred. Internal explanatory consistency is not authentication,
physical calibration, full repository/environment reproducibility or atomic
source capture. No arbitrary serializer, library/backend format change, universal
provenance framework, SQLite parity, durable partial recovery/resume, physical
termination, hardware/UI, W04 or package publication is accepted here.
