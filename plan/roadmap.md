# Proposed twelve-month roadmap

Status: **PROPOSED**. Relative cadence and authority are defined in
[README](README.md). Every row is future work, not an implemented capability.
The main product measure is whether a researcher can construct, execute, inspect,
repeat and adapt a concrete experiment without rewriting the procedure.

| Month | Purpose | Proposed scope and concrete outcome | Dependencies | Measurable exit gate |
| --- | --- | --- | --- | --- |
| M01 — Reference experiment | Prove that the existing layers form a useful workflow. | Thermal step-response vertical slice; explicit simulation timing; failure/repeat behavior; bounded run metadata; simulated adapter seam. Four weekly increments in [M01](milestone-01.md). | Accepted Release 3 primitives and inspected `huo`/`shui` callers. | All four weekly acceptance matrices pass; one fresh-environment command executes, saves, reloads and plots the reference experiment without hardware. |
| M02 — Reusable execution | Remove example-specific orchestration without creating a large framework. | Extract only demonstrated common `huo` execution needs; explicit acquire/advance/sample policy; run state, cooperative cancellation, retry/reset boundaries and owned-resource lifecycle. | M01 acceptance and observed duplication. | Reference experiment uses the extracted path; success, failure, cancel and repeat tests pass; legacy count/scan behavior is characterized and unchanged. |
| M03 — Reproducible run record | Make results explainable after the original session ends. | Versioned bounded manifest connecting procedure configuration, setup/model declarations, units, software/environment identity, outcome and samples; documented serialization and missing-field policy; evaluate deferred simulation description needs. | M02 lifecycle; M01 metadata findings. | Save/load preserves all required manifest fields and sample relationships; unsupported values fail visibly; schema compatibility tests and a clean-session rerun comparison pass. |
| M04 — Physical adapter contract | Make the route to equipment concrete and honest. | One instrument/protocol family selected from an actual use case; simulated VISA contract tests, unit conversion, timeout and explicit stop/cleanup behavior; hardware validation checklist. | M02 execution policies and M03 provenance. | Same procedure runs through model-backed and simulated-VISA adapters with declared timing differences; timeout/cleanup tests pass. Real-equipment qualification remains separately gated by explicit authorization and access. |
| M05 — Scans and composition | Support a useful family of experiments. | Apply reference execution/provenance to count, one-dimensional and grid scans; define partial results and per-point timing; preserve current composition contracts. | M02–M04 adapter and run contracts. | Count, scan and grid examples pass expected row/order/value assertions plus mid-scan failure/cancel cases; no duplicate or fabricated samples. |
| M06 — Analysis and reporting | Turn recorded runs into defensible scientific observations. | Reusable result inspection, reference step-response metrics and comparison plots; explicit units, missing/failed sample handling and provenance links; no universal fitting engine. | M03 records and M05 representative datasets. | Synthetic data with known response yields documented numerical tolerances; reopened runs reproduce derived metrics; reports identify source run IDs and excluded samples. |
| M07 — Reliability and recovery | Make longer unattended software runs inspectable. | Structured progress/events, bounded buffering, persisted partial-result policy and interruption diagnostics; assess checkpoint/restart only for supported procedures. | M02 lifecycle, M03 records, M05 scans. | Fault injection at acquisition/save/cancel boundaries preserves declared committed samples and outcome; a long synthetic run meets an agreed memory bound; unsupported resume fails explicitly. |
| M08 — Lightweight adoption | Reduce the work required to try or extend softlab. | Audit eager imports/dependency necessity; propose compatible optional extras if justified; concise quickstart, installation matrix and extension cookbook. | Stable contracts from M01–M07; dependency audit before changes. | Core installation and documented extras validated in clean environments on supported CI versions; quickstart completes without notebook dependencies where the approved packaging design permits it; existing public imports remain compatible or have an approved migration. |
| M09 — Second-domain validation | Test whether abstractions generalize beyond the chosen thermal example. | Select one additional user-backed domain, e.g. circuit response; reuse execution, adapters, storage and inspection; record genuine gaps before extending abstractions. | M05–M08 and a concrete second use case. | Two domain examples share the agreed execution/run contracts; domain-specific code stays outside generic primitives; gap review accepts or rejects each proposed extension with evidence. |
| M10 — Simulation credibility | Define what simulation can and cannot predict. | Parameter identification/validation workflow for the two reference cases; measured-vs-simulated comparison where authorized data exists; sensitivity and uncertainty reporting with bounded scope. | M06 analysis, M09 models, validated source data. | Known synthetic parameter case is recovered within stated tolerance; model error and validity range are reported separately from software repeatability. If physical data is unavailable, report synthetic-only status and leave physical validation gate pending. |
| M11 — Researcher usability | Make the common workflow accessible without weakening the library. | User walkthroughs; improve notebook helpers or a minimal application entry only when observed friction justifies it; avoid a general web platform. | M08 adoption, M09 case studies, researcher feedback. | At least two independent users complete setup → run → inspect → repeat against simulation using the guide; friction findings are resolved or explicitly rescheduled; any UI change receives applicable design/review evidence. |
| M12 — Supported release | Consolidate a maintainable, documented product increment. | Compatibility and debt review, support/deprecation policy, packaging/version decision, end-to-end release evidence and next-year roadmap. | All intended release scopes accepted; M10 physical-data limitations explicitly dispositioned. | Candidate CI, documented regression/compile/import/package checks, reference workflows, independent review, principles and Product Owner release acceptance pass before promotion; unresolved limitations appear in release notes. |

## Delivery and evidence policy

Each milestone starts by selecting a small serial backlog and agreeing observable
acceptance criteria. Outcomes take precedence over adding APIs. Months M04, M10
and M11 require external input: do preparatory simulation/document work without
claiming hardware qualification or user-study success. Missing input moves the
relevant gate and dependent scope; it is not a silent waiver.

Monthly gates include all relevant project checks, temporary synthetic data,
resource cleanup, review closure and recorded baseline warnings. Performance
bounds are set from a measured baseline before optimization work; no hard
real-time or thread-safety guarantee is implied. Review M03 architecture and
product fit, M06 scientific usefulness, and M09 cross-domain reuse before funding
the next block. Stop or narrow abstractions that the two concrete workflows do
not justify. Reserve roughly one week per month across the increments for review,
integration, defects and documentation; reduce feature scope if this is consumed.

## Deferred unless justified separately

A general GUI/server, distributed execution, broad instrument catalog, universal
ODE/PDE solver, model graph persistence, automatic calibration, arbitrary object
serialization, cloud collaboration and hard real-time control have no scheduled
commitment. The roadmap can justify a specific bounded capability, but does not
implicitly authorize these larger projects. Existing OBS-004/005/006 and DEFECT-2
remain tracked baseline debt; milestone kickoff evaluates impact and schedules
separate approved fixes rather than changing their contracts incidentally.
