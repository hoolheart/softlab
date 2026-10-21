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
| M04 — Adapter contract and access decision | Make equipment adaptation and readiness concrete. | Select one actual transport; budget-first USB-serial rig with reviewed firmware/protocol and optional simulated-VISA lane; units, timeout/stop behavior. Record explicit equipment/access/data/budget GO/DEFER/NO-GO for [pilot](hardware-pilot.md). | M02 execution, M03 provenance; reviewed parts/firmware estimate. | Mandatory model-backed and simulated adapter contract tests pass. Decision record names missing inputs/owner/review date. No hardware qualification without separately authorized pilot evidence. |
| M05 — Scans and conditional pilot | Expand useful procedures and exercise one named setup when ready. | Count/one-dimensional/grid scan timing and partial-result semantics; separately schedule M05-HW thermal pilot after M04 GO. Reserve firmware/adapter preparation and one pilot week; reduce scan scope or move dates to maintain serial work. | M02–M04 accepted contracts; pilot additionally requires equipment, landed budget, reviewed firmware/fixture and operation authorization. | Mandatory software scan row/order/failure/cancel assertions pass. Conditional HP-01–05 qualify only named setup; unavailable access is DEFER with owner/date, never PASS. |
| M06 — Analysis and reporting | Turn recorded runs into defensible scientific observations. | Reusable result inspection, reference step-response metrics and comparison plots; explicit units, missing/failed sample handling and provenance links; no universal fitting engine. | M03 records and M05 representative datasets. | Synthetic data with known response yields documented numerical tolerances; reopened runs reproduce derived metrics; reports identify source run IDs and excluded samples. |
| M07 — Reliability and recovery | Make longer unattended software runs inspectable. | Structured progress/events, bounded buffering, persisted partial-result policy and interruption diagnostics; assess checkpoint/restart only for supported procedures. | M02 lifecycle, M03 records, M05 scans. | Fault injection at acquisition/save/cancel boundaries preserves declared committed samples and outcome; a long synthetic run meets an agreed memory bound; unsupported resume fails explicitly. |
| M08 — Lightweight adoption | Reduce the work required to try or extend softlab. | Audit eager imports/dependency necessity; propose compatible optional extras if justified; concise quickstart, installation matrix and extension cookbook. | Stable contracts from M01–M07; dependency audit before changes. | Core installation and documented extras validated in clean environments on supported CI versions; quickstart completes without notebook dependencies where the approved packaging design permits it; existing public imports remain compatible or have an approved migration. |
| M09 — Second-domain validation | Test whether abstractions generalize beyond the chosen thermal example. | Select one additional user-backed domain, e.g. circuit response; reuse execution, adapters, storage and inspection; record genuine gaps before extending abstractions. | M05–M08 and a concrete second use case. | Two domain examples share the agreed execution/run contracts; domain-specific code stays outside generic primitives; gap review accepts or rejects each proposed extension with evidence. |
| M10 — Simulation credibility | Define what models can predict. | Bounded identification/sensitivity/comparison for two reference cases; pilot or authorized archived measured data with immutable originals and provenance. | M06 analysis, M09 models; physical claims require sufficiently documented measured data. | Mandatory known-synthetic parameter/tolerance tests pass. Conditional empirical validation names dataset/range/error; missing data stays pending. Archived data never qualifies current adapters or hardware stop paths. |
| M11 — Researcher usability | Make the common workflow accessible without weakening the library. | User walkthroughs; improve notebook helpers or a minimal application entry only when observed friction justifies it; avoid a general web platform. | M08 adoption, M09 case studies, researcher feedback. | At least two independent users complete setup → run → inspect → repeat against simulation using the guide; friction findings are resolved or explicitly rescheduled; any UI change receives applicable design/review evidence. |
| M12 — Supported release | Consolidate an honestly scoped product increment. | Compatibility/debt/support/packaging review and release evidence; explicitly distinguish simulation-validated software, named hardware-qualified setups and dataset-specific model validation. | Accepted intended scope; explicit disposition of deferred pilot/empirical gates. | Mandatory candidate CI/regression/compile/import/package checks, review/principles and Product Owner acceptance pass before promotion. Hardware claims require HP-01–05 matrix; unrun gates remain pending, or release scope/date is reviewed. |

## Delivery and evidence policy

Each milestone starts by selecting a small serial backlog and agreeing observable
acceptance criteria. Outcomes take precedence over adding APIs. The M04 decision, conditional M05 hardware pilot, M10 empirical branch
and M11 user walkthroughs require external input: do preparatory simulation/document work without
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

The [hardware pilot plan](hardware-pilot.md) defines M04 disposition and M05-HW
acceptance separately from mandatory software gates. The [parts shortlist](recommended-instruments.md)
targets below US$100 total without assuming owned supply/meter; actual landed
cost and engineering readiness control GO. Firmware/USB-serial work is separately
estimated, and any serial dependency requires an explicit scoped decision. M01
and its first-release PRD remain hardware-free.
