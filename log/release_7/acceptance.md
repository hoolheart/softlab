# M01-W04 independent product acceptance

Date: 2026-10-30. Reviewer: independent of example and guide implementation.
Verdict: **PASS for the bounded W04 product outcome**. The M01 product outcome
is accepted in the milestone matrix; subsequent integration and promotion are
recorded in the [authoritative closure](closure.md). The
reviewer also executed tests and relies on the separately authored independent
code review for code-review conclusions.

| PRD criterion | Assessment and evidence |
| --- | --- |
| AC-01 | PASS: common procedure, ordered schema and adapter operation/failure checks in [tester results](test/results.md); [code review](reviews/code-review.md) approves the shared path. |
| AC-02 | PASS: independent numerical boundaries and recorded timing checked for both modes; guide describes simulated observation and limits physical interpretation. |
| AC-03 | PASS under this PRD's stated alternative: tester ran the headless direct command, inspected persisted data and manifest in a fresh process, and checked the PNG. Environment was the existing Python 3.13.15 virtual environment, explicitly documented, not a fresh installation. |
| AC-04 | PASS: guide maps control/measurement, units, timing, ownership, errors and physical settling, and labels unverified physical behavior; no device was operated. |
| AC-05 | PASS: independent full regression and targeted failure, cleanup, legacy and manifest tests; evidence and timing of later nonbehavioral changes are in tester results. |
| AC-06 | PASS for the documented backlog and milestone assessment: [backlog](follow-on-backlog.md) records proposed dispositions, and [matrix](milestone-acceptance.md) links accepted weekly evidence and records the fresh-environment exit evidence. Later milestone work is not authorized by this assessment. |
| AC-07 | PASS for applicable pre-integration review and testing: independent design and code review, tester evidence and [principle inspection](reviews/principle-inspection.md) are recorded. Review findings were closed by their reviewer; no test failures remain open. Actual local environment and warning limits are reported. Candidate-level checks remain separate. |
| AC-08 | PASS: candidate CI, integration and promotion verification are recorded in the [closure summary](closure.md). |

The roadmap's fresh-environment condition is met by the isolated Python 3.9.6
walkthrough and independent repeat in [test results](test/results.md), with the
installation retry, pip upgrade and pandas warnings recorded. The subsequent CI, integration, promotion and release closure facts are recorded
in the closure summary. Existing baseline R4-BASE-SCHED-01, R4-BASE-META-01,
R4-BASE-HDF5-01, OBS-004/005/006 and DEFECT-2 remain tracked. No hardware, VISA, package publication, full Python 3.9 regression or universal
warning-free claim is made.
