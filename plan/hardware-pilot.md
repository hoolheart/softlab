# Proposed hardware decision and pilot

Status: **PROPOSED; no purchasing or instrument operation authorized.**
Prepared 2026-10-18. Relative months follow [the roadmap](roadmap.md).
M01 remains entirely simulation-first. This pilot has a separately reviewed task
scope and operator authorization; inclusion in a roadmap is not authorization.

## M04 decision gate: equipment, access, data and budget

Purpose: decide whether a small thermal hardware pilot can execute in M05,
after the adapter contract, without making hardware availability a hidden
prerequisite for software development. Product Owner records the decision;
Architect reviews technical feasibility; Tester specifies evidence and operator
performs only explicitly authorized operations.

| Required decision | Evidence to record before GO |
| --- | --- |
| Equipment | Inventory owned/borrowable parts first; for the below-US$100 target assess a microcontroller, digital sensor, switched low-voltage heater, protected supply, check meter, cables and fixture. Commercial supply/meter/DAQ is an optional borrowed alternative. Record exact model, installed options, firmware and manual revision; verify the selected sensor conversion, connector and remote interface. Use [shortlist](recommended-instruments.md) only to fill gaps. |
| Access | Named equipment owner/operator, booking dates, location, control-host OS and USB-serial backend/protocol for the budget rig (VISA backend/resource only for bench instruments), permitted connection and written task authorization. No autonomous discovery of other instruments. |
| Data | Where raw readings/configuration/logs go, backup owner and retention; permission to use/share readings and any archived dataset. Reserve immutable originals and fresh run paths. |
| Budget | Below-US$100 total target, confirmed user-approved ceiling and currency/region/tax treatment, itemized local quotation including probe/options/fixture/calibration/transport, procurement lead time and owner. Reuse/borrow first. No order without separate authorization. |
| Fixture/readiness | Reviewed wiring and thermal contact, power/current/temperature limits, independently acting thermal cutoff, accessible output disable and conservative first-step settings. Resolve sensor faults and power-off behavior before heating. |
| Acceptance | Named models/configuration to qualify; required readback/timeout/stop tests; sample cadence, run duration and repeatability/model-comparison tolerances agreed before running. No post-hoc thresholds chosen to declare success. |

Record one explicit outcome by M04 acceptance: **GO for M05**, **DEFER with owner,
missing inputs and next review date**, or **NO-GO for this horizon**. A deferred
pilot is pending, not passed. Mandatory M04 software gates remain model-backed and simulated adapter contract
tests with honest timing/timeout semantics. For the budget rig, add reviewed
firmware/USB-serial protocol and fault tests; simulated VISA is a separate
optional lane, not a requirement to purchase SCPI equipment. M04
software acceptance may proceed with the documented hardware disposition;
hardware qualification remains a separate unpassed gate.

## M05-HW: separately scheduled conditional thermal pilot

Target: one reviewed working week within M05, after M04 software acceptance,
GO and accepted firmware/adapter preparation. Provisionally reserve another
1–2 working weeks in M04/M05 for custom USB-serial firmware/adapter work; refine
the estimate at M04 and move scan scope/dates if needed. Keep the one-active-task policy: schedule it serially with scan tasks; reduce
scan scope or move dates if capacity is insufficient. Do not assume two concurrent
teams. If access arrives later, rebook a named week in M06–M09 at monthly review;
if unavailable by M09, explicitly retain simulation-only release scope and do
not infer pilot success from archived data.

Scope: one low-energy resistive heater thermally attached to a small mass,
one temperature channel, one protected supply/driver channel, one host; ambient baseline → bounded
power step → cool-down. A second ambient channel is useful only if already
available. Reuse the same procedure intent and data contract as simulation, with
a hardware acquisition/settling policy. Do not impose coordinator ticks or its
measurement-edge latency on physical time. Record commanded duty/power intent and reviewed supply/current limits. A low-cost
rig without independent voltage/current telemetry records those fields as
unavailable; power inferred from nominal voltage/resistance/duty is a model
estimate. With bench readbacks, label V×I as readback-based. Neither is a
traceable calorimetric measurement.
The physical fixture receives a separate reviewed run/model configuration and
conservative heater/current/temperature limits. Do not copy the simulated 10 W
command: at 5 V that would require 2 A, exceeding the proposed driver's documented
1.5 A continuous limit. That limit is an upper component rating, not a recommended
operating current; the engineering review must choose a lower suitable limit
for the complete protected fixture.

Proposed serial stages and acceptance IDs:

1. **HP-01, readiness:** independent test/design review accepts limits, wiring,
   sensor mode, temperature plausibility, explicit output-off path and operator
   procedure. No-load/low-energy communications checks are separately authorized.
2. **HP-02, adapter:** identify exact board/instrument/firmware and protocol, set/read declared units,
   bounded command/query timeout and visible errors. Record serial backend/version, USB identity, firmware/protocol and command
   sequence for the budget rig; record VISA backend/resource type for bench gear. Qualify only the exercised commands.
3. **HP-03, experiment:** at least three bounded step/cool-down repetitions create
   distinct IDs with actual timestamps, command/readback and temperature rows;
   persist/reopen/plot each. Explain warm-up, sample latency and missing samples.
4. **HP-04, failure/stop:** rehearse safe sensor-disconnect handling and a benign
   communication failure under the approved test setup. Request output off,
   verify through the agreed independent observation/operator check, and report
   any unknown physical state explicitly. Coroutine cancellation or `*OPC?`
   alone is not proof that heating stopped. Do not deliberately overheat to test
   the cutoff during this software pilot.
5. **HP-05, review:** Tester records results against pre-agreed thresholds,
   reviewer closes findings, Product Owner accepts or rejects the bounded pilot.
   Retain raw data and a qualification matrix naming models/options/firmware,
   host/backend, sensor/fixture, timing/ranges, commands and limitations.

Successful software communication is not model validation. Compare thermal
curves descriptively in the pilot; a model failing to fit remains an honest
scientific result. M10 performs model identification/validation separately.
HP-01–05 must all pass before claiming this named setup hardware-qualified.
Failed/aborted runs remain linked evidence; neither repeatability nor physical
stop is inferred from successful file output.

## M10 data provenance and scientific validation

M10's mandatory software gate uses known synthetic data and a documented
identification/comparison tolerance. Its conditional physical-evidence gate may
use either accepted pilot recordings or authorized archived measured data.
For archived data record owner/permission, original checksum/path, collection
date, exact available equipment/sensor/calibration/configuration, units,
timestamps, preprocessing and missing context. Keep original data immutable.
Reject an empirical claim when essential context is missing, or narrow its
stated validity range. Archived data can support a measured-vs-model comparison;
it does **not** qualify current adapters, current hardware, cleanup or stop paths.

## M12 release claims and pending work

Maintain separate statements: **simulation-validated software** (named procedures,
versions, fixtures and CI evidence); **hardware-qualified setup** only for the
named HP-01–05 matrix that actually passed; and **empirical model validation**
only for the named dataset/configuration/range analyzed in M10. An absent or
failed hardware pilot appears as pending/deferred with reason, owner and next
review, not a release-wide hardware claim. A simulation-only release can be
accepted after its mandatory gates, explicit hardware scope disposition and
Product Owner acceptance. If hardware qualification was made a release promise,
remove that promise through reviewed scope change or delay release; never mark
the unrun gate passed.
