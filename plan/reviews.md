# Planning review

Status: **APPROVED — planning documents only**.
Reviewer: independent Architect (sw-jerry), 2026-10-18.
Reviewed baseline: local `dev` baseline `82883ad`, planning branch
`codex/long-term-plan`. No implementation, runtime tests, CI, commit, push,
integration or release acceptance is certified by this review.

## Documents and method

Reviewed [plan overview](README.md), [roadmap](roadmap.md),
[first milestone](milestone-01.md) and [first-release PRD](release-01-prd.md)
against [project principles](../principles.md), [architecture](../arch.md),
common process/hooks, scheduler, coordinated simulation, HDF5 and plotting source.
This is source/document inspection, not execution evidence. Production
architecture remains unchanged.

## Findings owned by this reviewer

| ID | Severity | Finding and required correction | Status |
| --- | --- | --- | --- |
| AR-01 | Required clarification | PRD FR-10 requires stopping the Coordinator using its documented lifecycle, but Coordinator has no `stop()` or `cleanup()` operation. Its `reset()` is a fallible model transition that clears final timing/state; it must not be treated as unconditional teardown. Require the runner to cease issuing steps, inspect final state, then release its privately owned graph references. If reset is deliberately used, describe its purpose and report its failures. Continue to require actual scheduler/backend/figure cleanup. | CLOSED by Architect after reinspection: FR-10/AC-10 and lifecycle prose now require cessation, final-boundary inspection and graph disposal, explicitly excluding implicit reset or fictitious stop APIs. |

## Feasibility and dependency assessment

The monthly sequence is coherent: concrete reference workflow precedes common
execution extraction, provenance precedes adapters/scans and later reporting,
and a second domain precedes broader generalization. Every month has an
observable exit rather than an API-count target. M04/M10/M11 external-input
gates are explicitly conditional, not granted by calendar passage. M12 is
release acceptance, not an inference from earlier CI. Review checkpoints at
M03/M06/M09 limit speculative investment.

Four weekly releases are ambitious but bounded by a single educational scalar
case, example-local glue, and removal of optional presentation/VISA work before
required evidence. W01 is plausible in one focused week if prerequisites are
already sound; mandatory injected failures, independent walkthrough and CI are
part of its estimate. A prerequisite production defect triggers scope review,
not unplanned framework development. W02 cancellation and W03 metadata remain
bounded examples before M02/M03 reusable contracts. Later-month performance
bounds and domain choice require kickoff decisions, appropriately deferred.

The W01 hook is feasible by inspection:
[`AtomJob.body`](../softlab/huo/process/common.py) invokes synchronous
`hook_before_get` before getter iteration and appends a row only after all
getters complete; `count` forwards this hook. Base Parameters can expose tick,
simulation time, commanded power and source boundary without changing public
semantics. The legacy wall timestamp is collected earlier, so it must remain
separately labeled as required by the PRD. Scheduler and backend boolean/status
results require explicit checks; source inspection does not establish that the
future runner has implemented those checks.

The numerical oracle is correct under
[`Coordinator.step`](../softlab/tu/simulation/coordination.py): pending control
10 W feeds the first candidate evolution while the measurement receives
previous-boundary output. The recurrence is `T_next = 0.9*T + 3`; measurements
at ticks 1–8 are `20, 21, 21.9, 22.71, 23.439, 24.0951, 24.68559, 25.217031`.
The source boundary is `k-1`, acquisition boundary `k`, with no hidden warm-up.
This arithmetic/source check does not substitute for independent tester tests.

HDF5 stringifies metadata and is not atomic; the PRD correctly requires explicit
text encoding, fresh paths, reopen checks and truthful incomplete artifacts.
Storage preserves experiment context without promising arbitrary Python object
serialization. Plotting consumes reopened records using existing `plot_record`;
Matplotlib's headless selection must occur before eager package plotting imports.
Failure after an advanced tick does not imply rollback; failed sampling cannot
fabricate a success row. Software cancellation is separately scoped to W02 and
never claims physical stop. Fresh instances support deterministic numeric repeats
without pretending wall timestamps or UUIDs are identical.

## Principle inspection — planning applicability

| Principle | Planning evidence / disposition |
| --- | --- |
| 1 Five-element architecture | PRD layer boundaries and roadmap use existing `tu`/`huo`/`shui`/`jin`; future work labeled proposed; `arch.md` untouched. |
| 2 Characterize behavior | PRD baseline callers, NFR-01 and preimplementation characterization; execution evidence remains future. |
| 3 Documented TDD gates | README workflow and PRD tester/developer/designer/architect sequence; runtime TDD N/A to documentation-only change. |
| 4 One active task and branches | One planning task on `codex/long-term-plan`; docs preserve `codex` → `dev` → `main`; no integration performed. |
| 5 Role/review ownership | PO drafts, independent Architect reviews; AR-01 remains reviewer-owned; UI N/A. |
| 6 Committed/pushed evidence | Planning docs specify future `log` artifacts and `docs(log):`; no commit/push certified here. Handoff evidence remains coordinator/developer responsibility. |
| 7 Environment verification | PRD lists exact regression/compile/import commands and configured Python CI; no Python changed and runtime checks N/A to planning. |
| 8 Baseline versus regression | Existing OBS/DEFECT debt explicitly tracked; no silent waiver or incidental fix authorized. |
| 9 Instruments/data safety | Synthetic thermal case, temporary paths, no hardware authorization, collision refusal, ownership/failure requirements; AR-01 clarification verified and closed by Architect. |
| 10 Minimal compatible extensions | No new dependency/framework/version changes; example-local glue first and extraction only after demonstrated duplication. |
| 11 Reproducible evidence | Weekly/month exits, independent review, principle checks and candidate CI before `dev`; release acceptance before `main`; future gates explicitly unrun. |

AR-01 was corrected by the Product Owner and reinspected against FR-10/AC-10
and the lifecycle section. All technical planning findings are closed; the
planning verdict is APPROVED. This does not authorize implementation. Formal
commit/push and final link/diff evidence remain separate from this technical
verdict; implementation and release gates remain future work.

## Budget hardware extension review — 2026-10-18

Reviewer: independent Architect (sw-jerry). Verdict: **APPROVED — planning only**, after Product Owner corrections and
independent Architect reinspection of all four findings below. This review supplements the
historical planning approval above; no hardware, firmware, serial transport or
physical model has been tested or qualified.

Reviewed [hardware pilot](hardware-pilot.md), [instrument shortlist](recommended-instruments.md),
and the README/roadmap changes. Independently opened official price sources and
checked primary component compatibility and the distinction between mandatory
software exits and conditional physical evidence.

| ID | Required correction | Status |
| --- | --- | --- |
| HW-AR-01 | Access and procurement checklist must accept the primary serial transport without requiring VISA; identify serial backend/protocol and VISA only where used. | CLOSED by Architect after corrected text reinspection |
| HW-AR-02 | Acknowledge the known below-US$100 target; distinguish observed component list prices from unquoted allowances rather than saying no numerical costs are vendor-derived. | CLOSED by Architect after corrected text reinspection |
| HW-AR-03 | Explicitly avoid carrying the simulated 10 W command into the proposed 5 V driver rig: the official driver lists 1.5 A continuous. Actual physical limits and model parameters need separate review; its maximum rating is not a recommended operating point. | CLOSED by Architect after corrected text reinspection |
| HW-AR-04 | The DAQ technical-overview `go.keysight.com/us/` link redirected to the Keysight homepage during independent retrieval. Replace it with a directly retrievable official PDF/product source. | CLOSED by Architect after corrected text reinspection |

### Independently checked evidence and limits

The four Adafruit storefront anchors were directly retrievable:
[Pico H](https://www.adafruit.com/product/5525) US$5.00 and **out of stock**;
[DS18B20 probe](https://www.adafruit.com/product/381) US$9.95 with cart button;
[MOSFET driver](https://www.adafruit.com/product/5648) US$3.95 and in-stock text;
[5 V/2 A supply](https://www.adafruit.com/product/276) US$7.95 and **out of stock**.
These are listed prices observed 2026-10-18, not delivered quotations or a
purchasable complete basket. Price anchors sum to US$26.85; unquoted allowances
sum to US$72.00, total US$98.85. Supply, check meter, all connector paths,
protection/fixture and US$30 shipping/tax contingency are included. Existing
host plus borrowed assembly tools/labor are explicit assumptions; no calibrated
reference is included. Only US$1.15 remains below US$100, so engineering/local
quotation is a genuine GO gate, with defer/borrow/replan rather than deletion
of necessary parts. No sufficiency of individual allowances is certified.

The [driver product](https://www.adafruit.com/product/5648) specifies 3–20 V
logic, 1.5 A continuous load and a 2 A connector rating. The
[official Pico documentation](https://www.raspberrypi.com/documentation/microcontrollers/pico-series.html)
and product documentation support a 3.3 V GPIO domain; that is compatible in
principle with the proposed command input. The
[DS18B20 manufacturer datasheet](https://www.analog.com/media/en/technical-documentation/data-sheets/ds18b20.pdf)
supports 3.0–5.5 V local power and up to 750 ms conversion at 12-bit resolution;
the assembled probe vendor narrows its documented system supply to 3–5 V.
The plan correctly powers data/pull-up at 3.3 V and requires validity/CRC and
sample-time handling. No assertion of calibrated temperature or safe assembled
wiring follows from individual compatible specifications.

The independent cutoff, current protection, accessible output-disable and
reviewed fixture are mandatory future engineering inputs, not completed design.
The enclosed 5 V supply is explicitly not a current-limited lab supply. The
plan forbids heating until readiness/authorization, keeps the pilot attended,
and requires physical stop observations beyond coroutine cancellation or
`*OPC?`. No detailed electrical design was created by this review.

Optional bench-source spot checks confirmed the
[DP800 datasheet](https://download.rigol.com/en/Manual/DC%20Power%26Loads/DP800/DP800_DataSheet_EN.pdf)
output/interface distinctions and retrieved the
[E36312A product](https://www.keysight.com/us/en/product/E36312A/80w-triple-output-power-supply-6v-5a-2x-25v-1a.html)
and [DMM6500 datasheet](https://download.tek.com/datasheet/1KW-61315-0_DMM6500_Datasheet_031618.pdf).
The [DAQ970A official product](https://www.keysight.com/us/en/product/DAQ970A/data-acquisition-system-usb-lan.html)
supports built-in DMM, USB/LAN and sensor-family claims. This is a source spot
check, not complete revalidation of every bench manual/options/firmware path.
The replacement [DAQ technical PDF](https://www.keysight.com/content/dam/keysight/en/doc/ungate/technical-overviews/5992-3168.pdf)
was located in official search and directly opened as a 44-page PDF during
reinspection; exact unit revision remains a future gate.

### Scope, capacity and principles

M04 makes a real GO/DEFER/NO-GO decision and reserves 1–2 weeks for reviewed
firmware/protocol/host adapter work; M05-HW reserves another week, serially with
scans. Mandatory scan scope/dates must move if needed. Startup-off, watchdog,
sensor faults, stale samples, command acknowledgement and optional serial
dependency choice are explicit prerequisites; these are meaningful extra work,
not assumed features of softlab or SCPI compatibility. M01 remains unchanged.

M04 software contracts can pass while physical qualification remains unrun;
M05 HP-01–05 qualify only the exercised named setup. M10 archived data may
support model comparison with provenance but cannot qualify current adapters.
M12 separates simulation software, setup qualification and empirical model
validation, and explicitly requires scope disposition if physical work is absent.

Principles 1–3: layers, compatibility characterization and future serial TDD gates
remain preserved; 4–6: one task, independent role closure and future evidence
commit/push rules remain applicable, with no Git actions certified here;
7–8: hardware/firmware/runtime checks remain unrun and missing components block
their gates, without invented passes; 9: conditional authorization, independent
cutoff, owned resources and immutable measured data are explicit; 10: custom
serial work is scoped and dependencies optional/proposal-only; 11: future
candidate CI and release acceptance remain mandatory. All eleven principles
are addressed at planning applicability; none grants execution approval.

Closure evidence: HW-AR-01 verified in the access row and procurement item 3;
HW-AR-02 verified in the budget row, allowance explanation and procurement item 4;
HW-AR-03 verified in pilot scope and driver compatibility prose; HW-AR-04
verified by directly opening the replacement official PDF. No open technical
planning findings remain. The US$98.85 figure remains a conditional allowance,
not a verified achievable landed purchase price. Runtime/firmware/electrical
qualification, procurement, Git integration and CI are not certified here.
