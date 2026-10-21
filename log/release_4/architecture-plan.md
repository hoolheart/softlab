# M01-W01 architecture plan and implementation disposition

Owner: sw-jerry (Architect). Date: 2026-10-18.
Baseline: `dev` `e034825`; activated [PRD](prd.md) at `c776fe8`.
This is a planned integration example, not implemented architecture or detailed
design. Existing [architecture](../../arch.md) remains authoritative for shipped APIs.

## Bounded composition

One example-local runner connects existing responsibilities: `tu` owns the
thermal object, station, heater control and delivered thermometer; `huo` performs
bounded count acquisition with explicit coordinator stepping in its existing
before-get hook; `shui` owns numeric records/groups and HDF5 persistence; `jin`
plots reopened data. `mu` requires no change. Use a private scheduler and fresh
model/station/process/record per invocation. No new library API, public export,
dependency, format, supported-Python or package-version change is planned.

Preparation establishes boundary zero; stage 10 W before acquisition. Advance
exactly once before each measurement read. Preserve previous-boundary delivery:
eight temperatures are 20, 21, 21.9, 22.71, 23.439, 24.0951, 24.68559,
25.217031 degC, with acquisition boundary k and source boundary k-1. Tester must
independently derive/characterize this before implementation. Wall time remains
distinct from simulation time. No sleep drives simulation.

Validate configuration and output collisions before ticks/final artifacts.
Store configuration/timing as explicitly encoded string metadata; preserve UUID,
column definitions/units and values through disconnect/reopen verification.
HDF5 can partially write: failures must report incomplete paths. Plot only the
reopened record, close figures, and report overall success only after verification.
Check boolean/status results rather than assuming absence of an exception means
success. Cleanup preserves primary and cleanup failures; stop issuing steps,
inspect the final boundary and discard the private graph without implicit reset.
Disconnect owned backends and stop owned scheduling resources without affecting
global/borrowed state. No autonomous coordinator cleanup API is invented.

## Designer-owned decisions and prerequisite gate

After approved tester plan, Designer chooses example/test/guide paths, exact
command, field order/names/units, text encoding, scheduler hookup, failure seams
and resource-lifecycle structure. Architect reviews those decisions before code.
Read existing hook callers, SimpleProcess, simulation coordinator/mock objects,
data model/HDF5 and plotting paths. Characterize touched public contracts first.
No implementation prerequisite defect is presently established; if one appears,
stop expansion for Architect/PO scope review. Existing OBS-004/005/006 and DEFECT-2
remain tracked in arch.md and excluded from incidental fixes.

## Delivery and evidence

[One task](tasks.md) carries every serial role phase. AC-01–12/NFR-01–05 require
independent numeric/order, invalid-input/collision, reopen/plot, injected-failure,
cleanup and repeat-isolation evidence. Full regression, compile/import and named
warning checks plus configured Python 3.9/3.13 candidate CI precede dev integration.
No runtime evidence is asserted here. UI/Figma, real hardware and publication are
N/A. W02 cancellation, W03 manifest and W04 adapters remain excluded. Release-end
architecture update describes actual results only; PO acceptance precedes main
promotion. Start inspection is [recorded separately](principle_compliance_report.md).


## Actual implementation disposition — 2026-10-21

The planned bounded composition is implemented in example `6f8b26d`, cleanup fix
`5c5f6c4`, guide `864891a`; [actual architecture](../../arch.md) updated at
`af1dc74`. Detailed design/Architect approval, DR-01 closure, red tests,
independent Reviewer CR-01 closure and Tester results are linked from the board.
No plan proposal above is a new supported API: the public Scheduler extension
is used only by the local serial adapter; HDF5 reload uses temporary copies and
disk-derived metadata. No library source/export/dependency/format change occurred.
R4-BASE-SCHED-01/META-01/HDF5-01 are explicitly tracked, unfixed future candidates;
local tested compatibility measures do not close them. Prior debt is untouched.

Task runtime acceptance evidence is complete per independent Tester on a7953e3
(229 local -W error tests, CLI/compile/import and AC/NFR traceability); exact
candidate CI and 23ae605 evidence CI are green. This actual-architecture record
is not a dev/main merge or PO release acceptance. Final documentation-candidate
CI, dev integration and PO release acceptance remain pending.
