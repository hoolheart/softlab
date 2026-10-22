# M01-W03-001 Architect detailed-design review

Architect reviewer: sw-jerry. 2026-10-22 Asia/Shanghai.
Subject design125d6be on codex/m01-w03-stored-context.
Verdict: **APPROVED — Tester feature red contracts may proceed**.

Reviewed [design](../design/M01-W03-001-design.md), [PRD](../prd.md),
[envelope](../architecture-plan.md), [test plan](../test/M01-W03-001-plan.md),
Developer approval9a8347f and [characterization](../test/M01-W03-001-characterization.md).
Read current HDF5 save/load, DataGroup metadata, example capture/reload/roundtrip/
execute lifecycle and existing W01/W02 metadata/outcome/cleanup callers. Historical
pending plan headers are superseded by Developer approval. No code/tests changed,
no runtime checks or implementation code review performed by Architect.

| Concern | Architecture/design assessment |
| --- | --- |
| Representation/minimality | Single thermal_manifest JSON-text attribute matches characterized str(value) backend; no sidecar, library repair/export/dependency/support or framework change. Existing mandatory legacy keys remain three; optional envelope hydration does not redefine arbitrary metadata elsewhere. |
| Closed schema/types | Version1 identifies exact required/exclusive keys and fixed semantics, strict dict/list/scalars, finite real normalization, bool/int distinction and declared availability nulls. Duplicate keys/nonfinite JSON tokens, malformed present content and incompatible schema/version reject distinctly. No object construction, stringification fallback or defaults for missing fields. |
| Source identity | SHA256 covers exact reference-script bytes only; source-relative tracked-repository Git checks distinguish actual clean/dirty/unavailable, failed queries never imply clean, unrelated cwd is ignored. Timeout/argument-list queries and unavailable reasons preserve installed use without Git. Producer identity remains stored evidence; inspection never substitutes reader identity. Concurrent mutation is explicitly outside atomic-snapshot claims. |
| Independent inspection | inspect_stored selects an explicit group or requires one discovered read-only; copied reload hydrates captured text. Fresh-process CLI emits typed JSON plus actual numeric rows, sends backend prints to stderr and distinguishes legacy_unavailable from invalid/unsupported. Source artifact remains untouched. |
| Data/config/clock relationship | Decoder requires actual disk-derived UUID, exact ordered columns/units/labels/dependent, count/tick/time equality and overlapping legacy text consistency. Finite sample columns, tick/source boundary sequences, dt/power checks and independent delayed Euler recurrence reject metadata internally consistent but inconsistent with observations. This is explanatory consistency, not authenticity or calibration. |
| Snapshot/lifecycle | Producer creates/validates envelope after completed acquisition before writer creation; snapshot leaves persistence/plot/cleanup/overall not_yet_established. Encoding failure preserves completed acquisition with failed persistence and no writer. Cancelled/failed partials remain memory-only. Existing W02 result/cause/structured cleanup and actual later-stage outcomes remain authoritative. |
| Reload/delivery safety | Absent envelope remains valid legacy inspection, never enriched from defaults. Present-invalid fails; producer roundtrip metadata equality additionally detects newly produced envelope removal. Copied decode/consistency failure still runs backend/scratch cleanup and carries original cause/ordered cleanup evidence. No atomic/no-byte storage claim. |
| Compatibility and tests | Existing successful run return, private reload signatures, numeric schema, text metadata, source-integrity measures and W02 interruption/reset/repeat contracts remain regression requirements. Approved test groups cover new strict encoding/actual HDF5 tampering/fresh subprocess/source seams/guide paths before production code. |

No blocking findings raised. Tester now owns executable red contracts against
committed preimplementation behavior; Developer implementation follows that
recorded gate. Tests must independently observe stored fields/types and actual
samples, source-query scope, missing versus present-invalid policy, representation
faults and later outcome/cleanup failures. These are approved test obligations,
not evidence that new behavior already works. No reviewer/test failure owned by
another role is closed here.

No demonstrated prerequisite authorizes a library change. Such a finding blocks
for Architect/PO scope review. R4-BASE-SCHED-01/META-01/HDF5-01 and historical
OBS-004/005/006/DEFECT-2 remain open under characterized private executor/text
hydration/copy-only measures. No general provenance, arbitrary serializer, SQLite
parity, partial durability/resume, W04/hardware/UI or publication is approved.

Feature red tests, implementation, independent code review/testing, actual
architecture closure, all-eleven completion principles, exact candidate CI/dev
integration and PO acceptance/main promotion remain pending. This review/board
handoff commits/pushes immediately with docs(log); coordinator verifies actual
commit/upstream/clean evidence before progression.
