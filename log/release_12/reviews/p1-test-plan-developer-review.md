# Phase 1 Characterization Test Plan — Developer Review (implementability/correctness)

Reviewer: sw-tom (Developer) | 2026-11-01 Asia/Shanghai
Branch: `codex/m03-p1-characterize`
Input: [log/release_12/test/characterization-test-plan.md](../test/characterization-test-plan.md)
against [log/release_12/prd.md](../prd.md) (Phase 1 row, M03-AC01) and
[plan/milestone-03.md](../../../plan/milestone-03.md).
Method: source-level inspection only — no test code written, no source modified,
no runtime characterization executed. Every cited entry point, defect and
behavior was checked against the current baseline on this branch.

## Verdict

**CHANGES REQUESTED** — the plan's structure, scope, ground rules, safety
protocol and gate linkage are sound and match the PRD Phase 1 row; all three
cited defects and nearly all cited entry points were verified as real. However,
four corrections are required before this plan is executable: one material
factual error about the signal-monitor client under inventory (F1), one
naming/caller-inventory accuracy gap (F2), one reachability clarification for
the execution-UUID probe (F3), and one unspecified load path whose two
alternatives diverge observably (F4). All fixes are plan-text corrections; no
re-planning is needed.

## Verified as accurate (positive confirmation)

1. **R4-BASE-META-01** — confirmed exactly as described.
   `softlab/shui/data/base.py:434-438`: `DataGroup.__init__` loops over
   `meta.items()`, coerces keys via `str(key)`, writes back into the caller's
   mapping, and never assigns `self._meta` (stays `{}`). The caller's dict is
   mutated (int keys coerced to str); `group.meta` remains empty.
2. **R4-BASE-HDF5-01** — confirmed. `softlab/shui/data/hdf5.py:137-142`:
   `load_data_group_from_hdf5_group` binds `meta = hdf5_data_grp.attrs` (a live
   proxy) and `pop`s `name`/`id`/`timestamp`/`backend`. Since `connect_impl`
   opens `h5py.File(path, 'a')` (hdf5.py:39), a load can destructively remove
   reserved attrs from the open file.
3. **`set_meta(None)` mismatch** — confirmed. `base.py:550-552`: the key is
   popped and then unconditionally re-assigned `self._meta[key] = value`, so
   the key remains with value `None` against the docstring "will be removed".
4. **HDF5 text metadata** — confirmed at `hdf5.py:81-82`
   (`hdf5_data_grp.attrs[key] = str(value)`); the plan's line citation is exact.
5. **SQLite metadata capability** — confirmed. `sqlite3.py:208-209` persists
   only `group.meta.get('sample', 'demo')` and `group.meta.get('run_id', 0)`;
   `load_impl` (sqlite3.py:159-162) reconstructs meta as only
   `{'sample', 'run_id'}`. Arbitrary extra metadata is dropped. (Schema quirk
   the plan can pick up: the `run_id` column is declared `integer` while the
   saved value is unconstrained — good characterization material, plan already
   asks for the schema inspection.)
6. **C1 entry points** — all exist in `examples/thermal_experiment.py`:
   `_MANIFEST_KEY` (L341), `_check_manifest` (L384), `_encode_manifest` (L510),
   `_decode_manifest` (L515), `_make_manifest` (L576), `_validate_manifest_data`
   (L608), `inspect_stored` (L659), `_load_group` (L674), `_reload_group`
   (L696), CLI `--inspect-file` (L930) with `--group-id` (L931). v1/v2 split is
   `version = 1 if delay else 2` (L582) with `_timing(1 if v1 else 0)`.
7. **Absent vs invalid distinction** — confirmed. `inspect_stored` (L668-671)
   returns `(group, None)` for absent envelope; `_load_group` (L683-685) raises
   `ValueError` for a present non-text manifest; invalid present text raises in
   `_decode_manifest` via `_reload_group` (L711-713). CLI reports
   `context: 'available' | 'legacy_unavailable'` (L952). Invalid-present is
   never treated as absent in current code.
8. **Completed-acquisition snapshot semantics** — confirmed verbatim:
   `_make_manifest` (L594-601) and `_check_manifest` expected block (L492-497)
   record `snapshot == 'completed_acquisition_before_persistence'`,
   `acquisition == 'completed'`, and `persistence`/`plotting`/`cleanup`/
   `overall == 'not_yet_established'`. `_make_manifest` raises when
   `result.acquisition != 'completed'` (L579-580).
9. **Independent oracles** — `ThermalPolicyComparison.euler`
   (tests/test_m02_w01_characterization.py:364-378) exists as cited; the
   C5a.2 reuse is valid. M02 test conventions (synthetic fixtures, disposable
   copies, defect-as-findings, child-process probes) match the plan's stated
   conventions.
10. **Ground rules are executable** — synthetic temp data only, disposable
    byte-copies with sha256 before/after (C3.1 mirrors the already-delivered
    `_reload_group` copy protocol, thermal_experiment.py:702-705), no hardware,
    no source changes, test doubles/harness instrumentation only (C4.2).
    Nothing in the plan requires modifying `softlab/` or `examples/`.
11. **Scope discipline** — no schema design, no fixes, no drift into Phase 2/3
    work. C5b.1's "which future component would own construction" is explicitly
    bounded as a design input, not implementation. Coverage matches the PRD
    Phase 1 row and M03-AC01: legacy v1/v2 + absent/invalid (C1), backend
    callers (C4), metadata read safety (C2/C3), two-client inventory (C5),
    independent oracles (C5a.2/C5b.5), backend capability inputs (C2.3/C2.4/
    C4.4). Nothing material is under-scoped.

## Findings and required fixes

### F1 (material) — C5b mischaracterizes the signal-monitor example's source semantics

The plan's C5b object is `examples/signal_monitor.py`, but C5b item 2 asserts
"the consumptive source semantics (each read consumes one queued value, per
the M02 `_MonitorSource` double) make input re-declaration a required rerun
input". That is **not true of the client under inventory**: in
`examples/signal_monitor.py` the readers are `read(channel, offset)` returning
`state['tick'] + offset` (signal_monitor.py:25-27) — deterministic, tick-derived
and **non-consumptive**; no queued value is ever consumed, and re-declaring
consumed inputs is not required for a clean-session rerun. The consumptive
`pop(0)` semantics belong to `_MonitorSource`
(tests/test_m02_w01_characterization.py:491-522), a test double used only by
the test-local `run_monitor` client defined in that same test file (L545+) —
a *different* client from the example. Correspondingly, the C5b item 5 oracle
values (`[10, 13, 17]` / `[1, 2, 4]` → `[9, 11, 13]`) are the M02 double's
values; for the actual example with `points=3` the stored rows are
signal `[6, 7, 8]`, reference `[3, 4, 5]`, differences `[3, 3, 3]`.

Because characterization asserts actual behavior of the unchanged baseline, a
plan that instructs the tester to record a false pre-asserted fact would
produce false evidence and misstate rerun inputs in the Phase 1 scope decision.

**Required fix:** rewrite C5b items 2 and 5 to characterize
`examples/signal_monitor.py` as written: reads are `tick + offset` per channel
with the offsets (5 signal, 2 reference) owned by the example; enumerate rerun
inputs as `points`, the read-policy offsets, and scheduler/loop ownership
(`run_count` owns a fresh `SerialScheduler`, softlab/huo/process/bounded.py:142);
and define the oracle against this client's actual values (or explicitly mark
all sample values as illustrative pending client selection). Reference the M02
`_MonitorSource` consumptive semantics only as test-double context, clearly
attributed, never as a property of the example client.

### F2 (moderate) — Wrong SQLite backend class name; incomplete C4 caller inventory

The plan twice names `SQLiteDataBackend` (C2.4, C4); the actual class is
`Sqlite3DataBackend` (softlab/shui/data/sqlite3.py:25, exported from
`softlab.shui.data`). The C4 static inventory also omits two real caller
groups: `softlab/shui/data/io.py` (`get_data_backend`, `load_groups`,
`reload_group`, `save_group` — production entry points that instantiate **both**
backends) and `tests/test_compatibility.py` (exercises both backends). C4.4
promises a "complete ownership map" for the persistence-boundary decision; the
omissions defeat that.

**Required fix:** correct the class name to `Sqlite3DataBackend` everywhere;
add `softlab/shui/data/io.py` and `tests/test_compatibility.py` to the C4.1
checklist (and record what io.py's `reload_group`/`save_group` do with backend
info, since that path depends on `group.backend` being populated).

### F3 (minor) — C1.5: execution UUID is not reachable via `RunResult`; name the actual path

The plan says the in-memory bounded-execution identity is available "via the
M02 shared seam / `RunResult`". `RunResult` (thermal_experiment.py:56-85) has
**no** run-identity field: `map_count`/`terminal` (L269-287) copy only rows,
boundary, acquisition, cause and cleanup fields from the `CountResult`. The
execution UUID (`CountResult.run_id`, bounded.py:38) **is** reachable today,
without source changes, via the `after_row` callback argument of the public
`run_count` seam (`after_count(snapshot)` receives the `CountResult`,
thermal_experiment.py:298; bounded.py:162-163) or via harness call
instrumentation. As written, the probe instruction points at a dead end.

**Required fix:** reword C1.5 to name the reachable observation path
(`after_row` callback's `CountResult.run_id`, or recorded call arguments), and
to record verbatim that `RunResult` itself does not retain the execution UUID.

### F4 (minor) — C2.3/C2.4 do not state which load path is exercised, and the paths diverge observably

"Save ... reopen and record what comes back" is ambiguous between two load
paths with very different observed behavior: a plain
`HDF5DataBackend.load_group` returns a group whose `meta` is **empty** (the
live attrs proxy passed to `DataGroup(..., meta)` is dropped by R4-BASE-META-01,
compounded by the reserved-attr pops of R4-BASE-HDF5-01), whereas the thermal
example's `_load_group`/`_reload_group` re-hydrates meta from captured attrs.
Characterizing "the" round-trip without naming the path would yield
unreproducible, misattributed evidence. Same ambiguity applies to C2.4's
reload-content evidence.

**Required fix:** state explicitly that C2.3/C2.4 exercise **both** the plain
backend load path and the example hydration path, recording each verbatim
(including the empty-meta outcome of the plain path, which is itself
characterization evidence for the defect disposition).

## Disposition

- F1–F4 are plan-text corrections only; the C1–C6 structure, verdicts
  protocol, blocker protocol, safety/hygiene rules and gate linkage are
  approved as designed.
- Re-review: after the Tester updates the plan, a focused re-check of the four
  findings suffices; the full review need not be repeated.
- No executable test may be written until the corrected plan passes this
  developer re-review and the subsequent architect/design review per the gate
  order.
