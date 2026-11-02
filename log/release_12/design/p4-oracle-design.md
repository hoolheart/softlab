# Release 12 (M03 Phase 4) — Oracle / rerun design freeze

Owner: Software Designer (sw-celeste) | 2026-11-03 Asia/Shanghai
Branch: `codex/m03-p4-acceptance`
Status: **FROZEN — binding input to Phase 4 test planning and implementation;
not test approval, not implementation, not acceptance**

Binding inputs: [p1-boundary-decision.md](p1-boundary-decision.md) (incl.
ordering addendum), [characterization-results.md](../test/characterization-results.md)
(C1, C3, C5a, C5b), [p3-storage-design.md](p3-storage-design.md) §3.5.1 and
§4, [PRD](../prd.md) M03-AC05/AC06. This document freezes the independent
expected sample/timing relationships, tolerances, excluded fields, and the
separate-process rerun procedure **before** Phase 4 implementation, as
required by `plan/milestone-03.md`. Nothing here is self-comparison: every
oracle below is derived from the client's own declared recurrence/read
semantics, independent of any stored object.

## 1. Reference scenario inputs (frozen declarations)

| Scenario | Frozen inputs | Oracle binds to |
| --- | --- | --- |
| Thermal v1 coordinated | `ThermalConfig()` defaults: `initial_temperature=20.0`, `ambient_temperature=20.0`, `heat_capacity=10.0`, `conductance=1.0`, `dt=1.0`, `power=10.0`, `points=8`; adapter `'coordinated'` (delay 1, `source_tick='tick-1'`) | The declared Euler recurrence `T += dt*(power - conductance*(T - ambient))/heat_capacity`, sampled **before** the step (delay=1), exactly as re-derived in characterization C5a. |
| Thermal v2 direct | Same `ThermalConfig()` defaults; adapter `'direct'` (delay 0, `source_tick='tick'`) | Same recurrence, sampled **after** the step (delay=0). |
| Signal monitor | `points=3`; read-policy offsets `{'signal': 5, 'reference': 2}` (frozen `SIGNAL_OFFSETS`; `signal_monitor.py:31/33`); non-consumptive `tick + offset` reads | The declared read semantics `read = tick + offset` per tick 1..3, and `difference = signal - reference` (caller-derived, never a stored column). |

## 2. Oracle tables (frozen expected values)

All oracles compare **per declared column, in envelope order, by row index**
(p3-storage-design §3.5.1 rule 5): disk enumeration order is non-semantic and
is never used as declaration order.

### 2.1 Thermal v1 coordinated — 8 rows, six declared columns

Declared order (already lexicographic): `commanded_power_w`,
`measurement_source_tick`, `sim_time_s`, `temperature_degc`, `tick`,
`timestamp`.

| Column | Frozen expected values (rows 1..8) |
| --- | --- |
| `tick` | `[1, 2, 3, 4, 5, 6, 7, 8]` |
| `measurement_source_tick` | `[0, 1, 2, 3, 4, 5, 6, 7]` (= `tick - 1`) |
| `sim_time_s` | `[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]` (= `tick * dt`) |
| `commanded_power_w` | `[10.0] * 8` (constant `power`) |
| `temperature_degc` | `[20.0, 21.0, 21.9, 22.709999999999997, 23.438999999999997, 24.095099999999995, 24.685589999999994, 25.217030999999995]` (IEEE-754 double reprs of the recurrence under the §1 inputs; sampled pre-step) |
| `timestamp` | **excluded** (§4) |

Timing relations asserted independently of the temperature oracle:
`measurement_source_tick == tick - 1`; `sim_time_s == tick * 1.0`;
`completed_rows == requested_rows == final_tick == 8`; `final_sim_time == 8.0`;
manifest `version == 1`, `clock.measurement_delay_ticks == 1`,
`clock.source_tick == 'tick-1'`.

### 2.2 Thermal v2 direct — 8 rows, same six declared columns

| Column | Frozen expected values (rows 1..8) |
| --- | --- |
| `tick` | `[1, 2, 3, 4, 5, 6, 7, 8]` |
| `measurement_source_tick` | `[1, 2, 3, 4, 5, 6, 7, 8]` (= `tick - 0`) |
| `sim_time_s` | `[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]` |
| `commanded_power_w` | `[10.0] * 8` |
| `temperature_degc` | `[21.0, 21.9, 22.709999999999997, 23.438999999999997, 24.095099999999995, 24.685589999999994, 25.217030999999995, 25.695327899999995]` (sampled post-step) |
| `timestamp` | **excluded** (§4) |

Timing relations: `measurement_source_tick == tick`; row/boundary counts as
v1; manifest `version == 2`, `clock.measurement_delay_ticks == 0`,
`clock.source_tick == 'tick'`.

### 2.3 Signal monitor — 3 rows, declared order `(timestamp, signal, reference)`

Note: this declared order is **not** lexicographic; by-name reconstruction in
envelope order (§3.5.1) is mandatory — no backend repair is introduced.

| Column | Frozen expected values (rows 1..3) |
| --- | --- |
| `signal` | `[6, 7, 8]` (= `tick + 5`) |
| `reference` | `[3, 4, 5]` (= `tick + 2`) |
| derived `difference` | `[3, 3, 3]` (= `signal - reference`, recomputed by the caller from stored source samples; never read from storage) |
| `timestamp` | **excluded** (§4) |

Timing/structural relations: per-tick read trace
`[('signal', 1), ('reference', 1), ('signal', 2), ('reference', 2),
('signal', 3), ('reference', 3)]`; reads are non-consumptive;
`completed_rows == requested_rows == 3`; `difference` is absent from stored
columns.

## 3. Frozen tolerances

| Value class | Method | Frozen tolerance | Justification |
| --- | --- | --- | --- |
| Integer-valued columns (`tick`, `measurement_source_tick`, signal `signal`/`reference`/`difference`) | exact equality | `==` | Deterministic integer arithmetic (`tick ± delay`, `tick + offset`); characterization C5a/C5b observed exact matches. |
| `sim_time_s`, `commanded_power_w` | exact equality | `==` | With frozen `dt=1.0`, `power=10.0`, products `tick*1.0` and `np.full(n, 10.0)` are exact IEEE doubles; C5a observed exact match for both adapters. |
| Thermal `temperature_degc` | element-wise closeness | `math.isclose(actual, expected, rel_tol=0.0, abs_tol=1e-10)` | Matches the evidenced tolerance (`atol=1e-10`, C5a; same bound used by `_validate_manifest_data`). IEEE-754 doubles make same-input reruns deterministic, so exactness would be defensible; the frozen `1e-10` absolute bound is retained because it is the **characterized** evidence and isolates the comparison from any interpreter/library float-printing drift without weakening detection of recurrence errors (one Euler step here shifts the value by ≥ 0.47). `rel_tol=0.0` is frozen: relative error must not mask absolute drift near zero. |
| Identity / structural facts (UUID equality to `DataGroup.id`, group-name/`id`-attr equality, column definitions incl. units, row counts, version/delay literals) | exact equality | `==` | Structural contract, no numeric noise. |

Rationale summary: deterministic synthetic doubles justify exactness wherever
characterization observed it; the single non-exact comparison
(`temperature_degc`, `abs_tol=1e-10`) is exactly the evidenced oracle bound
and nothing looser is permitted.

## 4. Exclusions (never compared in rerun)

| Excluded item | Why | Separate assertion retained |
| --- | --- | --- |
| `timestamp` column and all wall-clock fields (`clock.wall_timestamp` value content, `software.python_version` micro value, file mtimes) | Wall-clock differs between sessions by construction; C5b verified only timestamps differ between two fresh `acquire(points=3)` calls. | The timestamp **column exists** with declared unit `'s'` and has `completed_rows` finite values (presence/shape checked, values not). |
| `execution_id` vs `group_id` values | Both are fresh UUIDs per run. | **Distinctness** asserted: `execution_id != group_id`; `group_id == str(DataGroup.id) == HDF5 group name == on-disk `id` attr`; `execution_id` appears in no stored UUID slot (C1.5/C5a, decision §1.1). |
| `source.digest`, `source.git_revision`, `source.git_dirty` values | Session/worktree-dependent. | Schema/state checked: each is `{state: 'available', value: <valid digest/hex+flag>}` or `{state: 'unavailable', reason: <bounded code>}`; never compared across runs. |
| Environment facts (`software.softlab_version`/`python_version` values, observed package versions) | May legitimately differ between the recording and rerun sessions. | Recorded verbatim in the walkthrough report (§6); structural presence required, value equality not asserted. |
| `thermal.png` plot bytes | Rendering is not bit-stable across matplotlib versions. | Existence of the artifact only. |
| Original-file byte hash vs rerun-file byte hash | Timestamps/UUIDs embedded in the file differ. | Per-run original-byte preservation (sha256 before/after inspection, C3 discipline) **is** asserted within each run. |

## 5. Clean-session rerun procedure (frozen steps and pass criteria)

Applies per scenario in §1. Every step runs in synthetic
`tempfile.TemporaryDirectory(prefix='softlab-m03-p4-*')` roots; no fixture
touches the repository tree or real instruments.

1. **Declare inputs** in the test/driver source: the §1 table row (config
   values or `points`/offsets), the oracle table from §2, the §3 tolerances,
   the §4 exclusions. Declarations precede any execution.
2. **Rerun execution (process A):** fresh Python process; run the client:
   thermal → `examples/thermal_experiment.execute(config, output_dir,
   delay=1|0)`; signal → `examples/signal_monitor.acquire(points=3)` then
   construct and persist its owned record via `build_signal_payload` /
   `build_envelope` / `save_run_group` (p3-storage-design §2.2 write path).
   Assert `result.overall_success` (thermal) / save `StorageReport` success
   (signal); disconnect owned resources.
3. **Record save verification:** the produced `*.h5` exists under the temp
   root; record sha256 of the original.
4. **Separate-process inspection (process B):** run
   `python tests/run_record_inspect_bootstrap.py <path> --profile {thermal|signal}`
   (the Phase 3 bootstrap around `softlab.shui.run_storage.inspect_run_group`;
   warnings fatal except the approved recorded baselines). Parse the JSON
   payload from stdout; exit code must be 0.
5. **Oracle comparison (process A, on B's payload plus its own in-memory
   expectations):** decode the envelope; per §3.5.1 rule 5, iterate the
   envelope's declared column sequence, retrieve each same-named stored
   dataset/column, and compare sample arrays **by row index** against §2
   using §3 tolerances; check all §2 timing/structural relations and §4
   retained assertions; re-verify original sha256 unchanged.

**Pass criteria (all required; no partial pass):**

- Process B exit 0 with a well-formed payload; no warning other than the
  approved recorded baselines (§6).
- Every §2 column matches within its §3 method/tolerance; all §2 timing and
  structural relations hold; `completed_rows == requested_rows` and equals
  the stored row count.
- All §4 "separate assertion retained" checks pass (UUID distinctness,
  source-availability states, timestamp presence/shape, original-byte
  preservation).
- Two sequential inspections of the same original each succeed on **fresh
  copies** (F-A guard, p3-storage-design §3.6) with the original sha256
  unchanged throughout.

## 6. Fresh-environment walkthrough requirements (AC06)

The Phase 4 walkthrough report **must** record, with verbatim evidence:

1. Exact commands actually run (from repo root): environment activation,
   `python -m unittest discover -s tests -p 'test_*.py'`,
   `python -m compileall -q softlab`, the import smoke check
   (`python -c "import softlab; print(softlab.__version__)"`), and the §5
   rerun/inspection commands per scenario.
2. Interpreter and dependency versions **actually observed** in the
   walkthrough environment (Python, softlab, numpy, pandas, h5py/libhdf5,
   matplotlib, sqlite3 where read), in the same style as the
   characterization-results execution record. The characterization-era values
   (Python 3.13.15, softlab 0.3.0, numpy 2.5.3, pandas 3.0.6, h5py 3.16.0 /
   libhdf5 2.2.0) are historical context, **not** presumed walkthrough
   values; the walkthrough records its own.
3. Every warning observed, verbatim, categorized as product / environment /
   CI-reporting artifact. Known approved baselines (bootstrap
   `install_warning_policy`): the four pyparsing deprecations on Python 3.9,
   the pandas `pd.concat` FutureWarning on Python 3.9/pandas 2.x, and the
   sqlite3 `version` DeprecationWarning on Python ≥ 3.13. **Recorded, never
   waived or suppressed**; any new warning fails the walkthrough until
   dispositioned.
4. Limitations actually encountered (missing optional extras, unavailable
   interpreters) recorded as limitations, not silently scoped away.

## 7. Traceability

| Frozen item | Evidence source |
| --- | --- |
| Thermal Euler recurrence, `atol=1e-10` temperature bound, exact `tick`/`measurement_source_tick`/`sim_time_s`/`commanded_power_w` | characterization-results **C5a** ("Stored samples validate against the independent Euler oracle ... within `atol=1e-10` ... match exactly"); re-derived recurrence equals M02 `ThermalPolicyComparison.euler`; comparison method mirrors `thermal_experiment._validate_manifest_data`. |
| v1/v2 timing literals (delay 1/0, `tick-1`/`tick`), six-column schema, 8-row counts, `final_sim_time == 8.0` | characterization-results **C1.4, C3** (on-disk manifest parse); `thermal_experiment._SCHEMA`, `_timing`, `_make_manifest`. |
| Signal `[3,3,3]` differences, per-channel `[6,7,8]`/`[3,4,5]`, offsets 5/2, non-consumptive `tick+offset`, `points=3`, read trace | characterization-results **C5b**; `examples/signal_monitor.py:31/33/39`. |
| By-name reconstruction in envelope order; row-index sample comparison; signal declared order non-lexicographic | p3-storage-design **§3.5.1** (esp. rule 5) and boundary-decision **ordering addendum**. |
| Separate-process bootstrap inspection; fresh-copy repeated inspection; original-byte sha256 preservation | p3-storage-design **§3.6, §4**; `tests/run_record_inspect_bootstrap.py`; characterization-results **C3, C6**. |
| Execution-vs-group UUID distinctness; `execution_id` in no stored UUID slot | characterization-results **C1.5/C5a**; boundary-decision §1.1 execution-identity row. |
| Acquisition-only outcome snapshot; later stages `not_yet_established` | characterization-results **C1.4**; boundary-decision §1.1 outcome row. |
| HDF5-only scope; exclusion of SQLite/io-generic paths | boundary-decision §2, §4 (D2, F-C, F-D dispositions). |
| Tolerances/exclusions/rerun procedure requirement itself | `plan/milestone-03.md` Phase-4 sentence; PRD **M03-AC05/AC06**. |

## 8. Evidence gaps flagged

1. **Walkthrough not yet executed.** §6 versions/warnings are requirements,
   not observations; the actual fresh-environment evidence is a Phase 4
   deliverable still to be produced (per AC06 this is expected at this gate).
2. **Signal persistence oracle is design-frozen ahead of its first executed
   integration run.** Its sample values (`[6,7,8]`/`[3,4,5]`/`[3,3,3]`) are
   characterized in-memory evidence (C5b); the stored-path round-trip of
   those exact values through `save_run_group`/`inspect_run_group` was
   designed in Phase 3 but its **executed** confirmation is a Phase 4 test
   deliverable. If Phase 4 execution observes any deviation, this freeze is
   reopened through review — never silently edited.
3. **Python 3.9 float determinism assumption.** The §2.1/§2.2 temperature
   reprs were computed on Python 3.13.15. IEEE-754 double arithmetic makes
   the recurrence interpreter-independent, and the `abs_tol=1e-10` bound
   absorbs any residual doubt; cross-version exact-repr equality is **not**
   asserted (only the tolerance is), consistent with §3.

## Design review

- **Reviewer**: sw-jerry (architecture), sw-camille (product, AC05/AC06 scope)
- **Review date**: pending
- **Status**: PENDING
