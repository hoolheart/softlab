# Phase 4 Walkthrough Test Plan — Developer Review (implementability)

Reviewer: sw-tom (Developer) | 2026-11-04 Asia/Shanghai
Branch: `codex/m03-p4-acceptance`
Input: [log/release_12/test/walkthrough-test-plan.md](../test/walkthrough-test-plan.md)
(commit `2864063`) against the **FROZEN** oracle design
([p4-oracle-design.md](../design/p4-oracle-design.md) at `63ed461`),
[prd.md](../prd.md) M03-AC05/AC06, and the current source:
`examples/thermal_experiment.py` (`execute` seam), `examples/signal_monitor.py`,
`softlab/shui/run_storage.py`, `softlab/shui/run_record.py`,
`softlab/shui/run_group.py`, `tests/run_record_inspect_bootstrap.py`.

Method: source-level inspection only — no test code written, no source
modified.

## Verdict

**CHANGES REQUESTED** — the plan is executable in substance: W1/W2 map
cleanly onto the existing `execute` seam and the Phase 3 bootstrap; the
DEP-1 dependency list is complete **and** minimal against freeze §5.1
(nothing demanded that the freeze does not require); W4's fresh-environment
procedure is executable in this repo with honestly-recorded limitations;
W6's debt references all exist at the cited sources. All four findings are
**plan-text clarifications only** — no re-planning, no oracle, tolerance,
exclusion or scope change. W1, W2, W4–W7 can proceed as written once the
text closes findings 1–3.

## Quote-check of frozen literals (sampled, not exhaustive)

Verified verbatim against the freeze at `63ed461`, no deviations:

- §2.1/§2.2 column values, timing relations (`tick-1` / `tick`, delay 1/0,
  manifest version 1/2), and row/boundary literals — match.
- §2.3 signal oracle: declared order `(timestamp, signal, reference)`,
  `[6,7,8]` / `[3,4,5]` / caller-derived `[3,3,3]`, read trace
  `[('signal',1),('reference',1),...,('signal',3),('reference',3)]` — match.
- §3 tolerances: exact `==` for integers/`sim_time_s`/`commanded_power_w`;
  `math.isclose(rel_tol=0.0, abs_tol=1e-10)` for `temperature_degc`;
  "nothing looser is permitted" — match.
- §4 exclusions and every "separate assertion retained" — match, including
  per-run (not cross-run) application.
- Warning baselines (freeze §6 item 4): four pyparsing deprecations on 3.9,
  pandas `pd.concat` FutureWarning on 3.9/pandas 2.x, sqlite3 `version`
  DeprecationWarning on ≥ 3.13 — match the bootstrap
  `install_warning_policy` literals in
  `tests/run_record_inspect_bootstrap.py:38-66`.

## Positive confirmations (implementability)

- **DEP-2 matches the real seam.** `execute(config, output_dir, *,
  stop=None, after_row=None, adapter='coordinated')` at
  `examples/thermal_experiment.py:840-843`; no `delay` parameter; adapter
  is the only call-side timing selector; `output_dir` must be empty or
  nonexistent (`_prepare_output`), which `TemporaryDirectory` roots satisfy.
- **DEP-4 matches the real bootstrap.** CLI form
  `python tests/run_record_inspect_bootstrap.py <path> --profile
  {thermal|signal} [--group-id <uuid>]`, exit 0 + single JSON payload on
  stdout, `original_sha256` included in the payload — all present at
  `tests/run_record_inspect_bootstrap.py:107-148`. `SIGNAL_HYDRATION_KEYS`
  is the frozen empty tuple (`run_storage.py:98`), so `--profile signal`
  needs no legacy-key hydration from the tester.
- **DEP-1 is complete and minimal against freeze §5.1.** All six required
  inputs are listed and nothing more: (1) acquisition-success gate; (2)
  `execution_id = str(result.run_id)` + fresh distinct `group_id`; (3)
  `build_run_group(name, envelope)` (`run_group.py:28`) + public
  `DataGroup.add_record(record)` before `save_run_group`; (4)
  `build_signal_payload(points=3, offsets={'signal': 5, 'reference': 2})`
  (`run_record.py:1080`, keyword-only — the plan's call form is compatible)
  and `build_envelope(...)` with declared columns in declaration order
  (`_SIGNAL_COLUMNS` at `run_record.py:206-210` is the frozen ordered
  signal schema); (5) signal-owned `software`/`source` facts + explicit
  HDF5 target under the temp root; (6) asserted-success `StorageReport` +
  owned-resource disconnect. **Nothing in DEP-1 requires touching legacy
  behavior:** thermal's `_source_facts` is private to `thermal_experiment`;
  signal wiring can replicate the capture shape independently, and
  `signal_monitor.acquire`'s existing return tuple
  `(CountResult, DataRecord, differences, reads)` already carries every
  acquisition-time fact the wiring and W3 need.
- **W1/W2 mechanics are executable.** sha256 before/after preservation is
  computable by the driver over the artifact files; by-row-index A-vs-B
  comparison under §3 is implementable by decoding the envelope from the
  bootstrap JSON (`envelope` field) and retrieving same-named stored
  datasets from the `records` payload — disk enumeration order never used.
- **W6 debt references exist.** PRD "Limits and debt" (`prd.md:73-74`);
  R4-BASE-SCHED-01/F-01/F-02, OBS-004/005/006, DEFECT-2 dispositions in
  `reviews/p1-product-assessment.md:179-188`; D1/D2 and F-A/F-B/F-C/F-D in
  `design/p1-boundary-decision.md:196-199`. No dangling references.
- **Version portability discipline is sound.** The plan never asserts
  characterization-era version values as expectations, gates warning
  baselines on versions actually observed in W4, and defers to W1–W3
  tolerances rather than cross-version exact reprs (consistent with freeze
  §8 item 3).

## Findings

### Required fixes (all plan-text only)

1. **[Medium] W3 step 7's read trace and non-consumptive-read assertions
   have no stated evidence source.** The per-tick read trace
   `[('signal',1),('reference',1),...]` and non-consumptive reads are
   **acquisition-time facts**; nothing in the stored artifact (and
   therefore nothing in the bootstrap JSON payload) can evidence them —
   `inspect_run_group` sees only stored columns. As written, a tester could
   attempt to assert these against storage and either fail or improvise.
   Fix: one clarifying sentence in W3 step 2 (or step 7) stating that the
   read trace and non-consumptive evidence are captured from each session
   driver's `acquire()` return (`reads` list, `record` reuse across ticks)
   and recorded verbatim in the results report alongside the stored-path
   assertions; the bootstrap JSON remains the source for all stored-column
   and envelope assertions. No new seam, no oracle change — C5b already
   established this in-memory evidence.

2. **[Low] W1/W2/W3 do not pin the subprocess invocation form.** "Fresh
   Python process; run `execute(...)`" is satisfiable two ways: the example
   CLI (`python examples/thermal_experiment.py --output-dir ... --adapter
   ...`, which routes through `execute` and satisfies DEP-2's signature
   probe) or a small driver script/`python -c` importing
   `examples.thermal_experiment` from the repo root (works via namespace
   package; `execute` requires an empty/nonexistent output dir, which temp
   roots satisfy). Both are legitimate; the plan should state that either
   form is acceptable, that the exact command line is recorded verbatim in
   the results report, and that for W3 the driver must invoke
   `acquire(points=3)` **plus** the DEP-1 wiring in the same process (the
   wiring is not yet a module entry point — it is Phase 4 code, and DEP-1
   correctly asserts against it rather than assuming one).

3. **[Low] Hygiene does not restate the no-autospec discipline.** The
   walkthrough should need **no mocks at all** — every assertion is
   real-seam (subprocess execution, stored bytes, bootstrap JSON). State
   this explicitly in Hygiene, carrying the Phase 1–3 rule forward: no
   `mock.patch` with `autospec=True` combined with `wraps` (CPython
   gh-75988 on Python 3.9), and if any recording double is ever introduced
   for diagnosis, it must delegate without skipping the real code path.
   This keeps W5's "no shared in-memory state" evidence clean and prevents
   a future edit from quietly reintroducing the 3.9 trap.

### Recorded as limitations at execution (not blockers, no plan change)

4. **[Info] W4 environment risks to record honestly in the results
   report**, per the plan's own item 6: (a) `python -m pip install -e .`
   in a fresh venv requires network/package-index reachability (build
   isolation pulls setuptools/wheel; the declared dependency set —
   numpy/scipy/pandas/matplotlib/plotly/ipywidgets/pyvisa/h5py — resolves
   from the index, several minutes of wheel downloads); (b) a Python 3.9
   interpreter may not exist on the walkthrough host — absence is a
   recorded limitation, and single-interpreter execution with the observed
   version table is acceptable because the plan never mandates a 3.9
   run, only version-portable assertions; (c) the pip-resolved dependency
   set and index reachability should be captured verbatim alongside the
   install output. The plan's existing "recorded blocker/limitation, never
   a pass by falling back" rule covers all three; this item only asks that
   the results template's Limitations section explicitly name them.

## Closing

Findings 1–3 are one-paragraph plan-text edits; finding 4 is an
execution-time recording instruction. None touches the frozen oracle,
tolerances, exclusions, dependency gates or scenario structure. After the
tester applies the three text fixes, this plan is approved for execution
without a further review round (delta check by sw-prod per the
evidence-reuse rule is sufficient).
