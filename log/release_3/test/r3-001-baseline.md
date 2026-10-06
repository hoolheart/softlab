# R3-001 characterization baseline — current behavior of the Release 2 gaps

Tester: sw-mike | Date: 2026-10-06
Branch: `codex/r3-001-corrections` (from `dev` @ `2dee90e`, tip `6ffc090`)
Recorded BEFORE any R3-001 test files or production changes exist on this
branch (`git status --short` clean except the two `log/release_3/test/`
records of this step). No production code was modified; reproduction
scripts were run from a temp directory outside the repository.

## Environment

| Item | Value |
| --- | --- |
| Python | 3.13.15 (`$PWD/.venv`, conda env; invoked as `$PWD/.venv/bin/python` — `conda activate` is unavailable in this non-interactive shell, an environment limit, not a project defect) |
| softlab version | `0.3.0` |
| Platform | macOS (darwin) |
| Real instruments | none (N/A per task); all fixtures synthetic |

## 1. Full regression suite (fresh baseline)

Command:

```
$PWD/.venv/bin/python -m unittest discover -s tests -p 'test_*.py'
```

Output (verbatim, tail):

```
.......................................................................................................................................
----------------------------------------------------------------------
Ran 135 tests in 0.498s

OK
Connected to sqlite3 backend, data is stored in /var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/tmp0fz4gk7w/readings.db.
Connected to HDF5 backend, data is stored in /var/folders/08/wwyghn9s1lq_1q33lyg5c8th0000gn/T/tmp0fz4gk7w/readings.hdf5.
```

- Test count: **135** (Release 1: 99; Release 2 SIM-001 added 36; this
  matches the Release 2 acceptance record's 135 = 99 + 36 arithmetic).
- Result: **OK**, exit code **0**.
- Warnings: **zero** — a re-run under `python -W default -m unittest
  discover ...` printed no warning lines (grep count 0).
- Warning-gate style check `-W error::Warning` was NOT re-run for this
  baseline (the Release 2 results record it green; re-running it is
  R3-TC-08b at execution time). Recorded as unverified-here, not waived.
- Import smoke with `warnings.simplefilter('always')`:
  `import softlab, softlab.tu` → clean, `0.3.0`, zero warnings.
- `python -m compileall -q softlab` → exit 0, no output.

The two backend lines are informational prints from data-backend tests,
not warnings.

## 2. Defect area A — standalone bridge readback (CONFIRMED)

Documented pattern under test: user guide §9 (control parameter wired
via `before_set=lambda old, new: sim.set_input('u', new)`). Guide line
243 claims: "Parameter read-back via `get()` returns the stored value,
which equals the object's current (pending) input." The parameter's own
store is written at set time only; nothing re-reads the object on get.

Reproduction (temp script, accumulator `x' = x + u`, `ValNumber()`
validator, exact commands and verbatim output):

```
== (a1) direct input change vs parameter read-back ==
after drive(1.0):        param read-back = 1.0 | object input = 1.0
after obj.set_input(u,5): param read-back = 1.0 | object input = 5.0
== (a2) read-back after reset ==
after reset:             param read-back = 3.0 | object input = 0.0
== (a3) write through a second controller ==
after drive2(2.0):       drive1 read-back = 1.0 | drive2 read-back = 2.0 | object input = 2.0
== (a4) rejected set keeps parameter and object consistent ==
drive("not-a-number") raised TypeError - (<class 'int'>, <class 'float'>, <class 'numpy.integer'>, <class 'numpy.floating'>) value required but <class 'str'> in <class 'softlab.tu.station.parameter.Parameter'>/drive
after rejected set:      param read-back = 1.0 | object input = 1.0
```

Findings:

- **A-1 (defect, matches PRD gap 1):** after a *direct* `obj.set_input`,
  the control parameter's read-back is **stale** (1.0 vs authoritative
  5.0).
- **A-2 (defect):** after `reset()`, read-back is stale (3.0 vs
  restored 0.0) — reset makes the divergence most visible.
- **A-3 (defect):** after a *second controller* writes through the same
  object, the first controller's read-back is stale (1.0 vs 2.0).
- **A-4 (works as documented):** a validator-rejected set never reaches
  the bridge; parameter and object stay consistent. This behavior must
  survive the fix (pinned as R3-TC-07d).
- The only existing test touching this, `tests/test_tu_simulation_integration.py`
  SIM-TC-06a (line 99: `assertEqual(drive(), 1.0)  # read-back
  contract`), asserts read-back immediately after the set and therefore
  cannot detect the staleness. **No existing test** exercises read-back
  after direct input changes, reset, or a second controller.

## 3. Defect area B — malformed callback keys (CONFIRMED, narrow)

Code path: `softlab/tu/simulation/object.py` `_validate_callback_result`,
lines 775–776 — `missing = sorted(expected - actual)` /
`extra = sorted(actual - expected)`. Sorting a mixed-type set raises an
incidental `TypeError`. Documented contract (guide error table §6):
missing/extra keys → `ValueError` naming the discrepancy; non-mapping →
`TypeError`.

Verbatim reproduction results:

```
== (b1) extra key, pure string -> documented ValueError ==
extra str key      : evolve raised ValueError: The evolve callback must return exactly the declared state names; missing [], extra ['z']
== (b2) extra key, non-string (int) -> ?
extra int key      : evolve raised ValueError: The evolve callback must return exactly the declared state names; missing [], extra [2]
== (b3) extra key, mixed str+int -> ?
mixed str+int keys : evolve raised TypeError: '<' not supported between instances of 'int' and 'str'
== (b4) extra key, None -> ?
extra None key     : evolve raised ValueError: The evolve callback must return exactly the declared state names; missing [], extra [None]
== (b5) extra key, tuple -> ?
extra tuple key    : evolve raised ValueError: The evolve callback must return exactly the declared state names; missing [], extra [('a',)]
== (b6) observe extra non-string key -> ?
observe extra int  : observe raised ValueError: The observe callback must return exactly the declared output names; missing [], extra [7]
== (b7) missing key, pure string -> documented ValueError ==
missing str key    : evolve raised ValueError: The evolve callback must return exactly the declared state names; missing ['x'], extra []
== (b8) non-mapping evolve result -> documented TypeError ==
non-mapping evolve : evolve raised TypeError: The evolve callback must return a mapping, got list
== (b9) non-mapping observe result -> documented TypeError ==
non-mapping observe: observe raised TypeError: The observe callback must return a mapping, got int
```

Findings:

- **B-1 (defect, matches PRD gap 2 exactly):** the failure requires
  **mixed string and non-string keys in the same discrepancy set**
  (b3). Single non-string keys (b2, b4, b5, b6) do not compare during
  `sorted` and correctly produce `ValueError`. Missing keys alone can
  never trigger it (expected names are all strings).
- **B-2 (coverage gap):** `grep -rn "must return exactly" tests/`
  returns nothing — **no existing test** covers missing/extra/mixed
  callback keys or non-mapping results for either callback. The defect
  is entirely unguarded.
- **B-3 (behavior to preserve):** non-mapping results raise `TypeError`
  (b8/b9); single non-str extra keys raise named `ValueError` (b2/b6).
  Pinned by R3-TC-07g/07h/07i.

## 4. Defect area C — reset claims (CONFIRMED as documentation overclaim)

Claim sites verified by direct read:

- `softlab/tu/simulation/object.py:660` — "On success the object is
  behaviorally identical to a newly constructed one, including any time
  inputs/states."
- `softlab/tu/simulation/object.py:666–669` — a failed reset leaves
  "inputs, states and therefore all subsequent observations … exactly
  as before the call".
- `docs/user-guide/simulation.md:132` — "On success the object is
  behaviorally identical to a newly constructed one."
- `arch.md:279` — "making the object behaviorally identical to a newly
  constructed one."

Verbatim reproduction results:

```
== (c1) reproducible factory + deterministic callbacks ==
after reset: x = 0.0 | y = 0.0
== (c2) factory with external state (call counter) ==
construction:        u = 1.0
after 1st reset:     u = 2.0
after 2nd reset:     u = 3.0
fresh construction:  u = 4.0 (differs from first construction value 1.0 and from resets)
== (c3) failed reset: owned stores preserved, but "identical future observations" does not hold under stateful observe ==
observation #1: 1.0
reset raised identically: RuntimeError('reset boom')
stores preserved: u = 0.0 | x = 0.0
next observation (stateful G): 2.0 != previous observation 1.0
```

(Findings c1–c3 were produced by a script; area c4's demonstration raised
the injected `RuntimeError` uncaught at the intended point — the
verbatim traceback is omitted here for brevity but the observable facts
are: the factory's external side effect — an append to a caller-visible
list — was visible after the failure, and the exception propagated with
its original identity.)

Findings:

- **C-1 (overclaim, matches PRD gap 3):** with a factory holding
  external state, each `reset()` returns a *different* value (1.0 → 2.0
  → 3.0), and none of these equals what a fresh construction at that
  moment would produce (4.0). "Behaviorally identical to a newly
  constructed one" is false in this documented-legal configuration.
- **C-2 (overclaim):** after a failed reset the *owned stores* are
  indeed preserved (u=0.0, x=0.0 — the build-then-swap mechanism works
  as designed), but "all subsequent observations are exactly as before"
  is false whenever `observe` itself carries external state (next
  observation 2.0 ≠ 1.0). The guarantee can only cover library-owned
  inputs/states.
- **C-3 (behavior to preserve):** exception identity on failed reset
  (`RuntimeError('reset boom')` propagated identically), store
  preservation after failed reset, and full reproducibility under
  deterministic factories/callbacks (c1) are all correct today —
  existing SIM-TC-04a/05i pin these and must keep passing (R3-TC-07k).

## 5. Defect area D — Release 2 closure records vs commits (findings)

All commands and results verbatim from this baseline run.

**D-1 — every cited commit exists.** `git log -1` on all 20 hashes cited
by the board/summary/acceptance (86cddb4, 5caabc7, 313ae9f, f1bbcd2,
7ccac0d, 3d6cb13, 917cfd7, 0aa3908, 4eab94d, dfa535c, e25a416, 88c140d,
cd90ff6, 9408c79, d52217c, 670523b, 470e2c7, c69272a, 80b0b05, f04e783)
resolved with messages matching their claimed roles (e.g. `4eab94d
feat(tu): add simulated-object foundation package`, `9408c79 feat(tu):
integrate simulation foundation (SIM-001)` with parents `f04e783 cd90ff6`).
Merge `9408c79` merges task tip `cd90ff6` into `f04e783` — consistent
with the board's evidence chain.

**D-2 — REC-1: cited CI run is the post-merge run, not the pre-merge
run.** The board and acceptance record claim "CI run 37298623628 …
before the `dev` merge". Verified via `gh run view`:

- Run `37298623628`: `event=push`, `headBranch=dev`, `headSha=9408c79`
  (the merge commit itself) — i.e. it ran **on dev after the merge**.
- The actual pre-merge task-branch run exists and is green: run
  `37298431248`, `event=push`, `headBranch=codex/sim-001-simulation-foundation`,
  `headSha=cd90ff6`, jobs `compatibility (3.13)` success and
  `compatibility (3.9)` success, created 2026-10-05T10:43:54Z, ~2
  minutes before the dev push of `9408c79` (10:45:40Z).

The *substance* (green CI on both Python versions existed before
integration) is supported, but the *cited run ID* identifies the
post-merge dev push. Reconciliation must correct the reference (record
verified commit/run references, per PRD gap 4), not waive the
discrepancy.

**D-3 — REC-2: board acceptance status is stale.** `sprint-board.md`
line 3 says "release close pending" and the board narrative says
"release acceptance and `main` promotion correctly recorded as pending",
but `log/release_2/acceptance.md` commits an **ACCEPTED (PASS)** verdict
at `c69272a` ("docs(log): issue release 2 acceptance verdict",
2026-10-05 18:58), and `sprint_summary.md` records "Goal achieved:
ACCEPTED by sw-camille (PASS) at `c69272a`". `main` promotion, however,
is genuinely unrecorded: every dev push including `c69272a` shows no
promotion, and no record claims it. Distinguishing acceptance (done,
`c69272a`) from promotion (pending, user decision) is exactly the
correction needed.

**D-4 — REC-3: sprint summary baseline reference mismatch.**
`sprint_summary.md` line 4 states "Baseline: `80b0b05`", but `80b0b05`
is "docs(log): release 1 sprint summary" (2026-10-01), while the SIM-001
baseline record states the branch was created "from `dev` @ `f04e783`"
("chore(log): integrate release 2 simulation preparation", 2026-10-05
17:35). Verified ancestry: `80b0b05` is an ancestor of `f04e783`, which
is the direct parent of the baseline commit `86cddb4`
(`git rev-parse 86cddb4^` = `f04e783…`). The correct baseline parent is
`f04e783`; `80b0b05` is at most an earlier dev ancestor.

**D-5 — no other stale claims found in the checked records.** Test
arithmetic is consistent: results record "135 = 99 baseline + 36 new";
this baseline re-run reproduces 135/OK. The code-review gate claim
"zero `huo`/`jin`/`shui`/`mu` production edits" re-verified:
`git diff 99da38c..cd90ff6 --stat -- softlab/huo softlab/jin softlab/shui softlab/mu`
produces empty output. Release 2 debt (OBS-004/005/006, DEFECT-2)
status on this baseline matches the Release 2 baseline record (no
warning fired; no waiver requested or granted).

## 6. Baseline defects, warnings, skips and limits (explicit ledger)

| Item | Status | Evidence / disposition |
| --- | --- | --- |
| Defect A (bridge readback stale after direct change/reset/other controller) | Confirmed | §2, verbatim output |
| Defect B (mixed str/non-str callback keys → incidental `TypeError`) | Confirmed | §3, case b3 |
| Overclaim C (reset "behaviorally identical"; failed-reset "identical future observations") | Confirmed | §4, sites at object.py:660,666–669, guide:132, arch.md:279 |
| REC-1/2/3 closure discrepancies | Confirmed | §5, gh/git verified |
| Suite warnings | **Zero** observed | `-W default` re-run, grep count 0 |
| Skips | None | No `skip` output in suite |
| `-W error::Warning` full gate on THIS branch | Not re-run here | Unverified-here; claimed green by Release 2 results; to be executed as R3-TC-08b |
| Release 1 debt OBS-004/005/006, DEFECT-2 | Unchanged, tracked | No warning fired in this run; no silent waiver |
| Real instruments / UI gates | N/A per task | No hardware access; no UI |
| Notebook examples (`tests/test_*.ipynb`) | Not executed | Not required for this baseline; suite + smoke cover the characterized areas |
| Environment limit | `conda activate` unavailable non-interactively | Used `$PWD/.venv/bin/python` directly; same interpreter |

No silent waivers: nothing above was exempted, suppressed or repaired as
part of this baseline. Additional findings beyond the authorized R3-001
scope were recorded (coverage gaps B-2 and SIM-TC-06a note in §2) and
are **not** being expanded into scope.

## 7. Notes

- This is a fresh baseline on the current branch; it does not reuse the
  historical "99 tests" figure except as a cross-check (135 = 99 + 36
  confirmed by running the suite).
- Reproduction scripts were one-off temp files outside the repository;
  the permanent regression/characterization tests with assertions will
  be added at the R3-001 implementation/testing phases per
  `r3-001-test-plan.md` (TDD: the failing cases R3-TC-07a–07c/07e/07f
  are the minimal reproductions of the confirmed defects).
