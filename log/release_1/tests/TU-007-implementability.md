# TU-007 test implementability review

Reviewer: sw-tom. Verdict: **ACCEPTED** (no blocking issues; 5
non-blocking observations handed to sw-celeste for the detailed design).
Reviewed candidate: tip `13ec8b7` on `codex/tu-007-theory` (test commit
`92b9083` plus expected-red evidence record `13ec8b7`; production
unchanged).

Reviewed the 11 acceptance cases and the proposed surface
(`describe()`, `add_attribute(description=)`,
`supported_configuration()`/`configuration()`/`configure()`,
`evaluate_features(strict=False)`) against
`softlab/tu/theory/model.py`, `softlab/tu/theory/mapping.py`,
`softlab/jin/misc/delegated.py`, `softlab/jin/misc/limited_attribute.py`,
the TU-001 compatibility matrix (`log/release_1/compatibility.md`,
theory rows and OBS-005), the Release 1 PRD TU-007 row, and all in-repo
`TheoryModel` consumers. I reproduced the recorded red phase:
`.venv/bin/python -m unittest tests.test_tu_theory` from the repository
root on the tip yields **11 tests, 3 pass, 8 errors**, matching the
gate record — the 3 passing cases are exactly the intended guards
(1, 2, 3); the 8 errors are `AttributeError`s for the five missing
contract names resolved through `Delegated.__getattr__` (proving they
are new names, not existing behavior) plus the missing
`add_attribute(description=...)` keyword (case 4). Full discover gives
**88 tests, 80 pass, 8 errors** — the designed RED set only; all
pre-existing suites stay green.

## Scope and confinement assessment

**Additive on `TheoryModel` only — confirmed implementable.** Every
RED case can be satisfied by edits confined to
`softlab/tu/theory/model.py`:

- `Mapping`, `batch_mapping`, and `mapping.py` need no change; guard
  case 2 pins their behavior and no RED case touches them.
- The legacy `features` property stays byte-identical; cases 10–11 pin
  its behavior from the outside rather than restructuring it.
- Public imports (`Mapping`, `TheoryModel`, `batch_mapping` via
  `softlab.tu.theory` → `softlab.tu`) are untouched; guard case 3 pins
  identity.
- `jin` needs no change: validator reuse falls out of calling the
  existing `LimitedAttribute.set()` (`self._vals.validate(value)`) per
  configuration key, i.e. the single existing validation path is
  reused, not duplicated (case 9 pins the same `ValueError` type and
  unchanged state, which is exactly what the existing chain produces).
- A grep of in-repo `TheoryModel` consumers finds only
  `theory/__init__.py` and the `Motion1D` example inside `model.py`
  itself — no production subclass exists, so the new keyword and
  methods have zero in-repo blast radius.

## Answers to the six review questions

1. **Additive confinement — yes.** See above; nothing outside
   `model.py` is required by any case.

2. **`describe()` / `add_attribute(description=)` — implementable,
   with one documented shadowing caveat (OBS-006 category, in-repo
   impact zero).** `Delegated.__getattr__` only fires when normal
   lookup fails, so real methods defined on `TheoryModel` take
   precedence over delegated attribute keys of the same name. Verified
   by grep: no in-repo attribute key or subclass uses any of the five
   new names (`describe`, `supported_configuration`, `configuration`,
   `configure`, `evaluate_features`), so no existing caller is
   shadowed. An external model that named an attribute e.g.
   `configuration` would lose dotted-call access to it (explicit
   `model._attributes['configuration']()` remains, per the OBS-006
   convention). Observation 1 below.

   `add_attribute(..., description: str = '')` is a backward-compatible
   keyword addition: existing three-argument calls (guard fixture, all
   in-repo examples) keep working; the duplicate-key `ValueError`
   stays in front of description registration, so a rejected duplicate
   must not leave an orphan description entry (see observation 2).

3. **Configuration surface — implementable as specified.**
   - *Value-independence of `supported_configuration()`*: return the
     key set of `_attributes` (test pins exactly `{value, gain}` both
     before and after a value change). Note this makes "supported"
     coextensive with "all attributes" for the fixture; observation 3.
   - *Round trip*: `configuration()` reads each
     `LimitedAttribute.get()`; `configure()` writes each via
     `LimitedAttribute.set()`; delegated access then sees the same
     values by construction (single source of truth, case 7). JSON
     safety is pinned only for the int/float fixture values;
     observation 4.
   - *KeyError / TypeError / no partial application*: two-phase
     apply — first verify every incoming key is supported (raise
     `KeyError(key)`, whose `str()` contains the key name as the test
     requires), then apply values through the validator chain. A
     failed validation leaves prior values intact because
     `LimitedAttribute.set` validates before assigning (case 9). The
     non-mapping `TypeError` check should accept
     `collections.abc.Mapping`, not just `dict`, so the test's `str`
     rejection holds without over-restricting legitimate mapping
     arguments (either satisfies the case; design should state it).
   - *Validator-chain reuse*: confirmed — no second validation layer
     is needed or permitted; case 9's "same `ValueError`, changes
     nothing" is the existing chain's natural behavior.

4. **`evaluate_features(strict=False)` — implementable; the apparent
   ambiguity about which existing method strict wraps resolves
   uniquely against the tests.** If `evaluate_features` routed through
   the legacy `features` property, `strict=True` could never propagate
   anything (the property swallows all exceptions). Cases 10 and 11
   therefore force the only consistent design: `evaluate_features`
   calls `self.calculate_features()` directly, bare-`raise`s the
   original object on failure when `strict=True`, and returns `{}` on
   failure when `strict=False` — mirroring the property's try/except
   body. The lenient path is behaviorally identical to `features`
   (both pinned to `{}` under a patched failure in case 11); identity
   of the *exception object* (`assertIs`) forbids any `raise ... from`
   wrapping, consistent with the TU-006 error-cause policy. The design
   docstring should state explicitly that strict wraps
   `calculate_features`, not `features` — the tests force it, but the
   next maintainer should not have to re-derive it.

5. **Guard cases 1–3 genuinely protect the legacy surface — yes.**
   Guard 1 pins: feature success and update through delegated writes,
   out-of-range validator rejection (value unchanged), duplicate-key
   rejection, the OBS-005 lenient `{}` fallback, and base
   `get_mapping` `NotImplementedError` — this covers every plausible
   regression vector of editing `model.py` (property rewrite,
   `add_attribute` signature break, delegation break). Guard 2 pins
   all five `Mapping`/`batch_mapping` check outcomes plus metadata —
   any accidental edit to `mapping.py` turns it red. Guard 3 pins
   import identity through both `softlab.tu.theory` and `softlab.tu`.
   Latent-contradiction sweep (TU-006 lesson): no two assertions about
   the same construction conflict; the fixture's `with_description`
   default-`False` split cleanly separates guard construction from RED
   construction, and the `assert_array_equal` fix is correct.

6. **PRD consistency — complete, nothing blocking.** Identity (case
   4), semantic descriptions (cases 4–5), supported serializable
   configuration declared explicitly (case 6), round trip verified
   (case 7), explicit rejection of unsupported configuration (case 8),
   validator reuse (case 9), opt-in strict evaluation (cases 10–11),
   legacy `features`/mapping/public-import compatibility (guards
   1–3), shape checks retained not duplicated (guard 2 plus the
   structural fact that no RED case touches `mapping.py`), strict is
   strictly opt-in (`strict=False` default pinned in case 11). No
   requirement dangles unaddressed the way OBS-002 did in TU-006 —
   OBS-005's disposition is pinned by cases 1, 10, and 11 (the swallow
   survives only on legacy and `strict=False` paths).

## Non-blocking observations (for the detailed design to pin)

1. **Reserved-name shadowing (OBS-006 category).** The five new method
   names become class-level attributes of `TheoryModel` and hence
   shadow any delegated attribute key of the same name (dotted-call
   access). In-repo impact is zero (verified by grep); the design
   should document these names as reserved and note that explicit
   `_attributes` lookup remains the OBS-006 escape hatch. No new test
   case is required — guards must pass against unchanged code and
   these names do not exist there.
2. **Description registration ordering.** `add_attribute` must check
   the duplicate key *before* recording the description, so a rejected
   duplicate leaves no orphan description entry that a later
   successful re-add would contradict. State the storage (a parallel
   `_attr_descriptions` dict is the natural choice) in the design.
3. **`supported_configuration` vs non-serializable attribute values.**
   The tests pin the declaration to exactly the attribute keys with
   JSON-safe values. The design should state the policy for models
   holding non-JSON-safe values (e.g. ndarray): either such keys are
   excluded from the supported set, or `configuration()` documents
   that JSON safety is the model author's responsibility. Not
   exercised by any case; stating the choice is sufficient.
4. **Atomicity of `configure` under multi-key validation failure.**
   Cases 8–9 pin no-partial-application only for unknown keys and for
   single-key validation failure. A multi-key `configure` where a
   later key fails validation may (pre-validate-all-then-apply) or
   may not (apply-in-order) leave earlier keys applied. The gate
   scopes no-partial-application to unknown keys, so this is not a
   test defect; the design should state which semantics `configure`
   guarantees (recommend: validate all values first, then apply, so
   rejection never partially applies).
5. **Guard 1 could optionally pin two more legacy bits.** The legacy
   `name` property (currently only reached indirectly via case 4's
   `describe()["name"]`) and base `calculate_features`
   `NotImplementedError` are not guarded. Adding them is optional and
   must not delay implementation; the current guards already cover
   every regression vector the implementation can introduce.

## Per-case feasibility notes

1. **`test_legacy_features_behavior_unchanged`** — green guard;
   stays green if `features`, `add_attribute`, and delegation are
   untouched. Implementation must not "refactor" the property.
2. **`test_mapping_shape_checks_retained_not_duplicated`** — green
   guard; pins `mapping.py` byte-behavior (five outcomes + metadata +
   batching). No RED case depends on `mapping.py`, so this guard
   purely protects against accidental edits.
3. **`test_public_imports_unchanged`** — green guard; additive
   methods on `TheoryModel` cannot affect import identity.
4. **`test_model_identity_and_semantic_description`** — RED;
   implementable via `schema_version` constant 1, `name` read,
   JSON-safe class identity (e.g. `type(self).__qualname__` — the
   `assertIn("Model", json.dumps(...))` check passes for any
   representation containing the class name), and per-attribute
   description entries defaulting to `""`. Empty-name identity via the
   existing `_name` constructor contract.
5. **`test_description_performs_no_evaluation`** — RED; trivially
   satisfied as long as `describe()` reads only name/class/attribute
   metadata and never calls `calculate_features`.
6. **`test_supported_configuration_declared_explicitly`** — RED;
   return the `_attributes` key set. Value-independence and JSON
   safety hold for the fixture by construction.
7. **`test_supported_configuration_round_trip`** — RED; reads via
   `LimitedAttribute.get()`, writes via `LimitedAttribute.set()`,
   re-apply identity follows from idempotent set of equal values.
8. **`test_unsupported_configuration_keys_rejected_explicitly`** —
   RED; two-phase key validation then apply. `KeyError(key)` string
   form contains the key name; `TypeError` for non-mapping arguments
   (recommend `collections.abc.Mapping` check).
9. **`test_existing_validators_reused_for_configuration`** — RED;
   falls out of routing through `LimitedAttribute.set()` — the same
   `ValInt(0, 10)` `ValueError`, value untouched because validation
   precedes assignment in the existing chain. Nothing to duplicate.
10. **`test_strict_evaluation_exposes_original_error`** — RED;
    direct `calculate_features()` call, bare `raise` preserves the
    identical object (`assertIs`). Consistent with TU-006 error-cause
    policy.
11. **`test_lenient_path_unchanged_beside_strict`** — RED; pins the
    full semantics matrix: strict success equals the legacy dict;
    under failure, `features`, `evaluate_features()`, and
    `evaluate_features(strict=False)` all return `{}` while
    `strict=True` raises the identical object. Forces the
    direct-call design of question 4.

## Case dependencies and recommended step split

Strict order **A (identity/description) → B (configuration) → C
(strict evaluation)** is achievable; each step makes exactly its RED
cases green while guards 1–3 stay green:

1. **Step A** (cases 4–5): `describe()`, description storage,
   `add_attribute` keyword. No dependency on B or C.
2. **Step B** (cases 6–9): configuration surface on top of existing
   `LimitedAttribute` access. Independent of A and C except shared
   fixture.
3. **Step C** (cases 10–11): `evaluate_features` wrapping
   `calculate_features` directly. Independent of A and B.

The gate's group-independence claim holds: no RED case in one group
depends on API from another group (fixture's `with_description` flag
isolates guard construction from case 4). Step order above is the
lowest-risk sequence but is not contractual.

## Verification performed for this review

- `.venv/bin/python -m unittest tests.test_tu_theory` (Python
  3.13.15, `.venv`, `MPLCONFIGDIR=/tmp/softlab-mpl`,
  `XDG_CACHE_HOME=/tmp/softlab-cache`): **11 tests, 3 pass, 8
  errors** — matching the gate record; the 3 guards are the green
  cases; all 8 errors are `AttributeError`/`TypeError` on missing
  contract API.
- `.venv/bin/python -m unittest discover -s tests -p 'test_*.py'`:
  **88 tests, 80 pass, 8 errors** — exactly the designed RED set; all
  pre-existing suites green.
- Grep of in-repo `TheoryModel` consumers and attribute keys: no
  usage of any of the five new method names; no production subclass;
  blast radius confined to `model.py`.
- Read `softlab/jin/misc/delegated.py` and
  `softlab/jin/misc/limited_attribute.py` to confirm delegation
  precedence (`__getattr__` only on lookup failure) and that
  `LimitedAttribute.set` validates before assigning (making cases 8–9
  fall out of the existing chain).

This review did not run the acceptance suite as a pass criterion (8
cases are expected RED), does not implement anything, and does not
claim detailed-design approval, independent review, testing, or
principle gates; those remain pending. No re-review of the test plan
is required — the observations above are designer handoffs, not test
defects.
