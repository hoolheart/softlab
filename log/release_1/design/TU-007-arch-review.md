# TU-007 architecture design review

Reviewer: sw-jerry (architect). Candidate: `601a816` on
`codex/tu-007-theory` (`log/release_1/design/TU-007.md`, step-split
design). Verdict: **CHANGES REQUESTED** — three issues, all minor and
design-text level; no redesign, no re-litigation of accepted decisions.
Scope is detailed-design compliance with the Release 1 PRD (TU-007 row),
the approved TU-007 test contract (`log/release_1/tests/TU-007.md`) and
its implementability review (sw-tom ACCEPTED, five non-blocking
observations), the TU-001 compatibility matrix (OBS-005/OBS-006), the
Release 1 architecture constraints (`log/release_1/arch-review.md`), and
the five-module architecture. This is not a code review, test execution
or integration approval.

I independently re-read `softlab/tu/theory/model.py` at the branch tip,
`softlab/tu/theory/mapping.py`'s class binding, `tests/test_tu_theory.py`
(all 11 cases), `softlab/jin/misc/delegated.py` and
`softlab/jin/misc/limited_attribute.py`, the TU-002/TU-003/TU-005
`schema_version` precedents, and `log/release_1/compatibility.md`
(OBS-005/OBS-006 rows). Source-level claims below are verified against
those files; I additionally executed an import check confirming that
`model.Mapping` is `softlab.tu.theory.mapping.Mapping`, not
`collections.abc.Mapping` (load-bearing for Issue 1).

## Assessment against review criteria

1. **Architecture fit — confirmed.** The change is additive-only in
   `softlab/tu/theory/model.py`: one private field, one
   backward-compatible keyword on `add_attribute`, five new methods,
   docstrings. `Mapping`, `batch_mapping`, the legacy `features`
   property, `get_mapping`, `calculate_features`, `name`, `__repr__` and
   the `Motion1D` example are untouched; no `__init__.py` change is
   needed because every addition is a method on the already-exported
   `TheoryModel`; `jin` is untouched and the existing
   `LimitedAttribute.set()` validator chain is the single validation
   path. New imports are stdlib only (`typing.Tuple`,
   `collections.abc`) — with one collision the design must pin
   (Issue 1). Five-element boundaries hold; the parallel
   `Dict[str, str]` for descriptions (rather than a field on
   `LimitedAttribute`) is the correct call: attaching theory-layer
   semantics to the generic `jin` attribute class would cross the
   `jin`/`tu` boundary for no necessity.
2. **describe/add_attribute — confirmed.** The ordering invariant is
   correct and complete: the duplicate-key `ValueError` precedes
   construction, and `LimitedAttribute.__init__` runs
   `set(initial_value)` (validated), so recording the description only
   after construction guarantees
   `set(_attr_descriptions) == set(_attributes)` after every successful
   `add_attribute` and leaves both dicts untouched after every failure —
   no orphan descriptions from either rejection cause. The
   `schema_version: 1` namespace is consistent with the TU-002/TU-003/
   TU-005 precedent: each versioned surface declares its own namespace
   and shares only the literal keys. Zero evaluation is structural:
   `describe()` reads `_name`, `type(self).__qualname__` and
   `_attr_descriptions` only, satisfying case 5 (always-raising model
   fully describable). Case 4's `assertIn("Model", json.dumps(...))`
   passes via the fixture qualname `_make_model.<locals>.Model`, as the
   design notes.
3. **Configuration — confirmed in substance; one annotation collision
   (Issue 1).** `supported_configuration()` returning
   `tuple(self._attributes.keys())` is value-independent by construction
   (case 6); the design correctly proves that filtering non-serializable
   keys would make the declaration value-dependent and violate case 6,
   so the JSON-safety-as-author-responsibility policy is the only
   reading consistent with the accepted tests, and it is honestly
   documented in the `configuration()` docstring mandate (stated, not
   policed) — consistent with the PRD's TU-005 serialization stance. The
   three-phase `configure` (TypeError → KeyError → pre-validate-all →
   apply) is sound: phase 3 validates every value against the existing
   `Validator` before any write, so rejection at any phase — including
   multi-key validation failure — performs zero writes; the apply phase
   routes through `LimitedAttribute.set()` (validate-then-assign),
   which is exactly the reuse-not-duplication case 9 pins. The
   deliberate double validation is justified (validators are pure; one
   validation implementation is kept). `KeyError(key)` bare-key form
   matches case 8's `assertIn("nope", str(...))`. Accepting
   `collections.abc.Mapping` rather than `dict` is the right call (the
   `str` rejection holds; legitimate mappings are not over-restricted).
   The phase-3 reach into `LimitedAttribute._vals` is justified but
   under-acknowledged (Issue 3).
4. **evaluate_features — confirmed; one wording overclaim (Issue 2).**
   The direct `calculate_features()` call is forced by cases 10–11 (the
   legacy property swallows everything, so strict could never propagate
   through it), the strict path is a bare propagation preserving
   exception-object identity (`assertIs`, consistent with the TU-006
   error-cause policy), the lenient path preserves OBS-005 `{}`
   semantics, and the legacy `features` property stays byte-identical
   with neither path implemented in terms of the other — three call
   sites sharing only `calculate_features()`. Catching `Exception`
   (not bare `except:`) in the new lenient path is the correct choice;
   only the "behaviorally identical" justification is inaccurate
   (Issue 2).
5. **Step split A→B→C — confirmed.** Group independence holds (no RED
   case in one group depends on another group's API; the fixture's
   `with_description: bool = False` default isolates guard construction
   from case 4). Green-condition predictions are accurate: after step A,
   cases 1–5 green and 6–11 RED on missing API (`AttributeError` via
   `Delegated.__getattr__` for the five names, matching the recorded
   red evidence); after B, only 10–11 RED; after C, all 11 green.
   Guards 1–3 stay green after every step because every production delta
   is additive.
6. **Shadowing, simplicity audit, docstring mandates, handoffs —
   confirmed.** The reserved-name resolution is the OBS-006 convention
   done properly: class-docstring reservation, the
   `model._attributes[name]()` escape hatch, zero in-repo impact
   (verified by the implementability review's grep — only
   `theory/__init__.py` and the `Motion1D` example consume
   `TheoryModel`, and no in-repo attribute key uses any of the five
   names). The simplicity audit gives a reason for every rejection
   (schema registry, `__init__.py` export, key filtering, serialization
   layer, apply-in-order, jin helper, property-routed lenient path,
   bare `except:`, versioning machinery, name mangling, locking) — each
   is genuinely an entity without necessity for this scope. The seven
   verbatim docstring mandates are complete and remove developer
   guesswork. Test-phase handoffs (optional guard enrichment for
   `name`/`calculate_features` NotImplementedError; optional pinnings
   for multi-key no-partial-application and `configure({})` no-op) are
   correctly scoped as optional and deferred to sw-mike — none is a
   design prerequisite.
7. **Clarifications needed** — the three numbered issues below.

## Issues (all required changes; all design-text level)

### Issue 1: `Mapping` name collision — `configure` annotation and the `collections.abc` import form are unpinned and collide with the module's existing `Mapping` binding

`softlab/tu/theory/model.py` line 14 binds the module-level name
`Mapping` to `softlab.tu.theory.mapping.Mapping` (verified by import:
`model.Mapping is softlab.tu.theory.mapping.Mapping`, not the ABC). The
design's public surface declares
`TheoryModel.configure(cfg: Mapping[str, Any]) -> None` — in this module
that annotation resolves to the **theory mapping class**, not
`collections.abc.Mapping`, which contradicts the phase-1 contract
("a non-`collections.abc.Mapping` argument raises `TypeError`"). Worse,
the design says only "`collections.abc.Mapping` imported as the
non-mapping check target" without pinning the import form: the naive
reading, `from collections.abc import Mapping`, would **rebind** the
module-level name, which (a) changes `get_mapping`'s `-> Mapping`
annotation (evaluated at class-body execution) to the ABC, and (b)
breaks the module's own `Motion1D` `__main__` example, which calls
`Mapping((2,1), (2,1), lambda ...)` — `collections.abc.Mapping(...)`
raises `TypeError`. This is exactly the kind of accidental module-level
breakage guard 2 and the import-identity invariants exist to prevent,
and the design must not leave it to the developer to discover.
**Requested change:** pin a non-colliding import form — e.g.
`import collections.abc` with `collections.abc.Mapping` at the
isinstance check, or an explicit alias such as
`from collections.abc import Mapping as MappingABC` — and use that form
in the `configure` signature annotation. Update the public-surface
signature, the "New import" paragraph, the step-B import delta row, and
any pseudocode/diagram reference accordingly, and state explicitly that
the module-level `Mapping` binding used by `get_mapping` and the
`Motion1D` example stays the theory mapping class.

### Issue 2: the lenient path is not "behaviorally identical to the property for every evaluation failure" — correct the overclaim

The design justifies `except Exception:` in the new lenient path with:
"behaviorally identical to the property for every evaluation failure
(all exception types an evaluation can raise are `Exception`
subclasses)". The parenthetical is false: `KeyboardInterrupt`,
`SystemExit` and `GeneratorExit` are `BaseException` subclasses, not
`Exception` — the legacy property's bare `except:` swallows them while
the new lenient path propagates them. The **choice** is correct — the
TU-001 matrix explicitly does not certify swallowing process-control
exceptions as desirable (OBS-005), and new code must not extend that
swallow — but the gate record must not assert a behavioral identity
that does not exist. **Requested change:** replace the identity claim
with an explicit divergence statement: the lenient path matches the
legacy property for all `Exception` subclasses (every failure the
acceptance suite exercises) and deliberately diverges for
`BaseException`-only process-control exceptions, which propagate on the
new path while the legacy property's bare `except:` stays
byte-identical. One sentence in the `evaluate_features` algorithm
section; no other text changes.

### Issue 3: the phase-3 reach into `LimitedAttribute._vals` is a cross-module private access — acknowledge the tradeoff explicitly

Phase 3 pre-validates via `self._attributes[key]._vals.validate(value)`
— reading a **private field** of a `jin` class from `tu`. The design
correctly rejects the alternative of adding a `validate()` helper to
`LimitedAttribute` (out of the authorized `model.py` scope), and the
reuse rationale ("the same validator object the attribute itself uses")
is right — this is the correct resolution under the constraints. But
the design never names the tradeoff: it couples `tu` to `jin`'s private
state layout, and it never states why the public-only alternative
(snapshot current values via `get()`, apply through `set()`, restore on
failure) was rejected. A design that mandates validator purity to
justify double validation should be equally explicit about the
encapsulation cost of its pre-validation mechanism. **Requested
change:** add one acknowledgment to decision 4 (or the simplicity
audit): the phase-3 pre-validation reads `LimitedAttribute._vals`, a
private field, as the smallest mechanism that pre-validates without
mutating; the public-only snapshot-restore alternative is rejected
because it mutates before it can fail (the restore path adds moving
parts and its own failure modes), and promoting a public `validate()`
accessor on `LimitedAttribute` is out of the authorized scope and may
be revisited if a second consumer appears.

## Disposition

Criteria 1–7 pass in substance; the three issues are minor, precisely
scoped, and require no redesign and no test-plan change. Once the design
text is corrected (pin the non-colliding `collections.abc` import form
and fix the `configure` annotation; replace the lenient-path identity
overclaim with the explicit divergence statement; acknowledge the
`_vals` private-access tradeoff), TU-007 may proceed to implementation
without a further full re-review — a focused confirmation of the three
corrections suffices. This review does not claim code review, test
pass, CI or integration; the implementation, independent review,
testing, principle and CI gates remain open.

## Focused confirmation of corrections (commit `acac791`)

Reviewer: sw-jerry (architect). Scope: the three corrections only; no
full re-review. Each verified against `log/release_1/design/TU-007.md`
at `acac791`.

1. **Issue 1 — `Mapping` collision: CONFIRMED.** The import form is
   pinned to a plain `import collections.abc` with
   `collections.abc.Mapping` spelled fully at both use sites (phase-1
   `isinstance`, `configure` annotation), the rebinding
   `from collections.abc import Mapping` form is explicitly forbidden
   with the correct rationale (`get_mapping`'s class-body-evaluated
   annotation and the `Motion1D` example instantiation), and the alias
   alternative is considered and rejected with reason. The annotation is
   consistent across every surface: public-surface signature, "New
   import" paragraph, phase-1 pseudocode, `configure` sequence diagram
   (`alt cfg is not a collections.abc.Mapping`), `configure` docstring
   mandate, step-B import delta row, and scope/dependency statements.
   The module-level `Mapping` binding (`get_mapping`, class-diagram
   `Mapping` node, `Motion1D` example) stays the theory mapping class
   and is stated as such.
2. **Issue 2 — lenient-path overclaim: CONFIRMED.** The "behaviorally
   identical" claim is gone from the normative text (it survives only in
   the design-review summary describing the fix). The algorithm section
   now states the exact contract: matches the legacy property for every
   `Exception` subclass (covering every failure the acceptance suite
   exercises) and **deliberately diverges** for `BaseException`-only
   process-control exceptions (`KeyboardInterrupt`, `SystemExit`,
   `GeneratorExit`), which propagate on the new path while the legacy
   bare `except:` stays byte-unchanged. The `evaluate_features` docstring
   mandate no longer quotes the overclaim and carries the same
   divergence statement, so the verbatim docstring the developer writes
   cannot reintroduce it.
3. **Issue 3 — `_vals` reach: CONFIRMED.** Decision 4 now names the
   tradeoff explicitly: reading `LimitedAttribute._vals` is a
   cross-module private access coupling `tu` to `jin`'s private state
   layout, chosen as the smallest mechanism that pre-validates without
   mutating. The public-only snapshot-restore alternative (`get()`
   snapshot → `set()` apply → restore on failure) is explicitly rejected
   with rationale (duplicates validation state and rollback logic `set()`
   already owns; adds a restore path with its own failure modes).
   Promoting a public `validate()` accessor on `LimitedAttribute` is
   recorded as out of the authorized `model.py` scope and revisitable if
   a second consumer of attribute pre-validation appears.

### Verdict: APPROVED

All three corrections are implemented as requested, internally
consistent across surface text, pseudocode, diagrams and docstring
mandates, and introduce no new issues. The TU-007 design is approved;
it may proceed to implementation. This confirmation claims no code
review, test pass, CI or integration; those gates remain open.
