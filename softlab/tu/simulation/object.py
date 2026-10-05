"""Deterministic simulated-object foundation.

This module defines :class:`SimulatedObject`, a plain class representing
*the thing being controlled or observed* in a simulated experiment —
distinct from a mock instrument, which represents *how* an experiment
controls or observes it.

A simulated object owns three declared variable namespaces:

- **inputs** — externally driven variables (including time or ``dt``
  when a model needs them; there is no dedicated clock API and no
  wall-clock access anywhere in the object),
- **states** — internal variables carried between evolutions,
- **outputs** — names of values derived from state by the observation
  function.

The model author supplies two deterministic callbacks:

- ``evolve(inputs, previous_states) -> next_states`` — the transition
  function ``x_next = evolve(u, x_previous)``,
- ``observe(states) -> outputs`` — the output function ``y = G(x)``.

Evolution happens only on explicit :meth:`SimulatedObject.evolve_once`
calls. Observation never evolves. :meth:`SimulatedObject.reset`
restores the documented initial condition. All committed values are
defended by copy-in/copy-out at every boundary crossing.

This module imports nothing from ``softlab``; the dependency direction
is strictly one-way (bridge closures in tests/examples reference the
object). NumPy + stdlib only.
"""

from collections.abc import Mapping as _Mapping
from collections.abc import Sequence as _Sequence
from typing import (
    Any,
    Callable,
    Dict,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
)
import numpy as np


class _ValueSpec(NamedTuple):
    """
    Private per-variable value specification.

    Pinned at construction from the variable's initial value and
    enforced on every later value committed to that variable:

    - ``category`` --- one of ``'int'``, ``'float'`` or ``'ndarray'``,
      an exact-type closed list (``bool`` and NumPy scalars are
      deliberately rejected categories);
    - ``dtype`` --- pinned ndarray dtype, ``None`` for scalars;
    - ``shape`` --- pinned ndarray shape, ``None`` for scalars.
    """

    category: str
    dtype: Optional[Any] = None
    shape: Optional[Tuple[int, ...]] = None


def _where(name: Optional[str]) -> str:
    """Format an optional variable-name suffix for error messages."""
    return '' if name is None else f' for variable {name!r}'


def _classify_value(value: Any, name: Optional[str] = None) -> _ValueSpec:
    """
    Classify a value against the closed supported-category list.

    Supported categories — enforced identically for initial values,
    ``set_input`` values, ``evolve`` results and ``observe`` results:

    - integer scalar --- exact ``int`` (``type(v) is int``);
    - float scalar --- exact ``float`` (``type(v) is float``);
    - numeric array --- exact ``np.ndarray`` (``type(v) is np.ndarray``)
      with a numeric dtype (``np.issubdtype(v.dtype, np.number)``).

    Args:
    - value --- the value to classify
    - name --- optional variable name, included in error messages

    Returns:
    - the pinned :class:`_ValueSpec` for the value (dtype and shape
      are pinned from ndarray values)

    Errors:
    - TypeError --- ``bool`` (checked before ``int``: ``bool``
      subclasses ``int`` but is not an exact ``int``); NumPy scalar
      types (the message directs model authors to ``float(v)``,
      ``int(v)`` or ``v.item()``); any other type (non-exact ndarray
      subclasses, ``list``/``tuple``/other sequences, ``str``, ``None``,
      dicts, arbitrary objects)
    - ValueError --- ndarray with object dtype or any other
      non-numeric dtype (this covers ragged sequences, which cannot
      exist as exact numeric ndarrays)

    Side-effects: none.
    """
    if type(value) is bool:
        raise TypeError(
            f'Boolean is not a supported value{_where(name)}: '
            'declare an exact int or float instead')
    if isinstance(value, np.generic):
        raise TypeError(
            f'NumPy scalar type {type(value).__name__} is not a '
            f'supported value{_where(name)}; convert with '
            'float(v), int(v) or v.item()')
    if type(value) is int:
        return _ValueSpec('int')
    if type(value) is float:
        return _ValueSpec('float')
    if type(value) is np.ndarray:
        if not np.issubdtype(value.dtype, np.number):
            raise ValueError(
                f'ndarray with non-numeric dtype {value.dtype} is not '
                f'a supported value{_where(name)}')
        return _ValueSpec('ndarray', value.dtype, tuple(value.shape))
    raise TypeError(
        f'Unsupported value type {type(value).__name__}'
        f'{_where(name)}; supported categories are exact int, '
        'exact float and exact numeric np.ndarray')


def _validate_against_spec(
    name: str,
    value: Any,
    spec: _ValueSpec,
) -> Any:
    """
    Validate a value against a pinned per-variable specification.

    Scalar variables accept only their declared scalar category (an
    ``int``-declared variable cannot later receive a ``float`` and vice
    versa; there is no implicit conversion anywhere). ndarray variables
    pin dtype and shape from the initial value; later values must be
    exact ndarrays with identical dtype and shape.

    Args:
    - name --- variable name, used in error messages
    - value --- candidate value
    - spec --- pinned :class:`_ValueSpec` to validate against

    Returns:
    - the value, ready to be committed (ndarray values are copied by
      the caller via :func:`_copy_value`; this helper performs
      validation only)

    Errors:
    - TypeError --- wrong category: a non-exact-``int`` value against
      an ``'int'`` spec, a non-exact-``float`` value against a
      ``'float'`` spec, or a non-exact-``np.ndarray`` value against an
      ``'ndarray'`` spec
    - ValueError --- ndarray dtype mismatch or shape mismatch, naming
      the variable and the expected/actual dtype/shape

    Side-effects: none.
    """
    if spec.category == 'int':
        if type(value) is not int:
            raise TypeError(
                f'Variable {name!r} was declared with an exact int '
                f'initial value and rejects type '
                f'{type(value).__name__}; no implicit conversion '
                'is performed')
        return value
    if spec.category == 'float':
        if type(value) is not float:
            raise TypeError(
                f'Variable {name!r} was declared with an exact float '
                f'initial value and rejects type '
                f'{type(value).__name__}; no implicit conversion '
                'is performed')
        return value
    if type(value) is not np.ndarray:
        raise TypeError(
            f'Variable {name!r} was declared with an exact '
            f'np.ndarray initial value and rejects type '
            f'{type(value).__name__}; no implicit conversion '
            'is performed')
    if value.dtype != spec.dtype:
        raise ValueError(
            f'Variable {name!r} expects ndarray dtype {spec.dtype}, '
            f'got {value.dtype}')
    if tuple(value.shape) != spec.shape:
        raise ValueError(
            f'Variable {name!r} expects ndarray shape {spec.shape}, '
            f'got {tuple(value.shape)}')
    return value


def _copy_value(value: Any) -> Any:
    """
    Copy a supported value across a boundary crossing.

    ndarray values are copied with ``np.array(v, copy=True)`` on every
    boundary crossing (construction, ``set_input``, ``evolve`` result,
    ``observe`` result, ``get_input``, ``get_state``,
    ``observe_outputs``, callback arguments). Scalars (``int``/``float``)
    are immutable and passed as-is.

    Args:
    - value --- a value already validated against the value contract

    Returns:
    - a detached copy (ndarray) or the identical object (scalar)

    Errors: none.

    Side-effects: none.
    """
    if type(value) is np.ndarray:
        return np.array(value, copy=True)
    return value


class SimulatedObject:
    """
    Deterministic simulated object with declared inputs/states/outputs.

    A simulated object represents *the thing being controlled or
    observed* — distinct from a mock instrument, which represents
    *how* an experiment controls or observes it. One object may be
    shared by several devices, so object state never implicitly
    belongs to any instrument.

    The object owns three declared variable namespaces: **inputs**
    (externally driven, including time/``dt`` when a model needs them;
    there is no dedicated clock API), **states** (internal variables
    carried between evolutions) and **outputs** (names of values
    derived from state by the observation function). Names must be
    unique across all three roles.

    The model author supplies two deterministic callbacks: ``evolve``
    (the transition function, ``x_next = evolve(u, x_previous)``) and
    ``observe`` (the output function, ``y = G(x)``). Evolution happens
    only on explicit :meth:`evolve_once` calls; observation never
    evolves. All committed values are defended by copy-in/copy-out.

    The contract is single-threaded, matching the existing ``Device``
    lifecycle contract. Callbacks (and initial-value factories) must
    not re-enter the same object.

    Args:
    - name --- non-empty object name
    - inputs --- mapping from input variable name to an initial value
      or a zero-argument factory producing one; because the value
      contract admits only exact ``int``, exact ``float`` and exact
      numeric ``np.ndarray``, any callable value is unambiguously a
      factory; factories are invoked once at construction (their first
      result fixes the variable's value specification) and again at
      every :meth:`reset` — the designed failure-injection point for
      reset atomicity
    - states --- mapping from state variable name to an initial value
      or a zero-argument factory, same rules as ``inputs``
    - outputs --- non-repeating sequence of output names; the names
      ``observe`` must produce
    - evolve --- deterministic transition callback
      ``(inputs, previous_states) -> next_states``
    - observe --- deterministic observation callback
      ``(states) -> outputs``

    Errors:
    - TypeError --- ``inputs``/``states`` not mappings, ``outputs``
      not a sequence, ``evolve``/``observe`` not callable, or an
      initial value outside the supported value contract
    - ValueError --- empty or duplicate names (across all three
      roles), or an ndarray initial value with a non-numeric dtype
    - any exception raised by an initial-value factory propagates
      identically (never caught, wrapped or replaced); no partially
      constructed object exists

    Side-effects: invokes each zero-argument factory at most once;
    otherwise none.
    """

    def __init__(
        self,
        name: str,
        inputs: Mapping[str, Any],
        states: Mapping[str, Any],
        outputs: Sequence[str],
        evolve: Callable[
            [Dict[str, Any], Dict[str, Any]], Mapping[str, Any]],
        observe: Callable[[Dict[str, Any]], Mapping[str, Any]],
    ) -> None:
        """Initialize a simulated object; see class docstring."""
        if type(name) is not str or len(name) == 0:
            raise ValueError(
                f'Object name must be a non-empty str, got {name!r}')
        if not isinstance(inputs, _Mapping):
            raise TypeError(
                'inputs must be a mapping of variable names to initial '
                f'values or factories, got {type(inputs).__name__}')
        if not isinstance(states, _Mapping):
            raise TypeError(
                'states must be a mapping of variable names to initial '
                f'values or factories, got {type(states).__name__}')
        if isinstance(outputs, (str, bytes)) or \
                not isinstance(outputs, _Sequence):
            raise TypeError(
                'outputs must be a sequence of output names, got '
                f'{type(outputs).__name__}')
        if not callable(evolve):
            raise TypeError(
                f'evolve must be callable, got {type(evolve).__name__}')
        if not callable(observe):
            raise TypeError(
                f'observe must be callable, got {type(observe).__name__}')

        output_names = tuple(outputs)
        for output in output_names:
            if type(output) is not str or len(output) == 0:
                raise ValueError(
                    f'Output names must be non-empty str, got '
                    f'{output!r}')

        # name uniqueness across all three roles; roles remain
        # distinct namespaces for access, but a name may not appear
        # in two roles
        roles: Dict[str, str] = {}
        for role, mapping in (('input', inputs), ('state', states)):
            for var_name in mapping.keys():
                if type(var_name) is not str or len(var_name) == 0:
                    raise ValueError(
                        f'{role.capitalize()} variable names must be '
                        f'non-empty str, got {var_name!r}')
                if var_name in roles:
                    raise ValueError(
                        f'Name {var_name!r} is declared as both '
                        f'{roles[var_name]} and {role}')
                roles[var_name] = role
        if len(set(output_names)) != len(output_names):
            dup = next(
                candidate for candidate in output_names
                if output_names.count(candidate) > 1)
            raise ValueError(f'Duplicate output name {dup!r}')
        for output in output_names:
            if output in roles:
                raise ValueError(
                    f'Name {output!r} is declared as both '
                    f'{roles[output]} and output')
            roles[output] = 'output'

        self._name: str = name
        self._evolve: Callable[
            [Dict[str, Any], Dict[str, Any]], Mapping[str, Any]] = evolve
        self._observe: Callable[
            [Dict[str, Any]], Mapping[str, Any]] = observe
        self._output_names: Tuple[str, ...] = output_names

        self._input_specs: Dict[str, _ValueSpec] = {}
        self._state_specs: Dict[str, _ValueSpec] = {}
        # source per variable: a callable factory, or None for a
        # literal-declared initial value
        self._input_sources: Dict[str, Optional[Callable[[], Any]]] = {}
        self._state_sources: Dict[str, Optional[Callable[[], Any]]] = {}
        # pristine copies retained ONLY for literal-declared initial
        # values; factory-declared variables retain nothing to copy
        self._pristine_inputs: Dict[str, Any] = {}
        self._pristine_states: Dict[str, Any] = {}

        committed_inputs: Dict[str, Any] = {}
        committed_states: Dict[str, Any] = {}
        for role, mapping, specs, sources, pristines, committed in (
            ('input', inputs, self._input_specs, self._input_sources,
             self._pristine_inputs, committed_inputs),
            ('state', states, self._state_specs, self._state_sources,
             self._pristine_states, committed_states),
        ):
            for var_name, declared in mapping.items():
                spec, source, pristine, working = \
                    self._resolve_initial(var_name, declared)
                specs[var_name] = spec
                sources[var_name] = source
                if source is None:
                    pristines[var_name] = pristine
                committed[var_name] = working

        self._inputs: Dict[str, Any] = committed_inputs
        self._states: Dict[str, Any] = committed_states

    @staticmethod
    def _resolve_initial(
        name: str,
        declared: Any,
    ) -> Tuple[_ValueSpec, Optional[Callable[[], Any]], Any, Any]:
        """
        Resolve one declared initial value into its specification.

        Any callable declared value is unambiguously a factory (the
        value contract admits only exact ``int``/``float``/ndarray).
        For a literal-declared value, two independent copies are made:
        a pristine copy retained for :meth:`reset` and the working
        committed value. For a factory-declared value, nothing is
        retained to copy — the initial value is rebuilt by re-invoking
        the factory at each :meth:`reset`.

        Args:
        - name --- variable name, used in error messages
        - declared --- initial value or zero-argument factory

        Returns:
        - ``(spec, source, pristine, working)`` where ``source`` is the
          factory or ``None`` for a literal, ``pristine`` is the
          pristine copy or ``None`` for a factory, and ``working`` is
          the committed value

        Errors: factory exceptions propagate identically;
        :func:`_classify_value` errors for invalid values.

        Side-effects: invokes the factory once when declared is a
        factory.
        """
        if callable(declared):
            value = declared()  # original exception propagates as-is
            spec = _classify_value(value, name)
            return spec, declared, None, _copy_value(value)
        spec = _classify_value(declared, name)
        return spec, None, _copy_value(declared), _copy_value(declared)

    @property
    def name(self) -> str:
        """
        Object name (read-only).

        Returns: the non-empty name string.

        Errors: none.

        Side-effects: none; never invokes callbacks.
        """
        return self._name

    @property
    def input_names(self) -> Tuple[str, ...]:
        """
        Declared input names, in declaration order (read-only).

        Returns: a tuple of the input names.

        Errors: none.

        Side-effects: none; never invokes callbacks.
        """
        return tuple(self._input_specs.keys())

    @property
    def state_names(self) -> Tuple[str, ...]:
        """
        Declared state names, in declaration order (read-only).

        Returns: a tuple of the state names.

        Errors: none.

        Side-effects: none; never invokes callbacks.
        """
        return tuple(self._state_specs.keys())

    @property
    def output_names(self) -> Tuple[str, ...]:
        """
        Declared output names, in declaration order (read-only).

        Returns: a tuple of the output names.

        Errors: none.

        Side-effects: none; never invokes callbacks.
        """
        return self._output_names

    def set_input(self, name: str, value: Any) -> None:
        """
        Atomically replace one stored input (pending publication).

        Validates ``value`` against the variable's specification and
        copies it in, then atomically replaces that one entry of the
        input store. Validation and copying fully precede the store
        mutation, so a failed ``set_input`` leaves the input store,
        state and outputs exactly as before. Never invokes ``evolve``
        and never changes state or outputs; the new input takes effect
        at the next :meth:`evolve_once`. Inputs persist across
        evolutions (they represent a drive level, not a one-shot
        message).

        Args:
        - name --- declared input variable name
        - value --- candidate value, validated against the variable's
          pinned specification (exact scalar category; ndarray dtype
          and shape)

        Returns: None.

        Errors:
        - KeyError --- unknown input name, naming the variable
        - TypeError --- value outside the supported categories, or
          scalar category mismatch against the pinned specification
        - ValueError --- ndarray dtype/shape mismatch, or non-numeric
          ndarray dtype

        Side-effects: replaces one entry of the committed input store
        on success only; no callback invocation.
        """
        if name not in self._input_specs:
            raise KeyError(f'Unknown input variable {name!r}')
        prepared = _validate_against_spec(
            name, value, self._input_specs[name])
        self._inputs[name] = _copy_value(prepared)

    def get_input(self, name: str) -> Any:
        """
        Return a copy of the current stored input.

        After a failed :meth:`set_input`, returns the previous value.

        Args:
        - name --- declared input variable name

        Returns: a copy of the current stored input (a fresh ndarray
        copy for ndarray variables; the immutable scalar itself for
        scalar variables). No returned object aliases committed
        storage.

        Errors: KeyError --- unknown input name, naming the variable.

        Side-effects: none; no callback invocation.
        """
        if name not in self._input_specs:
            raise KeyError(f'Unknown input variable {name!r}')
        return _copy_value(self._inputs[name])

    def evolve_once(self) -> None:
        """
        Advance the model by exactly one evolution step.

        Invokes the ``evolve`` callback exactly once with fresh
        defensive copies of the current input store and the current
        state store (``x_next = evolve(u, x_previous)``), validates
        the returned next-state mapping — keys exactly the declared
        state names, each value validated against the variable's
        specification and copied in — then commits by swapping the
        state store (build-then-swap). Never invokes ``observe``.
        Returns ``None``; read results through :meth:`get_state` and
        :meth:`observe_outputs`.

        Args: none.

        Returns: None.

        Errors:
        - the identical exception object raised by the ``evolve``
          callback propagates (never caught, wrapped or replaced),
          including ``BaseException`` subclasses; committed state and
          the input store are left untouched
        - TypeError --- the callback returned a non-mapping
        - ValueError --- the result keys are not exactly the declared
          state names (message names the discrepancy)
        - TypeError/ValueError --- a returned value violates the
          variable's specification (the mapping is validated fully
          before any commit, so nothing is partially published)

        Side-effects: on success, replaces the committed state store
        with the validated next states; exactly one ``evolve``
        invocation per call and from nowhere else.
        """
        evolve_inputs = {
            var: _copy_value(value)
            for var, value in self._inputs.items()
        }
        previous_states = {
            var: _copy_value(value)
            for var, value in self._states.items()
        }
        result = self._evolve(evolve_inputs, previous_states)
        next_states = self._validate_callback_result(
            result, self._state_specs, 'evolve')
        self._states = next_states

    def get_state(self, name: str) -> Any:
        """
        Return a copy of the named committed state.

        Pure: no callback invocation, no mutation; never evolves.

        Args:
        - name --- declared state variable name

        Returns: a copy of the named committed state (a fresh ndarray
        copy for ndarray variables; the immutable scalar itself for
        scalar variables). No returned object aliases committed
        storage.

        Errors: KeyError --- unknown state name, naming the variable.

        Side-effects: none; no callback invocation.
        """
        if name not in self._state_specs:
            raise KeyError(f'Unknown state variable {name!r}')
        return _copy_value(self._states[name])

    def observe_outputs(self) -> Dict[str, Any]:
        """
        Invoke the observation callback once and return all outputs.

        Invokes ``observe`` (G) exactly once per call with a fresh copy
        of the committed state, validates the returned mapping — keys
        exactly the declared output names; values validated against
        the supported value contract (category and ndarray
        numeric-dtype checks; outputs are derived data with no
        per-output fixed spec) — and returns them in a fresh ``dict``
        with copied-out values. Never touches the input or state
        stores, so repeated calls are stable under a deterministic
        ``G``. Observation is all-outputs-at-once by design; there is
        no per-output observation method.

        Args: none.

        Returns: a new dict object with fresh output values on every
        call; no returned object aliases committed storage.

        Errors:
        - the identical exception object raised by the ``observe``
          callback propagates (never caught, wrapped or replaced);
          nothing is mutated, prior observations are unaffected
        - TypeError --- the callback returned a non-mapping
        - ValueError --- the result keys are not exactly the declared
          output names (message names the discrepancy)
        - TypeError/ValueError --- a returned value violates the
          supported value contract

        Side-effects: exactly one ``observe`` invocation per call; no
        committed store is touched.
        """
        states = {
            var: _copy_value(value)
            for var, value in self._states.items()
        }
        result = self._observe(states)
        return self._validate_callback_result(
            result, None, 'observe')

    def reset(self) -> None:
        """
        Rebuild the complete initial condition (build-then-swap).

        Restores declared initial inputs and states: fresh copies of
        pristine literal initial values, fresh factory invocations for
        factory-declared variables (in declaration order). Executes in
        two phases: **build** — produce and validate every candidate
        value into fresh local dicts, with the committed stores
        untouched — then **swap** — rebind the internal input store
        and state store. Never invokes ``evolve`` or ``observe``. On
        success the object is behaviorally identical to a newly
        constructed one, including any time inputs/states.

        Any exception during the build phase — a raising factory (the
        documented injection point) or a factory result that violates
        the specification — propagates as the identical original
        exception object and leaves the object in its complete,
        unmodified pre-reset committed state: inputs, states and
        therefore all subsequent observations are exactly as before
        the call, and the object remains fully usable
        (:meth:`set_input`, :meth:`evolve_once`,
        :meth:`observe_outputs` and a later :meth:`reset` all behave
        normally). No rollback path exists because no mutation
        precedes the swap. External side effects of user factories are
        outside this guarantee.

        Args: none.

        Returns: None.

        Errors: any factory or validation exception propagates
        identically (never caught, wrapped or replaced).

        Side-effects: on success, rebinds the committed input and
        state stores; at most one factory invocation per
        factory-declared variable; no callback invocation.
        """
        new_inputs: Dict[str, Any] = {}
        for var, spec in self._input_specs.items():
            new_inputs[var] = self._reset_value(
                var, spec, self._input_sources[var],
                self._pristine_inputs)
        new_states: Dict[str, Any] = {}
        for var, spec in self._state_specs.items():
            new_states[var] = self._reset_value(
                var, spec, self._state_sources[var],
                self._pristine_states)
        self._inputs = new_inputs
        self._states = new_states

    def _reset_value(
        self,
        name: str,
        spec: _ValueSpec,
        source: Optional[Callable[[], Any]],
        pristines: Dict[str, Any],
    ) -> Any:
        """
        Build one candidate reset value (build phase of reset).

        Args:
        - name --- variable name, used in error messages
        - spec --- pinned value specification
        - source --- factory for factory-declared variables, else None
        - pristines --- pristine literal copies for this variable role

        Returns: a validated, copied-in candidate value.

        Errors: factory exceptions propagate identically; validation
        errors from :func:`_validate_against_spec`.

        Side-effects: invokes the factory once when source is not
        None.
        """
        if source is not None:
            value = source()  # original exception propagates as-is
        else:
            value = _copy_value(pristines[name])
        prepared = _validate_against_spec(name, value, spec)
        return _copy_value(prepared)

    def _validate_callback_result(
        self,
        result: Any,
        specs: Optional[Mapping[str, _ValueSpec]],
        callback_name: str,
    ) -> Dict[str, Any]:
        """
        Validate an ``evolve``/``observe`` callback result.

        Checks the result is a mapping whose keys are exactly the
        declared names, then validates and copies in every value —
        against the per-variable pinned specification for ``evolve``
        results, or against the supported value contract for
        ``observe`` results (outputs are derived data with no fixed
        per-output spec).

        Args:
        - result --- the object returned by the user callback
        - specs --- pinned per-variable specifications (``evolve``),
          or None to validate against the value contract (``observe``)
        - callback_name --- 'evolve' or 'observe', used in messages

        Returns: a fresh dict of validated, copied-in values, ready to
        be committed or returned.

        Errors:
        - TypeError --- result is not a mapping
        - ValueError --- result keys are not exactly the declared
          names (message names the missing/extra discrepancy)
        - TypeError/ValueError --- a value violates its specification
          or the value contract

        Side-effects: none; the user callback has already run (its
        own exceptions propagate identically before this is called).
        """
        if not isinstance(result, _Mapping):
            raise TypeError(
                f'The {callback_name} callback must return a mapping, '
                f'got {type(result).__name__}')
        expected = set(
            self._state_specs.keys() if specs is not None
            else self._output_names)
        actual = set(result.keys())
        if actual != expected:
            missing = sorted(expected - actual)
            extra = sorted(actual - expected)
            raise ValueError(
                f'The {callback_name} callback must return exactly '
                f'the declared '
                f'{"state" if specs is not None else "output"} names; '
                f'missing {missing}, extra {extra}')
        validated: Dict[str, Any] = {}
        names = self._state_specs.keys() if specs is not None \
            else self._output_names
        for var in names:
            value = result[var]
            if specs is not None:
                prepared = _validate_against_spec(
                    var, value, specs[var])
            else:
                _classify_value(value, var)  # contract check only
                prepared = value
            validated[var] = _copy_value(prepared)
        return validated
