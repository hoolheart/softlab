"""Coordinated simulation: endpoint specification and coordinator.

This module realizes the R3-003 coordinated-simulation design: the
:class:`Endpoint` membership reference and the :class:`Coordinator`,
the opt-in owner of participant enrollment, connections, the committed
source snapshot, delivered measurement samples, the shared clock and
the coordinated two-state (STOPPED/RUNNING) lifecycle.

Design invariants implemented here (approved detailed design,
``log/release_3/design/r3-003-detailed-design.md``):

- **Publication is the only writer.** Every tick, prepare and reset is
  a two-phase transaction: a candidate phase (the only place user code
  runs) produces detached candidate values, and a publication phase of
  pure dict/attribute rebinds commits them. There is no rollback path
  because no mutation precedes publication.
- **One committed snapshot per boundary.** Edges read exclusively the
  committed source snapshot ``S_k`` (all enrolled participants'
  outputs) plus the frozen pending control values; candidate outputs
  are unobservable until publication, so no same-tick propagation
  exists by construction.
- **Exhaustive copies.** Every boundary crossing copies ndarray values
  (reusing ``object.py``'s private helpers — deliberate intra-package
  reuse), including one fresh copy per fan-out destination.
- **Canonical ordering.** Every candidate-phase iteration is sorted by
  name; insertion and registration order cannot influence results.
- **Reentrancy.** Every coordinator operation checks ``_busy`` and
  holds the flag for its whole duration (cleared in ``finally``);
  mock-control writes and the station ``sim_dt`` setter consult the
  flag too, so no unspecified window remains.

The station reference is stored untyped and duck-checked (``_objects``
dict, ``sim_dt`` property, ``_coordinator`` slot), so this module never
imports ``softlab.tu.station`` (design D1). Stdlib + NumPy only.
"""

import math
import numbers
from typing import (
    Any,
    Dict,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from softlab.tu.simulation.mock import (
    MockControl,
    MockMeasurement,
)
# Intra-package reuse of the object value-copy helper (deliberate; see
# module docstring and the design's Implementation Notes).
from softlab.tu.simulation.object import _copy_value


def _validate_interval(value: Any) -> float:
    """
    Validate and coerce one candidate simulation interval.

    Implements design D6 steps 1-4 (the documented
    coercion/representability rule): non-bool ``numbers.Real`` values
    are coerced to the IEEE-754 double nearest the original value via
    ``float()`` (so ``Decimal`` is rejected with ``TypeError`` — it is
    not registered as ``numbers.Real`` — while NumPy real scalars and
    ``Fraction`` are accepted through coercion); booleans, non-real
    values, non-finite doubles and non-positive doubles are rejected.
    Shared by the ``Station.sim_dt`` setter and ``Coordinator``
    re-validation.

    Args:
    - value --- candidate interval

    Returns:
    - the coerced finite positive ``float`` interval

    Errors:
    - TypeError --- ``bool`` or any non-``numbers.Real`` value
    - ValueError --- NaN, ±inf, zero or negative value

    Side-effects: none.
    """
    if type(value) is bool:
        raise TypeError(
            'sim_dt must be a positive finite real number; booleans '
            'are rejected')
    if not isinstance(value, numbers.Real):
        raise TypeError(
            f'sim_dt must be a positive finite real number, got '
            f'{type(value).__name__}')
    interval = float(value)
    if not math.isfinite(interval):
        raise ValueError(f'sim_dt must be finite, got {value!r}')
    if interval <= 0.0:
        raise ValueError(f'sim_dt must be positive, got {value!r}')
    return interval


class _EndpointBase(NamedTuple):
    participant: str
    signal: Optional[str] = None


class Endpoint(_EndpointBase):
    """
    Stable, hashable specification of a connection endpoint.

    ``(object_name, signal_name)`` addresses an enrolled object's
    declared input or output; ``(endpoint_name, None)`` addresses an
    enrolled mock control or measurement. Because object names are
    read-only and mock endpoint names are read-only by construction,
    the pair is a stable membership reference. ``Endpoint`` is hashable
    and keys the coordinator's edge map (destination -> source), so
    single-driver-per-destination is dict-key uniqueness.

    **Resolution rule (design D3):** an ``Endpoint`` with
    ``signal=None`` resolves in the mock-endpoint namespace; one with a
    ``str`` signal resolves to an enrolled object's declared signal.
    The two namespaces are disjoint, so a mock endpoint may share a
    name with an enrolled object — disambiguated by signal presence.
    Direction/kind semantics are assigned by
    :meth:`Coordinator.connect`, not by ``Endpoint`` itself.

    Validation happens here, at construction, before any coordinator
    contact: ``participant`` must be a non-empty ``str``; ``signal``
    must be ``None`` or a non-empty ``str``.

    Args:
    - participant --- enrolled object name or mock endpoint name
    - signal --- object signal name, or None for mock endpoints

    Errors:
    - TypeError --- non-string ``participant``/``signal``
    - ValueError --- empty string ``participant``/``signal``

    Side-effects: none.
    """

    __slots__ = ()

    def __new__(
        cls,
        participant: str,
        signal: Optional[str] = None,
    ) -> 'Endpoint':
        """Create a validated endpoint; see class docstring."""
        if type(participant) is not str:
            raise TypeError(
                f'Endpoint participant must be a str, got '
                f'{type(participant).__name__}')
        if len(participant) == 0:
            raise ValueError('Endpoint participant must be a non-empty str')
        if signal is not None:
            if type(signal) is not str:
                raise TypeError(
                    f'Endpoint signal must be a str or None, got '
                    f'{type(signal).__name__}')
            if len(signal) == 0:
                raise ValueError(
                    'Endpoint signal must be a non-empty str or None')
        return super().__new__(cls, participant, signal)


def _endpoint_key(endpoint: Endpoint) -> Tuple[str, str]:
    """Canonical sort key: never compares None with str (design D8)."""
    return (endpoint.participant,
            '' if endpoint.signal is None else endpoint.signal)


class Coordinator:
    """
    Station-associated, opt-in owner of a coordinated simulation.

    A coordinator enrolls station objects (claiming exclusive ownership
    of each), owns mock control/measurement endpoints, connections and
    interval bindings, and drives the shared clock. Construction is
    ``Coordinator(station)`` — one coordinator per station, permanent
    (no retirement API); ordinary station use never constructs one.

    **Lifecycle (design D5):** two states, explicit transitions —
    STOPPED (graph mutable; ``step`` rejected) and RUNNING (after a
    successful :meth:`prepare`; graph frozen; ``sim_dt`` fixed). A
    successful :meth:`reset` republishes tick 0 / time 0.0 and returns
    to STOPPED; reset before the first successful prepare is rejected.
    The clock (``tick``/``sim_time``) is readable only once started.

    **Reentrancy rule (design P2-2):** every coordinator operation
    checks ``_busy`` and holds the flag for its whole duration,
    clearing it in ``finally``; nested calls (including mock-control
    writes and ``sim_dt`` changes reached from callbacks) are rejected
    with ``RuntimeError``. ``_busy`` is set only after the
    operation-specific precondition checks, so those keep raising
    their own specific errors.

    **Clock policy (design D7):** ``tick`` is an exact unbounded
    ``int``; ``sim_time`` is computed, never accumulated —
    ``sim_time == float(tick) * sim_dt`` exactly, so roundoff is
    bounded by one int-to-float conversion rounding plus one
    multiplication rounding (no accumulation drift). A candidate time
    that is not finite fails the tick atomically with
    ``OverflowError`` before any publication.

    **Safe detach (design D12):** ``disconnect`` -> ``withdraw`` ->
    ``Station.rm_object``; every removal path is gated on reference
    absence, so no dangling reference can arise by construction. All
    graph edits are validate-fully-then-mutate-once and there is
    deliberately no batch API.

    Args:
    - station --- the associated station; duck-checked (must expose an
      ``_objects`` dict, a ``sim_dt`` property and a ``_coordinator``
      slot)

    Errors:
    - TypeError --- ``station`` fails the duck check
    - RuntimeError --- the station already has a coordinator

    Side-effects: on success only, registers itself in
    ``station._coordinator``; never creates or mutates any participant.
    """

    def __init__(self, station: Any) -> None:
        """Create and register a coordinator; see class docstring."""
        if not isinstance(getattr(station, '_objects', None), dict):
            raise TypeError(
                'Coordinator requires a station exposing an _objects '
                f'dict, got {type(station).__name__}')
        if not hasattr(station, 'sim_dt') or \
                not hasattr(station, '_coordinator'):
            raise TypeError(
                'Coordinator requires a station exposing sim_dt and '
                '_coordinator attributes')
        if station._coordinator is not None:
            raise RuntimeError(
                'Station already has a coordinator; one coordinator '
                'per station, for the station lifetime')
        self._station: Any = station
        self._participants: Dict[str, Any] = {}
        self._controls: Dict[str, MockControl] = {}
        self._measurements: Dict[str, MockMeasurement] = {}
        # destination Endpoint -> source Endpoint; the dict key is the
        # single-driver rule
        self._edges: Dict[Endpoint, Endpoint] = {}
        # destination Endpoint -> resolved (source_ref, signal) pair;
        # identity survives anything except explicit disconnect + detach
        self._resolved: Dict[Endpoint, Tuple[Any, Optional[str]]] = {}
        # (object_name, input_name) interval bindings (design D11)
        self._bindings: Set[Tuple[str, str]] = set()
        # committed source snapshot S_k: every enrolled participant's
        # output Endpoints -> validated committed values; written only
        # at publication; control values are NOT stored here
        self._snapshot: Dict[Endpoint, Any] = {}
        self._tick: int = 0
        self._sim_time: float = 0.0
        self._prepared: bool = False
        self._started: bool = False
        self._busy: bool = False
        station._coordinator = self

    # ------------------------------------------------------------------
    # introspection
    # ------------------------------------------------------------------

    @property
    def station(self) -> Any:
        """
        The associated station (untyped, duck-checked).

        Returns: the station passed to the constructor.

        Errors: none.

        Side-effects: none.
        """
        return self._station

    @property
    def prepared(self) -> bool:
        """
        Whether the coordinator is RUNNING (True) or STOPPED (False).

        Returns: the committed lifecycle state.

        Errors: none.

        Side-effects: none; never prepares, evolves or ticks.
        """
        return self._prepared

    @property
    def tick(self) -> int:
        """
        Committed tick count: exact, unbounded Python int from 0.

        Readable only after the first successful ``prepare()`` (a
        failed tick-zero leaves no tick — nothing was committed).

        Returns: the committed tick.

        Errors: RuntimeError --- 'simulation not started' before the
        first successful prepare.

        Side-effects: none.
        """
        if not self._started:
            raise RuntimeError('Simulation not started')
        return self._tick

    @property
    def sim_time(self) -> float:
        """
        Committed simulated time.

        **Multiplication policy (design D7):** ``sim_time`` equals the
        Python expression ``float(tick) * sim_dt`` exactly — computed
        fresh at each publication, never accumulated — so the result is
        deterministic and free of accumulation drift. Readable only
        after the first successful ``prepare()``.

        Returns: the committed simulated time.

        Errors: RuntimeError --- 'simulation not started' before the
        first successful prepare.

        Side-effects: none.
        """
        if not self._started:
            raise RuntimeError('Simulation not started')
        return self._sim_time

    @property
    def participants(self) -> Sequence[str]:
        """
        Live key view of enrolled object names.

        Returns: a view of the enrolled participant names.

        Errors: none.

        Side-effects: none; never invokes callbacks.
        """
        return self._participants.keys()

    @property
    def connections(self) -> Tuple[Tuple[Endpoint, Endpoint], ...]:
        """
        Detached, canonically sorted ``(source, dest)`` edge listing.

        This is the graph-inspection surface used by atomicity
        assertions: after any rejected graph edit the listing is
        bit-identical to its value before the attempt.

        Returns: a new tuple of endpoint pairs, sorted by the canonical
        endpoint key.

        Errors: none.

        Side-effects: none.
        """
        return tuple(sorted(
            ((source, dest) for dest, source in self._edges.items()),
            key=lambda pair: (_endpoint_key(pair[0]),
                              _endpoint_key(pair[1]))))

    # ------------------------------------------------------------------
    # endpoint resolution (design D3)
    # ------------------------------------------------------------------

    def _resolve_source(self, endpoint: Endpoint) -> Tuple[Any, str]:
        """
        Resolve a source endpoint to ``(source_ref, signal)``.

        ``signal=None`` resolves in the mock-endpoint namespace (a
        control; a measurement is the wrong kind); a ``str`` signal
        resolves to an enrolled object's declared **output**.
        """
        if endpoint.signal is None:
            if endpoint.participant in self._controls:
                return self._controls[endpoint.participant], 'control'
            if endpoint.participant in self._measurements:
                raise ValueError(
                    f'Mock measurement {endpoint.participant!r} cannot '
                    'be used as a connection source')
            raise KeyError(
                f'No enrolled mock control named '
                f'{endpoint.participant!r}')
        obj = self._participants.get(endpoint.participant)
        if obj is None:
            raise KeyError(
                f'No enrolled participant named '
                f'{endpoint.participant!r}')
        if endpoint.signal in obj.output_names:
            return obj, endpoint.signal
        if endpoint.signal in obj.input_names:
            raise ValueError(
                f'Signal {endpoint.signal!r} of participant '
                f'{endpoint.participant!r} is an input and cannot be '
                'used as a connection source')
        raise KeyError(
            f'Participant {endpoint.participant!r} declares no '
            f'signal {endpoint.signal!r}')

    def _resolve_dest(self, endpoint: Endpoint) -> Any:
        """
        Resolve a destination endpoint to the live destination ref.

        ``signal=None`` resolves in the mock-endpoint namespace (a
        measurement; a control is the wrong kind); a ``str`` signal
        resolves to an enrolled object's declared **input**.
        """
        if endpoint.signal is None:
            if endpoint.participant in self._measurements:
                return self._measurements[endpoint.participant]
            if endpoint.participant in self._controls:
                raise ValueError(
                    f'Mock control {endpoint.participant!r} cannot be '
                    'used as a connection destination')
            raise KeyError(
                f'No enrolled mock measurement named '
                f'{endpoint.participant!r}')
        obj = self._participants.get(endpoint.participant)
        if obj is None:
            raise KeyError(
                f'No enrolled participant named '
                f'{endpoint.participant!r}')
        if endpoint.signal in obj.input_names:
            return obj
        if endpoint.signal in obj.output_names:
            raise ValueError(
                f'Signal {endpoint.signal!r} of participant '
                f'{endpoint.participant!r} is an output and cannot be '
                'used as a connection destination')
        raise KeyError(
            f'Participant {endpoint.participant!r} declares no '
            f'signal {endpoint.signal!r}')

    def _is_driven(self, object_name: str, input_name: str) -> bool:
        """Whether the input is driven by an edge or a binding."""
        if (object_name, input_name) in self._bindings:
            return True
        for dest in self._edges:
            if dest.participant == object_name and \
                    dest.signal == input_name:
                return True
        return False

    # ------------------------------------------------------------------
    # enrollment (design D4)
    # ------------------------------------------------------------------

    def enroll(self, object_name: str) -> None:
        """
        Enroll a station object, claiming exclusive ownership.

        The claim spans the whole enrollment — STOPPED and RUNNING
        alike — and is released only by :meth:`withdraw` (design D4);
        it is claimed here, at enrollment, so conflicts are
        deterministic under every interleaving. Claimed objects reject
        standalone ``evolve_once``/``reset`` and driven-input
        ``set_input``; read access stays safe.

        Check order: stopped state, reentrancy, membership, duplicate,
        foreign claim; the claim and the enrollment record are the last
        mutations.

        Args:
        - object_name --- name of a station object member

        Returns: None.

        Errors:
        - RuntimeError --- RUNNING (graph frozen), or operation in
          progress
        - KeyError --- not a station object member
        - ValueError --- already enrolled in this coordinator
        - RuntimeError --- owned by another active coordinated
          simulation

        Side-effects: on success only, sets the object's ownership slot
        and records the participant; no callback invocation.
        """
        if self._prepared:
            raise RuntimeError(
                'Enrollment requires the stopped state; reset first')
        if self._busy:
            raise RuntimeError(
                'Coordinator operation already in progress')
        obj = self._station._objects.get(object_name)
        if obj is None:
            raise KeyError(
                f'No station object member named {object_name!r}')
        if object_name in self._participants:
            raise ValueError(
                f'Participant {object_name!r} is already enrolled '
                'in this coordinator')
        owner = getattr(obj, '_active_coordinator', None)
        if owner is not None and owner is not self:
            raise RuntimeError(
                f'Object {object_name!r} is owned by another active '
                'coordinated simulation; withdraw it there first')
        self._busy = True
        try:
            obj._active_coordinator = self
            self._participants[object_name] = obj
        finally:
            self._busy = False

    def withdraw(self, object_name: str) -> None:
        """
        Release a participant's ownership and enrollment.

        **Safe-detach sequence (design D12):** ``disconnect`` every
        edge onto/from the participant and ``unbind_interval`` every
        binding first — withdrawal is rejected while any edge or
        binding references the participant. Only then does
        ``Station.rm_object`` succeed. After withdrawal the object's
        standalone ``evolve_once``/``reset``/``set_input`` are
        available again.

        Check order: stopped state, reentrancy, enrollment membership,
        reference absence; the release is the last mutation.

        Args:
        - object_name --- an enrolled participant name

        Returns: None.

        Errors:
        - RuntimeError --- RUNNING, operation in progress, or edges /
          bindings still reference the participant
        - KeyError --- not enrolled in this coordinator

        Side-effects: on success only, clears the object's ownership
        slot and removes the enrollment record; no cleanup of values,
        no callback invocation.
        """
        if self._prepared:
            raise RuntimeError(
                'Withdrawal requires the stopped state; reset first')
        if self._busy:
            raise RuntimeError(
                'Coordinator operation already in progress')
        obj = self._participants.get(object_name)
        if obj is None:
            raise KeyError(
                f'No enrolled participant named {object_name!r}')
        referenced = any(
            (dest.participant == object_name and
             dest.signal is not None) or
            (source.participant == object_name and
             source.signal is not None)
            for dest, source in self._edges.items())
        bound = any(pname == object_name
                    for pname, _ in self._bindings)
        if referenced or bound:
            raise RuntimeError(
                f'Cannot withdraw {object_name!r}: connections or '
                'interval bindings still reference it; disconnect and '
                'unbind first')
        self._busy = True
        try:
            del self._participants[object_name]
            obj._active_coordinator = None
        finally:
            self._busy = False

    # ------------------------------------------------------------------
    # mock endpoint membership (design D2)
    # ------------------------------------------------------------------

    def add_control(self, control: MockControl) -> None:
        """
        Enroll a mock control endpoint (STOPPED only).

        Check order: stopped state, reentrancy, type, duplicate name in
        the shared mock-endpoint namespace, foreign enrollment; linking
        the endpoint back to this coordinator is the last mutation.

        Args:
        - control --- the :class:`MockControl` to enroll

        Returns: None.

        Errors:
        - RuntimeError --- RUNNING or operation in progress
        - TypeError --- not a :class:`MockControl`
        - ValueError --- duplicate endpoint name
        - RuntimeError --- endpoint enrolled in another coordinator

        Side-effects: on success only, enrolls the endpoint and sets
        its coordinator link.
        """
        if self._prepared:
            raise RuntimeError(
                'Endpoint changes require the stopped state; reset '
                'first')
        if self._busy:
            raise RuntimeError(
                'Coordinator operation already in progress')
        if not isinstance(control, MockControl):
            raise TypeError(
                f'Expected a MockControl, got {type(control).__name__}')
        if control.name in self._controls or \
                control.name in self._measurements:
            raise ValueError(
                f'Mock endpoint name {control.name!r} is already '
                'enrolled')
        if control._coordinator is not None and \
                control._coordinator is not self:
            raise RuntimeError(
                f'Mock endpoint {control.name!r} is enrolled in '
                'another coordinator')
        self._busy = True
        try:
            self._controls[control.name] = control
            control._coordinator = self
        finally:
            self._busy = False

    def add_measurement(self, measurement: MockMeasurement) -> None:
        """
        Enroll a mock measurement endpoint (STOPPED only).

        Same check order and contract as :meth:`add_control`.

        Args:
        - measurement --- the :class:`MockMeasurement` to enroll

        Returns: None.

        Errors: as :meth:`add_control`.

        Side-effects: as :meth:`add_control`.
        """
        if self._prepared:
            raise RuntimeError(
                'Endpoint changes require the stopped state; reset '
                'first')
        if self._busy:
            raise RuntimeError(
                'Coordinator operation already in progress')
        if not isinstance(measurement, MockMeasurement):
            raise TypeError(
                f'Expected a MockMeasurement, got '
                f'{type(measurement).__name__}')
        if measurement.name in self._controls or \
                measurement.name in self._measurements:
            raise ValueError(
                f'Mock endpoint name {measurement.name!r} is already '
                'enrolled')
        if measurement._coordinator is not None and \
                measurement._coordinator is not self:
            raise RuntimeError(
                f'Mock endpoint {measurement.name!r} is enrolled in '
                'another coordinator')
        self._busy = True
        try:
            self._measurements[measurement.name] = measurement
            measurement._coordinator = self
        finally:
            self._busy = False

    def rm_control(self, name: str) -> Optional[MockControl]:
        """
        Remove a mock control endpoint (STOPPED only).

        Rejected while any edge references the endpoint; otherwise the
        endpoint is removed, its coordinator link is cleared and it is
        returned. Missing names keep the lenient ``None`` semantics of
        the station ``rm_*`` convention.

        Args:
        - name --- the enrolled control name

        Returns: the removed endpoint, or None if no such endpoint.

        Errors:
        - RuntimeError --- RUNNING, operation in progress, or an edge
          still references the endpoint

        Side-effects: on success only, removes the endpoint and clears
        its coordinator link.
        """
        if self._prepared:
            raise RuntimeError(
                'Endpoint changes require the stopped state; reset '
                'first')
        if self._busy:
            raise RuntimeError(
                'Coordinator operation already in progress')
        if name not in self._controls:
            return None
        marker = Endpoint(name)
        if any(dest == marker or source == marker
               for dest, source in self._edges.items()):
            raise RuntimeError(
                f'Cannot remove mock control {name!r}: a connection '
                'still references it; disconnect first')
        self._busy = True
        try:
            removed = self._controls.pop(name)
            removed._coordinator = None
        finally:
            self._busy = False
        return removed

    def rm_measurement(self, name: str) -> Optional[MockMeasurement]:
        """
        Remove a mock measurement endpoint (STOPPED only).

        Same check order and contract as :meth:`rm_control`.

        Args:
        - name --- the enrolled measurement name

        Returns: the removed endpoint, or None if no such endpoint.

        Errors: as :meth:`rm_control`.

        Side-effects: as :meth:`rm_control`.
        """
        if self._prepared:
            raise RuntimeError(
                'Endpoint changes require the stopped state; reset '
                'first')
        if self._busy:
            raise RuntimeError(
                'Coordinator operation already in progress')
        if name not in self._measurements:
            return None
        marker = Endpoint(name)
        if any(dest == marker or source == marker
               for dest, source in self._edges.items()):
            raise RuntimeError(
                f'Cannot remove mock measurement {name!r}: a '
                'connection still references it; disconnect first')
        self._busy = True
        try:
            removed = self._measurements.pop(name)
            removed._coordinator = None
        finally:
            self._busy = False
        return removed

    # ------------------------------------------------------------------
    # connections and interval bindings (design D3/D11/D12)
    # ------------------------------------------------------------------

    def connect(self, source: Endpoint, dest: Endpoint) -> None:
        """
        Add one edge, destination driven by source (STOPPED only).

        **Resolution rule (design D3):** a ``signal=None`` endpoint
        resolves in the mock-endpoint namespace; a ``str`` signal
        resolves to an enrolled object's declared signal. A source must
        resolve to an object **output** or a **control**; a destination
        to an object **input** or a **measurement**. Single-driver per
        destination holds by dict-key uniqueness: a second driver
        (edge or interval binding) on the same input is rejected — no
        implicit summation, coercion or conflict resolution.

        The edit is validate-fully-then-mutate-once (there is no batch
        API): after any rejection the graph is bit-identical, checkable
        via the :attr:`connections` listing.

        Check order: stopped state, reentrancy, argument types, source
        resolution, destination resolution, single-driver; storing the
        edge and its resolved references are the last mutations.

        Args:
        - source --- the driving endpoint
        - dest --- the driven endpoint

        Returns: None.

        Errors:
        - RuntimeError --- RUNNING or operation in progress
        - TypeError --- non-Endpoint arguments
        - KeyError --- unknown participant/signal/endpoint
        - ValueError --- wrong direction or kind, or destination
          already driven

        Side-effects: on success only, adds the edge and stores the
        resolved references; no callback invocation.
        """
        if self._prepared:
            raise RuntimeError(
                'Topology changes require the stopped state; reset '
                'first')
        if self._busy:
            raise RuntimeError(
                'Coordinator operation already in progress')
        if not isinstance(source, Endpoint):
            raise TypeError(
                f'Connection source must be an Endpoint, got '
                f'{type(source).__name__}')
        if not isinstance(dest, Endpoint):
            raise TypeError(
                f'Connection destination must be an Endpoint, got '
                f'{type(dest).__name__}')
        src_ref, src_signal = self._resolve_source(source)
        self._resolve_dest(dest)
        if dest in self._edges:
            raise ValueError(
                f'Input {dest!r} already has a driver; '
                'single-driver per destination')
        if dest.signal is not None and self._is_driven(
                dest.participant, dest.signal):
            raise ValueError(
                f'Input {dest!r} is interval-bound; single-driver '
                'per destination')
        self._busy = True
        try:
            self._edges[dest] = source
            self._resolved[dest] = (src_ref, src_signal)
        finally:
            self._busy = False

    def disconnect(self, dest: Endpoint) -> None:
        """
        Remove the edge keyed by ``dest`` (STOPPED only).

        ``dest`` identifies the edge uniquely (single-driver). Safe
        detach: ``disconnect`` -> ``withdraw`` -> ``Station.rm_object``.

        Check order: stopped state, reentrancy, edge presence; the
        removal is the last mutation.

        Args:
        - dest --- the driven endpoint of the edge to remove

        Returns: None.

        Errors:
        - RuntimeError --- RUNNING or operation in progress
        - KeyError --- no edge drives ``dest``

        Side-effects: on success only, removes the edge and its
        resolved references; no callback invocation.
        """
        if self._prepared:
            raise RuntimeError(
                'Topology changes require the stopped state; reset '
                'first')
        if self._busy:
            raise RuntimeError(
                'Coordinator operation already in progress')
        if dest not in self._edges:
            raise KeyError(f'No connection drives {dest!r}')
        self._busy = True
        try:
            del self._edges[dest]
            del self._resolved[dest]
        finally:
            self._busy = False

    def bind_interval(self, object_name: str, input_name: str) -> None:
        """
        Bind an object's float input to the shared interval (STOPPED).

        **Binding semantics (design D11):** the binding is an explicit
        declaration — nothing is ever inferred from a variable named
        ``dt`` or ``t``. At every transition the bound input's
        candidate value is ``float(sim_dt)`` (the live, mid-run
          immutable interval), overlaid before ``evolve``; a model
        accumulating ``t' = t + dt`` therefore observes
        ``t == k * sim_dt`` after ``k`` transitions. Reset restores the
        bound input to its declared initial value; the next transition
        delivers the interval again.

        A bound input is a *driven* input: an edge connection to it, a
        contradictory second binding, and ``set_input`` on it are all
        rejected (single-driver, extended to bindings).

        Check order: stopped state, reentrancy, enrollment, input
        declaration, float declaration, single-driver; adding the
        binding is the last mutation.

        Args:
        - object_name --- an enrolled participant name
        - input_name --- a declared input of that participant

        Returns: None.

        Errors:
        - RuntimeError --- RUNNING or operation in progress
        - KeyError --- unknown participant or input
        - TypeError --- input not float-declared (the delivered value
          is always ``float(sim_dt)``)
        - ValueError --- input already driven by an edge or binding

        Side-effects: on success only, adds the binding; no callback
        invocation.
        """
        if self._prepared:
            raise RuntimeError(
                'Binding changes require the stopped state; reset '
                'first')
        if self._busy:
            raise RuntimeError(
                'Coordinator operation already in progress')
        obj = self._participants.get(object_name)
        if obj is None:
            raise KeyError(
                f'No enrolled participant named {object_name!r}')
        if input_name not in obj.input_names:
            raise KeyError(
                f'Participant {object_name!r} declares no input '
                f'{input_name!r}')
        if obj._input_specs[input_name].category != 'float':
            raise TypeError(
                f'Input {input_name!r} of {object_name!r} is not '
                'float-declared; interval binding delivers '
                'float(sim_dt)')
        if self._is_driven(object_name, input_name):
            raise ValueError(
                f'Input {input_name!r} of {object_name!r} is already '
                'driven; single-driver per input')
        self._busy = True
        try:
            self._bindings.add((object_name, input_name))
        finally:
            self._busy = False

    def unbind_interval(self, object_name: str, input_name: str) -> None:
        """
        Remove an interval binding (STOPPED only).

        Check order: stopped state, reentrancy, binding presence; the
        removal is the last mutation.

        Args:
        - object_name --- an enrolled participant name
        - input_name --- a bound input of that participant

        Returns: None.

        Errors:
        - RuntimeError --- RUNNING or operation in progress
        - KeyError --- no such binding

        Side-effects: on success only, removes the binding; no callback
        invocation.
        """
        if self._prepared:
            raise RuntimeError(
                'Binding changes require the stopped state; reset '
                'first')
        if self._busy:
            raise RuntimeError(
                'Coordinator operation already in progress')
        if (object_name, input_name) not in self._bindings:
            raise KeyError(
                f'No interval binding on input {input_name!r} of '
                f'{object_name!r}')
        self._busy = True
        try:
            self._bindings.discard((object_name, input_name))
        finally:
            self._busy = False

    # ------------------------------------------------------------------
    # lifecycle: prepare / step / reset (design D8/D9/D10)
    # ------------------------------------------------------------------

    def prepare(self) -> None:
        """
        Tick-zero preparation: explicit, all-or-nothing (STOPPED only).

        Steps (design D9): (1) require ``station.sim_dt`` set and
        re-validate the stored value with the documented D6 rule;
        (2) candidate phase — per participant (sorted), exactly one
        ``observe`` invocation on the committed states computes the
        tick-zero outputs (no ``evolve``, no factory invocation);
        (3) validate every edge's tick-zero delivery against the
        destination input's pinned spec (an incompatible connection
        fails preparation, naming source and destination); (4)
        publication — the committed snapshot ``S_0`` := all
        tick-zero outputs, ``tick = 0``, ``sim_time = 0.0``, state :=
        RUNNING. Measurements are **not** delivered: their
        constructor-declared initial sample remains the tick-zero read.

        Failure anywhere publishes nothing: no tick, no sim-time, no
        state change, no ownership change — a later healthy
        ``prepare()`` works from untouched declarations, and the
        original callback exception propagates identically.

        Check order: already-prepared, reentrancy, interval presence
        and validation; publication is the only mutation.

        Args: none.

        Returns: None.

        Errors:
        - RuntimeError --- already prepared, operation in progress, or
          ``sim_dt`` unset
        - TypeError/ValueError --- ``sim_dt`` fails re-validation, or
          an edge's tick-zero delivery is incompatible
        - any exception raised by an ``observe`` callback propagates
          identically (never caught, wrapped or replaced)

        Side-effects: on success only, publishes the snapshot, the zero
        clock and the RUNNING state; exactly one ``observe`` invocation
        per participant and from nowhere else.
        """
        if self._prepared:
            raise RuntimeError(
                'Simulation is already prepared; reset before '
                'preparing again')
        if self._busy:
            raise RuntimeError(
                'Coordinator operation already in progress')
        sim_dt = self._station.sim_dt
        if sim_dt is None:
            raise RuntimeError(
                'Station sim_dt is not set; assign a positive finite '
                'interval before prepare')
        _validate_interval(sim_dt)  # live re-validation (design D6)
        self._busy = True
        try:
            outputs_c: Dict[str, Dict[str, Any]] = {}
            for name in sorted(self._participants):
                obj = self._participants[name]
                outputs_c[name] = obj._observe_candidate(obj._states)
            self._validate_edge_deliveries(outputs_c)
            # ---- publication: no user code below this line ----
            self._publish_snapshot(outputs_c)
            self._tick = 0
            self._sim_time = 0.0
            self._prepared = True
            self._started = True
        finally:
            self._busy = False

    def step(self) -> None:
        """
        Advance the network by exactly one transition (RUNNING only).

        One transition ``k -> k + 1`` (design D8):

        1. *Freeze* the boundary-``k`` sources: copies of every
           enrolled control's pending value (sorted) and of the
           committed snapshot ``S_k`` (all participant outputs).
        2. *Candidates* (the only user code): per participant (sorted),
           build the delivered inputs — one fresh copy per fan-out
           destination, each validated against the destination's pinned
           spec (a tick-time incompatibility fails the tick here,
           naming source and destination), plus ``float(sim_dt)`` for
           each interval-bound input — then exactly one ``evolve`` and
           one ``observe`` invocation.
        3. *Measurement candidates and clock candidate* (no user
           code): delivered samples from the frozen boundary copies;
           the candidate ``(tick + 1, float(tick + 1) * sim_dt)`` — if
           the product is not finite, the tick fails atomically with
           ``OverflowError`` before any publication.
        4. *Publication* (no user code): commit every participant's
           candidate inputs/states, rebuild ``S_{k+1}`` from the
           candidate outputs, publish delivered measurement samples,
           advance the clock.

        Pending control values persist across ticks (a control is a
        level); a transition only ever copies *from* the pending store,
        so accepted pending writes survive failed ticks. Because no
        candidate is observable before publication, no participant can
        read another's newly computed same-tick value, and any failure
        in phases 1-3 leaves the committed network and clock exactly as
        before with the original callback exception identity.

        The interval is read live from the station at each step; the
        setter's prepared/busy rejection makes caching unnecessary.

        Check order: prepared state, reentrancy; publication is the
        only mutation.

        Args: none.

        Returns: None.

        Errors:
        - RuntimeError --- not prepared or operation in progress
        - TypeError/ValueError --- a delivered value is incompatible
          with a destination input's pinned spec
        - OverflowError --- the candidate simulated time is not finite
          (the tick fails atomically)
        - any exception raised by an ``evolve``/``observe`` callback
          propagates identically (never caught, wrapped or replaced)

        Side-effects: on success only, commits every participant,
        rebuilds the snapshot, publishes measurement samples and
        advances the clock; exactly one ``evolve`` and one ``observe``
        invocation per participant per tick and from nowhere else.
        """
        if not self._prepared:
            raise RuntimeError(
                'Simulation is not prepared; call prepare() first')
        if self._busy:
            raise RuntimeError(
                'Coordinator operation already in progress')
        sim_dt = float(self._station.sim_dt)  # live read (design D6)
        self._busy = True
        try:
            # Phase 1 — freeze boundary-k sources (no user code)
            frozen_controls = {
                name: _copy_value(self._controls[name]._pending)
                for name in sorted(self._controls)}
            frozen_outputs = {
                endpoint: _copy_value(value)
                for endpoint, value in self._snapshot.items()}
            # Phase 2 — per-participant candidates (user code here)
            results: Dict[str, Tuple[Dict[str, Any], Dict[str, Any],
                                     Dict[str, Any]]] = {}
            sorted_edges = sorted(self._edges, key=_endpoint_key)
            for name in sorted(self._participants):
                obj = self._participants[name]
                delivered: Dict[str, Any] = {}
                for dest in sorted_edges:
                    if dest.participant != name or dest.signal is None:
                        continue
                    source = self._edges[dest]
                    value = self._frozen_edge_value(
                        source, frozen_controls, frozen_outputs)
                    try:
                        prepared = obj._validate_input_value(
                            dest.signal, value)
                    except (TypeError, ValueError) as exc:
                        raise type(exc)(
                            f'Incompatible delivery on connection '
                            f'{source!r} -> {dest!r}: {exc}') from exc
                    delivered[dest.signal] = _copy_value(prepared)
                for pname, input_name in sorted(self._bindings):
                    if pname == name:
                        delivered[input_name] = sim_dt
                results[name] = obj._step_candidate(delivered)
            # Phase 3 — measurement candidates + clock candidate
            delivered_samples: Dict[str, Any] = {}
            for dest in sorted_edges:
                if dest.signal is not None:
                    continue
                value = self._frozen_edge_value(
                    self._edges[dest], frozen_controls, frozen_outputs)
                delivered_samples[dest.participant] = _copy_value(value)
            next_tick = self._tick + 1
            next_time = float(next_tick) * sim_dt
            if not math.isfinite(next_time):
                raise OverflowError(
                    f'Simulated time overflows at tick {next_tick} '
                    f'with sim_dt {sim_dt!r}; the tick fails '
                    'atomically before any publication')
            # Phase 4 — publication (no user code)
            for name in sorted(self._participants):
                inputs_c, states_c, _ = results[name]
                self._participants[name]._commit(inputs_c, states_c)
            self._publish_snapshot(
                {name: results[name][2] for name in results})
            for mname in sorted(delivered_samples):
                self._measurements[mname]._delivered = \
                    delivered_samples[mname]
            self._tick = next_tick
            self._sim_time = next_time
        finally:
            self._busy = False

    def reset(self) -> None:
        """
        Coordinated reset: staged rebuild, publish zero, land STOPPED.

        Requires a previously successful ``prepare()``; reset before
        the first successful prepare is rejected. Steps (design D10):

        1. *Candidate phase* (user code): per participant (sorted),
           ``_reset_candidate()`` — fresh factory invocations for
           factory-declared variables, in declaration order, original
           exception identity — then one ``observe`` invocation on the
           candidate states. Nothing is committed.
        2. *Validate* all edge tick-zero deliveries against candidate
           outputs and restored control initials (same rule as
           ``prepare``).
        3. *Publication* (no user code): commit every participant's
           restored inputs/states; ``S_0`` := candidate outputs; every
           control ``_pending`` := copy of its constructor initial
           (accepted pending writes do **not** leak across reset);
           every measurement ``_delivered`` := copy of its constructor
           initial sample; ``tick = 0``; ``sim_time = 0.0``; state :=
           STOPPED. Bindings, edges, enrollment and ownership are
           retained — reset restores values, not topology.

        Any failure in phases 1-2 leaves the prior committed network —
        states, inputs, snapshot, delivered samples, pending controls,
        clock and lifecycle state — exactly as before and fully usable:
        a subsequent ``step()`` behaves as if the reset was never
        attempted (staging is all-or-nothing because publication is the
        only writer). **External side-effect limit:** atomicity covers
        library-owned simulation state only; factory/callback side
        effects are outside the guarantee, and reproducible
        observations require deterministic callbacks and reproducible
        factories.

        After a successful reset the clock reads zero, topology is
        editable, the interval is changeable, and a re-run
        (``prepare()`` + ``step()``) reproduces the original trace
        under deterministic callbacks/factories.

        Check order: started state, reentrancy; publication is the
        only mutation.

        Args: none.

        Returns: None.

        Errors:
        - RuntimeError --- never started or operation in progress
        - TypeError/ValueError --- an edge's delivery is incompatible
          with the restored values
        - any exception raised by a factory or callback propagates
          identically (never caught, wrapped or replaced)

        Side-effects: on success only, republishes the rebuilt network
        and the zero clock; at most one factory invocation per
        factory-declared variable; no ``evolve`` invocation.
        """
        if not self._started:
            raise RuntimeError(
                'Simulation has never been prepared; nothing to reset')
        if self._busy:
            raise RuntimeError(
                'Coordinator operation already in progress')
        self._busy = True
        try:
            results: Dict[str, Tuple[Dict[str, Any], Dict[str, Any],
                                     Dict[str, Any]]] = {}
            for name in sorted(self._participants):
                obj = self._participants[name]
                inputs_c, states_c = obj._reset_candidate()
                outputs_c = obj._observe_candidate(states_c)
                results[name] = (inputs_c, states_c, outputs_c)
            self._validate_edge_deliveries(
                {name: results[name][2] for name in results},
                use_control_initials=True)
            # ---- publication: no user code below this line ----
            for name in sorted(self._participants):
                inputs_c, states_c, _ = results[name]
                self._participants[name]._commit(inputs_c, states_c)
            self._publish_snapshot(
                {name: results[name][2] for name in results})
            for name in sorted(self._controls):
                control = self._controls[name]
                control._pending = _copy_value(control._initial)
            for name in sorted(self._measurements):
                measurement = self._measurements[name]
                measurement._delivered = _copy_value(
                    measurement._initial)
            self._tick = 0
            self._sim_time = 0.0
            self._prepared = False
        finally:
            self._busy = False

    # ------------------------------------------------------------------
    # private protocol consumed by SimulatedObject (design D4)
    # ------------------------------------------------------------------

    def _guard_set_input(self, obj: Any, name: str) -> None:
        """
        Guard for a claimed object's ``set_input`` (design D4).

        Raises ``RuntimeError`` if this coordinator is mid-operation
        (``_busy``) or ``name`` is a driven input of ``obj`` (an edge
        destination or an interval-bound input); returns otherwise, so
        unconnected inputs still stage pending values through the
        controlled boundary between ticks.

        Args:
        - obj --- the enrolled (claimed) object
        - name --- a declared input name of ``obj``

        Returns: None.

        Errors: RuntimeError --- mid-operation write or driven input.

        Side-effects: none.
        """
        if self._busy:
            raise RuntimeError(
                'Cannot set an input while the coordinator is '
                'mid-operation')
        if (obj.name, name) in self._bindings:
            raise RuntimeError(
                f'Input {name!r} of {obj.name!r} is interval-bound; '
                'it is driven by the shared sim_dt')
        for dest in self._edges:
            if dest.participant == obj.name and dest.signal == name:
                raise RuntimeError(
                    f'Input {name!r} of {obj.name!r} is driven by a '
                    'connection; disconnect it first')

    # ------------------------------------------------------------------
    # private helpers
    # ------------------------------------------------------------------

    def _edge_source_value(
            self,
            source: Endpoint,
            outputs_c: Dict[str, Dict[str, Any]],
            use_control_initials: bool = False) -> Any:
        """Copy of an edge's source value from candidates or controls.

        At ``prepare`` the source value is the tick-zero candidate
        output (or the control's current pending value); at ``reset``
        the control side is validated against the restored initials.
        """
        if source.signal is None:
            control = self._controls[source.participant]
            if use_control_initials:
                return _copy_value(control._initial)
            return _copy_value(control._pending)
        return _copy_value(outputs_c[source.participant][source.signal])

    def _validate_edge_deliveries(
            self,
            outputs_c: Dict[str, Dict[str, Any]],
            use_control_initials: bool = False) -> None:
        """
        Validate every edge's delivery against its destination spec.

        The tick-zero source value (candidate output or control value)
        is validated against each destination input's pinned spec; an
        incompatible connection fails with ``TypeError``/``ValueError``
        naming the source and destination endpoints. Measurement sinks
        accept any contract-valid value. Invokes no user code.
        """
        for dest in sorted(self._edges, key=_endpoint_key):
            if dest.signal is None:
                continue  # measurement sink: no per-variable spec
            source = self._edges[dest]
            value = self._edge_source_value(
                source, outputs_c, use_control_initials)
            obj = self._participants[dest.participant]
            try:
                obj._validate_input_value(dest.signal, value)
            except (TypeError, ValueError) as exc:
                raise type(exc)(
                    f'Incompatible connection {source!r} -> '
                    f'{dest!r}: {exc}') from exc

    @staticmethod
    def _frozen_edge_value(
            source: Endpoint,
            frozen_controls: Dict[str, Any],
            frozen_outputs: Dict[Endpoint, Any]) -> Any:
        """Copy of an edge's source value from the frozen boundary."""
        if source.signal is None:
            return _copy_value(frozen_controls[source.participant])
        return _copy_value(
            frozen_outputs[Endpoint(source.participant, source.signal)])

    def _publish_snapshot(
            self,
            outputs_c: Dict[str, Dict[str, Any]]) -> None:
        """
        Publish the committed source snapshot from candidate outputs.

        Cardinality (fixed by design): **all** enrolled participants'
        output endpoints -> validated committed values, whether or not
        the output currently sources an edge. Values are the already
        copied, validated candidates. Control values are never stored
        here — transitions read them fresh from the pending stores.
        Publication helper: pure dict rebinds, no user code.
        """
        snapshot: Dict[Endpoint, Any] = {}
        for name in sorted(outputs_c):
            for signal in sorted(outputs_c[name]):
                snapshot[Endpoint(name, signal)] = \
                    outputs_c[name][signal]
        self._snapshot = snapshot
