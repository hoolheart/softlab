"""Opt-in mock simulation endpoints.

This module defines the two mock endpoint kinds of the coordinated
simulation design (R3-003): :class:`MockControl`, a control **source**
owning a staged *pending* value, and :class:`MockMeasurement`, a
measurement **sink** owning the most recently *delivered* sample.

Both kinds are coordinator-owned signal ports: they are enrolled with a
:class:`~softlab.tu.simulation.coordination.Coordinator` via
``add_control``/``add_measurement`` — never added to a station as
devices — so no device ``Parameter`` hook is ever executed by the
coordinator and ``Parameter``/``Device`` behavior is untouched.

Values are validated against exactly the same closed value contract as
``SimulatedObject`` variables (exact ``int``, exact ``float``, exact
numeric ``np.ndarray``) by reusing the private helpers of
``softlab.tu.simulation.object`` — deliberate intra-package reuse, no
duplication. Writes and reads copy in/out, so no stored ndarray aliases
caller memory and vice versa. Per-destination compatibility is
validated at delivery time against each connected input's pinned spec,
not at write time (an endpoint does not know its destinations' specs).

NumPy + stdlib only; this module imports nothing from ``station``.
"""

from typing import Any, Optional

# Intra-package reuse of the object value contract (deliberate; see
# module docstring).
from softlab.tu.simulation.object import _classify_value, _copy_value


class MockControl:
    """
    Opt-in mock control source owning a pending (staged) value.

    A control is a *level*, not an event: writes stage the pending
    source value (last write wins), readback shows the pending value
    immediately, and the next coordinator transition samples and
    delivers the current pending value across the outgoing edge(s).
    Pending values persist across ticks and survive failed ticks,
    because a transition only ever copies *from* the pending store.

    Not a station ``Device``; enrolled with a coordinator via
    ``add_control``. While enrolled, ``write`` is rejected with
    ``RuntimeError`` if the owning coordinator is mid-operation
    (reentrancy guard).

    Args:
    - name --- non-empty endpoint name (read-only afterwards)
    - initial --- constructor initial value, validated against the
      value contract; stored twice as copies: ``_initial`` (pristine,
      for coordinated reset) and ``_pending``

    Errors:
    - ValueError --- empty or non-string ``name``
    - TypeError --- ``initial`` outside the supported value categories
    - ValueError --- ndarray initial with a non-numeric dtype

    Side-effects: none beyond the copies described above; never ticks,
    never touches any object or the clock.
    """

    def __init__(self, name: str, initial: Any) -> None:
        """Initialize a mock control; see class docstring."""
        if type(name) is not str or len(name) == 0:
            raise ValueError(
                f'Mock endpoint name must be a non-empty str, got '
                f'{name!r}')
        _classify_value(initial, name)  # contract check
        self._name: str = name
        self._initial: Any = _copy_value(initial)
        self._pending: Any = _copy_value(initial)
        # owning coordinator link; set/cleared only by add/rm; consulted
        # for the write busy-guard
        self._coordinator: Optional[Any] = None

    @property
    def name(self) -> str:
        """
        Endpoint name (read-only).

        Returns: the non-empty name string.

        Errors: none.

        Side-effects: none.
        """
        return self._name

    def write(self, value: Any) -> None:
        """
        Stage the pending source value (last write wins).

        Validates ``value`` against the value contract, copies it in,
        and replaces the pending store entry. Validation and copying
        fully precede the mutation, so a failed ``write`` leaves the
        pending store exactly as before. If enrolled and the owning
        coordinator is mid-operation (``_busy``), the write is rejected
        with ``RuntimeError`` (reentrancy guard). Per-destination
        compatibility is validated at delivery time, not here.

        Args:
        - value --- candidate value, validated against the value
          contract (exact int/float/numeric ndarray)

        Returns: None.

        Errors:
        - RuntimeError --- the owning coordinator is mid-operation
        - TypeError --- value outside the supported value categories
        - ValueError --- ndarray value with a non-numeric dtype

        Side-effects: on success only, replaces the pending store
        entry; never ticks, never touches any object or the clock.
        """
        coordinator = self._coordinator
        if coordinator is not None and coordinator._busy:
            raise RuntimeError(
                f'Cannot write mock control {self._name!r} while its '
                'coordinator is mid-operation')
        _classify_value(value, self._name)  # contract check
        self._pending = _copy_value(value)

    def read(self) -> Any:
        """
        Control readback: a copy of the current pending value.

        Pure; never ticks, never fetches from any other participant.

        Returns: a detached copy of the pending value (a fresh ndarray
        copy for ndarray values; the immutable scalar itself for
        scalar values).

        Errors: none.

        Side-effects: none.
        """
        return _copy_value(self._pending)


class MockMeasurement:
    """
    Opt-in mock measurement sink owning the delivered sample.

    A measurement owns the most recently delivered sample: the copy
    of the source value committed at the most recent transition
    publication. The **tick-zero sample is the explicitly initialized
    constructor value** — a measurement is *not* delivered during
    ``prepare()``; its constructor-declared initial sample remains the
    read result until the first transition publishes a delivery. Reads
    never fetch the current source directly, never bypass the edge
    delay and never advance time.

    Not a station ``Device``; enrolled with a coordinator via
    ``add_measurement``.

    Args:
    - name --- non-empty endpoint name (read-only afterwards)
    - initial --- the explicitly initialized tick-zero sample,
      validated against the value contract; stored twice as copies:
      ``_initial`` (pristine, for coordinated reset) and ``_delivered``

    Errors:
    - ValueError --- empty or non-string ``name``
    - TypeError --- ``initial`` outside the supported value categories
    - ValueError --- ndarray initial with a non-numeric dtype

    Side-effects: none beyond the copies described above; never ticks.
    """

    def __init__(self, name: str, initial: Any) -> None:
        """Initialize a mock measurement; see class docstring."""
        if type(name) is not str or len(name) == 0:
            raise ValueError(
                f'Mock endpoint name must be a non-empty str, got '
                f'{name!r}')
        _classify_value(initial, name)  # contract check
        self._name: str = name
        self._initial: Any = _copy_value(initial)
        self._delivered: Any = _copy_value(initial)
        # owning coordinator link; set/cleared only by add/rm
        self._coordinator: Optional[Any] = None

    @property
    def name(self) -> str:
        """
        Endpoint name (read-only).

        Returns: the non-empty name string.

        Errors: none.

        Side-effects: none.
        """
        return self._name

    def read(self) -> Any:
        """
        Return a copy of the most recently delivered sample.

        The constructor initial sample is returned until the first
        transition publishes a delivery. Pure; never fetches the
        current source, never bypasses the edge delay, never advances
        time.

        Returns: a detached copy of the delivered sample (a fresh
        ndarray copy for ndarray values; the immutable scalar itself
        for scalar values).

        Errors: none.

        Side-effects: none.
        """
        return _copy_value(self._delivered)
