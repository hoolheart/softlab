# softlab `tu` extensions user guide

## 1. Header

Scope: this guide documents the TU-002–TU-007 additions to the `tu`
(element) layer shipped in softlab 0.3.0 — versioned setup descriptions,
the device lifecycle contract, the explicit VISA operation path, opt-in
measurement readings, the VISA lifecycle and abort surface, and the
theory-model inspection/configuration contract. See `README.rst` for the
overall project layout. Every extension is opt-in; legacy callers keep
their established behavior (see "Migration-free guarantee" below).

## 2. Parameter measurement contract

`Parameter` (in `softlab.tu.station.parameter`, re-exported from
`softlab.tu.station`) gains an opt-in richer read beside the legacy
value-only `()` / `get()` paths:

- `read() -> Reading`: acquires through the identical chain as `get()`
  (permission check, `before_get` hook, encoder) and returns a frozen
  `Reading` value object. On success: `value` is the exact object `get()`
  returned (never copied), `acquired_at` is the `time.time()` epoch
  seconds stamped after the acquisition chain completed, `quality` is
  `'ok'`, `error` is `None`. On failure: `value` is `None`,
  `acquired_at` is `None`, `quality` is `'failed'` (the closed
  two-literal vocabulary is `{'ok', 'failed'}`), and `error` is the
  original exception object (identity preserved, never wrapped). The
  declared `uncertainty` and `calibration` constructor metadata are
  echoed on both paths.
- A permission failure (`gettable` is `False`) raises
  `RuntimeError('Parameter <name> is not gettable')` before any
  acquisition — identical type and message to legacy `get()`.
- `describe_reading() -> Dict[str, Any]`: versioned description
  (`schema_version` 1) of the reading surface — the constructor-declared
  optional `unit`, `value_type`, `shape`, `channel`, `uncertainty` and
  `calibration` declarations, echoed verbatim. Description lookup
  performs no acquisition.
- Six optional constructor keywords (`unit`, `value_type`, `shape`,
  `channel`, `uncertainty`, `calibration`, all defaulting to `None`)
  declare the metadata; they are stored verbatim, with no validation and
  no copying.

`Parameter.describe(metadata=None) -> Dict[str, Any]` (TU-002) returns a
fresh, JSON-compatible description of the parameter: `schema_version`
(1), `name`, `type` (module-qualified class name, descriptive only),
`metadata` (validated, recursively copied), `settable` and `gettable`.
It never reads the cached value, never calls a validator, codec or hook,
and performs no I/O. Metadata accepts exact built-in `dict` / `list` /
`str` / `int` / `float` / `bool` / `None` with `str` keys and finite
floats; unsupported types/keys raise `TypeError`, nonfinite floats and
self-referential containers raise `ValueError`.

Legacy value-only semantics are explicitly retained: `parameter()`,
`parameter.get()` and `parameter(value)` behave exactly as before, with
identical exception propagation and identical acquisition counts — the
`huo` count/scan consumption path is untouched.

## 3. Device lifecycle

`Device` (in `softlab.tu.station.device`) gains an opt-in lifecycle and
capability contract:

- `supports(capability: str) -> bool`: side-effect-free capability
  detection. The base answers `True` for `'prepare'` and `'cleanup'`,
  `False` for `'connection'` and for any unknown string (including the
  empty string); it never performs I/O and never raises. Subclasses may
  override additively
  (`return capability in ('trigger',) or super().supports(capability)`).
- `prepare() -> None`: runs the subclass hook `_prepare_impl()` and
  marks the device initialized. On an already-initialized device it is a
  no-op (the hook is not re-run). After a failed `prepare()`, call
  `cleanup()` first to discharge the pending partial acquisition before
  re-preparing; `prepare()` in that state is outside the contract. The
  plain base `Device` performs zero I/O at every point.
- `initialized -> bool`: readiness state.
- `cleanup() -> None`: releases the owned resource by running
  `_cleanup_impl()` exactly once per acquisition attempt; repeated calls
  are safe no-ops. Borrowed resources held by a device that never
  attempted acquisition are never touched.

`Device.describe(metadata=None)` follows the TU-002 shape with
`parameters` and `children` maps keyed by lookup key; a device active on
its own ancestry raises `ValueError`. The built-in traversal never
invokes a subclass `describe()` override and performs no I/O. The
lifecycle contract is single-threaded: no locking, no atomicity
guarantee across threads.

## 4. Station

`Station.describe(metadata=None)` aggregates the TU-002 description:
`schema_version` (1), `name`, `type`, `metadata` and a `devices` map
keyed by lookup key. It performs no device I/O and never opens a
connection.

## 5. VISA lifecycle and explicit operation path

`VisaHandle` (in `softlab.tu.station.visa`, re-exported from
`softlab.tu.station`) mirrors the lifecycle contract — it is not a
`Device` — and adds an explicit abort surface:

- `initialized -> bool`, `supports(capability)`, `prepare()`,
  `cleanup()`: lifecycle outcomes matching the TU-004 contract.
  `prepare()` re-opens through the retained resource manager and
  re-applies the full construction configuration; a failed re-open
  propagates the original exception object and leaves the handle
  cleaned-up. `cleanup()` releases an owned resource exactly once;
  `close()` is now an alias of `cleanup()`. A cleaned-up borrowed handle
  cannot re-open: `prepare()` raises
  `RuntimeError('Cannot re-open a borrowed visa resource')`.
- `timeout_seconds -> Optional[float]`: seconds-based timeout property,
  converting in both directions to the PyVISA millisecond convention.
  `None` disables the timeout. Default construction forwards
  `timeout_seconds * 1000` milliseconds (5000 ms); an explicit raw
  `timeout` (constructor, set or get) is forwarded unchanged and never
  rescaled (the resolved OBS-001 semantics).
- `abort() -> AbortReport`: records an abort request and returns an
  advisory report immediately — bookkeeping only: no I/O, no waiting, no
  operation lock. In-flight → `AbortReport(aborted=True,
  stopped=False)`; idle → `AbortReport(aborted=False, stopped=False)`.
  `stopped` is always `False`: an abort request never claims the
  equipment stopped, and `handle.stopped` is untouched by `abort()`.
  Whether the in-flight call actually terminates early is decided by the
  backend. `AbortReport` is defined in `softlab.tu.station.visa` (a
  `NamedTuple` with fields `aborted` and `stopped`).
- `confirm_stop() -> bool`: device-confirmed stop, delegated to
  `query('*OPC?', None)` — exactly one resource query per call, with the
  serialization guard and identical-object error propagation. Only an
  exact `'1'` after stripping confirms; anything else returns `False`
  and leaves `stopped` unchanged. `stopped` is a latch: once
  device-confirmed it stays set until `cleanup()` resets it.

`VisaCommand` (in `softlab.tu.station.visa`) gains an explicit operation
path beside the legacy read-only command contract:

- `execute() -> Any`: performs the command's declared operation and
  returns its result, with the handle's serialization guard and
  identical-object error propagation.
- `describe_operation() -> Dict[str, Any]`: versioned description
  (`schema_version` 1) of the operation's side-effect semantics.
  Description lookup executes nothing.

Legacy `VisaCommand` invocation is retained: a legacy command get
executes exactly once and writes remain denied, unchanged.

## 6. Theory model

`TheoryModel` (in `softlab.tu.theory.model`) gains an inspection and
configuration contract:

- `describe() -> Dict[str, Any]`: versioned semantic description
  (`schema_version` 1): the model `name`, the model kind as the class's
  `__qualname__`, and one entry per attribute carrying its semantic
  `description` (`''` when none was given). Performs no evaluation:
  `calculate_features` is never called, so a model whose evaluation
  raises is still fully describable.
- `supported_configuration() -> Tuple[str, ...]`: the supported
  configuration keys — exactly the registered attribute keys, in
  registration order, independent of current values.
- `configuration() -> Dict[str, Any]`: the current configuration as a
  fresh dict mapping each supported key to its attribute's current
  value; JSON-serializable exactly when the attribute values are.
- `configure(cfg) -> None`: applies a configuration mapping in three
  phases — a non-mapping argument raises `TypeError` naming the received
  type; any unknown key raises `KeyError` naming the key; every value is
  pre-validated against its attribute's validator before any value is
  applied. Rejection at any phase leaves the previous configuration
  fully intact — no partial application.
- `evaluate_features(strict=False) -> Dict[str, Any]`: evaluates by
  calling `calculate_features()` directly. With `strict=True` the
  original exception object propagates unchanged; with the default
  lenient mode any `Exception` returns `{}`, while `BaseException`-only
  process-control exceptions deliberately propagate. The legacy
  `features` property is a separate, unchanged code path that swallows
  all exceptions and falls back to `{}`.

## 7. Worked examples

The examples below are runnable as written (a scheduler from
`softlab.huo.scheduler`, a virtual device, plain parameters and a small
model; no hardware, no new dependencies).

(a) Describe, prepare, count, cleanup on a virtual device:

```python
import asyncio
from softlab.huo.process import count, run_process
from softlab.huo.scheduler import get_scheduler
from softlab.jin.validator import ValNumber
from softlab.tu.station import Device, Parameter, Station

class VirtualDevice(Device):
    """Synthetic virtual device with one numeric parameter."""
    def __init__(self, name: str = 'virt'):
        super().__init__(name)
        self._signal = Parameter('signal', ValNumber())
        self._signal(1.0)
        self.add_parameter(self._signal)

device = VirtualDevice('meter')
station = Station('bench')
station.add_device(device)

# Description is inspection-only: no acquisition happens
description = station.describe()
assert description['schema_version'] == 1
assert 'meter' in description['devices']

# Unsupported capability detectable; preparation gates usage
assert not device.supports('read')
assert not device.initialized
device.prepare()
assert device.initialized

scheduler = get_scheduler()
scheduler.start()
try:
    proc = count('acquire', None, None, device.signal, times=3)
    success, _ = run_process(proc, scheduler, verbose=False)
    assert success
    table = proc.record.table
    assert table['signal'].tolist() == [1.0, 1.0, 1.0]  # value-only
finally:
    if scheduler.is_running:
        scheduler.stop()

device.cleanup()
assert not device.initialized
```

(b) Legacy `scan()` beside the new `read()` on the same parameter:

```python
from softlab.huo.process import scan
from softlab.huo.scheduler import get_scheduler
from softlab.tu.station import Reading

device = VirtualDevice('meter')
device.prepare()
setter = Parameter('frequency', ValNumber())
setter(0.0)

scheduler = get_scheduler()
scheduler.start()
try:
    proc = scan('sweep', [device.signal], None, None,
                setter, [10.0, 20.0, 30.0])
    success, _ = run_process(proc, scheduler, verbose=False)
finally:
    if scheduler.is_running:
        scheduler.stop()
assert success
assert proc.record.table['frequency'].tolist() == [10.0, 20.0, 30.0]

# New measurement contract on the same parameter, no huo change
reading = device.signal.read()
assert isinstance(reading, Reading)
assert reading.value == 1.0
assert reading.quality == 'ok'
assert reading.error is None
assert reading.acquired_at is not None
assert device.signal.describe_reading()['schema_version'] == 1
device.cleanup()
```

(c) Model configuration round trip:

```python
import json
from softlab.jin.validator import ValInt
from softlab.tu.theory import TheoryModel

class Model(TheoryModel):
    def __init__(self, name=None):
        super().__init__(name)
        self.add_attribute('value', ValInt(0, 10), 2,
                           description='seed value')
        self.add_attribute('gain', ValNumber(0.0), 1.5)

    def calculate_features(self):
        return {'double': self.value() * 2}

model = Model('model')
assert model.describe()['attributes']['value']['description'] == 'seed value'
assert set(model.supported_configuration()) == {'value', 'gain'}
initial = model.configuration()
json.dumps(initial)  # must not raise
assert initial == {'value': 2, 'gain': 1.5}

model.configure({'value': 4, 'gain': 2.5})
assert model.configuration() == {'value': 4, 'gain': 2.5}
assert model.evaluate_features(strict=True) == {'double': 8}
assert model.evaluate_features() == {'double': 8}  # lenient default
assert model.features == {'double': 8}  # legacy path unchanged
```

## 8. Limitations

Resolved observations (recorded for transparency):

- **OBS-001 (resolved, TU-006):** timeout units. Default construction
  forwards `timeout_seconds * 1000` milliseconds (5000 ms); an explicit
  raw `timeout` is forwarded unchanged and never rescaled; the
  `timeout_seconds` property converts in both directions and `None`
  disables the timeout.
- **OBS-002 (fixed, TU-006):** `VisaHandle.write_raw` routes to
  `resource.write_raw(message)` with the identical bytes object, the
  return value forwarded and error identity preserved.
- **OBS-003 (closed, TU-006):** `_acquire()` is the single acquisition
  site; any post-acquisition failure closes the acquired resource
  exactly once and propagates the original exception object unchanged.
  The resource manager is retained for the handle's lifetime; there is
  no manager-close API.

Known limitations:

- **OBS-004:** `QuantizedParameter` and `VisaParameter` call base
  initialization before installing fields used by setting hooks/codecs;
  a non-None settable `init_value` can fail. Avoid non-None settable
  `init_value` on these classes.
- **OBS-005:** `TheoryModel.features` and
  `evaluate_features(strict=False)` catch all exceptions and fall back
  to `{}`; `evaluate_features(strict=True)` is the opt-in escape hatch
  that re-raises the original error object.
- **OBS-006:** delegated attribute names may collide with methods
  (`child`, `device`); explicit `_attributes`/`device()` lookup remains
  available for such names.
- **DEFECT-2:** constructing a `Parameter` that is neither settable nor
  gettable emits a warning. This is a deliberate, documented warning on
  a programmer-error path and is not silenced.
- **Description serialization limits:** description payloads contain
  JSON-safe scalar values; ndarray attribute values in model
  configuration are the model author's responsibility.

## 9. Migration-free guarantee

> **Migration-free guarantee.** Existing code that calls none of the APIs
> listed in this guide requires no changes: all public import paths, the
> value-only records produced by `count()`/`scan()`, the legacy
> `TheoryModel.features` property (including its `{}` fallback on
> evaluation failure), and the builder helpers `register_device_builder`,
> `get_device_builder`, `set_default_station` and `default_station`
> behave exactly as before. Every extension described in this guide is
> opt-in.

## 10. Versioning note

All description payloads emitted by the extension APIs —
`Parameter.describe()`, `Device.describe()`, `Station.describe()`,
`Parameter.describe_reading()`, `VisaCommand.describe_operation()` and
`TheoryModel.describe()` — carry `schema_version: 1`. Version 1 makes no
promise to invoke subclass `describe()` overrides during parent
traversal or to include custom fields; the no-I/O and JSON guarantees
apply to the built-in traversal only.
