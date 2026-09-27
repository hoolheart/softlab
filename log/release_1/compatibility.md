# TU-001 compatibility matrix

Scope: unchanged production sources, characterized by `tests/test_tu_contracts.py`
and the existing `tests/test_compatibility.py`. Test names below omit `test_`.

| Contract | Existing consumer/example | Assertion |
| --- | --- | --- |
| Callable get/set; first setting argument; validation → decoder → before-set → store → after-set; before-get → encoder | `huo/process/common.py:AtomJob.body`, `tests/test_common_proc.ipynb` | `validation_codecs_and_hooks_have_ordered_values` |
| Settable initialization uses hooks; read-only initialization validates without decoding; permission failures precede hooks | `station/parameter.py` examples; notebook computed readings | `initialization_and_permissions` |
| Failed validation/decoder/before-set does not replace cached value | Parameter extension points | `validation_codecs_and_hooks_have_ordered_values`, `failed_decoder_or_before_hook_preserves_value` |
| Arbitrary object values; snapshot contains class and does not read | Notebook snapshots, device/station snapshot recursion | `arbitrary_value_and_snapshot_do_not_read` |
| Proxy forwards access/validation/hooks | `parameter.py` proxy examples | `proxy_forwards_validation_permissions_and_hooks` |
| Quantization modes and input bounds after construction | `parameter.py` quantization examples | `quantization_modes_and_bounds` |
| Nested lookup, delegation, parent/owner links, ordered settings and removal | `device.py` examples | `nested_paths_delegation_ownership_and_removal` |
| Registry, construction kwargs, default station identity, snapshot | `station.py` examples | `station_builder_registry_and_default_are_restored` (registry and default restored) |
| Eager VISA opening/clear, direct communication error propagation, explicit close | `tests/test_visa.ipynb` | `eager_open_clear_and_transport_errors` |
| VISA formatting, codec read, inspection without I/O | VISA notebook channels | `parameter_commands_codecs_and_snapshot`; existing simulator test |
| Legacy command get executes exactly once; set denied | `VisaCommand` public contract | `legacy_command_reads_execute_once_and_writes_denied` |
| Rejected non-message resource closes | `VisaHandle.__init__` | `non_message_resource_is_closed_on_rejection` |
| Validated model attributes and legacy feature failure fallback | `theory/model.py:Motion1D` example | `model_attributes_features_and_legacy_failure` |
| Fixed ndarray type/shape, result checks, batching | `theory/mapping.py` example | `mapping_shapes_types_and_batch` |
| Public exports retain identity | `huo`, notebooks, top-level import | `public_exports_retain_class_identity` |
| Count/scan retain numeric value-only behavior | `huo/process/common.py`, notebooks | Existing `test_count_and_scan_record_values` |

## Observations requiring disposition, not desired contracts

- **OBS-001 (TU-006):** `VisaHandle.timeout` forwards numeric values directly to
  PyVISA while its docstrings say seconds. PyVISA resource timeout convention is
  milliseconds. Do not freeze this discrepancy with an assertion or silently
  rescale existing callers; TU-006 must document compatible units/defaults.
- **OBS-002 (TU-006):** `VisaHandle.write_raw` calls resource `write`, not
  `write_raw`. Source observation; no hardware reproduction. Resolve explicitly
  in TU-006 with a focused regression and compatible error handling.
- **OBS-003 (TU-004/TU-006):** resource acquisition precedes clear/termination
  setup and there is no explicit cleanup guard for their failure. Resource-manager
  ownership is also unspecified. No failed-initialization cleanup guarantee is
  claimed; lifecycle design must determine ownership and verify failure cleanup.
- **OBS-004 (TU-005/TU-006):** `QuantizedParameter` and `VisaParameter` call base
  initialization before installing fields used by setting hooks/codecs. Non-None
  settable `init_value` can fail. Tests avoid asserting this defect as desired
  behavior; require targeted reproduction and disposition before release acceptance.
- **OBS-005 (TU-007):** `TheoryModel.features` catches all exceptions and returns
  `{}`. Its ordinary evaluation-error fallback is deliberately characterized
  because PRD requires legacy compatibility. New strict evaluation must expose
  errors; swallowing process-control exceptions is not certified as desirable.
- **OBS-006 (TU-004):** delegated names may collide with methods (`child`,
  `device`); explicit lookup remains available. Tests use explicit lookup for
  collisions. Parent-cycle prevention beyond direct self-parenting is not covered.

No production files changed. Real hardware, notebook execution, concurrency,
exhaustive driver failures, array serialization and new lifecycle contracts are
not validated by this characterization task. Later tasks must supply their own
acceptance coverage; passing this matrix does not close the observations.
