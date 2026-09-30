"""Acceptance tests for theory-model inspection and strict evaluation (TU-007).

Proposed contract under test (test surface for developer/designer review;
no production implementation is asserted to exist):

- ``TheoryModel.describe() -> Dict[str, Any]`` --- opt-in versioned
  semantic description (``schema_version`` 1) exposing model identity
  (``name``, model kind) and per-attribute semantic descriptions.
  ``add_attribute`` gains an optional ``description`` keyword (default
  ``''``). Description performs no evaluation: a model whose
  ``calculate_features`` raises is still describable.
- ``TheoryModel.supported_configuration()`` --- declares the supported
  serializable configuration keys explicitly (independent of current
  values). ``TheoryModel.configuration() -> Dict[str, Any]`` reads the
  current configuration; ``TheoryModel.configure(cfg)`` applies it.
  Supported configuration round trips: ``configure`` followed by
  ``configuration`` returns equal values, the values are JSON
  serializable, and configuration stays consistent with delegated
  attribute access (single source of truth).
- Unsupported configuration is rejected explicitly: unknown keys raise
  ``KeyError`` naming the key with no partial application; a
  non-mapping argument raises ``TypeError``.
- Existing validators are reused, not duplicated: ``configure`` values
  pass through the same ``Validator`` chain as delegated attribute
  writes, so out-of-range values raise the same exceptions and leave
  the previous values untouched.
- ``TheoryModel.evaluate_features(strict: bool = False)`` --- opt-in
  strict evaluation path. With ``strict=True`` the original exception
  object from ``calculate_features`` propagates unchanged. With
  ``strict=False`` the legacy lenient semantics are preserved (``{}``
  on failure). The legacy ``features`` property is untouched and keeps
  its characterized swallow-all behavior (OBS-005).
- Existing ``Mapping`` shape/type checks are retained, not duplicated:
  no second shape-check layer is introduced anywhere; fixed-shape
  ndarray mappings and ``batch_mapping`` stay byte-compatible. Public
  imports of ``softlab.tu.theory`` are unchanged.
"""

import json
import unittest
from unittest.mock import patch

import numpy as np
from numpy.testing import assert_array_equal

from softlab.jin.validator import (
    ValInt,
    ValNumber,
)
from softlab.tu.theory import (
    Mapping,
    TheoryModel,
    batch_mapping,
)


def _make_model(with_description: bool = False) -> TheoryModel:
    """Fixture model: validated attributes, one optionally described."""
    class Model(TheoryModel):
        def __init__(self, name=None):
            super().__init__(name)
            if with_description:
                self.add_attribute(
                    "value", ValInt(0, 10), 2, description="seed value")
            else:
                self.add_attribute("value", ValInt(0, 10), 2)
            self.add_attribute("gain", ValNumber(0.0), 1.5)

        def calculate_features(self):
            return {"double": self.value() * 2}

    return Model("model")


class LegacyGuardTests(unittest.TestCase):
    """Guards passing against unchanged production code."""

    def test_legacy_features_behavior_unchanged(self):
        model = _make_model()
        self.assertEqual(model.features, {"double": 4})
        model.value(3)
        self.assertEqual(model.features, {"double": 6})
        with self.assertRaises(ValueError):
            model.value(11)
        with self.assertRaises(ValueError):
            model.add_attribute("value", ValInt(), 1)
        failure = ValueError("bad")
        with patch.object(model, "calculate_features",
                          side_effect=failure):
            # OBS-005 legacy lenient fallback stays characterized: the
            # legacy property still swallows and returns {}.
            self.assertEqual(model.features, {})
        with self.assertRaises(NotImplementedError):
            model.get_mapping("missing")

    def test_mapping_shape_checks_retained_not_duplicated(self):
        # Existing fixed-shape ndarray mapping behavior, exercised as a
        # guard: the new contract must reuse these checks, never add a
        # parallel shape-check layer with different semantics.
        mapping = Mapping((2, 1), (2, 1), lambda x: x * 2, {"unit": "V"})
        data = np.arange(8).reshape(4, 2)
        assert_array_equal(batch_mapping(mapping, data), data * 2)
        assert_array_equal(mapping(np.ones((2, 1))), np.ones((2, 1)) * 2)
        self.assertEqual(mapping.metadata, {"unit": "V"})
        with self.assertRaises(TypeError):
            mapping([[1], [2]])
        with self.assertRaises(ValueError):
            mapping(np.zeros((1, 2)))
        with self.assertRaises(RuntimeError):
            mapping()
        with self.assertRaises(RuntimeError):
            Mapping((1,), (1,), lambda x: "bad")(np.ones(1))
        with self.assertRaises(ValueError):
            batch_mapping(mapping, np.ones(4))

    def test_public_imports_unchanged(self):
        import softlab.tu as tu
        self.assertIs(tu.theory.Mapping, Mapping)
        self.assertIs(tu.theory.TheoryModel, TheoryModel)
        self.assertIs(tu.theory.batch_mapping, batch_mapping)


class ModelIdentityTests(unittest.TestCase):
    def test_model_identity_and_semantic_description(self):
        model = _make_model(with_description=True)
        describe = model.describe  # resolve API first: missing API is
        description = describe()   # the red cause
        self.assertEqual(description["schema_version"], 1)
        self.assertEqual(description["name"], "model")
        self.assertIn("Model", json.dumps(description))
        attributes = description["attributes"]
        self.assertEqual(attributes["value"]["description"], "seed value")
        self.assertEqual(attributes["gain"]["description"], "")
        self.assertIn("value", attributes)
        self.assertIn("gain", attributes)
        # Identity survives an empty name (existing constructor contract).
        unnamed = _make_model()
        unnamed._name = ""
        self.assertEqual(unnamed.describe()["name"], "")

    def test_description_performs_no_evaluation(self):
        class Fragile(TheoryModel):
            def calculate_features(self):
                raise RuntimeError("must not be called")

        fragile = Fragile("fragile")
        fragile.add_attribute("value", ValInt(0, 10), 1)
        description = fragile.describe()  # must not raise
        self.assertEqual(description["name"], "fragile")
        self.assertIn("value", description["attributes"])


class ConfigurationTests(unittest.TestCase):
    def test_supported_configuration_declared_explicitly(self):
        model = _make_model()
        supported = model.supported_configuration  # resolve API first
        keys = tuple(supported())
        self.assertEqual(set(keys), {"value", "gain"})
        # Declaration is independent of current values and JSON-safe.
        model.value(7)
        self.assertEqual(tuple(model.supported_configuration()), keys)
        json.dumps(list(keys))

    def test_supported_configuration_round_trip(self):
        model = _make_model()
        configuration = model.configuration  # resolve API first
        initial = configuration()
        self.assertEqual(initial, {"value": 2, "gain": 1.5})
        json.dumps(initial)  # supported configuration is serializable
        model.configure({"value": 5, "gain": 0.5})
        updated = model.configuration()
        self.assertEqual(updated, {"value": 5, "gain": 0.5})
        # Single source of truth: delegated access sees the same values.
        self.assertEqual(model.value(), 5)
        self.assertEqual(model.gain(), 0.5)
        # Round trip is an identity: re-applying the read configuration
        # changes nothing.
        model.configure(updated)
        self.assertEqual(model.configuration(), updated)

    def test_unsupported_configuration_keys_rejected_explicitly(self):
        model = _make_model()
        with self.assertRaises(KeyError) as raised:
            model.configure({"nope": 1})
        self.assertIn("nope", str(raised.exception))
        # Rejection leaves the previous configuration fully intact.
        self.assertEqual(model.configuration(), {"value": 2, "gain": 1.5})
        with self.assertRaises(TypeError):
            model.configure("value")  # type: ignore[arg-type]

    def test_existing_validators_reused_for_configuration(self):
        model = _make_model()
        with self.assertRaises(ValueError):
            model.configure({"value": 11})  # ValInt(0, 10) range kept
        # Failed validation changes nothing (existing validator chain).
        self.assertEqual(model.configuration(), {"value": 2, "gain": 1.5})
        self.assertEqual(model.value(), 2)


class StrictEvaluationTests(unittest.TestCase):
    def test_strict_evaluation_exposes_original_error(self):
        model = _make_model()
        evaluate = model.evaluate_features  # resolve API first
        failure = ValueError("bad")
        with patch.object(model, "calculate_features",
                          side_effect=failure):
            with self.assertRaises(ValueError) as raised:
                evaluate(strict=True)
        self.assertIs(raised.exception, failure)

    def test_lenient_path_unchanged_beside_strict(self):
        model = _make_model()
        evaluate = model.evaluate_features
        # Strict success equals the legacy feature dictionary exactly.
        self.assertEqual(evaluate(strict=True), {"double": 4})
        self.assertEqual(model.features, {"double": 4})
        failure = ValueError("bad")
        with patch.object(model, "calculate_features",
                          side_effect=failure):
            # Legacy property and lenient strict=False keep OBS-005
            # semantics; only strict=True exposes the error.
            self.assertEqual(model.features, {})
            self.assertEqual(evaluate(), {})
            self.assertEqual(evaluate(strict=False), {})
            with self.assertRaises(ValueError) as raised:
                evaluate(strict=True)
            self.assertIs(raised.exception, failure)


if __name__ == "__main__":
    unittest.main()
