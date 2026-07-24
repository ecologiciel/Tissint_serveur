import io
from types import SimpleNamespace

import pytest
from PIL import Image

from expert_dataset import (
    build_audit,
    build_single_image_prediction,
    image_quality_report,
    normalize_image_assets,
    perceptual_hash,
    sha256_hex,
    validate_annotation,
)


def image_bytes(size=(800, 800), color=(110, 90, 70)):
    buffer = io.BytesIO()
    Image.new("RGB", size, color).save(buffer, format="JPEG")
    return buffer.getvalue()


def test_image_fingerprint_and_normalization_are_deterministic():
    data = image_bytes()
    normalized, thumbnail, quality = normalize_image_assets(data)

    assert sha256_hex(data) == sha256_hex(data)
    assert perceptual_hash(data) == perceptual_hash(data)
    assert Image.open(io.BytesIO(normalized)).format == "JPEG"
    assert Image.open(io.BytesIO(thumbnail)).size[0] <= 512
    assert quality["width"] == 800
    assert "score" in quality


def test_annotation_taxonomy_rejects_mismatched_subclass():
    validate_annotation("meteorite", "chondrite", None)
    validate_annotation("terrestrial_rock", None, "slag")
    with pytest.raises(ValueError):
        validate_annotation("meteorite", None, None)
    with pytest.raises(ValueError):
        validate_annotation("terrestrial_rock", "chondrite", "slag")


def test_prediction_keeps_model_level_outputs_and_fusion_band():
    prediction = build_single_image_prediction(
        {
            "dinov2": {"prob_bin": 0.90, "prob_sub": [0.0, 0.0, 0.0, 0.0, 1.0, 0.0]},
            "swin": {"prob_bin": 0.80, "prob_sub": [0.0, 0.0, 0.0, 0.0, 1.0, 0.0]},
            "convnext": {"prob_bin": 0.70, "prob_sub": [0.0, 0.0, 0.0, 0.0, 1.0, 0.0]},
        }
    )

    assert prediction["meteorite_probability"] == pytest.approx(0.80)
    assert prediction["decision_band"] == "strong_meteorite"
    assert prediction["dominant_class"] == "Metallique"
    assert set(prediction["models"]) == {"dinov2", "swin", "convnext"}


def test_audit_reports_error_types_calibration_and_subgroups():
    rows = [
        (
            SimpleNamespace(
                id="tp",
                raw_prediction={"meteorite_probability": 0.91, "models": {"a": {"meteorite_probability": 0.9}}},
                quality_report={"passed": True, "issues": []},
                item_metadata={"origin": "collection-a"},
            ),
            SimpleNamespace(final_label="meteorite"),
        ),
        (
            SimpleNamespace(
                id="fp",
                raw_prediction={"meteorite_probability": 0.88, "models": {"a": {"meteorite_probability": 0.9}, "b": {"meteorite_probability": 0.4}}},
                quality_report={"passed": False, "issues": ["BLURRY"]},
                item_metadata={"origin": "collection-b"},
            ),
            SimpleNamespace(final_label="terrestrial_rock"),
        ),
        (
            SimpleNamespace(
                id="fn",
                raw_prediction={"meteorite_probability": 0.20, "models": {"a": {"meteorite_probability": 0.2}}},
                quality_report={"passed": True, "issues": []},
                item_metadata={"origin": "collection-a"},
            ),
            SimpleNamespace(final_label="meteorite"),
        ),
    ]

    summary, errors, recommendations = build_audit(rows)

    assert summary["metrics"]["precision"] == pytest.approx(0.5)
    assert summary["metrics"]["recall"] == pytest.approx(0.5)
    assert "confusion_matrix" in summary
    assert "calibration" in summary
    assert "class:meteorite" in summary["by_subgroup"]
    assert {error["error_type"] for error in errors} >= {
        "false_positive",
        "false_negative",
        "high_confidence_error",
        "model_disagreement",
        "quality_related",
    }
    assert recommendations
