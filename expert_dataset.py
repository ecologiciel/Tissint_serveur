import hashlib
import io
import json
from collections import Counter, defaultdict
from typing import Any, Iterable

import numpy as np
from PIL import Image, ImageOps


TAXONOMY_VERSION = "taxonomy-v1"
ANNOTATION_POLICY_VERSION = "annotation-policy-v1"
MODEL_VERSION = "trio-v1"
POSITIVE_THRESHOLD = 0.80
UNCERTAIN_THRESHOLD = 0.50

METEORITE_SUBCLASSES = {
    "chondrite",
    "carbonaceous_chondrite",
    "achondrite",
    "iron_meteorite",
    "stony_iron",
    "meteorite_unknown",
}
TERRESTRIAL_FAMILIES = {
    "slag",
    "hematite",
    "magnetite",
    "basalt",
    "quartz",
    "sedimentary_rock",
    "industrial_material",
    "terrestrial_unknown",
}
TOP_LABELS = {"meteorite", "terrestrial_rock", "uncertain", "unusable", "non_rock"}
MODEL_CLASS_TO_TAXONOMY = {
    "Achondrite": "achondrite",
    "Carbonee": "carbonaceous_chondrite",
    "Chondrite": "chondrite",
    "Metallique": "iron_meteorite",
    "Meteore_Unknown": "meteorite_unknown",
}


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def perceptual_hash(data: bytes) -> str:
    with Image.open(io.BytesIO(data)) as raw_image:
        image = ImageOps.exif_transpose(raw_image).convert("L").resize((16, 16))
        values = np.asarray(image, dtype=np.float32)
    median = float(np.median(values))
    bits = (values >= median).flatten()
    return "".join("1" if value else "0" for value in bits)


def image_quality_report(data: bytes) -> dict:
    with Image.open(io.BytesIO(data)) as raw_image:
        image = ImageOps.exif_transpose(raw_image).convert("RGB")
        width, height = image.size
        gray = np.asarray(image.convert("L"), dtype=np.float32)
        rgb = np.asarray(image, dtype=np.uint8)

    if gray.size:
        gx = np.diff(gray, axis=1)
        gy = np.diff(gray, axis=0)
        sharpness = float(np.var(gx) + np.var(gy))
        brightness = float(np.mean(gray))
        highlight_ratio = float(np.mean(gray >= 245))
        shadow_ratio = float(np.mean(gray <= 18))
        saturation_high_ratio = float(np.mean(np.max(rgb, axis=2) >= 248))
    else:
        sharpness = brightness = highlight_ratio = shadow_ratio = saturation_high_ratio = 0.0

    issues: list[str] = []
    if width < 640 or height < 640:
        issues.append("LOW_RESOLUTION")
    if sharpness < 20.0:
        issues.append("BLURRY")
    if brightness < 25.0:
        issues.append("UNDEREXPOSED")
    if brightness > 235.0:
        issues.append("OVEREXPOSED")
    if highlight_ratio > 0.20 or saturation_high_ratio > 0.20:
        issues.append("GLARE_OR_HIGHLIGHTS")

    quality_score = max(
        0.0,
        min(
            1.0,
            0.30 * min(width, height) / 640.0
            + 0.35 * min(sharpness / 200.0, 1.0)
            + 0.20 * (1.0 - min(abs(brightness - 128.0) / 128.0, 1.0))
            + 0.15 * (1.0 - min(highlight_ratio / 0.20, 1.0)),
        ),
    )
    return {
        "passed": not issues,
        "issues": issues,
        "width": width,
        "height": height,
        "bytes": len(data),
        "sharpness": round(sharpness, 4),
        "brightness": round(brightness, 4),
        "highlight_ratio": round(highlight_ratio, 5),
        "shadow_ratio": round(shadow_ratio, 5),
        "score": round(quality_score, 4),
    }


def normalize_image_assets(data: bytes) -> tuple[bytes, bytes, dict]:
    """Create the immutable normalized image, thumbnail and quality metadata."""
    with Image.open(io.BytesIO(data)) as raw_image:
        image = ImageOps.exif_transpose(raw_image).convert("RGB")
        normalized_buffer = io.BytesIO()
        image.save(normalized_buffer, format="JPEG", quality=92, optimize=True)
        thumbnail = image.copy()
        thumbnail.thumbnail((512, 512))
        thumbnail_buffer = io.BytesIO()
        thumbnail.save(thumbnail_buffer, format="JPEG", quality=84, optimize=True)
    return normalized_buffer.getvalue(), thumbnail_buffer.getvalue(), image_quality_report(data)


def _mean(values: Iterable[float]) -> float:
    values_list = [float(value) for value in values]
    return float(np.mean(values_list)) if values_list else 0.0


def _decision_band(probability: float) -> str:
    if probability >= POSITIVE_THRESHOLD:
        return "strong_meteorite"
    if probability >= UNCERTAIN_THRESHOLD:
        return "uncertain"
    return "terrestrial_candidate"


def build_single_image_prediction(raw_models: dict[str, dict[str, Any]]) -> dict:
    class_names = ["None", "Achondrite", "Carbonee", "Chondrite", "Metallique", "Meteore_Unknown"]
    binary_scores = [float(item.get("prob_bin", 0.0)) for item in raw_models.values()]
    sub_vectors = [item.get("prob_sub") or [] for item in raw_models.values()]
    combined_sub = np.mean(sub_vectors, axis=0) if sub_vectors else np.array([])
    top_index = int(np.argmax(combined_sub)) if combined_sub.size else None
    probability = _mean(binary_scores)
    dominant_class = class_names[top_index] if top_index is not None and top_index < len(class_names) else None
    class_confidence = float(combined_sub[top_index]) if top_index is not None else None

    models = {}
    for model_name, result in raw_models.items():
        sub_scores = result.get("prob_sub") or []
        model_index = int(np.argmax(sub_scores)) if sub_scores else None
        models[model_name] = {
            "meteorite_probability": float(result.get("prob_bin", 0.0)),
            "dominant_class": class_names[model_index] if model_index is not None and model_index < len(class_names) else None,
            "class_confidence": float(sub_scores[model_index]) if model_index is not None else None,
        }

    return {
        "model_version": MODEL_VERSION,
        "meteorite_probability": probability,
        "decision_band": _decision_band(probability),
        "dominant_class": dominant_class,
        "class_confidence": class_confidence,
        "models": models,
        "raw": raw_models,
    }


def validate_annotation(top_label: str | None, meteorite_subclass: str | None, terrestrial_family: str | None) -> None:
    if top_label not in TOP_LABELS:
        raise ValueError("top_label invalide")
    if top_label == "meteorite":
        if meteorite_subclass not in METEORITE_SUBCLASSES:
            raise ValueError("Une sous-classe météoritique valide est obligatoire")
        if terrestrial_family is not None:
            raise ValueError("Une annotation météorite ne peut pas contenir une famille terrestre")
    elif top_label == "terrestrial_rock":
        if terrestrial_family not in TERRESTRIAL_FAMILIES:
            raise ValueError("Une famille terrestre valide est obligatoire")
        if meteorite_subclass is not None:
            raise ValueError("Une annotation terrestre ne peut pas contenir une sous-classe météoritique")
    else:
        if meteorite_subclass is not None or terrestrial_family is not None:
            raise ValueError("Une annotation incertaine ou inutilisable ne peut pas contenir de sous-classe")


def build_audit(rows: list[tuple[Any, Any]]) -> tuple[dict, list[dict], list[str]]:
    counts = Counter()
    labels = Counter()
    predicted_labels = Counter()
    errors: list[dict] = []
    score_buckets: defaultdict[str, dict[str, int]] = defaultdict(lambda: Counter())
    subgroup_rows: defaultdict[str, list[tuple[bool, bool, float]]] = defaultdict(list)
    calibration_bins: defaultdict[int, list[tuple[float, bool]]] = defaultdict(list)

    def metrics_for(values: list[tuple[bool, bool, float]]) -> dict[str, float | int]:
        tp = sum(1 for predicted, actual, _score in values if predicted and actual)
        fp = sum(1 for predicted, actual, _score in values if predicted and not actual)
        fn = sum(1 for predicted, actual, _score in values if not predicted and actual)
        tn = sum(1 for predicted, actual, _score in values if not predicted and not actual)
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        specificity = tn / (tn + fp) if tn + fp else 0.0
        return {
            "total": len(values),
            "true_positive": tp,
            "false_positive": fp,
            "false_negative": fn,
            "true_negative": tn,
            "precision": round(precision, 6),
            "recall": round(recall, 6),
            "f1": round(2 * precision * recall / (precision + recall), 6) if precision + recall else 0.0,
            "balanced_accuracy": round((recall + specificity) / 2.0, 6),
        }

    for item, consensus in rows:
        prediction = item.raw_prediction or {}
        score = float(prediction.get("meteorite_probability") or 0.0)
        predicted_positive = score >= POSITIVE_THRESHOLD
        actual_positive = consensus.final_label == "meteorite"
        predicted_label = "meteorite" if predicted_positive else "terrestrial_rock"
        actual_label = consensus.final_label or "unknown"
        labels[actual_label] += 1
        predicted_labels[predicted_label] += 1
        bucket = "<0.50" if score < 0.50 else "0.50-0.79" if score < 0.80 else ">=0.80"
        score_buckets[bucket]["total"] += 1
        quality = item.quality_report or {}
        origin = str((item.item_metadata or {}).get("origin") or "unknown")
        model_scores = [
            float(model.get("meteorite_probability"))
            for model in (prediction.get("models") or {}).values()
            if isinstance(model, dict) and model.get("meteorite_probability") is not None
        ]
        model_disagreement = max(model_scores) - min(model_scores) if model_scores else 0.0
        subgroup_rows[f"class:{actual_label}"].append((predicted_positive, actual_positive, score))
        subgroup_rows[f"quality:{'passed' if quality.get('passed') else 'flagged'}"].append(
            (predicted_positive, actual_positive, score)
        )
        subgroup_rows[f"origin:{origin}"].append((predicted_positive, actual_positive, score))
        calibration_bins[min(int(score * 10), 9)].append((score, actual_positive))

        if predicted_positive and actual_positive:
            counts["true_positive"] += 1
            score_buckets[bucket]["true_positive"] += 1
        elif predicted_positive and not actual_positive:
            counts["false_positive"] += 1
            score_buckets[bucket]["false_positive"] += 1
            errors.append({"item_id": item.id, "error_type": "false_positive", "score": score, "actual": actual_label})
        elif not predicted_positive and actual_positive:
            counts["false_negative"] += 1
            score_buckets[bucket]["false_negative"] += 1
            errors.append({"item_id": item.id, "error_type": "false_negative", "score": score, "actual": actual_label})
        else:
            counts["true_negative"] += 1

        if (predicted_positive and not actual_positive and score >= POSITIVE_THRESHOLD) or (
            not predicted_positive and actual_positive and score < UNCERTAIN_THRESHOLD
        ):
            errors.append({
                "item_id": item.id,
                "error_type": "high_confidence_error",
                "score": score,
                "actual": actual_label,
            })
        if model_disagreement >= 0.20:
            counts["model_disagreement"] += 1
            errors.append({
                "item_id": item.id,
                "error_type": "model_disagreement",
                "score": score,
                "actual": actual_label,
            })
        if quality.get("issues"):
            counts["quality_related"] += 1
            errors.append({
                "item_id": item.id,
                "error_type": "quality_related",
                "score": score,
                "actual": actual_label,
            })
        predicted_subclass = MODEL_CLASS_TO_TAXONOMY.get(prediction.get("dominant_class"))
        if (
            actual_label == "meteorite"
            and getattr(consensus, "meteorite_subclass", None)
            and predicted_subclass
            and predicted_subclass != getattr(consensus, "meteorite_subclass", None)
        ):
            counts["subclass_error"] += 1
            errors.append({
                "item_id": item.id,
                "error_type": "subclass_error",
                "score": score,
                "actual": getattr(consensus, "meteorite_subclass", None),
            })

        if actual_label != predicted_label:
            counts["label_disagreement"] += 1

    tp = counts["true_positive"]
    fp = counts["false_positive"]
    fn = counts["false_negative"]
    tn = counts["true_negative"]
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    specificity = tn / (tn + fp) if tn + fp else 0.0
    balanced_accuracy = (recall + specificity) / 2.0
    calibration = {}
    expected_calibration_error = 0.0
    for bin_index in range(10):
        values = calibration_bins.get(bin_index, [])
        if not values:
            continue
        average_score = sum(score for score, _actual in values) / len(values)
        observed_rate = sum(1 for _score, actual in values if actual) / len(values)
        gap = abs(average_score - observed_rate)
        expected_calibration_error += len(values) / len(rows) * gap if rows else 0.0
        calibration[f"{bin_index / 10:.1f}-{(bin_index + 1) / 10:.1f}"] = {
            "count": len(values),
            "average_score": round(average_score, 6),
            "observed_positive_rate": round(observed_rate, 6),
            "absolute_gap": round(gap, 6),
        }
    by_subgroup = {
        key: metrics_for(values)
        for key, values in subgroup_rows.items()
    }
    for bucket, values in score_buckets.items():
        bucket_tp = values.get("true_positive", 0)
        bucket_fp = values.get("false_positive", 0)
        bucket_fn = values.get("false_negative", 0)
        bucket_precision = bucket_tp / (bucket_tp + bucket_fp) if bucket_tp + bucket_fp else 0.0
        bucket_recall = bucket_tp / (bucket_tp + bucket_fn) if bucket_tp + bucket_fn else 0.0
        values["precision"] = round(bucket_precision, 6)
        values["recall"] = round(bucket_recall, 6)

    summary = {
        "total": len(rows),
        "counts": dict(counts),
        "labels": dict(labels),
        "predicted_labels": dict(predicted_labels),
        "metrics": {
            "precision": round(precision, 6),
            "recall": round(recall, 6),
            "f1": round(f1, 6),
            "specificity": round(specificity, 6),
            "balanced_accuracy": round(balanced_accuracy, 6),
        },
        "confusion_matrix": {
            "actual_meteorite_predicted_meteorite": tp,
            "actual_meteorite_predicted_non_meteorite": fn,
            "actual_non_meteorite_predicted_meteorite": fp,
            "actual_non_meteorite_predicted_non_meteorite": tn,
        },
        "score_buckets": {key: dict(value) for key, value in score_buckets.items()},
        "by_subgroup": by_subgroup,
        "calibration": {
            "expected_calibration_error": round(expected_calibration_error, 6),
            "bins": calibration,
        },
        "positive_threshold": POSITIVE_THRESHOLD,
        "uncertain_threshold": UNCERTAIN_THRESHOLD,
        "model_version": MODEL_VERSION,
    }
    recommendations = []
    if fp:
        recommendations.append("Prioriser les faux positifs pour réduire les fausses alertes météorites.")
    if fn:
        recommendations.append("Prioriser les faux négatifs pour préserver le rappel scientifique.")
    if labels.get("meteorite", 0) < 20:
        recommendations.append("Augmenter le nombre de spécimens météoritiques validés avant entraînement.")
    if counts["label_disagreement"]:
        recommendations.append("Ajouter une revue ciblée des images proches du seuil et des désaccords Trio.")
    if counts["subclass_error"]:
        recommendations.append("Créer un lot hard examples dédié aux erreurs de sous-classe météoritique.")
    if counts["quality_related"]:
        recommendations.append("Séparer les cas qualité dégradée et renforcer le protocole de prise de vue.")
    return summary, errors, recommendations


def render_audit_html(summary: dict, recommendations: list[str]) -> str:
    rows = "".join(f"<li>{item}</li>" for item in recommendations)
    return f"""<!doctype html>
<html lang=\"fr\"><head><meta charset=\"utf-8\"><title>Audit Vision Trio</title>
<style>body{{font-family:Arial,sans-serif;margin:2rem}}table{{border-collapse:collapse}}td,th{{border:1px solid #ccc;padding:.4rem}}</style>
</head><body><h1>Audit Vision Trio</h1>
<p>Modèle: {summary.get('model_version')} — seuil positif: {summary.get('positive_threshold')}</p>
<h2>Métriques</h2><table><tr><th>Métrique</th><th>Valeur</th></tr>
{''.join(f"<tr><td>{key}</td><td>{value}</td></tr>" for key, value in summary.get('metrics', {}).items())}
</table><h2>Recommandations</h2><ul>{rows}</ul>
<h2>Résumé</h2><pre>{json.dumps(summary, ensure_ascii=False, indent=2)}</pre>
</body></html>"""
