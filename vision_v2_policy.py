"""Release-gated folder decision policy for a future Vision Trio v2 deployment.

This module is intentionally not wired into the v1 inference route. A v2 weight
bundle may call it only when its signed release.json has passed the frozen-test
publication gate.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import median
from typing import Any


@dataclass(frozen=True)
class V2FolderDecision:
    verdict: str
    calibrated_score: float
    auto_publish: bool
    reasons: list[str]
    disclaimer: str = "Tri visuel Tissint, non certifie par laboratoire."


def release_can_auto_publish(release: dict[str, Any]) -> bool:
    gate = release.get("publication_gate") or {}
    return bool(
        release.get("contract_version") == "trio-vision-release-v2"
        and release.get("automatic_publication_enabled")
        and float(gate.get("fpr", 1.0)) <= 0.01
        and float(gate.get("fpr_upper_95", 1.0)) <= 0.01
    )


def decide_folder(views: list[dict[str, Any]], release: dict[str, Any]) -> V2FolderDecision:
    """Fuse exactly the conservative v2 contract: median, coherence, and abstention."""
    if len(views) < 3:
        return V2FolderDecision("uncertain", 0.0, False, ["insufficient_views"])
    scores = [float(view.get("calibrated_probability", 0.0)) for view in views]
    score = float(median(scores))
    reasons: list[str] = []
    if any(not bool(view.get("quality_passed", False)) for view in views):
        reasons.append("low_quality")
    if any(bool(view.get("out_of_distribution", False)) for view in views):
        reasons.append("out_of_distribution")
    if max(scores) - min(scores) >= float(release.get("view_disagreement_max", 0.20)):
        reasons.append("view_conflict")
    trio_scores = [view.get("trio_disagreement", 0.0) for view in views]
    if any(float(value or 0.0) >= float(release.get("trio_disagreement_max", 0.15)) for value in trio_scores):
        reasons.append("trio_disagreement")
    if reasons:
        return V2FolderDecision("uncertain", score, False, reasons)
    negative_threshold = float(release.get("negative_threshold", 0.20))
    publish_threshold = float(release.get("publication_gate", {}).get("threshold", math.inf))
    if all(value < negative_threshold for value in scores):
        return V2FolderDecision("rejected", score, False, ["three_consistent_negative_views"])
    coherent_high_views = sum(value >= publish_threshold for value in scores)
    if score >= publish_threshold and coherent_high_views >= 2:
        return V2FolderDecision(
            "publishable_candidate",
            score,
            release_can_auto_publish(release),
            ["two_or_more_coherent_high_views"],
        )
    return V2FolderDecision("uncertain", score, False, ["intermediate_score"])
