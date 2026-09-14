from vision_v2_policy import decide_folder, release_can_auto_publish


def _release(**overrides):
    value = {
        "contract_version": "trio-vision-release-v2",
        "automatic_publication_enabled": True,
        "negative_threshold": 0.20,
        "publication_gate": {"threshold": 0.92, "fpr": 0.005, "fpr_upper_95": 0.009},
    }
    value.update(overrides)
    return value


def test_v2_policy_forces_doubt_when_one_view_is_ood():
    views = [
        {"calibrated_probability": 0.96, "quality_passed": True},
        {"calibrated_probability": 0.95, "quality_passed": True, "out_of_distribution": True},
        {"calibrated_probability": 0.94, "quality_passed": True},
    ]
    decision = decide_folder(views, _release())
    assert decision.verdict == "uncertain"
    assert not decision.auto_publish


def test_v2_policy_never_auto_publishes_without_release_gate():
    views = [{"calibrated_probability": score, "quality_passed": True} for score in (0.94, 0.95, 0.96)]
    release = _release(automatic_publication_enabled=False)
    assert not release_can_auto_publish(release)
    assert not decide_folder(views, release).auto_publish
