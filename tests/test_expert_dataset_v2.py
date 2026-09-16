import pytest

from expert_dataset import validate_v2_annotation, v2_stratified_group_split, v2_training_role


def test_v2_annotation_does_not_allow_a_forced_subclass_on_uncertain_case():
    with pytest.raises(ValueError):
        validate_v2_annotation("uncertain", "chondrite", None, "field_expert_single")


def test_v2_annotation_accepts_unknown_candidate_subclass():
    validate_v2_annotation("meteorite_candidate", None, None, "field_expert_single")


def test_v2_split_keeps_each_group_in_exactly_one_split():
    groups = {f"specimen-{index}": "candidate" if index % 2 else "terrestrial" for index in range(60)}
    first = v2_stratified_group_split(groups)
    second = v2_stratified_group_split(groups)
    assert first == second
    assert set(first) == set(groups)
    assert set(first.values()) <= {"train", "validation", "test"}


def test_weak_facebook_label_is_train_only_role():
    assert v2_training_role(
        verdict="terrestrial",
        evidence_tier="field_expert_single",
        confidence="high",
        quality_passed=True,
        audit_status="not_required",
        source_type="facebook_group",
    ) == "weak_labels"


def test_poor_human_quality_is_excluded_from_candidate_training():
    assert v2_training_role(
        verdict="terrestrial",
        evidence_tier="catalog_verified",
        confidence="high",
        quality_passed=True,
        audit_status="audited",
        source_type="catalog",
        human_image_quality="poor",
    ) == "unusable"


def test_catalogue_reference_requires_a_non_uncertain_label():
    assert v2_training_role(
        verdict="uncertain",
        evidence_tier="catalog_verified",
        confidence="high",
        quality_passed=True,
        audit_status="audited",
        source_type="catalog",
    ) == "unresolved"
