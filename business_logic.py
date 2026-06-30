from typing import Any, Dict, Optional


HESITANT_THRESHOLD = 0.70
SUCCESS_THRESHOLD = 0.8084
UNKNOWN_CLASS = "Meteore_Unknown"
RARE_CLASS_CONFIDENCE_THRESHOLD = 0.85


class BusinessOrchestrator:
    def __init__(self):
        self.rare_classes = {"Achondrite", "Carbonee"}
        self.hesitant_threshold = HESITANT_THRESHOLD
        self.success_threshold = SUCCESS_THRESHOLD
        self.threshold = SUCCESS_THRESHOLD

    def _resolve_message_language(self, language: Optional[str] = None) -> str:
        if not language:
            return "ar"
        requested = language.lower()
        return "fr" if any(part.strip().startswith("fr") for part in requested.split(",")) else "ar"

    def _should_show_subclass(self, dominant_class: str) -> bool:
        return dominant_class != UNKNOWN_CLASS

    def build_scan_actions(self, status_code: str, has_interior_cut: bool = False) -> Dict[str, bool]:
        return {
            "add_to_collection": status_code in {"DIAGNOSTIC_SUCCESS_HIGH", "DIAGNOSTIC_HESITANT"},
            "enable_marketplace_button": status_code == "DIAGNOSTIC_SUCCESS_HIGH",
            "invite_interior_cut": status_code != "DIAGNOSTIC_REJECTED" and not has_interior_cut,
        }

    def build_message(
        self,
        status_code: str,
        dominant_class: str,
        meteorite_probability: float,
        actions: Dict[str, bool],
        language: Optional[str] = None,
        has_interior_cut: bool = False,
    ) -> Dict[str, str]:
        resolved_language = self._resolve_message_language(language)
        score = f"{meteorite_probability * 100:.1f}"
        show_subclass = self._should_show_subclass(dominant_class)

        if status_code == "DIAGNOSTIC_SUCCESS_HIGH":
            tone = "success"
            title = "Candidat très prometteur" if resolved_language == "fr" else "مرشح واعد جدا"
            if resolved_language == "fr":
                class_part = f" et la classe la plus probable est {dominant_class}" if show_subclass else ""
                cut_part = (
                    "La photo de coupe intérieure fournie a été prise en compte dans cette analyse."
                    if has_interior_cut
                    else "Une photo de coupe intérieure renforcera la classification et pourra améliorer sa valeur."
                )
                body = (
                    f"Félicitations, candidat très prometteur. Le score d'analyse est de {score}%"
                    f"{class_part}. Ce spécimen est éligible au marketplace. {cut_part}"
                )
            else:
                class_part = f" والفئة الأقرب هي {dominant_class}" if show_subclass else ""
                cut_part = (
                    "تم احتساب صورة القطع الداخلي المرفقة في هذا التحليل."
                    if has_interior_cut
                    else "إضافة صورة لقطع داخلي ستقوي التصنيف وقد ترفع من قيمتها."
                )
                body = (
                    f"تهانينا، هذا مرشح واعد جدا. نتيجة التحليل هي {score}%"
                    f"{class_part}. هذه العينة مؤهلة للعرض في السوق. {cut_part}"
                )
        elif status_code == "DIAGNOSTIC_HESITANT":
            tone = "warning"
            title = "Résultat à confirmer" if resolved_language == "fr" else "نتيجة تحتاج إلى تأكيد"
            if resolved_language == "fr":
                class_part = f" et la classe la plus probable est {dominant_class}" if show_subclass else ""
                cut_part = (
                    "La photo de coupe intérieure fournie a été prise en compte, mais le résultat reste à confirmer."
                    if has_interior_cut
                    else "Une photo de coupe intérieure est indispensable pour trancher."
                )
                body = (
                    f"Félicitations avec prudence. Le score d'analyse est de {score}%"
                    f"{class_part}, mais le résultat reste incertain. {cut_part}"
                )
            else:
                class_part = f" والفئة الأقرب هي {dominant_class}" if show_subclass else ""
                cut_part = (
                    "تم احتساب صورة القطع الداخلي المرفقة، لكن النتيجة ما زالت تحتاج إلى تأكيد."
                    if has_interior_cut
                    else "صورة لقطع داخلي ضرورية جدا للحسم."
                )
                body = (
                    f"تهانينا بحذر. نتيجة التحليل هي {score}%"
                    f"{class_part}، لكن النتيجة ما زالت غير حاسمة. {cut_part}"
                )
        else:
            tone = "neutral"
            title = "Continuez vos recherches" if resolved_language == "fr" else "واصل البحث"
            if resolved_language == "fr":
                body = (
                    f"Continuez vos recherches. Le score d'analyse est de {score}%. "
                    "Il s'agit probablement d'une pierre minérale terrestre. Continuez, "
                    "vous êtes sur la bonne voie."
                )
            else:
                body = (
                    f"واصل البحث. نتيجة التحليل هي {score}%. غالبا هذه عينة معدنية أرضية "
                    "وليست نيزكا. استمر، فأنت على الطريق الصحيح."
                )

        return {
            "language": resolved_language,
            "tone": tone,
            "title": title,
            "body": body,
        }

    def evaluate_decision(
        self,
        fusion_output: Dict[str, Any],
        language: Optional[str] = None,
        has_interior_cut: bool = False,
    ) -> Dict[str, Any]:
        prob = fusion_output["meteorite_probability"]
        dominant_class = fusion_output["dominant_class"]
        confidence = fusion_output["class_confidence"]
        is_meteorite = bool(fusion_output["is_meteorite"] and prob >= self.hesitant_threshold)

        status_code = "DIAGNOSTIC_REJECTED"
        radar_admin = False

        if is_meteorite:
            if prob >= self.success_threshold:
                status_code = "DIAGNOSTIC_SUCCESS_HIGH"
            else:
                status_code = "DIAGNOSTIC_HESITANT"

            if dominant_class in self.rare_classes and confidence >= RARE_CLASS_CONFIDENCE_THRESHOLD:
                radar_admin = True

        actions = self.build_scan_actions(status_code, has_interior_cut=has_interior_cut)

        return {
            "status_code": status_code,
            "is_meteorite": is_meteorite,
            "meteorite_probability": prob,
            "dominant_class": dominant_class,
            "class_confidence": confidence,
            "actions": actions,
            "trigger_radar_admin": radar_admin,
            "metadata_applied": fusion_output.get("metadata_applied", {}),
            "message": self.build_message(
                status_code=status_code,
                dominant_class=dominant_class,
                meteorite_probability=prob,
                actions=actions,
                language=language,
                has_interior_cut=has_interior_cut,
            ),
        }
