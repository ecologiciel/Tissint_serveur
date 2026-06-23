from typing import Any, Dict, Optional


class BusinessOrchestrator:
    def __init__(self):
        self.rare_classes = {"Achondrite", "Carbonee"}
        self.threshold = 0.8084

    def _resolve_message_language(self, language: Optional[str] = None) -> str:
        if not language:
            return "ar"
        requested = language.lower()
        return "fr" if any(part.strip().startswith("fr") for part in requested.split(",")) else "ar"

    def build_scan_actions(self, status_code: str) -> Dict[str, bool]:
        return {
            "add_to_collection": status_code in {"DIAGNOSTIC_SUCCESS_HIGH", "DIAGNOSTIC_HESITANT"},
            "enable_marketplace_button": status_code == "DIAGNOSTIC_SUCCESS_HIGH",
            "invite_interior_cut": status_code != "DIAGNOSTIC_REJECTED",
        }

    def build_message(
        self,
        status_code: str,
        dominant_class: str,
        meteorite_probability: float,
        actions: Dict[str, bool],
        language: Optional[str] = None,
    ) -> Dict[str, str]:
        resolved_language = self._resolve_message_language(language)
        score = f"{meteorite_probability * 100:.1f}"

        if status_code == "DIAGNOSTIC_SUCCESS_HIGH":
            tone = "success"
            title = "Candidat très prometteur" if resolved_language == "fr" else "مرشح واعد جدا"
            if resolved_language == "fr":
                body = (
                    f"Félicitations, candidat très prometteur. Le score d'analyse est de {score}% "
                    f"et la classe la plus probable est {dominant_class}. Ce spécimen est éligible "
                    "au marketplace. Une photo de coupe intérieure renforcera la classification "
                    "et pourra améliorer sa valeur."
                )
            else:
                body = (
                    f"تهانينا، هذا مرشح واعد جدا. نتيجة التحليل هي {score}% "
                    f"والفئة الأقرب هي {dominant_class}. هذه العينة مؤهلة للعرض في السوق. "
                    "إضافة صورة لقطع داخلي ستقوي التصنيف وقد ترفع من قيمتها."
                )
        elif status_code == "DIAGNOSTIC_HESITANT":
            tone = "warning"
            title = "Résultat à confirmer" if resolved_language == "fr" else "نتيجة تحتاج إلى تأكيد"
            if resolved_language == "fr":
                body = (
                    f"Félicitations avec prudence. Le score d'analyse est de {score}% "
                    f"et la classe la plus probable est {dominant_class}, mais le résultat reste "
                    "incertain. Une photo de coupe intérieure est indispensable pour trancher."
                )
            else:
                body = (
                    f"تهانينا بحذر. نتيجة التحليل هي {score}% "
                    f"والفئة الأقرب هي {dominant_class}، لكن النتيجة ما زالت غير حاسمة. "
                    "صورة لقطع داخلي ضرورية جدا للحسم."
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

    def evaluate_decision(self, fusion_output: Dict[str, Any], language: Optional[str] = None) -> Dict[str, Any]:
        is_meteorite = fusion_output["is_meteorite"]
        prob = fusion_output["meteorite_probability"]
        dominant_class = fusion_output["dominant_class"]
        confidence = fusion_output["class_confidence"]

        status_code = "DIAGNOSTIC_REJECTED"
        radar_admin = False

        if is_meteorite:
            if prob >= self.threshold:
                status_code = "DIAGNOSTIC_SUCCESS_HIGH"
            else:
                status_code = "DIAGNOSTIC_HESITANT"

            if dominant_class in self.rare_classes and confidence >= 0.85:
                radar_admin = True

        actions = self.build_scan_actions(status_code)

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
            ),
        }
