from typing import Dict, Any

class BusinessOrchestrator:
    def __init__(self):
        self.rare_classes = {"Achondrite", "Carbonee"}
        self.threshold = 0.8084

    def evaluate_decision(self, fusion_output: Dict[str, Any]) -> Dict[str, Any]:
        is_meteorite = fusion_output["is_meteorite"]
        prob = fusion_output["meteorite_probability"]
        dominant_class = fusion_output["dominant_class"]
        confidence = fusion_output["class_confidence"]

        # Valeurs par défaut (Cas Rejet)
        status_code = "DIAGNOSTIC_REJECTED"
        actions = {
            "add_to_collection": False,
            "enable_marketplace_button": False,
            "invite_interior_cut": False
        }
        radar_admin = False

        if is_meteorite:
            if prob >= self.threshold:
                status_code = "DIAGNOSTIC_SUCCESS_HIGH"
                actions["add_to_collection"] = True
                actions["enable_marketplace_button"] = True
                actions["invite_interior_cut"] = True
            else:
                status_code = "DIAGNOSTIC_HESITANT"
                actions["add_to_collection"] = True
                actions["enable_marketplace_button"] = False
                actions["invite_interior_cut"] = True

            # Règle silencieuse du Radar Admin
            if dominant_class in self.rare_classes and confidence >= 0.85:
                radar_admin = True

        return {
            "status_code": status_code,
            "is_meteorite": is_meteorite,
            "meteorite_probability": prob,
            "dominant_class": dominant_class,
            "class_confidence": confidence,
            "actions": actions,
            "trigger_radar_admin": radar_admin,
            "metadata_applied": fusion_output.get("metadata_applied", {})
        }
