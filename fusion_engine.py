import numpy as np
from typing import List, Optional, Dict, Any

class MeteoriteFusionEngine:
    def __init__(self):
        self.classes = ['None', 'Achondrite', 'Carbonee', 'Chondrite', 'Metallique', 'Meteore_Unknown']

    def fuse_outputs(
        self, 
        vision_outputs: Dict[str, Any], 
        weight: Optional[float] = None, 
        magnetic: Optional[bool] = None,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Fusionne le triptyque de vision et applique les filtres physiques (poids, magnétisme).
        """
        # 1. Extraction et Soft Voting du bloc Extérieur
        ext_data = vision_outputs["exterior"]
        ext_bin = np.mean([ext_data["dino"]["prob_bin"], ext_data["swin"]["prob_bin"], ext_data["convnext"]["prob_bin"]])
        ext_sub = np.mean([ext_data["dino"]["prob_sub"], ext_data["swin"]["prob_sub"], ext_data["convnext"]["prob_sub"]], axis=0)

        final_bin_prob = float(ext_bin)
        final_sub_vector = ext_sub.astype(float)

        # 2. Intégration de l'analyse Intérieure (si disponible)
        if vision_outputs.get("interior") is not None:
            int_data = vision_outputs["interior"]
            int_bin = np.mean([int_data["dino"]["prob_bin"], int_data["swin"]["prob_bin"], int_data["convnext"]["prob_bin"]])
            int_sub = np.mean([int_data["dino"]["prob_sub"], int_data["swin"]["prob_sub"], int_data["convnext"]["prob_sub"]], axis=0)
            
            # Fusion binaire équilibrée
            final_bin_prob = float((ext_bin + int_bin) / 2.0)
            # Avantage scientifique à la coupe interne pour déterminer la famille (40/60)
            final_sub_vector = (ext_sub * 0.4) + (int_sub * 0.6)
            final_sub_vector = final_sub_vector.astype(float)

        # 3. Application des règles expertes alphanumériques
        predicted_class_idx = int(np.argmax(final_sub_vector))
        predicted_class_name = self.classes[predicted_class_idx]
        class_confidence = float(final_sub_vector[predicted_class_idx])

        if magnetic is not None:
            # Cas critique : Détection visuelle métallique démentie par l'absence de magnétisme
            if predicted_class_name == "Metallique" and not magnetic:
                # Anomalie physique : On écrase la confiance de la sous-classe et on dégrade le score binaire
                final_sub_vector[4] = 0.05  # Effondrement de la probabilité métallique
                final_bin_prob = min(final_bin_prob, 0.40)  # La probabilité globale chute sous le seuil de doutes
                
                # Recalcul de la classe dominante suite à la pénalité
                predicted_class_idx = int(np.argmax(final_sub_vector))
                predicted_class_name = self.classes[predicted_class_idx]
                class_confidence = float(final_sub_vector[predicted_class_idx])
                print("⚠️ Alerte Fusion : Incohérence majeure détectée entre le visuel Métallique et le Magnétisme (False). Score pénalisé.")
            
            # Cas favorable : Confirmation du magnétisme pour une roche identifiée comme métallique ou chondrite
            elif predicted_class_name in ["Metallique", "Chondrite"] and magnetic:
                final_bin_prob = min(final_bin_prob * 1.05, 1.0) # Bonus de sécurité de 5%
                class_confidence = min(class_confidence * 1.05, 1.0)

        # 4. Formatage du verdict final
        is_meteorite = final_bin_prob >= 0.5
        
        # Sécurité : Si le filtre binaire dit "Oui" mais qu'aucune sous-classe n'est solide, on bascule sur Unknown
        if is_meteorite and predicted_class_name == "None":
            predicted_class_name = "Meteore_Unknown"
            class_confidence = float(final_bin_prob)

        return {
            "is_meteorite": bool(is_meteorite),
            "meteorite_probability": float(final_bin_prob),
            "dominant_class": predicted_class_name,
            "class_confidence": float(class_confidence),
            "metadata_applied": {
                "weight_provided": weight is not None,
                "magnetic_status": magnetic,
                "has_coordinates": latitude is not None and longitude is not None
            }
        }
