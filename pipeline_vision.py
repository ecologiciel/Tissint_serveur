import torch
import torch.nn as nn
import io
import numpy as np
from PIL import Image
from torchvision import transforms
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Dict, Any

# Importation de nos architectures
import model_dinov2
import model_swin
import model_convnext

class VisionPipeline:
    def __init__(self):
        self.device = torch.device("cpu")
        self.num_classes = 6  # ['None', 'Achondrite', 'Carbonee', 'Chondrite', 'Metallique', 'Meteore_Unknown']
        print("🤖 Initialisation et chargement des 3 modèles sur CPU...")
        
        # 1. Chargement DINOv2
        self.mdl_dino = model_dinov2.MultiTaskDINOv2(num_sub_classes=self.num_classes)
        self.mdl_dino.load_state_dict(torch.load("best_dinov2_fine_tuned.pth", map_location=self.device))
        self.mdl_dino.eval().to(self.device)
        
        # 2. Chargement Swin V2
        self.mdl_swin = model_swin.MultiTaskSwinV2(num_sub_classes=self.num_classes)
        self.mdl_swin.load_state_dict(torch.load("best_swin_fine_tuned.pth", map_location=self.device))
        self.mdl_swin.eval().to(self.device)
        
        # 3. Chargement ConvNeXt V2
        self.mdl_conv = model_convnext.MultiTaskConvNeXtV2(num_sub_classes=self.num_classes)
        self.mdl_conv.load_state_dict(torch.load("best_convnext_fine_tuned.pth", map_location=self.device))
        self.mdl_conv.eval().to(self.device)

        # Transformation unifiée pour l'inférence production (224x224)
        self.transform = transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
        ])
        print("🚀 Les 3 modèles sont prêts pour l'inférence parallèle.")

    def _preprocess_image(self, image_bytes: bytes) -> torch.Tensor:
        """Convertit les octets bruts d'une image en tenseur prêt pour le modèle."""
        image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        return self.transform(image).unsqueeze(0).to(self.device)

    def _predict_single_model(self, model_name: str, tensor: torch.Tensor) -> Dict[str, Any]:
        """Exécute l'inférence sur un modèle spécifique de manière isolée pour le thread."""
        with torch.no_grad():
            if model_name == "dino":
                out_bin, out_sub = self.mdl_dino(tensor)
            elif model_name == "swin":
                out_bin, out_sub = self.mdl_swin(tensor)
            elif model_name == "convnext":
                out_bin, out_sub = self.mdl_conv(tensor)
            else:
                raise ValueError("Modèle inconnu")
            
            prob_bin = torch.sigmoid(out_bin).item()
            prob_sub = torch.softmax(out_sub, dim=1).cpu().squeeze(0).numpy().astype(float).tolist()
            
        return {"prob_bin": prob_bin, "prob_sub": prob_sub}

    def predict_image_parallel(self, image_bytes: bytes) -> Dict[str, Dict[str, Any]]:
        """Orchestre l'inférence d'une seule image sur les 3 modèles en parallèle via des threads."""
        tensor = self._preprocess_image(image_bytes)
        models = ["dino", "swin", "convnext"]
        
        results = {}
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(self._predict_single_model, m, tensor): m for m in models}
            for future in futures:
                model_name = futures[future]
                results[model_name] = future.result()
                
        return results

    def process_full_scan(self, list_exterior_bytes: List[bytes], interior_bytes: Optional[bytes] = None) -> Dict[str, Any]:
        """
        Traite l'ensemble du lot de photos (Multi-angles extérieures + Coupe intérieure optionnelle).
        Calcule la moyenne des prédictions pour le lot extérieur.
        """
        # 1. Analyse du lot extérieur
        ext_dino_bin, ext_swin_bin, ext_conv_bin = [], [], []
        ext_dino_sub, ext_swin_sub, ext_conv_sub = [], [], []
        
        for img_bytes in list_exterior_bytes:
            res = self.predict_image_parallel(img_bytes)
            
            ext_dino_bin.append(res["dino"]["prob_bin"])
            ext_dino_sub.append(res["dino"]["prob_sub"])
            
            ext_swin_bin.append(res["swin"]["prob_bin"])
            ext_swin_sub.append(res["swin"]["prob_sub"])
            
            ext_conv_bin.append(res["convnext"]["prob_bin"])
            ext_conv_sub.append(res["convnext"]["prob_sub"])
            
        # Moyennage des scores extérieurs pour l'analyse volumétrique (Utilisation de numpy)
        output = {
            "exterior": {
                "dino": {
                    "prob_bin": float(np.mean(ext_dino_bin)), 
                    "prob_sub": np.mean(ext_dino_sub, axis=0).astype(float).tolist()
                },
                "swin": {
                    "prob_bin": float(np.mean(ext_swin_bin)), 
                    "prob_sub": np.mean(ext_swin_sub, axis=0).astype(float).tolist()
                },
                "convnext": {
                    "prob_bin": float(np.mean(ext_conv_bin)), 
                    "prob_sub": np.mean(ext_conv_sub, axis=0).astype(float).tolist()
                }
            },
            "interior": None
        }
        
        # 2. Analyse de la coupe intérieure si présente (Étape 2 anticipée ou directe)
        if interior_bytes:
            res_int = self.predict_image_parallel(interior_bytes)
            output["interior"] = res_int
            
        return output
