import torch
import torch.nn as nn
import torchvision.models as models

class MultiTaskSwinV2(nn.Module):
    def __init__(self, num_sub_classes, freeze_backbone=True):
        super(MultiTaskSwinV2, self).__init__()
        
        print("🤖 Chargement du backbone Swin Transformer V2 (Tiny)...")
        weights = models.Swin_V2_T_Weights.DEFAULT
        self.backbone = models.swin_v2_t(weights=weights)
        
        embedding_dim = self.backbone.head.in_features
        self.backbone.head = nn.Identity()
        
        if freeze_backbone:
            for param in self.backbone.parameters():
                param.requires_grad = False
            print("🔒 Backbone Swin V2 initialement gelé.")
        else:
            print("🔓 Backbone Swin V2 dégelé.")

        # Tête 1 : Binaire
        self.binary_head = nn.Sequential(
            nn.Linear(embedding_dim, 256),
            nn.LayerNorm(256),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(256, 1)
        )
        
        # Tête 2 : Sous-classes
        self.sub_class_head = nn.Sequential(
            nn.Linear(embedding_dim, 256),
            nn.LayerNorm(256),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(256, num_sub_classes)
        )

    def unfreeze_last_stage(self):
        """Dégèle uniquement le dernier étage (Stage 4) de Swin V2 ainsi que la normalisation finale."""
        # 1. On s'assure que tout le backbone est bien bloqué au départ
        for param in self.backbone.parameters():
            param.requires_grad = False
            
        # 2. On dégèle le stage 4 (les dernières couches d'attention du Transformer)
        for param in self.backbone.features[7].parameters():
            param.requires_grad = True
            
        # 3. On dégèle la couche de normalisation finale
        for param in self.backbone.norm.parameters():
            param.requires_grad = True
            
        print("🔓 [Unfreeze] Le Stage 4 (features[7]) et la Norm de Swin V2 sont dégelés pour le fine-tuning !")

    def forward(self, x):
        features = self.backbone(x)
        binary_logits = self.binary_head(features).squeeze(-1)
        sub_class_logits = self.sub_class_head(features)
        return binary_logits, sub_class_logits