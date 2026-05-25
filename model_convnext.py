import torch
import torch.nn as nn
import timm

class MultiTaskConvNeXtV2(nn.Module):
    def __init__(self, num_sub_classes, freeze_backbone=True):
        super(MultiTaskConvNeXtV2, self).__init__()
        
        print("🤖 Chargement du backbone ConvNeXt V2 (Nano)...")
        # Chargement via timm (modèle pré-entraîné sur ImageNet-1k)
        self.backbone = timm.create_model('convnextv2_nano', pretrained=True)
        
        # On extrait la dimension des caractéristiques finales (640 pour la version Nano)
        embedding_dim = self.backbone.head.fc.in_features
        
        # On neutralise la tête de classification par défaut de timm
        self.backbone.reset_classifier(num_classes=0)
        
        if freeze_backbone:
            for param in self.backbone.parameters():
                param.requires_grad = False
            print("🔒 Backbone ConvNeXt V2 gelé (Seules les têtes vont s'entraîner)")
        else:
            print("🔓 Backbone ConvNeXt V2 dégelé")

        # Tête 1 : Classification Binaire (Météorite vs Terrestre)
        self.binary_head = nn.Sequential(
            nn.Linear(embedding_dim, 256),
            nn.BatchNorm1d(256),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(256, 1)
        )
        
        # Tête 2 : Classification des Sous-classes (6 classes)
        self.sub_class_head = nn.Sequential(
            nn.Linear(embedding_dim, 256),
            nn.BatchNorm1d(256),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(256, num_sub_classes)
        )

    def unfreeze_last_stage(self):
        """Dégèle le dernier bloc de convolution (Stage 3) pour affiner la capture des textures."""
        for param in self.backbone.parameters():
            param.requires_grad = False
            
        # Dans timm, les blocs de ConvNeXt sont dans 'stages'. Le dernier est stages[3]
        for param in self.backbone.stages[3].parameters():
            param.requires_grad = True
            
        print("🔓 [Unfreeze] Le Stage 3 de ConvNeXt V2 est dégelé pour le fine-tuning !")

    def forward(self, x):
        # Sortie du backbone : [Batch_Size, 640]
        features = self.backbone(x)
        
        binary_logits = self.binary_head(features).squeeze(-1)
        sub_class_logits = self.sub_class_head(features)
        
        return binary_logits, sub_class_logits