import torch
import torch.nn as nn

class MultiTaskDINOv2(nn.Module):
    def __init__(self, num_sub_classes, freeze_backbone=True):
        super(MultiTaskDINOv2, self).__init__()
        
        print("🤖 Chargement du backbone DINOv2 (ViT-Small)...")
        self.backbone = torch.hub.load('facebookresearch/dinov2', 'dinov2_vits14')
        embedding_dim = 384 
        
        if freeze_backbone:
            for param in self.backbone.parameters():
                param.requires_grad = False
            print("🔒 Backbone DINOv2 initialement gelé.")
        
        self.binary_head = nn.Sequential(
            nn.Linear(embedding_dim, 128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, 1)
        )
        
        self.sub_class_head = nn.Sequential(
            nn.Linear(embedding_dim, 128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, num_sub_classes)
        )

    def unfreeze_last_block(self):
        """Dégèle uniquement le dernier bloc de transformateurs (Block 11) et les couches de normalisation finales."""
        # 1. On s'assure que tout le backbone est gelé au départ
        for param in self.backbone.parameters():
            param.requires_grad = False
            
        # 2. On dégèle le bloc 11 (le dernier bloc du ViT-Small qui en compte 12, indexés de 0 à 11)
        for param in self.backbone.blocks[11].parameters():
            param.requires_grad = True
            
        # 3. On dégèle la normalisation finale
        for param in self.backbone.norm.parameters():
            param.requires_grad = True
            
        print("🔓 [Unfreeze] Le bloc 11 de DINOv2 et la couche Norm sont désormais dégelés pour le fine-tuning !")

    def forward(self, x):
        features = self.backbone(x) 
        binary_logits = self.binary_head(features).squeeze(-1)
        sub_class_logits = self.sub_class_head(features)
        return binary_logits, sub_class_logits