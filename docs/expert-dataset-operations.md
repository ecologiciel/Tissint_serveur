# Exploitation du mode expert

## Cycle opérationnel

1. Un admin attribue le rôle `expert` depuis l’espace Expert ou crée un compte expert.
2. Il crée un dataset et un lot d’import, puis atteste le droit d’utiliser les images.
3. Le PWA choisit un dossier ou plusieurs images. Avec S3, les fichiers vont directement
   vers les URLs présignées; sinon l’API traite les fichiers un par un.
4. Le worker lance Vision Trio, conserve SHA-256, le quasi-doublon, le rapport qualité et
   les trois sorties de modèle.
5. Les experts annotent sans valeur préremplie. Les cas incertains, dégradés, les
   désaccords du Trio et 10% d’échantillons de contrôle sont dirigés vers une seconde revue.
6. Un admin arbitre les conflits. Seules les décisions humaines à confiance `high` et de
   qualité suffisante deviennent éligibles à l’entraînement.
7. L’audit compare le Trio aux décisions humaines et rend compte des erreurs, de la
   calibration, des sous-groupes et de l’accord humain.
8. L’export ZIP est un snapshot immuable: images, manifests JSONL, CSV, taxonomie,
   checksums et fiche dataset. Il utilise un split déterministe 70/15/15 par spécimen ou
   groupe de quasi-doublons.

## Worker

Le worker traite les items inference_pending et imported. Le statut est persistant,
les fichiers restent dans le stockage et un redémarrage reprend la file. Exemple :

    python expert_worker.py --batch-size 4

L’API peut exécuter une tâche de fond pour le développement local, mais le worker
séparé est requis pour les lots de production.

## Export et audit

Un export contient `images/train`, `images/validation`, `images/test`, `manifest.jsonl`,
les manifests par split, `annotations.csv`, `taxonomy.json`, `checksums.sha256` et une
fiche dataset. Les images `uncertain`, non utilisables, de qualité insuffisante ou sans
décision humaine à confiance haute restent documentées dans les manifests d’exclusion,
mais n’entrent jamais dans les labels supervisés.

Les URLs S3 sont privées et présignées. En production, configurer
STORAGE_BACKEND=s3, le bucket privé, le versioning, le chiffrement et un TTL court
pour les URLs. Le mode local est réservé au développement et aux tests.
