# Exploitation du mode expert

## Cycle opérationnel

1. Créer un compte expert via POST /api/v1/admin/expert-accounts, puis transmettre
   le mot de passe par un canal sécurisé.
2. Créer un dataset depuis un compte admin.
3. Importer les originaux avec scripts/import_expert_dataset.py ou l’upload S3 présigné.
4. Lancer expert_worker.py en staging/production.
5. Faire annoter les cas dans /expert.
6. Contrôler les métriques et lancer l’audit depuis l’espace expert.
7. Geler le jeu de test par spécimen.
8. Générer un export versionné après validation des consensus.

## Worker

Le worker traite les items inference_pending et imported. Le statut est persistant,
les fichiers restent dans le stockage et un redémarrage reprend la file. Exemple :

    python expert_worker.py --batch-size 4

L’API peut exécuter une tâche de fond pour le développement local, mais le worker
séparé est requis pour les lots de production.

## Export et audit

Un export contient manifest.jsonl, train.jsonl, validation.jsonl et test.jsonl. Le
split est déterministe à partir du specimen_id. Les images uncertain, non utilisables
ou sans consensus validé ne sont pas exportées comme labels supervisés fermes.

Les URLs S3 sont privées et présignées. En production, configurer
STORAGE_BACKEND=s3, le bucket privé, le versioning, le chiffrement et un TTL court
pour les URLs. Le mode local est réservé au développement et aux tests.
