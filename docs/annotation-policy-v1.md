# Annotation Policy v1

Cette politique est la référence gelée de taxonomy-v1. Toute évolution doit créer
une nouvelle version de politique et de taxonomie ; une annotation existante n’est
jamais réécrite.

## Verdict principal

- meteorite : l’expert estime que l’objet est une météorite.
- terrestrial_rock : l’objet est une roche terrestre ou un matériau terrestre connu.
- uncertain : les éléments visuels ne permettent pas une décision fiable.
- unusable : l’image ne permet pas une lecture scientifique (flou, exposition, objet absent).
- non_rock : l’objet n’est pas une roche.

Une valeur inconnue doit être saisie comme unknown, not_observed ou not_applicable.
Il est interdit d’inventer une origine, un groupe de spécimen ou une coupe intérieure.

## Sous-classes contrôlées

Pour meteorite : chondrite, carbonaceous_chondrite, achondrite, iron_meteorite,
stony_iron, meteorite_unknown.

Pour terrestrial_rock : slag, hematite, magnetite, basalt, quartz,
sedimentary_rock, industrial_material, terrestrial_unknown.

L’expert choisit la valeur meteorite_unknown ou terrestrial_unknown lorsqu’il sait
que la famille est pertinente mais ne peut pas la préciser. Il choisit uncertain si
même le verdict principal n’est pas fiable.

## Confiance et commentaire

La confiance est obligatoire pour une annotation label ou review : high, medium,
low, not_assessed. Le commentaire décrit une observation vérifiable : texture,
inclusions, métal visible, croûte de fusion, coupe, éclairage ou limitation de
l’image. Il ne doit pas transformer une hypothèse en vérité.

## Seconde revue obligatoire

Le serveur oriente vers une seconde revue les météorites, les verdicts uncertain,
les confiances faibles, les classes rares, les images avec problèmes qualité, les
désaccords inter-modèles et les cas où le verdict contredit fortement le score Trio.
Deux annotations identiques valident le consensus ; un désaccord reste needs_review.

## Spécimens et images

Toutes les vues d’un même spécimen partagent le même specimen_id. Les images d’un
spécimen ne doivent jamais être réparties entre train, validation et test. Une image
avec plusieurs objets est uncertain ou unusable si le spécimen annoté ne peut pas
être isolé de manière fiable.
