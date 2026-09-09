# Journal des incidents — 9 septembre 2026

## Audit qualité de données — granularités mélangées dans gold_punctuality_trends

**Contexte** : correction prévue depuis le 03/09 (dette technique), reprise
et menée à terme ce jour.

**Découverte** : scope_value (31 valeurs distinctes, scope_type="region")
mélange 3 granularités différentes :
- 16 anciennes régions administratives (pré-2016 à 2020 selon les cas)
- 7 régions actuelles post-fusion
- 3 sous-réseaux TER (Étoile Amiens confirmé via recherche externe --
  lot de lignes représentant 15-17% du réseau Hauts-de-France ; Loire
  Océan et Sud Azur suspectés similaires par schéma structurel, non
  confirmés avec la même rigueur)

**Vérification temporelle** : anciennes et nouvelles appellations
régionales ne se chevauchent JAMAIS dans le temps (transition propre,
échelonnée 2016-2020 selon les régions) -- pas un bug de mélange, une
vraie évolution de nommage fidèlement reflétée par les données.

**Vérification du double comptage** : Étoile Amiens (dès janvier 2025)
chevauche intégralement Hauts-de-France sans que ce dernier ne baisse en
conséquence (-276 sur 26 829, quand Étoile Amiens représente ~4 777/mois)
-- confirme un double comptage réel d'environ 18% sur l'échantillon testé
si sommé sans filtrage.

**Décision** : aucune donnée supprimée. Filtrage explicite en DAX
(exclusion de Etoile Amiens/Loire Océan/Sud Azur) pour tout total agrégé
par région, données individuelles conservées et consultables au niveau
sous-réseau si besoin.

## CV mis à jour

Volumes réels vérifiés : Bronze 166 917 lignes (vs 58 838 au 31/08),
Silver 17 778 perturbations distinctes, historique ponctualité 21 814
lignes inchangé. Entrée CV enrichie avec l'audit qualité de données
ci-dessus comme bullet différenciant.
