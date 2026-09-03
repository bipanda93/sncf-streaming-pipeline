# Journal des incidents — 3 septembre 2026

## 1. Page 3 -- trois corrections liées au filtrage multi-tables

| Visuel | Symptôme | Cause | Correction |
|---|---|---|---|
| Nb Perturbations par Région et Niveau | Même total partout ("414") | Relation by_region -> context en sens unique ("Une seule"), pas de relation directe alert_level | TREATAS + VALUES(alert_level) dans CALCULATE |
| Niveau Moyen (Priorité) par région | Même valeur partout ("2,13") | Mesure sans TREATAS, calculée sur toute la table sans respecter le filtre région | TREATAS ajouté |
| Niveau par région (tableau) | Valeur identique par colonne | Mauvais champ source -- alert_level pris depuis by_region (qui n'a pas cette colonne) au lieu de context | Champ corrigé, pas un problème de TREATAS |

Le troisième cas confirme un vrai réflexe utile : avant d'ajouter de la
complexité DAX, vérifier d'abord que le bon champ source est utilisé --
pas toujours un problème de relation.

## 2. Page 4 -- première utilisation de gold_punctuality_trends

Première exploitation de cette table (21 814 lignes), jamais touchée
avant. 4 visuels construits : courbe de ponctualité dans le temps,
comparaison TER/Intercités/TGV, taux d'annulation par région, écart
taux actuel vs historique par région (TREATAS réutilisé, 3e application
du pattern).

## 3. Découverte -- granularités mélangées dans gold_punctuality_trends

En construisant le taux d'annulation, découverte que TGV apparaît sur
3 niveaux simultanés (national/axe/liaison) dans la même colonne --
risque de double comptage si sommé sans filtrer sur scope_type.
Corrigé en filtrant explicitement sur scope_type="region" (TER
uniquement, seul type disponible à ce niveau).

**Deuxième découverte, plus profonde** : scope_value contient un mélange
d'anciennes appellations régionales pré-2016 ("Rhône Alpes", "Midi
Pyrénées", "Languedoc Roussillon") ET de nouvelles régions déjà
fusionnées (Occitanie, Auvergne-Rhône-Alpes) -- chevauchement non encore
quantifié précisément, correction reportée à une session dédiée.

**Vérifié et confirmé sans risque** : ce problème est strictement isolé
à gold_punctuality_trends. La table gold_disruption_context
(regions_affectees), utilisée par la carte et tous les tableaux des
Pages 2 et 3, ne contient que les 13 régions post-2016 -- aucune
contamination du reste du dashboard.

**Décision** : visuel "Taux d'annulation" construit avec un titre
explicite ("par ancien découpage régional TER") plutôt que de bloquer
la session sur une correction de fond. Vraie correction (table de
correspondance ou re-agrégation) à traiter séparément.
