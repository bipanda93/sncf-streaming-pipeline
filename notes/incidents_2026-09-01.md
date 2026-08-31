# Journal des incidents — 1er septembre 2026

## 1. Export Silver échoue -- colonne imbriquée impacted_objects

| | |
|---|---|
| **Contexte** | Ajout de sncf_disruptions (Silver) à l'export Power BI, nécessaire pour calculer des mesures journalières (Gold ne contient que les perturbations actives à l'instant T) |
| **Symptôme** | `ArrowInvalid: cannot mix list and non-list, non-null values` sur la colonne `impacted_objects` |
| **Cause racine** | `impacted_objects` est une structure imbriquée complexe (liste d'objets gares/horaires, héritée du schéma JSON brut) -- jamais aplatie à ce stade, contrairement aux tables Gold. PyArrow ne sait pas l'écrire en Parquet telle quelle |
| **Correction** | Sélection explicite des colonnes utiles à Power BI avant l'export de Silver, excluant `impacted_objects` et les colonnes techniques Kafka |
| **Résultat** | sncf_disruptions exporté avec succès (7961 lignes, vs 417 pour Gold -- cohérent avec l'historique complet actif+passé) |

## 2. Mesure DAX "Jour" créée par erreur, cascade de 3 échecs

| | |
|---|---|
| **Contexte** | Construction des mesures temporelles (Perturbations Aujourd'hui, Moyenne Journalière) dans Power BI Desktop |
| **Symptôme** | 3 erreurs en cascade : "Jour introuvable", "fonction PLACEHOLDER non autorisée", "valeur unique indéterminée pour period_begin" |
| **Cause racine** | "Jour" créé via "Nouvelle mesure" au lieu de "Nouvelle colonne" -- une mesure n'a pas de contexte de ligne, ne peut pas dériver une date par perturbation sans qu'on le lui dise explicitement. Les 2 mesures dépendantes héritaient de cette erreur de fond |
| **Correction** | Suppression des 3 éléments cassés, recréation de "Jour" comme colonne calculée (onglet "Outils de colonne", pas "Outils de mesure"), puis recréation des 2 mesures qui en dépendent |
| **Point de vigilance retenu** | Toujours vérifier l'onglet actif en haut de l'écran (Outils de colonne vs Outils de mesure) avant de valider une définition DAX -- c'est le seul repère visuel fiable |

## Clarification obtenue en fin de session -- 3 mesures de comptage, 3 sens différents

| Mesure | Ce qu'elle compte |
|---|---|
| Nb Perturbations Actives | Instantané, tout ce qui est ouvert maintenant, peu importe la date de début |
| Perturbations Aujourd'hui | Uniquement celles dont period_begin = aujourd'hui, actives ou déjà closes |
| Moyenne Journalière | Moyenne du nombre de perturbations distinctes par jour, sur tout l'historique Silver |

Actives > Aujourd'hui est normal et attendu : Actives inclut les perturbations de longue durée démarrées les jours précédents, qu'Aujourd'hui ne voit jamais (filtre strict sur la date de début).
