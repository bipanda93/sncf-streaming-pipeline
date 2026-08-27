# Journal des incidents — 27 août 2026

## 1. Fabric rejette les fichiers Parquet — précision nanoseconde des timestamps

| | |
|---|---|
| **Contexte** | Rechargement des 5 fichiers Parquet fraîchement exportés dans Fabric (Files → Load to Tables) |
| **Symptôme** | `Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))`, code `InvalidTable` |
| **Cause racine** | Les versions récentes de PyArrow écrivent par défaut au format Parquet 2.6, qui préserve nativement la précision nanoseconde des timestamps -- contrairement à un comportement plus ancien qui les ramenait automatiquement en microseconde. Delta Lake (moteur des tables Fabric) n'accepte que la précision microseconde |
| **Correction** | Ajout explicite de `coerce_timestamps='us', allow_truncated_timestamps=True` sur tous les appels `to_parquet()` dans `export_for_powerbi.py` |
| **Vérification préventive** | Fichiers inspectés directement (schéma PyArrow) avant un nouveau passage par Fabric -- confirmé `timestamp[us]` sur toutes les colonnes concernées, évitant un deuxième aller-retour raté |
| **Résultat** | Les 5 tables chargées avec succès, aucune erreur |

## 2. Fausse alerte -- relation mal interprétée sur le diagramme Fabric

En vérifiant le modèle sémantique, une ligne de relation a été interprétée à tort comme reliant directement `gold_disruption_by_region` à `gold_disruption_by_station` (ce qui aurait été une erreur de modélisation). Vérification via "Edit relationship" : la relation était en réalité correcte (`gold_disruption_by_region` → `gold_disruption_context`, many-to-one sur `disruption_id`) -- la ligne du diagramme passait simplement visuellement à proximité de l'autre table. Aucun bug réel, juste un aller-retour évitable. **Enseignement** : vérifier les relations via la liste/le dialogue d'édition plutôt que de tracer les lignes à l'œil sur un canevas chargé.

## 3. Process -- les mesures DAX doivent être créées une par une

Tentative de coller les 6 mesures DAX prévues d'un coup dans une seule barre de formule "New measure" -- erreur de syntaxe, l'éditeur n'accepte qu'une définition de mesure par validation. Corrigé en répétant "New measure" → coller une seule mesure → valider, pour chacune des 6.
