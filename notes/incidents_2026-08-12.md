# Journal des incidents — 12 août 2026

Session de travail sur le projet SNCF Streaming Pipeline : construction et
validation complète des couches Silver, temps réel et historique (5 jeux
de régularité).

**Bilan de la session** : 2 problèmes techniques rencontrés et résolus,
2 pipelines Silver fonctionnels et vérifiés (temps réel + historique).

---

## 1. Silver historique — cast ANSI sur valeurs vides

| | |
|---|---|
| **Problème** | `historical_silver_transform.py` plante à l'écriture : `SparkNumberFormatException: [CAST_INVALID_INPUT] The value '' of the type "STRING" cannot be cast to "DOUBLE"` |
| **Cause** | Incohérence entre deux parties du code : `_invalid_flag()` traite volontairement une chaîne vide comme une donnée manquante normale (pas une erreur), mais le `.cast(DoubleType())` classique utilisé juste après ne fait pas cette distinction -- Spark 4.x étant en mode ANSI par défaut, un cast invalide lève une exception au lieu de renvoyer `null` silencieusement (comportement des anciennes versions) |
| **Test révélateur** | Traceback complet à l'écriture Delta, avec le message d'erreur citant explicitement la solution : *"Use try_cast to tolerate malformed input and return NULL instead"* |
| **Solution** | Remplacement de `col(c).cast(DoubleType())` par `expr(f"try_cast(\`{c}\` AS DOUBLE)")` -- les vraies valeurs corrompues (non vides mais non numériques) restent isolées dans la reject zone en amont ; `try_cast` ne traite donc plus que le cas légitime des champs vides |
| **Validation** | `2366 lignes Bronze -> 2366 valides (0 rejetées)` -- plus aucune perte, schéma et statistiques (`taux_de_regularite` entre 73.68 et 98.03) cohérents après vérification |

## 2. Silver historique — faute de frappe dans un nom de colonne

| | |
|---|---|
| **Problème** | `historical_silver_transform.py --dataset tgv_liaisons` échoue : `AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column ... 'prct_cause_gestion_traffic' cannot be resolved` |
| **Cause** | Faute de frappe lors de la recopie du schéma réel dans `NUMERIC_COLUMNS` : `gestion_traffic` (deux "f") écrit au lieu de `gestion_trafic` (un seul "f", orthographe réelle de la colonne SNCF) -- erreur introduite par Claude en retranscrivant un schéma pourtant déjà vérifié, jamais recontrôlée caractère par caractère avant usage |
| **Test révélateur** | Message d'erreur Spark auto-suggestif, listant les colonnes réellement disponibles et la plus proche correspondance -- a directement pointé la faute sans débogage supplémentaire nécessaire |
| **Solution** | Correction du nom dans `NUMERIC_COLUMNS["tgv_liaisons"]` |
| **Validation** | `12544 lignes Bronze -> 12544 valides (0 rejetées)` |

---

## Enseignement méthodologique

Les deux incidents de cette session ont une origine différente de ceux de la
veille (11 août) : pas des pièges d'écosystème externe (image Docker
dépréciée, format CSV ambigu), mais des erreurs introduites directement dans
le code généré -- une incohérence de logique (#1) et une simple faute de
frappe (#2). Les deux ont été détectées immédiatement grâce au même réflexe
que la veille : ne jamais accepter un résultat sans vérifier le compte de
lignes et le détail de l'erreur, y compris quand l'erreur vient du code
lui-même plutôt que d'une source externe.
