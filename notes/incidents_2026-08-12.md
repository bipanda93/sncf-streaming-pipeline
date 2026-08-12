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

---

## 3. Gold realtime_alerts — NULL silencieux sur les perturbations NO_SERVICE

| | |
|---|---|
| **Problème** | `nb_affected_stations` valait `NULL` (pas `0`) sur les 12 alertes `critical`/`NO_SERVICE` — précisément les alertes les plus graves |
| **Test révélateur** | Comparaison `avec_stations (118) + sans_stations (0)` ≠ `total (130)` — 12 lignes disparaissaient silencieusement du décompte |
| **Cause** | `impacted_objects` n'est PAS null (confirmé par inspection directe du contenu brut) — c'est `impacted_stops`, à l'intérieur du seul élément du tableau, qui vaut `None`. Logique métier réelle : un train totalement annulé n'a pas d'arrêts avec horaires "retardés" à lister |
| **1re tentative** | `coalesce()` imbriqué à l'intérieur des `transform()` — n'a pas fonctionné (toujours 12 NULL après) |
| **Solution retenue** | `coalesce()` appliqué une seule fois, à la toute fin du calcul (sur `nb_affected_stations`/`affected_stations`), plutôt qu'au milieu d'une expression imbriquée complexe |
| **Validation** | `nb_null: 0`, alertes `critical` affichant `0` (pas `NULL`) avec `affected_stations` vide |

## 4. Gold disruption_context — identifiant de dataset obsolète

| | |
|---|---|
| **Problème** | `curl` sur `referentiel-gares-voyageurs` → `NotFoundResource` |
| **Cause** | Identifiant hérité de l'ancien portail `data.sncf.com` (migré vers `ressources.data.sncf.com`) ; le bon identifiant est `gares-de-voyageurs` |
| **Solution** | Recherche web pour confirmer l'identifiant actuel avant nouvelle tentative |
| **Validation** | `curl` réussi, schéma réel récupéré (`nom`, `codeinsee`, `codes_uic`...) |

## 5. Gold disruption_context — incohérence de nom de colonne dans la jointure

| | |
|---|---|
| **Problème** | `AnalysisException: UNRESOLVED_USING_COLUMN_FOR_JOIN` — `nom_gare_norm` introuvable côté gauche |
| **Cause** | Colonne créée sous le nom `premiere_gare_norm`, jointure tentée sur `nom_gare_norm` — simple incohérence de nommage entre deux lignes du même fichier |
| **Solution** | Renommage de la colonne créée pour correspondre au nom utilisé dans la jointure |
| **Validation** | Script exécuté sans erreur ensuite |

## 6. Gold disruption_context — Delta bloque le changement de schéma

| | |
|---|---|
| **Problème** | `DELTA_METADATA_MISMATCH` à l'écriture — la Phase 2 ajoute 2 colonnes (`region_geographique`, `region_taux_historique_moyen`) par rapport au schéma déjà écrit en Phase 1 |
| **Cause** | Garde-fou intentionnel de Delta Lake : un `overwrite` ne modifie pas le schéma sans autorisation explicite |
| **Solution** | Ajout de `.option("overwriteSchema", "true")` — changement de schéma volontaire, donc autorisé explicitement |
| **Validation** | Écriture réussie |

## 7. Gold disruption_context — désaccords de nommage des gares (plusieurs passes)

| Passe | Taux de correspondance | Correctif appliqué |
|---|---|---|
| 1 | 92/130 (70,8%) | Correspondance exacte seule |
| 2 | 106/130 (81,5%) | Retrait du suffixe "Hall X & Y" + normalisation des tirets-séparateurs (`" - "` → espace) |
| 3 | 110/130 (84,6%) | Table d'alias explicite (`Paris Nord`→`Paris Gare du Nord`, `Valence Ville`→`Valence`) + règle des tirets de liaison (`-sur-`, `-de-`... → espaces) |
| 4 | 116/130 (89,2%) | Géocodage de TOUTES les gares affectées, pas seulement la première |

**Décision assumée** : arrêt de la correction cas par cas au-delà de ce point — les 14 restants sont des gares étrangères (Frankfurt, Milano, Bruxelles) ou hors référentiel ferroviaire français (gares routières), volontairement non corrigées pour éviter une correspondance floue risquant de fusionner deux gares différentes par erreur.

## 8. Gold disruption_context — mélange de régions pré/post réforme territoriale 2016

| | |
|---|---|
| **Problème** | `region_taux_historique_moyen` restait `NULL` sur une ligne pourtant `region_geographique` correctement identifiée (`Provence-Alpes-Côte d'Azur`) |
| **Investigation** | Comparaison de la colonne région du jeu régularité TER avec la liste attendue de 13 régions → **31 valeurs distinctes** trouvées, pas 13 |
| **Cause réelle** | L'historique TER remonte à 2013, avant la réforme territoriale du 01/01/2016 — mélange d'anciennes régions (`Alsace`, `Lorraine`, `Champagne Ardenne`...) et de régions actuelles orthographiées de façon incohérente (`Pays-de-la-Loire` avec tirets vs `Provence Alpes Côte d'Azur` avec espaces), plus 3 noms de réseaux commerciaux locaux (`Etoile Amiens`, `Loire Océan`, `Sud Azur`) qui ne sont pas des régions administratives |
| **Solution** | Construction d'un mapping `SNCF_TO_CURRENT_REGION` consolidant anciennes régions → région fusionnée actuelle (basé sur la réforme officielle) + variantes orthographiques → nom canonique. Exclusion volontaire des 3 noms commerciaux (aucune correspondance officielle vérifiable) |
| **Validation** | 115/130 avec taux résolu (contre 116 régions identifiées) ; le seul écart (Île-de-France) confirmé comme correct — cette région est opérée sous la marque Transilien, absente par nature du jeu régularité TER |

---

## Enseignement méthodologique de la session Gold

Deux incidents (le 1 et le 8) partagent un point commun révélateur : **un résultat "presque bon" est plus dangereux qu'un résultat clairement cassé**, parce qu'il invite à ne pas creuser. `nb_affected_stations` à 118/130 semblait presque complet ; une région identifiée avec un taux `NULL` semblait n'être qu'un détail. Dans les deux cas, s'arrêter là aurait laissé une vraie faille de données invisible en production. La discipline appliquée tout au long de cette session — comparer systématiquement deux totaux qui devraient concorder, plutôt que de se satisfaire d'un chiffre qui « a l'air correct » — a permis de détecter les deux.
