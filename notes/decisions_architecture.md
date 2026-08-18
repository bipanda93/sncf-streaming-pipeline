# Journal des décisions d'architecture — Projet SNCF Streaming Pipeline

Ce document répertorie les choix techniques structurants du projet, avec leur
contexte, les alternatives écartées, et la justification retenue. Contrairement
à `journal_projet.md` (chronologique, mélange décisions/tests/incidents), ce
fichier n'indexe que les décisions elles-mêmes -- pensé pour être consulté
rapidement, ou repris directement dans le mémoire.

---

## Infrastructure & cloud

### Azure plutôt qu'AWS
**Contexte** : choix du cloud cible dès la conception initiale du projet.
**Alternative écartée** : AWS.
**Décision** : Azure.
**Justification** : alignement avec le marché français des grands comptes et
ESN ciblés par la recherche d'emploi (critère pondéré à 25% dans le
benchmarking de la Partie 1 du mémoire).

### GitHub Actions plutôt que Jenkins (pour ce projet)
**Contexte** : choix de l'outil CI/CD.
**Alternative écartée** : Jenkins (utilisé sur un autre projet du portfolio,
volontairement, pour diversifier les compétences démontrées).
**Décision** : GitHub Actions pour SNCF.
**Justification** : adoption 2026 mesurée à 33% contre 28% pour Jenkins en
France ; le lint/test (`ci.yml`) est fonctionnel, le déploiement
(`deploy.yml`) reste à finaliser.

### Apache Kafka officiel plutôt que Bitnami
**Contexte** : choix de l'image Docker Kafka pour le développement local.
**Alternative écartée** : image Bitnami (utilisée initialement).
**Décision** : `apache/kafka:3.7.0`.
**Justification** : Bitnami a fermé l'accès gratuit à ses images versionnées
depuis août 2025 -- image officielle plus pérenne pour un projet destiné à
être repris/démontré sur plusieurs mois.

### Installation Airflow via Docker plutôt que pip local
**Contexte** : mise en place de l'orchestration.
**Alternative écartée** : `pip install apache-airflow` en local.
**Décision** : conteneur Docker dédié (`Dockerfile.airflow`).
**Justification** : l'installation locale a déclenché une cascade de
compilations depuis les sources (Rust, puis LLVM/Clang complet, potentiellement
plusieurs heures) à cause d'une toolchain système désynchronisée. Docker isole
le problème -- même stratégie déjà appliquée pour Kafka/Bitnami. Plus proche de
la vraie pratique en entreprise (Airflow tourne presque toujours en conteneur
en production).

### Airflow standalone plutôt qu'une configuration multi-services
**Contexte** : architecture du service Airflow dans `docker-compose.yml`.
**Alternative écartée** : services séparés (Postgres + scheduler + api-server +
triggerer + dag-processor), plus proche d'un déploiement de production.
**Décision** : un seul service, commande `airflow standalone` (scheduler +
api-server + base SQLite en un seul processus).
**Justification** : suffisant pour du développement local ; la complexité
d'une configuration multi-services de production n'apporte rien à ce stade et
retarderait la mise en place réelle des DAGs. Le déploiement Azure final
utilisera de toute façon un Airflow managé (hors Docker Compose).

---

## Ingestion & traitement des données

### Architecture multisource dès la conception
**Contexte** : conception initiale du pipeline.
**Décision** : deux flux distincts (API temps réel + 5 jeux historiques),
chacun avec son propre chemin Bronze/Silver, convergeant en Gold.
**Justification** : un flux temps réel seul ne permet aucune analyse de
tendance ni de comparaison au contexte historique -- l'objectif du mémoire
(détection ET compréhension des causes de retard) exige les deux.

### Spark pour tout le pipeline SNCF plutôt que dbt
**Contexte** : choix de l'outil de transformation pour Silver/Gold, y compris
la partie historique (batch, tabulaire) qui aurait pu être un candidat naturel
pour dbt.
**Alternative écartée** : dbt pour la partie historique (Spark pour le reste).
**Décision** : Spark pour l'intégralité du projet SNCF.
**Justification** : (1) dbt ne gère pas nativement le streaming, contrairement
à Spark Structured Streaming, utilisé pour le flux temps réel ; (2) cohérence
d'un seul moteur sur tout le projet, plus simple à défendre en soutenance ;
(3) le risque de propagation d'erreur évoqué initialement ne dépend pas du
choix d'outil mais de la séparation des jobs (déjà garantie par construction).
dbt reste démontré ailleurs dans le portfolio (Leclerc, Olist) -- diversité au
niveau du portfolio global, pas nécessairement à l'intérieur d'un même projet.

### Statuts de perturbation conservés en Silver, filtrés en Gold
**Contexte** : la table Silver temps réel contient des perturbations
`active`, `past` et `future`.
**Alternative écartée** : filtrer sur `active` uniquement dès Silver.
**Décision** : conserver les trois statuts en Silver ; laisser Gold filtrer
selon le besoin métier.
**Justification** : validé a posteriori -- filtrer trop tôt aurait fait perdre
silencieusement 79% des données (répartition réelle observée : 74 future / 415
past / 130 active sur 619 lignes), y compris pour des usages futurs
(tendances, planification) que Silver n'a pas à anticiper.

---

## Modélisation Gold

### Une table Gold = une question métier
**Contexte** : conception des tables Gold.
**Alternative écartée** : une table Gold unique regroupant toutes les
métriques.
**Décision** : trois tables séparées (`realtime_alerts`,
`punctuality_trends`, `disruption_context`), chacune répondant à une question
métier précise.
**Justification** : principe vérifié par recherche externe avant d'écrire le
code -- pratique standard en architecture medallion de production ("une
table, une définition, un cas d'usage" ; frontière Silver/Gold définie par
"cette transformation nécessite-t-elle une connaissance métier ?").

### Jointure géographique consolidée pré/post réforme territoriale 2016
**Contexte** : `gold_disruption_context` joint les perturbations à des
régions historiques.
**Découverte** : le jeu régularité TER mélange des noms de région d'avant et
d'après la réforme territoriale de 2016 (l'historique remonte à 2013), plus
des noms de réseaux commerciaux qui ne sont pas des régions.
**Décision** : table de consolidation explicite (anciennes régions -> région
fusionnée actuelle, basée sur la réforme officielle) ; exclusion volontaire
des noms commerciaux (pas de correspondance officielle vérifiable).
**Justification** : rejeté la correspondance floue (Levenshtein etc.) au
profit d'une liste explicite -- un faux positif (deux régions fusionnées par
erreur) serait pire qu'un faux négatif assumé et documenté.

---

## IA & enrichissement

### Enrichissement LLM mensuel et rétrospectif, pas continu
**Contexte** : rôle de Claude API dans le pipeline.
**Alternative écartée** : enrichissement à chaque perturbation, en continu.
**Décision** : job mensuel, indépendant des alertes temps réel (qui restent
fonctionnelles sans lui).
**Justification** : le coût réel (~0,70$/jour en continu) n'était pas le
facteur limitant -- le choix reflète le rôle voulu (récapitulatif rétrospectif
pour du reporting, pas un outil d'alerte en direct, qui nécessiterait une
fraîcheur incompatible avec un cycle mensuel).

### Isolation Forest pour la détection d'anomalies (flux temps réel)
**Contexte** : choix d'algorithme pour la détection automatique d'anomalies
sur les métriques de pipeline.
**Alternatives comparées** : distance de Mahalanobis, One-Class SVM, Local
Outlier Factor (grille à 5 critères).
**Décision** : Isolation Forest (score 88/100), Mahalanobis en 2e position
(72/100).
**Justification** : ne suppose aucune distribution particulière des données
(contrairement à Mahalanobis, qui suppose une forme ~gaussienne) ; robuste
avec un historique de runs encore modeste ; standard de facto en production
pour ce cas d'usage.

### Détection d'anomalies à deux vitesses selon la source
**Contexte** : les deux sources de données n'accumulent pas d'historique de
runs au même rythme (temps réel : continu ; historique/LLM : mensuel).
**Décision** : Isolation Forest pour le flux temps réel (une fois ~50-100
runs accumulés) ; seuils statistiques uniquement pour le flux mensuel.
**Justification** : un modèle ML entraîné sur un flux mensuel n'atteindrait le
volume minimum nécessaire (~50-100 échantillons) qu'après environ 4 ans --
hors de portée du calendrier du mémoire. Présenter un tel modèle comme "IA"
aurait été un habillage sans fondement statistique réel (ML-washing).

### RAG écarté pour l'instant
**Contexte** : réflexion sur l'ajout d'une "profondeur IA" supplémentaire.
**Décision** : non retenu dans le périmètre actuel.
**Justification** : le corpus disponible (résumés LLM mensuels) est encore
trop petit (~6500 phrases/an) pour justifier une architecture de retrieval --
un simple filtre SQL + prompt classique suffit. Si repris plus tard,
Databricks AI Search (natif à l'écosystème déjà en place) serait le choix
architectural cohérent plutôt qu'un vector store externe.

### Pas de revendication "Data Mesh"
**Contexte** : question sur l'application du concept de Data Mesh au projet.
**Décision** : ne pas revendiquer une architecture Data Mesh complète.
**Justification** : le Data Mesh est fondamentalement organisationnel
(propriété décentralisée par équipes, gouvernance fédérée) -- incompatible
avec un projet à un seul contributeur. Seul le principe "data as a product"
est authentiquement appliqué (via la conception des tables Gold), sans en
tirer une revendication plus large qui affaiblirait la crédibilité en
soutenance face à un jury connaissant le concept.

---

## Orchestration Airflow

### 4 DAGs séparés plutôt qu'un DAG unique
**Contexte** : orchestration du pipeline complet (ingestion, transformation,
historique, enrichissement).
**Alternative écartée** : un DAG unique déclenchant tout le pipeline sur un
seul planning.
**Décision** : 4 DAGs indépendants, chacun avec sa propre fréquence
(`ingestion_bronze_temps_reel` horaire, `silver_gold_temps_reel` toutes les
4h, `historique_mensuel` et `enrichissement_llm_mensuel` mensuels).
**Justification** : chaque fréquence répond à une contrainte réelle et
différente (fraîcheur des alertes, marge d'entraînement Isolation Forest,
cadence de publication SNCF, maîtrise du coût LLM). Les fusionner en un DAG
unique aurait forcé tous les traitements sur le même rythme -- ex.
réenrichissement LLM 24 fois par jour au lieu d'une fois par mois,
contredisant une décision prise et documentée séparément.

### DAG utilitaire de déclenchement manuel (5e DAG)
**Contexte** : besoin identifié de pouvoir relancer les 4 DAGs d'un coup, à la
demande (démonstration soutenance, test de bout en bout), sans modifier leurs
plannings automatiques respectifs.
**Décision** : `pipeline_complet_manuel`, `schedule=None` (jamais de
déclenchement automatique), utilisant `TriggerDagRunOperator` pour enchaîner
les 4 DAGs en séquence sur demande explicite.
**Justification** : répond au besoin ponctuel sans dupliquer ni contredire la
logique de fréquences déjà justifiée -- les deux mécanismes coexistent sans
conflit, chacun répondant à une question différente ("quand le pipeline
tourne-t-il en production" vs "comment tout relancer à la main quand j'en ai
besoin"). Point de vigilance appliqué : `deferrable=False` forcé
explicitement sur chaque tâche pour éviter un bug documenté d'Airflow 3.x
(blocage indéfini en état "deferred" avec `wait_for_completion=True`).
