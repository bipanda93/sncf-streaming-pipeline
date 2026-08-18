# Journal du Projet SNCF Streaming Pipeline — Historique des décisions

## Informations générales
- **Projet** : SNCF Streaming Pipeline (mémoire académique, soutenance déc. 2026)
- **Auteur** : Bipanda Franck Ulrich
- **Formation** : Mastère Data Engineering — Digital School de Paris
- **Architecture** : Azure-native, multisource (temps réel + historique)

---

## ARCHITECTURE — DÉCISIONS MAJEURES

| # | Décision | Raison |
|---|---|---|
| 1 | Azure plutôt qu'AWS pour l'ensemble du projet | Azure légèrement dominant chez les grands comptes français (50+ salariés) selon Elitek (2026), cohérent avec les employeurs ciblés |
| 2 | GitHub Actions plutôt que Jenkins pour le CI/CD | Adoption 2026 : 33% GitHub Actions vs 28% Jenkins en France ; Jenkins conservé sur le Projet 1 pour diversifier le portfolio |
| 3 | Architecture multisource dès le départ | Flux temps réel seul ne permet pas d'analyse de tendances avant plusieurs mois d'exploitation ; ajout d'une source batch historique (régularité SNCF, 2013→aujourd'hui) |
| 4 | Docker Compose en développement, AKS en cible production | Benchmark chiffré : Docker Compose gagne en dev (coût nul, déjà éprouvé) mais s'effondre en cible production (pas d'auto-scaling) |
| 5 | Image Kafka officielle `apache/kafka` plutôt que Bitnami | Bitnami a fermé l'accès gratuit aux images versionnées depuis août 2025 (`bitnami/kafka:3.7` introuvable) |
| 6 | Spark/Delta Lake 4.1.0 | Version alignée avec Java 17 déjà installé ; dernière version stable au moment du projet |

---

## INGESTION TEMPS RÉEL — VALIDATIONS

| # | Test | Résultat |
|---|---|---|
| 7 | `search_places("Gare de Lyon")` avec vraie clé API | ✅ Authentification confirmée, vrai `stop_area_id` obtenu |
| 8 | `get_disruptions()` — première tentative | ⚠️ Seulement 25/2591 perturbations reçues (pagination silencieuse non gérée) |
| 9 | Correctif pagination : `count=1000` + filtre `since`/`until` sur la journée | ✅ 599 perturbations reçues en un seul appel, sous la limite de 1000 |
| 10 | `main.py --once` → Kafka (`sncf-raw`) | ✅ Pipeline bout en bout fonctionnel, vérifié visuellement sur Kafka UI |
| 11 | `bronze_ingestion.py` (Spark Structured Streaming) → Delta Bronze | ✅ 629 lignes en Bronze, identique au total Kafka cumulé, schéma conforme |

**Point de vigilance documenté** : les 629 lignes contiennent des doublons (perturbations vues à plusieurs cycles d'ingestion) — dédoublonnage prévu en couche Silver, pas en Bronze (principe : Bronze = brut, jamais transformé).

---

## SOURCE HISTORIQUE — EN COURS

| # | Test | Résultat |
|---|---|---|
| 12 | `curl -I` sur l'export CSV régularité TER (data.sncf.com) | ✅ HTTP 200, aucune authentification requise, quota 150 000 requêtes/jour |
| 13 | `curl` schéma réel du CSV TER | ✅ 9 colonnes confirmées : `date;region;nombre_de_trains_programmes;...;taux_de_regularite;...` |
| 14 | `RegularityClient` mode mock | ✅ CSV synthétique conforme au schéma réel |
| 15 | `RegularityClient` mode réel — téléchargement complet | ⏳ En attente du résultat |

---

## PROCHAINES ÉTAPES (mise à jour 12/08/2026, fin de session Gold)

### Terminé
- [x] Ingestion temps réel (SNCF API -> Kafka) + Bronze temps réel
- [x] Ingestion historique (5 jeux régularité) + Bronze historique
- [x] Silver temps réel (dédoublonnage) + Silver historique (typage, 5 jeux)
- [x] Gold realtime_alerts (alertes actives, niveaux de sévérité)
- [x] Gold punctuality_trends (tendances historiques unifiées, 5 sources)
- [x] Gold disruption_context (jointure temporelle + géographique)

### À faire
- [ ] Enrichissement LLM (`src/enrichment/`) — résumés Claude API des perturbations, publication sur `sncf-llm`
- [ ] Orchestration Airflow (`dags/`) — 3 DAGs encore en squelette
- [ ] Power BI — connexion DirectQuery vers les 3 tables Gold
- [ ] Monitoring (`monitoring/`) — Prometheus/Grafana ou Azure Monitor
- [ ] Infrastructure Terraform (`infra/terraform/`) — tous les .tf encore vides
- [ ] Migration Kafka local -> Event Hubs (déjà prévue dans le code, jamais testée en réel)
- [ ] `deploy.yml` (CI/CD déploiement) — seul `ci.yml` (lint/test) est fonctionnel aujourd'hui

---

## SILVER TEMPS RÉEL — DÉCISIONS ET VALIDATION

| # | Décision / Test | Détail |
|---|---|---|
| 22 | Extraction JSON : `from_json` avec schéma explicite (plutôt que `get_json_object` champ par champ) | Plus robuste, une seule passe de parsing, schéma Spark structuré reflétant la structure réelle observée dans les données SNCF (severity, messages, impacted_objects imbriqués) |
| 23 | Statuts conservés : `active`, `past` ET `future` (aucun filtre en Silver) | La profondeur historique est déjà couverte par la source régularité ; laisser Gold/Power BI décider du filtrage selon l'usage |
| 24 | Validation a posteriori de la décision #23 | Découverte d'un 3e statut (`future`, perturbations planifiées) jamais anticipé — répartition réelle : 74 future / 415 past / 130 active sur 619 lignes. Filtrer sur `active` seul en Silver aurait fait perdre silencieusement 79% des données |
| 25 | Dédoublonnage validé | 629 lignes Bronze -> 619 perturbations distinctes, 0 doublon restant confirmé (comptage `disruption_id` distincts = lignes totales) |

---

## SILVER HISTORIQUE — DÉCISIONS ET VALIDATION

| # | Décision / Test | Détail |
|---|---|---|
| 26 | Vérification préalable des schémas des 5 jeux, avant tout code | Confirme que les 5 jeux N'ONT PAS le même schéma (ex. `tgv_globale` = 3 colonnes sans notion de région ; `tgv_liaisons` = 26 colonnes avec détail des causes en %). Colonnes numériques définies explicitement par jeu (`NUMERIC_COLUMNS`), pas de détection automatique -- trop risqué vu l'hétérogénéité réelle |
| 27 | Outil : Spark (pas dbt) pour le Silver historique | Voir section "DÉCISION — Outil de transformation Silver pour la source historique" ci-dessus pour le raisonnement complet (risque de propagation d'erreur écarté : l'isolation vient de la séparation des jobs, pas du choix d'outil) |
| 28 | Validation complète sur les 5 jeux | TER 2366→2366 · Intercités 5949→5949 · TGV globale 138→138 · TGV axes 817→817 · TGV liaisons 12544→12544 — **21 814 lignes au total, 0 rejet** |

---

## GOLD — JOINTURE GÉOGRAPHIQUE COMPLÈTE (toutes gares + consolidation régionale)

**Évolution demandée** : geocoder TOUTES les gares affectées par une perturbation (pas seulement la première) -- permet de détecter qu'une perturbation touche plusieurs régions. Résultat : 51/130 perturbations touchent 2 régions ou plus (jusqu'à 4), une réalité complètement invisible avec l'approche "première gare seulement".

**Découverte majeure lors de la validation** : le jeu régularité TER mélange des noms de région d'AVANT et d'APRÈS la réforme territoriale du 01/01/2016 (l'historique remonte à 2013), plus 3 noms de réseaux commerciaux locaux sans statut de région (Etoile Amiens, Loire Océan, Sud Azur). 31 valeurs distinctes trouvées pour ce qui devrait être 13 régions actuelles.

**Décision** : construction d'un mapping `SNCF_TO_CURRENT_REGION` consolidant :
- Les anciennes régions pré-2016 vers leur région fusionnée actuelle (mapping basé sur la réforme officielle : ex. Alsace + Champagne-Ardenne + Lorraine -> Grand Est)
- Les variantes orthographiques des régions actuelles (SNCF n'utilise aucune convention de ponctuation cohérente : `Pays-de-la-Loire` mais `Provence Alpes Côte d'Azur`)
- Exclusion volontaire des 3 noms commerciaux (Etoile Amiens, Loire Océan, Sud Azur) -- leur rattacher une région précise aurait été une supposition géographique, pas une correspondance officielle vérifiable

**Résultat final** : 116/130 alertes avec au moins une région identifiée (89,2%), 115/130 avec un taux historique résolu. Le seul écart restant (Île-de-France) est un comportement correct, pas un bug : l'Île-de-France est opérée sous la marque Transilien, pas TER, donc absente par nature du jeu régularité TER.

**Enseignement méthodologique** : une hypothèse de départ raisonnable ("les régions SNCF sont stables depuis 2016") s'est révélée incomplète face aux vraies données -- l'historique remonte avant la réforme territoriale, invalidant l'hypothèse de stabilité. Découvert uniquement parce qu'un résultat "presque correct" (une région identifiée mais un taux NULL) a été creusé plutôt qu'accepté tel quel.

---

## DÉCISION — Fréquence et rôle de l'enrichissement LLM

**Question de départ** : estimation du coût de l'enrichissement LLM (Claude API)
sur les perturbations temps réel.

**Estimation réalisée** (modèle Haiku 4.5, 1$/5$ par million de tokens
entrée/sortie -- tarif vérifié en direct) : sur la base de 599 perturbations/jour
(volume réel mesuré via l'API SNCF), ~400 tokens entrée + ~150 tokens sortie par
perturbation -> environ 0,70$/jour, ~20$/mois en fonctionnement quotidien continu.
Coût jugé faible pour un projet académique -- ce n'était donc pas la vraie
contrainte derrière la demande de réduire la fréquence.

**Décision retenue** : l'enrichissement LLM devient un job MENSUEL, orchestré par
un DAG Airflow dédié, séparé des DAGs d'ingestion (qui restent sur leur cadence
actuelle, quasi continue, pour préserver le vrai suivi temps réel du pipeline).
Airflow permet nativement des fréquences différentes entre DAGs -- aucune
contrainte technique à ce choix.

**Tension identifiée et assumée explicitement** : à cette fréquence,
l'enrichissement devient RÉTROSPECTIF, pas un outil d'alerte en direct -- une
perturbation SNCF dure typiquement quelques heures, largement résolue avant le
prochain cycle mensuel. Point important : ce n'est PAS un problème pour les
alertes temps réel elles-mêmes, puisque `gold_realtime_alerts` calcule déjà le
niveau de sévérité et la cause sans dépendre du LLM -- l'enrichissement mensuel
s'ajoute comme une couche de reporting complémentaire ("voici ce qui s'est passé
ce mois-ci et pourquoi"), pas comme un prérequis à l'alerting opérationnel.

**Question laissée ouverte** (à trancher à la reprise) : le rapport mensuel
prend-il la forme d'un résumé par perturbation (une phrase générée par incident
du mois), ou d'un seul récit consolidé (une synthèse globale du mois en un seul
texte) ? Impact direct sur l'architecture de `claude_client.py` et
`delay_analyzer.py` (nombreux petits appels API vs un seul appel plus riche).

---

## DÉCISION — Ajout d'une "profondeur IA" : périmètre, algorithme et orchestration

**Contexte** : réflexion sur l'ajout d'un volet IA plus poussé au projet, au-delà de
l'enrichissement LLM déjà en place, pour renforcer le mémoire.

### Chantiers envisagés et écartés

**RAG (Retrieval-Augmented Generation)** sur les résumés LLM mensuels — écarté pour
l'instant, pas par manque de valeur mais par manque de nécessité actuelle : 545
résumés/mois représentent un corpus trop petit (~6 500 phrases/an) pour justifier une
architecture de retrieval — un simple filtre SQL + prompt classique ferait le même
travail, plus simplement. Noté comme chantier bonus si le temps le permet après les
priorités du calendrier (Airflow, Power BI, Terraform). Architecture retenue si repris
un jour : Databricks AI Search (anciennement Vector Search, renommé récemment) plutôt
qu'un vector store externe -- reste dans l'écosystème Databricks déjà en place, et se
gouverne nativement via Unity Catalog.

**Terraform / Power BI** — écartés : aucun angle IA naturel pour le premier (pur IaC) ;
Power BI a son propre Copilot intégré, peu démonstratif d'une compétence propre.

### Chantier retenu : détection automatique d'anomalies (gouvernance des données)

Motivé par l'expérience réelle du projet : 16 incidents rencontrés cette session, la
plupart silencieux (aucune exception levée, juste une donnée incomplète ou incorrecte
-- ex. pagination à 25 lignes sur 2 591 réelles, NULL caché sur les alertes les plus
critiques). Un détecteur d'anomalies en amont de Gold aurait pu signaler plusieurs de
ces écarts automatiquement, avant qu'ils ne se propagent.

**Conception à deux niveaux** :
- Niveau 1 (seuils statistiques) : nouvelle table `gold_pipeline_metrics` (une ligne
  par run -- row_count, taux de nullité, durée d'exécution...), comparée à la
  distribution historique du même job (Statistical Process Control, technique utilisée
  par les outils pro du domaine comme Great Expectations, Monte Carlo)
- Niveau 2 (ML) : Isolation Forest, capable de détecter une combinaison anormale de
  métriques même quand aucune ne dépasse individuellement un seuil

### Comparaison d'algorithmes pour le Niveau 2

| Algorithme | Peu de données | Interprétabilité | Adoption prod | Implémentation | Dimensionnalité | Score /100 |
|---|---|---|---|---|---|---|
| **Isolation Forest** | 4 | 4 | 5 | 5 | 4 | **88** |
| Distance de Mahalanobis | 3 | 5 | 3 | 5 | 2 | 72 |
| One-Class SVM | 3 | 2 | 3 | 3 | 3 | 56 |
| Local Outlier Factor | 2 | 3 | 2 | 4 | 2 | 52 |

**Isolation Forest retenu** : ne suppose aucune distribution particulière (contrairement
à Mahalanobis, qui suppose une forme ~gaussienne), robuste avec un historique encore
modeste, standard de facto pour ce cas d'usage en production.

### Contrainte critique découverte : volume de données d'entraînement

Un modèle comme Isolation Forest nécessite ~50-100 exécutions historiques minimum pour
produire des scores statistiquement fiables. Or les deux sources du projet n'accumulent
pas d'historique au même rythme :

| Source | Fréquence | Temps pour ~50 échantillons |
|---|---|---|
| Ingestion temps réel | Continue | Quelques jours à quelques semaines selon la fréquence du DAG |
| Historique + LLM mensuel | Une fois par mois | ~4 ans -- hors de portée du calendrier du mémoire |

**Conséquence assumée** : présenter un Isolation Forest entraîné sur le flux mensuel
aurait été du *ML-washing* -- une façade IA sans fondement statistique réel, risquant
d'être détectée par un jury un peu pointu.

### Décision finale

- **Flux temps réel** → Isolation Forest, entraîné progressivement une fois qu'Airflow
  tourne depuis plusieurs semaines. Implication directe sur la conception du DAG
  d'ingestion : choisir une fréquence laissant une vraie marge d'ici la soutenance de
  décembre (ex. toutes les 4h -> ~540 runs sur 3 mois, confortable ; quotidien -> ~90
  runs, marge faible).
- **Historique/mensuel** → seuils statistiques (Niveau 1) uniquement, explicitement
  non-ML faute de volume suffisant dans la durée du projet -- limite assumée et
  documentée plutôt que dissimulée, dans le prolongement de la posture déjà adoptée sur
  les biais de confirmation du benchmarking (Partie 1 du mémoire).
- Présentation recommandée en soutenance : "un modèle entraîné sur les données
  réellement disponibles à ce stade du projet, avec ses limites de volume assumées" --
  pas un système mature en production.

### Clarification d'orchestration

Confusion initiale entre trois couches à ne pas mélanger :
- **CI/CD (GitHub Actions, pas Jenkins ici)** : build/test/déploiement de code, ne
  concerne pas l'exécution du modèle
- **AKS (Kubernetes)** : infrastructure de calcul, exécute simplement le code -- pas un
  orchestrateur de pipeline
- **Airflow** : le bon outil pour ce besoin -- décide quand le job de détection
  d'anomalies tourne et dans quel ordre (après le DAG d'ingestion temps réel)

Architecture retenue : DAG "ingestion_temps_reel" -> déclenche DAG
"detection_anomalies" (entraînement/mise à jour du modèle, score du run le plus récent,
alerte via claude_client.py si anomalie détectée). MLflow (nativement intégré à
Databricks, déjà mentionné dans l'architecture d'origine) comme outil naturel de suivi
des versions successives du modèle, sans introduire de nouvelle brique technique.

---

## AIRFLOW — ORCHESTRATION COMPLÈTE

**Installation** : tentative initiale via `pip install apache-airflow` directement sur la machine locale -- a déclenché une cascade de compilations depuis les sources (Rust, puis OpenSSL, cmake, jusqu'à LLVM/Clang complets) à cause d'un compilateur Rust absent et d'une toolchain Xcode désynchronisée. Interrompu avant que la compilation LLVM (potentiellement plusieurs heures) ne se termine. Pivot vers Docker -- même stratégie que pour Kafka (Bitnami) plus tôt dans le projet : isoler l'installation plutôt que de déboguer l'environnement local indéfiniment. Plus proche de la vraie pratique en entreprise (Airflow tourne presque toujours en conteneur en production).

**Version** : Airflow 3.3.1 -- changement d'architecture majeur par rapport à la v2 (dernière connue) : `schedule_interval` -> `schedule`, `PythonOperator`/`BashOperator` déplacés dans le package séparé `apache-airflow-providers-standard`, `SequentialExecutor` supprimé (`LocalExecutor` fonctionne désormais avec SQLite pour du dev local), webserver renommé `api-server`.

**Architecture retenue** : un seul service Docker (`airflow standalone`, scheduler + api-server + base SQLite en un seul processus) plutôt qu'une configuration multi-services (Postgres + scheduler + webserver + triggerer séparés) -- suffisant pour du développement local, évite la complexité d'une configuration de production non nécessaire à ce stade.

**Réseau** : les tâches Airflow tournant dans un conteneur ne peuvent pas joindre Kafka via `localhost:9094` (valable uniquement depuis la machine hôte) -- nécessite `kafka:9092` (adresse interne au réseau Docker), configuré via variable d'environnement du service `airflow` dans `docker-compose.yml`.

**Persistance des données** : `/tmp/delta` monté en volume identique entre l'hôte et le conteneur -- les jobs Spark exécutés par Airflow écrivent dans le même Delta Lake que les runs manuels précédents (vérifié : 629 + 879 = 1508 lignes Bronze après le premier run orchestré).

### Découverte architecturale importante (signalée par l'utilisateur, pas anticipée)

Le premier DAG écrit (`silver_gold_temps_reel`) ne couvrait que Silver -> Gold, laissant l'ingestion Bronze temps réel (`main.py`, `bronze_ingestion.py`) en dehors de toute orchestration -- ces scripts n'auraient continué de tourner que si lancés manuellement, contredisant l'objectif de pipeline autonome. Corrigé par l'ajout d'un DAG dédié (`ingestion_bronze_temps_reel`), réutilisant le mode `--once` déjà présent dans les deux scripts depuis le début du projet mais jamais pleinement exploité.

### Les 4 DAGs planifiés, testés en conditions réelles

| DAG | Fréquence | Résultat du test réel |
|---|---|---|
| `ingestion_bronze_temps_reel` | Toutes les heures | 879 perturbations publiées, Bronze 629 -> 1508 |
| `silver_gold_temps_reel` | Toutes les 4h | Silver 1508 -> 1498 distinctes ; 250 alertes actives ; 226/250 régions identifiées |
| `historique_mensuel` | 1er du mois, 3h | 11 tâches (5 jeux x load+silver + 1 gold), 21814 lignes -- identique au run manuel (CSV inchangé depuis) |
| `enrichissement_llm_mensuel` | 1er du mois, 4h | Testé sur juillet 2026 (mois vide) -- gère proprement l'absence de données, `state=success` |

### 5e DAG utilitaire -- pipeline_complet_manuel

Besoin identifié : pouvoir déclencher les 4 DAGs planifiés d'un coup, à la demande (démonstration soutenance, test de bout en bout), sans dupliquer ni fusionner leurs plannings automatiques respectifs -- chaque fréquence reste justifiée par une vraie contrainte (volume Isolation Forest, coût LLM, cadence de publication SNCF), les fusionner aurait réintroduit les problèmes qu'on avait précisément écartés (ex. LLM re-déclenché à chaque run horaire).

Solution : DAG séparé (`pipeline_complet_manuel`), `schedule=None` -- ne se déclenche jamais automatiquement, uniquement via `airflow dags trigger`. Utilise `TriggerDagRunOperator` (déjà anticipé dans le document d'architecture d'origine du projet). Point de vigilance appliqué de façon préventive : `deferrable=False` forcé explicitement sur chaque tâche -- un bug documenté d'Airflow 3.x fait que `wait_for_completion=True` combiné à `deferrable=True` peut laisser une tâche bloquée indéfiniment en état "deferred", sans jamais échouer ni réussir. Découvert par recherche avant implémentation, jamais rencontré en pratique.

### Restant

`gouvernance_rgpd` (6e DAG prévu, rétention et purge des données personnelles) -- non commencé, nécessite une réflexion sur la politique de rétention avant d'être codé.

**Mise à jour** : un secret réel (`ARM_CLIENT_SECRET` du Service Principal) a
été committé par erreur peu après la rédaction de la section ci-dessus, à
cause du même type de bug de chemin relatif que celui déjà rencontré avec `az`
dans ce même module. Révoqué immédiatement, commit fautif annulé (jamais
poussé vers un remote), `.gitignore` corrigé. Détail complet dans
notes/incidents_2026-08-18.md, incident n°6.
