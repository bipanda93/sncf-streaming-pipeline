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

---

## MONITORING — Prometheus/Grafana vs Azure Monitor, décision affinée

**Point de départ** : le plan d'architecture d'origine mentionnait
"Prometheus/Grafana ou Azure Monitor" pour le monitoring -- jamais tranché,
jamais construit. Redécouvert tardivement dans le projet, après que
l'infrastructure Azure native (AKS, Databricks, Event Hubs) ait déjà été
largement engagée.

**Première comparaison, sur le seul critère du coût** :
- Azure Monitor : 5 Go d'ingestion gratuits/mois, puis 2,30$/Go (logs
  Analytics) -- pour l'échelle de ce projet (un seul nœud AKS léger, peu de
  ressources), le volume réel resterait probablement dans les 5 Go gratuits
  ou proche.
- Prometheus/Grafana auto-hébergé : logiciel gratuit, mais nécessiterait un
  nœud/pool AKS supplémentaire pour tourner -- risque concret de rouvrir le
  problème de quota vCPU rencontré lors du premier apply réel (3 familles de
  VM refusées avant de trouver Standard_D2s_v6, voir
  notes/incidents_2026-08-19.md).

**Conclusion initiale** : Azure Monitor, pour éviter ce risque de quota.

**Approfondissement demandé** : recherche spécifique sur "Prometheus hébergé
sur Azure" -- révèle l'existence d'Azure Monitor managed service for
Prometheus, une option intermédiaire non considérée initialement.

**Ce que ce service change** : facturation basée sur l'ingestion/requête des
données (comme Azure Monitor classique), PAS sur l'hébergement -- élimine
entièrement le risque de quota AKS, puisqu'aucun nœud dédié n'est nécessaire
pour faire tourner Prometheus lui-même. Support complet de PromQL, intégration
native avec Azure Managed Grafana (service Grafana managé, pas
auto-hébergé), rétention 18 mois sans coût de stockage additionnel.

**Point de vigilance identifié pendant la recherche** : un chiffre de
~4019$/mois trouvé dans une source de comparaison ne s'applique PAS à
l'échelle de ce projet -- c'est un scénario de référence entreprise, utilisé
pour comparer AWS/GCP/Azure entre eux à grande échelle, sans rapport avec un
seul petit cluster AKS de démonstration.

**DÉCISION FINALE** : Azure Monitor managed service for Prometheus + Azure
Managed Grafana -- combine le coût maîtrisé d'Azure Monitor (facturation à
l'usage, pas à l'infrastructure) avec une vraie compétence Prometheus/PromQL
et Grafana à démontrer en entretien, sans jamais recréer le risque de quota
déjà rencontré. Remplace la proposition initiale "Azure Monitor seul avec
Grafana en simple visualisation".

---

## 21 AOÛT — Incident critique de stockage, module Power BI amorcé

**Bilan de la journée** : diagnostic et correction du bug le plus impactant
du projet (perte de données silencieuse sous `/tmp`), amorce du chantier
Power BI (préparation KPIs, script d'export), mise en place d'un plan de
gestion de projet (Jira) pour la suite jusqu'à la soutenance.

**Détection** : `export_for_powerbi.py`, script écrit pour préparer la
connexion Power BI, échoue sur une table introuvable -- déclenche
l'investigation qui révèle que `/tmp/delta` (emplacement de stockage
utilisé depuis le tout début du projet, 11/08) est vidé par macOS à chaque
redémarrage du Mac. Explique rétrospectivement pourquoi l'historique
Isolation Forest ne dépassait jamais 2-3 points malgré Airflow actif en
continu depuis le 17/08. Détail complet dans
notes/incidents_2026-08-21.md.

**Correction** : stockage déplacé vers `<racine_projet>/data/delta`,
calculé dynamiquement dans `config.py` -- ne dépend plus d'un emplacement
système volatile. Pipeline à relancer entièrement pour reconstituer un
historique qui, cette fois, survivra aux redémarrages.

**Power BI** : réflexion sur le choix des KPIs (principe retenu : le test
du "et alors ?" -- écarter tout chiffre qui ne déclenche aucune décision).
Point technique clarifié : Power BI ne comprend pas nativement le journal
de transactions Delta Lake -- un script d'export dédié
(`export_for_powerbi.py`) produit des instantanés Parquet plats,
consommables sans ambiguïté, plutôt que de connecter Power BI directement
au dossier Delta ou de maintenir Databricks actif en continu (coût).

**Gestion de projet** : plan en 5 phases construit pour la suite du
projet, jusqu'à la semaine de soutenance (17/12) -- couvre à la fois les
chantiers techniques restants et la rédaction du mémoire. Jira identifié
comme outil de suivi (connecteur Atlassian Rovo repéré, pas encore
connecté).

---

## 26-27 AOÛT — Rafraîchissement des données, deux incidents Airflow

**Contexte** : reprise du chantier Power BI après plusieurs jours -- les
données Gold dataient du 21/08 (5 jours), le pipeline local n'ayant tourné
que pendant que le Mac était allumé (pas de vraie orchestration continue,
limite déjà identifiée précédemment).

**Incident 1** : `pipeline_complet_manuel` déclenché plusieurs fois sans
remarquer qu'un run précédent tournait -- 3 exécutions empilées, toutes
bloquées en `queued` plus d'une heure. Cause racine non identifiée avec
certitude ; résolu par recréation complète du conteneur Airflow. Décision :
DAG mis de côté, préférence pour un déclenchement direct des DAGs
individuels.

**Incident 2**, découvert en cascade : la base de métadonnées Airflow
(utilisateurs, historique, état pause/actif) ne survit pas à un
`docker-compose down`/`up` -- jamais montée en volume persistant. Correction
proposée, pas encore appliquée. Détail complet dans
notes/incidents_2026-08-26.md.

**Résultat** : données rafraîchies avec succès malgré les deux incidents --
Bronze 6044 lignes, Gold 295 alertes (92% géolocalisées). Export Power BI
relancé, prêt à recharger dans Fabric.

---

## 27 AOÛT — Fabric opérationnel, premier modèle sémantique construit

**Contexte** : reprise après la correction des incidents Airflow de la
veille -- données fraîches (295 alertes, 92% géolocalisées) déjà
exportées.

**Incident découvert et corrigé** : Fabric rejetait les fichiers Parquet
à cause d'une incompatibilité de précision sur les timestamps (nanoseconde
vs microseconde) -- détail complet dans
notes/incidents_2026-08-27.md.

**Progrès** : les 5 tables Gold chargées avec succès dans le Lakehouse
Fabric. Modèle sémantique vérifié (relations `gold_disruption_context` →
`by_region`/`by_station`, correctement en "un vers plusieurs"). 6 mesures
DAX créées : durée moyenne des perturbations, régions/gares affectées,
répartition par niveau d'alerte, comparaison au taux historique.

**Point de fonctionnement clarifié** : le rapport Power BI (mode Direct
Lake) se rafraîchit automatiquement dès que les tables Delta sous-jacentes
changent -- pas besoin de reconstruire le rapport à chaque nouvelle
donnée. Seule l'alimentation des tables reste manuelle en 3 étapes
(export Python → upload Fabric → Load to Tables), le blocage Service
Principal (permission tenant IEF2I) empêchant toujours une automatisation
complète via Airflow.

**Reste à faire** : construction des visuels (KPI en cartes, top
régions/gares, comparaison historique) -- prévue pour la prochaine
session.

---

## 28 AOÛT — Trois incidents Airflow/Kafka corrigés, chaîne complète reconstituée

**Contexte** : reprise après plusieurs jours sans exécution automatique
continue (Airflow ne tourne que pendant que la machine est active, limite
déjà connue). Tentative de rafraîchir les données a révélé une cascade de
trois incidents distincts, tous corrigés dans la même session.

**Incident 1** : fichier de checkpoint Kafka corrompu (`Malformed line`)
-- Kafka recréé (`stop`/`rm`/`up`), ce qui a réinitialisé le topic sans
que le checkpoint Spark ne le sache.

**Incident 2** : `KafkaIllegalStateException` -- l'offset attendu par
Spark (6044) n'existait plus dans le nouveau Kafka (repartie à 1776).
Corrigé par `failOnDataLoss=false` dans `bronze_ingestion.py`, cohérent
avec le rôle de Kafka comme simple tampon de transit dans cette
architecture (pas la source de vérité).

**Incident 3** : `ConcurrentTransactionException` -- `max_active_runs`
passé à 3 pour absorber un run bloqué a permis à deux exécutions
d'écrire simultanément sur le même checkpoint Delta. Corrigé par un
retour à `max_active_runs=1`, la bonne valeur pour une architecture
streaming+checkpoint (un seul écrivain à la fois, structurellement).

**Résultat** : chaîne complète reconstituée avec succès -- Bronze 11 396
lignes, Gold 470 alertes (90,2% géolocalisées, cohérent avec
l'historique). Export Power BI relancé sur les 5 fichiers. Détail complet
des 3 incidents dans notes/incidents_2026-08-28.md.

---

## 1er SEPTEMBRE — Bascule stratégique Power BI Desktop, en parallèle de Fabric

**Déclencheur** : question posée par un tiers sur l'architecture Fabric/Power BI
(Direct Lake vs Import, coûts, pérennité) a révélé un vrai risque non anticipé --
l'essai Fabric expire dans ~7 semaines. À l'expiration : Lakehouse et pipelines
deviennent immédiatement inutilisables, données récupérables seulement 7 jours si
réassignation à une capacité payante (inabordable sur budget étudiant).

**Décision** : construire l'architecture de restitution en parallèle sur Power BI
Desktop (connecteur Dossier, mode Import, fichiers Parquet locaux) --
indépendante de Fabric, 0€ durablement. Le travail déjà fait dans Fabric
(Direct Lake, 6 mesures DAX) est conservé comme vitrine technique, pas
abandonné, mais cesse d'être la seule dépendance du projet.

**Réalisé ce jour, côté Power BI Desktop** :
- Import des 5 tables Gold + relations reconstruites
- Structure en 3 pages : Vue d'ensemble, Analyse Géographique, Types de
  Perturbations
- Palette de couleurs validée : marron (#4A3728), doré (#C9A227), blanc/crème
  -- barres en dégradé hiérarchique par rang, répartition alertes en barre
  empilée (anneau en complément si la place le permet)
- Ajout de sncf_disruptions (Silver) à l'export -- nécessaire pour les 2
  nouvelles mesures temporelles, impossibles à calculer depuis Gold seul
  (voir notes/incidents_2026-09-01.md)
- 8 mesures DAX au total : les 6 initiales + Perturbations Aujourd'hui +
  Moyenne Journalière

**Sujet exploré et volontairement écarté pour l'instant** : architecture
multi-stockage fédérée (Trino + PostgreSQL/MongoDB/Iceberg/Kafka) -- vérifié
sur le marché français réel (offres DGSE, Innovaccer) : compétence de niveau
architecte/lead (5+ ans), pas pertinente pour un premier poste. Noté comme
piste de veille technique / futur projet post-CDI, pas construite.

**Reste à faire** : construire concrètement les 6 cartes KPI sur la Page 1
dans Power BI, puis Pages 2 et 3.

---

## 2 SEPTEMBRE — Page 2 entièrement construite, carte interactive fonctionnelle

**Page 1** : correction de la confusion carte KPI/carte géographique sur
les 6 cartes, validée avec de vraies données.

**Page 2 (Analyse Géographique)** : construite en totalité, au-delà du
plan initial -- Top 5 régions (avec mesure TREATAS pour compter les gares
distinctes sans relation directe), carte choroplèthe de France par
région (série d'incidents résolue un par un : catégorie géographique,
type de visuel, instabilité applicative, réglages de zoom/géocodage),
segment interactif validé (sélection d'une région filtre gares et carte,
sans ajustement de relation nécessaire). Détail complet des incidents
dans notes/incidents_2026-09-02.md.

**Page 3 (Types de Perturbations)** : démarrée -- graphique niveau
d'alerte, tableau type de perturbation par région fonctionnel, tableau
niveau par région en cours de correction (reporté au 03/09).

**Nouvelle compétence acquise** : pattern TREATAS pour filtrer une table
sans relation physique directe, en s'appuyant sur un `disruption_id`
partagé par construction entre tables issues de la même source.

---

## 4-8 SEPTEMBRE — Rattrapage : Mermaid, mesures journalières, incident Ivy

**Documentation** : diagramme du modèle de données (`docs/schema_donnees.md`,
Mermaid) et besoins métier (`docs/01_besoins_metier.md`) créés. Blocage
d'affichage résolu après plusieurs tentatives (cache VS Code, changement
d'extension) -- cause réelle : les mots-clés `PK`/`FK` cassaient le rendu
sur l'extension installée, retirés du diagramme.

**Power BI** : deux nouvelles mesures, `Moyenne Journalière Incidents
(Région)` (sans TREATAS, `Jour` et `disruption_id` co-résidents sur
`by_region`) et `Moyenne Journalière Gares (Région)` (avec TREATAS,
réutilisation directe du pattern établi).

**Point resté ouvert, non résolu** : une observation ("le nombre
d'incidents par région semble identique à la moyenne journalière")
signalée mais jamais clarifiée avec de vrais chiffres avant que la
session ne bifurque. À reprendre : demander le tableau exact
(regions_affectees, Nb Incidents par Région, Moyenne Journalière
Incidents (Région)) avant de diagnostiquer.

**Prompt de continuité** créé (`prompt_continuite_sncf.md`, fourni en
téléchargement) pour permettre une reprise dans un nouveau Projet Claude
séparé si besoin -- mémoire non partagée entre Projets.

**8 septembre** : incident Ivy/permissions résolu (voir
incidents_2026-09-08.md) -- pipeline de nouveau opérationnel.

## 2026-09-07 — Page 2 finalisée : infobulles carte + bug AVERAGEX

Page 2 (Analyse Géographique) est maintenant complète. Ajout d'infobulles sur
la carte choroplèthe en réutilisant les champs déjà présents dans les
tableaux existants (aucune nouvelle mesure nécessaire) — la carte affiche
maintenant les vraies valeurs au survol au lieu d'exiger un aller-retour
visuel vers les tableaux.

En chemin, corrigé un bug DAX sur les mesures de moyenne journalière
(`AVERAGEX` + `DISTINCTCOUNT` sans `CALCULATE` → renvoyait le total brut au
lieu d'une moyenne). Détail dans `incidents_2026-09-07.md`. Même famille de
symptôme que TREATAS, mécanisme différent (transition de contexte ligne →
filtre à l'intérieur d'un itérateur).

## 2026-09-08 — Pages 3/4 stylées, bug TODAY() corrigé, infra Docker/Airflow réparée

Harmonisation visuelle des Pages 3 et 4 avec la charte crème/doré/marron déjà
validée — technique la plus rapide : dupliquer un visuel déjà stylé
(Ctrl+C/Ctrl+V) plutôt que reconstruire le style à la main. Page 3 : titre
corrigé, donut aux couleurs indiscernables remplacé par un tableau simple.
Page 4 : couleurs du bar chart "Taux d'Annulation Par Ancien Découpage
Régional TER" appliquées via une échelle de couleurs liée à la valeur
(Format > Couleurs des données > fx > Échelle de couleurs) plutôt que des
couleurs fixes par catégorie — plus robuste, technique à réutiliser pour
Leclerc dès qu'une palette dynamique par valeur est utile.

Corrigé aussi `Perturbations Aujourd'hui` (`TODAY()` → `MAX(Jour)`, détail
dans `incidents_2026-09-08.md`).

Après-midi passée sur l'infra Docker/Airflow : DAG en échec (résolution DNS
`kafka`), plusieurs hypothèses testées et écartées avant un `down`/`up`
complet qui a réglé le problème. Profité de ce redémarrage pour enfin ajouter
la persistance Airflow (dette technique connue depuis des sessions), puis
tentative + fix d'un cache Ivy pour accélérer les runs futurs. Détail complet
dans `incidents_2026-09-08.md`.

Reste ouvert : écart 627 vs 419 sur "Perturbations Journalière" (pas
bloquant, à vérifier avant la soutenance), coquille possible sur l'onglet
"Type de Pertubations".

## 2026-09-10 — Phase 2 lancée : premier déploiement Azure réel

Décision de passer directement en production sur l'abonnement Azure
rattaché à l'école (IEF2I) sans attendre la confirmation du budget exact
— réponse école en attente. Fix `sku` Event Hubs (Basic → Standard,
bloquant pour Kafka), puis premier `terraform apply` réel du projet : 30
ressources créées (AKS, Databricks, Event Hubs, Storage/ADLS Gen2, Key
Vault, VNet, Grafana, Monitor Workspace), après résolution de 3 blocages
en cascade (providers non enregistrés, sku et version Grafana rejetés par
l'API). Règle d'accès `listen` ajoutée côté Event Hubs. Détail complet
dans `incidents_2026-09-10.md`.

## 2026-09-11 — Accès Key Vault résolu, incident de sécurité, intégration code complète

Bascule de `azure-cli` (Docker via fonction shell) vers un binaire natif
(pip, après un détour raté par Homebrew qui compilait depuis les sources).
Ça a révélé la vraie cause de l'accès Key Vault bloqué : Terraform
s'authentifie via un Service Principal dédié (`ARM_*`), distinct de
l'identité personnelle — la policy d'accès a été corrigée avec deux blocs
séparés au lieu d'un seul écrasé (erreur de la veille). En chemin,
`ARM_CLIENT_SECRET` exposé par erreur en clair dans la conversation,
roté à deux reprises avant qu'une méthode sûre (aucun affichage à l'écran)
soit mise en place.

Intégration complète de la lecture Key Vault dans `config.py`
(`KAFKA_SASL_PASSWORD`, mode dynamique choisi en cohérence avec la
décision de la veille) : un bug de portée de variable Python et un
timeout `AzureCliCredential` corrigés en chemin. Confirmé fonctionnel de
bout en bout — producteur et Spark basculent tous les deux vers Event
Hubs en changeant uniquement `.env`, sans avoir touché
`bronze_ingestion.py`. Détail complet dans `incidents_2026-09-11.md`.

Reste ouvert : test bout-en-bout réel (bascule `.env`, vérifier qu'un
message atteint vraiment Event Hubs), dérive Terraform cosmétique sur
`upgrade_settings` (AKS), réponse école sur le budget.

## 2026-09-11 (suite) — CI/CD Terraform débloquée

Après l'intégration Key Vault du matin, un email GitHub a signalé
l'échec du pipeline CI/CD (`deploy.yml`) sur `terraform-plan` — pipeline
existant découvert à cette occasion, bien conçu (plan automatique,
apply/destroy strictement manuels). Cause initiale simple (secrets GitHub
Actions jamais créés) compliquée par une série de faux départs :
texte de commande collé au lieu d'une valeur, plusieurs `pbcopy`
enchaînés sans coller entre chaque. La vraie cause racine s'est révélée
plus intéressante : `terraform`, comme `az`, est un wrapper Docker défini
dans `.zshrc` — mais lui charge en plus un fichier `.env.terraform`
séparé (`infra/terraform/`, ignoré par Git), jamais consulté jusqu'ici
puisque `.zshrc` seul avait toujours suffi pour `az`. Une fois cette
double source de vérité identifiée, alignement des 4 secrets GitHub sur
ce fichier, avec vérification de fraîcheur via un `terraform plan` local
avant de propager `ARM_CLIENT_SECRET`. CI confirmée verte. Détail complet
dans `incidents_2026-09-11.md` (point 7).

Aucun changement de code dans ce fil — uniquement configuration GitHub
et clarification de l'environnement local, rien à commiter.

## 2026-09-11 (suite 2) — Test bout-en-bout Event Hubs réussi

Après avoir débloqué le CI/CD, bascule réelle de `.env` vers Azure et
premier vrai test de la chaîne complète. Un bug trouvé et corrigé en
chemin : le producteur utilisait par erreur la clé de lecture d'Event
Hubs au lieu de la clé d'écriture (les deux clés existaient depuis le
début côté Terraform, mais `config.py` ne les distinguait pas). Une fois
séparées, 812 perturbations publiées avec succès sur Azure, puis lues et
écrites dans le Delta local par Spark — boucle complète confirmée en
conditions réelles pour la première fois. Détail dans
`incidents_2026-09-11.md` (point 8).

Migration Azure réelle : Event Hubs est maintenant le seul composant
confirmé "en usage réel" (pas juste créé) — ADLS Gen2 et AKS restent à
faire.
