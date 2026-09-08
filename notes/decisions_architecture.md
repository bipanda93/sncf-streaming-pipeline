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

---

## Sécurité & gestion des secrets

### Vérification systématique de git status avant tout commit touchant des identifiants
**Contexte** : un `.gitignore` créé avec un chemin relatif incorrect (motif
doublement imbriqué, sans effet réel) a laissé passer un secret Azure
(`ARM_CLIENT_SECRET`) dans un commit, sans qu'aucune erreur ne le signale au
moment de l'écriture du fichier.
**Décision** : avant tout commit dans un dossier contenant des identifiants
(`.env`, `.env.terraform`...), vérifier explicitement le contenu de `git
status`/`git add` -- jamais supposer qu'un `.gitignore` fonctionne du simple
fait qu'il existe.
**Justification** : un `.gitignore` mal placé échoue silencieusement --
aucune erreur, aucun avertissement, juste une protection qui ne protège rien.
Seule une vérification active de la liste réelle des fichiers avant commit
aurait détecté le problème plus tôt (elle l'a fait, mais après le premier
commit fautif, pas avant).
**Remédiation appliquée** : révocation immédiate du secret exposé (`az ad sp
credential reset`) en priorité absolue, indépendamment du nettoyage de
l'historique Git -- un secret qui a fuité se révoque, il ne se cache pas
mieux.

---

## Réseau

### VNet explicite plutôt que réseau géré automatiquement par chaque service
**Contexte** : Databricks et AKS peuvent chacun créer leur propre réseau
isolé automatiquement, sans configuration Terraform dédiée.
**Alternative écartée** : laisser chaque service gérer son réseau
séparément (plus simple à écrire, mais services isolés les uns des autres,
tout le trafic entre eux transiterait par internet public).
**Décision** : un VNet unique, avec des sous-réseaux dédiés par service.
**Justification** : permet une connectivité privée entre Databricks, AKS et
ADLS Gen2 (jamais via internet public) -- pattern attendu en architecture de
production réelle, pas un raccourci de POC. Coût nul : un VNet n'est pas une
ressource facturée en soi, contrairement à AKS ou Databricks -- aucun
compromis budgétaire à faire sur ce choix précis.

---

## CI/CD Terraform

### Plan automatique, apply/destroy strictement manuels
**Contexte** : mise en place du pipeline CI/CD pour l'infrastructure
Terraform, après une session ayant révélé des coûts réels et 6 erreurs
corrigées lors d'un apply manuel (voir notes/incidents_2026-08-19.md).
**Alternative écartée** : apply automatique sur merge vers main (pattern
GitOps classique).
**Décision** : `terraform plan` automatique sur chaque push/PR touchant
`infra/terraform/` (gratuit, sans risque) ; `apply` et `destroy`
déclenchés uniquement via `workflow_dispatch` (action manuelle explicite
depuis GitHub), protégés par une règle d'approbation d'environnement.
**Justification** : un push accidentel ne doit jamais pouvoir créer ou
détruire de l'infrastructure facturée sans supervision humaine directe --
le pattern GitOps classique (auto-apply sur merge) est adapté à des
équipes avec une infrastructure stable et un budget de production réel,
pas à un projet académique en développement actif sur un compte étudiant
à crédit limité.

---

## Monitoring

### Azure Monitor managed service for Prometheus + Azure Managed Grafana
**Contexte** : le plan d'architecture d'origine laissait le choix ouvert
entre Prometheus/Grafana auto-hébergé et Azure Monitor, jamais tranché ni
construit -- redécouvert tardivement dans le projet.
**Alternatives écartées** :
- Prometheus/Grafana auto-hébergé sur AKS : nécessiterait un nœud/pool
  supplémentaire, risquant de reproduire le problème de quota vCPU déjà
  rencontré (3 familles de VM refusées avant `Standard_D2s_v6`, voir
  notes/incidents_2026-08-19.md).
- Azure Monitor classique seul (sans PromQL/Grafana) : solution de repli
  initiale, moins riche pour démontrer une compétence Prometheus/Grafana en
  entretien.
**Décision** : Azure Monitor managed service for Prometheus (facturé à
l'ingestion/requête, pas à l'hébergement) + Azure Managed Grafana (service
managé, intégration native).
**Justification** : élimine le risque de quota (aucun nœud dédié requis
pour Prometheus lui-même) tout en conservant un vrai PromQL et un vrai
Grafana -- combine la contrainte de coût du compte étudiant avec la valeur
de démontrer ces deux outils, largement utilisés en entreprise.

---

## Stockage

### Chemin de base calculé dynamiquement, hors de /tmp
**Contexte** : toutes les tables Delta du projet vivaient sous `/tmp/delta`
depuis la conception initiale -- jamais questionné jusqu'à ce qu'un
incident révèle que macOS vide intégralement `/tmp` à chaque redémarrage
du Mac (voir notes/incidents_2026-08-21.md, incident n°3).
**Alternative écartée** : garder `/tmp/delta` avec une sauvegarde
périodique manuelle -- rejeté, un mécanisme de contournement n'aurait pas
supprimé le risque, seulement retardé la prochaine perte de données.
**Décision** : chemin de base calculé depuis l'emplacement de `config.py`
lui-même (`<racine_projet>/data/delta`), jamais sous un dossier système
volatile.
**Justification** : fonctionne identiquement sur l'hôte et dans le
conteneur Airflow sans montage Docker séparé (le dossier projet entier est
déjà monté) -- simplifie `docker-compose.yml` en plus de corriger le
problème de fond. `data/` exclu du suivi Git (fichiers binaires, pas de
valeur à versionner).

---

## Restitution — traitement des valeurs manquantes

### NULL sur region_taux_historique_moyen (trip canceled) — géré côté Power BI, pas corrigé en pipeline
**Contexte** : 8 lignes (annulations totales, gold_disruption_context) ont
`region_taux_historique_moyen` NULL -- caractéristique réelle (aucune gare
affectée à géolocaliser pour une annulation complète), pas un bug.
**Décision** : documenté via une mention en bas de page Power BI plutôt
qu'un correctif de pipeline -- gain marginal ne justifiant pas le temps
à ce stade du projet.
**Point de vigilance noté** : `nb_regions_affectees` (NULL) et
`nb_affected_stations` (0, via coalesce déjà appliqué) traitent la même
situation différemment -- à harmoniser si une comparaison directe des deux
métriques est faite un jour.

---

## Orchestration

### pipeline_complet_manuel mis de côté (deuxième incident)
**Contexte** : bloqué en `queued` permanent lors d'un déclenchement
multiple accidentel (26/08) -- cause racine non identifiée avec certitude,
deuxième incident réel sur ce DAG après celui du 17/08.
**Décision** : préférer le déclenchement direct des DAGs individuels
(`ingestion_bronze_temps_reel`, `silver_gold_temps_reel`...) plutôt que le
wrapper, jusqu'à investigation plus poussée.
**Statut** : DAG conservé dans le projet (documente une intention
d'orchestration manuelle complète), mais déconseillé en pratique pour
l'instant.

### Base de métadonnées Airflow -- persistance non appliquée (ouvert)
**Contexte** : `docker-compose down` + `up` efface entièrement la base
interne d'Airflow (contrairement à `restart`, qui la préserve) --
découvert le 26/08 après un `no such table: dag`.
**Correction proposée, non appliquée** : volume dédié
(`./airflow_metadata:/opt/airflow/persistent_db`) +
`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` pointant dessus.
**Statut** : ouvert -- à appliquer avant la prochaine recréation complète
du conteneur, sous peine de revivre la perte du compte admin et de
l'historique des runs.

---

## Power BI / Fabric — mécanisme de rafraîchissement confirmé

### Direct Lake : rapport auto-actualisé, alimentation des tables manuelle
**Contexte** : clarification nécessaire sur ce qui se passe quand les
données sources évoluent (nouvelles perturbations Kafka) après la
construction du rapport.
**Confirmé** : en mode Direct Lake, le rapport lit l'état courant des
tables Delta à chaque ouverture -- aucune reconstruction du rapport
nécessaire quand les données changent. Distinction claire entre "le
rapport" (construit une fois, stable) et "les données" (à rafraîchir
régulièrement).
**Limite persistante** : l'alimentation des tables reste manuelle en 3
étapes (export → upload → Load to Tables), tant que le blocage Service
Principal (voir board Trello, "Problèmes et requêtes") n'est pas levé.

---

## Power BI — architecture double, Fabric + Desktop (01/09)

### Contexte
L'essai Microsoft Fabric expire ~mi-octobre 2026. À l'expiration, les
éléments Fabric (Lakehouse, Direct Lake) deviennent inutilisables, avec
seulement 7 jours de grâce pour récupérer les données avant risque de perte.
Une capacité payante (F2 minimum, ~262$/mois) est hors budget étudiant.

### Décision
Deux architectures de restitution maintenues en parallèle :
1. **Fabric / Direct Lake** (déjà construite) -- conservée comme démonstration
   technique de ce mode de fonctionnement, pas comme dépendance du projet
2. **Power BI Desktop / Import** (nouvelle) -- fichiers Parquet locaux via
   connecteur Dossier, 0€ durablement, aucune dépendance à une capacité cloud

### Justification
Import préserve automatiquement relations/clés/mesures DAX à chaque
actualisation, tant que le schéma (noms de colonnes, types) reste stable
entre exports -- pas de reconstruction nécessaire à chaque nouvelle donnée.

### Conséquence pour le portfolio
Permet de présenter les deux architectures en entretien comme un choix
raisonné (compromis coût/performance/pérennité), plutôt que de subir la
perte du travail Fabric à l'expiration de l'essai.

---

## Power BI — TREATAS plutôt que relation bidirectionnelle (03/09)

### Contexte
Mesure `Nb Perturbations par Région et Niveau` (tableau croisé région ×
alert_level) renvoyait le même total partout -- diagnostic : la relation
gold_disruption_by_region -> gold_disruption_context existe bien
("Plusieurs à un"), mais en direction de filtre "Une seule", donc un
filtre posé côté by_region n'atteint jamais context.

### Deux options pesées
1. Passer la relation en filtrage bidirectionnel ("Les deux")
2. Garder le filtrage à sens unique, transporter le filtre région
   explicitement via TREATAS dans les mesures qui en ont besoin

### Décision : option 2, TREATAS
Changer la direction du filtre si tard dans la construction du dashboard
aurait un effet global sur tout le modèle (risque de résultats gonflés
sur d'autres mesures déjà validées, via by_region qui a plusieurs lignes
par perturbation) -- non retestable entièrement dans le temps restant.
TREATAS confine le changement à la seule mesure qui en a besoin, rend le
filtre explicite plutôt qu'implicite dans le modèle.

### Principe retenu
Une relation change le comportement de tout le modèle ; une formule DAX
change le comportement d'une seule mesure. Préférer la formule ciblée
quand le risque de duplication est réel et le temps de retest limité.

---

## Power BI — granularités mélangées dans gold_punctuality_trends (03/09)

### Contexte
La table combine 5 jeux de données (TER région, Intercités liaison, TGV
national/axe/liaison) via un système scope_type/scope_value pensé pour
l'analyse ligne à ligne -- pas conçu à l'origine pour un agrégat "toutes
compagnies confondues" sans risque de double comptage.

### Découverte du 03/09
scope_value mélange en plus, au sein du seul scope_type="region" (TER),
des appellations pré-2016 et des régions déjà fusionnées post-2016 --
chevauchement potentiel non quantifié.

### Décision
Isoler le périmètre exact du risque avant de corriger : vérifié que
gold_disruption_context (source de la carte et des tableaux Pages 2/3)
n'est pas affecté, seule gold_punctuality_trends l'est. Visuel construit
avec titre explicite plutôt que bloquant. Correction de fond (table de
correspondance ancien/nouveau découpage, ou re-agrégation à la source)
reportée à une session dédiée -- à traiter avant la rédaction du mémoire
si ce visuel y figure.

## 2026-09-08 — Stratégie de persistance des volumes Docker (Airflow)

**Contexte** : deux besoins différents de persistance sur le service
`airflow` du `docker-compose.yml` — l'état interne critique (métadonnées,
compte admin, historique DAG runs) et un cache de performance (dépendances
Ivy/Spark, re-téléchargées à chaque rebuild d'image).

**Décision** : séparer les deux plutôt que tout mettre dans un seul volume.

- État critique (`/opt/airflow`) → **volume nommé Docker** (`airflow_home`).
  Géré entièrement par Docker, persiste à travers un `down` (sans `-v`), pas
  besoin d'accès direct depuis l'hôte.
- Cache de performance (`/home/airflow/.ivy2.5.2`) → **bind mount local**
  (`./.ivy_cache`). Nécessaire car Docker Desktop Mac crée les volumes nommés
  avec propriétaire `root`, incompatible avec l'utilisateur non-root du
  conteneur Airflow custom (`Dockerfile.airflow`) — écriture refusée sinon
  (`FileNotFoundException` constaté). Un bind mount hérite des permissions du
  dossier hôte, pas de ce problème.

**Alternative écartée** : tout mettre dans un seul volume nommé — plus simple
à écrire, mais se heurte au même problème de permissions sur le sous-dossier
Ivy.

**Alternative écartée** : `chown`/`chmod` du volume au démarrage via un
entrypoint custom dans le Dockerfile — plus "propre" en théorie, mais
complexité additionnelle non justifiée par le gain (quelques secondes par
run) vu le temps disponible avant la soutenance.

**Portée** : ce pattern (bind mount pour tout conteneur custom non-root qui
a besoin d'écrire dans un volume) est directement réutilisable pour Leclerc,
en particulier pour les conteneurs Airbyte OSS auto-hébergés.
