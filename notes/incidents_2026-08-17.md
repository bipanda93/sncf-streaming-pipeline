# Journal des incidents — 17 août 2026

Session de travail sur le projet SNCF Streaming Pipeline : mise en place
complète de l'orchestration Airflow (installation, 4 DAGs planifiés, 1 DAG
utilitaire).

**Bilan de la session** : 1 incident critique évité de justesse (compilation
Rust/LLVM), plusieurs surprises liées à la migration Airflow 2->3
documentées, 1 lacune architecturale corrigée (signalée par l'utilisateur),
5 DAGs construits et validés en conditions réelles.

---

## 1. Installation locale d'Airflow -- cascade de compilation Rust/LLVM

| | |
|---|---|
| **Problème** | `pip install apache-airflow` échoue : `error: can't find Rust compiler` (dépendance `libcst`) |
| **Tentative de contournement** | `brew install rust` -- déclenche une cascade de compilations de dépendances (OpenSSL, cmake) puis, de façon alarmante, la compilation complète de LLVM/Clang depuis les sources (potentiellement plusieurs heures) |
| **Décision** | Interruption de la compilation (Ctrl+C) avant qu'elle ne se termine -- signal clair que l'installation locale directe n'était pas la bonne voie |
| **Solution retenue** | Pivot vers Docker, même stratégie que pour Kafka/Bitnami plus tôt dans le projet -- isoler l'installation plutôt que déboguer indéfiniment l'environnement local (toolchain Xcode désynchronisée sur cette machine) |
| **Validation** | Conteneur Airflow opérationnel en quelques minutes de build, sans aucune compilation sur la machine hôte |

## 2. Migration Airflow 2.x -> 3.x -- changements d'API non anticipés

| | |
|---|---|
| **Problème potentiel** | Connaissance préalable d'Airflow basée sur la v2 (DAGs précédents du portfolio) -- risque d'écrire du code utilisant une syntaxe obsolète |
| **Vérification préventive** | Recherche des changements avant d'écrire le moindre DAG, plutôt que de découvrir les erreurs une par une |
| **Changements identifiés** | `schedule_interval=` -> `schedule=` ; `PythonOperator`/`BashOperator` déplacés vers `apache-airflow-providers-standard` (installation séparée requise) ; `SequentialExecutor` supprimé (`LocalExecutor` + SQLite désormais suffisant en local) ; commande `webserver` renommée `api-server` |
| **Résultat** | DAGs écrits directement avec la bonne syntaxe dès la première tentative, aucun débogage de compatibilité nécessaire |

## 3. Mot de passe admin généré aléatoirement (SimpleAuthManager)

| | |
|---|---|
| **Observation** | Aucun identifiant défini explicitement dans `docker-compose.yml`, pourtant un utilisateur admin fonctionnel existe |
| **Cause** | Comportement voulu du mode `standalone` : `SimpleAuthManager` (auth manager par défaut d'Airflow 3.x) génère un mot de passe aléatoire au premier démarrage, imprimé dans les logs et stocké en clair dans `/opt/airflow/simple_auth_manager_passwords.json.generated` -- design volontaire pour éviter un mot de passe par défaut prévisible sur tous les environnements Airflow |
| **Vérification** | Confirmé via une recherche : les anciennes variables d'environnement `_AIRFLOW_WWW_USER_PASSWORD` (fonctionnelles en v2 avec FAB) ne s'appliquent plus à `SimpleAuthManager` -- impossible de fixer ce mot de passe de façon déterministe par simple configuration |
| **Solution pragmatique** | Modification directe du fichier JSON généré pour un mot de passe mémorisable (`admin`/`admin`), acceptable en dev local uniquement |

## 4. Lacune architecturale -- ingestion Bronze non orchestrée

| | |
|---|---|
| **Problème** | Le premier DAG écrit (`silver_gold_temps_reel`) ne couvrait que Silver -> Gold ; `main.py` et `bronze_ingestion.py` (temps réel) restaient hors de toute orchestration, ne tournant que si lancés manuellement |
| **Origine de la détection** | Signalée par l'utilisateur, pas anticipée en amont -- raisonnement initial erroné : "ces scripts utilisent une boucle continue, donc pas adaptés à Airflow", en oubliant que le mode `--once` (construit dès le début du projet pour les tests) permettait justement de les rendre orchestrables |
| **Solution** | Nouveau DAG dédié (`ingestion_bronze_temps_reel`, horaire), réutilisant `--once` sur les deux scripts -- Airflow devient le mécanisme de répétition, remplaçant la boucle Python interne plutôt que de cohabiter à côté |
| **Validation** | 879 perturbations ingérées lors du premier run réel, Bronze passé de 629 à 1508 lignes |

## 5. Réseau Docker -- Kafka injoignable via l'adresse habituelle

| | |
|---|---|
| **Problème potentiel identifié en amont** | `KAFKA_BOOTSTRAP_SERVERS=localhost:9094` (valeur par défaut du projet) ne fonctionnerait pas depuis un conteneur -- `localhost` y désigne le conteneur lui-même, pas la machine hôte |
| **Solution préventive** | Variable d'environnement `KAFKA_BOOTSTRAP_SERVERS=kafka:9092` (adresse interne au réseau Docker) définie explicitement pour le service `airflow` dans `docker-compose.yml`, avant même le premier test |
| **Validation** | Premier run réel réussi sans erreur de connexion Kafka |

---

## Enseignement méthodologique

Deux incidents de cette session (le 1 et le 4) partagent un point commun :
dans les deux cas, la bonne décision est venue d'un changement de stratégie
plutôt que d'un acharnement sur l'approche initiale -- interrompre une
compilation qui dérapait plutôt que d'attendre, et reconnaître qu'un
raisonnement architectural initial (Bronze "pas adapté à Airflow") était
incomplet plutôt que de le défendre. Le 4e incident illustre aussi la valeur
d'un regard extérieur : c'est l'utilisateur, pas l'assistant, qui a détecté
la lacune -- une bonne architecture bénéficie d'être questionnée par
plusieurs angles, pas seulement validée par celui qui l'a conçue.
