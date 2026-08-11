# Journal des incidents — 11 août 2026

Session de travail sur le projet SNCF Streaming Pipeline : validation complète
de l'ingestion temps réel avec une vraie clé API, et construction de la source
batch historique. Ce document recense chaque problème rencontré, le test qui
l'a révélé, la solution appliquée, et la validation qui la confirme.

**Bilan de la session** : 11 problèmes distincts rencontrés et résolus,
2 pipelines Bronze fonctionnels et vérifiés (temps réel + historique, 5 jeux
de données), aucun contournement laissé sans validation.

---

## 1. Environnement et configuration

### 1.1 — Fichiers créés vides malgré les commandes données

| | |
|---|---|
| **Problème** | Plusieurs fichiers (`docker-compose.yml`, `requirements.txt`, `.gitignore`) créés avec `touch` en tout début de projet n'ont jamais été réellement remplis sur le terminal, alors qu'ils l'étaient dans les échanges |
| **Test révélateur** | `cat docker-compose.yml`, `cat requirements.txt` → fichiers vides |
| **Solution** | Recopie systématique du contenu via `cat > fichier << 'EOF'`, avec vérification `cat` immédiatement après chaque création |
| **Validation** | Contenu affiché conforme à chaque fois avant de continuer |

### 1.2 — `.env` corrompu par TextEdit

| | |
|---|---|
| **Problème** | `open -e .env` puis édition dans TextEdit a vidé le fichier (piège connu de TextEdit sur les fichiers cachés sans extension reconnue) |
| **Test révélateur** | `grep -c "SNCF_API_TOKEN=." .env` → `0` |
| **Solution** | Reconstruction via `cp .env.example .env`, édition avec `nano` (éditeur terminal) plutôt que TextEdit |
| **Validation** | `grep "SNCF_API_TOKEN" .env \| sed -E 's/=.+/=<VALEUR_PRESENTE>/'` → confirme une valeur présente sans jamais l'afficher en clair |

### 1.3 — Dossiers parasites `git/` et `init/`

| | |
|---|---|
| **Problème** | `mkdir sncf-streaming-pipeline git init` collé sur une seule ligne (sans validation entre les deux commandes) a créé 3 dossiers séparés au lieu d'exécuter `mkdir` puis `git init` |
| **Test révélateur** | `ls -la ~ \| grep -E "git\|init"` → deux dossiers vides inattendus |
| **Solution** | `rmdir ~/git ~/init` (sans risque, dossiers vides confirmés avant suppression) |
| **Validation** | Nouvelle vérification `ls -la ~` : dossiers absents |

### 1.4 — `.git` préexistant dans le dossier personnel

| | |
|---|---|
| **Problème** | Un `.git` avec de vrais commits (Projet 1, UCI Retail) existait déjà dans `~`, indépendamment de notre incident — risque de confusion entre deux dépôts imbriqués |
| **Test révélateur** | `git -C ~ log --oneline -20` → 2 commits réels affichés |
| **Solution** | Aucune suppression (des commits réels auraient été perdus) — clarification que Git s'arrête au premier `.git` trouvé en remontant depuis le dossier courant, donc aucun conflit tant que les commandes sont lancées depuis le bon dossier |
| **Validation** | Confirmation que les commandes Git du projet SNCF utilisent bien son propre `.git`, sans interférence |

### 1.5 — `.gitignore` vide au moment du premier commit

| | |
|---|---|
| **Problème** | `.gitignore` jamais rempli réellement — `.env` (contenant le vrai token SNCF) est apparu dans `git status` comme fichier non ignoré, juste avant un premier commit |
| **Test révélateur** | `git status` → `.env` listé parmi les fichiers non trackés |
| **Solution** | Remplissage de `.gitignore` (`__pycache__/`, `.venv/`, `.env`, etc.) **avant** tout `git add` |
| **Validation** | `git status` après correction : `.env` absent de la liste ; confirmé absent du commit final (`git show --stat`) |

---

## 2. Ingestion temps réel — API SNCF

### 2.1 — Image Docker Bitnami Kafka dépréciée

| | |
|---|---|
| **Problème** | `docker-compose up -d` échoue : `bitnami/kafka:3.7: not found` |
| **Test révélateur** | Erreur `failed to resolve reference` au démarrage des conteneurs |
| **Solution** | Recherche confirmant que Bitnami a fermé l'accès gratuit à ses images versionnées depuis août 2025 — bascule vers l'image officielle `apache/kafka:3.7.0` (KRaft natif, maintenue directement par le projet Apache) |
| **Validation** | `docker-compose up -d` réussi, `docker-compose ps` confirme les deux conteneurs `Up` |

### 2.2 — Pagination silencieuse sur `/disruptions`

| | |
|---|---|
| **Problème** | `get_disruptions()` ne retournait que 25 perturbations, sans erreur — soupçonné à tort comme une limite géographique (Gare de Lyon), en réalité une pagination non gérée |
| **Test révélateur** | Inspection de `raw["pagination"]` → `total_result: 2591`, `items_per_page: 25` |
| **Solution** | Ajout des paramètres `count=1000` + filtre `since`/`until` sur la journée en cours ; test complémentaire confirmant que 599 perturbations/jour restent sous la limite de 1000 (pas besoin d'une vraie boucle de pagination) |
| **Validation** | `main.py --once` publie ~599-629 perturbations réelles sur Kafka (vérifié visuellement sur Kafka UI), contre 25 avant correctif |

---

## 3. Databricks / Spark — Bronze temps réel

### 3.1 — Connecteur Kafka manquant

| | |
|---|---|
| **Problème** | `bronze_ingestion.py` échoue : `AnalysisException: Failed to find data source: kafka` |
| **Test révélateur** | Traceback au premier lancement, après téléchargement réussi des seuls JARs Delta |
| **Solution** | `configure_spark_with_delta_pip` ne télécharge que les JARs Delta ; ajout explicite du package Kafka via `extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0"]` (version vérifiée sur Maven Central, alignée avec Spark/Scala) |
| **Validation** | Job Bronze temps réel exécuté avec succès, 629 lignes écrites, schéma conforme, vérifié via lecture Delta |

### 3.2 — Fichiers de config non synchronisés (récurrent)

| | |
|---|---|
| **Problème** | Plusieurs fois (`config.py` avant `bronze_ingestion.py`, puis avant `historical_loader.py`), un seul bloc `cat >` sur plusieurs proposés dans le même message a été exécuté côté terminal, laissant `config.py` désynchronisé |
| **Test révélateur** | `AttributeError: module 'src.ingestion.config' has no attribute 'DELTA_BRONZE_PATH'` (puis `CHECKPOINT_BRONZE_PATH`, puis `DELTA_BRONZE_HISTORICAL_PATH`) |
| **Solution** | Isoler et recoller uniquement le bloc manquant, avec vérification systématique via `python3 -c "from src.ingestion import config; print(config.XXX)"` avant de relancer le job |
| **Validation** | Import réussi affichant la bonne valeur, à chaque occurrence |

---

## 4. Source historique — téléchargement et parsing CSV

### 4.1 — BOM UTF-8 en tête de fichier

| | |
|---|---|
| **Problème** | En-tête réel `\ufeffdate` au lieu de `date` — invisible à l'affichage normal, aurait cassé toute référence à la colonne `date` en aval |
| **Test révélateur** | `repr(header)` → `'\ufeffdate;region;...'` |
| **Solution** | Décodage explicite en `utf-8-sig` (au lieu de `response.text` par défaut) dans `regularity_client.py` |
| **Validation** | `header.startswith("date")` → `True` après correctif |

### 4.2 — Fragmentation des champs multi-lignes par le lecteur CSV de Spark (incident principal de la session)

| Tentative | Méthode | Résultat | Diagnostic |
|---|---|---|---|
| 1 | `split("\n")` naïf en Python | 3196 lignes (référence erronée) | Sur-comptage : ignore les guillemets CSV |
| 2 | `spark.read.csv()` sans `multiLine` | 3170 lignes écrites | Fragmentation du champ `commentaires` (retours à la ligne entre guillemets) |
| 3 | `spark.read.csv()` avec `multiLine=True` | 2405 lignes écrites | Amélioration incomplète — ambiguïté persistante sur l'échappement des guillemets |
| 4 | Re-sérialisation CSV via `csv.writer` avant lecture Spark | 2405 lignes | Aucun changement — le lecteur Spark restait la source du problème, pas le format du fichier |
| 5 | `csv.reader` Python pur + `spark.createDataFrame(data_rows, schema=header)`, sans repasser par le lecteur CSV de Spark | **2366 lignes** ✅ | Résultat stable et reproductible, cohérent avec le comptage de référence (`csv.reader` : 2366 lignes, 9 colonnes constantes, 0 ligne vide) |

**Solution retenue** : `historical_loader.py` parse le CSV avec le module standard Python (`csv.reader`), puis construit directement le DataFrame Spark depuis les lignes déjà découpées — Spark ne réinterprète jamais le format CSV brut lui-même.

**Validation** : lecture depuis Delta confirmant 2366 lignes, données réelles cohérentes (régions françaises, taux de régularité, historique depuis 2015).

**Limite connue, documentée plutôt qu'ignorée** : cette solution charge l'intégralité du fichier en mémoire côté driver avant de construire le DataFrame — `WARN TaskSetManager: Stage contains a task of very large size (1095 KiB)` observé sur le jeu `tgv_liaisons` (12544 lignes). Non bloquant sur le volume actuel (fichiers mensuels, quelques Mo maximum), mais ne scalerait pas à un fichier de plusieurs Go — acceptable pour cette source (mise à jour mensuelle, volumétrie modeste), à revoir si un jeu de données nettement plus volumineux était ajouté.

---

## Enseignement méthodologique transversal

Le point commun à la majorité de ces incidents : **aucun n'a levé d'exception à l'exécution**. Les commandes se terminaient sans erreur en écrivant silencieusement une donnée incomplète ou incorrecte (25 perturbations au lieu de 599, 3170 lignes fragmentées au lieu de 2366, un `.gitignore` vide n'empêchant rien). La discipline appliquée tout au long de cette session — vérifier le contenu réel après chaque étape plutôt que de faire confiance à l'absence d'erreur — a permis de les détecter avant qu'ils ne se propagent dans les couches suivantes (Silver, Gold).
