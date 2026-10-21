# Incidents 2026-10-21 — CI GitHub Actions et protection de `main`

## 1. Aucun test réel dans le dépôt

**Symptôme** : `pytest tests/ -v` répond `collected 0 items` /
`no tests ran`.

**Cause** : `tests/test_sncf_client.py` et `tests/test_kafka_producer.py`
font 0 ligne, et `tests/test_streaming/` et `tests/test_enrichment/` sont
vides. Ce sont les squelettes créés au début du projet, jamais remplis. Le
README annonçait « CI/CD : GitHub Actions » alors que seul le Terraform
était couvert, par `deploy.yml`.

**Fix** : premier vrai test, `tests/test_dags.py` (chargement des 6 DAGs,
un DAG par fichier, `dag_id` uniques), exécuté en CI par le job
`dag-tests`. Les tests Spark restent à écrire.

---

## 2. `pip install apache-airflow==3.3.1` impossible sur le Mac (Intel)

**Symptôme** : `Failed building wheel for libcst` /
`error: can't find Rust compiler`, y compris après `pip install --upgrade pip`.

**Cause** : la version de `libcst` imposée par le fichier de contraintes
d'Airflow 3.3.1 n'est pas distribuée toute compilée pour macOS x86_64.
pip tente de la compiler et il lui faut Rust.

**Fix** : tests lancés dans le conteneur `sncf-airflow` (Airflow 3.3.1,
Linux, mêmes conditions que la CI) —
`docker exec -w /opt/airflow/project sncf-airflow python -m pytest tests/test_dags.py -v`.
Le projet y est monté dans `/opt/airflow/project`. Constat annexe :
l'environnement conda `base` contient un PySpark à moitié désinstallé
(`Ignoring invalid distribution ~yspark`).

---

## 3. `terraform fmt -check` : `monitoring.tf` mal formaté

**Symptôme** : `terraform fmt -check -recursive` liste `monitoring.tf`
(le contrôle aurait échoué en CI).

**Cause** : fichier jamais passé par `terraform fmt`.

**Fix** : `terraform -chdir=infra/terraform fmt monitoring.tf`
(2 lignes d'alignement, aucun changement de comportement).

---

## 4. Ruff : 23 erreurs, dont 2 vrais défauts de logique

**Symptôme** : `ruff check src/ dags/ tests/ --statistics` →
`UP017` x9, `BLE001` x5, `I001` x4, `UP045` x3, `F401` x1, `RUF059` x1.

**Cause et fix** :
- 17 corrections automatiques sûres (`--fix`) : `timezone.utc` → `UTC`,
  imports triés (les imports longs découpés une ligne par nom, d'où de gros
  diffs sans changement de logique), `Optional[X]` → `X | None`, import
  `count` inutilisé retiré de `gold_disruption_context.py`.
- `BLE001` x3 (`delay_analyzer.py`, `audit_logger.py`,
  `pipeline_metrics.py`) : le même code, copié trois fois, prenait
  **n'importe quelle exception** pour « la table n'existe pas ». Une session
  Spark tombée ou un problème de droits passait pour une table absente ;
  dans `audit_logger.py`, le code aurait alors cru devoir créer la table
  d'audit. Remplacé par une fonction partagée
  `src/common/delta_utils.py::delta_table_existe()`
  (`DeltaTable.isDeltaTable`, `delta-spark==4.1.0`).
- `RUF059` + `BLE001` (`dashboard_streamlit.py`) : `load_delta_table`
  renvoyait l'erreur de lecture dans `error`, jamais utilisée : les échecs
  de lecture étaient **perdus en silence**. Désormais affichés par
  `st.warning`, sauf pour les zones de rejet, où une table absente est
  normale. `except Exception` conservé à cette frontière avec un
  `noqa: BLE001` justifié.
- `BLE001` (`compute_fraicheur`) : réduit à `(TypeError, ValueError)`.

---

## 5. Correction de la zone de rejet absente du commit

**Symptôme** : `dashboard_streamlit.py` affichait `3 +, 2 -` dans
`git show --stat` au lieu des `4 +` attendus ; `grep "is_reject_zone:"`
ne trouvait rien.

**Cause** : le script de correction n'avait pas été lancé avant
`git add src/`. Sans lui, une zone de rejet vide (situation saine)
déclenchait un avertissement.

**Fix** : commit séparé `fix: pas d'avertissement pour une zone de rejet
vide (situation saine)`. Réflexe : lire `git show --stat` avant de passer
à la suite.

---

## 6. `deploy.yml` : apply/destroy dangereux avec un state local

**Symptôme** : aucun bloc `backend` dans `infra/terraform/*.tf` ;
`terraform-plan` réussit en CI (les secrets `ARM_*` sont encore valides)
mais part d'un state vide et annonce la création de toutes les ressources.

**Cause** : le state Terraform est local. Un `apply` lancé depuis GitHub
aurait créé des ressources Azure facturées puis perdu le state à la fin du
run ; aucun `destroy` n'aurait pu les retrouver.

**Fix** : `deploy.yml` limité à un plan manuel (`workflow_dispatch`),
`apply`/`destroy` retirés et laissés en commentaire, réactivables après la
mise en place d'un backend `azurerm` distant. La validation automatique du
Terraform passe par `terraform-validate` (sans secret).

---

## 7. `.dockerignore` vide

**Symptôme** : `transferring context: 2B` pour `.dockerignore` lors du
build.

**Cause** : fichier absent ou vide. L'image ne contient pas `.env`
(le Dockerfile ne copie que `requirements.txt`, `dags/` et `src/`), mais
le contexte envoyé au moteur Docker l'incluait.

**Fix** : `.dockerignore` avec `.env`, `.git`, `**/__pycache__`,
`.pytest_cache`, `.ruff_cache`, `infra/terraform/.terraform`.
