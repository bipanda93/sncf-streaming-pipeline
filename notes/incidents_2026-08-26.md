# Journal des incidents — 26-27 août 2026

## 1. pipeline_complet_manuel — runs bloqués indéfiniment en "queued"

| | |
|---|---|
| **Contexte** | Tentative de rafraîchir les données avant de continuer Power BI. `pipeline_complet_manuel` déclenché plusieurs fois sans remarquer qu'un run précédent tournait encore -- 3 exécutions empilées (16:35, 16:41, 16:42) |
| **Symptôme** | Chacune des 3 exécutions crée, via `TriggerDagRunOperator`, un run d'`ingestion_bronze_temps_reel` -- les 3 restent bloqués en `queued`, `start_date` jamais renseignée, plus d'une heure sans progression. La tâche `trigger_ingestion_bronze` elle-même boucle indéfiniment ("Waiting for dag run to complete execution") |
| **Fausses pistes écartées, une par une** | CPU à 130% sur le conteneur Airflow (suspicion de saturation) -- `docker-compose restart` ramène le CPU à ~10%, mais ne débloque PAS les runs, écartant l'hypothèse ressources. Scheduler vérifié vivant (`jobs check` -- "Found one alive job"). Pool de créneaux vérifié non épuisé (128 slots libres sur `default_pool`) |
| **Cause racine** | Non identifiée avec certitude -- le comportement exact qui bloque des DagRuns créés via `TriggerDagRunOperator` en `queued` permanent, sans qu'aucune ressource ne soit épuisée, reste à creuser si le cas se reproduit |
| **Résolution** | `docker-compose down` puis `up -d` (recréation complète du conteneur, pas un simple `restart`) -- a eu pour effet indirect de vider entièrement la base de métadonnées Airflow, emportant avec elle les runs bloqués. Un déclenchement direct d'`ingestion_bronze_temps_reel` (sans passer par le wrapper) a ensuite réussi normalement |
| **Décision** | `pipeline_complet_manuel` mis de côté jusqu'à nouvel ordre -- deuxième incident réel le concernant (après celui du 17/08 sur `deferrable=True`). Déclenchement direct des DAGs individuels préféré en attendant |
| **Incident de process, au passage** | Plusieurs tentatives de diagnostic via CLI ont échoué sur la syntaxe (`-d`, `--dag-id` refusés) avant de découvrir que `dag_id` est un argument positionnel dans cette version (`airflow dags list-runs <dag_id> --state ...`). Cohérent avec les autres changements syntaxiques déjà rencontrés sur Airflow 3.x ce projet |

## 2. Base de métadonnées Airflow non persistante

| | |
|---|---|
| **Découverte** | En tentant de résoudre l'incident 1 via `docker-compose down` + `up -d`, le déclenchement suivant échoue : `sqlalchemy.exc.OperationalError: no such table: dag` -- la base SQLite interne d'Airflow (utilisateurs, historique des runs, état pause/actif des DAGs) a été entièrement recréée vide |
| **Cause racine** | `docker-compose.yml` ne montait aucun volume pour cette base -- elle vivait uniquement à l'intérieur du conteneur. `docker-compose restart` (redémarre le même conteneur) la préserve ; `docker-compose down` + `up` (détruit et recrée le conteneur) l'efface entièrement -- une distinction non anticipée avant ce jour |
| **Conséquence en cascade** | Les 6 DAGs réapparaissent après `airflow db migrate`, mais tous en pause par défaut (à réactiver manuellement). Le compte admin doit être recréé (`airflow users create`) |
| **Complication supplémentaire** | Airflow 3.x utilise `SimpleAuthManager` pour l'écran de connexion réel -- indépendant des comptes créés via `airflow users create` (mécanisme FAB classique). `SimpleAuthManager` génère son propre mot de passe aléatoire dans `simple_auth_manager_passwords.json.generated`, régénéré à chaque fois qu'aucun admin n'est trouvé au démarrage -- déjà découvert le 17/08, redécouvert ici pour la même cause racine (base vidée) |
| **Correction proposée** | Volume dédié pour la base (`./airflow_metadata:/opt/airflow/persistent_db` + `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` pointant dessus) -- rédigée mais **non encore appliquée** par l'utilisateur au moment de la rédaction de cet incident |
| **Statut** | Ouvert -- la prochaine fois qu'un `docker-compose down`/`up` sera nécessaire, le même problème se reproduira tant que la correction n'est pas effectivement appliquée |

## Résultat final malgré les deux incidents

Les données ont bien été rafraîchies : Bronze 3311 → 6044 lignes, Silver → 2862 perturbations distinctes, Gold realtime_alerts → 295 alertes, Gold disruption_context → 295 (92% géolocalisées). Export Power BI relancé avec succès sur les 5 fichiers.
