# CI/CD

## Intégration continue

Chaque pull request vers `main` lance huit contrôles (`.github/workflows/ci.yml`), tous obligatoires pour fusionner (ruleset sur `main`, sans contournement possible) :

- **dag-tests** : les 6 DAGs Airflow se chargent sans erreur, un DAG par fichier, identifiants uniques (Airflow 3.3.1, comme l'image de production) ;
- **unit-tests** : tests unitaires du client SNCF (mode mock, erreurs HTTP, pagination) et du producteur Kafka (enveloppe, clé, flush), sans réseau ni broker ;
- **lint** : analyse statique du code Python avec ruff ;
- **secret-scan** : recherche de secrets dans les commits avec Gitleaks ;
- **terraform-validate** : formatage et validation de la configuration Terraform, sans connexion à Azure ;
- **compose-validate** : validation de `docker-compose.yml` ;
- **docker-build** : construction de l'image Airflow et vérification de Java 17, PySpark, Delta et Airflow à l'intérieur ;
- **docs-build** : construction du site de documentation en mode strict, qui échoue sur un lien cassé.

Dependabot propose chaque semaine les mises à jour des dépendances Python, des actions GitHub, de l'image Docker et de Terraform.

Aucun contrôle ne dépend d'Azure, de Kafka ou de l'API SNCF, et aucun secret n'est à configurer : la CI tourne sur GitHub seul.

Le déploiement Terraform (`deploy.yml`) se limite à un plan déclenché manuellement : le state Terraform étant local, `apply` et `destroy` ne se font que depuis le poste de travail (voir `notes/decisions_architecture.md`, 2026-10-21).
