# CI/CD

## Intégration continue

Chaque pull request vers `main` lance huit contrôles (`.github/workflows/ci.yml`), tous obligatoires pour fusionner (ruleset sur `main`, sans contournement possible) :

- **dag-tests** : les 6 DAGs Airflow se chargent sans erreur, un DAG par fichier, identifiants uniques (Airflow 3.3.2, comme l'image de production) ;
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

## Déploiement : limite assumée du sujet

Le déploiement continu (CD) est la limite du périmètre de ce projet, pour une raison de coût : un CD complet exige une infrastructure en fonctionnement, facturée en continu sur un compte étudiant à crédit limité. La chaîne a donc été déployée et validée une fois sur Azure : 30 ressources créées le 10/09/2026, 37 détruites le 14/09/2026 (infrastructure complétée entre-temps).

Le dimensionnement conforte ce choix : environ 1 700 perturbations actives et un pic mesuré de 6,8 Go de mémoire pour Spark, très en dessous des volumes pour lesquels Databricks et AKS sont conçus.

Ce qu'un CD complet apporterait, et ce qui le remplace aujourd'hui :

| Apport d'un CD complet | Ce qui existe aujourd'hui | Pourquoi pas davantage |
|---|---|---|
| Déploiement automatique à chaque fusion dans `main` | `apply` et `destroy` manuels, en local | Chaque déploiement crée des ressources facturées |
| State Terraform partagé et verrouillé (backend distant) | State local ; `deploy.yml` limité à un plan manuel | Un `apply` depuis GitHub perdrait le state (décision du 21/10) |
| Environnements dev et prod avec approbation | Variable `var.environment`, Docker Compose en développement | Dépend de la voie retenue (voir ci-dessous) |
| Publication automatique de l'image et mise à jour du cluster | `docker-build` vérifie l'image à chaque pull request, sans la publier | Le cluster n'existe plus, il a été détruit pour limiter les coûts |
| Tests après déploiement (fumée, intégration Spark sur infrastructure réelle) | Tests unitaires, tests des DAGs, build Docker, validation Terraform, sans service externe | Exigerait une infrastructure active à chaque exécution |
| Retour arrière et détection de dérive | `destroy` et `apply` rejouables depuis le code Terraform versionné | Nécessite un state partagé et une infrastructure active |
| Authentification sans secret (fédération OIDC GitHub vers Azure) | Aucun secret dans la CI ; secrets Azure uniquement en local | Utile seulement si GitHub déploie réellement sur Azure |
| Arrêt automatique pour limiter les coûts | Destruction manuelle après chaque validation | Dépend de l'existence d'un déploiement automatisé |

### Deux voies pour aller plus loin

Le choix n'est pas tranché. Deux voies restent ouvertes (comparaison détaillée, composant par composant, dans [Deux voies de déploiement](voies_deploiement.md)) :

| | Voie A : continuer sur Azure | Voie B : serveur privé (VPS) |
|---|---|---|
| Principe | Backend Terraform distant, déploiement automatique de l'infrastructure existante (AKS, Event Hubs, Databricks) | Docker Compose sur un serveur dimensionné au besoin mesuré (16 Go), sans Databricks ni AKS |
| Ce que le CD ajouterait | `apply` depuis GitHub avec state verrouillé, authentification OIDC, environnements dev et prod avec approbation | Job GitHub Actions qui met à jour les conteneurs du serveur après chaque fusion |
| Coût | Services facturés tant qu'ils existent ; un second environnement doublerait les coûts | Un serveur à coût fixe, sans service managé facturé à l'usage |
| Adéquation aux volumes | Surdimensionné pour environ 1 700 perturbations actives | Proportionné au pic de 6,8 Go mesuré |
| Statut | Non retenue pour l'instant | Non retenue pour l'instant : serveur à choisir |

La décision complète figure dans `notes/decisions_architecture.md` (2026-11-04).

