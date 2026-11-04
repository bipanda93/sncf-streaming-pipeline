# Deux voies de déploiement : Azure ou serveur privé

Le déploiement continu est la limite assumée du sujet (voir [CI/CD](ci_cd.md)). Pour aller plus loin, deux voies restent ouvertes et la décision n'est pas prise : continuer sur Azure, ou déployer sur un serveur privé (VPS). Cette page compare les deux composant par composant, à partir de ce que le code et l'infrastructure font réellement aujourd'hui.

## Ce qui rend le choix possible

Le pipeline est portable par configuration. Les mêmes variables d'environnement choisissent la cible, sans changer le code :

- **Messagerie** : sans protocole SASL, le client Kafka se connecte à un broker Kafka ordinaire ; avec SASL, à Azure Event Hubs (`src/ingestion/config.py`).
- **Stockage** : sans nom de compte de stockage, les tables Delta s'écrivent sur le disque (`DELTA_BASE_PATH`) ; avec un nom de compte, sur Azure Data Lake Storage (`abfss://`).
- **Secrets** : lus dans Azure Key Vault seulement en mode SASL ; sinon dans les variables d'environnement.

`docker-compose.yml` définit déjà Kafka (image officielle `apache/kafka`), Kafka UI et Airflow. C'est la pile sur laquelle le pipeline tourne en développement.

## Comparaison composant par composant

| Composant | Voie A : Azure | Voie B : serveur privé | Ce qui change avec le serveur |
|---|---|---|---|
| Messagerie | Event Hubs (protocole Kafka, SASL) | Kafka en conteneur | Code inchangé ; persistance du volume et sauvegardes à gérer soi-même |
| Stockage Delta | Data Lake Gen2 (conteneurs Bronze, Silver, Gold) | Disque du serveur | Code inchangé ; espace disque, sauvegardes et redondance à gérer soi-même |
| Calcul Spark | Databricks provisionné, accès bloqué, non utilisé (décision du 14/09) | Spark dans le conteneur Airflow | Identique à l'existant : le pipeline ne s'exécutait déjà pas sur Databricks |
| Orchestration | Airflow sur AKS (Helm, KubernetesExecutor), validé le 12/09 | Airflow en Docker Compose | Plus de pods ni d'isolation par tâche ; un seul nœud, comme le cluster de test |
| Secrets | Key Vault, une policy par identité | Variables d'environnement ou fichier protégé | Plus d'audit ni de rotation centralisés |
| Registre d'images | Container Registry | Build sur le serveur, ou registre externe | Non testé |
| Monitoring | Prometheus et Grafana (managés par Azure, puis déployés sur AKS) | Prometheus et Grafana en conteneurs | À ajouter : ils ne figurent pas dans `docker-compose.yml` aujourd'hui |
| Réseau et sécurité | Réseau virtuel, groupes de sécurité, points de terminaison privés | Pare-feu du serveur, accès SSH par clé, ports non exposés | Travail de sécurité nouveau : Azure le portait jusqu'ici |
| Infrastructure as code | Terraform (`infra/terraform/`, 13 fichiers) | Docker Compose, éventuellement un script de provisionnement | Terraform Azure ne s'applique plus ; il reste dans le dépôt comme preuve du déploiement du 10/09 |
| Restitution | Power BI : Fabric et Desktop | Dashboard Streamlit lit les tables locales ; export Parquet pour Power BI Desktop | Impact faible sur Power BI, à vérifier pour Fabric |
| CI/CD | `apply` depuis GitHub, state distant, authentification OIDC | Job GitHub Actions qui met à jour les conteneurs par SSH | Plus simple, mais n'existe pas encore |

## Ce que chaque voie démontre

**Voie A.** Elle prolonge le choix initial d'Azure, aligné sur les employeurs ciblés (voir [décisions d'architecture](notes/decisions_architecture.md)). Elle démontre l'infrastructure as code complète et un CD de bout en bout. Elle coûte tant que les services existent, et elle est surdimensionnée pour environ 1 700 perturbations actives.

**Voie B.** Elle démontre le dimensionnement adapté au besoin (pic mesuré de 6,8 Go de mémoire pour Spark, serveur de 16 Go) et la portabilité du pipeline : la même architecture, validée sur Azure, tourne sur une infrastructure simple. Elle demande de reprendre à sa charge ce qu'Azure fournissait : sauvegardes, sécurité réseau, supervision, redémarrages.

Les deux voies ne s'excluent pas dans le récit du mémoire : l'architecture a été validée sur Azure (10/09/2026), puis son exploitation durable est proposée sur une infrastructure proportionnée.

## Critères de décision

- **Coût** : factures du 10 au 14 septembre sur Azure, à chiffrer ; prix d'un serveur de 16 Go.
- **Temps** : la mise en place du serveur est du travail pris sur la rédaction du mémoire.
- **Valeur pour le jury et les recruteurs** : un CD complet sur Azure, ou un déploiement proportionné et argumenté.
- **Besoin réel** : l'entraînement de l'Isolation Forest n'exige pas de production, seulement 50 à 100 exécutions accumulées par Airflow.

## Statut

Aucune des deux voies n'est retenue. Aucun déploiement automatique n'existe encore, quelle que soit la voie.
