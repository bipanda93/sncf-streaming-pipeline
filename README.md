# Pipeline de Streaming Temps Réel SNCF

Architecture data engineering cloud-native pour l'ingestion, le traitement et la restitution des perturbations ferroviaires SNCF en temps réel, avec historique de régularité et détection d'anomalies.

**Auteur** : Bipanda Franck Ulrich — Mastère Data Engineering, Digital School de Paris

---

## Vue d'ensemble

Ce projet ingère en continu les données de perturbations du réseau SNCF (API Navitia), les traite via une architecture medallion (Bronze/Silver/Gold), et les restitue dans un dashboard Power BI interactif a 4 pages. Il combine streaming temps réel (Kafka) et historique batch (données de régularité 2013-2026) dans une même architecture.

## Chiffres clés

- 166 917 événements ingérés en continu (Bronze)
- 17 778 perturbations distinctes après dédoublonnage (Silver)
- 21 814 lignes d'historique de régularité (2013-2026)
- 6 DAGs Airflow en production
- 9 modules Terraform, 33 ressources Azure (déploiement continu depuis le 10/09)

## Architecture

API SNCF (Navitia) vers Kafka vers Bronze (Delta) vers Silver (dédoublonné) vers Gold (5 tables) vers Power BI

Architecture medallion complète, avec une table de pont (bridge table) pour gérer les relations multi-valeurs (une perturbation touche souvent plusieurs régions et gares a la fois). Diagramme détaillé : docs/schema_donnees.md

## Stack technique

| Couche | Outils |
|---|---|
| Ingestion | Kafka, Python |
| Traitement | PySpark, Delta Lake |
| Orchestration | Apache Airflow (6 DAGs) |
| Infrastructure | Terraform, Azure (Event Hubs, Databricks, AKS, ADLS Gen2) |
| Restitution | Power BI (architecture double : Fabric Direct Lake + Desktop Import) |
| IA | Isolation Forest (détection d'anomalies), Claude API (enrichissement) |
| CI/CD | GitHub Actions |

## Orchestration Airflow

6 DAGs en production :
- ingestion_bronze_temps_reel : ingestion Kafka vers Bronze, horaire
- silver_gold_temps_reel : Silver vers Gold vers export Power BI, toutes les 4h
- historique_mensuel : chargement de l'historique de régularité
- enrichissement_llm_mensuel : enrichissement via Claude API
- gouvernance_rgpd : politique de rétention et traçabilité
- pipeline_complet_manuel : exécution manuelle de bout en bout

## Dashboard Power BI

4 pages, style visuel cohérent (fond crème, accents dorés, texte marron) :
1. Vue d'ensemble : KPI temps réel (perturbations actives, durée moyenne, taux historique)
2. Analyse géographique : carte choroplèthe interactive de France, segment région vers gares
3. Types de perturbations : répartition par niveau d'alerte et sévérité
4. Historique et ponctualité : évolution temporelle, comparaison par type de transport

Modélisation DAX avancée, notamment le pattern TREATAS pour filtrer entre tables sans relation physique directe (nécessaire car une perturbation peut toucher plusieurs régions/gares simultanément).

## Défis techniques résolus

- Corruption de checkpoint Kafka après arrêt brutal, diagnostiquée et résolue
- Perte silencieuse de données via /tmp non persistant, corrigée
- Conflits de transaction Delta Lake liés a la concurrence des DAGs
- Audit qualité de données : détection d'un mélange de 3 granularités géographiques dans l'historique de ponctualité, quantification d'un risque de double comptage (~18% sur l'échantillon testé), corrigé par filtrage explicite
- Incident de sécurité : exposition accidentelle d'un secret Azure dans un commit, révoqué immédiatement et documenté
- Migration Kafka vers Azure Event Hubs : clé d'authentification en écriture confondue avec la clé en lecture seule (erreur TOPIC_AUTHORIZATION_FAILED), diagnostiquée et corrigée en séparant les deux usages dans la configuration
- CI/CD Terraform : deux outils locaux conteneurisés (Azure CLI et Terraform) reposant sur des mécanismes de credentials distincts, source d'une confusion de diagnostic résolue en clarifiant l'architecture d'outillage locale

Détail complet de chaque incident : dossier notes/

## Décisions d'architecture documentées

Chaque choix structurant est justifié et tracé dans notes/decisions_architecture.md, notamment le choix d'une architecture double Power BI, l'usage de tables de pont plutôt qu'un schéma en étoile classique, et le choix de TREATAS plutôt qu'une relation bidirectionnelle en DAX.

## Limites connues

- AKS et Databricks : infrastructure créée et déployée en continu, mais sans charge de travail active pour l'instant (Event Hubs, lui, est utilisé et testé en conditions réelles de bout en bout)
- ADLS Gen2 créé côté infrastructure, migration du code applicatif encore à faire (le stockage Delta local reste la source active)
- Isolation Forest pas encore entraîné (attend l'accumulation d'historique)
- Certains sous-réseaux TER historiques nécessitent encore une correspondance fine vers les régions actuelles

## Lancer le projet

Nécessite un fichier .env avec les identifiants Kafka et le token API SNCF, puis : docker-compose up -d
