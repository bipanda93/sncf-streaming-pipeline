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

## PROCHAINES ÉTAPES

- [ ] Confirmer le test #15 (téléchargement réel complet via le client Python)
- [ ] `historical_loader.py` — chargement du CSV vers Delta Bronze historique
- [ ] Couche Silver (dédoublonnage temps réel + typage)
- [ ] Couche Gold (KPIs, jointure avec l'historique)
