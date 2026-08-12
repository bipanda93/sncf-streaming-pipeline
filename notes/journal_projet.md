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

---

## SILVER TEMPS RÉEL — DÉCISIONS ET VALIDATION

| # | Décision / Test | Détail |
|---|---|---|
| 22 | Extraction JSON : `from_json` avec schéma explicite (plutôt que `get_json_object` champ par champ) | Plus robuste, une seule passe de parsing, schéma Spark structuré reflétant la structure réelle observée dans les données SNCF (severity, messages, impacted_objects imbriqués) |
| 23 | Statuts conservés : `active`, `past` ET `future` (aucun filtre en Silver) | La profondeur historique est déjà couverte par la source régularité ; laisser Gold/Power BI décider du filtrage selon l'usage |
| 24 | Validation a posteriori de la décision #23 | Découverte d'un 3e statut (`future`, perturbations planifiées) jamais anticipé — répartition réelle : 74 future / 415 past / 130 active sur 619 lignes. Filtrer sur `active` seul en Silver aurait fait perdre silencieusement 79% des données |
| 25 | Dédoublonnage validé | 629 lignes Bronze -> 619 perturbations distinctes, 0 doublon restant confirmé (comptage `disruption_id` distincts = lignes totales) |

---

## SILVER HISTORIQUE — DÉCISIONS ET VALIDATION

| # | Décision / Test | Détail |
|---|---|---|
| 26 | Vérification préalable des schémas des 5 jeux, avant tout code | Confirme que les 5 jeux N'ONT PAS le même schéma (ex. `tgv_globale` = 3 colonnes sans notion de région ; `tgv_liaisons` = 26 colonnes avec détail des causes en %). Colonnes numériques définies explicitement par jeu (`NUMERIC_COLUMNS`), pas de détection automatique -- trop risqué vu l'hétérogénéité réelle |
| 27 | Outil : Spark (pas dbt) pour le Silver historique | Voir section "DÉCISION — Outil de transformation Silver pour la source historique" ci-dessus pour le raisonnement complet (risque de propagation d'erreur écarté : l'isolation vient de la séparation des jobs, pas du choix d'outil) |
| 28 | Validation complète sur les 5 jeux | TER 2366→2366 · Intercités 5949→5949 · TGV globale 138→138 · TGV axes 817→817 · TGV liaisons 12544→12544 — **21 814 lignes au total, 0 rejet** |

---

## GOLD — JOINTURE GÉOGRAPHIQUE COMPLÈTE (toutes gares + consolidation régionale)

**Évolution demandée** : geocoder TOUTES les gares affectées par une perturbation (pas seulement la première) -- permet de détecter qu'une perturbation touche plusieurs régions. Résultat : 51/130 perturbations touchent 2 régions ou plus (jusqu'à 4), une réalité complètement invisible avec l'approche "première gare seulement".

**Découverte majeure lors de la validation** : le jeu régularité TER mélange des noms de région d'AVANT et d'APRÈS la réforme territoriale du 01/01/2016 (l'historique remonte à 2013), plus 3 noms de réseaux commerciaux locaux sans statut de région (Etoile Amiens, Loire Océan, Sud Azur). 31 valeurs distinctes trouvées pour ce qui devrait être 13 régions actuelles.

**Décision** : construction d'un mapping `SNCF_TO_CURRENT_REGION` consolidant :
- Les anciennes régions pré-2016 vers leur région fusionnée actuelle (mapping basé sur la réforme officielle : ex. Alsace + Champagne-Ardenne + Lorraine -> Grand Est)
- Les variantes orthographiques des régions actuelles (SNCF n'utilise aucune convention de ponctuation cohérente : `Pays-de-la-Loire` mais `Provence Alpes Côte d'Azur`)
- Exclusion volontaire des 3 noms commerciaux (Etoile Amiens, Loire Océan, Sud Azur) -- leur rattacher une région précise aurait été une supposition géographique, pas une correspondance officielle vérifiable

**Résultat final** : 116/130 alertes avec au moins une région identifiée (89,2%), 115/130 avec un taux historique résolu. Le seul écart restant (Île-de-France) est un comportement correct, pas un bug : l'Île-de-France est opérée sous la marque Transilien, pas TER, donc absente par nature du jeu régularité TER.

**Enseignement méthodologique** : une hypothèse de départ raisonnable ("les régions SNCF sont stables depuis 2016") s'est révélée incomplète face aux vraies données -- l'historique remonte avant la réforme territoriale, invalidant l'hypothèse de stabilité. Découvert uniquement parce qu'un résultat "presque correct" (une région identifiée mais un taux NULL) a été creusé plutôt qu'accepté tel quel.
