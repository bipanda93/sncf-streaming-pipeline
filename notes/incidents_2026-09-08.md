# Incidents 2026-09-08 — Bug DAX + Docker/Airflow

## 1. `Perturbations Aujourd'hui` affichait "--"

**Symptôme** : carte KPI Page 1 vide au lieu d'un chiffre.

**Cause** : mesure basée sur `TODAY()` littéral, alors que `sncf_disruptions`
n'est alimenté que par cycles de test Azure (infra créée/détruite), pas en
continu. Dès que la date du jour dépassait la dernière date réellement
présente dans les données, le filtre ne matchait aucune ligne.

**Fix** :
```dax
Perturbations Aujourd'hui = 
VAR DernierJour = MAX(sncf_disruptions[Jour])
RETURN
CALCULATE(
    DISTINCTCOUNT(sncf_disruptions[disruption_id]),
    sncf_disruptions[Jour] = DernierJour
)
```
Ancre la mesure sur le dernier jour réellement disponible plutôt que sur la
date calendaire — robuste peu importe quand le rapport est ouvert (soutenance).

**Point de vigilance non résolu** : la carte affiche maintenant 627, un
chiffre supérieur à "Perturbations Actives" (419). Plausible si Silver
(actives + closes) a eu un pic d'événements un jour de test particulier, mais
pas formellement vérifié avec `MAX(Jour)` + filtre visuel dédié. À confirmer
avant la soutenance.

---

## 2. Airflow ne résout pas `kafka` (DNS) — DAG `ingestion_bronze_temps_reel` en échec

**Symptôme** : tâche `kafka_to_bronze` échoue avec
`DNS resolution failed for kafka` / `No resolvable bootstrap urls given in bootstrap.servers`.

**Hypothèse initiale (fausse)** : deux stacks Docker séparées (conteneurs
Airflow orphelins aux noms auto-générés vus dans Docker Desktop vs. le vrai
groupe `sncf-streaming-...`) → réseaux Docker distincts.

**Vérification** : le `docker-compose.yml` réel montrait une config correcte
(`KAFKA_BOOTSTRAP_SERVERS: kafka:9092`, même fichier compose que Kafka) — donc
pas un problème d'architecture au niveau du fichier.

**Deuxième hypothèse testée et infirmée** : Kafka pas encore prêt au démarrage
(delay KRaft). Les timestamps ont montré Kafka up depuis 11 minutes avant
l'échec — largement suffisant, cette piste ne tenait pas.

**Résolution** : un `docker-compose down` + `up` complet a réglé la résolution
DNS. Cause exacte non confirmée avec certitude (probable état réseau
incohérent après plusieurs manipulations manuelles/redémarrages partiels
antérieurs, ou conteneurs orphelins qui polluaient le contexte réseau local).

**Leçon** : en cas de doute persistant sur l'état réseau Docker après
plusieurs manipulations manuelles, un down/up complet reste le diagnostic le
plus fiable — plus rapide que d'essayer de tracer la cause exacte quand
plusieurs hypothèses successives ne tiennent pas.

---

## 3. Persistance Airflow — dette technique enfin réglée

Ajout d'un volume nommé `airflow_home:/opt/airflow` dans le service `airflow`
du `docker-compose.yml`. Corrige la dette documentée depuis plusieurs
sessions : un `down` effaçait le compte admin et l'historique des DAG runs
faute de volume dédié pour l'état interne. Fait en même temps que le down/up
de résolution réseau (pas de perte de données supplémentaire).

**Règle à respecter désormais** : ne jamais faire `docker-compose down -v`
(le `-v` supprimerait aussi ce volume). Un `down` simple le préserve.

---

## 4. Cache Ivy — tentative, échec, fix (permissions Docker)

**Tentative** : volume nommé `ivy_cache` pour éviter de re-télécharger les ~19
dépendances Spark/Kafka/Delta à chaque tâche (passé de ~9s à plusieurs
minutes après le rebuild d'image du point 3).

**Échec** : `FileNotFoundException` à l'écriture dans le cache. Cause :
Docker crée les volumes nommés avec propriétaire `root`, alors que le
conteneur Airflow (image custom `Dockerfile.airflow`) tourne avec un
utilisateur non-root — écriture refusée dans le volume flambant neuf.

**Fix** : remplacement du volume nommé par un bind mount local
(`./.ivy_cache:/home/airflow/.ivy2.5.2`). Les bind mounts héritent des
permissions du dossier hôte sur Docker Desktop Mac, pas de ce problème de
propriétaire root.

**Vérifié** : le DAG `kafka_to_bronze` s'exécute avec succès après ce fix.

**Réutilisable pour Leclerc** : tout volume Docker destiné à un conteneur
custom non-root doit être un bind mount, pas un volume nommé, sauf besoin
explicite de laisser Docker gérer entièrement le stockage (comme
`airflow_home`, qui lui n'a posé aucun problème de permission).
