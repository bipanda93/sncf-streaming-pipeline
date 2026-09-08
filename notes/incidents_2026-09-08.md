# Journal des incidents — 8 septembre 2026

## Cache Ivy créé par root, bloquant les DAGs Airflow

| | |
|---|---|
| **Symptôme** | `ingestion_bronze_temps_reel` échoue en boucle -- FileNotFoundException puis Permission denied sur `.ivy2.5.2/cache` |
| **Contexte** | Conteneur Airflow relancé (CREATED 23 min, Up 12 min -- redémarrage intermédiaire détecté), 2 semaines de fonctionnement sans incident avant ce jour |
| **Cause probable** | `.ivy2.5.2` créé par `root:root` pendant la séquence de démarrage du conteneur, avant que Spark (tournant en `airflow`) n'ait pu le créer lui-même -- dossier non persistant, comportement dépendant de l'ordre exact du démarrage. Cause exacte non confirmée sans logs de démarrage du conteneur |
| **Correction** | `chown -R airflow:root /home/airflow/.ivy2.5.2` (le compte airflow appartient au groupe `root`, gid=0, pas à un groupe "airflow" séparé) |
| **Lien avec dette connue** | Même famille que la base de métadonnées Airflow non persistante -- vrai correctif de fond identique (volume dédié pour le home directory), toujours en attente |

## Panne DNS Docker — résolution api.sncf.com échoue

| | |
|---|---|
| **Symptôme** | `sncf_to_kafka` échoue -- NameResolutionError sur api.sncf.com, puis le démon Docker lui-même devient injoignable (500 Internal Server Error sur docker.sock) |
| **Contexte** | ~22h34, plusieurs heures après l'incident Ivy du matin -- panne distincte, pas une suite |
| **Cause** | Instabilité de Docker Desktop sur macOS, cause exacte non identifiée -- le démon ne répondait plus correctement aux commandes exec |
| **Correction** | Redémarrage complet de la machine -- DNS résolu immédiatement après (`76.223.85.109`), conteneurs relancés par `docker-compose up -d` |
| **Point rassurant** | Contrairement à l'incident du matin, les conteneurs ont redémarré sans être recréés (`CREATED 9 hours ago` conservé) -- permissions `.ivy2.5.2` non affectées cette fois |
