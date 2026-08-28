
## 3. ConcurrentTransactionException -- max_active_runs=3 incompatible avec l'architecture streaming+checkpoint

| | |
|---|---|
| **Contexte** | `max_active_runs` passé de 1 à 3 pour absorber le zombie de l'incident précédent -- semblait raisonnable a priori |
| **Symptôme** | Le run planifié de 19h00 (retenté automatiquement, `retries=2`) et un déclenchement manuel à 19h04 ont tourné en même temps -- Delta Lake a rejeté la deuxième écriture (`ConcurrentTransactionException`), les deux visant le même fichier de checkpoint |
| **Cause racine** | Une tâche de streaming avec checkpoint est structurellement un seul écrivain à la fois -- augmenter `max_active_runs` sur CE type de DAG n'apporte jamais de parallélisme utile, seulement un risque de conflit |
| **Correction** | Retour à `max_active_runs=1`. Conséquence acceptée : un déclenchement manuel pendant qu'un run planifié tourne active sera mis en `queued`, pas exécuté en parallèle -- comportement voulu, pas une limite à contourner |
| **Résolu en même temps** | `failOnDataLoss=false` (voir incident n°... plus haut) a permis au run suivant de repartir malgré la discontinuité d'offset Kafka -- Bronze remonté à 11396 lignes, 21h21 |
