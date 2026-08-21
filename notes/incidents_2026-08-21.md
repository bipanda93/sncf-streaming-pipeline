# Journal des incidents — 21 août 2026

**L'incident le plus impactant du projet à ce jour.**

## 1. Doublon apparent du DAG silver_gold_temps_reel (fausse alerte)

| | |
|---|---|
| **Symptôme** | `airflow dags list` (CLI) affiche `silver_gold_temps_reel` deux fois, alors que l'interface web n'en montre qu'un seul |
| **Diagnostic** | `dags list-import-errors` ne révèle aucune erreur ; l'interface web confirme `Dernière version du Dag : v2` |
| **Cause** | Système de versionnage des DAGs d'Airflow 3.x -- un artefact d'affichage CLI lors de la transition entre deux versions du fichier (juste après l'ajout des tâches `pipeline_metrics`/`anomaly_detector`), sans conséquence réelle |
| **Résolution** | Aucune action nécessaire -- confirmé fausse alerte, l'interface web fait foi |

## 2. Échec transitoire de silver_transform (résolu à la ré-exécution)

| | |
|---|---|
| **Symptôme** | Le run planifié automatique de 08h00 échoue sur `silver_transform`, avec cascade `upstream_failed` sur les 4 tâches suivantes |
| **Diagnostic** | Relance manuelle (`dags test`) : succès total, 2261 perturbations traitées sans erreur |
| **Cause probable, reliée à l'incident n°3 ci-dessous** | Très probablement un redémarrage du Mac survenu en plein milieu du run planifié -- cohérent avec la découverte, plus tard dans la journée, que `/tmp` est vidé à chaque redémarrage |
| **Résolution** | Résolue de fait par la correction de l'incident n°3 (déplacement du stockage hors de `/tmp`) |

## 3. Perte de données silencieuse — stockage sous /tmp vidé par macOS

| | |
|---|---|
| **Symptôme initial** | `export_for_powerbi.py` échoue : `PATH_NOT_FOUND: /tmp/delta/gold/realtime_alerts` -- une table vérifiée avec succès le matin même (316 lignes, 11h59) n'existe plus le soir |
| **Diagnostic** | `ls -la /tmp/delta/` révèle que le dossier entier a été recréé à 19h00-19h01 -- seuls `bronze/` et `checkpoints/` existent, `silver/` et `gold/` ont complètement disparu |
| **Cause racine** | Sur macOS, `/tmp` (symlink vers `/private/tmp`) est intégralement vidé à chaque redémarrage du Mac -- comportement système documenté, pas un bug ponctuel. Toutes les données du projet (Bronze, Silver, Gold, métriques, audit log) vivaient sous `/tmp/delta/` depuis le 11/08 |
| **Portée réelle** | Bien plus large que le script d'export qui a révélé le problème -- explique très probablement pourquoi l'historique pour Isolation Forest ne dépassait jamais 2-3 points malgré `silver_gold_temps_reel` actif depuis le 17/08, et explique aussi l'incident n°2 ci-dessus |
| **Détection** | Repérée uniquement parce qu'un nouveau script (export Power BI) a échoué sur un chemin manquant -- sans ce déclencheur, le problème serait resté invisible |
| **Correction** | Chemin de base déplacé de `/tmp/delta` vers `<racine_projet>/data/delta`, calculé dynamiquement depuis l'emplacement de `config.py` -- fonctionne identiquement sur l'hôte et dans le conteneur Airflow. Montage Docker `/tmp/delta:/tmp/delta` devenu inutile, retiré. `data/` ajouté au `.gitignore` |
| **Perte de données** | Totale et irréversible sur toutes les données accumulées avant ce jour -- pipeline à relancer entièrement |

## Enseignement méthodologique

Le choix initial de `/tmp/delta` comme emplacement de stockage n'avait jamais
été questionné -- `/tmp` évoque intuitivement un espace "temporaire mais qui
reste là", hypothèse fausse sur macOS spécifiquement. Ce choix serait resté
sans conséquence visible sur un serveur Linux qui redémarre rarement --
c'est l'usage prolongé sur une machine de développement personnelle qui a
révélé le problème. Les incidents n°1 et n°2, pris isolément, semblaient
mineurs et sans lien ; reliés à l'incident n°3, ils forment une même cause
racine -- illustration de l'intérêt de ne pas classer un symptôme comme
"résolu" simplement parce qu'il ne se reproduit pas immédiatement.

## 4. Pagination Navitia incomplète — jusqu'à 21% des perturbations perdues silencieusement

| | |
|---|---|
| **Découverte** | En relançant l'ingestion après l'incident n°3, un warning déjà présent dans le code (mais jamais réellement déclenché jusqu'ici) s'active : `1000 perturbation(s) reçue(s) sur 1275/1305 au total` |
| **Cause** | `count=1000` avait été fixé comme "le maximum autorisé par Navitia" dès la conception -- vrai pour un seul appel, mais aucune boucle n'avait jamais été codée pour aller chercher les pages suivantes. Le volume réel de perturbations actives a dépassé ce seuil pour la première fois aujourd'hui |
| **Vérification préventive** | Documentation officielle Navitia consultée avant correction pour confirmer le nom exact du paramètre de pagination (`start_page`), plutôt que de deviner et risquer une boucle infinie ou des doublons |
| **Correction** | Boucle `while` dans `get_disruptions()`, incrémente `start_page` jusqu'à récupérer `total_result` perturbations, plafonnée à `MAX_PAGES=10` par sécurité |
| **Validation** | 1311 perturbations récupérées sur 1311 au total, 2 pages, aucun avertissement |
| **Incident de process** | La première tentative de correction a échoué silencieusement (fichier jamais réécrit) sans qu'aucune erreur ne s'affiche -- détecté uniquement parce que le message de log observé était identique à l'ancienne version. Rattrapé par une vérification explicite (`grep`) avant tout nouveau test, plutôt que de supposer que l'écriture avait réussi |
