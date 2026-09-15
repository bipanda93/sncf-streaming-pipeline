# Incidents 2026-09-14 — Dashboard Streamlit (santé des données)

## 1. `ModuleNotFoundError` — `streamlit run` ne résout pas les chemins comme `python3 -m`

**Symptôme** : `from src.ingestion import config` échoue avec
`ModuleNotFoundError: No module named 'src'`, alors même que le script
est lancé depuis la racine du projet.

**Cause** : `streamlit run chemin/script.py` ajoute le **dossier du
script** (`src/`) au chemin de recherche Python, pas le dossier courant
d'où la commande est lancée — comportement différent de `python3 -m
src.xxx`, déjà rencontré et compris pour Airflow. Python cherche donc un
module `src` à l'intérieur de `src/`, qui n'existe pas.

**Fix** : insertion explicite de la racine du projet en tout début de
fichier, avant l'import concerné —
`sys.path.insert(0, str(Path(__file__).resolve().parent.parent))`.

---

## 2. `AttributeError: 'str' object has no attribute 'tzinfo'` — types hétérogènes entre tables Delta

**Symptôme** : le calcul de fraîcheur plante sur certaines tables, pas
toutes — `last_write.tzinfo` échoue car `last_write` est une chaîne de
texte pour certaines colonnes, un vrai `datetime` pour d'autres.

**Cause** : les colonnes d'horodatage ne sont pas toutes du même type
sous-jacent selon la table (confirmé empiriquement : `sncf_raw` retourne
une string ISO complète, d'autres tables un vrai `datetime`). Le premier
code supposait un seul type partout.

**Fix** : remplacement par `pd.Timestamp(last_write)`, qui parse
indifféremment string/date/datetime, plutôt que de traiter chaque type
séparément.

---

## 3. Découverte opérationnelle réelle, via le dashboard lui-même

Une fois fonctionnel, le dashboard a révélé un vrai décalage dans le
pipeline, invisible depuis Grafana (infra seule) ou Power BI (données
finies) : `sncf_raw` (Bronze) repassé "Fraîche" après reprise de
l'ingestion, mais `sncf_disruptions` et `disruption_context` restés
"À surveiller" (dernière écriture 13/09) — le maillon Bronze→Silver ne
s'était pas encore rattrapé malgré la reprise de l'ingestion brute.
Confirme la valeur de l'outil au-delà du simple exercice technique : il
détecte un vrai problème de fraîcheur intermédiaire dans le pipeline dès
sa première utilisation réelle.

## 4. Instabilité Docker Desktop — `disruption_context` échoue en résolution DNS

**Symptôme** : `gold_disruption_context` (DAG `silver_gold_temps_reel`, local)
échoue avec `socket.gaierror: [Errno -3] Temporary failure in name
resolution` sur `ressources.data.sncf.com` — un appel réseau externe
(open data SNCF), pas une ressource Azure.

**Cause** : Docker Desktop avait déjà montré un signe d'instabilité plus
tôt le matin même (`terraform state list` avait échoué avec `502 Bad
Gateway` / `unexpected EOF`), sans lien avec ce DAG. Résolution DNS
interne au conteneur compromise pendant cette même fenêtre d'instabilité.

**Fix** : redémarrage de Docker Desktop. Vérifié avant de retenter :
`docker exec sncf-airflow python3 -c "socket.gethostbyname(...)"` renvoie
une IP valide. Retry de la tâche réussi.

**Leçon** : quand plusieurs incidents apparemment sans rapport
surviennent dans une même fenêtre de temps (un échec Terraform local le
matin, un échec DNS dans un conteneur l'après-midi), chercher une cause
d'infrastructure locale partagée avant de diagnostiquer chaque symptôme
séparément.

---

## 5. Clé API Claude — plusieurs échecs de copier-coller en chaîne

**Contexte** : ajout de `ANTHROPIC_API_KEY` pour débloquer
`enrichissement_llm_mensuel`, jamais configurée jusqu'ici.

**Symptôme initial** : `TypeError: Could not resolve authentication
method` — clé absente de `.env` ET jamais transmise dans
`docker-compose.yml` (aucune des deux n'existait). Root cause simple,
fonctionnalité jamais câblée depuis le début du projet.

**Cascade d'échecs en tentant de la configurer** : au moins 4 tentatives
successives où le texte d'une commande shell (`echo "..."`, un `grep`,
un `sed`) s'est retrouvé collé dans `.env` à la place de la vraie clé —
même famille d'incident que le jeton ACR du 12/09, mais répété plusieurs
fois d'affilée cette fois, y compris après vérification explicite du
presse-papier entre chaque tentative (`pbpaste` confirmant "vide" ou "ça
a l'air bon" juste avant l'échec suivant).

**Fix définitif** : abandon des chaînes de commandes en aveugle,
basculement vers `nano` — éditeur visuel où la valeur collée est
directement lisible à l'écran avant sauvegarde. Résolu du premier coup
une fois cette méthode adoptée.

**Leçon** : au-delà d'un certain nombre d'échecs répétés sur le même
geste (copier-coller via chaîne de commandes), le bon réflexe n'est pas
de re-vérifier le presse-papier une fois de plus mais de changer de
méthode pour une où l'erreur est visible avant validation, pas après.

---

## 6. API Claude accessible mais facturation non configurée

**Une fois la clé correctement transmise** (confirmé : préfixe `sk-ant-`
reçu par le conteneur), `delay_analyzer` échoue différemment :
`anthropic.BadRequestError: 400 - 'Your credit balance is too low to
access the Anthropic API'`.

**Diagnostic** : ce n'est plus un problème d'authentification (la clé
est acceptée, la requête atteint bien `api.anthropic.com`) mais de
facturation — l'API Claude n'a pas d'offre gratuite, un moyen de
paiement/des crédits doivent être ajoutés sur `platform.claude.com`
avant le moindre appel, même pour tester.

**État laissé tel quel** : authentification confirmée fonctionnelle,
facturation non activée — `enrichissement_llm_mensuel` reste bloqué
jusqu'à l'ajout de crédits, décision reportée.

---

## 7. `historique_mensuel` — contention ressources sur 5 jobs Spark parallèles

**Symptôme** : les 5 tâches `silver_*` (une par dataset historique)
échouent systématiquement, avec des logs s'arrêtant net juste après "Pre
Execute" — aucun traceback Python, aucun message d'erreur applicatif.

**Diagnostic** : toutes les tâches `silver_*` démarrent à la même
seconde exacte (visible dans l'UI Airflow) — 5 jobs Spark lancés en
parallèle dans le même conteneur `standalone`, qui partagent la même
enveloppe de ressources. Log vide sans traceback = signature typique
d'un processus tué de l'extérieur (SIGKILL), pas un bug de code.

**Confirmation** : `historical_silver_transform --dataset tgv_axes`
relancé en isolation manuelle (`docker exec`, sans concurrence) — réussi
sans erreur, 817 lignes traitées. Confirme que le code est sain, seule
l'exécution simultanée pose problème.

**Fix** : pool Airflow dédié (`spark_silver_pool`, 1 slot), assigné aux 5
tâches `silver_*` via l'argument `pool=` — garantit qu'une seule tâche
Spark de ce type tourne à la fois, sans changer l'ordre logique du DAG.

**Non confirmé de bout en bout** : le scheduler Airflow local s'est
révélé mort en cours de route (voir point 8) avant qu'un run complet
avec le pool en place n'ait pu être observé jusqu'à son terme. Le fix
est appliqué et logiquement correct, mais sa validation complète reste
à faire à la prochaine session.

---

## 8. Scheduler Airflow local mort silencieusement

**Symptôme** : après le fix du pool, les 5 tâches `silver_*` restent
indéfiniment en "Aucun statut" (pas même "en file d'attente") — y
compris `gold_punctuality_trends`, qui pourtant ne dépend pas du pool.
Une tâche `running` introuvable en base.

**Diagnostic** : `airflow jobs check --job-type SchedulerJob` → "No
alive jobs found." Le scheduler lui-même n'était plus actif dans le
conteneur — explique pourquoi même les tâches sans lien avec le pool ne
progressaient plus.

**Cause probable** : même famille d'instabilité Docker Desktop que le
point 4, en cascade jusqu'ici.

**Fix** : `docker-compose restart airflow`. **Non revérifié avant la
suite de la session** (destroy Azure lancé juste après) — à confirmer en
priorité à la prochaine ouverture.

---

## 9. Destroy Azure — aucun coffre en soft-delete trouvé, contrairement à l'attendu

**Contexte** : fin de la Phase 2, `terraform destroy` lancé pour limiter
les coûts avant la soutenance — 37 ressources détruites avec succès.

**Surprise** : `az keyvault purge --name kv-sncf-dev` →
`No deleted Vault or HSM was found`. Vérifié plus largement : `az
keyvault list-deleted` (sans filtre, tout l'abonnement) renvoie une
liste **complètement vide** — aucun coffre en soft-delete nulle part,
pas seulement celui de ce projet.

**Contredit une hypothèse documentée depuis le 12/09** (le risque de
réservation de nom pendant 90 jours après suppression). Cause exacte non
identifiée avec certitude — possible spécificité de l'abonnement
étudiant, ou mécanisme différent de ce qui était anticipé.

**Conséquence pratique, positive** : le prochain `terraform apply` avant
la soutenance devrait pouvoir recréer `kv-sncf-dev` sans blocage de nom
réservé — le risque redouté ne s'est pas matérialisé, contrairement à ce
qui était prévu.

## 4. Instabilité Docker Desktop — `disruption_context` échoue en résolution DNS

**Symptôme** : `gold_disruption_context` (DAG `silver_gold_temps_reel`, local)
échoue avec `socket.gaierror: [Errno -3] Temporary failure in name
resolution` sur `ressources.data.sncf.com` — un appel réseau externe
(open data SNCF), pas une ressource Azure.

**Cause** : Docker Desktop avait déjà montré un signe d'instabilité plus
tôt le matin même (`terraform state list` avait échoué avec `502 Bad
Gateway` / `unexpected EOF`), sans lien avec ce DAG. Résolution DNS
interne au conteneur compromise pendant cette même fenêtre d'instabilité.

**Fix** : redémarrage de Docker Desktop. Vérifié avant de retenter :
`docker exec sncf-airflow python3 -c "socket.gethostbyname(...)"` renvoie
une IP valide. Retry de la tâche réussi.

**Leçon** : quand plusieurs incidents apparemment sans rapport
surviennent dans une même fenêtre de temps (un échec Terraform local le
matin, un échec DNS dans un conteneur l'après-midi), chercher une cause
d'infrastructure locale partagée avant de diagnostiquer chaque symptôme
séparément.

---

## 5. Clé API Claude — plusieurs échecs de copier-coller en chaîne

**Contexte** : ajout de `ANTHROPIC_API_KEY` pour débloquer
`enrichissement_llm_mensuel`, jamais configurée jusqu'ici.

**Symptôme initial** : `TypeError: Could not resolve authentication
method` — clé absente de `.env` ET jamais transmise dans
`docker-compose.yml` (aucune des deux n'existait). Root cause simple,
fonctionnalité jamais câblée depuis le début du projet.

**Cascade d'échecs en tentant de la configurer** : au moins 4 tentatives
successives où le texte d'une commande shell (`echo "..."`, un `grep`,
un `sed`) s'est retrouvé collé dans `.env` à la place de la vraie clé —
même famille d'incident que le jeton ACR du 12/09, mais répété plusieurs
fois d'affilée cette fois, y compris après vérification explicite du
presse-papier entre chaque tentative (`pbpaste` confirmant "vide" ou "ça
a l'air bon" juste avant l'échec suivant).

**Fix définitif** : abandon des chaînes de commandes en aveugle,
basculement vers `nano` — éditeur visuel où la valeur collée est
directement lisible à l'écran avant sauvegarde. Résolu du premier coup
une fois cette méthode adoptée.

**Leçon** : au-delà d'un certain nombre d'échecs répétés sur le même
geste (copier-coller via chaîne de commandes), le bon réflexe n'est pas
de re-vérifier le presse-papier une fois de plus mais de changer de
méthode pour une où l'erreur est visible avant validation, pas après.

---

## 6. API Claude accessible mais facturation non configurée

**Une fois la clé correctement transmise** (confirmé : préfixe `sk-ant-`
reçu par le conteneur), `delay_analyzer` échoue différemment :
`anthropic.BadRequestError: 400 - 'Your credit balance is too low to
access the Anthropic API'`.

**Diagnostic** : ce n'est plus un problème d'authentification (la clé
est acceptée, la requête atteint bien `api.anthropic.com`) mais de
facturation — l'API Claude n'a pas d'offre gratuite, un moyen de
paiement/des crédits doivent être ajoutés sur `platform.claude.com`
avant le moindre appel, même pour tester.

**État laissé tel quel** : authentification confirmée fonctionnelle,
facturation non activée — `enrichissement_llm_mensuel` reste bloqué
jusqu'à l'ajout de crédits, décision reportée.

---

## 7. `historique_mensuel` — contention ressources sur 5 jobs Spark parallèles

**Symptôme** : les 5 tâches `silver_*` (une par dataset historique)
échouent systématiquement, avec des logs s'arrêtant net juste après "Pre
Execute" — aucun traceback Python, aucun message d'erreur applicatif.

**Diagnostic** : toutes les tâches `silver_*` démarrent à la même
seconde exacte (visible dans l'UI Airflow) — 5 jobs Spark lancés en
parallèle dans le même conteneur `standalone`, qui partagent la même
enveloppe de ressources. Log vide sans traceback = signature typique
d'un processus tué de l'extérieur (SIGKILL), pas un bug de code.

**Confirmation** : `historical_silver_transform --dataset tgv_axes`
relancé en isolation manuelle (`docker exec`, sans concurrence) — réussi
sans erreur, 817 lignes traitées. Confirme que le code est sain, seule
l'exécution simultanée pose problème.

**Fix** : pool Airflow dédié (`spark_silver_pool`, 1 slot), assigné aux 5
tâches `silver_*` via l'argument `pool=` — garantit qu'une seule tâche
Spark de ce type tourne à la fois, sans changer l'ordre logique du DAG.

**Non confirmé de bout en bout** : le scheduler Airflow local s'est
révélé mort en cours de route (voir point 8) avant qu'un run complet
avec le pool en place n'ait pu être observé jusqu'à son terme. Le fix
est appliqué et logiquement correct, mais sa validation complète reste
à faire à la prochaine session.

---

## 8. Scheduler Airflow local mort silencieusement

**Symptôme** : après le fix du pool, les 5 tâches `silver_*` restent
indéfiniment en "Aucun statut" (pas même "en file d'attente") — y
compris `gold_punctuality_trends`, qui pourtant ne dépend pas du pool.
Une tâche `running` introuvable en base.

**Diagnostic** : `airflow jobs check --job-type SchedulerJob` → "No
alive jobs found." Le scheduler lui-même n'était plus actif dans le
conteneur — explique pourquoi même les tâches sans lien avec le pool ne
progressaient plus.

**Cause probable** : même famille d'instabilité Docker Desktop que le
point 4, en cascade jusqu'ici.

**Fix** : `docker-compose restart airflow`. **Non revérifié avant la
suite de la session** (destroy Azure lancé juste après) — à confirmer en
priorité à la prochaine ouverture.

---

## 9. Destroy Azure — aucun coffre en soft-delete trouvé, contrairement à l'attendu

**Contexte** : fin de la Phase 2, `terraform destroy` lancé pour limiter
les coûts avant la soutenance — 37 ressources détruites avec succès.

**Surprise** : `az keyvault purge --name kv-sncf-dev` →
`No deleted Vault or HSM was found`. Vérifié plus largement : `az
keyvault list-deleted` (sans filtre, tout l'abonnement) renvoie une
liste **complètement vide** — aucun coffre en soft-delete nulle part,
pas seulement celui de ce projet.

**Contredit une hypothèse documentée depuis le 12/09** (le risque de
réservation de nom pendant 90 jours après suppression). Cause exacte non
identifiée avec certitude — possible spécificité de l'abonnement
étudiant, ou mécanisme différent de ce qui était anticipé.

**Conséquence pratique, positive** : le prochain `terraform apply` avant
la soutenance devrait pouvoir recréer `kv-sncf-dev` sans blocage de nom
réservé — le risque redouté ne s'est pas matérialisé, contrairement à ce
qui était prévu.
