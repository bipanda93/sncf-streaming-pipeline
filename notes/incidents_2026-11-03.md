# Incidents 2026-11-03 — Tests unitaires, notification Slack et poste local

## 1. `pytest` : `ModuleNotFoundError: No module named 'src'`

**Symptôme** : les nouveaux tests échouent à l'import de `src.ingestion...`
dès qu'on lance `pytest tests/`.

**Cause** : `pytest` ne place pas la racine du dépôt dans le chemin de
recherche des modules.

**Fix** : un fichier `pytest.ini` à la racine avec `pythonpath = .`.

**Leçon retenue** : un test qui passe sur un poste mais pas en CI vient
presque toujours du chemin d'import ou des dépendances ; déclarer le
chemin dans la configuration plutôt que dans l'environnement du poste.

---

## 2. `ModuleNotFoundError: confluent_kafka` en local et dépendances du job `unit-tests`

**Symptôme** : les tests du producteur Kafka échouent faute du module
`confluent_kafka`.

**Cause** : le module n'était pas installé dans l'environnement local, et
le job CI doit installer lui-même ses dépendances.

**Fix** : le job `unit-tests` installe uniquement les dépendances utiles
(`requests`, `python-dotenv`, `confluent-kafka`, `pytest`) aux versions
épinglées, sans Spark ni Airflow. `requests.get` et `Producer` sont
remplacés par des doubles de test : aucun test n'appelle le réseau.

**Leçon retenue** : un job de tests unitaires ne doit installer que ce que
le code testé importe ; c'est plus rapide et cela révèle les dépendances
cachées.

---

## 3. Port 8080 déjà utilisé : conflit avec un autre projet Airflow

**Symptôme** : `docker compose up` échoue, `port is already allocated`.

**Cause** : les conteneurs d'un autre projet Airflow occupaient le port
8080 du poste.

**Fix** : arrêt des conteneurs de l'autre projet. L'interface Airflow de ce
projet est publiée sur le port 8081.

**Leçon retenue** : sur un poste qui héberge plusieurs projets, vérifier
les ports publiés avant de lancer une pile ; un port par projet évite ce
conflit.

---

## 4. Exécution Airflow en échec : simple problème de connexion

**Symptôme** : un run de `ingestion_bronze_temps_reel` apparaît en échec.

**Cause** : une coupure de connexion pendant l'appel à l'API SNCF. Le code
n'est pas en cause.

**Fix** : aucun changement de code. Les `retries` (2) du DAG relancent la
tâche, et le run suivant est repassé au vert.

**Leçon retenue** : lire d'abord le journal de la tâche avant de toucher
au code ; un échec ponctuel absorbé par les relances n'est pas un bug.

---

## 5. Adresse du webhook Slack visible dans une capture d'écran

**Symptôme** : l'adresse du webhook Slack était lisible sur une capture
d'écran du secret GitHub.

**Cause** : la capture a été prise au moment de la saisie du secret.

**Décision** : l'adresse n'est ni dans le dépôt ni dans un message ; elle
vit uniquement dans le secret GitHub `SLACK_WEBHOOK_URL`. Risque accepté
tant que la capture n'est pas partagée ; rotation du webhook à faire au
moindre doute (un webhook ne permet que d'écrire dans un canal).

**Leçon retenue** : une adresse de webhook se traite comme un secret ; ne
jamais la coller dans une capture, un message ou un fichier suivi par git.
