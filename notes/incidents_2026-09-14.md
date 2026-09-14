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
