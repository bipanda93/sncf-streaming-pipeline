# Incidents 2026-11-23 — Fuseau horaire des dates SNCF dans Silver

## 1. Dates de Silver décalées d'une heure (deux heures en été)

**Symptôme** : en comparant une même perturbation dans Bronze et dans Silver,
les dates différaient d'une heure (`updated_at` = `20261120T083758` dans
Bronze, 09:37:58 affiché dans Silver), alors que les deux versions de la
ligne Bronze portaient la même date.

**Diagnostic** : Silver affiché en UTC redonne exactement les chaînes de la
source (08:37:58) : Spark lisait donc les heures de l'API comme de l'UTC. Test
sur Bronze (564 035 messages distincts) : lues comme de l'UTC, 94 086 mises à
jour (environ 17 %) sont postérieures à l'appel qui les rapporte, ce qui est
impossible ; lues comme de l'heure de Paris, 0.

**Cause racine** : l'API renvoie des heures locales sans fuseau
(`20261120T083758`). `to_timestamp` les interprète dans le fuseau de la
session Spark, configuré nulle part dans le dépôt. Silver relu en UTC redonne
les valeurs de la source, ce qui indique une écriture sous UTC (exécution dans
le conteneur Airflow), alors que le Mac est en Europe/Paris. Les dates
stockées étaient donc décalées de l'écart avec Paris : 1 h en hiver, 2 h en été.

**Fix** : fonction `parse_sncf_timestamp` (conversion Europe/Paris vers UTC)
et fuseau de session fixé à UTC dans `silver_transform.py` ; test unitaire
`tests/test_silver_dates.py`. Silver et Gold recalculés en entier depuis
Bronze, qui conserve les chaînes brutes : aucun historique perdu. Vérifié sur
la perturbation `db15fec1…` : 07:37:58, 07:05:00 et 08:26:00 en UTC.

**Leçon retenue** : une date sans fuseau est ambiguë ; fixer explicitement le
fuseau de la session Spark plutôt que de dépendre de la machine, et vérifier
une hypothèse de fuseau par une propriété qui ne peut pas être violée (une
mise à jour ne peut pas suivre l'appel qui la rapporte) plutôt que sur un
seul exemple.
