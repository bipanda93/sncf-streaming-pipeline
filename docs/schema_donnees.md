# Modèle de données — Pipeline SNCF

## Structure réelle : table de pont, pas un schéma en étoile classique

Une perturbation touche souvent **plusieurs** régions et **plusieurs** gares
à la fois -- un schéma en étoile classique (1 fait = 1 valeur par dimension)
provoquerait un produit croisé (voir notes/incidents). D'où l'usage de
tables de pont (`by_region`, `by_station`), reliées uniquement à
`gold_disruption_context`, jamais entre elles.

```mermaid
erDiagram

GOLD_DISRUPTION_CONTEXT ||--o{ GOLD_DISRUPTION_BY_REGION : "1 perturbation vers N regions"
GOLD_DISRUPTION_CONTEXT ||--o{ GOLD_DISRUPTION_BY_STATION : "1 perturbation vers N gares"
GOLD_DISRUPTION_CONTEXT ||--|| GOLD_REALTIME_ALERTS : "1 vers 1"
GOLD_DISRUPTION_CONTEXT ||--|| SNCF_DISRUPTIONS : "1 vers 1"

GOLD_DISRUPTION_CONTEXT {
string disruption_id
string alert_level
string severity_name
datetime period_begin
datetime period_end
int nb_regions_affectees
float region_taux_historique_moyen
    }

GOLD_DISRUPTION_BY_REGION {
string disruption_id
string regions_affectees
    }

GOLD_DISRUPTION_BY_STATION {
string disruption_id
string affected_stations
date Jour
    }

GOLD_REALTIME_ALERTS {
string disruption_id
string alert_level
int nb_affected_stations
    }

SNCF_DISRUPTIONS {
string disruption_id
string status
datetime period_begin
date Jour
    }

GOLD_PUNCTUALITY_TRENDS {
string dataset_key
string scope_type
string scope_value
string transport_type
date period_date
float taux_ponctualite
int nb_annulations
int nb_trains_prevus
    }
```

## Note importante -- ce que le diagramme ne montre pas

`GOLD_PUNCTUALITY_TRENDS` est volontairement isolée (aucune relation) --
table historique batch, indépendante du flux temps réel. Les relations
`by_region -> context` et `by_station -> context` existent bien
physiquement, mais en filtrage à sens unique -- `TREATAS` est utilisé en
DAX pour les mesures qui doivent filtrer `context` depuis `by_region`
(voir decisions_architecture.md, section TREATAS du 03/09).
