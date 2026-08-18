# Politique de rétention et gouvernance des données — Projet SNCF Streaming Pipeline

## 1. Applicabilité du RGPD

Aucune donnée traitée par ce pipeline n'est une donnée à caractère
personnel au sens de l'Article 4 du RGPD (perturbations ferroviaires,
statistiques de régularité agrégées, résumés générés par LLM — aucune ne
se rapporte à une personne physique identifiée ou identifiable).

**Droit à l'oubli (Art. 17) : non applicable**, conséquence directe de ce
qui précède — aucune donnée personnelle à effacer sur demande.

Les principes de minimisation et de limitation de conservation (Art. 5)
sont néanmoins appliqués ci-dessous, par discipline d'ingénierie plutôt
que par obligation légale stricte sur ces données précises.

## 2. Politique de rétention par table

| Table | Rétention | Justification |
|---|---|---|
| Bronze temps réel | 12 mois glissants | Données opérationnelles brutes, déjà distillées dans Silver/Gold |
| Bronze historique | Indéfinie | Réécrite en overwrite à chaque run mensuel, volume stable |
| Silver (temps réel + historique) | Indéfinie | Déjà nettoyée, volume raisonnable |
| Gold (3 tables) | Indéfinie | Objectif même du projet : analyse de tendance long terme |
| gold_audit_log | Indéfinie | Registre de conformité — sa valeur croît avec le temps |

## 3. Gouvernance des sources externes

### API SNCF (Navitia)
Soumise aux CGU et à la Licence SNCF Open Data. SNCF conserve l'entière
propriété des données — attribution de la source maintenue dans toute
documentation ou restitution (mémoire, Power BI). Pas de redistribution
des données brutes hors du périmètre académique du projet.

### API Claude (Anthropic)
Soumise à l'Acceptable Use Policy d'Anthropic
(anthropic.com/legal/aup). Usage conforme : synthèse de texte
uniquement, aucun contenu sensible transmis (cohérent avec la section 1
— aucune donnée personnelle dans le pipeline).

## 4. Traçabilité

Chaque exécution des jobs de gouvernance (purge de rétention) s'enregistre
dans `gold_audit_log` : job, horodatage, table source/cible, nombre de
lignes traitées, statut. Rend la conformité démontrable plutôt
qu'affirmée (principe de responsabilité, RGPD Art. 5).

**Extension possible, non implémentée à ce stade** : appliquer le même
mécanisme de traçabilité aux 4 DAGs de pipeline existants — aujourd'hui,
seul `gouvernance_rgpd` écrit dans `gold_audit_log`.
