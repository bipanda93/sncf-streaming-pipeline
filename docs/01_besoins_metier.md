# Besoins métier — Pipeline SNCF

## Problématique générale

Comment permettre à un opérateur ferroviaire (ou un voyageur) de détecter,
contextualiser et anticiper les perturbations du réseau, en croisant
données temps réel et historique de régularité ?

## Questions métier auxquelles le dashboard répond

| Question | Page / KPI concerné |
|---|---|
| Combien de perturbations sont actives maintenant ? | Page 1 -- Nb Perturbations Actives |
| Combien de perturbations ont eu lieu aujourd'hui (actives + closes) ? | Page 1 -- Perturbations Aujourd'hui |
| Quelles régions sont les plus touchées ? | Page 2 -- Top 5, carte choroplèthe |
| Quel est le type d'incident dominant (retard, annulation...) ? | Page 3 -- répartition par niveau/type |
| Cette région est-elle historiquement fiable ou non ? | Page 4 -- écart taux actuel vs historique |
| Quel est le taux d'annulation réel, par région ? | Page 4 -- Taux Annulation (TER) |

## Question métier identifiée, mais volontairement hors périmètre

**"Puis-je voyager ce soir sans risque de problème au retour ?"** --
question posée le 03/09, discutée et jugée non répondable avec les
données actuelles : ni le temps réel (photo instantanée) ni l'historique
(moyenne sur plusieurs mois) ne captent un vrai motif horaire
(matin/soir, jour de semaine). Chantier futur possible, nécessiterait
d'exploiter `period_begin` à l'heure près -- jamais construit à ce jour.

## Limites connues et assumées

- `gold_punctuality_trends[scope_value]` mélange anciennes et nouvelles
  appellations régionales -- correction reportée (voir
  decisions_architecture.md, 03/09)
- Isolation Forest non entraîné -- attend l'accumulation d'historique
- Infrastructure Azure jamais testée en charge continue
