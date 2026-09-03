# Journal des incidents — 2 septembre 2026

## 1. Page 1 -- 6 cartes construites comme cartes géographiques par erreur

Les 6 KPI de la Vue d'ensemble affichaient des mappemondes (TomTom/Bing)
au lieu de chiffres -- mauvaise icône sélectionnée dans le panneau
Visualisations ("Carte" KPI vs "Carte" géographique, ambiguïté du même
mot en français). Corrigé par l'utilisateur lui-même en changeant le type
de visuel sur chaque carte, sans passer par une reconstruction complète.

## 2. Top 5 régions -- filtre fantôme faussant le tri

Après avoir retiré `affected_stations` d'un tableau, un filtre resté
accroché à ce champ (non supprimé, juste modifié) a continué à s'appliquer
silencieusement -- résultat trié dans un ordre incohérent (alphabétique
approximatif plutôt que par nombre d'incidents). Corrigé en supprimant
entièrement le filtre fantôme et en le recréant proprement sur le bon
champ (`regions_affectees`).

## 3. Nb Gares Distinctes par Région -- introduction du pattern TREATAS

Besoin : compter les gares distinctes liées à une région, sans relation
physique directe entre `gold_disruption_by_region` et
`gold_disruption_by_station` (relation volontairement évitée depuis
l'incident du produit croisé, voir notes précédentes). Résolu via
TREATAS, en s'appuyant sur le fait que les deux tables descendent de la
même ligne source (`gold_disruption_context`), donc partagent les mêmes
`disruption_id` par construction. Ce pattern sera réutilisé et approfondi
le 03/09 (voir decisions_architecture.md).

## 4. Carte choroplèthe -- série d'incidents en cascade

Construction de la carte de France par région, la plus longue série
d'incidents de la session :

| Sous-incident | Cause | Correction |
|---|---|---|
| "Ce champ ne peut pas être utilisé ici" (Légende) | Une mesure ne peut pas être glissée dans le champ Légende du visuel Carte (points) -- catégories uniquement | Bascule vers le visuel "Carte choroplèthe", champ "Couleur" dédié aux mesures |
| "D'autres données d'emplacement sont nécessaires" | `regions_affectees` non reconnu comme donnée géographique par Bing | Modélisation -> Catégorie de données -> "État ou province" |
| Plantage (OperationCanceled) en配置ant le dégradé | Instabilité de l'application, pas un bug de configuration | Résolu par un simple redémarrage de l'ordinateur |
| Carte figée sur les USA | Réglage de fond de carte réinitialisé par défaut sur un mauvais territoire | Reconfiguré manuellement après redémarrage |
| Zoom manuel ne tenait jamais | "Zoom automatique" recalculait le cadrage à chaque modification de mise en forme | Désactivé pendant l'édition, réactivé une fois la carte terminée (utile pour le zoom auto sur sélection) |
| Régions fusionnées 2016 (Grand Est, Hauts-de-France, Auvergne-Rhône-Alpes) restaient grises | "Culture de géocodage" pointait vers un référentiel "France avant 2016" | Changé en "France" simple -- trouvé par l'utilisateur lui-même |

**Résultat final** : carte choroplèthe fonctionnelle, dégradé marron/beige
correct sur les 13 régions, zoom automatique sur sélection opérationnel.

## 5. Segment interactif région -> gares -- validé sans modification de relation

Test de l'interactivité (sélection d'une région filtrant la liste des
gares et la carte) : fonctionne directement avec les relations et mesures
TREATAS déjà en place, aucun ajustement de relation nécessaire.

## 6. Page 3 -- tableau alert_level x région, valeur répétée

Tableau croisé affichant "high" dans chaque cellule au lieu d'un nombre --
le champ `alert_level` lui-même avait été glissé dans "Valeurs" au lieu
d'une mesure de comptage. Corrigé en remplaçant par la bonne mesure.
Un second problème (comptage identique partout, "414") a été découvert et
creusé le lendemain -- voir decisions_architecture.md, section TREATAS du
03/09.
