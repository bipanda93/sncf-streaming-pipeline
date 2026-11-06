# Rapport Power BI

Le rapport Power BI Desktop (mode Import) lit les tables Gold exportées en
Parquet par `src/monitoring/export_for_powerbi.py`. Il est refait page par
page depuis novembre 2026 : page 1 vue d'ensemble, page 2 régions et gares,
page 3 types de perturbations, page 4 ponctualité historique. Les pages 2 et 3
sont abouties ; les pages 1 et 4 ont encore des finitions à faire (section
« Reste à faire » en bas de page).

## Identité visuelle

Alignée sur ce site : fond photo de nuit, panneaux `#0E1628` avec environ 25 %
de transparence, texte `#E8ECF5`, texte secondaire `#8FA0C0`, accent `#F6D860`.
La police Segoe UI est prévue mais pas encore appliquée partout. Les niveaux d'alerte gardent partout les mêmes couleurs :
critique `#D62728`, élevé `#FF7F0E`, moyen `#F2C12E`, info `#8A97AD`, faible `#4C9BE8`.

## Modèle

| Table | Rôle |
|---|---|
| `gold_disruption_context` | Une ligne par perturbation, table centrale du modèle |
| `gold_disruption_by_region` | Perturbation x région (colonnes multi-valuées éclatées) |
| `gold_disruption_by_station` | Perturbation x gare (idem) |
| `gold_realtime_alerts` | Niveau d'alerte et rang de priorité |
| `gold_punctuality_trends` | Historique de ponctualité, sans relation (granularités mélangées) |
| `Effets` | Table de correspondance saisie en DAX : libellé français des effets |

Relations : `by_region` et `by_station` vers `context` (plusieurs vers un, sens
unique) ; `realtime_alerts` vers `context` (un vers un) ; `Effets[nom_api]` vers
`context[severity_name]`. Le filtrage entre tables sans relation directe passe
par `TREATAS` (voir la décision du 03/09).

## Pages

1. **Vue d'ensemble** : bande de KPI temps réel (perturbations actives, durée
   moyenne, régions touchées par perturbation, moyenne journalière).
2. **Régions et gares** : carte et tableau de détail par région (perturbations,
   moyenne par jour, gares touchées, gares par jour, gares par perturbation).
3. **Types de perturbations** : une matrice unique région x effet avec une
   colonne de priorité moyenne et un dégradé de couleur, plus les répartitions
   par niveau d'alerte (barres empilées).
4. **Ponctualité** : cinq KPI (globale, TER, TGV, Intercités, taux
   d'annulation), ponctualité moyenne par année, écart des régions TER à la
   moyenne (5 meilleures et 5 moins ponctuelles), taux d'annulation par
   région d'origine.

## Mesures clés

```dax
-- Priorité moyenne d'une sélection de perturbations (pattern TREATAS)
Niveau Moyen (Priorité 2) =
CALCULATE(
    AVERAGE(gold_disruption_context[alert_priority_rank]),
    TREATAS(VALUES(gold_disruption_by_region[disruption_id]),
            gold_disruption_context[disruption_id])
)

-- Une seule matrice : la ligne « Priorité moy. » de la table Effets
-- affiche la priorité, les autres lignes affichent le nombre d'incidents
Valeur matrice =
IF(SELECTEDVALUE(Effets[libellé]) = "Priorité moy.",
   CALCULATE([Niveau Moyen (Priorité 2)], REMOVEFILTERS(Effets)),
   [Nb Incidents par Région])

KPI Taux d'annulation =
DIVIDE(SUM(gold_punctuality_trends[nb_annulations]),
       SUM(gold_punctuality_trends[nb_trains_prevus]))

Écart à la moyenne TER =
IF(ISBLANK([Ponctualité TER]), BLANK(),
   [Ponctualité TER] - [Ponctualité TER référence])
```

`Écart Top Mid Bottom` classe les régions avec `RANKX` sur `ALLSELECTED` et ne
garde que les N premières, les N dernières et un bloc central de N régions.

## Pièges rencontrés

- **Valeur vide moins un nombre** : sans le test `ISBLANK`, les périmètres non
  TER (TGV, France) apparaissaient avec un écart de moins la moyenne.
- **Matrice qui répète les mesures** sous le champ colonne : d'où la table
  `Effets` et le pseudo-membre « Priorité moy. ».
- **Barres empilées à 100 %** : toutes de la même longueur, donc inutiles pour
  comparer des volumes ; barres empilées simples retenues.
- **Mesure créée comme colonne calculée** : l'erreur « impossible de déterminer
  la valeur » disparaît en la recréant comme mesure.
- **Moyenne par jour** : calculée sur les jours où la région a des données,
  pas sur tous les jours de la période.
- **Lignes sans région ou sans gare** : supprimées par l'export après
  l'éclatement, donc absentes des vues par région.
- **Anciens noms de régions** dans l'historique de ponctualité (avant 2016) :
  ils apparaissent encore dans deux graphiques de la page 4.

## Reste à faire

- Page 1 : afficher les nombres entiers (les cartes affichent « 2K »), renommer
  les libellés.
- Page 3 : les 167 alertes critiques n'apparaissent dans aucune région et la
  matrice n'a pas de colonne « Aucun service ». Hypothèse à vérifier : ces
  alertes n'ont pas de région dans `gold_disruption_context`, et l'export les
  supprime. Décider d'une ligne « Non localisée » dans l'export ou d'un calcul
  côté rapport.
- Page 4 : table de correspondance anciens noms vers régions actuelles ;
  écart avec 2 décimales ; taux d'annulation en pourcentage (le graphique
  affiche des fractions comme 0,065) ; graphique d'évolution en lignes par type
  de transport (c'est une aire à une seule série).
- Toutes les pages : police Segoe UI, texte alternatif des visuels.
- Vérifier que les volumes du rapport correspondent au dernier export.
