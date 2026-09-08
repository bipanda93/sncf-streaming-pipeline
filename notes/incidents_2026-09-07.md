# Incident 2026-09-07 — AVERAGEX renvoie le total brut au lieu d'une moyenne

## Symptôme
`Moyenne Journalière Incidents (Région)` et `Moyenne Journalière Gares (Région)`
affichaient exactement la même valeur que les colonnes brutes correspondantes
(`Incidents par Région`, `Gares Impliquées par Région`) — région par région,
au chiffre près (ex: Île-de-France 166 / 166,00, Auvergne-Rhône-Alpes 118 / 118,00).

## Diagnostic
Comparaison des deux tableaux avec les vraies valeurs avant toute modification DAX
(réflexe déjà établi). Le pattern "identique partout" a d'abord fait penser à un
TREATAS manquant (piège récurrent du projet), mais ici la mesure ne croise
qu'une seule table (`gold_disruption_by_region`), donc TREATAS n'était pas en cause.

## Cause racine
```dax
-- Cassée
AVERAGEX(
    VALUES(gold_disruption_by_region[Jour]),
    DISTINCTCOUNT(gold_disruption_by_region[disruption_id])   -- pas de CALCULATE
)
```
`AVERAGEX` crée un contexte de ligne sur chaque valeur de `Jour`, mais
`DISTINCTCOUNT` est une fonction d'agrégation qui ne lit que le contexte de
filtre — jamais le contexte de ligne — sauf transition explicite via `CALCULATE`.
Sans lui, chaque itération recalculait la même valeur (celle du contexte de
filtre ambiant, toute la région confondue), et la "moyenne" d'une constante
répétée N fois est cette constante.

## Fix
```dax
Moyenne Journalière Incidents (Région) = 
AVERAGEX(
    VALUES(gold_disruption_by_region[Jour]),
    CALCULATE(
        DISTINCTCOUNT(gold_disruption_by_region[disruption_id])
    )
)
```

## Vérification
Toutes les valeurs ont changé et divergent correctement après le fix. Le ratio
(brut ÷ moyenne) est ressorti proche d'un entier partout — 3 pour la plupart
des régions, 2 pour Bretagne et Pays de la Loire — cohérent avec un nombre de
jours actifs différent par région, pas un artefact du bug.

## Leçon
Même famille de symptôme que le piège TREATAS ("même valeur partout"), mais
mécanisme différent : TREATAS corrige la propagation de filtre entre tables
liées en filtrage à sens unique ; `CALCULATE` ici corrige la transition
ligne → filtre à l'intérieur d'un itérateur (`AVERAGEX`, `SUMX`, etc.). Le
réflexe diagnostique (vraies valeurs avant de toucher au DAX) résout les deux.
