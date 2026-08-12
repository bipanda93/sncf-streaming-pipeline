"""
Gold -- Delta Gold realtime_alerts + Delta Gold punctuality_trends ->
Delta Gold disruption_context.

Phase 1 (temporelle) : comparaison au même mois, national, par transport.
Phase 2 (géographique) : comparaison à toutes les régions des gares
affectées (pas seulement la première), sur le réseau TER.

Découverte importante lors de la validation : le jeu régularité TER
mélange des noms de régions D'AVANT et D'APRÈS la réforme territoriale du
1er janvier 2016 (l'historique remonte à 2013), plus 3 noms de réseaux
commerciaux locaux (Etoile Amiens, Loire Océan, Sud Azur) qui ne sont pas
des régions administratives. SNCF_TO_CURRENT_REGION consolide les
anciennes régions vers leur région fusionnée actuelle (mapping basé sur
la réforme officielle, pas une supposition) et normalise les variantes
orthographiques des régions actuelles (SNCF n'utilise aucune convention
de ponctuation cohérente : "Pays-de-la-Loire" mais "Provence Alpes Côte
d'Azur"). Les 3 noms commerciaux restent volontairement exclus -- leur
rattacher une région précise serait une supposition géographique de ma
part, pas une correspondance officielle vérifiable.

Usage :
    python -m src.streaming.gold_disruption_context
"""
import csv
import io
import logging
import re
from datetime import datetime, timezone

import requests
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array_distinct, avg, col, collect_set, concat_ws, count, countDistinct,
    explode, lit, month, regexp_replace, size, split, trim, udf, upper, when, year,
)
from pyspark.sql.types import DoubleType

from src.ingestion import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

STATIONS_URL = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/gares-de-voyageurs/exports/csv"

DEPARTEMENT_TO_REGION = {
    "01": "Auvergne-Rhône-Alpes", "03": "Auvergne-Rhône-Alpes", "07": "Auvergne-Rhône-Alpes",
    "15": "Auvergne-Rhône-Alpes", "26": "Auvergne-Rhône-Alpes", "38": "Auvergne-Rhône-Alpes",
    "42": "Auvergne-Rhône-Alpes", "43": "Auvergne-Rhône-Alpes", "63": "Auvergne-Rhône-Alpes",
    "69": "Auvergne-Rhône-Alpes", "73": "Auvergne-Rhône-Alpes", "74": "Auvergne-Rhône-Alpes",
    "21": "Bourgogne-Franche-Comté", "25": "Bourgogne-Franche-Comté", "39": "Bourgogne-Franche-Comté",
    "58": "Bourgogne-Franche-Comté", "70": "Bourgogne-Franche-Comté", "71": "Bourgogne-Franche-Comté",
    "89": "Bourgogne-Franche-Comté", "90": "Bourgogne-Franche-Comté",
    "22": "Bretagne", "29": "Bretagne", "35": "Bretagne", "56": "Bretagne",
    "18": "Centre-Val de Loire", "28": "Centre-Val de Loire", "36": "Centre-Val de Loire",
    "37": "Centre-Val de Loire", "41": "Centre-Val de Loire", "45": "Centre-Val de Loire",
    "2A": "Corse", "2B": "Corse",
    "08": "Grand Est", "10": "Grand Est", "51": "Grand Est", "52": "Grand Est",
    "54": "Grand Est", "55": "Grand Est", "57": "Grand Est", "67": "Grand Est",
    "68": "Grand Est", "88": "Grand Est",
    "02": "Hauts-de-France", "59": "Hauts-de-France", "60": "Hauts-de-France",
    "62": "Hauts-de-France", "80": "Hauts-de-France",
    "75": "Île-de-France", "77": "Île-de-France", "78": "Île-de-France", "91": "Île-de-France",
    "92": "Île-de-France", "93": "Île-de-France", "94": "Île-de-France", "95": "Île-de-France",
    "14": "Normandie", "27": "Normandie", "50": "Normandie", "61": "Normandie", "76": "Normandie",
    "16": "Nouvelle-Aquitaine", "17": "Nouvelle-Aquitaine", "19": "Nouvelle-Aquitaine",
    "23": "Nouvelle-Aquitaine", "24": "Nouvelle-Aquitaine", "33": "Nouvelle-Aquitaine",
    "40": "Nouvelle-Aquitaine", "47": "Nouvelle-Aquitaine", "64": "Nouvelle-Aquitaine",
    "79": "Nouvelle-Aquitaine", "86": "Nouvelle-Aquitaine", "87": "Nouvelle-Aquitaine",
    "09": "Occitanie", "11": "Occitanie", "12": "Occitanie", "30": "Occitanie", "31": "Occitanie",
    "32": "Occitanie", "34": "Occitanie", "46": "Occitanie", "48": "Occitanie", "65": "Occitanie",
    "66": "Occitanie", "81": "Occitanie", "82": "Occitanie",
    "44": "Pays de la Loire", "49": "Pays de la Loire", "53": "Pays de la Loire",
    "72": "Pays de la Loire", "85": "Pays de la Loire",
    "04": "Provence-Alpes-Côte d'Azur", "05": "Provence-Alpes-Côte d'Azur",
    "06": "Provence-Alpes-Côte d'Azur", "13": "Provence-Alpes-Côte d'Azur",
    "83": "Provence-Alpes-Côte d'Azur", "84": "Provence-Alpes-Côte d'Azur",
}

HALL_SUFFIX_RE = re.compile(r"\s+-?\s*hall\s+[0-9]+(\s*&\s*[0-9]+)*\s*$", re.IGNORECASE)
DASH_SEPARATOR_RE = re.compile(r"\s+-\s+")
CONNECTOR_DASH_RE = re.compile(r"(?i)-(sur|en|de|du|des|la|le|les)-")

STATION_ALIASES = {
    "PARIS NORD": "PARIS GARE DU NORD",
    "VALENCE VILLE": "VALENCE",
}

# Consolidation des noms de région trouvés dans le jeu régularité TER vers
# le nom canonique actuel (celui produit par DEPARTEMENT_TO_REGION) --
# anciennes régions (réforme 2016) + variantes orthographiques SNCF.
SNCF_TO_CURRENT_REGION = {
    # Anciennes régions -> région fusionnée actuelle (réforme du 01/01/2016)
    "Alsace": "Grand Est",
    "Champagne Ardenne": "Grand Est",
    "Lorraine": "Grand Est",
    "Aquitaine": "Nouvelle-Aquitaine",
    "Limousin": "Nouvelle-Aquitaine",
    "Poitou Charentes": "Nouvelle-Aquitaine",
    "Auvergne": "Auvergne-Rhône-Alpes",
    "Rhône Alpes": "Auvergne-Rhône-Alpes",
    "Bourgogne": "Bourgogne-Franche-Comté",
    "Franche Comté": "Bourgogne-Franche-Comté",
    "Languedoc Roussillon": "Occitanie",
    "Midi Pyrénées": "Occitanie",
    "Nord Pas de Calais": "Hauts-de-France",
    "Picardie": "Hauts-de-France",
    "Basse Normandie": "Normandie",
    "Haute Normandie": "Normandie",
    "Centre": "Centre-Val de Loire",
    # Régions actuelles, variantes orthographiques SNCF -> nom canonique
    "Auvergne-Rhône-Alpes": "Auvergne-Rhône-Alpes",
    "Bourgogne-Franche-Comté": "Bourgogne-Franche-Comté",
    "Bretagne": "Bretagne",
    "Centre Val-de-Loire": "Centre-Val de Loire",
    "Grand Est": "Grand Est",
    "Hauts-de-France": "Hauts-de-France",
    "Normandie": "Normandie",
    "Nouvelle Aquitaine": "Nouvelle-Aquitaine",
    "Occitanie": "Occitanie",
    "Pays-de-la-Loire": "Pays de la Loire",
    "Provence Alpes Côte d'Azur": "Provence-Alpes-Côte d'Azur",
    # Etoile Amiens, Loire Océan, Sud Azur : volontairement absents --
    # réseaux commerciaux TER, pas des régions administratives, non
    # mappés faute de correspondance officielle vérifiable.
}


def _normalize_name_python(name: str) -> str:
    name = HALL_SUFFIX_RE.sub("", name)
    name = DASH_SEPARATOR_RE.sub(" ", name)
    name = CONNECTOR_DASH_RE.sub(r" \1 ", name)
    name = re.sub(r"\s+", " ", name).strip().upper()
    return STATION_ALIASES.get(name, name)


def _normalize_name_col(c):
    step1 = regexp_replace(c, r"(?i)\s+-?\s*hall\s+[0-9]+(\s*&\s*[0-9]+)*\s*$", "")
    step2 = regexp_replace(step1, r"\s+-\s+", " ")
    step3 = regexp_replace(step2, r"(?i)-(sur|en|de|du|des|la|le|les)-", " $1 ")
    step4 = regexp_replace(step3, r"\s+", " ")
    normalized = upper(trim(step4))
    aliased = normalized
    for original, alias in STATION_ALIASES.items():
        aliased = when(normalized == original, lit(alias)).otherwise(aliased)
    return aliased


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder.appName("sncf-gold-disruption-context")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def _build_monthly_context(spark, historical_df, current_month_num: int):
    monthly = (
        historical_df.withColumn("month_num", month(col("period_date")))
        .withColumn("year_num", year(col("period_date")))
        .filter(col("month_num") == current_month_num)
        .filter(col("taux_ponctualite").isNotNull())
        .groupBy("transport_type")
        .agg(avg("taux_ponctualite").alias("avg_taux"), countDistinct("year_num").alias("nb_annees"))
    )
    rows = {r["transport_type"]: r for r in monthly.collect()}
    context = {"context_month": current_month_num}
    for transport in ["TER", "TGV", "Intercités"]:
        key = transport.lower().replace("é", "e")
        r = rows.get(transport)
        context[f"{key}_taux_historique_moyen"] = float(r["avg_taux"]) if r else None
        context[f"{key}_nb_annees_reference"] = int(r["nb_annees"]) if r else 0
    return spark.createDataFrame([context])


def _load_stations_region_lookup(spark):
    response = requests.get(STATIONS_URL, timeout=30)
    csv_text = response.content.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(csv_text), delimiter=";")
    rows = list(reader)
    header, data_rows = rows[0], rows[1:]
    name_idx, insee_idx = header.index("nom"), header.index("codeinsee")

    lookup_rows = []
    for r in data_rows:
        if len(r) <= max(name_idx, insee_idx):
            continue
        nom, insee = r[name_idx].strip(), r[insee_idx].strip()
        dept = insee[:2] if len(insee) >= 2 else ""
        region = DEPARTEMENT_TO_REGION.get(dept)
        if nom and region:
            lookup_rows.append((_normalize_name_python(nom), region))

    return (
        spark.createDataFrame(lookup_rows, schema=["nom_gare_norm", "region_geographique"])
        .dropDuplicates(["nom_gare_norm"])
    )


def _build_regional_rate_map(historical_df, current_month_num: int) -> dict:
    """
    Consolide TOUTES les variantes de noms de région (anciennes régions
    pré-2016 + variantes orthographiques) vers le nom canonique actuel
    avant de moyenner -- l'historique "Alsace" (avant 2016) contribue
    ainsi à la moyenne de "Grand Est" (sa région fusionnée actuelle).
    """
    regional_raw = (
        historical_df.filter((col("scope_type") == "region") & (col("dataset_key") == "ter"))
        .withColumn("month_num", month(col("period_date")))
        .filter(col("month_num") == current_month_num)
        .filter(col("taux_ponctualite").isNotNull())
    )

    mapping_expr = None
    for sncf_name, canonical in SNCF_TO_CURRENT_REGION.items():
        if mapping_expr is None:
            mapping_expr = when(col("scope_value") == sncf_name, canonical)
        else:
            mapping_expr = mapping_expr.when(col("scope_value") == sncf_name, canonical)
    mapping_expr = mapping_expr.otherwise(lit(None)) if mapping_expr is not None else lit(None)

    remapped = regional_raw.withColumn("region_canonique", mapping_expr).filter(
        col("region_canonique").isNotNull()
    )

    aggregated = remapped.groupBy("region_canonique").agg(
        avg("taux_ponctualite").alias("taux_region"),
        countDistinct("scope_value").alias("nb_variantes_consolidees"),
    )
    rows = aggregated.collect()
    return {r["region_canonique"]: r["taux_region"] for r in rows}


def _add_geographic_context(alerts_df, stations_lookup, region_rate_map: dict):
    exploded = (
        alerts_df.select("disruption_id", explode(split(col("affected_stations"), ",\\s*")).alias("gare_brute"))
        .filter(trim(col("gare_brute")) != "")
        .withColumn("nom_gare_norm", _normalize_name_col(col("gare_brute")))
    )
    matched = exploded.join(stations_lookup, on="nom_gare_norm", how="left")
    per_disruption_regions = matched.groupBy("disruption_id").agg(
        array_distinct(collect_set("region_geographique")).alias("regions_array")
    )

    def _avg_rate(regions):
        if not regions:
            return None
        rates = [region_rate_map[r] for r in regions if r in region_rate_map]
        return sum(rates) / len(rates) if rates else None

    avg_rate_udf = udf(_avg_rate, DoubleType())

    with_regions = alerts_df.join(per_disruption_regions, on="disruption_id", how="left").withColumn(
        "regions_affectees", concat_ws(", ", col("regions_array"))
    ).withColumn(
        "nb_regions_affectees", size(col("regions_array"))
    )
    return with_regions.withColumn(
        "region_taux_historique_moyen", avg_rate_udf(col("regions_array"))
    ).drop("regions_array")


def run_gold_disruption_context() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    alerts_df = spark.read.format("delta").load(config.DELTA_GOLD_REALTIME_ALERTS_PATH)
    historical_df = spark.read.format("delta").load(config.DELTA_GOLD_PUNCTUALITY_TRENDS_PATH)
    current_month_num = datetime.now(timezone.utc).month

    monthly_context_df = _build_monthly_context(spark, historical_df, current_month_num)
    stations_lookup = _load_stations_region_lookup(spark)
    region_rate_map = _build_regional_rate_map(historical_df, current_month_num)

    with_geo = _add_geographic_context(alerts_df, stations_lookup, region_rate_map)
    gold_df = with_geo.crossJoin(monthly_context_df)

    alerts_count = alerts_df.count()
    gold_count = gold_df.count()
    geo_matched = gold_df.filter(col("nb_regions_affectees") > 0).count()
    rate_resolved = gold_df.filter(col("region_taux_historique_moyen").isNotNull()).count()

    gold_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        config.DELTA_GOLD_DISRUPTION_CONTEXT_PATH
    )

    logger.info(
        "Gold disruption_context -- %d alerte(s) -> %d ligne(s), %d avec region identifiee, %d avec taux historique resolu (mois %02d, %d regions canoniques dans le mapping) -> '%s'",
        alerts_count, gold_count, geo_matched, rate_resolved, current_month_num,
        len(region_rate_map), config.DELTA_GOLD_DISRUPTION_CONTEXT_PATH,
    )


if __name__ == "__main__":
    run_gold_disruption_context()
