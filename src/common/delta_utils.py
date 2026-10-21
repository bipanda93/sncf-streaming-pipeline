"""Outils partagés pour les tables Delta."""
from delta.tables import DeltaTable


def delta_table_existe(spark, path):
    """Indique si une table Delta existe à ce chemin.

    Contrairement à un try/except Exception, une session Spark tombée ou un
    problème de droits remonte comme une vraie erreur, au lieu d'être pris
    pour une table absente.
    """
    return DeltaTable.isDeltaTable(spark, path)
