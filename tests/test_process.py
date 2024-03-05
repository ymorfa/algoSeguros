from pyspark.sql import DataFrame


def get_unique_vals(data: DataFrame, column: str) -> list:
    """
    Esta función toma un DataFrame de Spark y el nombre de una columna,
    y devuelve una lista de valores únicos en esa columna.
    """
    unique_vals = data.select(column).distinct().rdd.map(lambda r: r[0]).collect()
    return unique_vals
