from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, array, concat, lit, udf
from pyspark.sql.types import StringType


def get_unique_vals(data: DataFrame, column: str) -> list:
    """
    Esta función toma un DataFrame de Spark y el nombre de una columna,
    y devuelve una lista de valores únicos en esa columna.
    """
    unique_vals = data.select(column).distinct().rdd.map(lambda r: r[0]).collect()
    return unique_vals

@udf(StringType())
def get_mes_numerico(mes: str) -> str:
    """Esta función me ayuda a mapear los meses del año como números
    """
    meses_dict = {
        'enero'   : '01',
        'febrero' : '02',
        'marzo'   : '03', 
        'abril'   : '04',
        'mayo'    : '05',  
        'junio'   : '06',
        'julio'   : '07',
        'agosto'  : '08',
        'septiembre' : '09',
        'octubre'    : '10',
        'noviembre'  : '11',
        'diciembre'  : '12' 
    }
    return meses_dict.get(mes)