import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import os, sys


root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)
data_path = os.path.join(root_path, 'data')



if __name__ == "__main__":
    print(">>> Filtrando Datos de Interés")
    findspark.init()
    spark = SparkSession.builder \
        .appName("Exploración de Datos AlgoSeguros") \
        .getOrCreate()
    
    df = spark.read.csv(os.path.join(data_path, 'raw', 'IDM_NM_ene24.csv'), header=True, inferSchema=True, encoding="latin1")
    df = df.filter(
    (col('Bien jurídico afectado') == "El patrimonio") & \
    (col('Tipo de delito') == "Robo")
    )
    df = df.toPandas()
    df.to_csv(os.path.join(data_path, 'processed', 'curated.csv'),  encoding="utf-8")
    print('>>> Terminado')
