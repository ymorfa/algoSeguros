import findspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import col, explode, array, lit, concat

import os, sys


root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)
data_path = os.path.join(root_path, 'data')

from src.tools import get_mes_numerico



if __name__ == "__main__":

    print(">>> 0. Inicializando Spark")
    findspark.init()
    conf = SparkConf()
    conf.set("spark.driver.memory", "4g")  # Establece la memoria para el driver
    conf.set("spark.executor.memory", "4g")  # Establece la memoria para los ejecutores

    spark = SparkSession.builder.appName("Exploración de Datos AlgoSeguros") \
            .config(conf=conf) \
            .getOrCreate()
    print('>>> Terminado \n')
    
    
    # Cargando los datos
    print(">>> 1. Cargando Datos")
    df = spark.read.csv(os.path.join(data_path, 'raw', 'IDM_NM_ene24.csv'), header=True, inferSchema=True, encoding="latin1")
    print('>>> Terminado \n')


    print(">>> 2. Filtrando Datos de Interés")
    df = df.filter((col('Bien jurídico afectado') == "El patrimonio") & (col('Tipo de delito') == "Robo"))
    df = df.drop(col('Bien jurídico afectado'), col('Tipo de delito'))
    print('>>> Terminado \n')

    print(">>> 3. Renombrando Columnas")
    new_cols = {column_name : column_name.replace(' ', '_').replace('.', '').strip().lower() for column_name in  df.columns}    
    for old_name, new_name in new_cols.items():
        df = df.withColumnRenamed(old_name, new_name)
    print('>>> Terminado \n')


    print(">>> Reestructurando Datos")
    meses = df.columns[7:] 

    df_reestructurado = df.select(
        col("año"), 
        col("clave_ent"),
        col("entidad"),
        col("cve_municipio"),
        col("municipio"),
        col("subtipo_de_delito"),
        col("modalidad"),
        explode(array(*[array([lit(mes), col(mes)]) for mes in meses])).alias("mes_incidencia"))
    
    df_reestructurado = df_reestructurado.select(
        col("clave_ent"),
        col("entidad"),
        col("cve_municipio"),
        col("municipio"),
        col("subtipo_de_delito"),
        col("modalidad"),
        concat(col("año"), lit("-"), get_mes_numerico(col("mes_incidencia")[0])).alias("mes-año"),
        col("mes_incidencia")[1].alias("incidencias")
    )
    print('>>> Terminado \n')

    print('>>> Guardando Datos')
    #df_reestructurado.write.options(encoding = 'utf-8', delimiter=',').format("csv").mode('overwrite').save(os.path.join(data_path, 'processed', 'curated.csv'))

    df = df_reestructurado.toPandas()
    df.to_csv(os.path.join(data_path, 'processed', 'curated.csv'), index=False,  encoding="utf-8")
    spark.stop()