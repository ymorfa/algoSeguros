import findspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import col, explode, array, lit, concat

import os, sys, json


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


    print(">>> 4. Reestructurando Datos")
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
        col("año"), 
        col("mes_incidencia")[0].alias('mes'),
        col("clave_ent"),
        col("entidad"),
        col("cve_municipio"),
        col("municipio"),
        col("subtipo_de_delito"),
        col("modalidad"),
        concat(col("año"), lit("-"), get_mes_numerico(col("mes_incidencia")[0])).alias("mes-año"),
        col("mes_incidencia")[1].alias("incidencias")
    )

    clave_ent_dict = dict(df_reestructurado.select('clave_ent', 'entidad').distinct().collect())
    clave_municipio_dict = dict(df_reestructurado.select('cve_municipio', 'municipio').distinct().collect())
    df_reestructurado = df_reestructurado.drop(col('entidad'), col('municipio'))
    df_reestructurado = df_reestructurado.withColumn('codigo_lugar', concat(df_reestructurado['cve_municipio'], lit('-'), df_reestructurado['clave_ent']))
    df_reestructurado = df_reestructurado.drop(col('clave_ent'), col('cve_municipio'))
    print('>>> Terminado \n')

    print(">>> 5. Eliminando Registros Vacios")
    df_filtered = df_reestructurado.filter(
        (col('año') < 2024) | ((col('año') == 2024) & (col('mes') == 'enero'))
    )
    print('>>> Terminado \n')
    
    print('>>> 6. Guardando Datos')
    #df_reestructurado.write.options(encoding = 'utf-8', delimiter=',').format("csv").mode('overwrite').save(os.path.join(data_path, 'processed', 'curated.csv'))

    df = df_filtered.toPandas()
    df.to_csv(os.path.join(data_path, 'processed', 'curated.csv'), index=False,  encoding="utf-8")
    spark.stop()

    json_file_path = os.path.join(data_path, 'parameters.json')
    
    data_to_save = {
        "calve_entidad": clave_ent_dict,
        "clave_municipo": clave_municipio_dict
    }

    if not os.path.exists(json_file_path): 
        with open(json_file_path, 'w') as json_file:
            json.dump(data_to_save, json_file, indent=4)
    else:
        with open(json_file_path, 'r') as json_file:
            data = json.load(json_file)

        for key, value in data_to_save.items():
            data[key] = value
    
        with open(json_file_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
    print('>>> Terminado \n')