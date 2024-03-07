import pandas as pd
import os, sys
import json


root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)
data_path = os.path.join(root_path, 'data')

if __name__ == "__main__":

    print(">>> 1. Cargando Datos")
    df = pd.read_csv(os.path.join(data_path, 'processed', 'curated.csv'), encoding='utf-8')
    print('>>> Terminado \n')

    print(">>> 2. Procesando Subtipos Datos y Modalidad")
    df.loc[df['subtipo_de_delito'] == 'Robo de maquinaria', 'subtipo_de_delito'] = df.loc[df['subtipo_de_delito'] == 'Robo de maquinaria', 'modalidad'].apply(lambda x: x[:-13])
    df.loc[df['subtipo_de_delito'] == 'Robo de vehículo automotor', 'subtipo_de_delito'] = df.loc[df['subtipo_de_delito'] == 'Robo de vehículo automotor', 'modalidad'].apply(lambda x: x[:-13])
    df.modalidad = df.modalidad.apply(lambda x: x[-13:])
    df.drop(columns=['modalidad'], inplace=True)

    subtipo_dict = {
    subtipo : id for id, subtipo in enumerate(df.subtipo_de_delito.unique())
        }
    df.subtipo_de_delito = df.subtipo_de_delito.apply(lambda x: subtipo_dict.get(x))

    print('>>> Terminado \n')

    print(">>> 3. Procesando Entidades y Municipios")
    lugares = (df.municipio.str.strip() +', '+ df.entidad.str.strip()).unique()
    lugares_dict = { lugar : id for id, lugar in enumerate(lugares)}
    df['mun_entd_id'] = (df.municipio.str.strip() +', '+ df.entidad.str.strip()).apply(lambda x: lugares_dict.get(x))
    df.drop(columns=['entidad', 'municipio'], inplace=True)
    print('>>> Terminado \n')

    print(">>> 4. Dando Formato")
    df.insert(0, 'date', pd.to_datetime(df['mes-año']).dt.date)
    df.drop(columns=['año', 'mes', 'mes-año'], inplace=True)

    df = df.sort_values('date').groupby(['mun_entd_id', 'subtipo_de_delito', 'date'], as_index=False).agg(
    incidencias = ('incidencias', 'mean'))
    df.columns = ['loc', 'subtipo','date', 'delitos']
    print('>>> Terminado \n')

    print(">>> 5. Separanda Conjuntos de Entrenamiento y Prueba")
    train = [] 
    test = []

    # Definimos una función para dividir los datos en train y test
    def split_train_test(group):
        train_group = group.iloc[:-30]  # Los primeros datos para entrenamiento
        test_group = group.iloc[-30:]    # Los últimos datos para prueba
        return train_group, test_group

    # Aplicamos la función de división a cada grupo
    for group_name, group_data in df.groupby(['loc', 'subtipo']):
        train_group, test_group = split_train_test(group_data)
        train.append(train_group)
        test.append(test_group)

    train = pd.concat(train)
    test = pd.concat(test)
    print('>>> Terminado \n')

    print(">>> 6. Estableciendo Parámetros")

    subtipo_dict_inv = {
        val : key for key, val in subtipo_dict.items()
    }
    lugares_dict_inv = {
        val : key for key, val in lugares_dict.items()
    }


    data_to_save = {
        "subtipo_dict_inv": subtipo_dict_inv,
        "lugares_dict_inv": lugares_dict_inv
    }

    file_name = os.path.join(data_path, 'parameters.json')

    # Guardar el diccionario en un archivo JSON
    with open(file_name, 'w') as json_file:
        json.dump(data_to_save, json_file)

    print('>>> Terminado \n')

    print(">>> 6. Guardando Datos")
    train.to_csv(os.path.join(data_path, 'processed', 'train_set.csv'), index=False,  encoding="utf-8")
    test.to_csv(os.path.join(data_path, 'processed', 'test_set.csv'), index=False,  encoding="utf-8")
    df.to_csv(os.path.join(data_path, 'processed', 'processed.csv'), index=False,  encoding="utf-8")
    



    
