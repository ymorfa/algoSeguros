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
    subtipo : f'R-{id}' for id, subtipo in enumerate(df.subtipo_de_delito.unique())
    }
    df.subtipo_de_delito = df.subtipo_de_delito.apply(lambda x: subtipo_dict.get(x))
    print('>>> Terminado \n')


    print(">>> 3. Dando Formato")
    df.insert(0, 'date', pd.to_datetime(df['mes-año']).dt.date)
    df.drop(columns=['año', 'mes', 'mes-año'], inplace=True)

    df = df.sort_values('date').groupby(['codigo_lugar', 'subtipo_de_delito', 'date'], as_index=False).agg(
    incidencias = ('incidencias', 'mean'))
    df.columns = ['cod_lugar', 'cod_subtipo','date','delitos']
    print('>>> Terminado \n')


    print(">>> 4. Guardando Datos")
    df.to_csv(os.path.join(data_path, 'processed', 'processed.csv'), index=False,  encoding="utf-8")
    
    subtipo_dict_inv = {
        val : key for key, val in subtipo_dict.items()
    }

    data_to_save = {
        "calve_subtipo": subtipo_dict_inv,
    }

    json_file_path = os.path.join(data_path, 'parameters.json')

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



    
