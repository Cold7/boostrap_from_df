import argparse
import pandas as pd
import multiprocessing as mp
import random

def take_random_elements(lst, num_elements):
    if num_elements >= len(lst):
        return lst

    random_elements = random.sample(lst, num_elements)
    return random_elements

def process_combination(combination):
    # Crea el sub-dataframe con la combinación de columnas
    sub_df = df[list(combination)]

    # Guarda el sub-dataframe en un archivo con el prefijo
    output_file = file_name+"_"+str(combination)[1:-1].replace(", ","-").replace("'","")+".tsv"
    sub_df.to_csv(output_file, sep="\t")


if __name__ == '__main__':
    global df

    # Configuración de argparse
    parser = argparse.ArgumentParser(description='Procesamiento paralelo de sub-dataframes')
    parser.add_argument('-f', '--file', type=str, help='Archivo de entrada', required= True)
    parser.add_argument('-p', '--processors', type=int, help='Número de procesadores a utilizar', required = True)
    parser.add_argument('-b', '--bootstrap', type=int, help='Número de bootstrap', required = True)
    parser.add_argument('-n', '--nexp', type=int, help='Número de experimentos a utilizar en cada boostrap', required = True)

    args = parser.parse_args()

    # Obtener los argumentos del usuario
    file_name = args.file
    num_processors = args.processors
    num_bootstrap = args.bootstrap

    # Leer el DataFrame completo
    df = pd.read_csv(file_name, sep='\t')
    df.set_index(df['ID'], inplace=True)
    del df['ID']
    # Obtener todas las columnas del DataFrame
    columns = df.columns.tolist()

    # Generar una lista de combinaciones únicas de columnas

    combinations = []
    i = 0
    while i < num_bootstrap:
        combination = take_random_elements(list(df.columns), args.nexp)
        if combination not in combinations:
            combinations.append(sorted(combination))
            i += 1
    # Procesamiento paralelo de sub-dataframes
    pool = mp.Pool(processes=num_processors)
    pool.map(process_combination, list(combinations), chunksize = 1)
