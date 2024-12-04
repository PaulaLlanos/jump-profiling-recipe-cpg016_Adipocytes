import pandas as pd
import os

def csv_to_parquet_inplace(input_folder):
    for dirpath, _, filenames in os.walk(input_folder):
        # Iterar sobre los archivos en el directorio de entrada
        for filename in filenames:
            #print(filename)
            # Filtrar archivos que terminen en "_augmented.csv.gz"
            if filename.endswith("_augmented.csv.gz"):
                # Ruta completa del archivo CSV.gz
                csv_path = os.path.join(dirpath, filename)
                #print('encontre un augmented')

                # Leer el archivo CSV comprimido
                df = pd.read_csv(csv_path, compression='gzip')

                # Generar el nombre del archivo Parquet quitando "_augmented.csv.gz"
                base_name = filename.replace("_augmented.csv.gz", "")
                parquet_path = os.path.join(dirpath, f"{base_name}.parquet")

                # Guardar el DataFrame como archivo Parquet
                df.to_parquet(parquet_path, index=False)
                print(f"Archivo guardado: {parquet_path}")

# Uso del script
input_folder = "/home/llanos/2024_10_07_cpg0014_Adipocytes/work/projects/cpg0014-jump-adipocyte/workspace/software/profiles"  # Ruta al directorio con los archivos CSV.gz
csv_to_parquet_inplace(input_folder)
