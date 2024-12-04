import os
import pandas as pd

# Define el directorio raíz donde están los archivos CSV
root_folder = "/home/llanos/2024_10_07_cpg0014_Adipocytes/work/projects/cpg0014-jump-adipocyte/workspace/software/profiles"

# Lista para almacenar los DataFrames de cada archivo
data = []

# Columnas necesarias para el procesamiento
required_cols = ['Metadata_plate_map_name','Metadata_broad_sample', 'Metadata_Plate', 
                 'Metadata_Well', 'Metadata_Count_Cells']

# Iterar por todos los archivos dentro de la carpeta y subcarpetas
for dirpath, _, filenames in os.walk(root_folder):
    for filename in filenames:
        # Procesar solo los archivos que terminan en "_augmented.csv.gz"
        if filename.endswith("_augmented.csv.gz"):
            # Obtener la ruta completa del archivo
            file_path = os.path.join(dirpath, filename)
            df = pd.read_csv(file_path)

            # Verificar si 'Metadata_plate_map_name' existe, si no agregarla
            if 'Metadata_plate_map_name' not in df.columns:
                df['Metadata_plate_map_name'] = 'JUMP-Target-2_compound_platemap'

            # Rellenar valores NaN en 'Metadata_broad_sample' con 'control'
            df['Metadata_broad_sample'] = df['Metadata_broad_sample'].fillna('control')
            
            sub_df = df.loc[:, ['Metadata_plate_map_name', 'Metadata_broad_sample', 
                    'Metadata_Plate', 'Metadata_Well', 'Metadata_Count_Cells']]


            # Agregar el DataFrame a la lista
            data.append(sub_df)
            
# Concatenar todos los DataFrames recopilados
if data:  # Intentar guardar solo si hay datos
    result = pd.concat(data, ignore_index=True)
    result.to_csv("orf_cell_counts_adipocytes.csv.gz", index=False)
    print("¡Archivos CSV combinados exitosamente!")
else:
    print("No se encontraron archivos '_augmented.csv.gz' válidos.")
