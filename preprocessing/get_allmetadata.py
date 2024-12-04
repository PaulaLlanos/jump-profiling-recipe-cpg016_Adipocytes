import os
import pandas as pd

# Define the path to the folder containing your CSV files
root_folder = "/home/llanos/2024_10_07_cpg0014_Adipocytes/work/projects/cpg0014-jump-adipocyte/workspace/software/profiles"

# List to store data from each CSV
data = []

# Iterate over all CSV files in the folder and subfolders
for dirpath, _, filenames in os.walk(root_folder):
    for filename in filenames:
        # Process only files that end with "_augmented.csv"
        if filename.endswith("_augmented.csv.gz"):
            # Get the full path of the CSV file
            file_path = os.path.join(dirpath, filename)
            
            # Extract the second-to-last folder name (Metadata_Batch)
            batch_name = os.path.basename(os.path.dirname(dirpath))

            # Load the CSV file and extract the required columns
            try:
                df = pd.read_csv(file_path, usecols=[
                    'Metadata_broad_sample', 'Metadata_Plate', 'Metadata_Well'
                ])
            except ValueError as e:
                print(f"Skipping {file_path}: {e}")
                continue  # Skip files missing the required columns

            # Rename 'Metadata_broad_sample' to 'Metadata_JCP2022'
            df = df.rename(columns={'Metadata_broad_sample': 'Metadata_JCP2022'})

            # Fill NaN values in 'Metadata_JCP2022' with 'control'
            df['Metadata_JCP2022'] = df['Metadata_JCP2022'].fillna('control')

            # Add the new columns
            df['Metadata_Batch'] = batch_name
            df['Metadata_Source'] = 'broad'
            df['Metadata_PlateType'] = 'COMPOUND'

            # Reorder the columns
            df = df[['Metadata_Source', 'Metadata_Batch', 
                     'Metadata_Plate', 'Metadata_Well', 'Metadata_JCP2022','Metadata_PlateType']]

            # Append the DataFrame to the list
            data.append(df)

# Concatenate all the collected DataFrames
if data:  # Only attempt to save if there is data
    result = pd.concat(data, ignore_index=True)
    result.to_csv("/home/llanos/2024_10_07_cpg0014_Adipocytes/jump-profiling-recipe/inputs/plate.csv.gz", index=False)
    print("CSV files combined successfully!")
else:
    print("No valid '_augmented.csv' files found.")

