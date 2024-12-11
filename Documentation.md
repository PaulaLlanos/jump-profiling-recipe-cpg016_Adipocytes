I run jump profiling recipe using ORF pipeline.
Here the link of repository:

https://github.com/PaulaLlanos/jump-profiling-recipe/tree/cpg0014_adipocytes

# Prepare Metadata

Code used: 'get_allmetadata.py'
output:'combined_metadata.csv'

It become necessary to get all CSVs in just one document, which should include Metadata and Features. In this big csv we should include also all batches and plates that we want to preprocess.

    # Check and download metadata 
        aws s3 sync --no-sign-request s3://cellpainting-gallery/cpg0014-jump-adipocyte/broad/workspace/metadata/platemaps/ metadata/platemaps/

        also, the metadata_broad_sample column was 'Nan' because the broad sample column in the plate map was empty, since it was a control plate. Based on the answer of Felipe Do Santo, we should consider thos control.txt plate as a DMSO plate. 

We need a csv file that contain also this information: 

Source (broad)
Batch
Plate
Well
Perturbation as Metadata_JCP2022, don't change the name of this columns, because we don't want to modify the code downstream.

# Convert profiles to parquet format
Code: convert_parquet_profiles.py
Output: 'inputs/broad/workspace/profiles/<Batch_name>/<plate_name>/<plante_name.parquet>

Once we got this, we should convert the csv in parquet files with the function load_Data in the preprocessing folder io.py this is the first step.

# Create cell count files to run ORF pipeline
Code: get_cell_counts.py
Output: orf_cell_counts_adipocytes.csv

Beside, it was necessary to creat a file of "orf_cell_counts_adipocytes.csv" since the ORF pipeline require to get the cell counts as a separate file.

# Create the environment

I create the environment using nix, you can check flake files to see te requirement detailed there. I create the environment in Moby Server (CS Lab server mantained by Alán)

cd jump-profiling-recipe/
nix develop . --impure --extra-experimental-features nix-command --extra-experimental-features flakes --show-trace

# To check phenotipic activity calculating mAP
    output: 'map_scores.parquet'
    code: below
    '''
    from preprocessing import metrics

    # Get average precision
    metrics.average_precision_negcon(parquet_path="outputs/orf/profiles_wellpos_cc_var_mad_outlier_featselect_sphering_harmony.parquet", ap_path="ap_scores.parquet", plate_types=["COMPOUND"])
    # Get Mean average precision
    metrics.mean_average_precision("ap_scores.parquet", "map_scores.parquet")
    '''
    




