# Files changes 

It become necessary to get all CSVs in just one document, which should include Metadata and Features. In this big csv we should include also all batches and plates that we want to preprocess.

We need a csv file that contain also this information: 

Source (broad)
Batch
Plate
Well
Perturbation as Metadata_JCP2022, don't change the name of this columns, because we don't want to modify the code downstream.

Once we got this, we should convert the csv in parquet files with the function load_Data in the preprocessing folder io.py this is the first step.

also, the metadata_broad_sample column was 'Nan' because the broad sample column in the plate map was empty, since it was a control plate. Based on the answer of Felipe Do Santo, we should consider thos control.txt plate as a DMSO plate. 

We checked the 'BR00135823_augmented.csv.gz' file to inspect if we have the columns that we need.

Beside, it was 


# Activate the environment

cd jump-profiling-recipe/
nix develop . --impure --extra-experimental-features nix-command --extra-experimental-features flakes --show-trace

# Download metadata files

aws s3 sync --no-sign-request s3://cellpainting-gallery/cpg0014-jump-adipocyte/broad/workspace/metadata/platemaps/ metadata/platemaps/

