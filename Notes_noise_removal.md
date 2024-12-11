MINI-GRANT - NOISE REMOVAL To include noise_removal
-----------

Since this should be done using jump repository, I should clone jump recipe in a new folder, together with the other files that I will need. Once I cloned it, I need to modify some files accordingly:
1-clone jump-profile-recipe
2- Using the file download_data.sh download jump repository



3- in the snakemake file include noise removal, this will give to snakefile a way to connect the input with the output:

rule noise_removal:
    input:
        "outputs/{scenario}/{pipeline}.parquet",
    output:
        "outputs/{scenario}/{pipeline}_noise_removal.parquet",
    run:
        pp.clean.outlier_removal(*input, *output)
    
4- In preprocessing files, in clean.py file. I should i include the noise_removal function.

5- Also in Compund.json, I should include the name of the function in the  steps.

 original: "pipeline": "profiles_wellpos_cc_var_mad_outlier_featselect_sphering_harmony",
 new version: "pipeline": "profiles_wellpos_cc_var_mad_outlier_noiseremoval_featselect_sphering_harmony",

6- Import the specific dependences to pycytominer folder, in adipocytes i don't import all pycytominer function, I just take a few function, I should take the one that i need in that folder as well. I should add noise_removal function there.

7- run snakemake 

Steps:

Clone jump repository into noise_removal_minigrant folder. Moby server
move files to clone the environment fro adipocytes project 

activate the environment
------
git add flake.nix in the new branch
cd jump-profiling-recipe/

nix develop . --impure --extra-experimental-features nix-command --extra-experimental-features flakes

run snakefile
-----

snakemake -c1 --configfile inputs/orf.json


harmonny is not found as a module:  to verify this in the environment check: 
python -c "import harmonypy; print(harmonypy.__version__)"

