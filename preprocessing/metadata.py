"""
Functions to load metadata information
"""

import logging
from collections.abc import Iterable

import pandas as pd

# logging.basicConfig(format='%(levelname)s:%(asctime)s:%(name)s:%(message)s', level=logging.WARN)
logger = logging.getLogger(__name__)
# logger.setLevel(logging.WARN)

UNTREATED = "JCP2022_999999"
UNKNOWN = "JCP2022_UNKNOWN"
BAD_CONSTRUCT = "JCP2022_900001"

MICRO_CONFIG = pd.read_csv(
    "https://raw.githubusercontent.com/jump-cellpainting/datasets/181fa0dc96b0d68511b437cf75a712ec782576aa/metadata/microscope_config.csv"
)
MICRO_CONFIG["Metadata_Source"] = "source_" + MICRO_CONFIG["Metadata_Source"].astype(
    str
)
MICRO_CONFIG = MICRO_CONFIG.set_index("Metadata_Source")["Metadata_Microscope_Name"]

POSCON_CODES = [
    "JCP2022_012818",
    "JCP2022_050797",
    "JCP2022_064022",
    "JCP2022_035095",
    "JCP2022_046054",
    "JCP2022_025848",
    "JCP2022_037716",
    "JCP2022_085227",
    "JCP2022_805264",
    "JCP2022_915132",
]
NEGCON_CODES = [
    "JCP2022_800001",
    "JCP2022_800002",
    "JCP2022_033924",
    "JCP2022_915131",
    "JCP2022_915130",
    "JCP2022_915129",
    "JCP2022_915128",
]


def find_feat_cols(cols: Iterable[str]):
    """Find column names for features"""
    feat_cols = [c for c in cols if not c.startswith("Meta")]
    return feat_cols


def find_meta_cols(cols: Iterable[str]):
    """Find column names for metadata"""
    meta_cols = [c for c in cols if c.startswith("Meta")]
    return meta_cols


def get_orf_plate_redlist(plate_types: list[str]):
    """Get set of plate_id's  that should be not considered in the analysis"""
    # https://github.com/jump-cellpainting/jump-orf-analysis/issues/1#issuecomment-921888625
    # Low concentration plates
    redlist = set(["BR00127147", "BR00127148", "BR00127145", "BR00127146"])
    # https://github.com/jump-cellpainting/aws/issues/70#issuecomment-1182444836
    redlist.add("BR00123528A")

    # filter ORF plates.
    metadata = pd.read_csv("inputs/experiment-metadata.tsv", sep="\t")
    query = 'Batch=="Batch12"'
    bad_plates = set(metadata.query(query).Assay_Plate_Barcode)
    redlist |= bad_plates
    return redlist


SOURCE3_BATCH_REDLIST = {
    "CP_32_all_Phenix1",
    "CP_33_all_Phenix1",
    "CP_34_mix_Phenix1",
    "CP_35_all_Phenix1",
    "CP_36_all_Phenix1",
    "CP59",
    "CP60",
}


def build_path(row: pd.Series) -> str:
    """Create the path to the parquet file"""
    template = (
        "./inputs/{Metadata_Source}/workspace/profiles/"
        "{Metadata_Batch}/{Metadata_Plate}/{Metadata_Plate}.parquet"
    )
    return template.format(**row.to_dict())


def get_plate_metadata(sources: list[str], plate_types: list[str]) -> pd.DataFrame:
    """Create filtered metadata DataFrame"""
    plate_metadata = pd.read_csv("./inputs/metadata/plate.csv.gz")
    # Filter plates from source_4
    if "ORF" in plate_types:
        redlist = get_orf_plate_redlist(plate_types)
        plate_metadata = plate_metadata[~plate_metadata["Metadata_Plate"].isin(redlist)]

    # Filter plates from source_3 batches without DMSO
    plate_metadata = plate_metadata[
        (~plate_metadata["Metadata_Batch"].isin(SOURCE3_BATCH_REDLIST))
        | (plate_metadata["Metadata_PlateType"] == "TARGET2")
    ]

    plate_metadata = plate_metadata[plate_metadata["Metadata_Source"].isin(sources)]
    plate_metadata = plate_metadata[
        plate_metadata["Metadata_PlateType"].isin(plate_types)
    ]
    return plate_metadata


def get_well_metadata(plate_types: list[str]):
    """Load well metadata"""
    well_metadata = pd.read_csv("./inputs/metadata/well.csv.gz")
    if "ORF" in plate_types:
        orf_metadata = pd.read_csv("./inputs/metadata/orf.csv.gz")
        well_metadata = well_metadata.merge(
            orf_metadata, how="left", on="Metadata_JCP2022"
        )
        # well_metadata = well_metadata[well_metadata['Metadata_pert_type']!='poscon']
    if "CRISPR" in plate_types:
        crispr_metadata = pd.read_csv("./inputs/metadata/crispr.csv.gz")
        well_metadata = well_metadata.merge(
            crispr_metadata, how="left", on="Metadata_JCP2022"
        )
    # Filter out wells

    well_metadata = well_metadata[
        ~well_metadata["Metadata_JCP2022"].isin([UNTREATED, UNKNOWN, BAD_CONSTRUCT])
    ]

    return well_metadata


def load_metadata(sources: list[str], plate_types: list[str]):
    """Load metadata only"""
    plate = get_plate_metadata(sources, plate_types)
    well = get_well_metadata(plate_types)
    meta = well.merge(plate, on=["Metadata_Source", "Metadata_Plate"])
    return meta
