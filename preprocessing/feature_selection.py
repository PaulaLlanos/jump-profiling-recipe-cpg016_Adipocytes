import logging

import pandas as pd

from pycytominer.operations import correlation_threshold, variance_threshold

from .metadata import find_feat_cols

logger = logging.getLogger(__name__)


def select_features(dframe_path, feat_selected_path, keep_image_features):
    """Run feature selection"""
    dframe = pd.read_parquet(dframe_path)
    features = find_feat_cols(dframe.columns)
    low_variance = variance_threshold(dframe, features)
    features = [f for f in features if f not in low_variance]
    logger.info(f"{len(low_variance)} features removed by variance_threshold")
    high_corr = correlation_threshold(dframe, features)
    features = [f for f in features if f not in high_corr]
    logger.info(f"{len(high_corr)} features removed by correlation_threshold")

    dframe.drop(columns=low_variance + high_corr, inplace=True)

    cols = find_feat_cols(dframe.columns)
    with open("blocklist_features.txt", "r") as fpointer:
        blocklist = fpointer.read().splitlines()[1:]
    blocklist = [c for c in cols if c in blocklist]
    dframe.drop(columns=blocklist, inplace=True)
    logger.info(f"{len(blocklist)} features removed by blocklist")

    if not keep_image_features:
        cols = find_feat_cols(dframe.columns)
        img_features = [c for c in cols if c.startswith("Image")]
        dframe.drop(columns=img_features, inplace=True)
        logger.info(f"{len(img_features)} Image features removed")

    cols = find_feat_cols(dframe.columns)
    nan_cols = [c for c in cols if dframe[c].isna().any()]
    dframe.drop(columns=nan_cols, inplace=True)
    logger.info(f"{len(nan_cols)} features removed due to NaN values")

    dframe.reset_index(drop=True).to_parquet(feat_selected_path)
    
def test_feature_select_noise_removal():
    """
    Testing noise_removal feature selection operation
    """
    # Set perturbation groups for the test dataframes
    data_df_groups = ["a", "a", "a", "b", "b", "b"]

    # Tests on data_df
    result1 = feature_select(
        profiles=data_df,
        features=data_df.columns.tolist(),
        samples="all",
        operation="noise_removal",
        noise_removal_perturb_groups=data_df_groups,
        noise_removal_stdev_cutoff=2.5,
    )
    result2 = feature_select(
        profiles=data_df,
        features=data_df.columns.tolist(),
        operation="noise_removal",
        noise_removal_perturb_groups=data_df_groups,
        noise_removal_stdev_cutoff=2,
    )
    result3 = feature_select(
        profiles=data_df,
        features=data_df.columns.tolist(),
        operation="noise_removal",
        noise_removal_perturb_groups=data_df_groups,
        noise_removal_stdev_cutoff=3.5,
    )

    expected_result1 = data_df[["x", "y"]]
    expected_result2 = data_df[[]]
    expected_result3 = data_df[["x", "y", "z", "zz"]]

    pd.testing.assert_frame_equal(result1, expected_result1)
    pd.testing.assert_frame_equal(result2, expected_result2)
    pd.testing.assert_frame_equal(result3, expected_result3)

    # Test on data_unique_test_df, which has 100 rows
    data_unique_test_df_groups = []

    # Create a 100 element list containing 10 replicates of 10 perturbations
    for elem in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]:
        data_unique_test_df_groups.append([elem] * 10)

    # Unstack so it's just a single list
    data_unique_test_df_groups = [
        item for sublist in data_unique_test_df_groups for item in sublist
    ]

    result4 = feature_select(
        profiles=data_unique_test_df,
        features=data_unique_test_df.columns.tolist(),
        operation="noise_removal",
        noise_removal_perturb_groups=data_unique_test_df_groups,
        noise_removal_stdev_cutoff=3.5,
    )
    result5 = feature_select(
        profiles=data_unique_test_df,
        features=data_unique_test_df.columns.tolist(),
        operation="noise_removal",
        noise_removal_perturb_groups=data_unique_test_df_groups,
        noise_removal_stdev_cutoff=500,
    )

    expected_result4 = data_unique_test_df[["a", "b"]]
    expected_result5 = data_unique_test_df[["a", "b", "c", "d"]]

    pd.testing.assert_frame_equal(result4, expected_result4)
    pd.testing.assert_frame_equal(result5, expected_result5)

    # Test the same as above, except that data_unique_test_df_groups is now made into a metadata column
    data_unique_test_df2 = data_unique_test_df.copy()
    data_unique_test_df2["perturb_group"] = data_unique_test_df_groups
    result4b = feature_select(
        profiles=data_unique_test_df2,
        features=data_unique_test_df.columns.tolist(),
        operation="noise_removal",
        noise_removal_perturb_groups="perturb_group",
        noise_removal_stdev_cutoff=3.5,
    )
    result5b = feature_select(
        profiles=data_unique_test_df2,
        features=data_unique_test_df.columns.tolist(),
        operation="noise_removal",
        noise_removal_perturb_groups="perturb_group",
        noise_removal_stdev_cutoff=500,
    )

    expected_result4b = data_unique_test_df2[["a", "b", "perturb_group"]]
    expected_result5b = data_unique_test_df2[["a", "b", "c", "d", "perturb_group"]]

    pd.testing.assert_frame_equal(result4b, expected_result4b)
    pd.testing.assert_frame_equal(result5b, expected_result5b)

    # Test assertion errors for the user inputting the perturbation groupings
    bad_perturb_list = ["a", "a", "b", "b", "a", "a", "b"]

    with pytest.raises(
        ValueError
    ):  # When the inputted perturb list doesn't match the length of the data
        feature_select(
            data_df,
            features=data_df.columns.tolist(),
            operation="noise_removal",
            noise_removal_perturb_groups=bad_perturb_list,
            noise_removal_stdev_cutoff=3,
        )

    with pytest.raises(
        ValueError
    ):  # When the perturb list is inputted as string, but there is no such metadata column in the population_df
        feature_select(
            profiles=data_df,
            features=data_df.columns.tolist(),
            operation="noise_removal",
            noise_removal_perturb_groups="bad_string",
            noise_removal_stdev_cutoff=2.5,
        )

    with pytest.raises(
        TypeError
    ):  # When the perturbation groups are not either a list or metadata column string
        feature_select(
            profiles=data_df,
            features=data_df.columns.tolist(),
            operation="noise_removal",
            noise_removal_perturb_groups=12345,
            noise_removal_stdev_cutoff=2.5,
        )

    with pytest.raises(
        ValueError
    ):  # When the perturbation group doesn't match b/c samples argument used
        # Add metadata_sample column
        data_sample_id_df = data_df.assign(
            Metadata_sample=[f"sample_{x}" for x in range(0, data_df.shape[0])]
        )
        feature_select(
            profiles=data_sample_id_df,
            features=data_df.columns.tolist(),
            samples="Metadata_sample != 'sample_1'",
            operation="noise_removal",
            noise_removal_perturb_groups=data_df_groups,
            noise_removal_stdev_cutoff=2.5,
        )

