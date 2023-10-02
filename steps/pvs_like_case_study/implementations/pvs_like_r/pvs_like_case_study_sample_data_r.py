#!/usr/bin/env python3
# coding: utf-8

# This script was automatically generated from a Jupyter notebook.
# Edit with care -- substantive edits should go in the notebook,
# or they will be overwritten the next time this script is generated.

import re
from pathlib import Path

import numpy as np
import pandas as pd

input_file_dir = Path("/input_data")
reference_file = pd.read_parquet(input_file_dir / "reference_file_sample.parquet")
census_2030 = pd.read_parquet(input_file_dir / "census_2030_sample.parquet")

# Use NaN for all forms of missingness, including empty string
reference_file = reference_file.fillna(np.nan).replace("", np.nan)
census_2030 = census_2030.fillna(np.nan).replace("", np.nan)

# We want to compare mailing address with physical address
reference_file = reference_file.rename(columns=lambda c: c.replace("mailing_address_", ""))

# My working theory: the purpose of the "geokey" is because address parts violate conditional independence
get_geokey = (
    lambda x: (
        x.street_number
        + " "
        + x.street_name
        + " "
        + x.unit_number.fillna("")
        + " "
        + x.city
        + " "
        + x.state.astype(str)
        + " "
        + x.zipcode
    )
    .str.strip()
    .str.split()
    .str.join(" ")
)
reference_file = reference_file.assign(geokey=get_geokey)
census_2030 = census_2030.assign(geokey=get_geokey)

# Add columns used to "cut the database": ZIP3 and a grouping of first and last initial
reference_file = reference_file.assign(zip3=lambda x: x.zipcode.str[:3])
census_2030 = census_2030.assign(zip3=lambda x: x.zipcode.str[:3])

# Page 20 of the NORC report: "Name-cuts are defined by combinations of the first characters of the first and last names. The twenty letter groupings
# for the first character are: A-or-blank, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, and U-Z."
initial_cut = (
    lambda x: x.fillna("A")
    .str[0]
    .replace("A", "A-or-blank")
    .replace(["U", "V", "W", "X", "Y", "Z"], "U-Z")
)
reference_file = reference_file.assign(
    first_initial_cut=lambda x: initial_cut(x.first_name),
    last_initial_cut=lambda x: initial_cut(x.last_name),
)
census_2030 = census_2030.assign(
    first_initial_cut=lambda x: initial_cut(x.first_name),
    last_initial_cut=lambda x: initial_cut(x.last_name),
)

reference_file

census_2030

common_cols = [c for c in reference_file.columns if c in census_2030.columns]
common_cols

import os
import pathlib
import sys

os.environ["R_HOME"] = str(pathlib.Path(sys.executable).parent.parent / "lib/R")

from rpy2.robjects import pandas2ri
from rpy2.robjects.packages import importr

pandas2ri.activate()

fastLink = importr("fastLink")

# %%time

# From the fastLink README:
# ## Run the algorithm on the random samples
# rs.out <- fastLink(
#   dfA = dfA.s, dfB = dfB.s,
#   varnames = c("firstname", "middlename", "lastname", "housenum", "streetname", "city", "birthyear"),
#   stringdist.match = c("firstname", "middlename", "lastname", "streetname", "city"),
#   partial.match = c("firstname", "lastname", "streetname"),
#   estimate.only = TRUE
# )

from rpy2 import robjects as ro

COMPARISON_COLUMNS = ["first_name", "middle_initial", "last_name", "date_of_birth", "geokey"]

prep_for_fastLink = (
    lambda df: df[COMPARISON_COLUMNS]
    .astype(str)
    .fillna(ro.NA_Character)
    .reset_index()
    .rename(columns={"index": "python_index"})
)

em_object = fastLink.fastLink(
    dfA=prep_for_fastLink(reference_file),
    dfB=prep_for_fastLink(census_2030),
    varnames=ro.StrVector(COMPARISON_COLUMNS),
    stringdist_match=ro.StrVector(["first_name", "last_name", "geokey"]),
    partial_match=ro.StrVector(["first_name", "last_name", "geokey"]),
    # Just run EM, don't link
    estimate_only=True,
)

PROBABILITY_THRESHOLD = 0.85

base = importr("base")

# Calculate this once to save time -- mapping from record_id to index of each dataframe
reference_file_index_of_ids = reference_file.reset_index().set_index("record_id")["index"]
census_index_of_ids = census_2030.reset_index().set_index("record_id")["index"]


# TODO: Have this function output more charts and diagnostics
def matching_pass_no_blocking():
    # fastLink really doesn't work well with blocking -- it requires you to call it separately
    # for each block, and with restrictive blocks, this is much, much slower than not using blocking
    # at all.
    census_to_match = census_2030[census_2030.pik.isnull()]

    # If we had wanted to do blocking, we would have done it something like this:
    # # fastLink's blocking (blockData) doesn't support blocking on multiple columns at once(!),
    # # so we implement our own blocking
    # # Technically we could have done it with some hacky approach involving appending the columns,
    # # but fastLink still requires you to make a separate linking call for each block anyway
    # census_2030_groups = census_2030[census_2030.pik.isnull()].groupby(blocking_cols, as_index=False)
    # reference_file_groups = reference_file.groupby(blocking_cols, as_index=False)

    # print(f'{census_2030_groups.ngroups} blocks')

    # varnames = ro.StrVector(COMPARISON_COLUMNS)
    # stringdist_match = ro.StrVector(["first_name", "last_name", "geokey"])
    # partial_match = ro.StrVector(["first_name", "last_name", "geokey"])

    # potential_links = []
    # for index, (key, census_2030_block) in enumerate(census_2030_groups):

    # try:
    #     reference_file_block = reference_file_groups.get_group(key)
    # except KeyError:
    #     # Nothing in the reference file for this block; so that implies there are no
    #     # matches to find
    #     continue

    # if len(reference_file_block) == 1:
    #     # HACK -- fastLink seems to not work at all if dfB is only one row
    #     reference_file_block = pd.concat([reference_file_block, pd.DataFrame(np.nan, index=[-1], columns=reference_file_block.columns)])

    # Then the rest of this logic until we print the number of potential links would be inside the loop

    with (ro.default_converter + pandas2ri.converter).context():
        conversion = ro.conversion.get_conversion()
        census_2030_r = conversion.py2rpy(prep_for_fastLink(census_to_match))
        reference_file_r = conversion.py2rpy(prep_for_fastLink(reference_file))

    fastLink_result = fastLink.fastLink(
        dfA=census_2030_r,
        dfB=reference_file_r,
        varnames=ro.StrVector(COMPARISON_COLUMNS),
        stringdist_match=ro.StrVector(["first_name", "last_name", "geokey"]),
        partial_match=ro.StrVector(["first_name", "last_name", "geokey"]),
        em_obj=em_object,
        threshold_match=PROBABILITY_THRESHOLD,
    )

    census_2030_matches_r_indices = fastLink_result.rx2("matches").rx2("inds.a")
    reference_file_matches_r_indices = fastLink_result.rx2("matches").rx2("inds.b")

    census_2030_matches = pd.Index(
        census_2030_r.rx(census_2030_matches_r_indices, "python_index")
    )
    reference_file_matches = pd.Index(
        reference_file_r.rx(reference_file_matches_r_indices, "python_index")
    )

    potential_links = (
        census_to_match.loc[census_2030_matches]
        .reset_index(drop=True)
        .add_suffix("_census_2030")
        .join(
            reference_file.loc[reference_file_matches]
            .reset_index(drop=True)
            .add_suffix("_reference_file")
        )
    )

    print(f"{len(potential_links)} links above threshold")

    # Post-processing: deal with multiple matches
    # According to the report, a record is considered not linkable if it has multiple matches above the threshold
    # I represent "not linkable" here with a PIK of -1 (different from NaN, which means yet-to-be-linked)
    potential_links = potential_links.merge(
        reference_file[["record_id", "pik"]],
        left_on="record_id_reference_file",
        right_on="record_id",
        how="left",
    ).drop(columns=["record_id"])
    print(f"{potential_links.record_id_census_2030.nunique()} input records have a match")
    census_records_with_multiple_potential_piks = (
        potential_links.groupby("record_id_census_2030")
        .pik.nunique()
        .pipe(lambda c: c[c > 1])
        .index
    )
    if len(census_records_with_multiple_potential_piks) > 0:
        print(
            f"{len(census_records_with_multiple_potential_piks)} input records matched to multiple PIKs, marking as unlinkable"
        )

    potential_links.loc[
        potential_links.record_id_census_2030.isin(
            census_records_with_multiple_potential_piks
        ),
        "pik",
    ] = -1

    assert (potential_links.groupby("record_id_census_2030").pik.nunique() == 1).all()
    links = potential_links.groupby("record_id_census_2030").pik.first().reset_index()
    census_2030.loc[
        census_index_of_ids.loc[links.record_id_census_2030], "pik"
    ] = links.pik.values

    print(
        f"Matched {len(links)} records; {census_2030.pik.isnull().mean():.2%} still eligible to match"
    )

    # Diagnostic showing the predicted values for each combination of column similarity values
    # Not trivial to do this with fastLink when there is blocking, so we skip it

    return None, links


census_2030["pik"] = np.nan

# %%time

all_combos, pik_pairs = matching_pass_no_blocking()

pik_pairs

# Sentinel value represents matching to more than one PIK
census_2030[census_2030.pik == -1]

census_2030.loc[census_2030.pik == -1, "pik"] = np.nan

census_2030

census_2030.pik.notnull().mean()

census_2030_ground_truth = (
    pd.read_parquet(input_file_dir / "census_2030_ground_truth_sample.parquet")
    .set_index("record_id")
    .simulant_id
)
reference_file_ground_truth = (
    pd.read_parquet(input_file_dir / "reference_file_ground_truth_sample.parquet")
    .set_index("record_id")
    .simulant_id
)

# Not possible to be PIKed, since they are truly not in the reference file
(~census_2030_ground_truth.isin(reference_file_ground_truth)).mean()

census_2030.pik.notnull().mean() / census_2030_ground_truth.isin(
    reference_file_ground_truth
).mean()

# Multiple Census rows assigned the same PIK, indicating the model thinks they are duplicates in Census
census_2030.pik.value_counts().value_counts()

# However, in this version of pseudopeople, there are no actual duplicates in Census
assert not census_2030_ground_truth.duplicated().any()

# Interesting: in pseudopeople, sometimes siblings are assigned the same (common) first name, making them almost identical.
# The only giveaway is their age and DOB.
# Presumably, this tends not to happen in real life.
duplicate_piks = census_2030.pik.value_counts()[census_2030.pik.value_counts() > 1].index
census_2030[census_2030.pik.isin(duplicate_piks)].sort_values("pik")

pik_simulant_id = census_2030.pik.map(reference_file_ground_truth)
pik_simulant_id

(
    pik_simulant_id[pik_simulant_id.notnull()]
    == census_2030_ground_truth[pik_simulant_id.notnull()]
).mean()

errors = census_2030[
    census_2030.pik.notnull() & (pik_simulant_id != census_2030_ground_truth)
]
confused_for = (
    reference_file.set_index("record_id")
    .loc[errors.pik]
    .reset_index()
    .set_index(errors.index)
)
errors[common_cols].compare(confused_for[common_cols], keep_shape=True, keep_equal=True)

census_2030.to_parquet("census_2030_with_piks_sample.parquet")

# Convert this notebook to a Python script
# ! ./convert_notebook.sh pvs_like_case_study_sample_data_r
