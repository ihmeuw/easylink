import copy
import json
import os
import re

import jellyfish
import numpy as np
import pandas as pd
from splink.duckdb.linker import DuckDBLinker


def load_file(file_path, file_format=None):
    if file_format is None:
        file_format = file_path.split(".")[-1]
    if file_format == "parquet":
        return pd.read_parquet(file_path)
    if file_format == "csv":
        return pd.read_csv(file_path)
    raise ValueError()


census_2030_path = os.environ["SIMULATED_CENSUS"].split(",")[0]
reference_file_path = os.environ["REFERENCE_FILE"].split(",")[1]
census_2030 = load_file(census_2030_path)
reference_file = load_file(reference_file_path)

census_2030_raw_input = census_2030.copy()


# Nickname processing
# Have not yet found a nickname list in PVS docs,
# so we do a minimal version for now -- could use
# another list such as the one in pseudopeople
# These examples all come directly from examples in the descriptions of PVS
nickname_standardizations = {
    "Bill": "William",
    "Chuck": "Charles",
    "Charlie": "Charles",
    "Cathy": "Catherine",
    "Matt": "Matthew",
}
has_nickname = census_2030.first_name.isin(nickname_standardizations.keys())
print(f"{has_nickname.sum()} nicknames in the Census")


census_2030 = pd.concat(
    [
        census_2030,
        census_2030[has_nickname].assign(
            first_name=lambda df: df.first_name.replace(nickname_standardizations)
        ),
    ],
    ignore_index=True,
)


# Note: The above will introduce duplicates on record_id, so we redefine
# record_id to be unique (without getting rid of the original, input file record ID)
def add_unique_id_col(df, col_name="unique_id", value_prefix=""):
    return (
        df.reset_index()
        .rename(columns={"index": col_name})
        .assign(**{col_name: lambda df: value_prefix + df[col_name].astype(str)})
    )


def add_unique_record_id(df, dataset_name):
    return add_unique_id_col(df, col_name="record_id", value_prefix=f"{dataset_name}_")


census_2030 = add_unique_record_id(
    census_2030.rename(columns={"record_id": "record_id_raw_input_file"}),
    "census_2030_preprocessed",
)


def standardize_address_part(column):
    return (
        column
        # Remove leading or trailing whitespace
        .str.strip()
        # Turn any strings of consecutive whitespace into a single space
        .str.replace("\s+", " ", regex=True)
        # Normalize case
        .str.upper()
        # Normalize the word street as described in the example quoted above
        # In reality, there would be many rules like this
        .str.replace("\b(STREET|STR)\b", "ST", regex=True)
        # Make sure missingness is represented consistently
        .replace("", pd.NA)
    )


address_cols = ["street_number", "street_name", "unit_number", "city", "state", "zipcode"]
for address_col in address_cols:
    census_2030[address_col] = census_2030[address_col].pipe(standardize_address_part)


census_2030 = census_2030[census_2030.first_name.notnull() | census_2030.last_name.notnull()]


reference_file = reference_file.rename(columns=lambda c: c.replace("mailing_address_", ""))


def split_dob(df, date_format="%Y%m%d"):
    df = df.copy()
    # Have to be floats because we want to treat as numeric for assessing similarity
    # Note that as of now, none of our pseudopeople noise types would change the punctuation ("/") in the date, but
    # they can insert non-numeric characters here or otherwise create invalid dates, in which case we fail to parse the date
    # and treat it as missing.
    dob = pd.to_datetime(df.date_of_birth, format=date_format, errors="coerce")
    df["month_of_birth"] = dob.dt.month
    df["year_of_birth"] = dob.dt.year
    df["day_of_birth"] = dob.dt.day
    return df.drop(columns=["date_of_birth"])


census_2030 = split_dob(census_2030, date_format="%m/%d/%Y")
reference_file = split_dob(reference_file)


# I don't fully understand the purpose of blocking on the geokey,
# as opposed to just blocking on its constituent columns.
# Maybe it is a way of dealing with missingness in those constituent
# columns (e.g. so an address with no unit number can still be blocked on geokey)?


def add_geokey(df):
    df = df.copy()
    strings = [
        df.street_number,
        " ",
        df.street_name,
        " ",
        df.unit_number.fillna(""),
        " ",
        df.city,
        " ",
        df.state.astype(str),
        " ",
        df.zipcode,
    ]
    geokey_str = ""
    for string in strings:
        geokey_str += string
    df["geokey"] = geokey_str
    # Normalize the whitespace -- necessary if the unit number was null
    df["geokey"] = df.geokey.str.replace("\s+", " ", regex=True)
    return df


reference_file = add_geokey(reference_file)
census_2030 = add_geokey(census_2030)


# Layne, Wagner, and Rothhaas p. 26: the name matching variables are
# First 15 characters First Name, First 15 characters Middle Name, First 12 characters Last Name
# Additionally, there are blocking columns for all of 1-3 initial characters of First/Last.
# We don't have a full middle name in pseudopeople (nor would that be present in a real CUF)
# so we have to stick to the first initial for middle.
def add_truncated_name_cols(df):
    df = df.copy()
    df["first_name_15"] = df.first_name.str[:15]
    df["last_name_12"] = df.last_name.str[:12]

    if "middle_name" in df.columns and "middle_initial" not in df.columns:
        df["middle_initial"] = df.middle_name.str[:1]

    for num_chars in [1, 2, 3]:
        df[f"first_name_{num_chars}"] = df.first_name.str[:num_chars]
        df[f"last_name_{num_chars}"] = df.last_name.str[:num_chars]

    return df


census_2030 = add_truncated_name_cols(census_2030)
reference_file = add_truncated_name_cols(reference_file)


# Layne, Wagner, and Rothhaas p. 26: phonetics are used in blocking (not matching)
# - Soundex for Street Name
# - NYSIIS code for First Name
# - NYSIIS code for Last Name
# - Reverse Soundex for First Name
# - Reverse Soundex for Last Name


def nysiis(input_string):
    result = jellyfish.nysiis(input_string)
    if result is None:
        return pd.NA

    return result


def soundex(input_string):
    result = jellyfish.soundex(input_string)
    if result is None:
        return pd.NA

    return result


def add_name_phonetics(df):
    df = df.copy()
    for col in ["first_name", "last_name"]:
        kwargs = {}
        df[f"{col}_nysiis"] = df[col].dropna().apply(nysiis, **kwargs)
        df[f"{col}_reverse_soundex"] = df[col].dropna().str[::-1].apply(soundex, **kwargs)

    return df


def add_address_phonetics(df):
    df = df.copy()
    kwargs = {}
    df["street_name_soundex"] = df.street_name.dropna().apply(jellyfish.soundex, **kwargs)
    return df


census_2030 = add_name_phonetics(census_2030)
census_2030 = add_address_phonetics(census_2030)
reference_file = add_address_phonetics(reference_file)


def add_zip3(df):
    return df.assign(zip3=lambda x: x.zipcode.str[:3])


def add_first_last_initial_categories(df):
    # Page 20 of the NORC report: "Name-cuts are defined by combinations of the first characters of the first and last names. The twenty letter groupings
    # for the first character are: A-or-blank, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, and U-Z."
    initial_cut = (
        lambda x: x.fillna("A")
        .str[0]
        .replace("A", "A-or-blank")
        .replace(["U", "V", "W", "X", "Y", "Z"], "U-Z")
    )
    return df.assign(
        first_initial_cut=lambda x: initial_cut(x.first_name),
        last_initial_cut=lambda x: initial_cut(x.last_name),
    )


census_2030 = add_zip3(census_2030)
census_2030 = add_first_last_initial_categories(census_2030)
reference_file = add_zip3(reference_file)

with open("/splink_model_params.json", "r") as f:
    splink_settings = json.load(f)

PROBABILITY_THRESHOLD = 0.85


common_cols = [c for c in reference_file.columns if c in census_2030.columns]
reference_file_index_of_ids = reference_file.reset_index().set_index("record_id")["index"]
census_index_of_ids = census_2030.reset_index().set_index("record_id")["index"]


def prep_table_for_splink(df, dataset_name):
    return df[common_cols].assign(dataset_name=dataset_name)


def pvs_matching_pass(blocking_cols, matching_cols):
    tables_for_splink = [
        prep_table_for_splink(reference_file, "reference_file"),
        prep_table_for_splink(census_2030[census_2030.pik.isnull()], "census_2030"),
    ]
    pass_splink_settings = copy.deepcopy(splink_settings)
    pass_splink_settings["comparisons"] = [
        c
        for c in pass_splink_settings["comparisons"]
        if c["output_column_name"] in matching_cols
    ]

    blocking_rule_parts = [f"l.{col} = r.{col}" for col in blocking_cols]
    blocking_rule = " and ".join(blocking_rule_parts)
    linker = DuckDBLinker(
        tables_for_splink,
        {
            **pass_splink_settings,
            **{
                "blocking_rules_to_generate_predictions": [blocking_rule],
            },
        },
        # Must match order of tables_for_splink
        input_table_aliases=["reference_file", "census_2030"],
    )

    potential_links = linker.predict(
        threshold_match_probability=PROBABILITY_THRESHOLD
    ).as_pandas_dataframe()
    # Name the columns better than "_r" and "_l"
    # In practice it seems to always be one dataset on the right and another on the left,
    # but it's "backwards" relative to the order above and I don't want to rely on it
    potential_links_census_left = potential_links[
        potential_links.source_dataset_l == "census_2030"
    ]
    assert (potential_links_census_left.source_dataset_r == "reference_file").all()
    potential_links_census_left = potential_links_census_left.rename(
        columns=lambda c: re.sub("_l$", "_census_2030", c)
    ).rename(columns=lambda c: re.sub("_r$", "_reference_file", c))

    potential_links_reference_left = potential_links[
        potential_links.source_dataset_l == "reference_file"
    ]
    assert (potential_links_reference_left.source_dataset_r == "census_2030").all()
    potential_links_reference_left = potential_links_reference_left.rename(
        columns=lambda c: re.sub("_l$", "_reference_file", c)
    ).rename(columns=lambda c: re.sub("_r$", "_census_2030", c))

    assert len(potential_links) == len(potential_links_census_left) + len(
        potential_links_reference_left
    )
    potential_links = pd.concat(
        [potential_links_census_left, potential_links_reference_left], ignore_index=True
    )

    print(f"{len(potential_links)} links above threshold")

    # Post-processing: deal with multiple matches
    # Do not consider multiple matches for the same record_id; simply do not link them.
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
            f"{len(census_records_with_multiple_potential_piks)} input records matched to multiple PIKs, dropping them from list of potential matches"
        )

    potential_links = potential_links[
        ~potential_links.record_id_census_2030.isin(
            census_records_with_multiple_potential_piks
        )
    ]

    assert (potential_links.groupby("record_id_census_2030").pik.nunique() == 1).all()
    links = potential_links.groupby("record_id_census_2030").pik.first().reset_index()
    census_2030.loc[
        census_index_of_ids.loc[links.record_id_census_2030], "pik"
    ] = links.pik.values.astype(int)

    print(
        f"Matched {len(links)} records; {census_2030.pik.isnull().mean():.2%} still eligible to match"
    )

    # Diagnostic showing the predicted values for each combination of column similarity values
    all_predictions = linker.predict().as_pandas_dataframe()
    all_combos = (
        all_predictions.groupby(list(all_predictions.filter(like="gamma_").columns))
        .match_probability.agg(["mean", "count"])
        .sort_values("mean")
    )

    return all_combos, links


if "pik" not in census_2030.columns:
    # If 'pik' column does not exist, create it and fill all its rows with np.nan
    census_2030["pik"] = np.nan
    census_2030["pik"] = census_2030["pik"].astype("Int64")


blocking_cols = os.getenv("BLOCKING_COLS").split(",")
matching_cols = os.getenv("MATCHING_COLS").split(",")
all_combos, pik_pairs = pvs_matching_pass(blocking_cols, matching_cols)


output_file_path = os.getenv("DUMMY_CONTAINER_OUTPUT_PATHS")
final_output = pd.merge(
    census_2030_raw_input,
    census_2030[["record_id_raw_input_file", "pik"]],
    left_on="record_id",
    right_on="record_id_raw_input_file",
    how="left",
    suffixes=("_raw", "_updated"),
)
final_output = final_output.drop(columns=["record_id_raw_input_file"]).drop_duplicates()
if "pik" in census_2030_raw_input:
    # Maybe show "bad cases" through validation errors
    final_output = final_output.rename(columns={"pik_updated": "pik"}).drop(
        columns=["pik_raw"]
    )
final_output.to_parquet(output_file_path)
