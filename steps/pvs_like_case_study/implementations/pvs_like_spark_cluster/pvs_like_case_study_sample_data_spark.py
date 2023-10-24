#!/usr/bin/env python3
# coding: utf-8

# This script was automatically generated from a Jupyter notebook.
# Edit with care -- substantive edits should go in the notebook,
# or they will be overwritten the next time this script is generated.

import re, os
import pandas as pd, numpy as np
    
reference_file = pd.read_parquet('reference_file_sample.parquet')
census_2030 = pd.read_parquet('census_2030_sample.parquet')
    
# Use NaN for all forms of missingness, including empty string
reference_file = reference_file.fillna(np.nan).replace('', np.nan)
census_2030 = census_2030.fillna(np.nan).replace('', np.nan)
    
# We want to compare mailing address with physical address
reference_file = reference_file.rename(columns=lambda c: c.replace('mailing_address_', ''))
    
# My working theory: the purpose of the "geokey" is because address parts violate conditional independence
get_geokey = lambda x: (x.street_number + ' ' + x.street_name + ' ' + x.unit_number.fillna('') + ' ' + x.city + ' ' + x.state.astype(str) + ' ' + x.zipcode).str.strip().str.split().str.join(' ')
reference_file = reference_file.assign(geokey=get_geokey)
census_2030 = census_2030.assign(geokey=get_geokey)
    
# Add columns used to "cut the database": ZIP3 and a grouping of first and last initial
reference_file = reference_file.assign(zip3=lambda x: x.zipcode.str[:3])
census_2030 = census_2030.assign(zip3=lambda x: x.zipcode.str[:3])

# Page 20 of the NORC report: "Name-cuts are defined by combinations of the first characters of the first and last names. The twenty letter groupings
# for the first character are: A-or-blank, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, and U-Z."
initial_cut = lambda x: x.fillna('A').str[0].replace('A', 'A-or-blank').replace(['U', 'V', 'W', 'X', 'Y', 'Z'], 'U-Z')
reference_file = reference_file.assign(first_initial_cut=lambda x: initial_cut(x.first_name), last_initial_cut=lambda x: initial_cut(x.last_name))
census_2030 = census_2030.assign(first_initial_cut=lambda x: initial_cut(x.first_name), last_initial_cut=lambda x: initial_cut(x.last_name))
    
reference_file
    
census_2030
    
common_cols = [c for c in reference_file.columns if c in census_2030.columns]
common_cols
    
def prep_table_for_splink(df, dataset_name):
    return (
        df[common_cols]
            .assign(dataset_name=dataset_name)
    )

tables_for_splink = [prep_table_for_splink(reference_file, "reference_file"), prep_table_for_splink(census_2030, "census_2030")]
    
[len(t) for t in tables_for_splink]
    
# estimate_probability_two_random_records_match did not seem to give me a reasonable estimate
# we estimate that around 90% of the census are present in the reference file
probability_two_random_records_match = (0.90 * len(census_2030)) / (len(reference_file) * len(census_2030))
probability_two_random_records_match
    
from splink.spark.comparison_library import (
    exact_match,
    levenshtein_at_thresholds,
)

settings = {
    "link_type": "link_only",
    "comparisons": [
        levenshtein_at_thresholds("first_name", 2, term_frequency_adjustments=True),
        exact_match("middle_initial"),
        levenshtein_at_thresholds("last_name", 2, term_frequency_adjustments=True),
        # For some reason, this makes everything crash!?
        # levenshtein_at_thresholds("date_of_birth", 1),
        exact_match("date_of_birth"),
        levenshtein_at_thresholds("geokey", 5),
    ],
    "probability_two_random_records_match": probability_two_random_records_match,
    "unique_id_column_name": "record_id",
}

# https://moj-analytical-services.github.io/splink/demos/examples/spark/deduplicate_1k_synthetic.html
from splink.spark.jar_location import similarity_jar_location

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import types

conf = SparkConf()
conf.setMaster(os.getenv("LINKER_SPARK_MASTER_URL", "local[2]"))
conf.set("spark.driver.memory", "12g")
conf.set("spark.default.parallelism", "2")

# Add custom similarity functions, which are bundled with Splink
# documented here: https://github.com/moj-analytical-services/splink_scalaudfs
path = similarity_jar_location()
conf.set("spark.jars", path)

sc = SparkContext.getOrCreate(conf=conf)

spark = SparkSession(sc)
spark.sparkContext.setCheckpointDir("./tmpCheckpoints")

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import types
schema = types.StructType([types.StructField(c, types.IntegerType() if c == "unique_id" else types.StringType(), True) for c in tables_for_splink[0].columns])
spark_tables = [spark.createDataFrame(df, schema) for df in tables_for_splink]

from splink.spark.linker import SparkLinker
linker = SparkLinker(
    tables_for_splink,
    settings,
    input_table_aliases=["reference_file", "census_2030"],
    spark=spark,
)

import warnings
# PySpark triggers a lot of Pandas warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# linker = DuckDBLinker(
#     tables_for_splink,
#     settings,
#     input_table_aliases=["reference_file", "census_2030"]
# )
    
# So it turns out that even when using Spark, splink uses DuckDB a little bit
# I get a totally bizarre error that only happens the first time DuckDB is called
# This "flushes it out"?!?
# I have no clue why this is happening and didn't want to spend the time to investigate.
# May be a duckdb bug, or something really weird about my environment.
import duckdb
r1 = duckdb.sql('SELECT 42 AS i')

try:
    duckdb.sql('SELECT i * 2 AS k FROM r1').show()
except duckdb.InvalidInputException:
    print('Hit the error!')
    pass

duckdb.sql('SELECT i * 3 AS k FROM r1').show()
    
# NOTE: This is not reproducible!
linker.estimate_u_using_random_sampling(max_pairs=1e5)

blocking_rule_for_training = "l.first_name = r.first_name and l.last_name = r.last_name"
linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training, fix_probability_two_random_records_match=True)

blocking_rule_for_training = "l.geokey = r.geokey"
linker.estimate_parameters_using_expectation_maximisation(blocking_rule_for_training, fix_probability_two_random_records_match=True)
    
linker.match_weights_chart()
    
# NOTE: EM appears to be finding people in the same family instead of the same person!
# See first_name m probabilities.
# For now, I address this by almost always blocking on first name.
# More experimentation needed to get reasonable values here.
linker.m_u_parameters_chart()
    
linker.parameter_estimate_comparisons_chart()
    
splink_settings = linker._settings_obj.as_dict()
    
PROBABILITY_THRESHOLD = 0.85
    
# Save these variables; this means that if you restart the kernel, you don't need to run this first part of the notebook again.
# %store splink_settings PROBABILITY_THRESHOLD
    
# Calculate this once to save time -- mapping from record_id to index of each dataframe
reference_file_index_of_ids = reference_file.reset_index().set_index('record_id')['index']
census_index_of_ids = census_2030.reset_index().set_index('record_id')['index']

# TODO: Have this function output more charts and diagnostics
def pvs_matching_pass(blocking_cols):
    tables_for_splink = [prep_table_for_splink(reference_file, "reference_file"), prep_table_for_splink(census_2030[census_2030.pik.isnull()], "census_2030")]

    blocking_rule_parts = [f"l.{col} = r.{col}" for col in blocking_cols]
    blocking_rule = " and ".join(blocking_rule_parts)
    linker = SparkLinker(
        tables_for_splink,
        {**splink_settings, **{
            "blocking_rules_to_generate_predictions": [blocking_rule],
        }},
        # Must match order of tables_for_splink
        input_table_aliases=["reference_file", "census_2030"],
        spark=spark,
    )

    potential_links = (
        linker.predict(threshold_match_probability=PROBABILITY_THRESHOLD)
            .as_pandas_dataframe()
    )
    # Name the columns better than "_r" and "_l"
    # In practice it seems to always be one dataset on the right and another on the left,
    # but it's "backwards" relative to the order above and I don't want to rely on it
    potential_links_census_left = potential_links[potential_links.source_dataset_l == 'census_2030']
    assert (potential_links_census_left.source_dataset_r == 'reference_file').all()
    potential_links_census_left = (
        potential_links_census_left
            .rename(columns=lambda c: re.sub('_l$', '_census_2030', c))
            .rename(columns=lambda c: re.sub('_r$', '_reference_file', c))
    )

    potential_links_reference_left = potential_links[potential_links.source_dataset_l == 'reference_file']
    assert (potential_links_reference_left.source_dataset_r == 'census_2030').all()
    potential_links_reference_left = (
        potential_links_reference_left
            .rename(columns=lambda c: re.sub('_l$', '_reference_file', c))
            .rename(columns=lambda c: re.sub('_r$', '_census_2030', c))
    )

    assert len(potential_links) == len(potential_links_census_left) + len(potential_links_reference_left)
    potential_links = pd.concat([potential_links_census_left, potential_links_reference_left], ignore_index=True)

    print(f'{len(potential_links)} links above threshold')

    # Post-processing: deal with multiple matches
    # According to the report, a record is considered not linkable if it has multiple matches above the threshold
    # I represent "not linkable" here with a PIK of -1 (different from NaN, which means yet-to-be-linked)
    potential_links = potential_links.merge(reference_file[['record_id', 'pik']], left_on='record_id_reference_file', right_on='record_id', how='left').drop(columns=['record_id'])
    print(f'{potential_links.record_id_census_2030.nunique()} input records have a match')
    census_records_with_multiple_potential_piks = potential_links.groupby('record_id_census_2030').pik.nunique().pipe(lambda c: c[c > 1]).index
    if len(census_records_with_multiple_potential_piks) > 0:
        print(f'{len(census_records_with_multiple_potential_piks)} input records matched to multiple PIKs, marking as unlinkable')

    potential_links.loc[potential_links.record_id_census_2030.isin(census_records_with_multiple_potential_piks), 'pik'] = -1

    assert (potential_links.groupby('record_id_census_2030').pik.nunique() == 1).all()
    links = potential_links.groupby('record_id_census_2030').pik.first().reset_index()
    census_2030.loc[census_index_of_ids.loc[links.record_id_census_2030], 'pik'] = links.pik.values

    print(f'Matched {len(links)} records; {census_2030.pik.isnull().mean():.2%} still eligible to match')
    
    # Diagnostic showing the predicted values for each combination of column similarity values
    all_predictions = linker.predict().as_pandas_dataframe()
    all_combos = all_predictions.groupby(list(all_predictions.filter(like='gamma_').columns)).match_probability.agg(['mean', 'count']).sort_values('mean')
    
    return all_combos, links
    
def geosearch_pass(blocking_cols):
    return pvs_matching_pass(["zip3"] + blocking_cols)
    
census_2030['pik'] = np.nan
    
all_combos, pik_pairs = geosearch_pass(["first_name", "middle_initial", "last_name", "geokey"])
    
all_combos
    
pik_pairs
    
all_combos, pik_pairs = geosearch_pass(["first_name", "geokey"])
    
all_combos
    
pik_pairs
    
all_combos, pik_pairs = geosearch_pass(["first_name", "middle_initial", "last_name", "street_number", "street_name"])
    
all_combos
    
pik_pairs
    
all_combos, pik_pairs = geosearch_pass(["first_name", "street_number", "street_name"])
    
all_combos
    
pik_pairs
    
all_combos, pik_pairs = geosearch_pass(["first_name", "last_name"])
    
all_combos
    
pik_pairs
    
all_combos
    
pik_pairs
    
def namesearch_pass(blocking_cols):
    return pvs_matching_pass(["first_initial_cut", "last_initial_cut"] + blocking_cols)
    
all_combos, pik_pairs = namesearch_pass(["first_name", "middle_initial", "last_name", "date_of_birth"])
    
all_combos
    
pik_pairs
    
all_combos, pik_pairs = namesearch_pass(["first_name", "date_of_birth"])
    
all_combos
    
pik_pairs
    
all_combos, pik_pairs = namesearch_pass(["last_name", "date_of_birth"])
    
all_combos
    
pik_pairs
    
all_combos, pik_pairs = namesearch_pass(["date_of_birth"])
    
all_combos
    
pik_pairs
    
# Sentinel value represents matching to more than one PIK
census_2030[census_2030.pik == -1]
    
census_2030.loc[census_2030.pik == -1, 'pik'] = np.nan
    
census_2030
    
census_2030.pik.notnull().mean()
    
census_2030_ground_truth = pd.read_parquet('census_2030_ground_truth_sample.parquet').set_index('record_id').simulant_id
reference_file_ground_truth = pd.read_parquet('reference_file_ground_truth_sample.parquet').set_index('record_id').simulant_id
    
# Not possible to be PIKed, since they are truly not in the reference file
(~census_2030_ground_truth.isin(reference_file_ground_truth)).mean()
    
census_2030.pik.notnull().mean() / census_2030_ground_truth.isin(reference_file_ground_truth).mean()
    
# Multiple Census rows assigned the same PIK, indicating the model thinks they are duplicates in Census
census_2030.pik.value_counts().value_counts()
    
# However, in this version of pseudopeople, there are no actual duplicates in Census
assert not census_2030_ground_truth.duplicated().any()
    
# Interesting: in pseudopeople, sometimes siblings are assigned the same (common) first name, making them almost identical.
# The only giveaway is their age and DOB.
# Presumably, this tends not to happen in real life.
duplicate_piks = census_2030.pik.value_counts()[census_2030.pik.value_counts() > 1].index
census_2030[census_2030.pik.isin(duplicate_piks)].sort_values('pik')
    
pik_simulant_id = census_2030.pik.map(reference_file_ground_truth)
pik_simulant_id
    
(pik_simulant_id[pik_simulant_id.notnull()] == census_2030_ground_truth[pik_simulant_id.notnull()]).mean()
    
errors = census_2030[census_2030.pik.notnull() & (pik_simulant_id != census_2030_ground_truth)]
confused_for = reference_file.set_index('record_id').loc[errors.pik].reset_index().set_index(errors.index)
errors[common_cols].compare(confused_for[common_cols], keep_shape=True, keep_equal=True)
    
census_2030.to_parquet('census_2030_with_piks_sample.parquet')
    
# Convert this notebook to a Python script
# ! ./convert_notebook.sh pvs_like_case_study_sample_data_spark
    
