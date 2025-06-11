# STEP_NAME: evaluating_pairs
# HAS_CUSTOM_RECIPE: true
# SCRIPT_BASE_COMMAND: Rscript

library(fastLink)
library(arrow)
library(dplyr)
library(stringr)

# Check required environment variables
required_env_vars <- c(
  "BLOCKS_DIR_PATH",
  "DUMMY_CONTAINER_DIAGNOSTICS_DIRECTORY",
  "DUMMY_CONTAINER_OUTPUT_PATHS",
  "COMPARISONS",
  "THRESHOLD_MATCH_PROBABILITY"
)
missing_vars <- required_env_vars[!nzchar(Sys.getenv(required_env_vars))]
if (length(missing_vars) > 0) {
  stop(sprintf(
    "The following required environment variables are not set or are empty: %s",
    paste(missing_vars, collapse = ", ")
  ))
}

blocks_dir <- Sys.getenv("BLOCKS_DIR_PATH")
diagnostics_dir <- Sys.getenv("DUMMY_CONTAINER_DIAGNOSTICS_DIRECTORY")
output_path <- Sys.getenv("DUMMY_CONTAINER_OUTPUT_PATHS")
comparisons <- strsplit(Sys.getenv("COMPARISONS"), ",")[[1]]

all_predictions <- list()

block_dirs <- list.dirs(blocks_dir, recursive = FALSE, full.names = TRUE)

for (block_dir in block_dirs) {
  records_path <- file.path(block_dir, "records.parquet")
  pairs_path <- file.path(block_dir, "pairs.parquet")
  if (!file.exists(records_path)) {
    stop(sprintf("File not found: %s", records_path))
  }
  if (!file.exists(pairs_path)) {
    stop(sprintf("File not found: %s", pairs_path))
  }

  records <- read_parquet(records_path) %>%
    mutate(
      unique_id = paste0(`Input Record Dataset`, "::", `Input Record ID`)
    )
  pairs <- read_parquet(pairs_path) %>%
    mutate(
      join_key_l = paste0(`Left Record Dataset`, "::", `Left Record ID`),
      join_key_r = paste0(`Right Record Dataset`, "::", `Right Record ID`)
    )

  # Subset records to only those that appear in pairs (left and right separately)
  left_ids <- unique(pairs$join_key_l)
  right_ids <- unique(pairs$join_key_r)
  dfA <- records %>% filter(unique_id %in% left_ids)
  dfB <- records %>% filter(unique_id %in% right_ids)

  # Prepare comparison columns and fastLink match flags
  comparison_cols <- sapply(comparisons, function(x) strsplit(x, ":")[[1]][1])
  comparison_methods <- sapply(comparisons, function(x) strsplit(x, ":")[[1]][2])

  stringdist.match <- comparison_cols[comparison_methods == "stringdist" | comparison_methods == "partial"]
  partial.match <- comparison_cols[comparison_methods == "partial"]

  # Estimate number of candidate pairs (after subsetting)
  nA <- nrow(dfA)
  nB <- nrow(dfB)
  n_requested_pairs <- nrow(pairs)
  n_possible_pairs <- nA * nB
  if (n_possible_pairs > 10 * n_requested_pairs) {
    warning(sprintf(
      "fastLink will compute %d candidate pairs, which is more than 10x the %d requested pairs.",
      n_possible_pairs, n_requested_pairs
    ))
  }

  # Trick: add a dummy column to dfB to avoid fastLink dedupe restriction if dfA and dfB are identical
  dfB_b <- dfB
  if (identical(dfA, dfB)) {
    dfB_b[["__dummy__"]] <- 1
  }

  # Run fastLink on all pairs (Cartesian product of reduced sets)
  fl_out <- fastLink::fastLink(
    dfA = dfA,
    dfB = dfB_b,
    varnames = comparison_cols,
    stringdist.match = stringdist.match,
    partial.match = partial.match,
    threshold.match = as.numeric(Sys.getenv("THRESHOLD_MATCH_PROBABILITY")),
    dedupe.matches = FALSE,
    return.all = TRUE
  )

  inds_a <- fl_out$matches$inds.a
  inds_b <- fl_out$matches$inds.b
  posteriors <- fl_out$posterior

  # Build matched pairs dataframe
  dfA_match <- dfA[inds_a, , drop = FALSE]
  dfB_match <- dfB[inds_b, , drop = FALSE]
  matches <- data.frame(
    join_key_l = dfA_match$unique_id,
    join_key_r = dfB_match$unique_id,
    Probability = posteriors
  )

  # Subset to only requested pairs
  matches <- matches %>%
    semi_join(pairs, by = c("join_key_l", "join_key_r"))

  # Parse out dataset and record ID from join keys
  predictions <- matches %>%
    transmute(
      `Left Record Dataset` = str_split_fixed(join_key_l, "::", 2)[,1],
      `Left Record ID` = as.integer(str_split_fixed(join_key_l, "::", 2)[,2]),
      `Right Record Dataset` = str_split_fixed(join_key_r, "::", 2)[,1],
      `Right Record ID` = as.integer(str_split_fixed(join_key_r, "::", 2)[,2]),
      Probability
    )

  all_predictions[[length(all_predictions) + 1]] <- predictions

  # Optionally, save diagnostics (e.g., plot of match probabilities)
  chart_path <- file.path(diagnostics_dir, paste0("match_weights_chart_", basename(block_dir), ".png"))
  png(chart_path)
  hist(predictions$Probability, main = "Match Probabilities", xlab = "Probability")
  dev.off()
}

all_predictions_df <- bind_rows(all_predictions)
print(all_predictions_df)
write_parquet(all_predictions_df, output_path)
