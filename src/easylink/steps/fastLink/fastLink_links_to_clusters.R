# STEP_NAME: links_to_clusters
# HAS_CUSTOM_RECIPE: true
# SCRIPT_BASE_COMMAND: Rscript

options(error = function() traceback(3))

library(fastLink)
library(arrow)
library(dplyr)
library(stringr)

# Check required environment variables
required_env_vars <- c(
    "LINKS_FILE_PATH",
    "OUTPUT_PATHS",
    "THRESHOLD_MATCH_PROBABILITY"
)
missing_vars <- required_env_vars[!nzchar(Sys.getenv(required_env_vars))]
if (length(missing_vars) > 0) {
    stop(sprintf(
        "The following required environment variables are not set or are empty: %s",
        paste(missing_vars, collapse = ", ")
    ))
}

links_file_path <- Sys.getenv("LINKS_FILE_PATH")
output_path <- Sys.getenv("OUTPUT_PATHS")

if (!file.exists(links_file_path)) {
    stop(sprintf("File not found: %s", links_file_path))
}

links <- read_parquet(links_file_path)

# Filter links by threshold
threshold <- as.numeric(Sys.getenv("THRESHOLD_MATCH_PROBABILITY"))
links <- links %>%
    filter(Probability >= threshold)

if (nrow(links) == 0) {
    clusters <- data.frame(
        `Input Record Dataset` = character(),
        `Input Record ID` = character(),
        `Cluster ID` = integer()
    )
} else {
    # NOTE: fastLink's dedupeMatches function is quite tightly coupled with other parts
    # of the fastLink pipeline.
    # It takes a bunch of objects, usually created by earlier steps in the pipeline,
    # which need to have particular formats.
    # Here, we "spoof" these objects using carefully constructed data.frames in order to
    # use the linear sum assignment algorithm in isolation (potentially with links not
    # from fastLink).
    # ChatGPT helped me write the below code.
    
    # Combine dataset and record ID for unique keys
    links <- links %>%
        mutate(
            Left_Record_Key = paste0(`Left Record Dataset`, "::", `Left Record ID`),
            Right_Record_Key = paste0(`Right Record Dataset`, "::", `Right Record ID`)
        )

    n <- nrow(links)

    # 1) Build matchesA / matchesB using the combined keys without touching `links`
    matchesA <- data.frame(
        idA = links[["Left_Record_Key"]],
        idB = links[["Right_Record_Key"]],
        prob = links[["Probability"]],
        stringsAsFactors = FALSE
    )
    matchesB <- matchesA  # identical copy

    # 2) Dummy one‐to‐one pattern key
    patterns <- data.frame(
        patID = seq_len(n)
    )

    # 3) Fake EM object, with dummy gamma columns
    EM <- list(
        patterns.w = data.frame(
            patID = patterns$patID,
            counts = 1,
            weights = 1,
            p.gamma.j.m = 1,
            p.gamma.j.u = 1
        ),
        # convert match‐probabilities into log‐odds scores
        zeta.j = log(matchesA$prob / (1 - matchesA$prob))
    )

    # 4) Map each row back to its original record keys
    matchesLink <- data.frame(
        inds.a = links[["Left_Record_Key"]],
        inds.b = links[["Right_Record_Key"]],
        stringsAsFactors = FALSE
    )

    # 5) Call dedupeMatches with linear programming
    deduped <- dedupeMatches(
        matchesA = matchesA,
        matchesB = matchesB,
        EM = EM,
        matchesLink = matchesLink,
        patterns = patterns,
        linprog = TRUE
    )

    # Parse out dataset and record ID from keys for both sides, then bind
    clusters <- deduped$matchesLink %>%
        transmute(
            `Input Record Dataset` = str_split_fixed(inds.a, "::", 2)[,1],
            `Input Record ID` = as.integer(str_split_fixed(inds.a, "::", 2)[,2]),
            `Cluster ID` = paste0("cluster_", inds.a, "_", inds.b)
        ) %>%
        bind_rows(
            deduped$matchesLink %>%
                transmute(
                    `Input Record Dataset` = str_split_fixed(inds.b, "::", 2)[,1],
                    `Input Record ID` = as.integer(str_split_fixed(inds.b, "::", 2)[,2]),
                    `Cluster ID` = paste0("cluster_", inds.a, "_", inds.b)
                )
        )
}

print(clusters)
# Write clusters to output
arrow::write_parquet(clusters, output_path)