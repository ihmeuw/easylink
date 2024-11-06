library(tidyverse)
library(lubridate)
library(yaml)

# Function to load file
load_file <- function(file_path, file_format = NULL) {
    if (is.null(file_format)) {
        file_format <- tools::file_ext(file_path)
    }
    if (file_format == "parquet") {
        return(arrow::read_parquet(file_path))
    } else if (file_format == "csv") {
        return(read_csv(file_path))
    } else {
        stop("Unsupported file format")
    }
}

diagnostics <- list()
input_env_vars <- strsplit(Sys.getenv("INPUT_ENV_VARS", "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS"), ",")[[1]]

df <- NULL

for (env_var in input_env_vars) {
    if (!(env_var %in% names(Sys.getenv()))) {
        message(paste("Missing required environment variable", env_var))
        stop()
    }

    message(paste("Loading files for", env_var))
    file_paths <- strsplit(Sys.getenv(env_var), ",")[[1]]
    diagnostics[[paste0("num_files_", tolower(env_var))]] <- length(file_paths)

    if (is.null(df)) {
        df <- load_file(file_paths[1])
        file_paths <- file_paths[-1]
    }

    for (path in file_paths) {
        df <- bind_rows(df, load_file(path)) %>%
        mutate(
            across(everything(), ~replace_na(.x, 0))
        )
    }
}

extra_implementation_specific_input_glob <- list.files(path = "/extra_implementation_specific_input_data", pattern = "input*", full.names = TRUE)
extra_implementation_specific_input_file_path <- Sys.getenv(
    "DUMMY_CONTAINER_EXTRA_IMPLEMENTATION_SPECIFIC_INPUT_FILE_PATH",
    if (length(extra_implementation_specific_input_glob) > 0) extra_implementation_specific_input_glob[1] else ""
)
diagnostics$extra_implementation_specific_input <- (extra_implementation_specific_input_file_path != "")
if (extra_implementation_specific_input_file_path != "") {
    message('Loading extra, implementation-specific input')
    df <- bind_rows(df, load_file(extra_implementation_specific_input_file_path)) %>%
        mutate(
            across(everything(), ~replace_na(.x, 0))
        )
}

message(paste('Total input length is', nrow(df)))

broken <- tolower(Sys.getenv("DUMMY_CONTAINER_BROKEN", "false")) %in% c('true', 'yes', '1')
diagnostics$broken <- broken
if (broken) {
    df <- rename(df, wrong = foo, column = bar, names = counter)
} else {
    increment <- as.integer(Sys.getenv("DUMMY_CONTAINER_INCREMENT", "1"))
    diagnostics$increment <- increment
    message(paste('Increment is', increment))

    df$counter <- as.integer(df$counter + increment)

    # Extract the maximum added column number from column names starting with "added_column_"
    added_columns_existing <- str_subset(names(df), "^added_column_")
    diagnostics$added_columns_existing <- added_columns_existing
    added_columns_existing_ints <- str_extract(added_columns_existing, "\\d+$") %>% as.integer
    if (length(added_columns_existing_ints) > 0) {
      max_added_column <- max(added_columns_existing_ints, na.rm = TRUE) + increment
    } else {
      max_added_column <- increment
    }

    min_added_column <- max(max_added_column - 4, 0)

    # Generate the names of the desired added columns
    added_columns_desired_names <- paste0('added_column_', min_added_column:max_added_column)
    diagnostics$added_columns_desired_names <- added_columns_desired_names
    diagnostics$new_columns <- list()

    # Create new columns if they do not exist
    for (column_name in added_columns_desired_names) {
        if (!(column_name %in% names(df))) {
            diagnostics$new_columns <- append(diagnostics$new_columns, column_name)
            column_value <- as.integer(str_extract(column_name, "\\d+$"))
            df <- df %>%
                mutate(!!column_name := column_value)
        }
    }

    # Drop the columns that are not desired
    columns_to_drop <- grep("added_column_", names(df), value = TRUE)
    columns_to_drop <- columns_to_drop[!(columns_to_drop %in% added_columns_desired_names)]
    diagnostics$columns_to_drop <- columns_to_drop
    df <- df %>% select(-all_of(columns_to_drop))
}

output_file_format <- Sys.getenv("DUMMY_CONTAINER_OUTPUT_FILE_FORMAT", "parquet")
output_file_paths <- strsplit(Sys.getenv("DUMMY_CONTAINER_OUTPUT_PATHS", paste0("/results/result.", output_file_format)), ",")[[1]]

diagnostics$num_output_files <- length(output_file_paths)
diagnostics$output_file_paths <- output_file_paths

for (output_file_path in output_file_paths) {
    message(paste('Writing output to', output_file_path, 'in', output_file_format, 'format'))

    if (output_file_format == "parquet") {
        arrow::write_parquet(df, output_file_path)
    } else if (output_file_format == "csv") {
        write_csv(df, output_file_path)
    } else {
        stop("Unsupported file format")
    }
}

diagnostics_dir <- Sys.getenv("DUMMY_CONTAINER_DIAGNOSTICS_DIRECTORY", "/diagnostics")
if (dir.exists(diagnostics_dir) && file.access(diagnostics_dir, mode = 2) == 0) {
    write_yaml(diagnostics, file.path(diagnostics_dir, 'diagnostics.yaml'))
}