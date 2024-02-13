from linker.configuration import Config
import snakemake


def make_config(config: Config) -> str:
    output_file = config.results_dir / "result.parquet"
    input_data = config.input_data