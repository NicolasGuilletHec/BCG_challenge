"""Bronze to Silver Pipeline."""

from constants.paths import SILVER_DIR
from src.utils.logger import logger


def bronze_to_silver():
    """Pipeline to transform bronze data to silver data.

    This function uses the previously defined functions to read bronze data,
    process it, and write the silver data.

    To add a step to this pipeline, define the function above and call it here.
    """
    # The structure is always the same:

    # 1. Read data from bronze:
    # logger.info(f"Reading XXXX data at {PATH_TO_DATA}")
    # data = pd.read_csv(PATH_TO_DATA)

    # 2. Process data
    # logger.info("Processing XXXX data")
    # processed_data = process_xxxx(data) -> Use your function here

    # 3. Write data to silver
    # logger.info(f"Writing processed data to {PATH_TO_SILVER}")
    # processed_data.to_parquet(PATH_TO_SILVER)

    # Ensure silver directory exists
    SILVER_DIR.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    # Execute the pipeline --> Called using DVC
    logger.info("Starting Bronze to Silver pipeline")
    bronze_to_silver()
    logger.info("Finished Bronze to Silver pipeline")
