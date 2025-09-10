import logging
from prefect import flow
from typing import Optional, Union, Any

from orchestration.flows.bl11021.move import process_new_11021_file

logger = logging.getLogger(__name__)


# TODO Once this PR (https://github.com/als-computing/splash_flows/pull/62) is merged, we can use config: Config733
@flow(name="dispatcher", flow_run_name="dispatcher-{file_path}")
def dispatcher(
    file_path: Optional[str] = None,
    is_export_control: bool = False,
    config: Optional[Union[dict, Any]] = None,
) -> None:
    """
    Dispatcher flow for BL11021 beamline that launches the new_11021_file_flow.

    :param file_path: Path to the file to be processed.
    :param is_export_control: Flag indicating if export control measures should be applied.
                              (Not used in the current BL11021 processing)
    :param config: Configuration settings for processing.
                   Expected to be an instance of Config11021 or a dict that can be converted.
    :raises ValueError: If no configuration is provided.
    :raises TypeError: If the provided configuration is not a dict or Config11021.
    """

    logger.info("Starting dispatcher flow for BL 11.0.2.1")
    logger.info(f"Parameters received: file_path={file_path}, is_export_control={is_export_control}")

    # Validate inputs and raise errors if necessary. The ValueErrors prevent the rest of the flow from running.
    if file_path is None:
        logger.error("No file_path provided to dispatcher.")
        raise ValueError("File path is required for processing.")

    if is_export_control:
        logger.error("Data is under export control. Processing is not allowed.")
        raise ValueError("Data is under export control. Processing is not allowed.")

    if config is None:
        logger.error("No configuration provided to dispatcher.")
        raise ValueError("Configuration (config) is required for processing.")

    try:
        process_new_11021_file(
            file_path=file_path,
            config=config
        )
        logger.info("Dispatcher flow completed successfully.")
    except Exception as e:
        logger.error(f"Error during processing in dispatcher flow: {e}")
        raise


if __name__ == "__main__":
    dispatcher()
