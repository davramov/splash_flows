import logging
from prefect import flow
from typing import Optional, Union, Any
import h5py
from pathlib import Path

from orchestration.flows.bl7011.config import Config7011
from orchestration.flows.bl7011.move import process_new_7011_file_task

logger = logging.getLogger(__name__)

required_attrs = ['ownerGroup', 'owner', 'contactEmail', 'sourceFolder', 'creationTime', 'type']

# TODO Once this PR (https://github.com/als-computing/splash_flows/pull/62) is merged, we can use config: Config7011
@flow(name="dispatcher", flow_run_name="dispatcher-{file_path}")
def dispatcher(
    file_path: Optional[str] = None,
    is_export_control: bool = False,
    metadata: dict[str, Any] = None,
    config: Optional[Union[dict, Any]] = None,
) -> None:
    """
    Dispatcher flow for BL7011 beamline that launches the new_7011_file_flow.

    :param file_path: Path to the file to be processed.
    :param is_export_control: Flag indicating if export control measures should be applied.
                              (Not used in the current BL7011 processing)
    :param config: Configuration settings for processing.
                   Expected to be an instance of Config7011 or a dict that can be converted.
    :raises ValueError: If no configuration is provided.
    :raises TypeError: If the provided configuration is not a dict or Config7011.
    """

    logger.info("Starting dispatcher flow for BL 7.0.1.1")
    logger.info(f"Parameters received: file_path={file_path}, is_export_control={is_export_control}")

    # Validate inputs and raise errors if necessary. The ValueErrors prevent the rest of the flow from running.
    if file_path is None:
        logger.error("No file_path provided to dispatcher.")
        raise ValueError("File path is required for processing.")

    if is_export_control:
        logger.error("Data is under export control. Processing is not allowed.")
        raise ValueError("Data is under export control. Processing is not allowed.")

    if config is None:
        config = Config7011()
        logger.info("No config provided. Using default Config7011.")

    # We'll need access to the file to author the wrapper h5. If direct access isn't available, the files will need
    # to be copied locally first

    wrapper_path = Path(file_path).with_name(Path(file_path).name+"_wrapper")
    with h5py.File(file_path, "r") as source_h5, h5py.File(wrapper_path, "w") as wrapper_h5:

        group = wrapper_h5.create_dataset("scicat")
        for attr in required_attrs:
            group.attrs[attr] = metadata.get(attr, 'Unknown')

        wrapper_h5['entry'] = h5py.ExternalLink(file_path, "/entry")

    try:
        process_new_7011_file_task(
            file_path=file_path,
            config=config
        )
        process_new_7011_file_task(
            file_path=str(wrapper_path),
            config=config
        )
        logger.info("Dispatcher flow completed successfully.")
    except Exception as e:
        logger.error(f"Error during processing in dispatcher flow: {e}")
        raise
