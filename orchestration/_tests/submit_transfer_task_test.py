# _tests/submit_transfer_task_test.py
import uuid
from pathlib import Path

from prefect import flow, get_run_logger

from orchestration.flows.bl832.config import Config832
from orchestration.globus.transfer import start_transfer
from orchestration.transfer_controller import globus_transfer_task


@flow(name="submit_transfer_task_test")
def submit_transfer_task_test(file_path: str = "/raw/transfer_tests/test.txt"):
    logger = get_run_logger()
    config = Config832()

    # Copy file to a uniquely-named file in the same folder
    file = Path(file_path)
    new_file = str(file.with_name(f"test_{str(uuid.uuid4())}.txt"))
    logger.info(f"New file: {new_file}")

    # Copy within spot832 (blocking — we need this file to exist before transferring it)
    success, _ = start_transfer(
        config.tc, config.spot832, file_path, config.spot832, new_file, logger=logger
    )
    logger.info(f"spot832 internal copy success: {success}")

    # Fire off spot832 -> data832, don't wait
    logger.info("Submitting spot832 -> data832 transfer, not waiting...")
    spot_to_data_future = globus_transfer_task.submit(
        file_path=new_file,
        source=config.spot832,
        destination=config.data832,
        config=config,
    )
    logger.info("spot832 -> data832 submitted. Moving on immediately.")

    logger.info("Doing other things while spot832 -> data832 runs...")
    logger.info("... still doing other things ...")

    # Must wait for spot->data before submitting data->nersc (real data dependency)
    logger.info("Waiting for spot832 -> data832 to complete before submitting to nersc...")
    spot_to_data_success = spot_to_data_future.result()
    logger.info(f"spot832 -> data832: {spot_to_data_success}")

    if not spot_to_data_success:
        logger.error("spot832 -> data832 failed, skipping data832 -> nersc832 transfer.")
        return

    # Now safe to submit data832 -> nersc832
    logger.info("Submitting data832 -> nersc832 transfer, not waiting...")
    data_to_nersc_future = globus_transfer_task.submit(
        file_path=new_file,
        source=config.data832,
        destination=config.nersc832,
        config=config,
    )
    logger.info("data832 -> nersc832 submitted. Moving on immediately.")

    logger.info("... doing more things while data832 -> nersc832 runs ...")

    data_to_nersc_success = data_to_nersc_future.result()
    logger.info(f"data832 -> nersc832: {data_to_nersc_success}")


if __name__ == "__main__":
    submit_transfer_task_test()
