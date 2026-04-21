"""Register data files and Zarr/HDF5 stores to the Tiled catalog.

Intended to run on a Ride worker. The file path must be accessible
to the Tiled server's filesystem.
"""
from dotenv import load_dotenv
import os
from pathlib import Path

from prefect import flow, get_run_logger, task
from tiled.client import from_uri
from tiled.client.register import register


@task(name="register-file-to-tiled", task_run_name="register-{path}")
async def register_file_to_tiled(
    path: Path,
    prefix: str | None = None,
    overwrite: bool = False,
) -> None:
    """Register a file or Zarr store to the Tiled catalog.

    Args:
        path: Absolute path on the client filesystem (used for logging).
        prefix: Optional sub-path within the Tiled catalog.
        overwrite: Whether to overwrite existing entries in the Tiled catalog.
    """
    logger = get_run_logger()
    load_dotenv()
    tiled_uri = os.environ["TILED_URI"]
    api_key = os.environ["TILED_SINGLE_USER_API_KEY"]

    client = from_uri(tiled_uri, api_key=api_key)

    logger.info(f"Registering {path} to Tiled catalog at {tiled_uri} with prefix {prefix!r}")
    try:
        await register(
            node=client,
            path=path,
            prefix=prefix or "/",
            overwrite=overwrite
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to register {path} to Tiled catalog at {tiled_uri} "
            f"(prefix={prefix!r}): {e}"
        ) from e
    logger.info(f"Registered {path} to Tiled catalog")


@flow(name="register-to-tiled", flow_run_name="register-{path}")
async def register_to_tiled(
    path: Path | str,
    prefix: str | None = None,
    overwrite: bool = False,
) -> None:
    """Register a file or Zarr store to the Tiled server.

    Args:
        path: Path to the file or Zarr store (client filesystem).
        prefix: Optional sub-path within the Tiled catalog.
        overwrite: Whether to overwrite existing entries in the Tiled catalog.
    """
    logger = get_run_logger()
    path = Path(path)
    logger.info(f"Submitting task: register {path} to Tiled (prefix={prefix!r})")
    await register_file_to_tiled(path, prefix=prefix, overwrite=overwrite)


if __name__ == "__main__":
    import asyncio

    zarr = Path("/Users/david/Documents/data/tomo/scratch/rec20230606_152011_jong-seto_fungal-mycelia.zarr")
    h5 = Path("/Users/david/Documents/data/tomo/raw/20241216_154449_ddd.h5")
    asyncio.run(register_to_tiled(zarr, prefix="scratch"))
    asyncio.run(register_to_tiled(h5, prefix="raw"))
