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


@task(name="register-file-to-tiled", task_run_name="register-{file_path}")
async def register_file_to_tiled(
    file_path: Path,
    catalog_path: str | None = None,
    tiled_path: Path | None = None,
) -> None:
    """Register a file or Zarr store to the Tiled catalog.

    Args:
        file_path: Absolute path on the client filesystem (used for logging).
        catalog_path: Optional sub-path within the Tiled catalog.
        tiled_path: Path as seen by the Tiled server. Defaults to file_path.
    """
    logger = get_run_logger()
    load_dotenv()
    tiled_uri = os.environ["TILED_URI"]
    api_key = os.environ["TILED_SINGLE_USER_API_KEY"]

    server_path = tiled_path if tiled_path is not None else file_path

    client = from_uri(tiled_uri, api_key=api_key)
    catalog = client[catalog_path] if catalog_path else client

    logger.info(f"Registering {file_path} → {server_path}")
    await register(catalog, server_path, overwrite=False)
    logger.info(f"Registered {server_path} to Tiled catalog")


@flow(name="register-to-tiled", flow_run_name="register-{file_path}")
async def register_to_tiled(
    file_path: Path | str,
    catalog_path: str | None = None,
    tiled_path: Path | str | None = None,
) -> None:
    """Register a file or Zarr store to the Tiled server.

    Args:
        file_path: Path to the file or Zarr store (client filesystem).
        catalog_path: Optional sub-path within the Tiled catalog.
        tiled_path: Path as seen by the Tiled server. Defaults to file_path.
    """
    logger = get_run_logger()
    file_path = Path(file_path)
    tiled_path = Path(tiled_path) if tiled_path else None
    logger.info(f"Registering {file_path} to Tiled (catalog_path={catalog_path!r})")
    await register_file_to_tiled(file_path, catalog_path=catalog_path, tiled_path=tiled_path)


if __name__ == "__main__":
    import asyncio

    zarr = Path("/Users/david/Documents/data/tomo/scratch/rec20230606_152011_jong-seto_fungal-mycelia.zarr")
    h5 = Path("/Users/david/Documents/data/tomo/raw/20241216_154449_ddd.h5")
    asyncio.run(register_to_tiled(zarr))
    asyncio.run(register_to_tiled(h5))
