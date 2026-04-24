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
    tags: list[str] | None = None,
) -> None:
    logger = get_run_logger()
    load_dotenv()
    tiled_uri = os.environ["TILED_URI"]
    tiled_api_key = os.environ["TILED_API_KEY"]

    client = from_uri(tiled_uri, api_key=tiled_api_key)

    logger.info(f"Registering {path} to Tiled catalog at {tiled_uri} with prefix {prefix!r}")
    try:
        await register(
            node=client,
            path=path,
            prefix=prefix or "/",
            overwrite=overwrite,
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to register {path} to Tiled catalog at {tiled_uri} "
            f"(prefix={prefix!r}): {e}"
        ) from e

    if not tags:
        return

    def _apply_tags(entry_node):
        existing_blob = entry_node.access_blob
        existing_tags = (existing_blob or {}).get("tags", [])
        merged_tags = list(set(existing_tags) | set(tags))
        op = "replace" if existing_blob is not None else "add"
        try:
            entry_node.patch_metadata(
                access_blob_patch=[{"op": op, "path": "", "value": {"tags": merged_tags}}],
            )
            logger.info(f"Tagged {entry_node.uri} with {merged_tags}")
        except Exception as e:
            logger.warning(f"Could not tag {entry_node.uri}: {e}")

    # Navigate to prefix node after registration
    node = client
    for segment in (prefix or "").strip("/").split("/"):
        if segment:
            node = node[segment]

    if path.is_dir() and not path.suffix:
        # TIFF directory: Tiled registers each file flat into the prefix node
        for key in node:
            _apply_tags(node[key])
    else:
        # .h5 or .zarr: registered as single node, keyed by stem
        # Even on COLLISION the entry exists — just try it directly
        entry_key = path.stem
        logger.info(f"Looking up entry key {entry_key!r} under {prefix!r}")
        try:
            _apply_tags(node[entry_key])
        except KeyError:
            # Key not found even after registration — log all available keys to diagnose
            available = sorted(node)
            logger.warning(
                f"Entry {entry_key!r} not found under {prefix!r}. "
                f"Available keys: {available}"
            )


@flow(name="register-to-tiled", flow_run_name="register-{path}")
async def register_to_tiled(
    path: Path | str,
    prefix: str | None = None,
    overwrite: bool = False,
    tags: list[str] | None = None,
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
    await register_file_to_tiled(path, prefix=prefix, overwrite=overwrite, tags=tags)


if __name__ == "__main__":
    import asyncio

    h5 = Path("/Users/david/Documents/data/tomo/raw/20241216_154449_ddd.h5")
    tiffs = Path("/Users/david/Documents/data/tomo/rec20230224_132553_sea_shell/")
    zarr = Path("/Users/david/Documents/data/tomo/scratch/rec20230606_152011_jong-seto_fungal-mycelia_flat-AQ_fungi2_fast.zarr")

    asyncio.run(register_to_tiled(path=h5, prefix="beamlines/bl832/raw/", tags=["bl832"], overwrite=False))
    asyncio.run(register_to_tiled(path=tiffs, prefix="beamlines/bl832/scratch", tags=["bl832", "dabramov"], overwrite=False))
    asyncio.run(register_to_tiled(path=zarr, prefix="beamlines/bl832/scratch", tags=["bl832"], overwrite=False))

    load_dotenv()
    client = from_uri(os.environ["TILED_URI"])
    checks = [
        (client["beamlines"]["bl832"]["raw"][h5.stem], ["bl832"], h5),
        (client["beamlines"]["bl832"]["scratch"], ["bl832", "dabramov"], tiffs),
        (client["beamlines"]["bl832"]["scratch"][zarr.stem], ["bl832"], zarr),
    ]
    for node, expected_tags, check_path in checks:
        if check_path.is_dir() and not check_path.suffix:
            key = next(iter(node))
            node = node[key]
        actual = node.access_blob.get("tags", [])
        status = "✓" if set(expected_tags) <= set(actual) else "✗"
        print(f"{status} {node.uri}: tags={actual}")    # prefix should be beamlines/bl832/raw/<project>/<filename>
