#!/usr/bin/env python3
"""Periodic storage cleanup for bl832recon1x.

Removes old Zarr volumes, scratch data, and Docker artifacts to prevent
storage saturation. Intended to run monthly as a cron job with root privileges.

Cron setup (run once):
    cd /home/bl832user/Documents/code/splash_flows
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -e .
    sudo crontab -e

Cron entry (runs at 2am on the 1st of each month):
    0 2 1 * * /home/bl832user/Documents/code/splash_flows/.venv/bin/python
    /home/bl832user/Documents/code/splash_flows/orchestration/flows/bl832/prune_bl832recon1x.py

Requires Ubuntu 24.04 (kernel 6.8+, ext4) for reliable creation time via stat st_birthtime.
"""

import logging
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

from orchestration.config import BeamlineConfig
from orchestration.prune_controller import FileSystemPruneController
from orchestration.transfer_endpoints import FileSystemEndpoint

# ---------------------------------------------------------------------------
# Configuration — edit here to adjust behaviour
# ---------------------------------------------------------------------------


class _LocalConfig(BeamlineConfig):
    """Minimal config for local-only cleanup — no Globus connections."""
    def __init__(self) -> None:
        super().__init__(beamline_id="8.3.2")

    def _beam_specific_config(self) -> None:
        pass  # no Globus, no transfer client


SAMPLE_ENDPOINT = FileSystemEndpoint(
    name="bl832recon1x_samples",
    root_path="/home/bl832user/Documents/example_samples",
    uri="bl832recon1x.lbl.gov",
)

# Add or remove endpoints to apply recursive file pruning.
# Each entry uses FileSystemEndpoint so root_path, name, and uri stay consistent
# with the rest of splash_flows.
SCRATCH_ENDPOINTS: list[FileSystemEndpoint] = [
    FileSystemEndpoint(
        name="bl832recon1x_scratch",
        root_path="/home/bl832user/Documents/data/scratch",
        uri="bl832recon1x.lbl.gov",
    ),
    # FileSystemEndpoint(
    #     name="bl832recon1x_new_folder",
    #     root_path="/home/bl832user/Documents/some/new/folder",
    #     uri="bl832recon1x.lbl.gov",
    # ),
]

PRUNE_AFTER_DAYS = 30
LOG_FILE = Path("/tmp/bl832_cleanup.log")

# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)


def get_creation_time(path: Path) -> datetime | None:
    """Return the birth (creation) time of a file or directory.

    Uses st_birthtime via getattr — available on Ubuntu 24.04 (kernel 6.8+, ext4).
    Returns None if creation time is unavailable or reported as zero.

    Args:
        path: Path to the file or directory.

    Returns:
        A timezone-aware datetime of the creation time, or None if unavailable.
    """
    try:
        stat = path.stat()
        creation_ts = getattr(stat, "st_birthtime", None)  # filesystem creation time
        if not creation_ts:
            return None
        return datetime.fromtimestamp(creation_ts, tz=timezone.utc)
    except OSError as e:
        logger.warning(f"Could not stat {path}: {e}")
        return None


def prune_zarr_volumes(endpoint: FileSystemEndpoint, cutoff: datetime, config: BeamlineConfig) -> None:
    """Remove Zarr volumes in the endpoint's root_path older than cutoff.

    Top-level directories prefixed with 'demo_' are preserved regardless of age.
    Each Zarr volume is removed atomically by FileSystemPruneController.

    Args:
        endpoint: FileSystemEndpoint representing the Zarr samples directory.
        cutoff: Datetime threshold; volumes created before this are removed.
        config: Beamline configuration passed to FileSystemPruneController.
    """
    sample_dir = Path(endpoint.root_path)

    if not sample_dir.is_dir():
        logger.warning(f"Sample dir does not exist, skipping: {sample_dir}")
        return

    logger.info(f"--- Zarr volume cleanup: {endpoint.name} ({sample_dir}) ---")

    controller = FileSystemPruneController(config)

    for zarr_dir in sorted(sample_dir.iterdir()):
        if not zarr_dir.is_dir():
            continue

        if zarr_dir.name.startswith("demo_"):
            logger.info(f"Skipping demo volume: {zarr_dir}")
            continue

        creation = get_creation_time(zarr_dir)
        if creation is None:
            logger.warning(f"Creation time unavailable for {zarr_dir} — skipping")
            continue

        if creation < cutoff:
            logger.info(f"Removing Zarr volume: {zarr_dir} (created {creation.date()})")
            controller.prune(
                file_path=zarr_dir.name,
                source_endpoint=endpoint,
                days_from_now=0,
            )
        else:
            logger.info(f"Retaining: {zarr_dir} (created {creation.date()})")


def prune_scratch_endpoint(endpoint: FileSystemEndpoint, cutoff: datetime, config: BeamlineConfig) -> None:
    """Recursively remove files in the endpoint's root_path created before cutoff.

    After removing old files, sweeps empty directories bottom-up.

    Args:
        endpoint: FileSystemEndpoint representing the scratch directory to prune.
        cutoff: Datetime threshold; files created before this are removed.
        config: Beamline configuration passed to FileSystemPruneController.
    """
    scratch_dir = Path(endpoint.root_path)

    if not scratch_dir.is_dir():
        logger.warning(f"Scratch dir does not exist, skipping: {endpoint.name} ({scratch_dir})")
        return

    logger.info(f"--- Scratch data cleanup: {endpoint.name} ({scratch_dir}) ---")

    controller = FileSystemPruneController(config)

    for file in sorted(scratch_dir.rglob("*")):
        if not file.is_file():
            continue

        creation = get_creation_time(file)
        if creation is None:
            logger.warning(f"Creation time unavailable for {file} — skipping")
            continue

        if creation < cutoff:
            logger.info(f"Removing file: {file} (created {creation.date()})")
            controller.prune(
                file_path=str(file.relative_to(scratch_dir)),
                source_endpoint=endpoint,
                days_from_now=0,
            )

    # Sweep empty directories left behind, deepest-first
    for directory in sorted(scratch_dir.rglob("*"), reverse=True):
        if directory.is_dir() and not any(directory.iterdir()):
            try:
                directory.rmdir()
                logger.info(f"Removed empty directory: {directory}")
            except OSError as e:
                logger.error(f"Failed to remove empty directory {directory}: {e}")

    logger.info(f"Scratch cleanup complete for {endpoint.name}")


def prune_docker() -> None:
    """Remove unused Docker images, stopped containers, and build cache."""
    logger.info("--- Docker cleanup ---")

    commands: list[list[str]] = [
        ["docker", "image", "prune", "-af"],
        ["docker", "container", "prune", "-f"],
        ["docker", "builder", "prune", "-f"],
    ]

    for cmd in commands:
        logger.info(f"Running: {' '.join(cmd)}")
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            if result.stdout.strip():
                logger.info(result.stdout.strip())
        except subprocess.CalledProcessError as e:
            logger.error(f"Docker command failed: {' '.join(cmd)}\n{e.stderr.strip()}")


def main() -> None:
    """Run all cleanup tasks for bl832recon1x."""
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler(),
        ],
    )
    config = _LocalConfig()
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=PRUNE_AFTER_DAYS)

    logger.info("==========================================")
    logger.info("Starting bl832 cleanup")
    logger.info(f"Pruning items created before {cutoff.date()}")
    logger.info("==========================================")

    prune_zarr_volumes(SAMPLE_ENDPOINT, cutoff, config)

    for endpoint in SCRATCH_ENDPOINTS:
        prune_scratch_endpoint(endpoint, cutoff, config)

    # prune_docker()

    logger.info("==========================================")
    logger.info("Cleanup complete")
    logger.info("==========================================")


if __name__ == "__main__":
    main()
