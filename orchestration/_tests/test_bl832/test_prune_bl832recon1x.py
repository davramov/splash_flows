"""Tests for orchestration/flows/bl832/prune_bl832recon1x.py."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from orchestration.flows.bl832.prune_bl832recon1x import (
    get_creation_time,
    prune_docker,
    prune_scratch_endpoint,
    prune_zarr_volumes,
)
from orchestration.transfer_endpoints import FileSystemEndpoint

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CUTOFF = datetime(2025, 1, 1, tzinfo=timezone.utc)
OLD_CREATION = datetime(2024, 6, 1, tzinfo=timezone.utc)
NEW_CREATION = datetime(2025, 6, 1, tzinfo=timezone.utc)

MODULE = "orchestration.flows.bl832.prune_bl832recon1x"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_config(mocker):
    """Mocked BeamlineConfig — avoids importing Config832 and triggering Globus/Prefect init."""
    return mocker.MagicMock()


@pytest.fixture()
def sample_endpoint(tmp_path: Path) -> FileSystemEndpoint:
    """FileSystemEndpoint pointing at a temporary Zarr samples directory."""
    return FileSystemEndpoint(
        name="test_samples",
        root_path=str(tmp_path),
        uri="bl832recon1x.lbl.gov",
    )


@pytest.fixture()
def scratch_endpoint(tmp_path: Path) -> FileSystemEndpoint:
    """FileSystemEndpoint pointing at a temporary scratch directory."""
    return FileSystemEndpoint(
        name="test_scratch",
        root_path=str(tmp_path),
        uri="bl832recon1x.lbl.gov",
    )


# ---------------------------------------------------------------------------
# get_creation_time
# ---------------------------------------------------------------------------


def test_get_creation_time_returns_datetime_when_available(mocker) -> None:
    """Returns a timezone-aware datetime when filesystem creation time is present."""
    mock_stat = mocker.MagicMock()
    mock_stat.st_birthtime = OLD_CREATION.timestamp()
    mock_path = mocker.MagicMock()
    mock_path.stat.return_value = mock_stat

    result = get_creation_time(mock_path)

    assert result == OLD_CREATION


def test_get_creation_time_falls_back_to_ctime(mocker) -> None:
    """Falls back to st_ctime when st_birthtime is not available."""
    mock_stat = mocker.MagicMock(spec=["st_ctime"])
    mock_stat.st_ctime = 0
    mock_stat.st_ctime = OLD_CREATION.timestamp()
    mock_path = mocker.MagicMock()
    mock_path.stat.return_value = mock_stat

    result = get_creation_time(mock_path)

    assert result == OLD_CREATION


def test_get_creation_time_returns_none_when_creation_time_is_zero(mocker) -> None:
    """Returns None when st_birthtime is zero (filesystem does not support creation time)."""
    mock_stat = mocker.MagicMock()
    mock_stat.st_birthtime = 0
    mock_stat.st_ctime = 0
    mock_path = mocker.MagicMock()
    mock_path.stat.return_value = mock_stat

    result = get_creation_time(mock_path)

    assert result is None


def test_get_creation_time_returns_none_when_attribute_missing(mocker) -> None:
    """Returns None when creation time attribute is not present on the stat result."""
    mock_stat = mocker.MagicMock(spec=[])
    mock_path = mocker.MagicMock()
    mock_path.stat.return_value = mock_stat

    result = get_creation_time(mock_path)

    assert result is None


def test_get_creation_time_returns_none_on_os_error(mocker) -> None:
    """Returns None and logs a warning when stat raises OSError."""
    mock_path = mocker.MagicMock()
    mock_path.stat.side_effect = OSError("permission denied")

    result = get_creation_time(mock_path)

    assert result is None


# ---------------------------------------------------------------------------
# prune_zarr_volumes
# ---------------------------------------------------------------------------


def test_prune_zarr_volumes_removes_old_volume(
    sample_endpoint: FileSystemEndpoint, mock_config, mocker
) -> None:
    """Calls prune_no_prefect for Zarr volumes created before the cutoff."""
    old_zarr = Path(sample_endpoint.root_path) / "old_scan"
    old_zarr.mkdir()

    mock_controller = mocker.MagicMock()
    mocker.patch(f"{MODULE}.FileSystemPruneController", return_value=mock_controller)
    mocker.patch(f"{MODULE}.get_creation_time", return_value=OLD_CREATION)

    prune_zarr_volumes(sample_endpoint, CUTOFF, mock_config)

    mock_controller.prune_no_prefect.assert_called_once_with(
        file_path="old_scan",
        source_endpoint=sample_endpoint,
    )


def test_prune_zarr_volumes_retains_new_volume(
    sample_endpoint: FileSystemEndpoint, mock_config, mocker
) -> None:
    """Does not call prune_no_prefect for Zarr volumes created after the cutoff."""
    new_zarr = Path(sample_endpoint.root_path) / "new_scan"
    new_zarr.mkdir()

    mock_controller = mocker.MagicMock()
    mocker.patch(f"{MODULE}.FileSystemPruneController", return_value=mock_controller)
    mocker.patch(f"{MODULE}.get_creation_time", return_value=NEW_CREATION)

    prune_zarr_volumes(sample_endpoint, CUTOFF, mock_config)

    mock_controller.prune_no_prefect.assert_not_called()


def test_prune_zarr_volumes_skips_demo_prefix(
    sample_endpoint: FileSystemEndpoint, mock_config, mocker
) -> None:
    """Does not call prune_no_prefect for directories prefixed with 'demo_'."""
    demo_zarr = Path(sample_endpoint.root_path) / "demo_sample"
    demo_zarr.mkdir()

    mock_controller = mocker.MagicMock()
    mocker.patch(f"{MODULE}.FileSystemPruneController", return_value=mock_controller)
    mocker.patch(f"{MODULE}.get_creation_time", return_value=OLD_CREATION)

    prune_zarr_volumes(sample_endpoint, CUTOFF, mock_config)

    mock_controller.prune_no_prefect.assert_not_called()


def test_prune_zarr_volumes_dry_run_does_not_delete(
    sample_endpoint: FileSystemEndpoint, mock_config, mocker
) -> None:
    """Does not call prune_no_prefect when dry_run=True."""
    old_zarr = Path(sample_endpoint.root_path) / "old_scan"
    old_zarr.mkdir()

    mock_controller = mocker.MagicMock()
    mocker.patch(f"{MODULE}.FileSystemPruneController", return_value=mock_controller)
    mocker.patch(f"{MODULE}.get_creation_time", return_value=OLD_CREATION)
    mocker.patch(f"{MODULE}._dir_size_mb", return_value=100.0)

    prune_zarr_volumes(sample_endpoint, CUTOFF, mock_config, dry_run=True)

    mock_controller.prune_no_prefect.assert_not_called()


def test_prune_zarr_volumes_skips_when_creation_time_unavailable(
    sample_endpoint: FileSystemEndpoint, mock_config, mocker
) -> None:
    """Does not call prune_no_prefect when creation time cannot be determined."""
    zarr = Path(sample_endpoint.root_path) / "unknown_age"
    zarr.mkdir()

    mock_controller = mocker.MagicMock()
    mocker.patch(f"{MODULE}.FileSystemPruneController", return_value=mock_controller)
    mocker.patch(f"{MODULE}.get_creation_time", return_value=None)

    prune_zarr_volumes(sample_endpoint, CUTOFF, mock_config)

    mock_controller.prune_no_prefect.assert_not_called()


def test_prune_zarr_volumes_skips_files(
    sample_endpoint: FileSystemEndpoint, mock_config, mocker
) -> None:
    """Does not call prune_no_prefect for loose files in the sample directory."""
    loose_file = Path(sample_endpoint.root_path) / "stray_file.txt"
    loose_file.touch()

    mock_controller = mocker.MagicMock()
    mocker.patch(f"{MODULE}.FileSystemPruneController", return_value=mock_controller)
    mocker.patch(f"{MODULE}.get_creation_time", return_value=OLD_CREATION)

    prune_zarr_volumes(sample_endpoint, CUTOFF, mock_config)

    mock_controller.prune_no_prefect.assert_not_called()


def test_prune_zarr_volumes_noop_when_directory_missing(mock_config, mocker) -> None:
    """Returns without error when the sample directory does not exist."""
    endpoint = FileSystemEndpoint(
        name="missing",
        root_path="/nonexistent/path",
        uri="bl832recon1x.lbl.gov",
    )
    mock_controller = mocker.MagicMock()
    mocker.patch(f"{MODULE}.FileSystemPruneController", return_value=mock_controller)

    prune_zarr_volumes(endpoint, CUTOFF, mock_config)

    mock_controller.prune_no_prefect.assert_not_called()


# ---------------------------------------------------------------------------
# prune_scratch_endpoint
# ---------------------------------------------------------------------------


def test_prune_scratch_endpoint_removes_old_file(
    scratch_endpoint: FileSystemEndpoint, mock_config, mocker
) -> None:
    """Calls prune_no_prefect for files created before the cutoff."""
    old_file = Path(scratch_endpoint.root_path) / "old_result.h5"
    old_file.touch()

    mock_controller = mocker.MagicMock()
    mocker.patch(f"{MODULE}.FileSystemPruneController", return_value=mock_controller)
    mocker.patch(f"{MODULE}.get_creation_time", return_value=OLD_CREATION)

    prune_scratch_endpoint(scratch_endpoint, CUTOFF, mock_config)

    mock_controller.prune_no_prefect.assert_called_once_with(
        file_path="old_result.h5",
        source_endpoint=scratch_endpoint,
    )


def test_prune_scratch_endpoint_retains_new_file(
    scratch_endpoint: FileSystemEndpoint, mock_config, mocker
) -> None:
    """Does not call prune_no_prefect for files created after the cutoff."""
    new_file = Path(scratch_endpoint.root_path) / "new_result.h5"
    new_file.touch()

    mock_controller = mocker.MagicMock()
    mocker.patch(f"{MODULE}.FileSystemPruneController", return_value=mock_controller)
    mocker.patch(f"{MODULE}.get_creation_time", return_value=NEW_CREATION)

    prune_scratch_endpoint(scratch_endpoint, CUTOFF, mock_config)

    mock_controller.prune_no_prefect.assert_not_called()


def test_prune_scratch_endpoint_recurses_into_subdirectories(
    scratch_endpoint: FileSystemEndpoint, mock_config, mocker
) -> None:
    """Calls prune_no_prefect for old files in nested subdirectories."""
    nested = Path(scratch_endpoint.root_path) / "subdir" / "nested"
    nested.mkdir(parents=True)
    nested_file = nested / "data.h5"
    nested_file.touch()

    mock_controller = mocker.MagicMock()
    mocker.patch(f"{MODULE}.FileSystemPruneController", return_value=mock_controller)
    mocker.patch(f"{MODULE}.get_creation_time", return_value=OLD_CREATION)

    prune_scratch_endpoint(scratch_endpoint, CUTOFF, mock_config)

    mock_controller.prune_no_prefect.assert_called_once_with(
        file_path="subdir/nested/data.h5",
        source_endpoint=scratch_endpoint,
    )


def test_prune_scratch_endpoint_dry_run_does_not_delete(
    scratch_endpoint: FileSystemEndpoint, mock_config, mocker
) -> None:
    """Does not call prune_no_prefect when dry_run=True."""
    old_file = Path(scratch_endpoint.root_path) / "old_result.h5"
    old_file.touch()

    mock_controller = mocker.MagicMock()
    mocker.patch(f"{MODULE}.FileSystemPruneController", return_value=mock_controller)
    mocker.patch(f"{MODULE}.get_creation_time", return_value=OLD_CREATION)

    prune_scratch_endpoint(scratch_endpoint, CUTOFF, mock_config, dry_run=True)

    mock_controller.prune_no_prefect.assert_not_called()


def test_prune_scratch_endpoint_removes_empty_directories_after_pruning(
    scratch_endpoint: FileSystemEndpoint, mock_config, mocker
) -> None:
    """Removes directories that are left empty after file deletion."""
    subdir = Path(scratch_endpoint.root_path) / "empty_after_prune"
    subdir.mkdir()
    old_file = subdir / "old.h5"
    old_file.touch()

    def fake_prune(**kwargs: object) -> bool:
        Path(scratch_endpoint.root_path, kwargs["file_path"]).unlink(missing_ok=True)
        return True

    mock_controller = mocker.MagicMock()
    mock_controller.prune_no_prefect.side_effect = fake_prune
    mocker.patch(f"{MODULE}.FileSystemPruneController", return_value=mock_controller)
    mocker.patch(f"{MODULE}.get_creation_time", return_value=OLD_CREATION)

    prune_scratch_endpoint(scratch_endpoint, CUTOFF, mock_config)

    assert not subdir.exists()


def test_prune_scratch_endpoint_dry_run_does_not_remove_empty_directories(
    scratch_endpoint: FileSystemEndpoint, mock_config, mocker
) -> None:
    """Does not sweep empty directories when dry_run=True."""
    subdir = Path(scratch_endpoint.root_path) / "subdir"
    subdir.mkdir()

    mock_controller = mocker.MagicMock()
    mocker.patch(f"{MODULE}.FileSystemPruneController", return_value=mock_controller)
    mocker.patch(f"{MODULE}.get_creation_time", return_value=OLD_CREATION)

    prune_scratch_endpoint(scratch_endpoint, CUTOFF, mock_config, dry_run=True)

    assert subdir.exists()


def test_prune_scratch_endpoint_skips_when_creation_time_unavailable(
    scratch_endpoint: FileSystemEndpoint, mock_config, mocker
) -> None:
    """Does not call prune_no_prefect when creation time cannot be determined."""
    f = Path(scratch_endpoint.root_path) / "unknown.h5"
    f.touch()

    mock_controller = mocker.MagicMock()
    mocker.patch(f"{MODULE}.FileSystemPruneController", return_value=mock_controller)
    mocker.patch(f"{MODULE}.get_creation_time", return_value=None)

    prune_scratch_endpoint(scratch_endpoint, CUTOFF, mock_config)

    mock_controller.prune_no_prefect.assert_not_called()


def test_prune_scratch_endpoint_noop_when_directory_missing(mock_config, mocker) -> None:
    """Returns without error when the scratch directory does not exist."""
    endpoint = FileSystemEndpoint(
        name="missing",
        root_path="/nonexistent/path",
        uri="bl832recon1x.lbl.gov",
    )
    mock_controller = mocker.MagicMock()
    mocker.patch(f"{MODULE}.FileSystemPruneController", return_value=mock_controller)

    prune_scratch_endpoint(endpoint, CUTOFF, mock_config)

    mock_controller.prune_no_prefect.assert_not_called()


# ---------------------------------------------------------------------------
# prune_docker
# ---------------------------------------------------------------------------


def test_prune_docker_runs_all_commands(mocker) -> None:
    """Calls subprocess.run for each of the three Docker prune commands."""
    mock_run = mocker.patch(f"{MODULE}.subprocess.run")
    mock_run.return_value = mocker.MagicMock(stdout="", returncode=0)

    prune_docker()

    assert mock_run.call_count == 3
    calls = [c.args[0] for c in mock_run.call_args_list]
    assert ["docker", "image", "prune", "-af"] in calls
    assert ["docker", "container", "prune", "-f"] in calls
    assert ["docker", "builder", "prune", "-f"] in calls


def test_prune_docker_logs_on_failure(mocker) -> None:
    """Logs an error without raising when a Docker command fails."""
    import subprocess

    mocker.patch(
        f"{MODULE}.subprocess.run",
        side_effect=subprocess.CalledProcessError(1, "docker", stderr="daemon not running"),
    )

    prune_docker()
