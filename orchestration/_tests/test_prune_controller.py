import pytest
from prefect.testing.utilities import prefect_test_harness


def _make_future(mocker, value=None):
    f = mocker.MagicMock()
    f.result.return_value = value
    return f


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield


@pytest.fixture(scope="session")
def prune_module():
    from orchestration.prune_controller import (
        FileSystemPruneController,
        GlobusPruneController,
        get_prune_controller,
        PruneMethod,
    )
    from orchestration.transfer_endpoints import FileSystemEndpoint
    from orchestration.globus.transfer import GlobusEndpoint
    return {
        "FileSystemPruneController": FileSystemPruneController,
        "GlobusPruneController": GlobusPruneController,
        "get_prune_controller": get_prune_controller,
        "PruneMethod": PruneMethod,
        "FileSystemEndpoint": FileSystemEndpoint,
        "GlobusEndpoint": GlobusEndpoint,
    }


# ── FileSystemPruneController.prune() ────────────────────────────────────────

class TestFileSystemPruneControllerPrune:

    @pytest.fixture
    def controller(self, prune_module, mocker):
        mock_config = mocker.MagicMock()
        mock_config.beamline_id = "8.3.2"
        return prune_module["FileSystemPruneController"](mock_config)

    @pytest.fixture
    def fs_endpoint(self, prune_module, tmp_path):
        return prune_module["FileSystemEndpoint"](
            name="src", root_path=str(tmp_path), uri="test://src"
        )

    def test_prune_no_file_path(self, controller, fs_endpoint, mocker):
        mocker.patch("orchestration.prune_controller.get_run_logger", return_value=mocker.MagicMock())
        assert controller.prune(file_path=None, source_endpoint=fs_endpoint) is False

    def test_prune_no_source_endpoint(self, controller, mocker):
        mocker.patch("orchestration.prune_controller.get_run_logger", return_value=mocker.MagicMock())
        assert controller.prune(file_path="raw/test.h5", source_endpoint=None) is False

    def test_prune_negative_days(self, controller, fs_endpoint, mocker):
        mocker.patch("orchestration.prune_controller.get_run_logger", return_value=mocker.MagicMock())
        assert controller.prune(file_path="raw/test.h5", source_endpoint=fs_endpoint, days_from_now=-1) is False

    def test_prune_immediate_success(self, controller, fs_endpoint, mocker):
        mocker.patch("orchestration.prune_controller.get_run_logger", return_value=mocker.MagicMock())
        mock_flow = mocker.patch("orchestration.prune_controller.prune_filesystem_endpoint")
        result = controller.prune(file_path="raw/test.h5", source_endpoint=fs_endpoint, days_from_now=0)
        assert result is True
        mock_flow.assert_called_once_with(
            relative_path="raw/test.h5",
            source_endpoint=fs_endpoint,
            check_endpoint=None,
            config=controller.config,
        )

    def test_prune_immediate_exception(self, controller, fs_endpoint, mocker):
        mocker.patch("orchestration.prune_controller.get_run_logger", return_value=mocker.MagicMock())
        mocker.patch(
            "orchestration.prune_controller.prune_filesystem_endpoint",
            side_effect=RuntimeError("flow failed"),
        )
        assert controller.prune(file_path="raw/test.h5", source_endpoint=fs_endpoint, days_from_now=0) is False

    def test_prune_deferred_blocks_on_future(self, controller, fs_endpoint, mocker):
        """Verifies the race-condition fix: .submit() is called and .result() blocks on it."""
        mocker.patch("orchestration.prune_controller.get_run_logger", return_value=mocker.MagicMock())
        mock_task = mocker.patch("orchestration.prune_controller.schedule_prefect_flow")
        future = _make_future(mocker)
        mock_task.submit.return_value = future

        result = controller.prune(file_path="raw/test.h5", source_endpoint=fs_endpoint, days_from_now=7)
        assert result is True
        mock_task.submit.assert_called_once()
        future.result.assert_called_once()

    def test_prune_deferred_exception(self, controller, fs_endpoint, mocker):
        mocker.patch("orchestration.prune_controller.get_run_logger", return_value=mocker.MagicMock())
        mock_task = mocker.patch("orchestration.prune_controller.schedule_prefect_flow")
        mock_task.submit.side_effect = RuntimeError("scheduling failed")
        assert controller.prune(file_path="raw/test.h5", source_endpoint=fs_endpoint, days_from_now=7) is False


# ── FileSystemPruneController.prune_no_prefect() ─────────────────────────────

class TestFileSystemPruneControllerNoPrefect:

    @pytest.fixture
    def controller(self, prune_module, mocker):
        mock_config = mocker.MagicMock()
        mock_config.beamline_id = "8.3.2"
        return prune_module["FileSystemPruneController"](mock_config)

    @pytest.fixture
    def src_endpoint(self, prune_module, tmp_path):
        return prune_module["FileSystemEndpoint"](
            name="src", root_path=str(tmp_path / "src"), uri="test://src"
        )

    @pytest.fixture
    def check_endpoint(self, prune_module, tmp_path):
        return prune_module["FileSystemEndpoint"](
            name="check", root_path=str(tmp_path / "check"), uri="test://check"
        )

    def test_no_file_path(self, controller, src_endpoint):
        assert controller.prune_no_prefect(file_path="", source_endpoint=src_endpoint) is False

    def test_no_source_endpoint(self, controller):
        assert controller.prune_no_prefect(file_path="raw/test.h5", source_endpoint=None) is False

    def test_path_not_exist(self, controller, src_endpoint, tmp_path):
        (tmp_path / "src").mkdir(parents=True, exist_ok=True)
        assert controller.prune_no_prefect(file_path="raw/missing.h5", source_endpoint=src_endpoint) is False

    def test_check_endpoint_path_missing(self, controller, src_endpoint, check_endpoint, tmp_path):
        src_dir = tmp_path / "src" / "raw"
        src_dir.mkdir(parents=True)
        (src_dir / "test.h5").write_text("data")
        (tmp_path / "check").mkdir(parents=True, exist_ok=True)
        # check path does not contain the file
        result = controller.prune_no_prefect(
            file_path="raw/test.h5",
            source_endpoint=src_endpoint,
            check_endpoint=check_endpoint,
        )
        assert result is False

    def test_prune_file_success(self, controller, src_endpoint, tmp_path):
        src_dir = tmp_path / "src" / "raw"
        src_dir.mkdir(parents=True)
        target = src_dir / "test.h5"
        target.write_text("data")

        result = controller.prune_no_prefect(file_path="raw/test.h5", source_endpoint=src_endpoint)
        assert result is True
        assert not target.exists()

    def test_prune_directory_success(self, controller, src_endpoint, tmp_path):
        target = tmp_path / "src" / "raw" / "scan001"
        target.mkdir(parents=True)
        (target / "file.h5").write_text("data")

        result = controller.prune_no_prefect(file_path="raw/scan001", source_endpoint=src_endpoint)
        assert result is True
        assert not target.exists()


# ── GlobusPruneController.prune() ─────────────────────────────────────────────

class TestGlobusPruneControllerPrune:

    @pytest.fixture
    def controller(self, prune_module, mocker):
        mock_config = mocker.MagicMock()
        mock_config.beamline_id = "8.3.2"
        return prune_module["GlobusPruneController"](mock_config)

    @pytest.fixture
    def globus_endpoint(self, prune_module):
        return prune_module["GlobusEndpoint"](
            uuid="uuid-src-1", uri="globus://src", root_path="/data/raw", name="src832"
        )

    def test_prune_no_file_path(self, controller, globus_endpoint, mocker):
        mocker.patch("orchestration.prune_controller.get_run_logger", return_value=mocker.MagicMock())
        assert controller.prune(file_path=None, source_endpoint=globus_endpoint) is False

    def test_prune_no_source_endpoint(self, controller, mocker):
        mocker.patch("orchestration.prune_controller.get_run_logger", return_value=mocker.MagicMock())
        assert controller.prune(file_path="raw/test.h5", source_endpoint=None) is False

    def test_prune_negative_days(self, controller, globus_endpoint, mocker):
        mocker.patch("orchestration.prune_controller.get_run_logger", return_value=mocker.MagicMock())
        assert controller.prune(file_path="raw/test.h5", source_endpoint=globus_endpoint, days_from_now=-1) is False

    def test_prune_immediate_success(self, controller, globus_endpoint, mocker):
        mocker.patch("orchestration.prune_controller.get_run_logger", return_value=mocker.MagicMock())
        mock_flow = mocker.patch("orchestration.prune_controller.prune_globus_endpoint")
        result = controller.prune(file_path="raw/test.h5", source_endpoint=globus_endpoint, days_from_now=0)
        assert result is True
        mock_flow.assert_called_once_with(
            relative_path="raw/test.h5",
            source_endpoint=globus_endpoint,
            check_endpoint=None,
            config=controller.config,
        )

    def test_prune_immediate_exception(self, controller, globus_endpoint, mocker):
        mocker.patch("orchestration.prune_controller.get_run_logger", return_value=mocker.MagicMock())
        mocker.patch(
            "orchestration.prune_controller.prune_globus_endpoint",
            side_effect=RuntimeError("flow failed"),
        )
        assert controller.prune(file_path="raw/test.h5", source_endpoint=globus_endpoint, days_from_now=0) is False

    def test_prune_deferred_blocks_on_future(self, controller, globus_endpoint, mocker):
        """Verifies the race-condition fix: .submit() is called and .result() blocks on it."""
        mocker.patch("orchestration.prune_controller.get_run_logger", return_value=mocker.MagicMock())
        mock_task = mocker.patch("orchestration.prune_controller.schedule_prefect_flow")
        future = _make_future(mocker)
        mock_task.submit.return_value = future

        result = controller.prune(file_path="raw/test.h5", source_endpoint=globus_endpoint, days_from_now=30)
        assert result is True
        mock_task.submit.assert_called_once()
        future.result.assert_called_once()

    def test_prune_deferred_exception(self, controller, globus_endpoint, mocker):
        mocker.patch("orchestration.prune_controller.get_run_logger", return_value=mocker.MagicMock())
        mock_task = mocker.patch("orchestration.prune_controller.schedule_prefect_flow")
        future = _make_future(mocker)
        future.result.side_effect = RuntimeError("scheduling failed")
        mock_task.submit.return_value = future
        assert controller.prune(file_path="raw/test.h5", source_endpoint=globus_endpoint, days_from_now=30) is False


# ── get_prune_controller() ────────────────────────────────────────────────────

class TestGetPruneController:

    @pytest.fixture
    def mock_config(self, mocker):
        cfg = mocker.MagicMock()
        cfg.beamline_id = "8.3.2"
        return cfg

    def test_get_globus_controller(self, prune_module, mock_config):
        controller = prune_module["get_prune_controller"](prune_module["PruneMethod"].GLOBUS, mock_config)
        assert isinstance(controller, prune_module["GlobusPruneController"])

    def test_get_filesystem_controller(self, prune_module, mock_config):
        controller = prune_module["get_prune_controller"](prune_module["PruneMethod"].SIMPLE, mock_config)
        assert isinstance(controller, prune_module["FileSystemPruneController"])
