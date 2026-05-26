# orchestration/_tests/test_mlflow.py
#
# Tests for orchestration/mlflow.py — the beamline-agnostic helper that
# wraps the MLflow Model Registry for checkpoint metadata lookup.
#
# Beamline-specific tests (e.g. _load_job_options, segmentation_sam3)
# live in _tests/test_bl832/test_mlflow.py.

import json
import pytest
from uuid import uuid4

import mlflow.utils.rest_utils as rest_utils

from prefect.blocks.system import Secret
from prefect.testing.utilities import prefect_test_harness


# ──────────────────────────────────────────────────────────────────────────────
# Session fixture
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        Secret(value=str(uuid4())).save(name="globus-client-id", overwrite=True)
        Secret(value=str(uuid4())).save(name="globus-client-secret", overwrite=True)
        yield


# ──────────────────────────────────────────────────────────────────────────────
# Shared fixtures
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_beamline_config(mocker):
    """Minimal BeamlineConfig mock with mlflow tracking_uri."""
    config = mocker.MagicMock()
    config.mlflow = {"tracking_uri": "http://mock-mlflow:5000"}
    return config


def _make_model_version(mocker, *, version="1", tags=None):
    """Helper: build a mock MlflowClient model version object."""
    mv = mocker.MagicMock()
    mv.version = version
    mv.tags = tags or {}
    return mv


# ──────────────────────────────────────────────────────────────────────────────
# get_checkpoint_info
# ──────────────────────────────────────────────────────────────────────────────

class TestGetCheckpointInfo:

    def test_returns_checkpoint_info_when_mlflow_reachable(self, mocker, mock_beamline_config):
        """Happy path: reachable server + valid production alias → ModelCheckpointInfo."""
        from orchestration.mlflow import get_checkpoint_info

        mocker.patch("orchestration.mlflow._is_mlflow_reachable", return_value=True)

        mv = _make_model_version(mocker, version="3", tags={
            "nersc_path": "/cfs/checkpoints/sam3_v3.pt",
            "alcf_path": "/eagle/checkpoints/sam3_v3.pt",
            "batch_size": "2",
            "prompts": json.dumps(["cell wall", "lumen"]),
        })
        mock_client = mocker.MagicMock()
        mock_client.get_model_version_by_alias.return_value = mv
        mocker.patch("orchestration.mlflow.get_mlflow_client", return_value=mock_client)

        info = get_checkpoint_info("sam3-petiole", mock_beamline_config, alias="production")

        assert info is not None
        assert info.model_name == "sam3-petiole"
        assert info.version == "3"
        assert info.alias == "production"
        assert info.nersc_path == "/cfs/checkpoints/sam3_v3.pt"
        assert info.alcf_path == "/eagle/checkpoints/sam3_v3.pt"

    def test_deserializes_json_inference_params(self, mocker, mock_beamline_config):
        """JSON-encoded tag values (lists, dicts) are decoded into Python objects."""
        from orchestration.mlflow import get_checkpoint_info

        mocker.patch("orchestration.mlflow._is_mlflow_reachable", return_value=True)
        mv = _make_model_version(mocker, tags={
            "nersc_path": "/cfs/sam3.pt",
            "prompts": json.dumps(["cell wall", "lumen"]),
            "confidence": json.dumps([0.6, 0.7]),
            "batch_size": "4",
        })
        mock_client = mocker.MagicMock()
        mock_client.get_model_version_by_alias.return_value = mv
        mocker.patch("orchestration.mlflow.get_mlflow_client", return_value=mock_client)

        info = get_checkpoint_info("sam3-petiole", mock_beamline_config)

        assert info.inference_params["prompts"] == ["cell wall", "lumen"]
        assert info.inference_params["confidence"] == [0.6, 0.7]
        assert info.inference_params["batch_size"] == 4  # "4" is valid JSON → int

    def test_returns_none_when_mlflow_unreachable(self, mocker, mock_beamline_config):
        """Unreachable tracking server → None (caller falls back to config defaults)."""
        from orchestration.mlflow import get_checkpoint_info

        mocker.patch("orchestration.mlflow._is_mlflow_reachable", return_value=False)

        info = get_checkpoint_info("sam3-petiole", mock_beamline_config)

        assert info is None

    def test_returns_none_when_alias_not_found(self, mocker, mock_beamline_config):
        """Missing production alias → MlflowException → None."""
        from orchestration.mlflow import get_checkpoint_info
        import mlflow.exceptions

        mocker.patch("orchestration.mlflow._is_mlflow_reachable", return_value=True)
        mock_client = mocker.MagicMock()
        mock_client.get_model_version_by_alias.side_effect = (
            mlflow.exceptions.MlflowException("Alias not found")
        )
        mocker.patch("orchestration.mlflow.get_mlflow_client", return_value=mock_client)

        info = get_checkpoint_info("sam3-petiole", mock_beamline_config)

        assert info is None

    def test_returns_none_when_nersc_path_tag_missing(self, mocker, mock_beamline_config):
        """A model version without 'nersc_path' tag → None."""
        from orchestration.mlflow import get_checkpoint_info

        mocker.patch("orchestration.mlflow._is_mlflow_reachable", return_value=True)
        mv = _make_model_version(mocker, tags={"alcf_path": "/eagle/sam3.pt"})
        mock_client = mocker.MagicMock()
        mock_client.get_model_version_by_alias.return_value = mv
        mocker.patch("orchestration.mlflow.get_mlflow_client", return_value=mock_client)

        info = get_checkpoint_info("sam3-petiole", mock_beamline_config)

        assert info is None

    def test_nersc_and_alcf_paths_excluded_from_inference_params(self, mocker, mock_beamline_config):
        """nersc_path and alcf_path must NOT appear in inference_params."""
        from orchestration.mlflow import get_checkpoint_info

        mocker.patch("orchestration.mlflow._is_mlflow_reachable", return_value=True)
        mv = _make_model_version(mocker, tags={
            "nersc_path": "/cfs/sam3.pt",
            "alcf_path": "/eagle/sam3.pt",
            "batch_size": "2",
        })
        mock_client = mocker.MagicMock()
        mock_client.get_model_version_by_alias.return_value = mv
        mocker.patch("orchestration.mlflow.get_mlflow_client", return_value=mock_client)

        info = get_checkpoint_info("sam3-petiole", mock_beamline_config)

        assert "nersc_path" not in info.inference_params
        assert "alcf_path" not in info.inference_params
        assert "batch_size" in info.inference_params


# ──────────────────────────────────────────────────────────────────────────────
# _is_mlflow_reachable
# ──────────────────────────────────────────────────────────────────────────────

class TestIsMlflowReachable:
    def test_returns_true_when_health_200(self, mocker):
        from orchestration.mlflow import _is_mlflow_reachable
        mock_resp = mocker.MagicMock()
        mock_resp.status_code = 200
        mocker.patch("requests.get", return_value=mock_resp)
        assert _is_mlflow_reachable("http://mlflow:5000") is True

    def test_returns_false_when_non_200(self, mocker):
        from orchestration.mlflow import _is_mlflow_reachable
        mock_resp = mocker.MagicMock()
        mock_resp.status_code = 503
        mocker.patch("requests.get", return_value=mock_resp)
        assert _is_mlflow_reachable("http://mlflow:5000") is False

    def test_returns_false_on_exception(self, mocker):
        from orchestration.mlflow import _is_mlflow_reachable
        mocker.patch("requests.get", side_effect=Exception("timeout"))
        assert _is_mlflow_reachable("http://mlflow:5000") is False

    def test_sends_api_key_header_when_env_set(self, mocker, monkeypatch):
        from orchestration.mlflow import _is_mlflow_reachable
        monkeypatch.setenv("AMSC_API_KEY", "test-key-123")
        mock_resp = mocker.MagicMock()
        mock_resp.status_code = 200
        mock_get = mocker.patch("requests.get", return_value=mock_resp)
        _is_mlflow_reachable("http://mlflow:5000")
        assert mock_get.call_args.kwargs["headers"].get("X-Api-Key") == "test-key-123"

    def test_omits_api_key_header_when_env_unset(self, mocker, monkeypatch):
        from orchestration.mlflow import _is_mlflow_reachable
        monkeypatch.delenv("AMSC_API_KEY", raising=False)
        mock_resp = mocker.MagicMock()
        mock_resp.status_code = 200
        mock_get = mocker.patch("requests.get", return_value=mock_resp)
        _is_mlflow_reachable("http://mlflow:5000")
        assert "X-Api-Key" not in mock_get.call_args.kwargs["headers"]


# ──────────────────────────────────────────────────────────────────────────────
# _enable_amsc_x_api_key
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def reset_amsc_patch():
    """Save and restore rest_utils.http_request to isolate AMSC patch state."""
    original = rest_utils.http_request
    yield
    rest_utils.http_request = original


class TestEnableAmscXApiKey:
    def test_returns_false_when_key_unset(self, monkeypatch, reset_amsc_patch):
        from orchestration.mlflow import _enable_amsc_x_api_key, _AMSC_PATCH_FLAG
        monkeypatch.delenv("AMSC_API_KEY", raising=False)
        assert _enable_amsc_x_api_key() is False
        assert not getattr(rest_utils.http_request, _AMSC_PATCH_FLAG, False)

    def test_returns_true_and_patches_when_key_set(self, monkeypatch, reset_amsc_patch):
        from orchestration.mlflow import _enable_amsc_x_api_key, _AMSC_PATCH_FLAG
        monkeypatch.setenv("AMSC_API_KEY", "test-key")
        assert _enable_amsc_x_api_key() is True
        assert getattr(rest_utils.http_request, _AMSC_PATCH_FLAG, False) is True

    def test_idempotent_second_call_does_not_rewrap(self, monkeypatch, reset_amsc_patch):
        from orchestration.mlflow import _enable_amsc_x_api_key
        monkeypatch.setenv("AMSC_API_KEY", "test-key")
        _enable_amsc_x_api_key()
        patched_once = rest_utils.http_request
        _enable_amsc_x_api_key()
        assert rest_utils.http_request is patched_once

    def test_injects_key_via_extra_headers_when_no_headers_kwarg(self, mocker, monkeypatch, reset_amsc_patch):
        from orchestration.mlflow import _enable_amsc_x_api_key
        monkeypatch.setenv("AMSC_API_KEY", "my-key")
        spy = mocker.MagicMock()
        spy._amsc_x_api_key_patched = False  # prevent MagicMock auto-attr from being truthy
        rest_utils.http_request = spy  # captured as 'original' by the patch closure
        _enable_amsc_x_api_key()

        rest_utils.http_request(mocker.MagicMock(), "/api", "GET")

        assert spy.call_args.kwargs.get("extra_headers", {}).get("X-Api-Key") == "my-key"

    def test_injects_key_into_existing_headers_kwarg(self, mocker, monkeypatch, reset_amsc_patch):
        from orchestration.mlflow import _enable_amsc_x_api_key
        monkeypatch.setenv("AMSC_API_KEY", "my-key")
        spy = mocker.MagicMock()
        spy._amsc_x_api_key_patched = False  # prevent MagicMock auto-attr from being truthy
        rest_utils.http_request = spy
        _enable_amsc_x_api_key()

        rest_utils.http_request(
            mocker.MagicMock(), "/api", "GET", headers={"Content-Type": "application/json"}
        )

        assert spy.call_args.kwargs["headers"]["X-Api-Key"] == "my-key"
        assert spy.call_args.kwargs["headers"]["Content-Type"] == "application/json"


# ──────────────────────────────────────────────────────────────────────────────
# get_mlflow_client
# ──────────────────────────────────────────────────────────────────────────────

class TestGetMlflowClient:
    def test_returns_client_with_tracking_uri(self, mocker, mock_beamline_config):
        from orchestration.mlflow import get_mlflow_client
        mock_enable = mocker.patch("orchestration.mlflow._enable_amsc_x_api_key")
        mock_set_uri = mocker.patch("mlflow.set_tracking_uri")
        mock_client_cls = mocker.patch("orchestration.mlflow.MlflowClient")

        result = get_mlflow_client(mock_beamline_config)

        mock_enable.assert_called_once()
        mock_set_uri.assert_called_once_with("http://mock-mlflow:5000")
        mock_client_cls.assert_called_once_with(tracking_uri="http://mock-mlflow:5000")
        assert result is mock_client_cls.return_value


# ──────────────────────────────────────────────────────────────────────────────
# register_checkpoint
# ──────────────────────────────────────────────────────────────────────────────

def _setup_register_mocks(mocker, mock_beamline_config, *, version="1", existing_model=True, existing_experiment=True):
    """Wire up standard mocks for register_checkpoint; returns mock_client."""
    import mlflow.exceptions
    mock_client = mocker.MagicMock()
    if not existing_model:
        mock_client.get_registered_model.side_effect = mlflow.exceptions.MlflowException("not found")
    mocker.patch("orchestration.mlflow.get_mlflow_client", return_value=mock_client)
    mocker.patch("mlflow.set_tracking_uri")

    if existing_experiment:
        mock_exp = mocker.MagicMock()
        mock_exp.experiment_id = "exp-1"
        mocker.patch("mlflow.get_experiment_by_name", return_value=mock_exp)
    else:
        mocker.patch("mlflow.get_experiment_by_name", return_value=None)
        mocker.patch("mlflow.create_experiment", return_value="exp-new")

    mock_run = mocker.MagicMock()
    mock_run.info.run_id = "run-abc-123"
    mock_start = mocker.patch("mlflow.start_run")
    mock_start.return_value.__enter__.return_value = mock_run
    mock_start.return_value.__exit__.return_value = False
    mocker.patch("mlflow.log_param")
    mocker.patch("mlflow.log_params")

    mock_mv = mocker.MagicMock()
    mock_mv.version = version
    mocker.patch("mlflow.register_model", return_value=mock_mv)
    return mock_client


class TestRegisterCheckpoint:
    def test_happy_path_returns_version(self, mocker, mock_beamline_config):
        from orchestration.mlflow import register_checkpoint
        mock_client = _setup_register_mocks(mocker, mock_beamline_config)

        version = register_checkpoint("sam3-petiole", "/cfs/sam3.pt", mock_beamline_config)

        assert version == "1"
        mock_client.set_registered_model_alias.assert_called_once_with(
            "sam3-petiole", "production", "1"
        )
        mock_client.set_model_version_tag.assert_any_call(
            "sam3-petiole", "1", "nersc_path", "/cfs/sam3.pt"
        )

    def test_creates_registered_model_when_not_found(self, mocker, mock_beamline_config):
        from orchestration.mlflow import register_checkpoint
        mock_client = _setup_register_mocks(mocker, mock_beamline_config, existing_model=False)

        register_checkpoint("sam3-petiole", "/cfs/sam3.pt", mock_beamline_config)

        mock_client.create_registered_model.assert_called_once_with("sam3-petiole")

    def test_skips_create_model_when_already_exists(self, mocker, mock_beamline_config):
        from orchestration.mlflow import register_checkpoint
        mock_client = _setup_register_mocks(mocker, mock_beamline_config, existing_model=True)

        register_checkpoint("sam3-petiole", "/cfs/sam3.pt", mock_beamline_config)

        mock_client.create_registered_model.assert_not_called()

    def test_creates_experiment_when_not_found(self, mocker, mock_beamline_config):
        from orchestration.mlflow import register_checkpoint
        _setup_register_mocks(mocker, mock_beamline_config, existing_experiment=False)
        # Re-patch to get a reference for assertion (second patch wins)
        mock_create_exp = mocker.patch("mlflow.create_experiment", return_value="exp-new")

        register_checkpoint("sam3-petiole", "/cfs/sam3.pt", mock_beamline_config)

        mock_create_exp.assert_called_once()

    def test_alcf_path_tag_set_when_provided(self, mocker, mock_beamline_config):
        from orchestration.mlflow import register_checkpoint
        mock_client = _setup_register_mocks(mocker, mock_beamline_config)

        register_checkpoint(
            "sam3-petiole", "/cfs/sam3.pt", mock_beamline_config, alcf_path="/eagle/sam3.pt"
        )

        mock_client.set_model_version_tag.assert_any_call(
            "sam3-petiole", "1", "alcf_path", "/eagle/sam3.pt"
        )

    def test_alcf_path_tag_omitted_when_empty(self, mocker, mock_beamline_config):
        from orchestration.mlflow import register_checkpoint
        mock_client = _setup_register_mocks(mocker, mock_beamline_config)

        register_checkpoint("sam3-petiole", "/cfs/sam3.pt", mock_beamline_config, alcf_path="")

        tag_names = [c.args[2] for c in mock_client.set_model_version_tag.call_args_list]
        assert "alcf_path" not in tag_names

    def test_inference_params_list_json_encoded(self, mocker, mock_beamline_config):
        from orchestration.mlflow import register_checkpoint
        mock_client = _setup_register_mocks(mocker, mock_beamline_config)

        register_checkpoint(
            "sam3-petiole",
            "/cfs/sam3.pt",
            mock_beamline_config,
            inference_params={"prompts": ["cell wall", "lumen"], "batch_size": 4},
        )

        tag_calls = {c.args[2]: c.args[3] for c in mock_client.set_model_version_tag.call_args_list}
        assert tag_calls["prompts"] == json.dumps(["cell wall", "lumen"])
        assert tag_calls["batch_size"] == "4"

    def test_no_inference_params_skips_tag_loop(self, mocker, mock_beamline_config):
        from orchestration.mlflow import register_checkpoint
        mock_client = _setup_register_mocks(mocker, mock_beamline_config)

        register_checkpoint("sam3-petiole", "/cfs/sam3.pt", mock_beamline_config)

        tag_names = {c.args[2] for c in mock_client.set_model_version_tag.call_args_list}
        assert tag_names == {"nersc_path"}


# ──────────────────────────────────────────────────────────────────────────────
# log_segmentation_metrics
# ──────────────────────────────────────────────────────────────────────────────

def _setup_log_metrics_mocks(mocker, mock_beamline_config):
    """Wire up standard mocks for log_segmentation_metrics; returns mock_run."""
    mocker.patch("mlflow.set_tracking_uri")
    mocker.patch("orchestration.mlflow._enable_amsc_x_api_key")
    mock_exp = mocker.MagicMock()
    mock_exp.experiment_id = "exp-1"
    mocker.patch("mlflow.get_experiment_by_name", return_value=mock_exp)

    mock_run = mocker.MagicMock()
    mock_run.info.run_id = "run-xyz-999"
    mock_start = mocker.patch("mlflow.start_run")
    mock_start.return_value.__enter__.return_value = mock_run
    mock_start.return_value.__exit__.return_value = False

    mocker.patch("mlflow.log_param")
    mocker.patch("mlflow.log_params")
    mocker.patch("mlflow.log_metrics")
    return mock_run


class TestLogSegmentationMetrics:
    def test_happy_path_returns_run_id(self, mocker, mock_beamline_config):
        from orchestration.mlflow import log_segmentation_metrics
        _setup_log_metrics_mocks(mocker, mock_beamline_config)

        run_id = log_segmentation_metrics("seg-run-1", "sam3", "job-42", mock_beamline_config)

        assert run_id == "run-xyz-999"

    def test_logs_slurm_job_id_and_model_params(self, mocker, mock_beamline_config):
        from orchestration.mlflow import log_segmentation_metrics
        _setup_log_metrics_mocks(mocker, mock_beamline_config)
        mock_log_param = mocker.patch("mlflow.log_param")

        log_segmentation_metrics("seg-run-1", "sam3", "job-42", mock_beamline_config)

        mock_log_param.assert_any_call("slurm_job_id", "job-42")
        mock_log_param.assert_any_call("model", "sam3")

    def test_full_timing_dict_logged_as_metrics(self, mocker, mock_beamline_config):
        from orchestration.mlflow import log_segmentation_metrics
        _setup_log_metrics_mocks(mocker, mock_beamline_config)
        mock_log_metrics = mocker.patch("mlflow.log_metrics")

        timing = {
            "total_seconds": 120.5,
            "num_images": 50,
            "throughput": 25.0,
            "time_per_image": "2.41s",
        }
        log_segmentation_metrics("seg-run-1", "sam3", "job-42", mock_beamline_config, timing=timing)

        logged = mock_log_metrics.call_args.args[0]
        assert logged["total_seconds"] == 120.5
        assert logged["num_images"] == 50.0
        assert logged["throughput_images_per_min"] == 25.0
        assert logged["time_per_image_seconds"] == pytest.approx(2.41)

    def test_time_per_image_unit_stripped(self, mocker, mock_beamline_config):
        from orchestration.mlflow import log_segmentation_metrics
        _setup_log_metrics_mocks(mocker, mock_beamline_config)
        mock_log_metrics = mocker.patch("mlflow.log_metrics")

        log_segmentation_metrics(
            "seg-run-1", "sam3", "job-42", mock_beamline_config,
            timing={"time_per_image": "3.23s"},
        )

        logged = mock_log_metrics.call_args.args[0]
        assert logged["time_per_image_seconds"] == pytest.approx(3.23)

    def test_non_numeric_time_per_image_omitted(self, mocker, mock_beamline_config):
        from orchestration.mlflow import log_segmentation_metrics
        _setup_log_metrics_mocks(mocker, mock_beamline_config)
        mock_log_metrics = mocker.patch("mlflow.log_metrics")

        log_segmentation_metrics(
            "seg-run-1", "sam3", "job-42", mock_beamline_config,
            timing={"time_per_image": "N/A"},
        )

        mock_log_metrics.assert_not_called()

    def test_parent_run_id_sets_nested_true(self, mocker, mock_beamline_config):
        from orchestration.mlflow import log_segmentation_metrics
        _setup_log_metrics_mocks(mocker, mock_beamline_config)
        mock_start = mocker.patch("mlflow.start_run")
        mock_run = mocker.MagicMock()
        mock_run.info.run_id = "child-run"
        mock_start.return_value.__enter__.return_value = mock_run
        mock_start.return_value.__exit__.return_value = False

        log_segmentation_metrics(
            "seg-run-1", "sam3", "job-42", mock_beamline_config,
            parent_run_id="parent-run-id-123",
        )

        kwargs = mock_start.call_args.kwargs
        assert kwargs["nested"] is True
        assert kwargs["parent_run_id"] == "parent-run-id-123"

    def test_extra_params_logged(self, mocker, mock_beamline_config):
        from orchestration.mlflow import log_segmentation_metrics
        _setup_log_metrics_mocks(mocker, mock_beamline_config)
        mock_log_params = mocker.patch("mlflow.log_params")

        log_segmentation_metrics(
            "seg-run-1", "sam3", "job-42", mock_beamline_config,
            params={"dataset": "beamline_832", "threshold": 0.5},
        )

        mock_log_params.assert_called_once_with({"dataset": "beamline_832", "threshold": 0.5})

    def test_amsc_patch_called(self, mocker, mock_beamline_config):
        from orchestration.mlflow import log_segmentation_metrics
        _setup_log_metrics_mocks(mocker, mock_beamline_config)
        mock_enable = mocker.patch("orchestration.mlflow._enable_amsc_x_api_key")

        log_segmentation_metrics("seg-run-1", "sam3", "job-42", mock_beamline_config)

        mock_enable.assert_called()

    def test_no_metrics_logged_when_no_timing(self, mocker, mock_beamline_config):
        from orchestration.mlflow import log_segmentation_metrics
        _setup_log_metrics_mocks(mocker, mock_beamline_config)
        mock_log_metrics = mocker.patch("mlflow.log_metrics")

        log_segmentation_metrics("seg-run-1", "sam3", "job-42", mock_beamline_config)

        mock_log_metrics.assert_not_called()
