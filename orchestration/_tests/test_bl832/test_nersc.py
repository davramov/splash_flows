# orchestration/_tests/bl832/test_nersc.py

import pytest
from uuid import uuid4

from prefect.blocks.system import Secret
from prefect.testing.utilities import prefect_test_harness


# ──────────────────────────────────────────────────────────────────────────────
# Session fixture
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    """Set up Prefect test harness and required secrets for the full session."""
    with prefect_test_harness():
        Secret(value=str(uuid4())).save(name="globus-client-id", overwrite=True)
        Secret(value=str(uuid4())).save(name="globus-client-secret", overwrite=True)
        yield


# ──────────────────────────────────────────────────────────────────────────────
# Shared fixtures
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_sfapi_client(mocker):
    """Mock sfapi_client.Client with a completed job on Perlmutter."""
    mock_client = mocker.MagicMock()

    mock_user = mocker.MagicMock()
    mock_user.name = "testuser"
    mock_client.user.return_value = mock_user

    mock_job = mocker.MagicMock()
    mock_job.jobid = "12345"
    mock_job.state = "COMPLETED"

    mock_compute = mocker.MagicMock()
    mock_compute.submit_job.return_value = mock_job
    mock_client.compute.return_value = mock_compute

    mocker.patch("orchestration.flows.bl832.nersc.Client", return_value=mock_client)
    return mock_client


@pytest.fixture
def mock_config832(mocker):
    """
    Mock Config832 constructor so any call to Config832() returns our mock.
    Tests that call flows must pass config=None so Prefect's type validation
    is never given a MagicMock — the flow will call Config832() internally and
    get our mock back.
    """
    mock_config = mocker.MagicMock()

    mock_config.ghcr_images832 = {
        "recon_image": "mock_recon_image",
        "multires_image": "mock_multires_image",
    }

    for attr in [
        "nersc832_alsdev_raw",
        "nersc832_alsdev_scratch",
        "nersc832_alsdev_recon_scripts",
        "nersc832_alsdev_pscratch_scratch",
        "nersc832_alsdev_pscratch_raw",
        "data832_scratch",
    ]:
        ep = mocker.MagicMock()
        ep.root_path = f"/mock/{attr}"
        setattr(mock_config, attr, ep)

    mock_config.nersc_recon_num_nodes = 4

    mocker.patch("orchestration.flows.bl832.nersc.Config832", return_value=mock_config)
    return mock_config


@pytest.fixture
def mock_recon_success():
    return {"success": True, "job_id": "11111", "timing": None}


@pytest.fixture
def mock_seg_sam3_success():
    return {"success": True, "job_id": "22222", "timing": None, "output_dir": "/out/sam3"}


def _make_future(mocker, value):
    """Return a mock Prefect future whose .result() yields the given value."""
    f = mocker.MagicMock()
    f.result.return_value = value
    return f


# ──────────────────────────────────────────────────────────────────────────────
# create_sfapi_client
# ──────────────────────────────────────────────────────────────────────────────

def test_create_sfapi_client_success(mocker):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

    mocker.patch("orchestration.flows.bl832.nersc.os.getenv", side_effect=lambda x: {
        "PATH_NERSC_CLIENT_ID": "/path/to/client_id",
        "PATH_NERSC_PRI_KEY": "/path/to/client_secret",
    }.get(x))
    mocker.patch("orchestration.flows.bl832.nersc.os.path.isfile", return_value=True)
    mocker.patch("builtins.open", side_effect=[
        mocker.mock_open(read_data="client_id_value")(),
        mocker.mock_open(read_data='{"key": "value"}')(),
    ])
    mocker.patch("orchestration.flows.bl832.nersc.JsonWebKey.import_key", return_value="mock_secret")
    mock_client_cls = mocker.patch("orchestration.flows.bl832.nersc.Client")

    client = NERSCTomographyHPCController.create_sfapi_client()

    mock_client_cls.assert_called_once_with("client_id_value", "mock_secret")
    assert client == mock_client_cls.return_value


def test_create_sfapi_client_missing_paths(mocker):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

    mocker.patch("orchestration.flows.bl832.nersc.os.getenv", return_value=None)

    with pytest.raises(ValueError, match="Missing NERSC credentials paths."):
        NERSCTomographyHPCController.create_sfapi_client()


def test_create_sfapi_client_missing_files(mocker):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

    mocker.patch("orchestration.flows.bl832.nersc.os.getenv", side_effect=lambda x: {
        "PATH_NERSC_CLIENT_ID": "/path/to/client_id",
        "PATH_NERSC_PRI_KEY": "/path/to/client_secret",
    }.get(x))
    mocker.patch("orchestration.flows.bl832.nersc.os.path.isfile", return_value=False)

    with pytest.raises(FileNotFoundError, match="NERSC credential files are missing."):
        NERSCTomographyHPCController.create_sfapi_client()


# ──────────────────────────────────────────────────────────────────────────────
# reconstruct
# ──────────────────────────────────────────────────────────────────────────────

def test_reconstruct_success(mocker, mock_sfapi_client, mock_config832):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController
    from sfapi_client.compute import Machine

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    controller = NERSCTomographyHPCController(client=mock_sfapi_client, config=mock_config832)

    result = controller.reconstruct(file_path="folder/file.h5")

    mock_sfapi_client.compute.assert_called_once_with(Machine.perlmutter)
    mock_sfapi_client.compute.return_value.submit_job.assert_called_once()
    mock_sfapi_client.compute.return_value.submit_job.return_value.complete.assert_called_once()
    assert result is True


def test_reconstruct_submission_failure(mocker, mock_sfapi_client, mock_config832):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mock_sfapi_client.compute.return_value.submit_job.side_effect = Exception("Submission failed")
    controller = NERSCTomographyHPCController(client=mock_sfapi_client, config=mock_config832)

    result = controller.reconstruct(file_path="folder/file.h5")

    assert result is False


# ──────────────────────────────────────────────────────────────────────────────
# build_multi_resolution
# ──────────────────────────────────────────────────────────────────────────────

def test_build_multi_resolution_success(mocker, mock_sfapi_client, mock_config832):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController
    from sfapi_client.compute import Machine

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    controller = NERSCTomographyHPCController(client=mock_sfapi_client, config=mock_config832)

    result = controller.build_multi_resolution(file_path="folder/file.h5")

    mock_sfapi_client.compute.assert_called_once_with(Machine.perlmutter)
    mock_sfapi_client.compute.return_value.submit_job.assert_called_once()
    mock_sfapi_client.compute.return_value.submit_job.return_value.complete.assert_called_once()
    assert result is True


def test_build_multi_resolution_submission_failure(mocker, mock_sfapi_client, mock_config832):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mock_sfapi_client.compute.return_value.submit_job.side_effect = Exception("Submission failed")
    controller = NERSCTomographyHPCController(client=mock_sfapi_client, config=mock_config832)

    result = controller.build_multi_resolution(file_path="folder/file.h5")

    assert result is False


# ──────────────────────────────────────────────────────────────────────────────
# segmentation_sam3
# ──────────────────────────────────────────────────────────────────────────────

def test_segmentation_sam3_success(mocker, mock_sfapi_client, mock_config832):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController
    from sfapi_client.compute import Machine

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mocker.patch("orchestration.flows.bl832.nersc.Variable.get", return_value={"defaults": True})
    controller = NERSCTomographyHPCController(client=mock_sfapi_client, config=mock_config832)
    mocker.patch.object(controller, "_fetch_seg_timing_from_output", return_value=None)

    result = controller.segmentation_sam3(recon_folder_path="folder/recfile")

    mock_sfapi_client.compute.assert_called_with(Machine.perlmutter)
    mock_sfapi_client.compute.return_value.submit_job.assert_called_once()
    mock_sfapi_client.compute.return_value.submit_job.return_value.complete.assert_called_once()
    assert isinstance(result, dict)
    assert result["success"] is True
    assert result["job_id"] == "12345"


def test_segmentation_sam3_submission_failure(mocker, mock_sfapi_client, mock_config832):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mocker.patch("orchestration.flows.bl832.nersc.Variable.get", return_value={"defaults": True})
    mock_sfapi_client.compute.return_value.submit_job.side_effect = Exception("GPU queue full")
    controller = NERSCTomographyHPCController(client=mock_sfapi_client, config=mock_config832)

    result = controller.segmentation_sam3(recon_folder_path="folder/recfile")

    assert isinstance(result, dict)
    assert result["success"] is False
    assert result["job_id"] is None


def test_segmentation_sam3_uses_variable_options(mocker, mock_sfapi_client, mock_config832):
    """Custom Prefect variable options should be forwarded into the job script."""
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mocker.patch("orchestration.flows.bl832.nersc.Variable.get", return_value={
        "defaults": False,
        "batch_size": 8,
        "patch_size": 512,
        "confidence": [0.6, 0.7],
        "overlap": 0.5,
        "qos": "debug",
        "account": "als_test",
        "constraint": "gpu",
        "checkpoint": "checkpoint_v7.pt",
    })

    controller = NERSCTomographyHPCController(client=mock_sfapi_client, config=mock_config832)
    mocker.patch.object(controller, "_fetch_seg_timing_from_output", return_value=None)

    captured_scripts = []
    original_return = mock_sfapi_client.compute.return_value.submit_job.return_value

    def capture_script(script):
        captured_scripts.append(script)
        return original_return

    mock_sfapi_client.compute.return_value.submit_job.side_effect = capture_script

    controller.segmentation_sam3(recon_folder_path="folder/recfile")

    assert captured_scripts, "submit_job was never called"
    script = captured_scripts[0]
    assert "checkpoint_v7.pt" in script
    assert "--patch-size 512" in script
    assert "0.6 0.7" in script
    assert "--overlap-ratio 0.5" in script
    assert "#SBATCH -q debug" in script
    assert "#SBATCH -A als_test" in script


# ──────────────────────────────────────────────────────────────────────────────
# segmentation_dino
# ──────────────────────────────────────────────────────────────────────────────

def test_segmentation_dino_success(mocker, mock_sfapi_client, mock_config832):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController
    from sfapi_client.compute import Machine

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mocker.patch("orchestration.flows.bl832.nersc.Variable.get", return_value={"defaults": True})
    controller = NERSCTomographyHPCController(client=mock_sfapi_client, config=mock_config832)

    result = controller.segmentation_dino(recon_folder_path="folder/recfile")

    mock_sfapi_client.compute.assert_called_with(Machine.perlmutter)
    mock_sfapi_client.compute.return_value.submit_job.assert_called_once()
    mock_sfapi_client.compute.return_value.submit_job.return_value.complete.assert_called_once()
    assert result is True


def test_segmentation_dino_submission_failure(mocker, mock_sfapi_client, mock_config832):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mocker.patch("orchestration.flows.bl832.nersc.Variable.get", return_value={"defaults": True})
    mock_sfapi_client.compute.return_value.submit_job.side_effect = Exception("No GPU nodes")
    controller = NERSCTomographyHPCController(client=mock_sfapi_client, config=mock_config832)

    result = controller.segmentation_dino(recon_folder_path="folder/recfile")

    assert result is False


def test_segmentation_dino_output_paths(mocker, mock_sfapi_client, mock_config832):
    """
    Output dir should swap /rec for /seg in the folder name and route to /dino.

    Given recon_folder_path="folder/recfile", the code does:
        seg_folder = "folder/recfile".replace("/rec", "/seg")  →  "folder/segfile"
        output_dir = ".../scratch/folder/segfile/dino"
    So the script contains "segfile" and "/dino", not a literal "/seg/" segment.
    """
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mocker.patch("orchestration.flows.bl832.nersc.Variable.get", return_value={"defaults": True})

    captured_scripts = []
    original_return = mock_sfapi_client.compute.return_value.submit_job.return_value

    def capture(script):
        captured_scripts.append(script)
        return original_return

    mock_sfapi_client.compute.return_value.submit_job.side_effect = capture
    controller = NERSCTomographyHPCController(client=mock_sfapi_client, config=mock_config832)
    controller.segmentation_dino(recon_folder_path="folder/recfile")

    script = captured_scripts[0]
    # The rec→seg substitution turns "recfile" into "segfile"
    assert "segfile" in script
    assert "/dino" in script


# ──────────────────────────────────────────────────────────────────────────────
# combine_segmentations
# ──────────────────────────────────────────────────────────────────────────────

def test_combine_segmentations_success(mocker, mock_sfapi_client, mock_config832):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController
    from sfapi_client.compute import Machine

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mocker.patch("orchestration.flows.bl832.nersc.Variable.get", return_value={"defaults": True})
    controller = NERSCTomographyHPCController(client=mock_sfapi_client, config=mock_config832)

    result = controller.combine_segmentations(recon_folder_path="folder/recfile")

    mock_sfapi_client.compute.assert_called_with(Machine.perlmutter)
    mock_sfapi_client.compute.return_value.submit_job.assert_called_once()
    mock_sfapi_client.compute.return_value.submit_job.return_value.complete.assert_called_once()
    assert result is True


def test_combine_segmentations_submission_failure(mocker, mock_sfapi_client, mock_config832):
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mocker.patch("orchestration.flows.bl832.nersc.Variable.get", return_value={"defaults": True})
    mock_sfapi_client.compute.return_value.submit_job.side_effect = Exception("Cluster down")
    controller = NERSCTomographyHPCController(client=mock_sfapi_client, config=mock_config832)

    result = controller.combine_segmentations(recon_folder_path="folder/recfile")

    assert result is False


def test_combine_segmentations_script_references_sam3_and_dino(mocker, mock_sfapi_client, mock_config832):
    """The combination job script should reference both model output directories."""
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

    mocker.patch("orchestration.flows.bl832.nersc.time.sleep")
    mocker.patch("orchestration.flows.bl832.nersc.Variable.get", return_value={"defaults": True})

    captured_scripts = []
    original_return = mock_sfapi_client.compute.return_value.submit_job.return_value

    def capture(script):
        captured_scripts.append(script)
        return original_return

    mock_sfapi_client.compute.return_value.submit_job.side_effect = capture
    controller = NERSCTomographyHPCController(client=mock_sfapi_client, config=mock_config832)
    controller.combine_segmentations(recon_folder_path="folder/recfile")

    script = captured_scripts[0]
    assert "/sam3" in script
    assert "/dino" in script
    assert "combine_sam_dino_v3" in script
    assert "/combined" in script


# ──────────────────────────────────────────────────────────────────────────────
# Prefect tasks
#
# We call task.fn() to bypass Prefect's task runner machinery, but the task
# bodies call get_run_logger() which requires an active flow/task run context.
# Patching it at the module level avoids MissingContextError without needing
# a full prefect_test_harness flow run.
# ──────────────────────────────────────────────────────────────────────────────

def test_nersc_segmentation_sam3_task_success(mocker, mock_config832):
    from orchestration.flows.bl832.nersc import nersc_segmentation_sam3_task

    mocker.patch("orchestration.flows.bl832.nersc.get_run_logger", return_value=mocker.MagicMock())
    mock_controller = mocker.MagicMock()
    mock_controller.segmentation_sam3.return_value = {
        "success": True, "job_id": "99", "timing": None, "output_dir": "/out"
    }
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    result = nersc_segmentation_sam3_task.fn(
        recon_folder_path="folder/recfile",
        config=mock_config832
    )

    mock_controller.segmentation_sam3.assert_called_once_with(recon_folder_path="folder/recfile")
    assert result == {"success": True, "job_id": "99", "timing": None, "output_dir": "/out"}


def test_nersc_segmentation_sam3_task_failure(mocker, mock_config832):
    from orchestration.flows.bl832.nersc import nersc_segmentation_sam3_task

    mocker.patch("orchestration.flows.bl832.nersc.get_run_logger", return_value=mocker.MagicMock())
    mock_controller = mocker.MagicMock()
    mock_controller.segmentation_sam3.return_value = {
        "success": False, "job_id": None, "timing": None, "output_dir": None
    }
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    result = nersc_segmentation_sam3_task.fn(
        recon_folder_path="folder/recfile",
        config=mock_config832
    )

    assert result["success"] is False


def test_nersc_segmentation_dino_task_success(mocker, mock_config832):
    from orchestration.flows.bl832.nersc import nersc_segmentation_dino_task

    mocker.patch("orchestration.flows.bl832.nersc.get_run_logger", return_value=mocker.MagicMock())
    mock_controller = mocker.MagicMock()
    mock_controller.segmentation_dino.return_value = True
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    result = nersc_segmentation_dino_task.fn(
        recon_folder_path="folder/recfile",
        config=mock_config832
    )

    mock_controller.segmentation_dino.assert_called_once_with(recon_folder_path="folder/recfile")
    assert result is True


def test_nersc_segmentation_dino_task_failure(mocker, mock_config832):
    from orchestration.flows.bl832.nersc import nersc_segmentation_dino_task

    mocker.patch("orchestration.flows.bl832.nersc.get_run_logger", return_value=mocker.MagicMock())
    mock_controller = mocker.MagicMock()
    mock_controller.segmentation_dino.return_value = False
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    result = nersc_segmentation_dino_task.fn(
        recon_folder_path="folder/recfile",
        config=mock_config832
    )

    assert result is False


def test_nersc_combine_segmentations_task_success(mocker, mock_config832):
    from orchestration.flows.bl832.nersc import nersc_combine_segmentations_task

    mocker.patch("orchestration.flows.bl832.nersc.get_run_logger", return_value=mocker.MagicMock())
    mock_controller = mocker.MagicMock()
    mock_controller.combine_segmentations.return_value = True
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    result = nersc_combine_segmentations_task.fn(
        recon_folder_path="folder/recfile",
        config=mock_config832
    )

    mock_controller.combine_segmentations.assert_called_once_with(recon_folder_path="folder/recfile")
    assert result is True


def test_nersc_combine_segmentations_task_failure(mocker, mock_config832):
    from orchestration.flows.bl832.nersc import nersc_combine_segmentations_task

    mocker.patch("orchestration.flows.bl832.nersc.get_run_logger", return_value=mocker.MagicMock())
    mock_controller = mocker.MagicMock()
    mock_controller.combine_segmentations.return_value = False
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    result = nersc_combine_segmentations_task.fn(
        recon_folder_path="folder/recfile",
        config=mock_config832
    )

    assert result is False


# ──────────────────────────────────────────────────────────────────────────────
# nersc_forge_recon_segment_flow
#
# Prefect validates the `config` parameter against Optional[Config832] at
# runtime, so passing a MagicMock raises ParameterTypeError. The fix is to
# pass config=None — the flow's `if config is None: config = Config832()`
# branch then runs, calling the already-mocked constructor and returning our
# mock_config832 instance.
# ──────────────────────────────────────────────────────────────────────────────

def test_forge_recon_segment_flow_success(mocker, mock_config832, mock_recon_success, mock_seg_sam3_success):
    from orchestration.flows.bl832.nersc import nersc_forge_recon_segment_flow

    mock_controller = mocker.MagicMock()
    mock_controller.reconstruct_multinode.return_value = mock_recon_success
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    mock_transfer = mocker.MagicMock()
    mock_transfer.copy.return_value = True
    mocker.patch("orchestration.flows.bl832.nersc.get_transfer_controller", return_value=mock_transfer)
    mocker.patch("orchestration.flows.bl832.nersc.get_prune_controller", return_value=mocker.MagicMock())

    mock_seg_task = mocker.patch(
        "orchestration.flows.bl832.nersc.nersc_segmentation_sam3_task",
        return_value=mock_seg_sam3_success
    )

    result = nersc_forge_recon_segment_flow(file_path="folder/file.h5", num_nodes=4)

    assert result is True
    mock_controller.reconstruct_multinode.assert_called_once()
    mock_seg_task.assert_called_once()
    assert mock_transfer.copy.call_count >= 2


def test_forge_recon_segment_flow_recon_failure(mocker, mock_config832):
    from orchestration.flows.bl832.nersc import nersc_forge_recon_segment_flow

    mock_controller = mocker.MagicMock()
    mock_controller.reconstruct_multinode.return_value = {"success": False, "job_id": None, "timing": None}
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)
    mocker.patch("orchestration.flows.bl832.nersc.get_transfer_controller", return_value=mocker.MagicMock())
    mocker.patch("orchestration.flows.bl832.nersc.get_prune_controller", return_value=mocker.MagicMock())

    with pytest.raises(ValueError, match="Reconstruction at NERSC Failed"):
        nersc_forge_recon_segment_flow(file_path="folder/file.h5", num_nodes=4)


def test_forge_recon_segment_flow_seg_failure(mocker, mock_config832, mock_recon_success):
    """Flow should return False (not raise) when only segmentation fails."""
    from orchestration.flows.bl832.nersc import nersc_forge_recon_segment_flow

    mock_controller = mocker.MagicMock()
    mock_controller.reconstruct_multinode.return_value = mock_recon_success
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    mock_transfer = mocker.MagicMock()
    mock_transfer.copy.return_value = True
    mocker.patch("orchestration.flows.bl832.nersc.get_transfer_controller", return_value=mock_transfer)
    mocker.patch("orchestration.flows.bl832.nersc.get_prune_controller", return_value=mocker.MagicMock())
    mocker.patch(
        "orchestration.flows.bl832.nersc.nersc_segmentation_sam3_task",
        return_value={"success": False, "job_id": None, "timing": None, "output_dir": None}
    )

    result = nersc_forge_recon_segment_flow(file_path="folder/file.h5", num_nodes=4)

    assert result is False


# ──────────────────────────────────────────────────────────────────────────────
# nersc_forge_recon_multisegment_flow
# ──────────────────────────────────────────────────────────────────────────────

def test_forge_recon_multisegment_flow_both_succeed(mocker, mock_config832, mock_recon_success):
    from orchestration.flows.bl832.nersc import nersc_forge_recon_multisegment_flow

    mock_controller = mocker.MagicMock()
    mock_controller.reconstruct_multinode.return_value = mock_recon_success
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    mock_transfer = mocker.MagicMock()
    mock_transfer.copy.return_value = True
    mocker.patch("orchestration.flows.bl832.nersc.get_transfer_controller", return_value=mock_transfer)
    mocker.patch("orchestration.flows.bl832.nersc.get_prune_controller", return_value=mocker.MagicMock())

    mock_sam3_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_segmentation_sam3_task")
    mock_dino_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_segmentation_dino_task")
    mock_combine_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_combine_segmentations_task")

    mock_sam3_task.submit.return_value = _make_future(
        mocker, {"success": True, "job_id": "1", "timing": None, "output_dir": "/out"}
    )
    mock_dino_task.submit.return_value = _make_future(mocker, True)
    mock_combine_task.submit.return_value = _make_future(mocker, True)

    result = nersc_forge_recon_multisegment_flow(file_path="folder/file.h5", num_nodes=4)

    assert result is True
    mock_sam3_task.submit.assert_called_once()
    mock_dino_task.submit.assert_called_once()
    mock_combine_task.submit.assert_called_once()


def test_forge_recon_multisegment_flow_only_sam3_succeeds(mocker, mock_config832, mock_recon_success):
    """When only SAM3 succeeds, combine should be skipped but flow returns True."""
    from orchestration.flows.bl832.nersc import nersc_forge_recon_multisegment_flow

    mock_controller = mocker.MagicMock()
    mock_controller.reconstruct_multinode.return_value = mock_recon_success
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    mock_transfer = mocker.MagicMock()
    mock_transfer.copy.return_value = True
    mocker.patch("orchestration.flows.bl832.nersc.get_transfer_controller", return_value=mock_transfer)
    mocker.patch("orchestration.flows.bl832.nersc.get_prune_controller", return_value=mocker.MagicMock())

    mock_sam3_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_segmentation_sam3_task")
    mock_dino_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_segmentation_dino_task")
    mock_combine_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_combine_segmentations_task")

    mock_sam3_task.submit.return_value = _make_future(
        mocker, {"success": True, "job_id": "1", "timing": None, "output_dir": "/out"}
    )
    mock_dino_task.submit.return_value = _make_future(mocker, False)

    result = nersc_forge_recon_multisegment_flow(file_path="folder/file.h5", num_nodes=4)

    assert result is True
    mock_combine_task.submit.assert_not_called()


def test_forge_recon_multisegment_flow_both_seg_fail(mocker, mock_config832, mock_recon_success):
    from orchestration.flows.bl832.nersc import nersc_forge_recon_multisegment_flow

    mock_controller = mocker.MagicMock()
    mock_controller.reconstruct_multinode.return_value = mock_recon_success
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)

    mock_transfer = mocker.MagicMock()
    mock_transfer.copy.return_value = False
    mocker.patch("orchestration.flows.bl832.nersc.get_transfer_controller", return_value=mock_transfer)
    mocker.patch("orchestration.flows.bl832.nersc.get_prune_controller", return_value=mocker.MagicMock())

    mock_sam3_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_segmentation_sam3_task")
    mock_dino_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_segmentation_dino_task")
    mock_combine_task = mocker.patch("orchestration.flows.bl832.nersc.nersc_combine_segmentations_task")

    mock_sam3_task.submit.return_value = _make_future(
        mocker, {"success": False, "job_id": None, "timing": None, "output_dir": None}
    )
    mock_dino_task.submit.return_value = _make_future(mocker, False)

    result = nersc_forge_recon_multisegment_flow(file_path="folder/file.h5", num_nodes=4)

    assert result is False
    mock_combine_task.submit.assert_not_called()


def test_forge_recon_multisegment_flow_recon_failure(mocker, mock_config832):
    from orchestration.flows.bl832.nersc import nersc_forge_recon_multisegment_flow

    mock_controller = mocker.MagicMock()
    mock_controller.reconstruct_multinode.return_value = {"success": False, "job_id": None, "timing": None}
    mocker.patch("orchestration.flows.bl832.nersc.get_controller", return_value=mock_controller)
    mocker.patch("orchestration.flows.bl832.nersc.get_transfer_controller", return_value=mocker.MagicMock())
    mocker.patch("orchestration.flows.bl832.nersc.get_prune_controller", return_value=mocker.MagicMock())

    with pytest.raises(ValueError, match="Reconstruction at NERSC Failed"):
        nersc_forge_recon_multisegment_flow(file_path="folder/file.h5", num_nodes=4)
