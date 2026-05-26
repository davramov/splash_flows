"""Tests for the bl832 dispatcher flow.
"""
import asyncio
import warnings
from uuid import UUID, uuid4, uuid5

import pytest
from prefect.blocks.system import Secret
from prefect.testing.utilities import prefect_test_harness
from prefect.variables import Variable
from pytest_mock import MockFixture

from orchestration._tests.test_globus import MockTransferClient

warnings.filterwarnings("ignore", category=DeprecationWarning)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


DEFAULT_DECISION_SETTINGS = {
    "alcf_recon_flow/alcf_recon_flow": True,
    "nersc_recon_flow/nersc_recon_flow": True,
    "nersc_petiole_segment_flow/nersc_petiole_segment_flow": True,
    "nersc_moon_segment_flow/nersc_moon_segment_flow": True,
    "new_832_file_flow/new_file_832": True,
}


@pytest.fixture(autouse=True, scope="session")
def bl832_dispatcher_prefect_fixture():
    """Set up Prefect test harness and bl832 variables/secrets for the session."""
    with prefect_test_harness():
        Secret(value=str(uuid4())).save(name="globus-client-id", overwrite=True)
        Secret(value=str(uuid4())).save(name="globus-client-secret", overwrite=True)
        Secret(value=str(uuid4())).save(name="globus-compute-endpoint", overwrite=True)

        Variable.set(
            name="pruning-config",
            value={"max_wait_seconds": 600},
            overwrite=True,
            _sync=True,
        )
        Variable.set(
            name="decision-settings",
            value=DEFAULT_DECISION_SETTINGS,
            overwrite=True,
            _sync=True,
        )
        Variable.set(
            name="alcf-allocation-root-path",
            value={"alcf-allocation-root-path": "/eagle/IRIProd/ALS"},
            overwrite=True,
            _sync=True,
        )
        Variable.set(
            name="bl832-settings",
            value={
                "delete_spot832_files_after_days": 1,
                "delete_data832_files_after_days": 35,
            },
            overwrite=True,
            _sync=True,
        )

        yield


@pytest.fixture(autouse=True)
def reset_iec_variable():
    """Reset is_export_control Prefect Variable to False before each test.

    Tests that need a different starting state (True, or missing) should
    override this within the test body after the fixture runs.
    """
    Variable.set(name="is_export_control", value=False, overwrite=True, _sync=True)
    yield


@pytest.fixture(autouse=True)
def reset_decision_settings():
    """Reset decision-settings Prefect Variable before each test.

    Tests that mutate decision-settings won't leak state to subsequent tests.
    """
    Variable.set(
        name="decision-settings",
        value=DEFAULT_DECISION_SETTINGS,
        overwrite=True,
        _sync=True,
    )
    yield


# ---------------------------------------------------------------------------
# Mocks
# ---------------------------------------------------------------------------


class MockEndpoint:
    """Mock Globus endpoint."""

    def __init__(self, root_path: str, uuid_value: str = None, name: str = None) -> None:
        self.root_path = root_path
        self.uuid = uuid_value or str(uuid4())
        self.uri = f"mock_endpoint_uri_{self.uuid}"
        self.name = name or f"mock_endpoint_{self.uuid[:8]}"


class MockSecret:
    """Deterministic secret values for tests."""

    value = "550e8400-e29b-41d4-a716-446655440000"

    @staticmethod
    def for_endpoint(endpoint_name: str) -> str:
        """Generate a deterministic UUID string based on endpoint name."""
        namespace = UUID("12345678-1234-5678-1234-123456789012")
        return str(uuid5(namespace, endpoint_name))


class MockConfig832:
    """Mock Config832 to avoid real Globus initialization."""

    def __init__(self) -> None:
        self.endpoints = {
            "spot832": MockEndpoint("mock_spot832_path", MockSecret.for_endpoint("spot832")),
            "data832": MockEndpoint("mock_data832_path", MockSecret.for_endpoint("data832")),
            "nersc832": MockEndpoint("mock_nersc832_path", MockSecret.for_endpoint("nersc832")),
            "alcf832_raw": MockEndpoint(
                "mock_alcf832_raw_path", MockSecret.for_endpoint("alcf832_raw")
            ),
            "alcf832_scratch": MockEndpoint(
                "mock_alcf832_scratch_path", MockSecret.for_endpoint("alcf832_scratch")
            ),
            "beegfs_raw": MockEndpoint(
                "mock_beegfs_raw_path", MockSecret.for_endpoint("beegfs_raw")
            ),
            "beegfs_scratch": MockEndpoint(
                "mock_beegfs_scratch_path", MockSecret.for_endpoint("beegfs_scratch")
            ),
        }
        self.tc = MockTransferClient()
        self.spot832 = self.endpoints["spot832"]
        self.data832 = self.endpoints["data832"]
        self.nersc832 = self.endpoints["nersc832"]
        self.alcf832_raw = self.endpoints["alcf832_raw"]
        self.alcf832_scratch = self.endpoints["alcf832_scratch"]
        self.beegfs_raw = self.endpoints["beegfs_raw"]
        self.beegfs_scratch = self.endpoints["beegfs_scratch"]
        self.scicat = "mock_scicat_value"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# Deployment names the dispatcher launches when not in IEC mode
DOWNSTREAM_DEPLOYMENTS = {
    "alcf_recon_flow/alcf_recon_flow",
    "nersc_recon_flow/nersc_recon_flow",
    "nersc_petiole_segment_flow/nersc_petiole_segment_flow",
    "nersc_moon_segment_flow/nersc_moon_segment_flow",
}


def _setup_dispatcher_mocks(mocker: MockFixture):
    """Patch external dependencies of the dispatcher flow.

    Returns:
        Tuple of (mock_run_deployment, mock_process_new_832_file_task)
        for tests to assert against.
    """
    mocker.patch("prefect.blocks.system.Secret.load", return_value=MockSecret())

    mock_process = mocker.patch(
        "orchestration.flows.bl832.dispatcher.process_new_832_file_task",
        return_value=None,
    )

    mock_run_deployment = mocker.patch(
        "orchestration.flows.bl832.dispatcher.run_deployment",
        new=mocker.AsyncMock(return_value=None),
    )

    return mock_run_deployment, mock_process


def _called_deployment_names(mock_run_deployment) -> set:
    """Extract the set of deployment names that run_deployment was called with."""
    return {
        call.kwargs.get("name") or (call.args[0] if call.args else None)
        for call in mock_run_deployment.call_args_list
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_dispatcher_happy_path(mocker: MockFixture):
    """Non-IEC param, Variable=False: process runs, all 4 downstream deployments launch."""
    mock_run_deployment, mock_process = _setup_dispatcher_mocks(mocker)

    from orchestration.flows.bl832.dispatcher import dispatcher

    result = asyncio.run(
        dispatcher(
            file_path="/global/raw/transfer_tests/test.txt",
            is_export_control=False,
            config=MockConfig832(),
        )
    )

    assert result is None
    mock_process.assert_called_once()
    assert mock_process.call_args.kwargs["is_export_control"] is False
    assert _called_deployment_names(mock_run_deployment) == DOWNSTREAM_DEPLOYMENTS


def test_dispatcher_iec_param_true_skips_downstream(mocker: MockFixture):
    """IEC param=True, Variable=False: process runs with IEC=True, no downstream deployments."""
    mock_run_deployment, mock_process = _setup_dispatcher_mocks(mocker)

    from orchestration.flows.bl832.dispatcher import dispatcher

    result = asyncio.run(
        dispatcher(
            file_path="/global/raw/transfer_tests/test.txt",
            is_export_control=True,
            config=MockConfig832(),
        )
    )

    assert result is None
    mock_process.assert_called_once()
    assert mock_process.call_args.kwargs["is_export_control"] is True
    mock_run_deployment.assert_not_called()


def test_dispatcher_iec_variable_overrides(mocker: MockFixture):
    """IEC param=False, Variable=True: OR-merge forces IEC mode, no downstream deployments."""
    Variable.set(name="is_export_control", value=True, overwrite=True, _sync=True)

    mock_run_deployment, mock_process = _setup_dispatcher_mocks(mocker)

    from orchestration.flows.bl832.dispatcher import dispatcher

    result = asyncio.run(
        dispatcher(
            file_path="/global/raw/transfer_tests/test.txt",
            is_export_control=False,
            config=MockConfig832(),
        )
    )

    assert result is None
    mock_process.assert_called_once()
    assert mock_process.call_args.kwargs["is_export_control"] is True
    mock_run_deployment.assert_not_called()


def test_dispatcher_iec_variable_missing_defaults_safe(mocker: MockFixture):
    """is_export_control Variable absent: Variable.get's default=False keeps normal flow."""
    real_variable_get = Variable.get

    def selective_get(name, default=None, *args, **kwargs):
        if name == "is_export_control":
            # Simulate the variable not existing: return the default
            return default
        # Pass other variables through to the real implementation
        return real_variable_get(name, *args, **kwargs)

    mocker.patch(
        "orchestration.flows.bl832.dispatcher.Variable.get",
        side_effect=selective_get,
    )

    mock_run_deployment, mock_process = _setup_dispatcher_mocks(mocker)

    from orchestration.flows.bl832.dispatcher import dispatcher

    result = asyncio.run(
        dispatcher(
            file_path="/global/raw/transfer_tests/test.txt",
            is_export_control=False,
            config=MockConfig832(),
        )
    )

    assert result is None
    mock_process.assert_called_once()
    assert mock_process.call_args.kwargs["is_export_control"] is False
    assert _called_deployment_names(mock_run_deployment) == DOWNSTREAM_DEPLOYMENTS


def test_dispatcher_respects_decision_settings(mocker: MockFixture):
    """Only deployments enabled in decision-settings should launch.

    Verifies the per-deployment `if decision_settings.get(...):` branches in dispatcher.
    """
    Variable.set(
        name="decision-settings",
        value={
            "alcf_recon_flow/alcf_recon_flow": True,
            "nersc_recon_flow/nersc_recon_flow": False,
            "nersc_petiole_segment_flow/nersc_petiole_segment_flow": False,
            "nersc_moon_segment_flow/nersc_moon_segment_flow": True,
            "new_832_file_flow/new_file_832": True,
        },
        overwrite=True,
        _sync=True,
    )

    mock_run_deployment, mock_process = _setup_dispatcher_mocks(mocker)

    from orchestration.flows.bl832.dispatcher import dispatcher

    asyncio.run(
        dispatcher(
            file_path="/global/raw/transfer_tests/test.txt",
            is_export_control=False,
            config=MockConfig832(),
        )
    )

    assert _called_deployment_names(mock_run_deployment) == {
        "alcf_recon_flow/alcf_recon_flow",
        "nersc_moon_segment_flow/nersc_moon_segment_flow",
    }
    mock_process.assert_called_once()


def test_dispatcher_raises_when_process_task_fails(mocker: MockFixture):
    """If process_new_832_file_task raises, dispatcher wraps it in ValueError.

    Also verifies downstream deployments are NOT launched when the upstream
    move task fails — confirming the synchronous-first ordering matters.
    """
    mock_run_deployment, mock_process = _setup_dispatcher_mocks(mocker)
    mock_process.side_effect = RuntimeError("disk full")

    from orchestration.flows.bl832.dispatcher import dispatcher

    with pytest.raises(ValueError, match="new_file_832 task Failed"):
        asyncio.run(
            dispatcher(
                file_path="/global/raw/transfer_tests/test.txt",
                is_export_control=False,
                config=MockConfig832(),
            )
        )

    mock_run_deployment.assert_not_called()
