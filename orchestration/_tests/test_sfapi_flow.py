# orchestration/_tests/test_sfapi_flow.py
import pytest
from uuid import uuid4

from prefect.blocks.system import Secret
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        Secret(value=str(uuid4())).save(name="globus-client-id", overwrite=True)
        Secret(value=str(uuid4())).save(name="globus-client-secret", overwrite=True)
        yield


def test_create_sfapi_client_success(mocker):
    """Valid credentials produce a Client instance."""
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

    mocker.patch("orchestration.flows.bl832.nersc.os.getenv", side_effect=lambda x: {
        "PATH_NERSC_CLIENT_ID": "/path/to/client_id",
        "PATH_NERSC_PRI_KEY": "/path/to/client_secret",
    }.get(x))
    mocker.patch("orchestration.flows.bl832.nersc.os.path.isfile", return_value=True)
    mocker.patch(
        "builtins.open",
        side_effect=[
            mocker.mock_open(read_data="my-client-id")(),
            mocker.mock_open(read_data='{"kty": "RSA", "n": "x", "e": "y"}')(),
        ]
    )
    mocker.patch("orchestration.flows.bl832.nersc.JsonWebKey.import_key", return_value="mock_secret")
    mock_client_cls = mocker.patch("orchestration.flows.bl832.nersc.Client")

    client = NERSCTomographyHPCController._create_sfapi_client()

    mock_client_cls.assert_called_once_with("my-client-id", "mock_secret")
    assert client is mock_client_cls.return_value


def test_create_sfapi_client_missing_paths(mocker):
    """Unset env vars raise ValueError."""
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

    mocker.patch("orchestration.flows.bl832.nersc.os.getenv", return_value=None)

    with pytest.raises(ValueError, match="Missing NERSC credentials paths."):
        NERSCTomographyHPCController._create_sfapi_client()


def test_create_sfapi_client_missing_files(mocker):
    """Env vars set but files absent raise FileNotFoundError."""
    from orchestration.flows.bl832.nersc import NERSCTomographyHPCController

    mocker.patch("orchestration.flows.bl832.nersc.os.getenv", side_effect=lambda x: {
        "PATH_NERSC_CLIENT_ID": "/path/to/client_id",
        "PATH_NERSC_PRI_KEY": "/path/to/client_secret",
    }.get(x))
    mocker.patch("orchestration.flows.bl832.nersc.os.path.isfile", return_value=False)

    with pytest.raises(FileNotFoundError, match="NERSC credential files are missing."):
        NERSCTomographyHPCController._create_sfapi_client()
