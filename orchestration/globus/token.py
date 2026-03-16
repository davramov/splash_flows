import json
import logging
import os
from pathlib import Path
import stat
import time

import globus_sdk
from globus_sdk.exc import GlobusAPIError

logger = logging.getLogger(__name__)

# Default token file location, matching the Globus SDK convention.
DEFAULT_TOKEN_FILE: Path = Path.home() / ".globus" / "auth_tokens.json"
GLOBUS_OIDC_TOKEN_URL: str = "https://auth.globus.org/v2/oauth2/token"


def get_access_token_confidential(
    client_id: str,
    client_secret: str,
    required_scopes: frozenset[str],
    resource_server: str,
    token_file: Path | None = None,
) -> str:
    """Get a valid Globus access token using a Confidential Client (machine-to-machine).

    No browser or user interaction required. If a valid unexpired token exists
    on disk it is reused; otherwise a new one is minted via the client
    credentials grant and saved.

    Args:
        client_id: Globus Confidential App client ID.
        client_secret: Globus Confidential App client secret.
        required_scopes: Set of OAuth2 scopes that must be present on the token.
        resource_server: Resource server key to extract from the token response.
        token_file: Path to the JSON token cache file. Defaults to
            ``~/.globus/auth_tokens.json``.

    Returns:
        A valid Globus access token string.

    Raises:
        RuntimeError: If the acquired token is missing required scopes.
        KeyError: If ``access_token`` is absent from the token response.
    """
    resolved_token_file = token_file or DEFAULT_TOKEN_FILE

    # 1. Do we already have a valid token?
    stored = load_token_file(resolved_token_file)
    if stored:
        expires_at = stored.get("expires_at_seconds")
        if expires_at and time.time() < expires_at:
            logger.info("Using cached Globus token (still valid).")
            return stored["access_token"]
        logger.info("Cached Globus token is expired; minting a new one.")
    else:
        logger.info("No cached Globus token found; minting a new one.")

    # 2. Mint a new token — same call whether first time or expired.
    globus_client = globus_sdk.ConfidentialAppAuthClient(client_id, client_secret)
    token_response = globus_client.oauth2_client_credentials_tokens(
        requested_scopes=" ".join(sorted(required_scopes))
    )
    auth_data = token_response.by_resource_server[resource_server]

    granted = set(auth_data.get("scope", "").split())
    missing = required_scopes - granted
    if missing:
        raise RuntimeError(
            f"Globus token is missing required scopes: {sorted(missing)}"
        )

    save_token_file(resolved_token_file, auth_data)
    logger.info(f"New Globus token saved to {resolved_token_file}.")

    return auth_data["access_token"]


def load_token_file(token_file: Path) -> dict | None:
    """Load saved Globus token data from disk.

    Args:
        token_file: Path to the JSON token file.

    Returns:
        Parsed token dict, or None if the file does not exist.
    """
    if not token_file.exists():
        return None
    with token_file.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_token_file(token_file: Path, tokens: dict) -> None:
    """Atomically save Globus token data to disk with owner-only permissions.

    Writes to a temporary file then renames to avoid partial writes.

    Args:
        token_file: Destination path for the JSON token file.
        tokens: Token dict to serialise.
    """
    _ensure_private_parent_dir(token_file)
    tmp = token_file.with_suffix(".tmp")
    with os.fdopen(
        os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600),
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(tokens, f, indent=2)
    os.replace(tmp, token_file)
    os.chmod(token_file, stat.S_IRUSR | stat.S_IWUSR)


def interactive_login(
    client: globus_sdk.NativeAppAuthClient,
    required_scopes: frozenset[str],
    resource_server: str,
) -> dict:
    """Run an interactive browser-based Globus login flow.

    Prints an authorization URL, waits for the user to paste an auth code,
    and exchanges it for tokens.

    Args:
        client: Globus NativeAppAuthClient to drive the flow.
        required_scopes: Set of OAuth2 scopes to request.
        resource_server: Resource server key to extract from the token response
            (e.g. ``"auth.globus.org"``).

    Returns:
        Token dict for the given resource server.
    """
    client.oauth2_start_flow(
        requested_scopes=" ".join(sorted(required_scopes)),
        refresh_tokens=True,
    )
    logger.info("Open this URL in your browser to authenticate with Globus:")
    logger.info(client.oauth2_get_authorize_url())
    code = input("\nEnter authorization code: ").strip()
    token_response = client.oauth2_exchange_code_for_tokens(code)
    return token_response.by_resource_server[resource_server]


def refresh_tokens(
    client: globus_sdk.NativeAppAuthClient,
    refresh_token: str,
    resource_server: str,
) -> dict | None:
    """Attempt a silent Globus token refresh.

    Args:
        client: Globus NativeAppAuthClient to drive the refresh.
        refresh_token: The stored refresh token.
        resource_server: Resource server key to extract from the token response.

    Returns:
        Fresh token dict for the given resource server, or None if refresh failed.
    """
    try:
        token_response = client.oauth2_refresh_token(refresh_token)
        return token_response.by_resource_server[resource_server]
    except GlobusAPIError as e:
        logger.warning(
            f"Globus token refresh failed ({e.http_status}); "
            "falling back to interactive login."
        )
        return None


def get_access_token(
    client_id: str,
    required_scopes: frozenset[str],
    resource_server: str,
    token_file: Path | None = None,
    force_login: bool = False,
) -> str:
    """Get a valid Globus access token, refreshing or logging in as needed.

    Attempts a silent refresh from the saved token file first. Falls back to
    interactive browser login if no saved tokens exist, the refresh token is
    absent, or the refresh fails. Saves the resulting tokens back to disk.

    Args:
        client_id: Globus NativeApp client ID.
        required_scopes: Set of OAuth2 scopes that must be present on the token.
        resource_server: Resource server key to extract from the token response.
        token_file: Path to the JSON token file. Defaults to
            ``~/.globus/auth_tokens.json``.
        force_login: If True, skip refresh and force interactive login.

    Returns:
        A valid Globus access token string.

    Raises:
        RuntimeError: If the acquired token is missing required scopes.
        KeyError: If ``access_token`` is absent from the token response.
    """
    resolved_token_file = token_file or DEFAULT_TOKEN_FILE
    globus_client = globus_sdk.NativeAppAuthClient(client_id)

    auth_data: dict | None = None

    if not force_login:
        stored = load_token_file(resolved_token_file)
        if stored and stored.get("refresh_token"):
            auth_data = refresh_tokens(
                globus_client, stored["refresh_token"], resource_server
            )

    if auth_data is None:
        logger.info("Initiating interactive Globus login.")
        auth_data = interactive_login(globus_client, required_scopes, resource_server)

    granted = set(auth_data.get("scope", "").split())
    missing = required_scopes - granted
    if missing:
        raise RuntimeError(
            f"Globus token is missing required scopes: {sorted(missing)}"
        )

    save_token_file(resolved_token_file, auth_data)
    logger.info(f"Globus token saved to {resolved_token_file}.")

    return auth_data["access_token"]


def _ensure_private_parent_dir(path: Path) -> None:
    """Create parent directories for path with owner-only permissions.

    Args:
        path: The file path whose parent directory should be created.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
