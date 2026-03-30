# orchestration/globus/token.py
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

# IRI API Globus scope and resource server.
# The IRI access token lives in other_tokens under this scope, not at the
# top level of the auth.globus.org response.
IRI_SCOPE: str = (
    "https://auth.globus.org/scopes/"
    "ed3e577d-f7f3-4639-b96e-ff5a8445d699/iri_api"
)
IRI_RESOURCE_SERVER: str = "ed3e577d-f7f3-4639-b96e-ff5a8445d699"


# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------

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


def _ensure_private_parent_dir(path: Path) -> None:
    """Create parent directories for path with owner-only permissions.

    Args:
        path: The file path whose parent directory should be created.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)


# ---------------------------------------------------------------------------
# IRI token helpers
# ---------------------------------------------------------------------------

def _parse_scope_string(scope_string: str) -> set[str]:
    """Split a space-separated scope string into a set.

    Args:
        scope_string: Space-separated OAuth2 scope string.

    Returns:
        Set of individual scope strings.
    """
    return set(scope_string.split()) if scope_string else set()


def extract_iri_token(token_response_data: dict) -> dict:
    """Extract the IRI access token entry from a Globus token response.

    The IRI token is not returned at the top level — it lives inside
    ``other_tokens``, identified by :data:`IRI_SCOPE`.

    Args:
        token_response_data: Full token response dict as returned by the
            Globus SDK (i.e. ``token_response.data``).

    Returns:
        Token dict for the IRI resource server.

    Raises:
        RuntimeError: If no token matching the IRI scope is found.
    """
    for token_data in token_response_data.get("other_tokens", []):
        if IRI_SCOPE in _parse_scope_string(token_data.get("scope", "")):
            return token_data
    raise RuntimeError(
        f"Missing token for required IRI scope: {IRI_SCOPE}. "
        "Re-run with --force-login and ensure consent is granted for the IRI scope."
    )


def _replace_iri_token(token_response_data: dict, iri_token_data: dict) -> dict:
    """Return a copy of token_response_data with the IRI entry replaced.

    Args:
        token_response_data: Full stored token response dict.
        iri_token_data: Updated IRI token dict to splice in.

    Returns:
        Updated token response dict.
    """
    merged = dict(token_response_data)
    other_tokens = list(merged.get("other_tokens", []))
    for i, token_data in enumerate(other_tokens):
        if IRI_SCOPE in _parse_scope_string(token_data.get("scope", "")):
            other_tokens[i] = iri_token_data
            break
    else:
        other_tokens.append(iri_token_data)
    merged["other_tokens"] = other_tokens
    return merged


def _get_iri_refresh_token(stored_tokens: dict) -> str | None:
    """Extract the IRI refresh token from stored token data, if present.

    Args:
        stored_tokens: Full stored token response dict.

    Returns:
        The IRI refresh token string, or None if absent.
    """
    try:
        return extract_iri_token(stored_tokens).get("refresh_token")
    except RuntimeError:
        return None


def _get_auth_refresh_token(stored_tokens: dict) -> str | None:
    """Extract the top-level Globus Auth refresh token from stored data.

    Args:
        stored_tokens: Full stored token response dict.

    Returns:
        The auth refresh token string, or None if absent.
    """
    if "refresh_token" in stored_tokens:
        return stored_tokens["refresh_token"]
    auth_tokens = stored_tokens.get("auth.globus.org")
    if isinstance(auth_tokens, dict):
        return auth_tokens.get("refresh_token")
    return None


# ---------------------------------------------------------------------------
# NativeApp flow (interactive)
# ---------------------------------------------------------------------------

def interactive_login(
    client: globus_sdk.NativeAppAuthClient,
    requested_scopes: frozenset[str],
    prompt_login: bool = False,
) -> dict:
    """Run an interactive browser-based Globus login flow.

    Prints an authorization URL, waits for the user to paste an auth code,
    and returns the full token response data including ``other_tokens``.

    Args:
        client: Globus NativeAppAuthClient to drive the flow.
        requested_scopes: Set of OAuth2 scopes to request. Should include
            :data:`IRI_SCOPE` to obtain an IRI API token.
        prompt_login: If True, add ``prompt=login`` to the authorize URL to
            force a fresh identity-provider login.

    Returns:
        Full token response dict (``token_response.data``), including
        ``other_tokens``.

    Raises:
        RuntimeError: If no authorization code is entered, or if the code
            exchange fails.
    """
    client.oauth2_start_flow(
        requested_scopes=" ".join(sorted(requested_scopes)),
        refresh_tokens=True,
    )
    logger.info("Open this URL in your browser to authenticate with Globus:")
    prompt = "login" if prompt_login else globus_sdk.MISSING
    logger.info(client.oauth2_get_authorize_url(prompt=prompt))
    code = input("\nEnter authorization code: ").strip()
    if not code:
        raise RuntimeError(
            "No authorization code entered. Re-run the script and paste the "
            "code shown by Globus after login."
        )
    try:
        token_response = client.oauth2_exchange_code_for_tokens(code)
    except GlobusAPIError as e:
        if e.http_status == 400:
            raise RuntimeError(
                "Authorization code exchange failed — the code was empty, "
                "invalid, expired, or already used. Re-run and try again."
            ) from e
        raise RuntimeError(
            f"Authorization code exchange failed with HTTP {e.http_status}."
        ) from e
    return token_response.data


def _refresh_single_token(
    client: globus_sdk.NativeAppAuthClient,
    refresh_token: str,
) -> dict | None:
    """Attempt a single Globus token refresh, returning raw response data.

    Args:
        client: NativeAppAuthClient to drive the refresh.
        refresh_token: The stored refresh token.

    Returns:
        Raw token response data dict, or None if the refresh failed.
    """
    try:
        token_response = client.oauth2_refresh_token(refresh_token)
        return token_response.data
    except GlobusAPIError as e:
        logger.warning(
            f"Globus token refresh failed ({e.http_status}); "
            "will fall back to interactive login."
        )
        return None


def _refresh_stored_tokens(
    client: globus_sdk.NativeAppAuthClient,
    stored_tokens: dict,
) -> tuple[dict | None, bool]:
    """Try to refresh stored tokens, preferring the IRI refresh token.

    Attempts the IRI-specific refresh token first, then falls back to the
    top-level Globus Auth refresh token.

    Args:
        client: NativeAppAuthClient to drive the refresh.
        stored_tokens: Full stored token response dict.

    Returns:
        Tuple of ``(updated_token_data, success)``. On failure both values
        are ``(None, False)``.
    """
    iri_refresh = _get_iri_refresh_token(stored_tokens)
    if iri_refresh:
        iri_token_data = _refresh_single_token(client, iri_refresh)
        if iri_token_data is not None:
            return _replace_iri_token(stored_tokens, iri_token_data), True

    auth_refresh = _get_auth_refresh_token(stored_tokens)
    if auth_refresh:
        auth_data = _refresh_single_token(client, auth_refresh)
        if auth_data is not None:
            return auth_data, True

    return None, False


def get_access_token(
    client_id: str,
    requested_scopes: frozenset[str],
    token_file: Path | None = None,
    force_login: bool = False,
    prompt_login: bool = False,
) -> str:
    """Get a valid IRI API access token via the NativeApp interactive flow.

    Attempts a silent refresh from the saved token file first. Falls back to
    interactive browser login if no saved tokens exist, the refresh token is
    absent, or the refresh fails. Saves the resulting tokens back to disk.

    The IRI token is extracted from ``other_tokens`` in the response — it is
    not the top-level Globus Auth token.

    Args:
        client_id: Globus NativeApp client ID.
        requested_scopes: Set of OAuth2 scopes to request. Must include
            :data:`IRI_SCOPE` to obtain a usable IRI API token.
        token_file: Path to the JSON token file. Defaults to
            ``~/.globus/auth_tokens.json``.
        force_login: If True, skip refresh and force interactive login.
        prompt_login: If True, add ``prompt=login`` to the authorize URL.

    Returns:
        A valid IRI API access token string.

    Raises:
        RuntimeError: If the IRI scope token is missing from the response.
    """
    resolved_token_file = token_file or DEFAULT_TOKEN_FILE
    globus_client = globus_sdk.NativeAppAuthClient(client_id)

    token_response_data: dict | None = None
    used_refresh = False

    if not force_login:
        stored = load_token_file(resolved_token_file)
        if stored:
            token_response_data, used_refresh = _refresh_stored_tokens(
                globus_client, stored
            )

    if token_response_data is None:
        logger.info("Initiating interactive Globus login.")
        token_response_data = interactive_login(
            globus_client, requested_scopes, prompt_login=prompt_login
        )

    # Extract IRI token — if a refresh ran but didn't return the IRI token,
    # fall back to interactive login before raising.
    try:
        iri_token = extract_iri_token(token_response_data)
    except RuntimeError:
        if used_refresh:
            logger.warning(
                "Refreshed tokens did not include the IRI token; "
                "falling back to interactive login."
            )
            token_response_data = interactive_login(
                globus_client, requested_scopes, prompt_login=prompt_login
            )
            iri_token = extract_iri_token(token_response_data)
        else:
            raise

    save_token_file(resolved_token_file, token_response_data)
    logger.info(f"Globus token saved to {resolved_token_file}.")

    return iri_token["access_token"]


# ---------------------------------------------------------------------------
# Confidential Client flow (machine-to-machine)
# ---------------------------------------------------------------------------

def get_access_token_confidential(
    client_id: str,
    client_secret: str,
    required_scopes: frozenset[str],
    resource_server: str,
    token_file: Path | None = None,
) -> str:
    """Get a valid Globus access token using a Confidential Client.

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

    stored = load_token_file(resolved_token_file)
    if stored:
        expires_at = stored.get("expires_at_seconds")
        if expires_at and time.time() < expires_at:
            logger.info("Using cached Globus token (still valid).")
            return stored["access_token"]
        logger.info("Cached Globus token is expired; minting a new one.")
    else:
        logger.info("No cached Globus token found; minting a new one.")

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
