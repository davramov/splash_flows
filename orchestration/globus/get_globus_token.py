#!/usr/bin/env python3
import argparse
import json
import os
import stat
import time
import urllib.error
import urllib.request
from pathlib import Path

import globus_sdk
from globus_sdk.exc import GlobusAPIError

DEFAULT_TOKEN_FILE: Path = Path.home() / ".globus" / "auth_tokens.json"
CLIENT_ID = "fae5c579-490a-4d76-b6eb-d78f65caeb63"
RESOURCE_SERVER = "auth.globus.org"
IRI_SCOPE = (
    "https://auth.globus.org/scopes/"
    "ed3e577d-f7f3-4639-b96e-ff5a8445d699/iri_api"
)
REQUIRED_SCOPES = {
    "openid",
    "profile",
    "email",
    "urn:globus:auth:scope:auth.globus.org:view_identities",
}
REQUESTED_SCOPES = REQUIRED_SCOPES | {IRI_SCOPE}
DEFAULT_IRI_VALIDATE_URL = "https://api.iri.nersc.gov/api/v1/account/projects"


def parse_args() -> argparse.Namespace:
    default_token_file = Path.home() / ".globus" / "auth_tokens.json"
    parser = argparse.ArgumentParser(
        description=(
            "Get Globus Auth tokens with required scopes. "
            "Tokens are saved to a secure local file by default."
        )
    )
    parser.add_argument(
        "--token-file",
        type=Path,
        default=default_token_file,
        help=f"Path for saved token JSON (default: {default_token_file})",
    )
    parser.add_argument(
        "--print-token",
        action="store_true",
        help="Print the access token to stdout (off by default).",
    )
    parser.add_argument(
        "--force-login",
        action="store_true",
        help="Skip refresh and force interactive browser login.",
    )
    parser.add_argument(
        "--refresh-only",
        action="store_true",
        help="Refresh saved tokens only; do not fall back to interactive login.",
    )
    parser.add_argument(
        "--prompt-login",
        action="store_true",
        help="Add prompt=login to the Globus authorize URL to force re-authentication.",
    )
    parser.add_argument(
        "--validate-iri",
        action="store_true",
        help="Validate the IRI token by calling the IRI account/projects endpoint.",
    )
    parser.add_argument(
        "--iri-validate-url",
        default=DEFAULT_IRI_VALIDATE_URL,
        help=(
            "IRI endpoint used by --validate-iri "
            f"(default: {DEFAULT_IRI_VALIDATE_URL})"
        ),
    )
    return parser.parse_args()


def parse_scope_string(scope_string: str) -> set[str]:
    return set(scope_string.split()) if scope_string else set()


def ensure_private_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)


def load_tokens(token_file: Path) -> dict | None:
    if not token_file.exists():
        return None
    with token_file.open("r", encoding="utf-8") as f:
        return json.load(f)


# def save_tokens(token_file: Path, tokens: dict) -> None:
#     ensure_private_parent_dir(token_file)
#     tmp = token_file.with_suffix(".tmp")
#     with os.fdopen(
#         os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600),
#         "w",
#         encoding="utf-8",
#     ) as f:
#         json.dump(tokens, f, indent=2)
#     os.replace(tmp, token_file)
#     os.chmod(token_file, stat.S_IRUSR | stat.S_IWUSR)


def save_tokens(token_file: Path, tokens: dict) -> None:
    ensure_private_parent_dir(token_file)
    # Per-process unique tmp name to avoid races between concurrent writers
    tmp = token_file.with_suffix(f".tmp.{os.getpid()}.{os.urandom(4).hex()}")
    try:
        with os.fdopen(
            os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600),
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(tokens, f, indent=2)
        os.replace(tmp, token_file)
        os.chmod(token_file, stat.S_IRUSR | stat.S_IWUSR)
    except Exception:
        # Clean up tmp if anything between open and replace failed
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def get_refresh_token(stored_tokens: dict) -> str | None:
    if "refresh_token" in stored_tokens:
        return stored_tokens.get("refresh_token")

    auth_tokens = stored_tokens.get(RESOURCE_SERVER)
    if isinstance(auth_tokens, dict):
        return auth_tokens.get("refresh_token")

    return None


def get_iri_token(token_response_data: dict) -> dict:
    for token_data in token_response_data.get("other_tokens", []):
        if IRI_SCOPE in parse_scope_string(token_data.get("scope", "")):
            return token_data
    raise RuntimeError(f"Missing token for required IRI scope: {IRI_SCOPE}")


def get_iri_refresh_token(stored_tokens: dict) -> str | None:
    try:
        return get_iri_token(stored_tokens).get("refresh_token")
    except RuntimeError:
        return None


def replace_iri_token(token_response_data: dict, iri_token_data: dict) -> dict:
    merged = dict(token_response_data)
    other_tokens = list(merged.get("other_tokens", []))
    for index, token_data in enumerate(other_tokens):
        if IRI_SCOPE in parse_scope_string(token_data.get("scope", "")):
            other_tokens[index] = iri_token_data
            break
    else:
        other_tokens.append(iri_token_data)
    merged["other_tokens"] = other_tokens
    return merged


def validate_auth_data(auth_data: dict) -> dict:
    if auth_data.get("resource_server") != RESOURCE_SERVER:
        raise RuntimeError(
            f"Missing token for required resource server: {RESOURCE_SERVER}"
        )

    granted = parse_scope_string(auth_data.get("scope", ""))
    missing = REQUIRED_SCOPES - granted
    if missing:
        raise RuntimeError(f"Missing required scopes: {sorted(missing)}")

    return get_iri_token(auth_data)


def validate_iri_token(iri_token_data: dict, validate_url: str) -> dict | list:
    request = urllib.request.Request(
        validate_url,
        headers={
            "accept": "application/json",
            "Authorization": f"Bearer {iri_token_data['access_token']}",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(request) as response:
            body = response.read().decode("utf-8")
            data = json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        details = body.strip() or exc.reason
        raise RuntimeError(
            f"IRI validation failed with HTTP {exc.code} from {validate_url}: {details}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"IRI validation request failed for {validate_url}: {exc.reason}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"IRI validation returned non-JSON data from {validate_url}"
        ) from exc

    if isinstance(data, dict):
        session_info = data.get("session_info")
        if isinstance(session_info, dict):
            authentications = session_info.get("authentications")
            if isinstance(authentications, dict) and not authentications:
                raise RuntimeError(
                    "IRI validation succeeded but session_info.authentications is empty. "
                    "Re-run with --force-login --prompt-login and use a Chrome incognito window."
                )

    return data


def interactive_login(
    client: globus_sdk.NativeAppAuthClient,
    *,
    prompt_login: bool = False,
) -> dict:
    client.oauth2_start_flow(
        requested_scopes=" ".join(sorted(REQUESTED_SCOPES)),
        refresh_tokens=True,
    )
    print("Open this URL, login, and consent:")
    prompt = "login" if prompt_login else globus_sdk.MISSING
    print(client.oauth2_get_authorize_url(prompt=prompt))
    code = input("\nEnter authorization code: ").strip()
    if not code:
        raise RuntimeError(
            "No authorization code entered. Re-run the script and paste the code "
            "shown by Globus after login."
        )
    try:
        token_response = client.oauth2_exchange_code_for_tokens(code)
    except GlobusAPIError as exc:
        if exc.http_status == 400:
            raise RuntimeError(
                "Authorization code exchange failed. The code was empty, invalid, "
                "expired, or already used. Re-run the script and complete the "
                "Globus login flow again."
            ) from exc
        raise RuntimeError(
            f"Authorization code exchange failed with HTTP {exc.http_status}. "
            "Re-run the script and try again."
        ) from exc
    return token_response.data


def refresh_tokens(
    client: globus_sdk.NativeAppAuthClient, refresh_token: str
) -> dict | None:
    try:
        token_response = client.oauth2_refresh_token(refresh_token)
        return token_response.data
    except GlobusAPIError as exc:
        print(
            f"Refresh failed ({exc.http_status}); switching to interactive login."
        )
        return None


def refresh_stored_tokens(
    client: globus_sdk.NativeAppAuthClient, stored_tokens: dict
) -> tuple[dict | None, bool]:
    iri_refresh_token = get_iri_refresh_token(stored_tokens)
    if iri_refresh_token:
        iri_token_data = refresh_tokens(client, iri_refresh_token)
        if iri_token_data is not None:
            return replace_iri_token(stored_tokens, iri_token_data), True

    auth_refresh_token = get_refresh_token(stored_tokens)
    if auth_refresh_token:
        auth_data = refresh_tokens(client, auth_refresh_token)
        if auth_data is not None:
            return auth_data, True

    return None, False


def get_iri_access_token(
    token_file: Path = DEFAULT_TOKEN_FILE,
    force_login: bool = False,
    prompt_login: bool = False,
) -> str:
    """
    Get a valid IRI access token, refreshing or prompting for login as needed.
    Tokens are saved to the specified token_file path (default: ~/.globus/auth_tokens.json).
    By default, the function will attempt to refresh saved tokens before falling back
    to interactive login. Use force_login=True to skip refresh and require interactive login.
    Use prompt_login=True to add prompt=login to the authorization URL, which forces
    re-authentication even if the user has an active Globus session in their browser.

    Args:
        token_file: Path to save and load token data (default: ~/.globus/auth_tokens.json)
        force_login: If True, skip token refresh and require interactive login
        prompt_login: If True, add prompt=login to the authorization URL to force re-authentication

    Returns:
        A valid IRI access token string with the required scopes.

    Raises:
        RuntimeError: If token refresh fails and interactive login is not allowed or fails,
            or if the resulting tokens do not include a valid IRI access token.
    """
    client = globus_sdk.NativeAppAuthClient(CLIENT_ID)

    # Fast path: if token exists and is not expired, return it directly without refreshing or saving
    if not force_login:
        stored = load_tokens(token_file)
        if stored:
            try:
                iri_token = get_iri_token(stored)
                expires_at = iri_token.get("expires_at_seconds", 0)
                if expires_at and time.time() < expires_at - 60:  # 60s buffer
                    return iri_token["access_token"]
            except RuntimeError:
                pass  # fall through to refresh

    auth_data = None
    used_refresh = False
    if not force_login:
        stored = load_tokens(token_file)
        if stored:
            auth_data, used_refresh = refresh_stored_tokens(client, stored)
    if auth_data is None:
        auth_data = interactive_login(client, prompt_login=prompt_login)
    try:
        iri_token_data = validate_auth_data(auth_data)
    except RuntimeError as exc:
        if used_refresh and "Missing token for required IRI scope" in str(exc):
            auth_data = interactive_login(client, prompt_login=prompt_login)
            iri_token_data = validate_auth_data(auth_data)
        else:
            raise
    save_tokens(token_file, auth_data)
    return iri_token_data["access_token"]


def main() -> None:
    args = parse_args()
    if args.force_login and args.refresh_only:
        raise RuntimeError("Choose only one of --force-login or --refresh-only")

    client = globus_sdk.NativeAppAuthClient(CLIENT_ID)

    auth_data = None
    used_refresh = False
    if not args.force_login:
        stored = load_tokens(args.token_file)
        if stored:
            auth_data, used_refresh = refresh_stored_tokens(client, stored)

    if auth_data is None:
        if args.refresh_only:
            raise RuntimeError(
                "Refresh-only mode failed. No usable saved refresh token was found "
                "or token refresh did not return the required IRI token."
            )
        auth_data = interactive_login(client, prompt_login=args.prompt_login)

    try:
        iri_token_data = validate_auth_data(auth_data)
    except RuntimeError as exc:
        if used_refresh and "Missing token for required IRI scope" in str(exc):
            print(
                "Refreshed tokens did not include the IRI token; "
                "switching to interactive login."
            )
            auth_data = interactive_login(client, prompt_login=args.prompt_login)
            iri_token_data = validate_auth_data(auth_data)
        else:
            raise

    save_tokens(args.token_file, auth_data)

    if args.validate_iri:
        validation_data = validate_iri_token(iri_token_data, args.iri_validate_url)
        print(f"IRI validation succeeded against {args.iri_validate_url}")
        if isinstance(validation_data, dict):
            session_info = validation_data.get("session_info")
            if isinstance(session_info, dict):
                session_id = session_info.get("session_id")
                if session_id:
                    print(f"IRI session_id: {session_id}")
        elif isinstance(validation_data, list):
            print(f"IRI validation response items: {len(validation_data)}")

    expires_at = iri_token_data.get("expires_at_seconds")
    if expires_at:
        ttl = int(expires_at - time.time())
        print(f"\nIRI access token valid for ~{max(ttl, 0)} seconds.")

    print(f"Saved token data to {args.token_file}")
    print(f"Granted Globus Auth scopes: {auth_data.get('scope', '')}")
    print(f"IRI token resource server: {iri_token_data.get('resource_server')}")
    print(f"IRI token scopes: {iri_token_data.get('scope', '')}")

    if args.print_token:
        print("\nIRI access token:")
        print(iri_token_data["access_token"])
    else:
        print(
            "IRI access token not printed "
            "(use --print-token to display it for the NERSC IRI API)."
        )


if __name__ == "__main__":
    main()
