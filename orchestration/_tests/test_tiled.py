"""Unit tests for ``orchestration.tiled``.

Tests run inside a Prefect ephemeral server (``prefect_test_harness``) so the
real task engine is exercised, but Tiled itself is mocked — no real Tiled
server, no network calls.

Coverage:
  - ``register_file_to_tiled``: h5/zarr branch, TIFF directory branch,
    no-tags skip, ``str`` path coercion, missing-entry warning, register failure.
  - ``_apply_tags``: existing access_blob (replace), no blob (add), tag
    deduplication, ``patch_metadata`` failure swallowed.
  - ``check_tags``: h5 ok, h5 tag-missing, TIFF dir uses first child,
    no access_blob, missing entry raises ``KeyError``.
  - ``register_to_tiled`` flow: delegates with the right args, coerces ``str``.

The async/sync bridge inside ``register_file_to_tiled`` (``run_coro_as_sync``
calling Tiled's async ``register``) is exercised on every happy path: tests
patch ``register`` as an ``AsyncMock`` and let ``run_coro_as_sync`` actually
drive it. The error-path test patches ``run_coro_as_sync`` directly to inject
a failure.
"""
import warnings

from prefect.testing.utilities import prefect_test_harness
import pytest
from pytest_mock import MockFixture

from orchestration.tiled import (
    _apply_tags,
    check_tags,
    register_file_to_tiled,
    register_to_tiled,
)

warnings.filterwarnings("ignore", category=DeprecationWarning)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    """Provide an ephemeral Prefect server for the whole test session."""
    with prefect_test_harness():
        yield


@pytest.fixture(autouse=True)
def _tiled_uri_env(monkeypatch):
    """Set ``TILED_URI`` so ``os.environ[...]`` reads in the module don't error."""
    monkeypatch.setenv("TILED_URI", "http://tiled.test")


# ---------------------------------------------------------------------------
# Mock Tiled node
# ---------------------------------------------------------------------------

class MockNode:
    """Stand-in for a Tiled node.

    Strict by design: ``node[missing_key]`` raises ``KeyError`` to match real
    Tiled behavior, so tests for the missing-entry warning path actually work.
    Build prefix chains explicitly with ``add_child`` or the
    ``build_prefix_chain`` helper below.
    """

    def __init__(self, access_blob=None, uri="http://tiled.test/node"):
        self.access_blob = access_blob
        self.uri = uri
        self._children: dict[str, "MockNode"] = {}
        self.patch_calls: list[dict] = []

    def add_child(self, key: str, node: "MockNode") -> "MockNode":
        self._children[key] = node
        return node

    def __getitem__(self, key):
        # Strict: missing keys raise like a real Tiled node would
        return self._children[key]

    def __iter__(self):
        return iter(self._children)

    def patch_metadata(self, **kwargs):
        self.patch_calls.append(kwargs)


def build_prefix_chain(segments: list[str], leaf: MockNode) -> MockNode:
    """Build ``client[seg1][seg2]...[segN] -> leaf`` and return the root.

    Used to mirror the prefix-navigation loop in ``register_file_to_tiled``
    and ``check_tags`` (``for segment in prefix.strip("/").split("/")``).
    """
    current = leaf
    for segment in reversed(segments):
        parent = MockNode()
        parent.add_child(segment, current)
        current = parent
    return current


# ---------------------------------------------------------------------------
# _apply_tags
# ---------------------------------------------------------------------------

def test_apply_tags_uses_add_op_when_no_existing_blob():
    """No access_blob present → JSON Patch op is ``add``."""
    node = MockNode(access_blob=None)
    _apply_tags(entry_node=node, tags=["bl832"])

    assert len(node.patch_calls) == 1
    patch = node.patch_calls[0]["access_blob_patch"][0]
    assert patch["op"] == "add"
    assert patch["path"] == ""
    assert set(patch["value"]["tags"]) == {"bl832"}


def test_apply_tags_uses_replace_op_when_blob_exists():
    """Existing access_blob → op is ``replace`` and tags are merged."""
    node = MockNode(access_blob={"tags": ["existing"]})
    _apply_tags(entry_node=node, tags=["bl832"])

    patch = node.patch_calls[0]["access_blob_patch"][0]
    assert patch["op"] == "replace"
    assert set(patch["value"]["tags"]) == {"existing", "bl832"}


def test_apply_tags_deduplicates_overlapping_tags():
    """Tag merging is a set union, so overlap doesn't produce duplicates."""
    node = MockNode(access_blob={"tags": ["bl832", "old"]})
    _apply_tags(entry_node=node, tags=["bl832", "new"])

    patch = node.patch_calls[0]["access_blob_patch"][0]
    merged = patch["value"]["tags"]
    assert set(merged) == {"bl832", "old", "new"}
    assert len(merged) == 3  # no duplicates


def test_apply_tags_swallows_patch_metadata_failure(mocker: MockFixture):
    """If ``patch_metadata`` raises, ``_apply_tags`` logs but does not propagate."""
    node = MockNode(access_blob={"tags": []})
    mock_patch_metadata = mocker.patch.object(
        node, "patch_metadata", side_effect=RuntimeError("permission denied")
    )

    # Should not raise
    _apply_tags(entry_node=node, tags=["bl832"])

    # Verify the call actually happened — the test is about swallowing, not skipping
    mock_patch_metadata.assert_called_once()


# ---------------------------------------------------------------------------
# register_file_to_tiled — happy paths (exercise run_coro_as_sync bridge)
# ---------------------------------------------------------------------------

def test_register_h5_with_tags_applies_to_stem(mocker: MockFixture, tmp_path):
    """For an .h5 file, the entry keyed by ``path.stem`` should be tagged."""
    h5 = tmp_path / "scan.h5"
    h5.touch()

    entry_node = MockNode(access_blob=None)
    prefix_node = MockNode()
    prefix_node.add_child("scan", entry_node)
    client = build_prefix_chain(["beamlines", "bl832", "raw"], prefix_node)

    fake_register = mocker.AsyncMock(return_value=None)
    mocker.patch("orchestration.tiled.from_uri", return_value=client)
    mocker.patch("orchestration.tiled.register", fake_register)

    register_file_to_tiled(
        path=h5,
        prefix="beamlines/bl832/raw",
        tags=["raw", "bl832"],
    )

    fake_register.assert_awaited_once()
    assert len(entry_node.patch_calls) == 1
    tags_written = entry_node.patch_calls[0]["access_blob_patch"][0]["value"]["tags"]
    assert set(tags_written) == {"raw", "bl832"}


def test_register_zarr_with_tags_applies_to_stem(mocker: MockFixture, tmp_path):
    """A .zarr store follows the same stem-keyed lookup as .h5."""
    zarr = tmp_path / "sample.zarr"
    # Real .zarr is a directory, but the code path is suffix-driven, so a file is fine
    zarr.touch()

    entry_node = MockNode(access_blob=None)
    prefix_node = MockNode()
    prefix_node.add_child("sample", entry_node)
    client = build_prefix_chain(["beamlines", "bl832", "scratch"], prefix_node)

    mocker.patch("orchestration.tiled.from_uri", return_value=client)
    mocker.patch("orchestration.tiled.register", mocker.AsyncMock(return_value=None))

    register_file_to_tiled(
        path=zarr,
        prefix="beamlines/bl832/scratch",
        tags=["bl832"],
    )

    assert len(entry_node.patch_calls) == 1


def test_register_no_tags_skips_apply_tags(mocker: MockFixture, tmp_path):
    """When ``tags`` is None/empty, the tag-application branch is skipped entirely."""
    h5 = tmp_path / "scan.h5"
    h5.touch()

    prefix_node = MockNode()
    mocker.patch("orchestration.tiled.from_uri", return_value=prefix_node)
    fake_register = mocker.AsyncMock(return_value=None)
    mocker.patch("orchestration.tiled.register", fake_register)

    register_file_to_tiled(path=h5)

    fake_register.assert_awaited_once()
    assert prefix_node.patch_calls == []


def test_register_string_path_is_coerced(mocker: MockFixture, tmp_path):
    """Passing ``path`` as a ``str`` must be coerced to ``Path``.

    Regression test: without ``Path(path)``, the ``path.is_dir()`` and
    ``path.suffix`` calls explode with ``AttributeError``.
    """
    h5 = tmp_path / "scan.h5"
    h5.touch()

    entry_node = MockNode(access_blob=None)
    prefix_node = MockNode()
    prefix_node.add_child("scan", entry_node)
    client = build_prefix_chain(["beamlines", "bl832", "raw"], prefix_node)

    mocker.patch("orchestration.tiled.from_uri", return_value=client)
    mocker.patch("orchestration.tiled.register", mocker.AsyncMock(return_value=None))

    # Pass a string, not a Path — would fail without the Path(path) coercion
    register_file_to_tiled(
        path=str(h5),
        prefix="beamlines/bl832/raw",
        tags=["raw"],
    )

    assert len(entry_node.patch_calls) == 1


def test_register_tiff_dir_tags_each_child(mocker: MockFixture, tmp_path):
    """A directory with no suffix should tag every child under the prefix node."""
    tiff_dir = tmp_path / "tiffs"
    tiff_dir.mkdir()
    (tiff_dir / "frame_0000.tiff").touch()
    (tiff_dir / "frame_0001.tiff").touch()

    child_a = MockNode(access_blob=None)
    child_b = MockNode(access_blob=None)
    prefix_node = MockNode()
    prefix_node.add_child("frame_0000", child_a)
    prefix_node.add_child("frame_0001", child_b)
    client = build_prefix_chain(["beamlines", "bl832", "scratch"], prefix_node)

    mocker.patch("orchestration.tiled.from_uri", return_value=client)
    mocker.patch("orchestration.tiled.register", mocker.AsyncMock(return_value=None))

    register_file_to_tiled(
        path=tiff_dir,
        prefix="beamlines/bl832/scratch",
        tags=["bl832"],
    )

    assert len(child_a.patch_calls) == 1
    assert len(child_b.patch_calls) == 1


# ---------------------------------------------------------------------------
# register_file_to_tiled — edge cases
# ---------------------------------------------------------------------------

def test_register_missing_entry_after_register_logs_warning(mocker: MockFixture, tmp_path):
    """If ``node[stem]`` raises ``KeyError`` post-register, no exception leaks and a warning is logged."""
    h5 = tmp_path / "missing.h5"
    h5.touch()

    # Prefix node has *some* entries but not the one we'll look up
    prefix_node = MockNode()
    prefix_node.add_child("other-entry", MockNode())
    client = build_prefix_chain(["beamlines", "bl832", "raw"], prefix_node)

    mocker.patch("orchestration.tiled.from_uri", return_value=client)
    mocker.patch("orchestration.tiled.register", mocker.AsyncMock(return_value=None))
    mock_logger = mocker.patch("orchestration.tiled.get_run_logger")

    # Should not raise — the KeyError is caught and logged with available keys
    register_file_to_tiled(
        path=h5,
        prefix="beamlines/bl832/raw",
        tags=["raw"],
    )

    mock_logger.return_value.warning.assert_called_once()
    warning_msg = mock_logger.return_value.warning.call_args[0][0]
    assert "missing" in warning_msg   # stem of the file
    assert "other-entry" in warning_msg  # available keys listed in the message


def test_register_raises_runtime_error_on_failure(mocker: MockFixture, tmp_path):
    """If the bridge or Tiled's ``register`` raises, wrap it in ``RuntimeError``."""
    h5 = tmp_path / "scan.h5"
    h5.touch()

    mocker.patch("orchestration.tiled.from_uri", return_value=MockNode())
    mocker.patch(
        "orchestration.tiled.run_coro_as_sync",
        side_effect=Exception("connection refused"),
    )

    with pytest.raises(RuntimeError, match="Failed to register .* connection refused"):
        register_file_to_tiled(path=h5)


# ---------------------------------------------------------------------------
# check_tags
# ---------------------------------------------------------------------------

def test_check_tags_returns_true_when_all_expected_present(mocker: MockFixture, tmp_path):
    """Subset semantics: ok iff expected_tags <= actual_tags."""
    h5 = tmp_path / "scan.h5"
    h5.touch()

    entry_node = MockNode(access_blob={"tags": ["bl832", "raw", "extra"]})
    prefix_node = MockNode()
    prefix_node.add_child("scan", entry_node)
    client = build_prefix_chain(["beamlines"], prefix_node)

    mocker.patch("orchestration.tiled.from_uri", return_value=client)

    ok, actual = check_tags(
        path=h5,
        prefix="beamlines",
        expected_tags={"bl832", "raw"},
    )

    assert ok is True
    assert set(actual) == {"bl832", "raw", "extra"}


def test_check_tags_returns_false_when_tag_missing(mocker: MockFixture, tmp_path):
    """A single missing expected tag flips ok to False."""
    h5 = tmp_path / "scan.h5"
    h5.touch()

    entry_node = MockNode(access_blob={"tags": ["bl832"]})
    prefix_node = MockNode()
    prefix_node.add_child("scan", entry_node)
    client = build_prefix_chain(["beamlines"], prefix_node)

    mocker.patch("orchestration.tiled.from_uri", return_value=client)

    ok, actual = check_tags(
        path=h5,
        prefix="beamlines",
        expected_tags={"bl832", "raw"},
    )

    assert ok is False
    assert actual == ["bl832"]


def test_check_tags_tiff_dir_uses_first_child(mocker: MockFixture, tmp_path):
    """For a TIFF dir, ``check_tags`` reads the first child of the prefix node."""
    tiff_dir = tmp_path / "tiffs"
    tiff_dir.mkdir()
    (tiff_dir / "frame_0000.tiff").touch()

    first_child = MockNode(access_blob={"tags": ["bl832"]})
    prefix_node = MockNode()
    prefix_node.add_child("frame_0000", first_child)
    client = build_prefix_chain(["beamlines", "bl832", "scratch"], prefix_node)

    mocker.patch("orchestration.tiled.from_uri", return_value=client)

    ok, actual = check_tags(
        path=tiff_dir,
        prefix="beamlines/bl832/scratch",
        expected_tags={"bl832"},
    )

    assert ok is True
    assert actual == ["bl832"]


def test_check_tags_no_access_blob_returns_empty(mocker: MockFixture, tmp_path):
    """When ``access_blob`` is None, actual tags should be ``[]``."""
    h5 = tmp_path / "scan.h5"
    h5.touch()

    entry_node = MockNode(access_blob=None)
    prefix_node = MockNode()
    prefix_node.add_child("scan", entry_node)
    client = build_prefix_chain(["beamlines"], prefix_node)

    mocker.patch("orchestration.tiled.from_uri", return_value=client)

    ok, actual = check_tags(
        path=h5,
        prefix="beamlines",
        expected_tags={"bl832"},
    )

    assert ok is False
    assert actual == []


def test_check_tags_missing_entry_raises_key_error(mocker: MockFixture, tmp_path):
    """If the stem isn't present under the prefix, the lookup raises ``KeyError``."""
    h5 = tmp_path / "missing.h5"
    h5.touch()

    prefix_node = MockNode()
    prefix_node.add_child("other", MockNode())
    client = build_prefix_chain(["beamlines"], prefix_node)

    mocker.patch("orchestration.tiled.from_uri", return_value=client)

    with pytest.raises(KeyError):
        check_tags(
            path=h5,
            prefix="beamlines",
            expected_tags={"bl832"},
        )


# ---------------------------------------------------------------------------
# register_to_tiled flow
# ---------------------------------------------------------------------------

def test_register_to_tiled_flow_delegates_to_task(mocker: MockFixture, tmp_path):
    """The flow is a thin wrapper that just calls the task with the same args."""
    h5 = tmp_path / "scan.h5"
    h5.touch()

    mock_task = mocker.patch("orchestration.tiled.register_file_to_tiled")

    register_to_tiled(
        path=h5,
        prefix="beamlines/bl832",
        overwrite=True,
        tags=["bl832"],
    )

    mock_task.assert_called_once_with(
        h5,
        prefix="beamlines/bl832",
        overwrite=True,
        tags=["bl832"],
    )


def test_register_to_tiled_flow_coerces_string_path(mocker: MockFixture):
    """Passing a string should be coerced to ``Path`` before forwarding."""
    from pathlib import Path

    mock_task = mocker.patch("orchestration.tiled.register_file_to_tiled")

    register_to_tiled(path="/data/sample.h5", prefix="any")

    forwarded_path = mock_task.call_args.args[0]
    assert isinstance(forwarded_path, Path)
    assert forwarded_path == Path("/data/sample.h5")
