import logging
from dataclasses import dataclass, field
import json
import requests
from typing import Any

import mlflow
from mlflow.tracking import MlflowClient

from orchestration.config import BeamlineConfig

logger = logging.getLogger(__name__)


@dataclass
class ModelCheckpointInfo:
    """Checkpoint and inference metadata for a registered model version.

    Attributes:
        model_name: Registered model name in MLflow (e.g. 'sam3-petiole').
        version: MLflow model version string.
        alias: The alias resolved to find this version (e.g. 'production').
        nersc_path: Primary checkpoint path on NERSC CFS.
        alcf_path: Primary checkpoint path on ALCF Eagle.
        inference_params: Inference hyperparameters and paths stored as tags,
            keyed by their config YAML name for direct overlay onto opts.
    """

    model_name: str
    version: str
    alias: str
    nersc_path: str
    alcf_path: str
    inference_params: dict[str, Any] = field(default_factory=dict)


def _is_mlflow_reachable(tracking_uri: str, timeout: float = 2.0) -> bool:
    """Check whether the MLflow tracking server is reachable.

    Makes a single lightweight GET to /health with a short timeout.
    Used to short-circuit registry lookups before MLflow's HTTP client
    fires its retry loop (which defaults to 6 retries with backoff).

    Args:
        tracking_uri: Base URL of the MLflow tracking server.
        timeout: Connection timeout in seconds.

    Returns:
        True if the server responds with HTTP 200, False otherwise.
    """
    try:
        response = requests.get(f"{tracking_uri}/health", timeout=timeout)
        return response.status_code == 200
    except Exception:
        return False


def get_mlflow_client(config: BeamlineConfig) -> MlflowClient:
    """Construct an MlflowClient pointed at the configured tracking server.

    Args:
        config: Beamline configuration object, used to read the tracking URI.

    Returns:
        An authenticated MlflowClient instance.
    """
    tracking_uri = config.mlflow["tracking_uri"]
    mlflow.set_tracking_uri(tracking_uri)
    return MlflowClient(tracking_uri=tracking_uri)


def get_checkpoint_info(
    model_name: str,
    config: BeamlineConfig,
    alias: str = "production",
) -> ModelCheckpointInfo | None:
    """Retrieve checkpoint path metadata for a registered model version.

    Looks up the model version registered under ``alias`` in the MLflow
    Model Registry and returns its ``nersc_path`` and ``alcf_path`` tags.
    Returns ``None`` (and logs a warning) if the model or alias is not found,
    allowing callers to fall back to config defaults.

    Args:
        model_name: Registered model name, e.g. ``'sam3-petiole'``.
        config: Beamline configuration object.
        alias: Model version alias to resolve, e.g. ``'production'``.

    Returns:
        A ``ModelCheckpointInfo`` with checkpoint paths, or ``None`` if not found.
    """
    if not _is_mlflow_reachable(config.mlflow["tracking_uri"]):
        logger.warning(
            f"MLflow server unreachable at {config.mlflow['tracking_uri']}. "
            "Falling back to config defaults."
        )
        return None

    client = get_mlflow_client(config)

    try:
        mv = client.get_model_version_by_alias(model_name, alias)
    except mlflow.exceptions.MlflowException as e:
        logger.warning(
            f"Could not resolve alias '{alias}' for model '{model_name}': {e}. "
            "Falling back to config defaults."
        )
        return None

    tags = mv.tags or {}
    nersc_path = tags.get("nersc_path", "")
    alcf_path = tags.get("alcf_path", "")

    if not nersc_path:
        logger.warning(
            f"Model '{model_name}' v{mv.version} has no 'nersc_path' tag. "
            "Falling back to config defaults."
        )
        return None

    # Deserialize all remaining tags; JSON-decode lists/dicts automatically
    reserved = {"nersc_path", "alcf_path"}
    inference_params: dict[str, Any] = {}
    for k, v in tags.items():
        if k in reserved:
            continue
        try:
            inference_params[k] = json.loads(v)
        except (json.JSONDecodeError, TypeError):
            inference_params[k] = v

    logger.info(
        f"Resolved '{model_name}' alias='{alias}' -> v{mv.version} "
        f"with {len(inference_params)} inference params."
    )
    return ModelCheckpointInfo(
        model_name=model_name,
        version=mv.version,
        alias=alias,
        nersc_path=nersc_path,
        alcf_path=alcf_path,
        inference_params=inference_params,
    )


def register_checkpoint(
    model_name: str,
    nersc_path: str,
    config: BeamlineConfig,
    alcf_path: str = "",
    alias: str = "production",
    description: str = "",
    inference_params: dict[str, Any] | None = None,
) -> str:
    """Register a model checkpoint in the MLflow Model Registry.

    Creates or updates a registered model and logs a new version with
    ``nersc_path`` and ``alcf_path`` stored as version-level tags.
    The new version is immediately assigned the given alias.

    Args:
        model_name: Registered model name, e.g. ``'sam3-petiole'``.
        nersc_path: Absolute path to the checkpoint on NERSC CFS.
        config: Beamline configuration object.
        alcf_path: Absolute path to the checkpoint on ALCF Eagle (optional).
        alias: Alias to assign to the new version after registration.
        description: Human-readable description for the model version.
        inference_params: Model-coupled inference hyperparameters and paths to
            store as tags, e.g. batch_size, prompts, conda_env_path.

    Returns:
        The new model version string.

    Raises:
        mlflow.exceptions.MlflowException: If registration or tagging fails.
    """
    client = get_mlflow_client(config)

    try:
        client.get_registered_model(model_name)
    except mlflow.exceptions.MlflowException:
        logger.info(f"Creating registered model '{model_name}'.")
        client.create_registered_model(model_name)

    mlflow.set_tracking_uri(config.mlflow["tracking_uri"])
    with mlflow.start_run(
        run_name=f"register_{model_name}",
        tags={"mlflow.note.content": description},
    ) as run:
        mlflow.log_param("nersc_path", nersc_path)
        mlflow.log_param("alcf_path", alcf_path)
        if inference_params:
            mlflow.log_params({
                k: (json.dumps(v) if isinstance(v, (list, dict)) else v)
                for k, v in inference_params.items()
            })
        run_id = run.info.run_id

    mv = mlflow.register_model(model_uri=f"runs:/{run_id}/model", name=model_name)
    version = mv.version

    client.set_model_version_tag(model_name, version, "nersc_path", nersc_path)
    if alcf_path:
        client.set_model_version_tag(model_name, version, "alcf_path", alcf_path)

    if inference_params:
        for k, v in inference_params.items():
            encoded = json.dumps(v) if isinstance(v, (list, dict)) else str(v)
            client.set_model_version_tag(model_name, version, k, encoded)

    client.set_registered_model_alias(model_name, alias, version)
    logger.info(
        f"Registered '{model_name}' v{version} alias='{alias}' "
        f"with {len(inference_params or {})} inference params."
    )
    return version


def log_segmentation_metrics(
    run_name: str,
    model_name: str,
    job_id: str,
    config: BeamlineConfig,
    timing: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    parent_run_id: str | None = None,
) -> str:
    """Log segmentation job metrics as a child MLflow run.

    Creates a child run under ``parent_run_id`` (if provided) and records
    timing metrics, SLURM job ID, and any additional params.

    Args:
        run_name: Display name for this run.
        model_name: Model identifier, used to tag the run for filtering.
        job_id: SLURM job ID for this segmentation job.
        config: Beamline configuration object.
        timing: Timing dict returned by ``_fetch_seg_timing_from_output``.
        params: Arbitrary key-value pairs to log as MLflow params.
        parent_run_id: If set, this run is nested under the parent.

    Returns:
        The MLflow run ID for the logged child run.
    """
    tracking_uri = config.mlflow["tracking_uri"]
    mlflow.set_tracking_uri(tracking_uri)

    run_tags: dict[str, str] = {"model": model_name, "slurm_job_id": job_id}

    with mlflow.start_run(
        run_name=run_name,
        nested=parent_run_id is not None,
        parent_run_id=parent_run_id,
        tags=run_tags,
    ) as run:
        mlflow.log_param("slurm_job_id", job_id)
        mlflow.log_param("model", model_name)

        if params:
            mlflow.log_params(params)

        if timing:
            metrics: dict[str, float] = {}
            if "total_seconds" in timing:
                metrics["total_seconds"] = float(timing["total_seconds"])
            if "num_images" in timing:
                metrics["num_images"] = float(timing["num_images"])
            if "throughput" in timing:
                metrics["throughput_images_per_min"] = float(timing["throughput"])
            if "time_per_image" in timing:
                # Stored as "3.23s" — strip the unit
                raw = str(timing["time_per_image"]).rstrip("s")
                try:
                    metrics["time_per_image_seconds"] = float(raw)
                except ValueError:
                    pass
            if metrics:
                mlflow.log_metrics(metrics)

        logger.info(f"Logged segmentation run '{run_name}' as MLflow run {run.info.run_id}")
        return run.info.run_id
