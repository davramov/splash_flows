from __future__ import annotations

import asyncio
from concurrent.futures import ProcessPoolExecutor

from academy.agent import action
from academy.handle import Handle
from academy.logging import init_logging
from academy.manager import Manager
from academy.exchange.cloud import HttpExchangeFactory
from agentic_blueprint_catalog.observability.user_agent import UserAgent
from agentic_blueprint_catalog.observability.monitored_agent import MonitoredAgent

from globus_compute_sdk import Executor as GlobusExecutor


ALCF_ENDPOINT_ID = "9a947ba5-f537-4681-acf3-cc66485aadec"
ALLOCATION_ROOT = "/eagle/SYNAPS-I"
SYNAPS_COLLECTION_ID = "728a8e30-32ef-4000-814c-f9ccbc00bf13"


class SegmentationAgent(MonitoredAgent):
    """Segmentation Agent."""

    def __init__(self, user_agent_handle: Handle[UserAgent]) -> None:
        super().__init__(user_agent_handle=user_agent_handle)

    @action
    async def segment(
        self,
        scan_id: str = "test_scan",
        reconstructed_volumes: str = "/eagle/SYNAPS-I/data/bl832/scratch/reconstruction/ALS-13540_lisabeth/rec20260304_174601_S3_A_AT_1/",
    ) -> str:
        """Segment a single reconstructed TIFF using ALCF AI SAM3 submit-image.

        Calls submit-image once per prompt (CLI accepts only one prompt at a time)
        and saves each preview as <stem>_<prompt>.png in the output dir.

        Requires `alcf-ai auth login` to have been run once on this host.
        """
        import asyncio
        import logging
        import shlex
        import subprocess
        from pathlib import Path

        alcf_ai_env = "/home/dabramov/agents/envs/alcf_ai"

        root = Path(reconstructed_volumes)
        if not root.exists():
            logging.error("Segmentation input path does not exist: %s", root)
            return "Segmentation failed"

        # Pick the middle slice
        images = sorted(root.glob("*.tiff"))
        if not images:
            logging.error("No .tiff files found in %s", root)
            return "Segmentation failed"
        image_path = images[len(images) // 2]

        prompts = ["crack", "void", "air"]

        output_dir = Path("/eagle/SYNAPS-I/data/bl832/scratch/sam3") / scan_id / "single_image_test"
        output_dir.mkdir(parents=True, exist_ok=True)

        logging.info("Segmenting %s with %d prompts: %s", image_path, len(prompts), prompts)

        # Build a single shell script that runs submit-image once per prompt.
        # Failures don't stop the loop — collect results for all prompts, report at end.
        setup = f"""
set -uo pipefail

module use /soft/modulefiles
module load conda
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate {shlex.quote(alcf_ai_env)}

failures=0
"""

        body_lines = []
        preview_paths = []
        for prompt in prompts:
            # Sanitize prompt for filename (spaces → underscores, no slashes)
            safe = prompt.replace(" ", "_").replace("/", "_")
            preview_path = output_dir / f"{image_path.stem}_{safe}.png"
            preview_paths.append(preview_path)

            body_lines.append(f"""
echo ">>> Prompt: {shlex.quote(prompt)}"
if alcf-ai sam3 submit-image \\
    {shlex.quote(str(image_path))} \\
    {shlex.quote(prompt)} \\
    --save-preview {shlex.quote(str(preview_path))}; then
  echo "    SUCCESS: {shlex.quote(str(preview_path))}"
else
  echo "    FAILED: prompt={shlex.quote(prompt)}"
  failures=$((failures + 1))
fi
""")

        teardown = """
echo ">>> Done. $failures failures."
exit $failures
"""

        shell = setup + "".join(body_lines) + teardown

        proc = await asyncio.create_subprocess_exec(
            "bash",
            "-lc",
            shell,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        stdout, stderr = await proc.communicate()

        out = stdout.decode(errors="replace").strip()
        err = stderr.decode(errors="replace").strip()

        if out:
            logging.info("submit-image stdout: %s", out[-4000:])
        if err:
            logging.warning("submit-image stderr: %s", err[-4000:])

        if proc.returncode != 0:
            logging.error("submit-image had %d failure(s)", proc.returncode)
            # Don't bail — partial success is still useful.

        # Report which previews actually landed on disk
        landed = [p for p in preview_paths if p.exists()]
        missing = [p for p in preview_paths if not p.exists()]

        if landed:
            logging.info("Saved %d/%d previews to %s", len(landed), len(preview_paths), output_dir)
        if missing:
            logging.warning("Missing previews: %s", [p.name for p in missing])

        if not landed:
            return "Segmentation failed"

        return str(output_dir)


class ReconstructionAgent(MonitoredAgent):
    """Reconstruction Agent."""

    def __init__(
        self,
        segmentation_agent_handle: Handle[SegmentationAgent],
        user_agent_handle: Handle[UserAgent],
    ) -> None:
        """Initialize Reconstruction Agent."""
        super().__init__(user_agent_handle=user_agent_handle)
        self.segmentation_agent_handle = segmentation_agent_handle
        print("Reconstruction agent init done")

    @action
    async def reconstruct(self, scan_id: str, scan_path: str) -> str | None:
        import logging
        import os
        import subprocess
        import tempfile
        import time
        from pathlib import Path

        rec_start = time.time()

        rundir = "/eagle/SYNAPS-I/data/bl832/raw"
        script_path = "/eagle/SYNAPS-I/reconstruction/scripts/globus_reconstruction_multinode.py"
        tomopy_env = "/eagle/SYNAPS-I/reconstruction/env/tomopy"

        scan = Path(scan_path)
        file_name = scan.name
        folder_name = scan.parent.name

        pbs_nodefile = os.environ.get("PBS_NODEFILE")
        if pbs_nodefile and os.path.exists(pbs_nodefile):
            with open(pbs_nodefile) as f:
                node_list = list(dict.fromkeys(line.strip() for line in f if line.strip()))
        else:
            node_list = ["localhost"]

        num_nodes = len(node_list)
        logging.info("Using %d nodes: %s", num_nodes, node_list)

        env_setup = (
            "export TMPDIR=/tmp && "
            "export NUMEXPR_MAX_THREADS=64 && "
            "export NUMEXPR_NUM_THREADS=64 && "
            "export OMP_NUM_THREADS=64 && "
            "export MKL_NUM_THREADS=64 && "
            "module use /soft/modulefiles && "
            "module load conda && "
            "source $(conda info --base)/etc/profile.d/conda.sh && "
            f"conda activate {tomopy_env} && "
            f"cd {rundir} && "
        )

        # Let the reconstruction script decide slice partitioning if possible.
        # If your script requires slice ranges, keep h5py inside this conda shell.
        get_slices_cmd = (
            env_setup
            + "python - <<'PY'\n"
            + "import h5py\n"
            + f"with h5py.File('{rundir}/{folder_name}/{file_name}', 'r') as f:\n"
            + "    print(f['/exchange/data'].shape[1])\n"
            + "PY"
        )

        num_slices = int(
            subprocess.check_output(["bash", "-lc", get_slices_cmd], text=True).strip()
        )
        slices_per_node = num_slices // num_nodes

        procs = []
        temp_hostfiles = []

        try:
            for i, node in enumerate(node_list):
                sino_start = i * slices_per_node
                sino_end = num_slices if i == num_nodes - 1 else (i + 1) * slices_per_node

                cmd = (
                    env_setup
                    + f"python {script_path} {file_name} {folder_name} {sino_start} {sino_end}"
                )

                with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".hosts") as f:
                    f.write(node + "\n")
                    hostfile = f.name

                temp_hostfiles.append(hostfile)

                full_cmd = [
                    "mpiexec",
                    "-n", "1",
                    "-ppn", "1",
                    "--cpu-bind", "depth",
                    "-d", "64",
                    "-hostfile", hostfile,
                    "bash",
                    "-lc",
                    cmd,
                ]

                logging.info("Launching on %s: slices %d-%d", node, sino_start, sino_end)
                procs.append(
                    (
                        subprocess.Popen(
                            full_cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True,
                        ),
                        node,
                        sino_start,
                        sino_end,
                    )
                )

            failed = []
            for proc, node, sino_start, sino_end in procs:
                stdout, stderr = proc.communicate()

                if stdout:
                    logging.info("STDOUT %s: %s", node, stdout[-2000:])
                if stderr:
                    logging.warning("STDERR %s: %s", node, stderr[-2000:])

                if proc.returncode != 0:
                    failed.append(node)
                    logging.error("FAILED on %s slices %d-%d", node, sino_start, sino_end)
                else:
                    logging.info("SUCCESS on %s slices %d-%d", node, sino_start, sino_end)

            if failed:
                raise RuntimeError(f"Reconstruction failed on nodes: {failed}")

            result = f"Reconstructed {file_name} across {num_nodes} nodes in {time.time() - rec_start:.1f}s"
            logging.info(result)
            return result

        finally:
            for hostfile in temp_hostfiles:
                try:
                    os.remove(hostfile)
                except OSError:
                    pass

    @action
    async def sanity_check(
        self,
        reconstructed_path: str = "/eagle/SYNAPS-I/data/bl832/scratch/reconstruction/ALS-13540_lisabeth/rec20260304_174601_S3_A_AT_1/",
    ) -> str:
        """Run vision-based sanity check on one reconstructed image."""
        import asyncio
        import logging
        import subprocess
        from pathlib import Path

        alcf_ai_env = "/home/dabramov/agents/envs/alcf_ai"
        model = "google/gemma-4-31B-it"

        root = Path(reconstructed_path)
        image_exts = {".png", ".jpg", ".jpeg", ".tif", ".tiff"}

        if root.is_file() and root.suffix.lower() in image_exts:
            image_path = root
        else:
            images = sorted(
                p for p in root.rglob("*")
                if p.is_file() and p.suffix.lower() in image_exts
            )
            if not images:
                logging.error("No reconstructed images found under %s", reconstructed_path)
                return "Check Failed!"
            image_path = images[len(images) // 2]

        logging.info("Vision sanity check image: %s", image_path)

        prompt = """
You are checking a tomography reconstruction image for obvious failure.

Look for:
- blank or nearly blank image
- severe striping/ringing artifacts
- corrupted/noisy output
- missing sample structure
- extreme saturation or clipping

Return exactly one line:
PASS: brief reason
or
FAIL: brief reason
"""

        code = f"""
import base64
import tempfile
from pathlib import Path

from PIL import Image, ImageOps
from alcf_ai import InferenceClient

image_path = Path({str(image_path)!r})
model = {model!r}
prompt = {prompt!r}

# Convert large TIFF slice to compact PNG preview.
preview_path = Path(tempfile.gettempdir()) / (image_path.stem + "_preview.png")

img = Image.open(image_path)
img = ImageOps.autocontrast(img.convert("L"))
img.thumbnail((1024, 1024))
img.save(preview_path, format="PNG", optimize=True)

b64 = base64.b64encode(preview_path.read_bytes()).decode("utf-8")
mime = "image/png"

client = InferenceClient()
oai = client.clusters("sophia").openai

resp = oai.chat.completions.create(
    model=model,
    messages=[
        {{
            "role": "user",
            "content": [
                {{"type": "text", "text": prompt}},
                {{
                    "type": "image_url",
                    "image_url": {{
                        "url": f"data:{{mime}};base64,{{b64}}"
                    }},
                }},
            ],
        }}
    ],
    temperature=0.0,
    max_tokens=128,
)

print(resp.choices[0].message.content)
"""

        shell = f"""
module use /soft/modulefiles
module load conda
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate {alcf_ai_env}
python - <<'PY'
{code}
PY
"""

        proc = await asyncio.create_subprocess_exec(
            "bash",
            "-lc",
            shell,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            logging.error(
                "Vision sanity check failed rc=%s stderr=%s",
                proc.returncode,
                stderr.decode(errors="replace")[-4000:],
            )
            return "Check Failed!"

        verdict = stdout.decode(errors="replace").strip()
        logging.info("Vision sanity verdict: %s", verdict)

        if verdict.upper().startswith("PASS"):
            return "Check Passed!"

        return "Check Failed!"

    @action
    async def reconstruct_and_segment(self, scan_id: str, scan_path: str) -> str:
        """Reconstruct -> Sanity check -> Segment."""
        import logging
        reconstructed = await self.reconstruct(scan_id, scan_path)
        if reconstructed is None:
            logging.warning(f"Reconstruction failed for {scan_id}; aborting.")
            return "Reconstruction aborted"
        sanity = await self.sanity_check()
        if sanity == "Check Failed!":
            # self.user_agent.prompt_user_agent("Something went wrong. Should i continue?",
            #                                  ["yes", "shut it down"])
            logging.warning("Reconstruction failed. Aborting segmentation")
            return "Segmentation aborted"

        segmented = await self.segmentation_agent_handle.segment()
        return segmented


async def main():

    init_logging()

    executors = {
        "local": ProcessPoolExecutor(max_workers=4),
        # The polaris config will not work until it is installed on the
        # polaris environment loaded via config_key
        "polaris": GlobusExecutor(
            endpoint_id=ALCF_ENDPOINT_ID,  # "9a947ba5-f537-4681-acf3-cc66485aadec",
            # user_endpoint_config={
            #     "queue": "debug",
            #     "walltime": "00:10:00",
            #     # TODO: FIX THE PATH BELOW!
            #     # "config_key": "source /home/yadunand/setup_academy.sh",
            #     "config_key": "source /home/dabramov/agents/setup_academy.sh",
            #     "account": "SYNAPS-I",
            #     # "ACCOUNT_ID": "dabramov"
            # },
            user_endpoint_config={
                "queue": "demand",
                "walltime": "00:30:00",
                "account": "SYNAPS-I",
                "config_key": "source /home/dabramov/agents/setup_academy.sh",
                "nodes_per_block": 4,
                "num_nodes": 4,
                "max_blocks": 1,
            },
        ),
    }
    # Create manager with agents and their assigned executors
    async with await Manager.from_exchange_factory(
        # factory=LocalExchangeFactory(),
        factory=HttpExchangeFactory("https://exchange.academy-agents.org"),  # Use cloud hosted exchange
        executors=executors,  # Use Process pool for testing
    ) as manager:
        user_agent = await manager.launch(UserAgent)

        seg_agent = await manager.launch(
            SegmentationAgent,
            kwargs={"user_agent_handle": user_agent},
            executor="polaris",
        )

        reco_agent = await manager.launch(
            ReconstructionAgent,
            kwargs={
                "segmentation_agent_handle": seg_agent,
                "user_agent_handle": user_agent,
            },
            executor="polaris",
        )

        await reco_agent.reconstruct_and_segment(
            scan_id="test_scan", scan_path="/eagle/SYNAPS-I/data/bl832/raw/ALS-13540_lisabeth/20260304_174601_S3_A_AT_1.h5"
        )

        await asyncio.sleep(10)


if __name__ == "__main__":
    print("Starting main")
    raise SystemExit(asyncio.run(main()))
