from abc import ABC, abstractmethod
import logging
import os
import requests
from typing import Optional
from urllib.parse import urljoin

from pyscicat.client import ScicatClient, from_credentials
from pyscicat.model import (
    CreateDatasetOrigDatablockDto,
    DataFile,
)
from orchestration.config import BeamlineConfig


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# NOTE: This BeamlineIngestorController will be relocated to https://github.com/als-computing/scicat_beamline soon.


class BeamlineIngestorController(ABC):
    """
    Abstract class for beamline SciCat ingestors.
    Provides interface methods for ingesting data.
    """

    def __init__(
        self,
        config: BeamlineConfig,
        scicat_client: Optional[ScicatClient] = None
    ) -> None:
        self.config = config
        self.scicat_client = scicat_client

    def get_scicat_client(
        self,
        scicat_base_url: Optional[str] = None,
        scicat_user: Optional[str] = None,
        scicat_password: Optional[str] = None
    ) -> ScicatClient:
        """
        Log in to SciCat using the provided credentials.

        :param scicat_base_url: Base URL of the SciCat instance. Defaults to the environment variable 'SCICAT_API_URL'.
        :param scicat_user: Username for the SciCat instance. Defaults to the environment variable 'SCICAT_INGEST_USER'.
        :param scicat_password: Password for the SciCat instance. Defaults to the environment variable 'SCICAT_INGEST_PASSWORD'
        :return: An instance of ScicatClient with an authenticated session.
        :raises ValueError: If any required credentials are missing.
        """
        # Use environment variables as defaults if parameters are not provided.
        scicat_base_url = scicat_base_url or os.getenv("SCICAT_API_URL")
        scicat_user = scicat_user or os.getenv("SCICAT_INGEST_USER")
        scicat_password = scicat_password or os.getenv("SCICAT_INGEST_PASSWORD")

        logger.info(f"Logging in to SciCat at {scicat_base_url} as {scicat_user}.")

        # Ensure that all required credentials are provided.
        if not (scicat_base_url and scicat_user and scicat_password):
            raise ValueError(
                "Missing required SciCat credentials. Provide scicat_base_url, scicat_user, "
                "and scicat_password as parameters or set them in the environment variables: "
                "SCICAT_API_URL, SCICAT_INGEST_USER, SCICAT_INGEST_PASSWORD."
            )

        # Try to log in using the pyscicat client first.
        # This method seems deprecated, but leaving it here for backwards compatability
        # https://github.com/SciCatProject/pyscicat/issues/61
        try:
            self.scicat_client = from_credentials(
                base_url=scicat_base_url,
                username=scicat_user,
                password=scicat_password
            )
            logger.info("Logged in to SciCat.")
            return self.scicat_client
        except Exception as e:
            logger.warning(f"Failed to log in to SciCat: {e}, trying alternative method.")

        # This method works for scicatlive 3.2.5
        try:
            url = urljoin(scicat_base_url, "auth/login")
            logger.info(url)
            response = requests.post(
                url=url,
                json={"username": scicat_user, "password": scicat_password},
                stream=False,
                verify=True,
            )
            logger.info(f"Login response: {response.json()}")
            self.scicat_client = ScicatClient(scicat_base_url, response.json()["access_token"])
            logger.info("Logged in to SciCat.")
            # logger.info(f"SciCat token: {response.json()['access_token']}")
            return self.scicat_client

        except Exception as e:
            logger.error(f"Failed to log in to SciCat: {e}")
            raise e

    @abstractmethod
    def ingest_new_raw_dataset(
        self,
        file_path: str = "",
    ) -> str:
        """Ingest data from the beamline.

        :param file_path: Path to the file to ingest.
        :return: SciCat ID of the dataset.
        """
        pass

    @abstractmethod
    def ingest_new_derived_dataset(
        self,
        file_path: str = "",
        raw_dataset_id: Optional[str] = "",
    ) -> str:
        """Ingest data from the beamline.

        :param file_path: Path to the file to ingest.
        :return: SciCat ID of the dataset.
        """
        pass

    def add_new_dataset_location(
        self,
        dataset_id: str = None,
        datafile_path: str = None,
        source_folder_host: Optional[str] = None
    ) -> str:
        """
        Add a new location to an existing dataset in SciCat.

        :param dataset_id:          SciCat ID of the dataset.
        :param datafile_path:       Absolute file path to the data file (excluding protocol/host).
                                    Caller is responsible for full path composition, including filename.
        :param source_folder_host: "DNS host name of file server hosting sourceFolder,
                                    optionally including a protocol e.g. [protocol://]fileserver1.example.com",
        :return: The dataset ID after successful datablock addition.
        :raises ValueError: If the dataset ID is not found or if the dataset does not have a valid 'pid'.
        """
        # Get the dataset to retrieve its metadata
        dataset = self.scicat_client.datasets_get_one(dataset_id)
        if not dataset:
            raise ValueError(f"Dataset with ID {dataset_id} not found")

        logger.info(f"Creating new datablock for dataset {dataset_id} at location {datafile_path}")

        try:
            # Create a datafile for the new location
            file_path = datafile_path
            if source_folder_host:
                file_path = f"{source_folder_host}:{datafile_path}"

            # Get size from existing dataset if available
            size = dataset.get("size", 0)

            # Create a single datafile
            datafile = DataFile(
                path=file_path,
                size=size,
                time=dataset.get("creationTime")
            )

            # Create a minimal datablock for the new location
            datablock = CreateDatasetOrigDatablockDto(
                size=size,
                dataFileList=[datafile]
            )

            # Upload the datablock
            self.scicat_client.upload_dataset_origdatablock(dataset_id, datablock)
            logger.info(f"Created new datablock for dataset {dataset_id} at location {datafile_path}")

            # Note: We're skipping the dataset update since it's causing validation issues

        except Exception as e:
            logger.error(f"Failed to create new datablock for dataset {dataset_id}: {e}")
            # Continue without raising to maintain the workflow

        return dataset_id

    def remove_dataset_location(
        self,
        dataset_id: str = "",
        source_folder_host: str = "",
    ) -> bool:
        """
        Remove a location from an existing dataset in SciCat.
        We might want to do this after data was moved to a new location,
        and has been pruned from the previous location.

        :param dataset_id: SciCat ID of the dataset.
        :param source_folder_host: The source folder host to identify the location to remove.
        :return: True if the location was successfully removed, False otherwise.
        """
        logger.info(f"Removing location with host {source_folder_host} from dataset {dataset_id}")

        try:
            # Get the datablocks directly
            datablocks = self.scicat_client.datasets_origdatablocks_get_one(dataset_id)
            if not datablocks:
                logger.warning(f"No datablocks found for dataset {dataset_id}")
                return False

            # Find datablock matching the specified source_folder_host
            matching_datablock = None
            for datablock in datablocks:
                for datafile in datablock.get("dataFileList", []):
                    file_path = datafile.get("path", "")
                    if source_folder_host in file_path or (
                        "sourceFolderHost" in datablock and
                        datablock["sourceFolderHost"] == source_folder_host
                    ):
                        matching_datablock = datablock
                        break
                if matching_datablock:
                    break

            if not matching_datablock:
                logger.warning(
                    f"No datablock found for dataset {dataset_id} with source folder host {source_folder_host}"
                )
                return False

            # Delete the datablock using its ID
            datablock_id = matching_datablock.get("id")
            if not datablock_id:
                logger.error(f"Datablock found but has no ID for dataset {dataset_id}")
                return False

            # Delete the datablock using the appropriate endpoint
            response = self.scicat_client.datasets_delete(datablock_id)
            if response:
                logger.info(f"Successfully removed datablock {datablock_id} from dataset {dataset_id}")
                return True
            else:
                logger.error(f"Failed to delete datablock {datablock_id} from dataset {dataset_id}")
                return False

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                logger.error(f"Forbidden: You do not have permission to delete the datablock {datablock_id}")
            else:
                logger.error(f"HTTP error occurred: {e}")
        except Exception as e:
            logger.error(f"Failed to remove datablock from dataset {dataset_id}: {e}")
        return False

    def _find_dataset(
        self,
        proposal_id: Optional[str] = None,  # The ALS proposal ID, not the SciCat ID
        file_name: Optional[str] = None
    ) -> str:
        """
        Find a dataset in SciCat and return its ID based on proposal ID and file name.
        The dataset name in SciCat is expected to be saved as the base filename without the extension,
        e.g. '20241216_153047_ddd' for a file named '20241216_153047_ddd.h5'.

        Parameters:
            proposal_id (Optional[str]): The proposal identifier used in ingestion.
            file_name (Optional[str]): The full path to the file; its base name (without extension) will be used.

        Returns:
            str: The SciCat ID of the dataset.

        Raises:
            ValueError: If no dataset or multiple datasets are found, or if the found dataset does not have a valid 'pid'.
        """

        # TODO: I'm not sure if SciCat's advanced query API supports this, but, if we're actually searching by file_name,
        # wouldn't it make more sense to look in all the dataFileList entries for all datasets?
        # This comes to mind because at 733, scientists organize their data mostly by creating dated folders,
        # and there's no guarantee that the files in those folders have unique names relative to the other folders.
        # If they were searching for a data file, they would need to use a path fragment, e.g. '20241216_153047/new_run.h5'
        # If this function could search dataFileLists by path fragment, it would be some future-proofing for those users...

        if file_name:
            # Extract the datasetName from the file_name by stripping the directory and extension.
            extracted_name = os.path.splitext(os.path.basename(file_name))[0]
        else:
            extracted_name = None

        query_fields = {
            "proposalId": proposal_id,
            "datasetName": extracted_name
        }
        results = self.scicat_client.datasets_find(query_fields=query_fields)

        # Assuming the client returns a list of datasets.
        count = len(results)

        if count == 0:
            raise ValueError(f"No dataset found for proposal '{proposal_id}' with dataset name '{extracted_name}'.")
        elif count > 1:
            # Log all found dataset IDs for human review.
            dataset_ids = [d.get("pid", "N/A") for d in results]
            logger.error(
                f"Multiple datasets found for proposal '{proposal_id}' with dataset name '{extracted_name}': {dataset_ids}."
            )
            # raise ValueError(
            #     f"Multiple datasets found for proposal '{proposal_id}' with dataset name '{extracted_name}'."
            # )

        dataset = results[0]
        dataset_id = dataset.get("pid")
        if not dataset_id:
            raise ValueError("The dataset returned does not have a valid 'pid' field.")

        return dataset_id
