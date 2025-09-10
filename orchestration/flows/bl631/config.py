from globus_sdk import TransferClient
from orchestration.globus import transfer


# TODO: Use BeamlineConfig base class (Waiting for PR #62 to be merged)
class Config631:
    def __init__(self) -> None:
        config = transfer.get_config()
        self.endpoints = transfer.build_endpoints(config)
        self.apps = transfer.build_apps(config)
        self.tc: TransferClient = transfer.init_transfer_client(self.apps["als_transfer"])
        self.data631 = self.endpoints["data631"]
        self.data631_raw = self.endpoints["data631_raw"]
        self.nersc631_alsdev_raw = self.endpoints["nersc631_alsdev_raw"]
