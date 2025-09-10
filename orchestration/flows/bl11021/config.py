from globus_sdk import TransferClient
from orchestration.globus import transfer


# TODO: Use BeamlineConfig base class (Waiting for PR #62 to be merged)
class Config11021:
    def __init__(self) -> None:
        config = transfer.get_config()
        self.endpoints = transfer.build_endpoints(config)
        self.apps = transfer.build_apps(config)
        self.tc: TransferClient = transfer.init_transfer_client(self.apps["als_transfer"])
        self.data11021 = self.endpoints["data11021"]
        self.data11021_raw = self.endpoints["data11021_raw"]
        self.nersc11021_alsdev_raw = self.endpoints["nersc11021_alsdev_raw"]
