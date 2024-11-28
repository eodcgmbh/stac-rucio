from pystac import Asset, Item, ItemCollection
from rucio.client import Client
from rucio.client.downloadclient import DownloadClient

from stac_rucio.models import RucioStac


class StacClient:
    def __init__(self):
        self.rucio = Client()
        self.rucio.whoami()
        self.downloader = DownloadClient()

    def rucio_item(self, items: dict):
        """Take a stac item, and extend the assets with the files as available from Rucio."""

        for item in items["features"]:
            config = RucioStac(**item["assets"]["mfcover"]["rucio:config"])

            replicas = [
                replica
                for replica in self.rucio.list_replicas(
                    dids=[{"scope": config.scope, "name": config.name}],
                    schemes=[
                        config.scheme,
                    ],
                )
            ]

            for replica in replicas:
                for key, value in replica["rses"].items():
                    item["assets"][key] = Asset(
                        href=value[0], title=f"Rucio Storage Element: {key}"
                    ).to_dict()

    def download(self, item, rse):
        """Download from a specic RSE."""

        config = RucioStac(**item.assets["mfcover"].extra_fields["rucio:config"])

        self.downloader.download_dids(
            items=[
                {
                    "did": f"{config.scope}:{config.name}",
                    "rse": rse,
                    "pfn": item.assets[rse].href,
                }
            ]
        )
