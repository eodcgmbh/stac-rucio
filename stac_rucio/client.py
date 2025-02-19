from pystac import Asset, Item, ItemCollection
from rucio.client import Client
from rucio.client.downloadclient import DownloadClient
from urllib.parse import urlparse, urlunparse

from stac_rucio.models import RucioStac

class ReplicaExists(Exception):
    pass

class StacClient:
    def __init__(self, target):
        self.rucio = Client()
        self.rucio.whoami()
        self.downloader = DownloadClient()
        # TODO Should handle a list of targets
        self.target = target

    def _ensure_port(self, href: str, port: int = 443):

        res = urlparse(href)
        new_netloc = res.netloc + f":{port}"

        return urlunparse(res._replace(netloc=new_netloc))

    # TODO This should check is replication rules exist for this item and rse. Found using filters in list_replication_rules somewhat difficult.
    def _get_existing_replicas(self, item: dict, rse: str = None):
        
        config = RucioStac(**item['assets'][self.target]["rucio:config"])

        replicas = [ 
            rep for rep in self.rucio.list_replicas(
                dids=[{"scope": self.rucio.account, "name": config.name}],
                schemes=[
                    config.scheme,
                ],
                rse_expression=rse
            )
        ]

        if replicas:
            if not replicas[0]["rses"]:
                # Replicas found, but not for the rse expression
                return []

        return replicas

    def rucio_item(self, items: dict):
        """Take a stac item, and extend the assets with the files as available from Rucio."""

        for item in items["features"]:

            replicas = self._get_existing_replicas(item)

            for replica in replicas:
                for key, value in replica["rses"].items():
                    # TODO For multiple targets, link asset to original asset.
                    item["assets"][target]["alternate"][key] = value[0]

    def create_replicas(self, items: list, src_rse: str):
        """Create replicas for stac items if they do not already exist at a non-deterministic RSE."""
        
        for item in items:
            # TODO Put this into it's own function and add exception handling for existing replicas.

            tmp = item.to_dict()
            config = RucioStac(**tmp["assets"][self.target]["rucio:config"])

            replicas = self._get_existing_replicas(tmp, src_rse)

            if not replicas:
                self.rucio.add_replica(
                    rse=src_rse,
                    scope=self.rucio.account,
                    name=config.name,
                    pfn=self._ensure_port(tmp["assets"][self.target]["href"]),
                    bytes_=config.size,
                    adler32=config.adler32
                )

        return True

    def create_replication_rules(self, items: list, src_rse: str, dst_rse: str):
        """Create replication rules for stac items existing as a non-deterministic RSE to replication to a different rse location."""
        
        for item in items:
            # TODO Put this into it's own function and add exception handling for existing replication rules.

            tmp = item.to_dict()
            config = RucioStac(**tmp["assets"][self.target]["rucio:config"])

            replicas = self._get_existing_replicas(tmp, dst_rse)

            if not replicas:
                self.rucio.add_replication_rule(
                    dids=[{"scope": self.rucio.account, "name": config.name}],
                    copies=1,
                    rse_expression=dst_rse
                )

        return True

    def replication_availability(self, items: list, rse: str):
        """Determine the number of available replications based off the replication rule state. """

        item_ids = [ item.id for item in items ]

        rules = [ x for x in self.rucio.list_replication_rules(filters={"scope":self.rucio.account}) if x["name"] in item_ids]

        available = [ rule for rule in rules if rule["state"] == "OK"]
        stuck = [ rule for rule in rules if rule["state"] == "STUCK"]
        replicating = [ rule for rule in rules if rule["state"] == "REPLICATING"]

        return {
            "OK": len(available),
            "STUCK": len(stuck),
            "REPLICATING": len(replicating),
            "Total": len(rules)
        }


    def download(self, item, rse):
        """Download from a specic RSE."""

        config = RucioStac(**item.assets[self.target].extra_fields["rucio:config"])

        self.downloader.download_dids(
            items=[
                {
                    "did": f"{self.rucio.account}:{config.name}",
                    "rse": rse,
                    "pfn": item.assets[self.target]["alternate"][rse],
                }
            ]
        )

    def get_available_names(self, items: list, rse: str):

        item_ids = [ item.id for item in items ]
        rules = [ 
            rule["name"] for rule in self.rucio.list_replication_rules(filters={"scope":self.rucio.account}) if rule["name"] in item_ids and rule["state"] == "OK"
        ]

        return rules


    def download_available(self, items: list, rse: str):

        available_names = self.get_available_names(items, rse)
        available_items = [ item for item in items if item.id in available_names ] 

        print("Available: ", len(available_items))
        for item in available_items:
            self.download(item, rse)

        return True
