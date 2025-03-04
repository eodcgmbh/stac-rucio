from pystac import Asset, Item, ItemCollection
from rucio.client import Client
from rucio.client.downloadclient import DownloadClient
from urllib.parse import urlparse, urlunparse

from stac_rucio.models import RucioStac

class ReplicaExists(Exception):
    pass

class StacClient:
    def __init__(self):
        self.rucio = Client()
        self.downloader = DownloadClient()
        self.scheme = "https"

    def _ensure_port(self, href: str, port: int = 443):

        res = urlparse(href)
        new_netloc = res.netloc + f":{port}"

        return urlunparse(res._replace(netloc=new_netloc))

    def create_replicas(self, items: list, rse: str, targets: list[str] ):
        """Create replicas for stac items if they do not already exist at a non-deterministic RSE."""
        
        for item in items:
            # TODO Put this into it's own function and add exception handling for existing replicas.
            
            for target in targets:

                tmp = item.to_dict()
                config = RucioStac(**tmp["assets"][target]["rucio:config"])

                replicas = self.get_existing_replicas(tmp, rse)

                if not replicas:
                    self.rucio.add_replica(
                        rse=rse,
                        scope=self.rucio.account,
                        name=self.get_rucio_name(tmp, target),
                        pfn=self._ensure_port(tmp["assets"][target]["href"]),
                        bytes_=config.size,
                        adler32=config.adler32
                    )

        return True

    def create_replication_rules(self, items: list, dst_rse: str, targets: list[str]):
        """Create replication rules for stac items existing as a non-deterministic RSE to replication to a different rse location."""
        
        for item in items:

            for target in targets:

                tmp = item.to_dict()
                replicas = self.get_existing_replicas(tmp, dst_rse)

                if not replicas:
                    self.rucio.add_replication_rule(
                        dids=[{"scope": self.rucio.account, "name": self.get_rucio_name(tmp, target) }],
                        copies=1,
                        rse_expression=dst_rse
                    )

        return True

    def delete_replication_rules(self, items: list, rse: str, state: str = None):
        
        rules = self.get_items_replication_rules(items, rse, state)

        for rule in rules:
            self.rucio.delete_replication_rule(rule["id"])

        return True

    def download(self, item, target, rse):
        """Download from a specic RSE."""


        self.downloader.download_dids(
            items=[
                {
                    "did": f"{self.rucio.account}:{self.get_rucio_name(item, target)}",
                    "rse": rse,
                    "pfn": item.to_dict()['assets'][target]["alternate"][rse],
                }
            ]
        )

    def download_available(self, items: list, rse: str):

        available_names = self.get_items_replication_rules(items, rse, "OK")
        available_items = [ item for item in items if item.id in available_names ] 

        for item in available_items:
            self.download(item, rse)

        return True

    # TODO This should check is replication rules exist for this item and rse. Found using filters in list_replication_rules somewhat difficult.
    def get_existing_replicas(self, item: dict, target: str, rse: str = None):
        
        replicas = [ 
            rep for rep in self.rucio.list_replicas(
                dids=[{"scope": self.rucio.account, "name":  self.get_rucio_name(item, target)}],
                schemes=[
                    self.scheme,
                ],
                rse_expression=rse
            )
        ]

        if replicas:
            if not replicas[0]["rses"]:
                # Replicas found, but not for the rse expression
                return []

        return replicas

    def get_items_replication_rules(self, items: list, rse: str, state: str = None):
        """Determine existing replication rules for a given item and state, if provided. """

        possible_rucio_names = [ 
            self.get_rucio_name(item.to_dict(), key)
            for item in items
            for key, asset in item.assets.items()
            if "rucio:config" in asset.extra_fields
        ]
          
        rules = [ 
            x for x in self.rucio.list_replication_rules(filters={ "scope" : self.rucio.account, "rse_expression": rse })
            if x["name"] in possible_rucio_names and ( state is None or x["state"] == state )
        ]

        return rules
    
    def get_rucio_name(self, item, target):
        """ """

        return "{item_id}_{target}".format(item_id=item["id"], target=target)

    def replication_availability(self, items: list, rse: str):
        """Determine the number of available replications based off the replication rule state. """

        rules = self.get_items_replication_rules(items, rse)

        available = [ rule for rule in rules if rule["state"] == "OK"]
        stuck = [ rule for rule in rules if rule["state"] == "STUCK"]
        replicating = [ rule for rule in rules if rule["state"] == "REPLICATING"]

        return {
            "OK": len(available),
            "STUCK": len(stuck),
            "REPLICATING": len(replicating),
            "Total": len(rules)
        }

    def rucio_item(self, items: dict):
        """Take a stac item, and extend the assets with the files as available from Rucio."""

        for item in items["features"]:

            for asset_key, asset in item["assets"].items():

                if "rucio:config" in asset:

                    replicas = self.get_existing_replicas(item=item, target=asset_key)

                    for replica in replicas:
                        for key, value in replica["rses"].items():
                            # TODO For multiple targets, link asset to original asset.
                            if not "alternate" in item["assets"][asset_key]:
                                item["assets"][asset_key]["alternate"] = {}

                            item["assets"][asset_key]["alternate"][key] = value[0]

