import ssdeep
import requests
import datasketch

from typing import Dict, List, Any
from karton.core import Karton, Task, Config


class AuroraConfig(Config):
    def __init__(self, path=None) -> None:
        super().__init__(path)
        self.aurora_config = dict(self.config.items("aurora"))


class Similarity(Karton):
    identity = "karton.similarity"
    filters = [
        {"type": "feature", "stage": "minhash"},
        {"type": "feature", "stage": "ssdeep"},
    ]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.minhash_lsh_dict = {}

    def process(self, task: Task) -> None:
        if task.headers["stage"] == "minhash":
            self.process_minhash(task)
        elif task.headers["stage"] == "ssdeep":
            self.process_ssdeep(task)

    def process_minhash(self, task: Task) -> None:
        sha256 = task.get_payload("sha256")
        seed = task.get_payload("seed")
        hash_values = task.get_payload("hash_values")
        minhash_type = task.headers["kind"]

        minhash = datasketch.LeanMinHash(seed=seed, hashvalues=hash_values)
        if minhash_type not in self.minhash_lsh_dict.keys():
            self.add_minhash_lsh(minhash_type)

        try:
            self.minhash_lsh_dict[minhash_type].insert(sha256, minhash)
        except ValueError as e:
            self.log.warning(f"Could not insert Minhash to LSH: {e}")

        lsh_sha256_list = self.minhash_lsh_dict[minhash_type].query(minhash)

        self.process_minhash_candidates(
            sha256, minhash, lsh_sha256_list, minhash_type
        )

    def process_minhash_candidates(
        self,
        sample_sha256: str,
        sample_minhash: datasketch.LeanMinHash,
        candidates_sha256: List[str],
        minhash_type: str,
    ) -> None:

        for candidate_sha256 in candidates_sha256:
            if candidate_sha256 == sample_sha256:
                continue

            candidate_minhash_info = self.get_sample_minhash(
                candidate_sha256, minhash_type
            )[0]

            candidate_minhash = datasketch.LeanMinHash(
                seed=candidate_minhash_info["seed"],
                hashvalues=candidate_minhash_info["hash_values"],
            )

            jaccard_coefficient = sample_minhash.jaccard(candidate_minhash)
            if jaccard_coefficient > 0.5:
                self.add_relation(
                    sample_sha256,
                    candidate_sha256,
                    jaccard_coefficient,
                    minhash_type,
                )

    def add_minhash_lsh(self, minhash_type: str) -> None:
        self.minhash_lsh_dict[minhash_type] = datasketch.MinHashLSH(
            threshold=0.5,
            num_perm=256,
            storage_config={
                "type": "redis",
                "basename": minhash_type.encode("UTF-8"),
                "redis": {"host": "redis", "port": 6379},
            },
        )

    def add_relation(
        self,
        sample_sha256: str,
        related_sha256: str,
        confidence: float,
        relation_type: str,
    ) -> None:
        relation_payload = {
            "parent_sha256": sample_sha256,
            "child_sha256": related_sha256,
            "type": relation_type,
            "confidence": confidence,
        }

        try:
            requests.post(f"{self.config.aurora_config['url']}/relation/", json=relation_payload)
        except requests.RequestException as e:
            self.log.error(
                f"Post request to relation failed with {e}. \nRelation payload: {relation_payload}"
            )

    def get_sample_minhash(self, sha256: str, minhash_type: str) -> Dict[str, Any]:
        try:
            r = requests.get(
                f"{self.config.aurora_config['url']}/sample/{sha256}/minhash?minhash_type={minhash_type}"
            )
        except requests.RequestException as e:
            self.log.error(
                f"Get request for sample minhash {minhash_type} failed with {e}. \nSample: {sha256}"
            )

        return r.json()

    def process_ssdeep(self, task: Task) -> None:
        sha256 = task.get_payload("sha256")
        chunksize = task.get_payload("chunksize")
        ssdeep_hash = task.get_payload("ssdeep")

        ssdeep_data_list = self.get_ssdeep_hashes(chunksize)

        for ssdeep_data in ssdeep_data_list:
            if ssdeep_data["sample"]["sha256"] == sha256:
                continue

            ssdeep_coefficient = (
                ssdeep.compare(ssdeep_hash, ssdeep_data["ssdeep"]) / 100.0
            )

            if ssdeep_coefficient > 0.5:
                self.add_relation(
                    sha256,
                    ssdeep_data["sample"]["sha256"],
                    ssdeep_coefficient,
                    "ssdeep",
                )

    def get_ssdeep_hashes(self, chunksize: int) -> Dict[str, Any]:
        try:
            r = requests.get(
                f"{self.config.aurora_config['url']}/ssdeep?chunksize={chunksize}"
            )
        except requests.RequestException as e:
            self.log.error(
                f"Get request for ssdeep hashes failed with {e}. \nChunksize: {chunksize}"
            )

        return r.json()
