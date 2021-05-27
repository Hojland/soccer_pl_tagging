from typing import List
import json
import hashlib
from pathlib import Path
import os
from boto3 import client
import sqlite3

from utils import utils

logger = utils.get_logger(f"{__name__}.log")


def download_dir(prefix: str, local: Path, bucket: str, s3_client: client):
    """
    params:
    - prefix: pattern to match in s3
    - local: local path to folder in which to place files
    - bucket: s3 bucket with target contents
    - s3_client: initialized s3 client object
    """
    keys = {}
    next_token = ""
    base_kwargs = {
        "Bucket": bucket,
        "Prefix": prefix,
    }
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != "":
            kwargs.update({"ContinuationToken": next_token})
        results = s3_client.list_objects_v2(**kwargs)
        contents = results.get("Contents")
        for i in contents:
            foreign_key = i.get("Key")
            local_key = local / Path(foreign_key.lstrip(prefix + "/"))
            keys[foreign_key] = local_key
        next_token = results.get("NextContinuationToken")

    for foreign_key, local_key in keys.items():
        s3_client.download_file(bucket, foreign_key, str(local_key))
        logger.info(f"downloaded {str(foreign_key)}")


def process_file(path: Path):
    def add_id(lst_dct: List[dict]):
        for dct in lst_dct:
            dct["id"] = hashlib.md5(str(dct).encode("utf-8")).hexdigest()
            if "text" in dct.keys():
                if isinstance(dct["text"], list):
                    dct["text"] = "\n\n".join(dct["text"])
        return lst_dct

    try:
        lst_dct = [json.loads(line) for line in open(path, "r").read().split("\n") if line]
        lst_dct = add_id(lst_dct)
        # path.rename(path.with_suffix(".jl"))
        with open(path, "w") as jsonl_file:
            for entry in lst_dct:
                json.dump(entry, jsonl_file)
                jsonl_file.write("\n")
    except TypeError as e:
        logger.info(f"failed for {str(path)} because of: {e}, which might be an already processed file")


class kvstore(dict):
    def __init__(self, filename=None, reset=False):
        self.conn = sqlite3.connect(filename)
        if reset:
            self.del_table()
        self.conn.execute("CREATE TABLE IF NOT EXISTS kv (key text unique, value timestamp)")

    def close(self):
        self.conn.commit()
        self.conn.close()

    def del_table(self):
        self.conn.execute("DROP TABLE [IF EXISTS] kv")

    def __len__(self):
        rows = self.conn.execute("SELECT COUNT(*) FROM kv").fetchone()[0]
        return rows if rows is not None else 0

    def iterkeys(self):
        c = self.conn.cursor()
        for row in self.conn.execute("SELECT key FROM kv"):
            yield row[0]

    def itervalues(self):
        c = self.conn.cursor()
        for row in c.execute("SELECT value FROM kv"):
            yield row[0]

    def iteritems(self):
        c = self.conn.cursor()
        for row in c.execute("SELECT key, value FROM kv"):
            yield row[0], row[1]

    def keys(self):
        return list(self.iterkeys())

    def values(self):
        return list(self.itervalues())

    def items(self):
        return list(self.iteritems())

    def __contains__(self, key):
        return self.conn.execute("SELECT 1 FROM kv WHERE key = ?", (key,)).fetchone() is not None

    def __getitem__(self, key):
        item = self.conn.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
        if item is None:
            raise KeyError(key)
        return item[0]

    def __setitem__(self, key, value):
        self.conn.execute("REPLACE INTO kv (key, value) VALUES (?,?)", (key, value))
        self.conn.commit()

    def __delitem__(self, key):
        if key not in self:
            raise KeyError(key)
        self.conn.execute("DELETE FROM kv WHERE key = ?", (key,))
        self.conn.commit()

    def __iter__(self):
        return self.iterkeys()

    def _get_key(self, id: str, hash: bool = False):
        if hash:
            id = hashlib.sha1(id).hexdigest()
        return id

    def _get_val(self):
        return utils.time_now()