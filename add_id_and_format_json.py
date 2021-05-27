#! /usr/bin/env python3

import hashlib
import json
import sys
import argparse
from pathlib import Path
from typing import List, Dict


parser = argparse.ArgumentParser(description="Add hash id to json")
parser.add_argument("--path", type=Path, nargs="+", help="file path or folder path to json object(s)")
args = parser.parse_args()
path = args.path[0]


def add_id(lst_dct: List[dict]):
    for dct in lst_dct:
        dct["id"] = hashlib.md5(str(dct).encode("utf-8")).hexdigest()
        if isinstance(dct["text"], list):
            dct["text"] = "\n\n".join(dct["text"])
    return lst_dct


def process_file(path: Path):
    try:
        lst_dct = [json.loads(line) for line in open(path, "r").read().split("\n") if line]
        lst_dct = add_id(lst_dct)
        # path.rename(path.with_suffix(".jl"))
        with open(path, "w") as jsonl_file:
            for entry in lst_dct:
                json.dump(entry, jsonl_file)
                jsonl_file.write("\n")
    except TypeError as e:
        print(f"failed for {str(path)} because of: {e}, which might be an already processed file")


if path.is_dir():
    for single_path in path.glob("*.jl"):
        process_file(single_path)


else:
    process_file(path)
