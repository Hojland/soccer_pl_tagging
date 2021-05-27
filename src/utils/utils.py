"""This module contains various helper functions mostly for Python lists.
"""
import logging
import math
import os
import re
import shutil
import sys
import time
import warnings
from datetime import datetime, timedelta
from typing import List

import numpy as np
import pandas as pd
import pytz


def remove_contents_of_dir(dir_path):
    """Removes contencts folder directory recursively.
    Args:
        dir_path (str): The directory to be deleted.
    """
    # try:
    for filename in os.listdir(dir_path):
        file_path = os.path.join(dir_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            warnings.warn("Failed to delete %s. Reason: %s" % (file_path, e))


def get_logger(log_name: str = __name__):
    """Creates new logger.
    Args:
        model_name (str):
            Folder name for the logger to be saved in.
            Accepted values: 'ncf', 'implicit_model'
        model_dir (str): Name of the logger file.
    Returns:
        logger: Logger object.
    """

    def copenhagen_time(*args):
        """Computes and returns local time in Copenhagen.
        Returns:
            time.struct_time: Time converted to CEST.
        """
        _ = args  # to explicitly remove warning
        utc_dt = pytz.utc.localize(datetime.utcnow()) + timedelta(minutes=5, seconds=30)
        local_timezone = pytz.timezone("Europe/Copenhagen")
        converted = utc_dt.astimezone(local_timezone)
        return converted.timetuple()

    logging.Formatter.converter = copenhagen_time
    logger = logging.getLogger(log_name)
    if logger.hasHandlers():
        logger.handlers.clear()

    # To files
    fh = logging.FileHandler(log_name)
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fh)
    logger.setLevel(logging.INFO)

    # to std out
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def time_now(local_tz: pytz.timezone = None):
    if not local_tz:
        local_tz = pytz.timezone("Europe/Copenhagen")
    now = datetime.today().replace(tzinfo=pytz.utc).astimezone(tz=local_tz)
    return now


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if "log_time" in kw:
            name = kw.get("log_name", method.__name__.upper())
            kw["log_time"][name] = int((te - ts) * 1000)
        else:
            print("%r  %2.2f ms" % (method.__name__, (te - ts) * 1000))
        return result

    return timed


def dict_merge(*args):
    out_dct = {}
    for arg in args:
        out_dct.update(arg)
    return out_dct


def join_lsts_dct(*args: List[dict]):
    lens = [len(arg) for arg in args]
    assert len(set(lens)) == 1, "lists are not same length"
    lst = list(zip(*args))
    return lst
