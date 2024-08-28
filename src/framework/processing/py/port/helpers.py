import math
import re
import logging 
from datetime import datetime, timezone
from typing import Any
from pathlib import Path
import zipfile
import io

import pandas as pd
import numpy as np
from dateutil.parser import parse

import port.unzipddp as unzipddp

logger = logging.getLogger(__name__)


def convert_unix_timestamp(timestamp: str) -> str:
    out = timestamp
    try:
        out = datetime.fromtimestamp(float(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(e)

    return  out


def dict_denester(
    inp: dict[Any, Any] | list[Any],
    new: dict[Any, Any] | None = None,
    name: str = "",
    run_first: bool = True,
) -> dict[Any, Any]:
    """
    Denest a dict or list, returns a new denested dict
    """

    if run_first:
        new = {}

    if isinstance(inp, dict):
        for k, v in inp.items():
            if isinstance(v, (dict, list)):
                dict_denester(v, new, f"{name}-{str(k)}", run_first=False)
            else:
                newname = f"{name}-{k}"
                new.update({newname[1:]: v})  # type: ignore

    elif isinstance(inp, list):
        for i, item in enumerate(inp):
            dict_denester(item, new, f"{name}-{i}", run_first=False)

    else:
        new.update({name[1:]: inp})  # type: ignore

    return new  # type: ignore



def find_item(d: dict[Any, Any],  key_to_match: str) -> str:
    """
    d is a denested dict
    match all keys in d that contain key_to_match

    return the value beloning to that key that is the least nested
    In case of no match return empty string

    example:
    key_to_match = asd

    asd-asd-asd-asd-asd-asd: 1
    asd-asd: 2
    qwe: 3

    returns 2

    This function is needed because your_posts_1.json contains a wide variety of nestedness per post
    """
    out = ""
    pattern = r"{}".format(f"^.*{key_to_match}.*$")
    depth = math.inf

    try:
        for k, v in d.items():
            if re.match(pattern, k):
                depth_current_match = k.count("-")
                if depth_current_match < depth:
                    depth = depth_current_match
                    out = str(v)
    except Exception as e:
        logger.error("bork bork: %s", e)

    return out



def find_items(d: dict[Any, Any],  key_to_match: str) -> list:
    """
    d is a denested dict
    find all items in a denested dict return list
    """
    out = []
    pattern = r"{}".format(f"^.*{key_to_match}.*$")
    depth = math.inf

    try:
        for k, v in d.items():
            if re.match(pattern, k):
                out.append(str(v))
    except Exception as e:
        logger.error("bork bork: %s", e)

    return out


def json_dumper(zfile: str) -> pd.DataFrame:
    """
    Reads all json files in zip, flattens them, and put them in a big df
    """
    out = pd.DataFrame()
    datapoints = []
    try:
        with zipfile.ZipFile(zfile, "r") as zf:
            for f in zf.namelist():
                logger.debug("Contained in zip: %s", f)
                fp = Path(f)
                if fp.suffix == ".json":
                    b = io.BytesIO(zf.read(f))
                    d = dict_denester(unzipddp.read_json_from_bytes(b))
                    for k, v in d.items():
                        datapoints.append({
                            "file name": fp.name, 
                            "key": k,
                            "value": v
                        })

        out = pd.DataFrame(datapoints)

    except Exception as e:
        logger.error("Exception was caught:  %s", e)

    return out


def fix_ascii_string(input: str) -> str:
    """
    Fixes the string encoding by attempting to encode it ignoring all ascii characters and then decoding it.

    Args:
        input (str): The input string that needs to be fixed.

    Returns:
        str: The fixed string after encoding and decoding, or the original string if an exception occurs.
    """
    try:
        fixed_string = input.encode("ascii", 'ignore').decode()
        return fixed_string
    except Exception:
        return input


def replace_months(input_string):

    month_mapping = {
        'mrt': 'mar',
        'mei': 'may',
        'okt': 'oct',
    }

    for dutch_month, english_month in month_mapping.items():
        if dutch_month in input_string:
            replaced_string = input_string.replace(dutch_month, english_month, 1)
            return replaced_string

    return input_string


def try_to_convert_any_timestamp_to_iso8601(timestamp: str) -> str:
    """
    WARNING 

    Use this function with caution and only as a last resort
    Conversion can go wrong when datetime formats are ambiguous
    When ambiguity occurs it chooses MM/DD instead of DD/MM

    Checkout: dateutil.parsers parse
    """
    timestamp = replace_months(timestamp)
    try:
       timestamp = parse(timestamp, dayfirst=False).isoformat()
    except Exception as e:
        timestamp = ""
    return timestamp


def epoch_to_iso(epoch_timestamp: str | int) -> str:
    """
    Convert epoch timestamp to an ISO 8601 string. Assumes UTC.
    """

    out = str(epoch_timestamp)
    try:
        epoch_timestamp = int(epoch_timestamp)
        out = datetime.fromtimestamp(epoch_timestamp, tz=timezone.utc).isoformat()
    except (OverflowError, OSError, ValueError, TypeError) as e:
        logger.error("Could not convert epoch time timestamp, %s", e)

    return out


def sort_isotimestamp_empty_timestamp_last(timestamp_series: pd.Series) -> pd.Series:
    """
    Can be used as follows:

    df = df.sort_values(by="Date", key=sort_isotimestamp_empty_timestamp_last)
    """

    def convert_timestamp(timestamp):
        out = np.inf
        try:
            if isinstance(timestamp, str) and len(timestamp) > 0:
                dt = datetime.fromisoformat(timestamp)
                out = -dt.timestamp()
        except Exception as e:
            logger.debug("Cannot convert timestamp: %s", e)

        return out

    return timestamp_series.apply(convert_timestamp)


def fix_latin1_string(input: str) -> str:
    """
    Fixes the string encoding by attempting to encode it using the 'latin1' encoding and then decoding it.

    Args:
        input (str): The input string that needs to be fixed.

    Returns:
        str: The fixed string after encoding and decoding, or the original string if an exception occurs.
    """
    try:
        fixed_string = input.encode("latin1").decode()
        return fixed_string
    except Exception:
        return input



def split_dataframe(df: pd.DataFrame, chunk_size: int) -> list[pd.DataFrame]:
    """
    Returns a list with dfs with n rows equal to chunk_size
    """
    chunks = []
    num_chunks = len(df) // chunk_size + 1

    for i in range(num_chunks):
        start = i * chunk_size
        end = min((i + 1) * chunk_size, len(df))
        chunks.append(df[start:end].reset_index(drop=True))

    return chunks

