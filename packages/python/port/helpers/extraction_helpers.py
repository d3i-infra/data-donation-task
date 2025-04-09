"""
This module contains helper functions that can be used during the data extraction process
""" 
import math
import re
import logging 
from datetime import datetime, timezone
from typing import Any, Callable
from pathlib import Path
import zipfile
import csv
import io
import json

import pandas as pd
import numpy as np


logger = logging.getLogger(__name__)


def dict_denester(inp: dict[Any, Any] | list[Any], new: dict[Any, Any] | None = None, name: str = "", run_first: bool = True) -> dict[Any, Any]:
    """
    Denests a dictionary or list, returning a new flattened dictionary.

    Args:
        inp (dict[Any, Any] | list[Any]): The input dictionary or list to be denested.
        new (dict[Any, Any] | None, optional): The dictionary to store denested key-value pairs. Defaults to None.
        name (str, optional): The current key name in the denesting process. Defaults to "".
        run_first (bool, optional): Flag to indicate if this is the first run of the function. Defaults to True.

    Returns:
        dict[Any, Any]: A new denested dictionary.

    Examples::

        >>> nested_dict = {"a": {"b": {"c": 1}}, "d": [2, 3]}
        >>> dict_denester(nested_dict)
        {"a-b-c": 1, "d-0": 2, "d-1": 3}
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


def find_item(d: dict[Any, Any], key_to_match: str) -> str:
    """
    Finds the least nested value in a denested dictionary whose key contains the given key_to_match.

    Args:
        d (dict[Any, Any]): A denested dictionary to search in.
        key_to_match (str): The substring to match in the keys.

    Returns:
        str: The value of the least nested key containing key_to_match.
             Returns an empty string if no match is found.

    Raises:
        Exception: Logs an error message if an exception occurs during the search.

    Examples::

        >>> d = {"asd-asd-asd": 1, "asd-asd": 2, "qwe": 3}
        >>> find_item(d, "asd")
        "2"
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
        logger.error(e)

    return out


def find_items(d: dict[Any, Any], key_to_match: str) -> list:
    """
    Finds all values in a denested dictionary whose keys contain the given key_to_match.

    Args:
        d (dict[Any, Any]): A denested dictionary to search in.
        key_to_match (str): The substring to match in the keys.

    Returns:
        list: A list of all values whose keys contain key_to_match.

    Raises:
        Exception: Logs an error message if an exception occurs during the search.

    Examples::

        >>> d = {"asd-1": "a", "asd-2": "b", "qwe": "c"}
        >>> find_items(d, "asd")
        ["a", "b"]
    """
    out = []
    pattern = r"{}".format(f"^.*{key_to_match}.*$")

    try:
        for k, v in d.items():
            if re.match(pattern, k):
                out.append(str(v))
    except Exception as e:
        logger.error("bork bork: %s", e)

    return out


def json_dumper(zfile: str) -> pd.DataFrame:
    """
    Reads all JSON files in a zip file, flattens them, and combines them into a single DataFrame.

    Args:
        zfile (str): Path to the zip file containing JSON files.

    Returns:
        pd.DataFrame: A DataFrame containing flattened data from all JSON files in the zip.

    Raises:
        Exception: Logs an error message if an exception occurs during the process.

    Examples::

        >>> df = json_dumper("data.zip")
        >>> print(df.head())
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
    Fixes the string encoding by removing non-ASCII characters.

    Args:
        input (str): The input string that needs to be fixed.

    Returns:
        str: The fixed string with only ASCII characters, or the original string if an exception occurs.

    Examples::

        >>> fix_ascii_string("Hello, 世界!")
        "Hello, !"
    """
    try:
        fixed_string = input.encode("ascii", 'ignore').decode()
        return fixed_string
    except Exception:
        return input


def replace_months(input_string: str) -> str:
    """
    Replaces Dutch month abbreviations with English equivalents in the input string.

    Args:
        input_string (str): The input string containing potential Dutch month abbreviations.

    Returns:
        str: The input string with Dutch month abbreviations replaced by English equivalents.

    Examples::

        >>> replace_months("15 mei 2023")
        "15 may 2023"
    """

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


def epoch_to_iso(epoch_timestamp: str | int | float) -> str:
    """
    Convert epoch timestamp to an ISO 8601 string, assuming UTC.

    Args:
        epoch_timestamp (str | int): The epoch timestamp to convert.

    Returns:
        str: The ISO 8601 formatted string, or the original input if conversion fails.

    Raises:
        Exception: Logs an error message if conversion fails.

    Examples::

        >>> epoch_to_iso(1632139200)
        "2021-09-20T12:00:00+00:00"
    """
    out = str(epoch_timestamp)
    try:
        epoch_timestamp = int(float(epoch_timestamp)) 
        out = datetime.fromtimestamp(epoch_timestamp, tz=timezone.utc).isoformat()
    except (OverflowError, OSError, ValueError, TypeError) as e:
        logger.error("Could not convert epoch time timestamp, %s", e)

    return out


def sort_isotimestamp_empty_timestamp_last(timestamp_series: pd.Series) -> pd.Series:
    """
    Creates a key for sorting a pandas Series of ISO timestamps, placing empty timestamps last.

    Args:
        timestamp_series (pd.Series): A pandas Series containing ISO formatted timestamps.

    Returns:
        pd.Series: A Series of sorting keys, with -timestamp for valid dates and infinity for invalid/empty dates.

    Examples::

        >>> df = df.sort_values(by="Date", key=sort_isotimestamp_empty_timestamp_last)
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

    Examples::

        >>> fix_latin1_string("café")
        "café"
    """
    try:
        fixed_string = input.encode("latin1").decode()
        return fixed_string
    except Exception:
        return input


class FileNotFoundInZipError(Exception):
    """
    The File you are looking for is not present in a zipfile
    """


def extract_file_from_zip(zfile: str, file_to_extract: str) -> io.BytesIO:
    """
    Extracts a specific file from a zipfile and returns it as a BytesIO buffer.

    Args:
        zfile (str): Path to the zip file.
        file_to_extract (str): Name or path of the file to extract from the zip.

    Returns:
        io.BytesIO: A BytesIO buffer containing the extracted file's content of the first file found.
                    Returns an empty BytesIO if the file is not found or an error occurs.

    Raises:
        FileNotFoundInZipError: Logs an error if the specified file is not found in the zip.
        zipfile.BadZipFile: Logs an error if the zip file is invalid.
        Exception: Logs any other unexpected errors.

    Examples::

        >>> extracted_file = extract_file_from_zip("archive.zip", "data.txt")
        >>> content = extracted_file.getvalue().decode('utf-8')
    """

    file_to_extract_bytes = io.BytesIO()

    try:
        with zipfile.ZipFile(zfile, "r") as zf:
            file_found = False

            for f in zf.namelist():
                logger.debug("Contained in zip: %s", f)
                if re.match(rf"^.*{re.escape(file_to_extract)}$", f):
                    file_to_extract_bytes = io.BytesIO(zf.read(f))
                    file_found = True
                    break

        if not file_found:
            raise FileNotFoundInZipError("File not found in zip")

    except zipfile.BadZipFile as e:
        logger.error("BadZipFile:  %s", e)
    except FileNotFoundInZipError as e:
        logger.error("File not found:  %s: %s", file_to_extract, e)
    except Exception as e:
        logger.error("Exception was caught:  %s", e)

    finally:
        return file_to_extract_bytes


def _json_reader_bytes(json_bytes: bytes, encoding: str) -> Any:
    """
    Reads JSON data from bytes using the specified encoding.
    This function should not be used directly.

    Args:
        json_bytes (bytes): The JSON data in bytes.
        encoding (str): The encoding to use for decoding the bytes.

    Returns:
        Any: The parsed JSON data.

    Examples:
        >>> data = _json_reader_bytes(b'{"key": "value"}', "utf-8")
        >>> print(data)
        {'key': 'value'}
    """
    json_str = json_bytes.decode(encoding)
    result = json.loads(json_str)
    return result


def _json_reader_file(json_file: str, encoding: str) -> Any:
    """
    Reads JSON data from a file using the specified encoding.
    This function should not be used directly.

    Args:
        json_file (str): Path to the JSON file.
        encoding (str): The encoding to use for reading the file.

    Returns:
        Any: The parsed JSON data.

    Examples::

        >>> data = _json_reader_file("data.json", "utf-8")
        >>> print(data)
        {'key': 'value'}
    """
    with open(json_file, 'r', encoding=encoding) as f:
        result = json.load(f)
    return result


def _read_json(json_input: Any, json_reader: Callable[[Any, str], Any]) -> dict[Any, Any] | list[Any]:
    """
    Reads JSON input using the provided json_reader function, trying different encodings.
    This function should not be used directly.

    Args:
        json_input (Any): The JSON input (can be bytes or file path).
        json_reader (Callable[[Any, str], Any]): A function to read the JSON input.

    Returns:
        dict[Any, Any] | list[Any]: The parsed JSON data as a dictionary or list.
                                    Returns an empty dictionary if parsing fails.

    Raises:
        TypeError: Logs an error if the parsed result is not a dict or list.
        json.JSONDecodeError: Logs an error if JSON decoding fails.
        Exception: Logs any other unexpected errors.

    Examples::

        >>> data = _read_json(b'{"key": "value"}', _json_reader_bytes)
        >>> print(data)
        {'key': 'value'}
    """

    out: dict[Any, Any] | list[Any] = {}

    encodings = ["utf8", "utf-8-sig"]
    for encoding in encodings:
        try:
            result = json_reader(json_input, encoding)

            if not isinstance(result, (dict, list)):
                raise TypeError("Did not convert bytes to a list or dict, but to another type instead")

            out = result
            logger.debug("Succesfully converted json bytes with encoding: %s", encoding)
            break

        except json.JSONDecodeError:
            logger.error("Cannot decode json with encoding: %s", encoding)
        except TypeError as e:
            logger.error("%s, could not convert json bytes", e)
            break
        except Exception as e:
            logger.error("%s, could not convert json bytes", e)
            break

    return out


def read_json_from_bytes(json_bytes: io.BytesIO) -> dict[Any, Any] | list[Any]:
    """
    Reads JSON data from a BytesIO buffer.

    Args:
        json_bytes (io.BytesIO): A BytesIO buffer containing JSON data.

    Returns:
        dict[Any, Any] | list[Any]: The parsed JSON data as a dictionary or list.
                                    Returns an empty dictionary if parsing fails.

    Examples::

        >>> buffer = io.BytesIO(b'{"key": "value"}')
        >>> data = read_json_from_bytes(buffer)
        >>> print(data)
        {'key': 'value'}
    """
    out: dict[Any, Any] | list[Any] = {}
    try:
        b = json_bytes.read()
        out = _read_json(b, _json_reader_bytes)
    except Exception as e:
        logger.error("%s, could not convert json bytes", e)

    return out


def read_json_from_file(json_file: str) -> dict[Any, Any] | list[Any]:
    """
    Reads JSON data from a file.

    Args:
        json_file (str): Path to the JSON file.

    Returns:
        dict[Any, Any] | list[Any]: The parsed JSON data as a dictionary or list.
                                    Returns an empty dictionary if parsing fails.

    Examples::

        >>> data = read_json_from_file("data.json")
        >>> print(data)
        {'key': 'value'}
    """
    out = _read_json(json_file, _json_reader_file)
    return out


def read_csv_from_bytes(json_bytes: io.BytesIO) -> list[dict[Any, Any]]:
    """
    Reads CSV data from a BytesIO buffer and returns it as a list of dictionaries.

    Args:
        json_bytes (io.BytesIO): A BytesIO buffer containing CSV data.

    Returns:
        list[dict[Any, Any]]: A list of dictionaries, where each dictionary represents a row in the CSV.
                              Returns an empty list if parsing fails.

    Examples:

        >>> buffer = io.BytesIO(b'name,age\\nAlice,30\\nBob,25')
        >>> data = read_csv_from_bytes(buffer)
        >>> print(data)
        [{'name': 'Alice', 'age': '30'}, {'name': 'Bob', 'age': '25'}]
    """
    out: list[dict[Any, Any]] = []

    try:
        stream = io.TextIOWrapper(json_bytes, encoding="utf-8")
        reader = csv.DictReader(stream)
        for row in reader:
            out.append(row)
        logger.debug("succesfully converted csv bytes with encoding utf8")

    except Exception as e:
        logger.error("%s, could not convert csv bytes", e)

    finally:
        return out


def read_csv_from_bytes_to_df(json_bytes: io.BytesIO) -> pd.DataFrame:
    """
    Reads CSV data from a BytesIO buffer and returns it as a pandas DataFrame.

    Args:
        json_bytes (io.BytesIO): A BytesIO buffer containing CSV data.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the CSV data.

    Examples:

        >>> buffer = io.BytesIO(b'name,age\\nAlice,30\\nBob,25')
        >>> df = read_csv_from_bytes_to_df(buffer)
        >>> print(df)
           name  age
        0  Alice   30
        1    Bob   25
    """
    return pd.DataFrame(read_csv_from_bytes(json_bytes))
