"""
This module contains helper functions that can be used during the data extraction process
""" 
import math
import re
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import IO, Any, Callable, Union
from pathlib import Path
import zipfile
import csv
import io
import json

import pandas as pd
import numpy as np


logger = logging.getLogger(__name__)

# Non-propagating logger for zip content enumeration.
# Contains PII (contact names in file paths). Inert by default —
# a developer must explicitly attach a handler in a debug session.
content_logger = logging.getLogger(f"{__name__}.content")
content_logger.propagate = False
content_logger.addHandler(logging.NullHandler())


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
                content_logger.debug("Contained in zip: %s", f)
                fp = Path(f)
                if fp.suffix == ".json":
                    b = io.BytesIO(zf.read(f))
                    d = dict_denester(read_json_from_bytes(b))
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


def epoch_to_iso(epoch_timestamp: str | int | float, errors: Counter | None = None) -> str:
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
    # Empty/falsy timestamps are expected absences, not errors
    if not epoch_timestamp and epoch_timestamp != 0:
        return ""

    out = str(epoch_timestamp)
    try:
        epoch_timestamp = int(float(epoch_timestamp))
        out = datetime.fromtimestamp(epoch_timestamp, tz=timezone.utc).isoformat()
    except (OverflowError, OSError, ValueError, TypeError) as e:
        logger.error("Could not convert epoch time timestamp, %s", e)
        if errors is not None:
            errors["TimestampParseError"] += 1

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


def extract_file_from_zip(zfile: str, file_to_extract: str, errors: Counter | None = None) -> io.BytesIO:
    """
    Extracts a specific file from a zipfile and returns it as a BytesIO buffer.

    Args:
        zfile (str): Path to the zip file.
        file_to_extract (str): Name or path of the file to extract from the zip.
        errors (Counter | None): Optional counter for aggregating error types.

    Returns:
        io.BytesIO: A BytesIO buffer containing the extracted file's content of the first file found.
                    Returns an empty BytesIO if the file is not found or an error occurs.
    """

    file_to_extract_bytes = io.BytesIO()

    try:
        with zipfile.ZipFile(zfile, "r") as zf:
            file_found = False

            for f in zf.namelist():
                content_logger.debug("Contained in zip: %s", f)
                if re.match(rf"^.*{re.escape(file_to_extract)}$", f):
                    file_to_extract_bytes = io.BytesIO(zf.read(f))
                    file_found = True
                    break

        if not file_found:
            raise FileNotFoundInZipError("File not found in zip")

    except zipfile.BadZipFile as e:
        logger.error("BadZipFile:  %s", e)
        if errors is not None:
            errors["BadZipFile"] += 1
    except FileNotFoundInZipError as e:
        logger.error("File not found:  %s: %s", file_to_extract, e)
        if errors is not None:
            errors["FileNotFoundInZipError"] += 1
    except Exception as e:
        logger.error("Exception was caught:  %s", e)
        if errors is not None:
            errors["Exception"] += 1

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


def _read_json(json_input: Any, json_reader: Callable[[Any, str], Any], errors: Counter | None = None) -> dict[Any, Any] | list[Any]:
    """
    Reads JSON input using the provided json_reader function, trying different encodings.
    This function should not be used directly.

    Args:
        json_input (Any): The JSON input (can be bytes or file path).
        json_reader (Callable[[Any, str], Any]): A function to read the JSON input.
        errors (Counter | None): Optional counter for aggregating error types.

    Returns:
        dict[Any, Any] | list[Any]: The parsed JSON data as a dictionary or list.
                                    Returns an empty dictionary if parsing fails.
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
            if errors is not None:
                errors["JSONDecodeError"] += 1
        except TypeError as e:
            logger.error("%s, could not convert json bytes", e)
            if errors is not None:
                errors["TypeError"] += 1
            break
        except Exception as e:
            logger.error("%s, could not convert json bytes", e)
            if errors is not None:
                errors["Exception"] += 1
            break

    return out


def read_json_from_bytes(json_bytes: io.BytesIO, errors: Counter | None = None) -> dict[Any, Any] | list[Any]:
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
        if not b:
            return out  # empty bytes → empty result, no parse attempt
        out = _read_json(b, _json_reader_bytes, errors=errors)
    except Exception as e:
        logger.error("%s, could not convert json bytes", e)
        if errors is not None:
            errors["Exception"] += 1

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


def read_csv_from_bytes(json_bytes: io.BytesIO, errors: Counter | None = None) -> list[dict[Any, Any]]:
    """
    Reads CSV data from a BytesIO buffer and returns it as a list of dictionaries.

    Args:
        json_bytes (io.BytesIO): A BytesIO buffer containing CSV data.
        errors (Counter | None): Optional counter for aggregating error types.

    Returns:
        list[dict[Any, Any]]: A list of dictionaries, where each dictionary represents a row in the CSV.
                              Returns an empty list if parsing fails.
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
        if errors is not None:
            errors["CSVDecodeError"] += 1

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


# --- Result types for ZipArchiveReader ---

@dataclass
class JsonExtractionResult:
    """Result of extracting and parsing a JSON file from a zip."""
    found: bool
    data: dict | list  # {} when not found
    member_path: str | None = None


@dataclass
class CsvExtractionResult:
    """Result of extracting and parsing a CSV file from a zip."""
    found: bool
    data: pd.DataFrame  # empty DataFrame when not found
    member_path: str | None = None


@dataclass
class RawExtractionResult:
    """Result of extracting raw bytes from a zip."""
    found: bool
    data: io.BytesIO  # empty BytesIO when not found
    member_path: str | None = None


class ZipArchiveReader:
    """Reads files from a zip archive using cached member inventory.

    Encapsulates the zip path or file-like, archive member list (from
    validation), and error counter. Provides json()/csv()/raw() methods
    with found/not-found signaling to eliminate cascading errors for
    expected-missing files.

    Per extraction/AD0007, the upload pipeline passes a file-like
    `AsyncFileAdapter` here directly so the zip is never materialized
    into Pyodide's heap. `zipfile.ZipFile` accepts both paths and
    seekable binary file-likes; the `zip_path` parameter and
    attribute name are retained for backwards compatibility with
    researcher-fork callers and will be renamed in PR 2.

    Usage:
        reader = ZipArchiveReader(zip_path, validation.archive_members, errors)
        result = reader.json("following.json")
        if result.found:
            data = result.data  # parsed dict/list
    """

    def __init__(
        self,
        zip_path: Union[str, IO[bytes]],
        archive_members: list[str],
        errors: Counter,
    ):
        self.zip_path = zip_path
        self.archive_members = archive_members
        self.errors = errors

    def resolve_member(self, filename: str) -> str | None:
        """Resolve a filename to an archive member path.

        Resolution rule:
        1. Exact path match → use it.
        2. Path-boundary suffix match (member.endswith("/" + filename)) →
           if exactly 1, use it.
        3. 0 matches → return None.
        4. Multiple matches → return None, log warning,
           increment errors["AmbiguousMemberMatch"].
        """
        # 1. Exact match
        if filename in self.archive_members:
            return filename

        # 2. Path-boundary suffix match
        matches = [m for m in self.archive_members if m.endswith("/" + filename)]

        if len(matches) == 1:
            return matches[0]
        elif len(matches) == 0:
            return None
        else:
            logger.warning(
                "Ambiguous member match: '%s' matched %d members in archive",
                filename, len(matches),
            )
            self.errors["AmbiguousMemberMatch"] += 1
            return None

    def _read_member_bytes(self, member_path: str) -> io.BytesIO:
        """Read a specific member from the zip by exact path."""
        try:
            with zipfile.ZipFile(self.zip_path, "r") as zf:
                return io.BytesIO(zf.read(member_path))
        except Exception as e:
            logger.error("Error reading zip member: %s", type(e).__name__)
            self.errors[type(e).__name__] += 1
            return io.BytesIO()

    def json(self, filename: str) -> JsonExtractionResult:
        """Extract and parse a JSON file.

        Returns JsonExtractionResult(found=False, data={}) if member
        not in archive. Skips JSON parsing entirely when not found.
        """
        member = self.resolve_member(filename)
        if member is None:
            return JsonExtractionResult(found=False, data={})

        b = self._read_member_bytes(member)
        raw = b.read()
        if not raw:
            return JsonExtractionResult(found=True, data={}, member_path=member)

        # Call _read_json directly (intentional — avoids BytesIO re-wrapping)
        data = _read_json(raw, _json_reader_bytes, errors=self.errors)
        return JsonExtractionResult(found=True, data=data, member_path=member)

    def json_all(self, pattern: str) -> list[JsonExtractionResult]:
        """Extract and parse all JSON files matching a regex pattern.

        Returns results sorted lexicographically by member path.
        Used for paginated exports (post_comments_1.json, _2.json, etc.).
        """
        matches = sorted(m for m in self.archive_members if re.search(pattern, m))
        results = []
        for member in matches:
            b = self._read_member_bytes(member)
            raw = b.read()
            if not raw:
                results.append(JsonExtractionResult(found=True, data={}, member_path=member))
                continue
            data = _read_json(raw, _json_reader_bytes, errors=self.errors)
            results.append(JsonExtractionResult(found=True, data=data, member_path=member))
        return results

    def csv(self, filename: str) -> CsvExtractionResult:
        """Extract and parse a CSV file.

        Returns CsvExtractionResult(found=False, data=pd.DataFrame())
        if member not in archive.
        """
        member = self.resolve_member(filename)
        if member is None:
            return CsvExtractionResult(found=False, data=pd.DataFrame())

        b = self._read_member_bytes(member)
        if not b.getvalue():
            return CsvExtractionResult(found=True, data=pd.DataFrame(), member_path=member)

        df = read_csv_from_bytes_to_df(b)
        return CsvExtractionResult(found=True, data=df, member_path=member)

    def raw(self, filename: str) -> RawExtractionResult:
        """Extract raw bytes from a zip member.

        Returns RawExtractionResult(found=False, data=io.BytesIO())
        if member not in archive. Used for HTML (Chrome bookmarks),
        text files (WhatsApp), and .js files (X — caller applies
        bytesio_to_listdict for JS prefix stripping).
        """
        member = self.resolve_member(filename)
        if member is None:
            return RawExtractionResult(found=False, data=io.BytesIO())

        b = self._read_member_bytes(member)
        return RawExtractionResult(found=True, data=b, member_path=member)
