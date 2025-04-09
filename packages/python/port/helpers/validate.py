"""
Contains classes to deal with input validation of DDPs

The idea of this module is to provide a uniform way to assign a validation status to a DDP validation
Which can be used and acted upon
"""

from dataclasses import dataclass, field
from pathlib import Path
from enum import Enum
import zipfile

import logging

logger = logging.getLogger(__name__)


class Language(Enum):
    """
    Enumeration of supported languages.
    """
    EN = 1
    NL = 2
    UNKNOWN = 3


class DDPFiletype(Enum):
    """
    Enumeration of supported DDP file types.
    """
    JSON = 1
    HTML = 2
    CSV = 3
    TXT = 4
    UNKOWN = 5


@dataclass
class DDPCategory:
    """
    Represents characteristics that define a DDP (Data Delivery Package) category.

    Args:
        id (str): Unique identifier for the DDP category.
        ddp_filetype (DDPFiletype): The file type of the DDP.
        language (Language): The language of the DDP.
        known_files (List[str]): A list of known files associated with this DDP category.

    Examples:
        >>> category = DDPCategory("cat1", DDPFiletype.JSON, Language.EN, ["file1.json", "file2.json"])
        >>> print(category.id)
        cat1
        >>> print(category.language)
        <Language.EN: 1>
    """
    id: str 
    ddp_filetype: DDPFiletype 
    language: Language 
    known_files: list[str] 


@dataclass
class StatusCode:
    """
    Represents a status code that can be used to set a DDP status.

    Args:
        id (int): The numeric identifier of the status code.
        description (str): A brief description of what the status code represents.

    Examples:
        >>> status = StatusCode(0, "Success")
        >>> print(status.id)
        0
        >>> print(status.description)
        Success
    """
    id: int
    description: str


@dataclass
class ValidateInput:
    """
    A class for validating input data against predefined categories and status codes.

    Args:
        all_status_codes (List[StatusCode]): A list of valid status codes.
        all_ddp_categories (List[DDPCategory]): A list of valid DDP categories.
        current_status_code (Optional[StatusCode]): The current status code. Defaults to None.
        current_ddp_category (Optional[DDPCategory]): The current DDP category. Defaults to None.

    Attributes:
        ddp_categories_lookup (Dict[str, DDPCategory]): A lookup dictionary for DDP categories.
        status_codes_lookup (Dict[int, StatusCode]): A lookup dictionary for status codes.

    Examples:
        >>> status_codes = [StatusCode(id=0, description="Success"), StatusCode(id=1, description="Error")]
        >>> ddp_categories = [DDPCategory(id="cat1", ddp_filetype=DDPFiletype.JSON, language=Language.EN, known_files=["file1.txt", "file2.txt"])]
        >>> validator = ValidateInput(all_status_codes=status_codes, all_ddp_categories=ddp_categories)
    """

    all_status_codes: list[StatusCode]
    all_ddp_categories: list[DDPCategory]
    current_status_code: StatusCode | None = None
    current_ddp_category: DDPCategory | None = None

    ddp_categories_lookup: dict[str, DDPCategory] = field(init=False)
    status_codes_lookup: dict[int, StatusCode] = field(init=False)

    def infer_ddp_category(self, file_list_input: list[str]) -> bool:
        """
        Compares a list of files to a list of known files and infers the DDPCategory.

        Args:
            file_list_input (List[str]): A list of input files to compare against known files.

        Returns:
            bool: True if a valid DDP category is inferred, False otherwise. It sets the current_status_code
            and current_ddp_category to either the DDP catogory match, or to an unknown category.

        Examples:
            >>> validator.infer_ddp_category(["file1.txt", "file2.txt"])
        """
        prop_category = {}
        for id, category in self.ddp_categories_lookup.items():
            n_files_found = [
                1 if f in category.known_files else 0 for f in file_list_input
            ]
            prop_category[id] = sum(n_files_found) / len(category.known_files) * 100

        if max(prop_category.values()) >= 5:
            highest = max(prop_category, key=prop_category.get)  # type: ignore
            self.ddp_category = self.ddp_categories_lookup[highest]
            self.set_current_status_code_by_id(0)
            logger.info("Detected DDP category: %s", self.ddp_category.id)
            return True
        else:
            logger.info("Not a valid input; not enough files matched when performing input validation")
            self.set_current_status_code_by_id(1)
            self.ddp_category = DDPCategory(id = "unknown", ddp_filetype=DDPFiletype.UNKOWN, language=Language.UNKNOWN, known_files=[])
            return False

    def set_current_status_code_by_id(self, id: int) -> None:
        """
        Set the status code based on the provided ID.

        Args:
            id (int): The ID of the status code to set.

        Examples:
            >>> validator.set_current_status_code_by_id(0)
        """
        self.current_status_code = self.status_codes_lookup.get(id, None)

    def get_status_code_id(self) -> int:
        """
        Return the current assigned status code ID. Note: zero is always used for OK.
        Non-zero otherwise.

        Returns:
            int: The ID of the current status code, or 1 if no status code is set.

        Examples:
            >>> validator.get_status_code_id()
        """
        if self.current_status_code == None:
            return 1
        else:
            return self.current_status_code.id

    def __post_init__(self) -> None:
        for status_code, ddp_category in zip(self.all_status_codes, self.all_ddp_categories):
            assert isinstance(status_code, StatusCode), "Input is not of class StatusCode"
            assert isinstance(ddp_category, DDPCategory), "Input is not of class DDPCategory"

        self.ddp_categories_lookup = {
            category.id: category for category in self.all_ddp_categories
        }
        self.status_codes_lookup = {
            status_code.id: status_code for status_code in self.all_status_codes
        }


def validate_zip(ddp_categories: list[DDPCategory], path_to_zip: str) -> ValidateInput:
    """
    Validates a DDP zip file against a list of DDP categories.

    This function attempts to open and read the contents of a zip file, then uses
    the ValidateInput class to infer the DDP category based on the files in the zip.
    If the zip file is invalid or cannot be read, it sets an error status code (an integer greather than 0).

    Args:
        ddp_categories (List[DDPCategory]): A list of valid DDP categories to compare against.
        path_to_zip (str): The file path to the zip file to be validated.

    Returns:
        ValidateInput: An instance of ValidateInput containing the validation results.

    Raises:
        zipfile.BadZipFile: This exception is caught internally and results in an error status code.

    Examples:
        >>> categories = [DDPCategory(id="cat1", ddp_filetype=DDPFiletype.JSON, language=Language.EN, known_files=["file1.txt", "file2.txt"])]
        >>> result = validate_zip(categories, "path/to/valid.zip")
        >>> result.get_status_code_id()
        0

        >>> result = validate_zip(categories, "path/to/invalid.zip")
        >>> result.get_status_code_id()
        1
    """
    
    status_codes = [
        StatusCode(id=0, description="Detected a zip from the DDPCategory list"),
        StatusCode(id=1, description="Undetected zip or bad zipfile"),
    ]
    validate = ValidateInput(status_codes, ddp_categories)

    try:
        paths = []
        with zipfile.ZipFile(path_to_zip, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                logger.debug("Found: %s in zip", p.name)
                paths.append(p.name)

        validate.infer_ddp_category(paths)
    except zipfile.BadZipFile:
        validate.set_current_status_code_by_id(1)

    return validate
