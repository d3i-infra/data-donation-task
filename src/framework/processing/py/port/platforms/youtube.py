"""
DDP extract Youtube
"""
from pathlib import Path
import logging
import zipfile
import re
import io

import pandas as pd
from lxml import etree

import port.unzipddp as unzipddp
import port.helpers as helpers

from port.validate import (
    DDPCategory,
    Language,
    DDPFiletype,
    ValidateInput,
    StatusCode,
)

logger = logging.getLogger(__name__)

VIDEO_REGEX = r"(?P<video_url>^http[s]?://www\.youtube\.com/watch\?v=[a-z,A-Z,0-9,\-,_]+)(?P<rest>$|&.*)"
CHANNEL_REGEX = r"(?P<channel_url>^http[s]?://www\.youtube\.com/channel/[a-z,A-Z,0-9,\-,_]+$)"

DDP_CATEGORIES = [
    DDPCategory(
        id="html_en",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.EN,
        known_files=[
            "archive_browser.html",
            "watch-history.html",
            "my-comments.html",
            "my-live-chat-messages.html",
            "subscriptions.csv",
            "comments.csv",
        ],
    ),
    DDPCategory(
        id="html_nl",
        ddp_filetype=DDPFiletype.HTML,
        language=Language.NL,
        known_files=[
            "archive_browser.html",
            "kijkgeschiedenis.html",
            "zoekgeschiedenis.html",
            "mijn-reacties.html",
            "abonnementen.csv",
            "reacties.csv",
        ],
    ),
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid DDP", message=""),
    StatusCode(id=1, description="Valid DDP unhandled format", message=""),
    StatusCode(id=2, description="Not a valid DDP", message=""),
    StatusCode(id=3, description="Bad zipfile", message=""),
]


def validate_zip(zfile: Path) -> ValidateInput:
    """
    Validates the input of an Youtube zipfile

    This function sets a validation object generated with ValidateInput
    This validation object can be read later on to infer possible problems with the zipfile
    I dont like this design myself, but I also havent found any alternatives that are better
    """

    validation = ValidateInput(STATUS_CODES, DDP_CATEGORIES)

    try:
        paths = []
        with zipfile.ZipFile(zfile, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".json", ".csv", ".html"):
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        if validation.infer_ddp_category(paths):
            validation.set_status_code(0)
        else:
            validation.set_status_code(1)

    except zipfile.BadZipFile:
        validation.set_status_code(3)

    return validation


# Extract Watch later.csv
def watch_later_to_df(youtube_zip: str) -> pd.DataFrame:
    """
    Parses 'Watch later.csv' from Youtube DDP
    Filename is the same for Dutch and English Language settings

    Note: 'Watch later.csv' is NOT a proper csv it 2 csv's in one
    """

    ratings_bytes = unzipddp.extract_file_from_zip(youtube_zip, "Watch later.csv")
    df = pd.DataFrame()

    try:
        # remove the first 3 lines from the .csv
        #ratings_bytes = io.BytesIO(re.sub(b'^(.*)\n(.*)\n\n', b'', ratings_bytes.read()))
        ratings_bytes = io.BytesIO(re.sub(b'^((?s).)*?\n\n', b'', ratings_bytes.read()))

        df = unzipddp.read_csv_from_bytes_to_df(ratings_bytes)
        df['Video-ID'] = 'https://www.youtube.com/watch?v=' + df['Video-ID']
    except Exception as e:
        logger.debug("Exception was caught:  %s", e)

    return df


# Extract subscriptions.csv
def subscriptions_to_df(youtube_zip: str, validation: ValidateInput) -> pd.DataFrame:
    """
    Parses 'subscriptions.csv' or 'abonnementen.csv' from Youtube DDP
    """

    # Determine the language of the file name
    file_name = "subscriptions.csv"
    if validation.ddp_category.language == Language.NL:
        file_name = "abonnementen.csv"

    ratings_bytes = unzipddp.extract_file_from_zip(youtube_zip, file_name)
    df = unzipddp.read_csv_from_bytes_to_df(ratings_bytes)
    return df


# Extract comments.csv
def my_comments_to_df(youtube_zip: str, validation: ValidateInput) -> pd.DataFrame:
    """
    Parses 'comments.csv' or 'reacties.csv' from Youtube DDP
    """

    # Determine the language of the file name
    file_name = "comments.csv"
    if validation.ddp_category.language == Language.NL:
        file_name = "reacties.csv"

    ratings_bytes = unzipddp.extract_file_from_zip(youtube_zip, file_name)
    df = unzipddp.read_csv_from_bytes_to_df(ratings_bytes)
    return df


# Extract watch history
def watch_history_extract_html(bytes: io.BytesIO) -> pd.DataFrame:
    """
    watch-history.html bytes buffer to pandas dataframe
    """

    out = pd.DataFrame()
    datapoints = []

    try:
        tree = etree.HTML(bytes.read())
        outer_container_class = "outer-cell mdl-cell mdl-cell--12-col mdl-shadow--2dp"
        watch_history_container_class = "content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1"
        ads_container_class = "content-cell mdl-cell mdl-cell--12-col mdl-typography--caption"
        r = tree.xpath(f"//div[@class='{outer_container_class}']")

        for e in r:
            is_ad = False
            ads_container = e.xpath(f"./div/div[@class='{ads_container_class}']")[0]
            ad_text = "".join(ads_container.xpath("text()"))
            if ads_container is not None and ("Google Ads" in ad_text or "Google Adverteren" in ad_text):
                is_ad = True
            
            if is_ad: 
                ad = "Yes"
            else: 
                ad = "No"
            v = e.xpath(f"./div/div[@class='{watch_history_container_class}']")[0]
            
            child_all_text_list = v.xpath("text()")

            datetime = child_all_text_list.pop()
            datetime = helpers.fix_ascii_string(datetime)
            atags = v.xpath("a")

            try:
                title = atags[0].text
                video_url = atags[0].get("href")
            except:
                if len(child_all_text_list) != 0:
                    title = child_all_text_list[0]
                else:
                    title = None
                video_url = None
                logger.debug("Could not find a title")
            try:
                channel_name = atags[1].text
            except:
                channel_name = None
                logger.debug("Could not find the channel name")

            datapoints.append(
                (title, video_url, ad, channel_name, datetime)
            )
        out = pd.DataFrame(datapoints, columns=["Title", "Url", "Advertisement", "Channel", "Date"])

    except Exception as e:
        logger.error("Exception was caught:  %s", e)

    return out


# Extract search history
def search_history_extract_html(bytes: io.BytesIO) -> pd.DataFrame:
    """
    search-history.html bytes buffer to pandas dataframe
    """

    out = pd.DataFrame()
    datapoints = []

    try:
        tree = etree.HTML(bytes.read())
        outer_container_class = "outer-cell mdl-cell mdl-cell--12-col mdl-shadow--2dp"
        search_history_container_class = "content-cell mdl-cell mdl-cell--6-col mdl-typography--body-1"
        ads_container_class = "content-cell mdl-cell mdl-cell--12-col mdl-typography--caption"
        r = tree.xpath(f"//div[@class='{outer_container_class}']")

        for e in r:
            is_ad = False
            ads_container = e.xpath(f"./div/div[@class='{ads_container_class}']")[0]
            ad_text = "".join(ads_container.xpath("text()"))
            if ads_container is not None and ("Google Ads" in ad_text or "Google Adverteren" in ad_text):
                is_ad = True
            
            if is_ad: 
                continue

            s = e.xpath(f"./div/div[@class='{search_history_container_class}']")[0]

            child_all_text_list = s.xpath("text()")

            datetime = child_all_text_list.pop()
            datetime = helpers.fix_ascii_string(datetime)
            atags = s.xpath("a")

            try:
                title = atags[0].text
                video_url = atags[0].get("href")
            except:
                if len(child_all_text_list) != 0:
                    title = child_all_text_list[0]
                else:
                    title = None
                video_url = None
                logger.debug("Could not find a title")
            try:
                channel_name = atags[1].text
            except:
                channel_name = None
                logger.debug("Could not find the channel name")

            datapoints.append(
                (title, video_url, datetime)
            )
        out = pd.DataFrame(datapoints, columns=["Search Terms", "Url", "Date"])

    except Exception as e:
        logger.error("Exception was caught:  %s", e)

    return out


def watch_history_to_df(youtube_zip: str, validation: ValidateInput) -> pd.DataFrame:
    """
    Works for watch-history.html and kijkgeschiedenis.html
    """
    out = pd.DataFrame()

    try:
        if validation.ddp_category.ddp_filetype == DDPFiletype.HTML:
            # Determine the language of the file name
            file_name = "watch-history.html"
            if validation.ddp_category.language == Language.NL:
                file_name = "kijkgeschiedenis.html"

            html_bytes_buf = unzipddp.extract_file_from_zip(youtube_zip, file_name)
            out = watch_history_extract_html(html_bytes_buf)
            out["Date"] = out["Date"].apply(helpers.try_to_convert_any_timestamp_to_iso8601)

        else:
            out = pd.DataFrame([("Er zit wel data in jouw data package, maar we hebben het er niet uitgehaald")], columns=["Extraction not implemented"])

    except Exception as e:
        logger.error("Exception was caught:  %s", e)

    return out


def search_history_to_df(youtube_zip: str, validation: ValidateInput) -> pd.DataFrame:
    """
    Works for search-history.html and zoekgeschiedenis.html
    """
    out = pd.DataFrame()

    try:
        if validation.ddp_category.ddp_filetype == DDPFiletype.HTML:
            # Determine the language of the file name
            file_name = "search-history.html"
            if validation.ddp_category.language == Language.NL:
                file_name = "zoekgeschiedenis.html"

            html_bytes_buf = unzipddp.extract_file_from_zip(youtube_zip, file_name)
            out = search_history_extract_html(html_bytes_buf)
            out["Date"] = out["Date"].apply(helpers.try_to_convert_any_timestamp_to_iso8601)

        else:
            out = pd.DataFrame([("Er zit wel data in jouw data package, maar we hebben het er niet uitgehaald")], columns=["Extraction not implemented"])
    except Exception as e:
        logger.error("Exception was caught:  %s", e)

    return out
