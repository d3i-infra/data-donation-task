"""
DDP YouTube module

"""
import logging

import pandas as pd

import port.api.props as props
import port.api.d3i_props as d3i_props
import port.helpers.extraction_helpers as eh
import port.helpers.validate as validate
from port.platforms.flow_builder import FlowBuilder

from port.helpers.validate import (
    DDPCategory,
    DDPFiletype,
    Language,
    ValidateInput,
)

logger = logging.getLogger(__name__)

DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.NL,
        known_files=[
            "abonnementen.csv",
            "kijkgeschiedenis.json",
            "zoekgeschiedenis.json",
        ],
    ),
    DDPCategory(
        id="json_nl",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "search-history.json",
            "watch-history.json",
            "subscriptions.cs",
        ],
    ),
]


def watch_history_to_df(zip: str, validation) -> pd.DataFrame:
    
    if validation.current_ddp_category.language == Language.NL:
        b = eh.extract_file_from_zip(zip, "kijkgeschiedenis.json")
        d = eh.read_json_from_bytes(b)

    elif validation.current_ddp_category.language == Language.EN:
        b = eh.extract_file_from_zip(zip, "watch-history.json")
        d = eh.read_json_from_bytes(b)

    else:
        d = {}

    out = pd.DataFrame()
    datapoints = []

    try:
        for item in d:
            datapoints.append((
                item.get("title", ""),
                item.get("titleUrl", ""),
                item.get("time", ""),
            ))

        out = pd.DataFrame(datapoints, columns=["Titel", "Link" ,"Datum en tijd"]) # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


def search_history_to_df(zip: str, validation) -> pd.DataFrame:
    
    if validation.current_ddp_category.language == Language.NL:
        b = eh.extract_file_from_zip(zip, "zoekgeschiedenis.json")
        d = eh.read_json_from_bytes(b)

    elif validation.current_ddp_category.language == Language.EN:
        b = eh.extract_file_from_zip(zip, "search-history.json")
        d = eh.read_json_from_bytes(b)

    else:
        d = {}

    out = pd.DataFrame()
    datapoints = []

    try:
        for item in d:
            datapoints.append((
                item.get("title", ""),
                item.get("time", ""),
            ))

        out = pd.DataFrame(datapoints, columns=["Zoekterm", "Datum en tijd"]) # pyright: ignore

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


def subscriptions_to_df(youtube_zip: str, validation) -> pd.DataFrame:
    """
    Parses 'subscriptions.csv' or 'abonnementen.csv' from Youtube DDP
    """

    if validation.current_ddp_category.language == Language.NL:
        file_name = "abonnementen.csv"

    elif validation.current_ddp_category.language == Language.EN:
        file_name = "subscriptions.csv"
    else:
        file_name = ""

    ratings_bytes = eh.extract_file_from_zip(youtube_zip, file_name)
    df = eh.read_csv_from_bytes_to_df(ratings_bytes)
    return df


def extraction(zip: str, validation: ValidateInput) -> list[d3i_props.PropsUIPromptConsentFormTableViz]:
    tables = [
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="youtube_kijkgeschiedenis",
            data_frame=watch_history_to_df(zip, validation),
            title=props.Translatable({
                "nl": "Your watch history",
                "en": "Your watch history"
            }),
            description=props.Translatable({
                "en": "List of videos you've watched on YouTube with dates and timestamps", 
                "nl": "Lijst van video's die je op YouTube hebt bekeken met datums en tijdstippen"
            }),
            visualizations=[
                {
                    "title": {
                        "en": "Wordcloud of the words in the video title, large words mean they occur more frequently in titles", 
                        "nl": "Wordcloud of the words in the video title, large words mean they occur more frequently in titles"
                    },
                    "type": "wordcloud",
                    "textColumn": "Titel",
                    "tokenize": True,
                }
            ]
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="youtube_zoekgeschiedenis",
            data_frame=search_history_to_df(zip, validation),
            title=props.Translatable({
                "nl": "Your search history",
                "en": "Your search history"
            }),
            description=props.Translatable({
                "en": "Record of search terms you've used on YouTube", 
                "nl": "Overzicht van zoektermen die je hebt gebruikt op YouTube"
            }),
            visualizations=[
                {
                    "title": {
                        "en": "Wordcloud of the words in your search history, large words mean they occur more frequently in your search history", 
                        "nl": "Wordcloud of the words in the search histopry, large words mean they occur more frequently in your search history"
                    },
                    "type": "wordcloud",
                    "textColumn": "Zoekterm",
                    "tokenize": True,
                }
            ]
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="youtube_abonnementen",
            data_frame=subscriptions_to_df(zip, validation),
            title=props.Translatable({
                "nl": "Abonnementen",
                "en": "Subscriptions"
            }),
            description=props.Translatable({
                "en": "List of YouTube channels you're subscribed to", 
                "nl": "Lijst van YouTube-kanalen waarop je bent geabonneerd"
            })
        )
    ]
    
    return [table for table in tables if not table.data_frame.empty]


class YouTubeFlow(FlowBuilder):
    def __init__(self, session_id: int):
        super().__init__(session_id, "YouTube")
        
    def validate_file(self, file):
        return validate.validate_zip(DDP_CATEGORIES, file)
        
    def extract_data(self, file, validation):
        return extraction(file, validation)


def process(session_id):
    flow = YouTubeFlow(session_id)
    return flow.start_flow()
