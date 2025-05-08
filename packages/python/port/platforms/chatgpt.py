"""
ChatGPT

This module provides an example flow of a ChatGPT data donation study

Assumptions:
It handles DDPs in the english language with filetype JSON.
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
)

logger = logging.getLogger(__name__)

DDP_CATEGORIES = [
    DDPCategory(
        id="json",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "chat.html", 
            "conversations.json",
            "message_feedback.json",
            "model_comparisons.json",
            "user.json"
        ]
    )
]


def conversations_to_df(chatgpt_zip: str)  -> pd.DataFrame:
    b = eh.extract_file_from_zip(chatgpt_zip, "conversations.json")
    conversations = eh.read_json_from_bytes(b)

    datapoints = []
    out = pd.DataFrame()

    try:
        for conversation in conversations:
            title = conversation["title"]
            for _, turn in conversation["mapping"].items():

                denested_d = eh.dict_denester(turn)
                is_hidden = eh.find_item(denested_d, "is_visually_hidden_from_conversation")
                if is_hidden != "True":
                    role = eh.find_item(denested_d, "role")
                    message = "".join(eh.find_items(denested_d, "part"))
                    model = eh.find_item(denested_d, "-model_slug")
                    time = eh.epoch_to_iso(eh.find_item(denested_d, "create_time"))

                    datapoint = {
                        "conversation title": title,
                        "role": role,
                        "message": message,
                        "model": model,
                        "time": time,
                    }
                    if role != "":
                        datapoints.append(datapoint)

        out = pd.DataFrame(datapoints)

    except Exception as e:
        logger.error("Data extraction error: %s", e)
        
    return out



def extraction(chatgpt_zip: str) -> list[d3i_props.PropsUIPromptConsentFormTableViz]:
    """
    Add your table definitions below in the list
    """
    tables = [
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="chatgpt_conversations",
            data_frame=conversations_to_df(chatgpt_zip),
            title=props.Translatable({
                "en": "Your conversations with ChatGPT",
                "nl": "Uw gesprekken met ChatGPT"
            }),
            description=props.Translatable({
                "en": "In this table you find your conversations with ChatGPT sorted by time. Below, you find a wordcloud, where the size of the words represents how frequent these words have been used in the conversations.", 
                "nl": "In this table you find your conversations with ChatGPT sorted by time. Below, you find a wordcloud, where the size of the words represents how frequent these words have been used in the conversations.", 
            }),
            visualizations=[
                {
                    "title": {
                        "en": "Your messages in a wordcloud", 
                        "nl": "Your messages in a wordcloud"
                    },
                    "type": "wordcloud",
                    "textColumn": "message",
                    "tokenize": True,
                }
            ]
        ),
    ]

    tables_to_render = [table for table in tables if not table.data_frame.empty]

    return tables_to_render



class ChatGPTFlow(FlowBuilder):
    def __init__(self, session_id: int):
        super().__init__(session_id, "ChatGPT")
        
    def validate_file(self, file):
        return validate.validate_zip(DDP_CATEGORIES, file)
        
    def extract_data(self, file_value, validation):
        return extraction(file_value)


def process(session_id):
    flow = ChatGPTFlow(session_id)
    return flow.start_flow()
