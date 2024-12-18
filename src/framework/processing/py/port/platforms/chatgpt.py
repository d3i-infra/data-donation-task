"""
ChatGPT

This module contains an example flow of a ChatGPT data donation study

To see what type of DDPs from ChatGPT it is designed for check DDP_CATEGORIES
"""
import logging

import pandas as pd

import port.api.props as props
import port.helpers.extraction_helpers as eh
from port.platforms.flow_builder import DataDonationFlow
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



def extraction_fun(chatgpt_zip: str) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []
    
    df = conversations_to_df(chatgpt_zip)
    if not df.empty:
        table_title = props.Translatable({
            "en": "Your conversations with ChatGPT",
            "nl": "Uw gesprekken met ChatGPT"
        })
        table_description = props.Translatable({
            "en": "In this table you find your conversations with ChatGPT sorted by time. Below, you find a wordcloud, where the size of the words represents how frequent these words have been used in the conversations.", 
            "nl": "In this table you find your conversations with ChatGPT sorted by time. Below, you find a wordcloud, where the size of the words represents how frequent these words have been used in the conversations.", 
        })
        wordcloud = {
            "title": {
                "en": "Your messages in a wordcloud", 
                "nl": "Your messages in a wordcloud"
            },
            "type": "wordcloud",
            "textColumn": "message",
            "tokenize": True,
        }
        table = props.PropsUIPromptConsentFormTable("chatgpt_conversations", table_title, df, table_description, [wordcloud])
        tables_to_render.append(table)

    return tables_to_render




TEXTS = {
    "submit_file_header": props.Translatable({
        "en": "Select your ChatGPT file", 
        "nl": "Selecteer uw ChatGPT bestand"
        }),
    "review_data_header": props.Translatable({
        "en": "Your ChatGPT data", 
        "nl": "Uw ChatGPT gegevens"
    }),
    "retry_header": props.Translatable({
        "en": "Try again", 
        "nl": "Probeer opnieuw"
    }),
    "review_data_description": props.Translatable({
       "en": "Below you will find a currated selection of ChatGPT data. In this case only the conversations you had with ChatGPT are show on screen. The data represented in this way are much more insightfull because you can actually read back the conversations you had with ChatGPT",
       "nl": "Below you will find a currated selection of ChatGPT data. In this case only the conversations you had with ChatGPT are show on screen. The data represented in this way are much more insightfull because you can actually read back the conversations you had with ChatGPT",
    }),
}

FUNCTIONS = {
    "extraction": extraction_fun
}



def process(session_id: int):
    flow = DataDonationFlow(
        platform_name="ChatGPT",
        ddp_categories=DDP_CATEGORIES,
        texts=TEXTS,
        functions=FUNCTIONS,
        session_id=session_id,
        is_donate_logs=False,
    )

    yield from flow.initialize_default_flow().run()
