"""
ChatGPT

This module provides an example flow of a ChatGPT data donation study
"""
import logging

import pandas as pd

import port.api.props as props
import port.helpers.extraction_helpers as eh
import port.helpers.port_helpers as ph
import port.helpers.validate as validate

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



def extraction(chatgpt_zip: str) -> list[props.PropsUIPromptConsentFormTable]:
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



# TEXTS
SUBMIT_FILE_HEADER = props.Translatable({
    "en": "Select your ChatGPT file", 
    "nl": "Selecteer uw ChatGPT bestand"
})

REVIEW_DATA_HEADER = props.Translatable({
    "en": "Your ChatGPT data", 
    "nl": "Uw ChatGPT gegevens"
})

RETRY_HEADER = props.Translatable({
    "en": "Try again", 
    "nl": "Probeer opnieuw"
})

REVIEW_DATA_DESCRIPTION = props.Translatable({
   "en": "Below you will find a currated selection of ChatGPT data. In this case only the conversations you had with ChatGPT are show on screen. The data represented in this way are much more insightfull because you can actually read back the conversations you had with ChatGPT",
   "nl": "Below you will find a currated selection of ChatGPT data. In this case only the conversations you had with ChatGPT are show on screen. The data represented in this way are much more insightfull because you can actually read back the conversations you had with ChatGPT",
})


def process(session_id: int):
    platform_name = "ChatGPT"

    table_list = None
    while True:
        logger.info("Prompt for file for %s", platform_name)

        file_prompt = ph.generate_file_prompt("application/zip")
        file_result = yield ph.render_page(SUBMIT_FILE_HEADER, file_prompt)

        if file_result.__type__ == "PayloadString":
            validation = validate.validate_zip(DDP_CATEGORIES, file_result.value)

            # Happy flow: Valid DDP
            if validation.get_status_code_id() == 0:
                logger.info("Payload for %s", platform_name)
                extraction_result = extraction(file_result.value)
                table_list = extraction_result
                break

            # Enter retry flow, reason: if DDP was not a ChatGPT DDP
            if validation.get_status_code_id() != 0:
                logger.info("Not a valid %s zip; No payload; prompt retry_confirmation", platform_name)
                retry_prompt = ph.generate_retry_prompt(platform_name)
                retry_result = yield ph.render_page(RETRY_HEADER, retry_prompt)

                if retry_result.__type__ == "PayloadTrue":
                    continue
                else:
                    logger.info("Skipped during retry flow")
                    break

        else:
            logger.info("Skipped at file selection ending flow")
            break

    if table_list is not None:
        logger.info("Prompt consent; %s", platform_name)
        review_data_prompt = ph.generate_review_data_prompt(f"{session_id}-chatgpt", REVIEW_DATA_DESCRIPTION, table_list)
        yield ph.render_page(REVIEW_DATA_HEADER, review_data_prompt)

    yield ph.exit(0, "Success")
    yield ph.render_end_page()
