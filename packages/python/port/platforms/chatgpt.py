"""
ChatGPT

This module provides an example flow of a ChatGPT data donation study

Assumptions:
It handles DDPs in the english language with filetype JSON.

Configuration
-------------
The ``extraction`` function is driven by ``port_config.json``.  Generate one with::

    pnpm generate-config chatgpt

Each extractor function carries its own table config in a ``Table config::``
JSON block inside its docstring.  The generator reads those blocks and
assembles the JSON file.

Platform info::

    {
        "name": "ChatGPT",
        "filetypes": ["json"],
        "languages": ["en", "nl"],
        "description": "Handles DDPs in English. These data donation flows have not been tested yet, if you find anything wrong with them report to datadonation@uu.nl and they will be fixed!",
        "time_last_tested": "not yet implemented"
    }
"""
import logging
from collections import Counter
from typing import Callable

import pandas as pd
import json

import port.helpers.extraction_helpers as eh
import port.helpers.validate as validate
from port.helpers.extraction_helpers import ZipArchiveReader
from port.helpers.flow_builder import FlowBuilder

from port.helpers.validate import (
    DDPCategory,
    DDPFiletype,
    Language,
)
from port.api.d3i_props import ExtractionResult
from port.api.file_utils import SeekableBinaryReader
from port.helpers.table_extractor import (
    load_port_config,
    run_extraction,
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
            "conversations-000.json",
            "message_feedback.json",
            "model_comparisons.json",
            "user.json"
        ]
    )
]


def shown_message_ids(conversation: dict) -> set[str] | None:
    """Return the ids of the messages ChatGPT shows in a conversation.

    A conversation is a tree: regenerating a reply, editing a prompt or
    picking one of two compared replies adds a sibling branch under the same
    parent. ChatGPT shows only the path from ``current_node`` back to the
    root; every message off that path is hidden from the conversation.

    Returns None when that path can't be determined (no or unknown
    ``current_node``), so callers don't mark anything as hidden.
    """
    mapping = conversation["mapping"]
    node_id = conversation.get("current_node")
    if not isinstance(node_id, str) or node_id not in mapping:
        return None

    shown: set[str] = set()
    # The shown check guards against a cyclic parent chain in malformed data.
    while isinstance(node_id, str) and node_id in mapping and node_id not in shown:
        shown.add(node_id)
        node = mapping[node_id]
        node_id = node.get("parent") if isinstance(node, dict) else None
    return shown


def conversations_to_df(reader: ZipArchiveReader, errors: Counter) -> pd.DataFrame:
    """Extract all ChatGPT conversations into a DataFrame.

    Parameters
    ----------
    reader:
        Archive reader used to load JSON files from the DDP zip.
    errors:
        Mutable counter that accumulates error type counts encountered during
        extraction.  Updated in-place.

    Returns
    -------
    pd.DataFrame
        Columns: ``conversation title``, ``role``, ``message``, ``content type``, ``model``, ``time``, ``message id``, ``reaction to``, ``hidden``, ``content references``, ``search_result_groups``.
        Empty DataFrame when the file is absent or parsing fails.

    Table documentation::

        {
          "summary": "Each row represents one message turn in a ChatGPT conversation, including the role (user or assistant), the message text, the model used, and the timestamp.",
          "source_file": "conversations files (conversations-000.json, conversations-001.json, ... or conversations.json)",
          "columns": {
            "conversation title": "Title of the conversation as stored in the export.",
            "role": "Role of the message author: 'user' or 'assistant'.",
            "message": "Full text of the message.",
            "content type": "The content type of the message, e.g. 'text', 'multimodal_text', 'thoughts', or 'reasoning_recap'.",
            "model": "ChatGPT model slug used to generate the assistant reply.",
            "time": "ISO 8601 timestamp of when the message was created.",
            "message id": "A unique identifier for the message.",
            "reaction to": "The id of the message this message reacts to. ``client-created-root`` indicates the first message in the chat.",
            "hidden": "True when ChatGPT doesn't show the message in the conversation: it is on another branch than the one currently shown, e.g. an earlier version of a regenerated reply, a reply to an edited prompt, or the reply not picked when two were compared. False when shown, or when the export doesn't say which branch is shown.",
            "content references": "Contains content items that are referenced in the message.",
            "search_result_groups": "Contains the web search result groups (sources) used to ground the message, if any."
          }
        }

    Table config::

        {
          "id": "chatgpt_conversations",
          "title": {
            "en": "Your conversations",
            "nl": "Uw gesprekken"
          },
          "description": {
            "en": "In this table you find your conversations with ChatGPT sorted by time. Below, you find a wordcloud, where the size of the words represents how frequent these words have been used in the conversations.",
            "nl": "In deze tabel vind je je gesprekken met ChatGPT gesorteerd op tijd. Hieronder vind je een woordwolk, waarbij de grootte van de woorden aangeeft hoe vaak ze zijn gebruikt in de gesprekken."
          },
          "headers": {
            "conversation title": {"en": "Conversation title", "nl": "Gesprektitel"},
            "role": {"en": "Role", "nl": "Rol"},
            "message": {"en": "Message", "nl": "Bericht"},
            "content type": {"en": "Content type", "nl": "Inhoudstype"},
            "model": {"en": "Model", "nl": "Model"},
            "time": {"en": "Time", "nl": "Tijd"},
            "message id": {"en": "ID", "nl": "ID"},
            "reaction to": {"en": "Reaction to", "nl": "Reactie op"},
            "hidden": {"en": "Hidden", "nl": "Verborgen"},
            "content references": {"en": "Content references", "nl": "Content referenties"},
            "search_result_groups": {"en": "Search result groups", "nl": "Zoekresultaatgroepen"}
          },
          "visualizations": [
            {
              "title": {
                "en": "Your messages in a wordcloud",
                "nl": "Je berichten in een woordwolk"
              },
              "type": "wordcloud",
              "textColumn": "message",
              "tokenize": true
            }
          ]
        }
    """
    results = reader.json_all(r"^conversations.*\.json")
    if not results:
        return pd.DataFrame()
    conversations = [conv for result in results for conv in result.data]

    datapoints = []
    out = pd.DataFrame()

    try:
        for conversation in conversations:
            title = conversation.get("title", "<no title>")
            if not isinstance(conversation.get("mapping"), dict):
                continue #not a valid conversation file, skip it
            shown_ids = shown_message_ids(conversation)
            for id, turn in conversation["mapping"].items():

                if isinstance(turn.get('message'), dict):
                    content_references = []
                    if isinstance(turn['message'].get('metadata'), dict):
                        content_references = json.dumps(turn['message']['metadata'].get('content_references', []))
                
                    search_result_groups = []
                    if isinstance(turn['message'].get('metadata'), dict):
                        search_result_groups = json.dumps(turn['message']['metadata'].get('search_result_groups', []))

                    message = ""
                    content_type = ""
                    if isinstance(turn['message'].get('content'), dict):
                        content_type = turn['message']['content'].get('content_type', '')
                        if content_type == 'text':
                            message = turn['message']['content'].get('parts', '')
                        elif content_type == 'multimodal_text':
                            # parts mixes plain strings with dicts (image/audio
                            # asset pointers, audio transcriptions); keep the text.
                            parts = turn['message']['content'].get('parts', [])
                            message = [
                                part if isinstance(part, str) else part.get('text')
                                for part in parts
                                if isinstance(part, (str, dict))
                            ]
                        elif content_type == 'thoughts':
                            message = turn['message']['content'].get('thoughts', {})
                            message = [thought.get('summary') for thought in message if 'summary' in thought]
                        elif content_type == 'reasoning_recap':
                            message = turn['message']['content'].get('content', '')
                        else:
                            message = "[Unhandled content type: " + str(content_type) + ']'
                        if isinstance(message, list):
                            message = " ".join(part for part in message if isinstance(part, str))

                    denested_d = eh.dict_denester(turn)
                    role = eh.find_item(denested_d, "role")
                    model = eh.find_item(denested_d, "-model_slug")
                    reaction_to = eh.find_item(denested_d, "parent")
                    time = eh.epoch_to_iso(eh.find_item(denested_d, "create_time"), errors=errors)
                    datapoint = {
                        "conversation title": title,
                        "role": role,
                        "message": message,
                        "content type": content_type,
                        "model": model,
                        "time": time,
                        "message id": id,
                        "reaction to": reaction_to,
                        "hidden": shown_ids is not None and id not in shown_ids,
                        "content references": content_references,
                        "search_result_groups": search_result_groups,
                    }
                    if role != "":
                        datapoints.append(datapoint)
        datapoints = sorted(datapoints, key=lambda d: d['time'])
        out = pd.DataFrame(datapoints)

    except Exception as e:
        logger.error("Data extraction error: %s", e)
        errors[type(e).__name__] += 1

    return out


# ---------------------------------------------------------------------------
# Extractor registry & platform info
# ---------------------------------------------------------------------------

#: Mapping from the string names used in port_config.json to actual extractor functions.
EXTRACTOR_REGISTRY: dict[str, Callable[..., pd.DataFrame]] = {
    "conversations_to_df": conversations_to_df,
}


# ---------------------------------------------------------------------------
# Main extraction & flow
# ---------------------------------------------------------------------------

def extraction(chatgpt_zip: SeekableBinaryReader, validation) -> ExtractionResult:
    """Extract data from a ChatGPT DDP zip and return consent-form tables.

    Parameters
    ----------
    chatgpt_zip:
        Seekable binary reader over the ChatGPT DDP zip — the upload
        adapter itself, never a path (ADR-0026).
    validation:
        Validation result object whose ``archive_members`` attribute is passed
        to ``ZipArchiveReader``.
    """
    config = load_port_config(EXTRACTOR_REGISTRY, "chatgpt")
    errors: Counter = Counter()
    reader = ZipArchiveReader(chatgpt_zip, validation.archive_members, errors)
    return run_extraction(reader, errors, config)


class ChatGPTFlow(FlowBuilder):
    """Flow implementation for the ChatGPT data donation study."""

    def __init__(self, session_id: str):
        super().__init__(session_id, "ChatGPT")

    def validate_file(self, file):
        return validate.validate_zip(DDP_CATEGORIES, file)

    def extract_data(self, file_value, validation):
        return extraction(file_value, validation)


def process(session_id):
    flow = ChatGPTFlow(session_id)
    return flow.start_flow()
