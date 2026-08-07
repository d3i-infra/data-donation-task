import logging

import port.api.d3i_props as d3i_props
import port.api.props as props
from port.api.commands import CommandSystemDonate, CommandSystemExit, CommandSystemLog, CommandUIRender

_logger = logging.getLogger(__name__)


def render_page(
    header_text: props.Translatable,
    body: (
        props.PropsUIPromptRadioInput
        | props.PropsUIPromptConsentForm
        | d3i_props.PropsUIPromptConsentFormViz
        | props.PropsUIPromptFileInput
        | d3i_props.PropsUIPromptFileInputMultiple
        | d3i_props.PropsUIPromptQuestionnaire
        | props.PropsUIPromptConfirm
    ),
) -> CommandUIRender:
    """
    Renders the UI components for a donation page.

    This function assembles various UI components including a header, body, and footer
    to create a complete donation page. It uses the provided header text and body content
    to customize the page.

    Args:
        header_text (props.Translatable): The text to be displayed in the header.
            This should be a translatable object to support multiple languages.
        body (
            props.PropsUIPromptRadioInput |
            props.PropsUIPromptConsentForm |
            props.PropsUIPromptFileInput |
            props.PropsUIPromptConfirm |
        ): The main content of the page. It must be compatible with `props.PropsUIPageDonation`.

    Returns:
        CommandUIRender: A render command object containing the fully assembled page. Must be yielded.
    """
    header = props.PropsUIHeader(header_text)
    page = props.PropsUIPageDataSubmission("does not matter", header, body)
    return CommandUIRender(page)


def generate_retry_prompt(platform_name: str) -> props.PropsUIPromptConfirm:
    """
    Generate a bilingual retry prompt for file processing errors.

    Returns a PropsUIPromptConfirm with "Try again" (ok → PayloadTrue) and
    "Continue" (cancel → PayloadFalse) buttons. Using standard feldspar
    PropsUIPromptConfirm instead of d3i PropsUIPromptRetry which only
    renders a single button. See ADR-0016 for the broader
    decision on custom vs standard prompt components.

    Args:
        platform_name: The name of the platform whose file could not be processed.
    """

    text = props.Translatable(
        {
            "en": f"Unfortunately, we cannot process your {platform_name} file. Continue, if you are sure that you selected the right file. Try again to select a different file.",
            "nl": f"Helaas, kunnen we uw {platform_name} bestand niet verwerken. Weet u zeker dat u het juiste bestand heeft gekozen? Ga dan verder. Probeer opnieuw als u een ander bestand wilt kiezen.",
            "es": f"Lamentablemente, no podemos procesar su archivo de {platform_name}. Intente de nuevo para seleccionar un archivo diferente",
        }
    )
    ok = props.Translatable({"en": "Try again", "nl": "Probeer opnieuw", "es": "Intentar de nuevo"})
    cancel = props.Translatable({"en": "Continue", "nl": "Doorgaan", "es": "Continuar"})
    return props.PropsUIPromptConfirm(text, ok, cancel)


def generate_file_prompt(
    extensions: str, multiple: bool = False
) -> props.PropsUIPromptFileInput | d3i_props.PropsUIPromptFileInputMultiple:
    """
    Generates a file input prompt for selecting file(s) for a platform.
    This function creates a bilingual (English and Dutch) file input prompt
    that instructs the user to select file(s) they've received from a platform
    and stored on their device.

    The prompt that is returned by this function needs to be rendered using: yield result = render_page(...)
    result.value should then contain the file handle(s).
    In case multiple is true, a list with file handles is returned.

    Args:
        extensions (str): A collection of allowed MIME types.
            For example: "application/zip, text/plain, application/json"
        multiple (bool, optional): Whether to allow multiple file selection.
            Defaults to False.

    Returns:
        props.PropsUIPromptFileInput | d3i_props.PropsUIPromptFileInputMultiple:
            A file input prompt object containing the description text and
            allowed file extensions. If multiple=True, returns a
            PropsUIPromptFileInputMultiple object for selecting multiple files.
    """
    description = props.Translatable(
        {
            "en": "Please follow the download instructions and choose the file that you stored on your device.",
            "nl": "Volg de download instructies en kies het bestand dat u opgeslagen heeft op uw apparaat.",
        }
    )
    if multiple:
        return d3i_props.PropsUIPromptFileInputMultiple(description, extensions)

    return props.PropsUIPromptFileInput(description, extensions)


def generate_review_data_prompt(
    description: props.Translatable, table_list: list[d3i_props.PropsUIPromptConsentFormTableViz]
) -> d3i_props.PropsUIPromptConsentFormViz:
    """
    Generates a data review form with a list of tables and a description, including default donate question and button.
    The participant can review these tables before they will be send to the researcher. If the participant consents to sharing the data
    the data will be stored at the configured storage location.

    Args:
        table_list (list[props.PropsUIPromptConsentFormTableViz]): A list of consent form tables to be included in the prompt.
        description (props.Translatable): A translatable description text for the consent prompt.

    Returns:
        props.PropsUIPromptConsentForm: A structured consent form object containing the provided table list, description,
        and default values for donate question and button.
    """
    donate_question = props.Translatable(
        {"en": "Do you want to share this data for research?", "nl": "Wilt u deze gegevens delen voor onderzoek?"}
    )

    donate_button = props.Translatable({"en": "Yes, share for research", "nl": "Ja, deel voor onderzoek"})

    return d3i_props.PropsUIPromptConsentFormViz(
        tables=table_list, description=description, donate_question=donate_question, donate_button=donate_button
    )


def donate(key: str, json_string: str) -> CommandSystemDonate:
    """
    Initiates a donation process using the provided key and data.

    This function triggers the donation process by passing a key and a JSON-formatted string
    that contains donation information.

    Args:
        key (str): The key associated with the donation process. The key will be used in the file name.
        json_string (str): A JSON-formatted string containing the donated data.

    Returns:
        CommandSystemDonate: A system command that initiates the donation process. Must be yielded.
    """
    return CommandSystemDonate(key, json_string)


def exit(code: int, info: str) -> CommandSystemExit:
    """
    Exits Next with the provided exit code and additional information.
    This if the code reaches this function, it will return to the task list in Next.

    Args:
        code (int): The exit code representing the type or status of the exit.
        info (str): A string containing additional information about the exit.

    Returns:
        CommandSystemExit: A system command that initiates the exit process in Next.

    Examples::

        yield exit(0, "Success")
    """
    return CommandSystemExit(code, info)


def emit_log(level: str, message: str):
    """Yield a CommandSystemLog to the host via the command protocol.

    Use via `yield from emit_log(...)` in generators (FlowBuilder, script.py).
    The host receives the log immediately; the PayloadVoid response is discarded.

    Messages sent through this function reach mono's /api/feldspar/log.
    They MUST be PII-free — no file paths, exception text, or participant data.

    Examples::

        yield from emit_log("info", "[LinkedIn] Consent: accepted")
        yield from emit_log("info", "Starting platform: Facebook")
    """
    _ = yield CommandSystemLog(level=level, message=message)


def generate_radio_prompt(
    title: props.Translatable, description: props.Translatable, items: list[str]
) -> props.PropsUIPromptRadioInput:
    """
    General purpose prompt selection menu
    """
    radio_items: list[props.RadioItem] = [{"id": i, "value": item} for i, item in enumerate(items)]
    return props.PropsUIPromptRadioInput(title, description, radio_items)


def generate_questionnaire() -> d3i_props.PropsUIPromptQuestionnaire:
    """
    Administer a basic questionnaire in Port.

    This function generates a prompt which can be rendered with render_page().
    The questionnaire demonstrates all currently implemented question types.
    In the current implementation, all questions are optional.

    You can build in logic by:
    - Chaining questionnaires together
    - Using extracted data in your questionnaires

    Usage:
        prompt = generate_questionnaire()
        results = yield render_page(header_text, prompt)

    The results.value contains a JSON string with question answers that
    can then be donated with donate().
    """

    questionnaire_description = props.Translatable(
        translations={
            "en": "Customer Satisfaction Survey for our Online Store",
            "nl": "Klanttevredenheidsonderzoek voor onze Online Winkel",
        }
    )

    open_question = props.Translatable(
        translations={"en": "How can we improve our services?", "nl": "Hoe kunnen we onze diensten verbeteren?"}
    )

    mc_question = props.Translatable(
        translations={"en": "How would you rate your overall experience?", "nl": "Hoe zou u uw algemene ervaring beoordelen?"}
    )

    mc_choices = [
        props.Translatable(translations={"en": "Excellent", "nl": "Uitstekend"}),
        props.Translatable(translations={"en": "Good", "nl": "Goed"}),
        props.Translatable(translations={"en": "Average", "nl": "Gemiddeld"}),
        props.Translatable(translations={"en": "Poor", "nl": "Slecht"}),
        props.Translatable(translations={"en": "Very Poor", "nl": "Zeer slecht"}),
    ]

    checkbox_question = props.Translatable(
        translations={
            "en": "Which of our products have you purchased? (Select all that apply)",
            "nl": "Welke van onze producten heeft u gekocht? (Selecteer alle toepasselijke)",
        }
    )

    checkbox_choices = [
        props.Translatable(translations={"en": "Electronics", "nl": "Elektronica"}),
        props.Translatable(translations={"en": "Clothing", "nl": "Kleding"}),
        props.Translatable(translations={"en": "Home Goods", "nl": "Huishoudelijke artikelen"}),
        props.Translatable(translations={"en": "Books", "nl": "Boeken"}),
        props.Translatable(translations={"en": "Food Items", "nl": "Voedingsproducten"}),
    ]

    open_ended_question = d3i_props.PropsUIQuestionOpen(id=1, question=open_question)

    multiple_choice_question = d3i_props.PropsUIQuestionMultipleChoice(id=2, question=mc_question, choices=mc_choices)

    checkbox_question_obj = d3i_props.PropsUIQuestionMultipleChoiceCheckbox(
        id=3, question=checkbox_question, choices=checkbox_choices
    )

    return d3i_props.PropsUIPromptQuestionnaire(
        description=questionnaire_description, questions=[multiple_choice_question, checkbox_question_obj, open_ended_question]
    )


def render_no_data_page(platform_name: str) -> CommandUIRender:
    """Render 'no relevant data found' with acknowledge button.

    Caller should yield and await response before returning.
    """
    header = props.PropsUIHeader(
        props.Translatable({
            "en": f"No data found",
            "nl": f"Geen gegevens gevonden",
        })
    )
    body = props.PropsUIPromptConfirm(
        text=props.Translatable({
            "en": f"Unfortunately, no relevant data was found in your {platform_name} file.",
            "nl": f"Helaas zijn er geen relevante gegevens gevonden in uw {platform_name} bestand.",
        }),
        ok=props.Translatable({"en": "Continue", "nl": "Doorgaan"}),
        cancel=props.Translatable({"en": "Continue", "nl": "Doorgaan"}),
    )
    page = props.PropsUIPageDataSubmission(platform_name, header, body)
    return CommandUIRender(page)


def render_safety_error_page(platform_name: str, error: Exception) -> CommandUIRender:
    """Render file safety error page.

    Caller should yield and await response before returning.
    """
    header = props.PropsUIHeader(
        props.Translatable({
            "en": "File cannot be processed",
            "nl": "Bestand kan niet worden verwerkt",
        })
    )
    body = props.PropsUIPromptConfirm(
        text=props.Translatable({
            "en": f"Your {platform_name} file could not be processed: {error}",
            "nl": f"Uw {platform_name} bestand kon niet worden verwerkt: {error}",
        }),
        ok=props.Translatable({"en": "Continue", "nl": "Doorgaan"}),
        cancel=props.Translatable({"en": "Continue", "nl": "Doorgaan"}),
    )
    page = props.PropsUIPageDataSubmission(platform_name, header, body)
    return CommandUIRender(page)


def render_donate_failure_page(platform_name: str) -> CommandUIRender:
    """Render donation failure page.

    Caller should yield and await response before returning.
    """
    header = props.PropsUIHeader(
        props.Translatable({
            "en": "Data submission failed",
            "nl": "Gegevensinzending mislukt",
        })
    )
    body = props.PropsUIPromptConfirm(
        text=props.Translatable({
            "en": f"Unfortunately, your {platform_name} data could not be submitted. Please try again later.",
            "nl": f"Helaas konden uw {platform_name} gegevens niet worden ingediend. Probeer het later opnieuw.",
        }),
        ok=props.Translatable({"en": "Continue", "nl": "Doorgaan"}),
        cancel=props.Translatable({"en": "Continue", "nl": "Doorgaan"}),
    )
    page = props.PropsUIPageDataSubmission(platform_name, header, body)
    return CommandUIRender(page)


def handle_donate_result(result) -> bool:
    """Inspect donate result. Returns True on success, False on failure.

    Both current bridges acknowledge a CommandSystemDonate with a structured
    result, so production and local dev alike reach Python as PayloadResponse:
    LiveBridge relays the host's reply, and FakeBridge returns the outcome of
    its own /data-submission POST. PayloadVoid arrives only from a bridge that
    resolves a donate without an acknowledgment (an older host, a stub bridge).

    PayloadResponse → check value.success (the path every current bridge takes)
    PayloadVoid / None → True (legacy no-acknowledgment shape)
    Anything else → log warning, return False
    """
    if result is None:
        return True

    result_type = getattr(result, "__type__", None)

    if result_type == "PayloadResponse":
        # value is { success: bool, key: str, status: int, error?: str }
        return bool(result.value.success)

    if result_type == "PayloadVoid":
        return True

    _logger.warning("Unexpected donate result type: %s", result_type)
    return False
