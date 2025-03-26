import port.api.props as props
from port.api.commands import (
    CommandSystemDonate, 
    CommandUIRender,
    CommandSystemExit,
)


def render_page(
    header_text: props.Translatable, 
    body: (
        props.PropsUIPromptRadioInput 
        | props.PropsUIPromptConsentForm 
        | props.PropsUIPromptFileInput 
        | props.PropsUIPromptFileInputMultiple 
        | props.PropsUIPromptConfirm 
        | props.PropsUIPromptQuestionnaire
    )
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
            props.PropsUIPromptFileInputMultiple | 
            props.PropsUIPromptConfirm | 
            props.PropsUIPromptQuestionnaire
        ): The main content of the page. It must be compatible with `props.PropsUIPageDonation`.

    Returns:
        CommandUIRender: A render command object containing the fully assembled page. Must be yielded.
    """
    header = props.PropsUIHeader(header_text)
    footer = props.PropsUIFooter()
    page = props.PropsUIPageDonation("does not matter", header, body, footer)
    return CommandUIRender(page)


def generate_retry_prompt(platform_name: str) -> props.PropsUIPromptConfirm:
    """
    Generates a confirmation prompt for retrying file processing.

    This function creates a bilingual (English and Dutch) confirmation prompt
    when a file from a specific platform cannot be processed. It allows the user
    to either try again with a different file or continue with the current file.

    Args:
        platform_name (str): The name of the platform associated with the file
            that couldn't be processed. This is inserted into the prompt text.

    Returns:
        props.PropsUIPromptConfirm: A confirmation prompt object containing
        the message, and labels for the "OK" (try again) and "Cancel" (continue) buttons.
    """
    text = props.Translatable({
        "en": f"Unfortunately, we cannot process your {platform_name} file. Continue, if you are sure that you selected the right file. Try again to select a different file.",
        "nl": f"Helaas, kunnen we uw {platform_name} bestand niet verwerken. Weet u zeker dat u het juiste bestand heeft gekozen? Ga dan verder. Probeer opnieuw als u een ander bestand wilt kiezen."
    })
    ok = props.Translatable({
        "en": "Try again",
        "nl": "Probeer opnieuw"
    })
    cancel = props.Translatable({
        "en": "Continue",
        "nl": "Verder"
    })
    return props.PropsUIPromptConfirm(text, ok, cancel)


def generate_file_prompt(extensions: str) -> props.PropsUIPromptFileInput:
    """
    Generates a file input prompt for selecting a file for a platform.

    This function creates a bilingual (English and Dutch) file input prompt
    that instructs the user to select a file they've received from a platform
    and stored on their device.

    Args:
        extensions (str): A collection of allowed MIME types. 
        For example: "application/zip, text/plain, application/json"

    Returns:
        props.PropsUIPromptFileInput: A file input prompt object containing
        the description text and allowed file extensions.
    """
    description = props.Translatable({
        "en": "Please follow the download instructions and choose the file that you stored on your device.",
        "nl": "Volg de download instructies en kies het bestand dat u opgeslagen heeft op uw apparaat.",
    })
    return props.PropsUIPromptFileInput(description, extensions)


def generate_review_data_prompt(
        id: str,
        description: props.Translatable,
        table_list: list[props.PropsUIPromptConsentFormTable]
) -> props.PropsUIPromptConsentForm:
    """
    Generates a data review form with a list of tables and a description, including default donate question and button.
    The participant can review these tables before they will be send to the researcher. If the participant consents to sharing the data
    the data will be stored at the configured storage location.

    Args:
        id (str): will be used as part of the filename when the data is stored
        table_list (list[props.PropsUIPromptConsentFormTable]): A list of consent form tables to be included in the prompt.
        description (props.Translatable): A translatable description text for the consent prompt.

    Returns:
        props.PropsUIPromptConsentForm: A structured consent form object containing the provided table list, description, 
        and default values for donate question and button.
    """
    donate_question = props.Translatable({
       "en": "Do you want to share this data for research?",
       "nl": "Wilt u deze gegevens delen voor onderzoek?"
    })

    donate_button = props.Translatable({
       "en": "Yes, share for research",
       "nl": "Ja, deel voor onderzoek"
    })

    return props.PropsUIPromptConsentForm(
       id=id,
       tables=table_list, 
       meta_tables=[],
       description=description,
       donate_question=donate_question,
       donate_button=donate_button
    )


def generate_radio_prompt(
        description: props.Translatable,
        list_of_options: list[str]
) -> props.PropsUIPromptRadioInput:
    """
    Generates a radio input form with a list of options and a description.

    Args:
        description (props.Translatable): A translatable description text for the radio prompt.
        list_of_options (list[str]): A list of options to be shown in the prompt.

    Returns:
        props.PropsUIPromptRadioInput: A structured radio input form object.
    """
    title = props.Translatable({
        "en": "",
        "nl": ""
    })
    radio_input = [{"id": index, "value": option} for index, option in enumerate(list_of_options)]

    return props.PropsUIPromptRadioInput(
        title=title, 
        description=description, 
        items=radio_input
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


def render_end_page():
    """
    Renders a thank you page, must be yielded.
    """
    page = props.PropsUIPageEnd()
    return CommandUIRender(page)
