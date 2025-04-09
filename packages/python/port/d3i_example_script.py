import zipfile
import json

import pandas as pd

import port.api.props as props
import port.api.d3i_props as d3i_props
import port.helpers.port_helpers as ph


SUBMIT_FILE_HEADER = props.Translatable({
    "en": "Select a random zipfile of choice", 
    "nl": "Selecteer een willekeurige zipfile",
})

REVIEW_DATA_HEADER = props.Translatable({
    "en": "Your random zip contents", 
    "nl": "De gegevens in uw zip"
})

REVIEW_DATA_DESCRIPTION = props.Translatable({
    "en": "Below you will find meta data about the contents of the zip file you submitted. Please review the data carefully and remove any information you do not wish to share. If you would like to share this data, click on the 'Yes, share for research' button at the bottom of this page. By sharing this data, you contribute to research <insert short explanation about your research here>.",
    "nl": "Hieronder ziet u gegevens over de zip die u heeft ingediend. Bekijk de gegevens zorgvuldig, en verwijder de gegevens die u niet wilt delen. Als u deze gegevens wilt delen, klik dan op de knop 'Ja, deel voor onderzoek' onderaan deze pagina. Door deze gegevens te delen draagt u bij aan onderzoek over <korte zin over het onderzoek>."
})

RETRY_HEADER = props.Translatable({
    "en": "Try again", 
    "nl": "Probeer opnieuw"
})


def process(session_id: str):
    platform_name = "Platform of interest"

    # Start of the data donation flow
    while True:
        # Ask the participant to submit a file
        file_prompt = ph.generate_file_prompt("application/zip, text/plain")
        file_prompt_result = yield ph.render_page(SUBMIT_FILE_HEADER, file_prompt)

        # If the participant submitted a file: continue
        if file_prompt_result.__type__ == 'PayloadString':

            # Validate the file the participant submitted
            # In general this is wise to do 
            is_data_valid = validate_the_participants_input(file_prompt_result.value)

            # Happy flow:
            # The file the participant submitted is valid
            if is_data_valid == True:

                # Extract the data you as a researcher are interested in, and put it in a pandas DataFrame
                # Show this data to the participant in a table on screen
                # The participant can now decide to donate
                extracted_data = extract_the_data_you_are_interested_in(file_prompt_result.value)
                consent_prompt = ph.generate_review_data_prompt(
                    description=REVIEW_DATA_DESCRIPTION, 
                    table_list=extracted_data
                )
                result = yield ph.render_page(REVIEW_DATA_HEADER, consent_prompt)
                if result.__type__ == "PayloadJSON":
                    reviewed_data = result.value
                    yield ph.donate(f"{session_id}", reviewed_data)
                if result.__type__ == "PayloadFalse":
                    value = json.dumps('{"status" : "data_submission declined"}')
                    yield ph.donate(f"{session_id}", value)


                break

            # Sad flow:
            # The data was not valid, ask the participant to retry
            if is_data_valid == False:
                retry_prompt = ph.generate_retry_prompt(platform_name)
                retry_prompt_result = yield ph.render_page(RETRY_HEADER, retry_prompt)

                # The participant wants to retry: start from the beginning
                if retry_prompt_result.__type__ == 'PayloadTrue':
                    continue
                # The participant does not want to retry or pressed skip
                else:
                    break

        # The participant did not submit a file and pressed skip
        else:
            break

    yield ph.exit(0, "Success")


def extract_the_data_you_are_interested_in(zip_file: str) -> list[d3i_props.PropsUIPromptConsentFormTableViz]:
    """
    This function extracts the data the researcher is interested in

    In this case we extract from the zipfile:
    * The file names
    * The compressed file size
    * The file size

    You could extract anything here
    """
    tables = []

    try:
        file = zipfile.ZipFile(zip_file)
        data = []
        for name in file.namelist():
            info = file.getinfo(name)
            data.append((name, info.compress_size, info.file_size))

        df = pd.DataFrame(data, columns=["File name", "Compressed file size", "File size"]) #pyright: ignore
        table_title = props.Translatable({
            "en": f"The contents of your zipfile contents:",
            "nl": "De inhoud van uw zip bestand"
        })
        wordcloud = {
            "title": {
                "en": "You can also add visualizations", 
                "nl": "You can also add visualizations"
            },
            "type": "wordcloud",
            "textColumn": "File name",
            "tokenize": True,
        }
        tables.append(
            d3i_props.PropsUIPromptConsentFormTableViz(
                id="zip_contents", 
                title=table_title, 
                data_frame=df, 
                visualizations=[wordcloud], 
                delete_option=True
            )
        )

    except Exception as e:
        print(f"Something went wrong: {e}")

    return tables


def validate_the_participants_input(zip_file: str) -> bool:
    """
    Check if the participant actually submitted a zipfile
    Returns True if participant submitted a zipfile, otherwise False

    In reality you need to do a lot more validation.
    Some things you could check:
    - Check if the the file(s) are the correct format (json, html, binary, etc.)
    - If the files are in the correct language
    """

    try:
        with zipfile.ZipFile(zip_file) as zf:
            return True
    except zipfile.BadZipFile:
        return False


