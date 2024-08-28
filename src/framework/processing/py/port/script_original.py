import port.api.props as props
from port.api.commands import (CommandSystemDonate, CommandUIRender, CommandSystemExit)

import pandas as pd
import zipfile

def process(session_id: str):
    platform = "Platform of interest"

    # Start of the data donation flow
    while True:
        # Ask the participant to submit a file
        file_prompt = generate_file_prompt(platform, "application/zip, text/plain")
        file_prompt_result = yield render_page(platform, file_prompt)

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
                consent_prompt = generate_consent_prompt(extracted_data)
                consent_prompt_result = yield render_page(platform, consent_prompt)

                # If the participant wants to donate the data gets donated
                if consent_prompt_result.__type__ == "PayloadJSON":
                    yield donate(f"{session_id}-{platform}", consent_prompt_result.value)

                break

            # Sad flow:
            # The data was not valid, ask the participant to retry
            if is_data_valid == False:
                retry_prompt = generate_retry_prompt(platform)
                retry_prompt_result = yield render_page(platform, retry_prompt)

                # The participant wants to retry: start from the beginning
                if retry_prompt_result.__type__ == 'PayloadTrue':
                    continue
                # The participant does not want to retry or pressed skip
                else:
                    break

        # The participant did not submit a file and pressed skip
        else:
            break

    yield exit_port(0, "Success")
    yield render_end_page()


def extract_the_data_you_are_interested_in(zip_file: str) -> pd.DataFrame:
    """
    This function extracts the data the researcher is interested in

    In this case we extract from the zipfile:
    * The file names
    * The compressed file size
    * The file size

    You could extract anything here
    """
    out = pd.DataFrame()

    try:
        file = zipfile.ZipFile(zip_file)
        data = []
        for name in file.namelist():
            info = file.getinfo(name)
            data.append((name, info.compress_size, info.file_size))

        out = pd.DataFrame(data, columns=["File name", "Compressed file size", "File size"])

    except Exception as e:
        print(f"Something went wrong: {e}")

    return out


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


def render_end_page():
    """
    Renders a thank you page
    """
    page = props.PropsUIPageEnd()
    return CommandUIRender(page)


def render_page(platform: str, body):
    """
    Renders the UI components
    """
    header = props.PropsUIHeader(props.Translatable({"en": platform, "nl": platform }))
    footer = props.PropsUIFooter()
    page = props.PropsUIPageDonation(platform, header, body, footer)
    return CommandUIRender(page)


def generate_retry_prompt(platform: str) -> props.PropsUIPromptConfirm:
    text = props.Translatable({
        "en": f"Unfortunately, we cannot process your {platform} file. Continue, if you are sure that you selected the right file. Try again to select a different file.",
        "nl": f"Helaas, kunnen we uw {platform} bestand niet verwerken. Weet u zeker dat u het juiste bestand heeft gekozen? Ga dan verder. Probeer opnieuw als u een ander bestand wilt kiezen."
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


def generate_file_prompt(platform, extensions) -> props.PropsUIPromptFileInput:
    description = props.Translatable({
        "en": f"Please follow the download instructions and choose the file that you stored on your device. Click “Skip” at the right bottom, if you do not have a {platform} file. ",
        "nl": f"Volg de download instructies en kies het bestand dat u opgeslagen heeft op uw apparaat. Als u geen {platform} bestand heeft klik dan op “Overslaan” rechts onder."
    })
    return props.PropsUIPromptFileInput(description, extensions)


def generate_consent_prompt(*args: pd.DataFrame) -> props.PropsUIPromptConsentForm:
    description = props.Translatable({
       "en": "Below you will find meta data about the contents of the zip file you submitted. Please review the data carefully and remove any information you do not wish to share. If you would like to share this data, click on the 'Yes, share for research' button at the bottom of this page. By sharing this data, you contribute to research <insert short explanation about your research here>.",
       "nl": "Hieronder ziet u gegevens over de zip die u heeft ingediend. Bekijk de gegevens zorgvuldig, en verwijder de gegevens die u niet wilt delen. Als u deze gegevens wilt delen, klik dan op de knop 'Ja, deel voor onderzoek' onderaan deze pagina. Door deze gegevens te delen draagt u bij aan onderzoek over <korte zin over het onderzoek>."
    })

    donate_question = props.Translatable({
       "en": "Do you want to share this data for research?",
       "nl": "Wilt u deze gegevens delen voor onderzoek?"
    })

    donate_button = props.Translatable({
       "en": "Yes, share for research",
       "nl": "Ja, deel voor onderzoek"
    })

    tables = [] 
    for index, df in enumerate(args):
        table_title = props.Translatable({
            "en": f"The contents of your zipfile contents (Table {index + 1}/{len(args)})",
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
        tables.append(props.PropsUIPromptConsentFormTable(f"zip_contents_{index}", table_title, df, visualizations=[wordcloud]))

    return props.PropsUIPromptConsentForm(
       tables,
       [],
       description = description,
       donate_question = donate_question,
       donate_button = donate_button
    )


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)


def exit_port(code, info):
    return CommandSystemExit(code, info)


##################################################################################
# Exercise for the reader

# Add an extra table to the output
# This table should calculate 2 aggegrate statistics about your the files in your zipfile

# 1. it should give the total number of files in the zipfile
# 2. it should give the total number of bytes of all files in the zipfile
# 3. As a bonus: count the number of times the letter a occurs in all text files in the zipfile. By all means use AI to find out how to do this

# Depending on your data the table could look like this:
# | Statistic | Value |
# -----------------------------
# | Total number of files | 12 | 
# | Total number of bytes | 762376 | 
# | Total occurrences of 'a' in text files | 2378 | 


##################################################################################
# Hints

# Hint 1: Write a function that extracts the statistics and put them in a dataframe. 
#  In order to do that you can copy extract_the_data_you_are_interested_in() and then modify it so it extracts the total number of files and bytes

# Hint 2: If you wrote that function, then
# Changes these lines:
# extracted_data = extract_the_data_you_are_interested_in(file_prompt_result.value)
# consent_prompt = generate_consent_prompt(extracted_data)

# to:
# extracted_data = extract_the_data_you_are_interested_in(file_prompt_result.value)
# extracted_data_statistics = extract_statistics_you_are_interested_in(file_prompt_result.value)
# consent_prompt = generate_consent_prompt(extracted_data, extracted_data_statistics)

##################################################################################
# Answer:

# Uncomment all these lines to see the answer in action

#def extract_statistics_you_are_interested_in(zip_file: str) -> pd.DataFrame:
#    """
#    Function that extracts the desired statistics
#    """
#    out = pd.DataFrame()
#    count = 0 
#    total_number_of_bytes = 0
#    total_a_count = 0
#
#    try:
#        file = zipfile.ZipFile(zip_file)
#        for name in file.namelist():
#            info = file.getinfo(name)
#            count += 1
#            total_number_of_bytes += info.file_size
#
#            # Check if the file is a text file
#            # if so, open it and count the letter a
#            if name.endswith('.txt'):
#                with file.open(name) as txt_file:
#                    content = txt_file.read().decode('utf-8')
#                    total_a_count += content.count('a')
#
#        data = [
#            ("Total number of files", count),
#            ("Total number of bytes", total_number_of_bytes),
#            ("Total occurrences of 'a' in text files", total_a_count),
#        ]
#
#        out = pd.DataFrame(data, columns=["Statistic", "Value"])
#
#    except Exception as e:
#        print(f"Something went wrong: {e}")
#
#    return out
#
#
#def process(session_id: str):
#    platform = "Platform of interest"
#
#    # Start of the data donation flow
#    while True:
#        # Ask the participant to submit a file
#        file_prompt = generate_file_prompt(platform, "application/zip, text/plain")
#        file_prompt_result = yield render_page(platform, file_prompt)
#
#        # If the participant submitted a file: continue
#        if file_prompt_result.__type__ == 'PayloadString':
#
#            # Validate the file the participant submitted
#            # In general this is wise to do 
#            is_data_valid = validate_the_participants_input(file_prompt_result.value)
#
#            # Happy flow:
#            # The file the participant submitted is valid
#            if is_data_valid == True:
#
#                # Extract the data you as a researcher are interested in, and put it in a pandas DataFrame
#                # Show this data to the participant in a table on screen
#                # The participant can now decide to donate
#                extracted_data = extract_the_data_you_are_interested_in(file_prompt_result.value)
#                extracted_data_statistics = extract_statistics_you_are_interested_in(file_prompt_result.value)
#                consent_prompt = generate_consent_prompt(extracted_data, extracted_data_statistics)
#                consent_prompt_result = yield render_page(platform, consent_prompt)
#
#                # If the participant wants to donate the data gets donated
#                if consent_prompt_result.__type__ == "PayloadJSON":
#                    yield donate(f"{session_id}-{platform}", consent_prompt_result.value)
#
#                break
#
#            # Sad flow:
#            # The data was not valid, ask the participant to retry
#            if is_data_valid == False:
#                retry_prompt = generate_retry_prompt(platform)
#                retry_prompt_result = yield render_page(platform, retry_prompt)
#
#                # The participant wants to retry: start from the beginning
#                if retry_prompt_result.__type__ == 'PayloadTrue':
#                    continue
#                # The participant does not want to retry or pressed skip
#                else:
#                    break
#
#        # The participant did not submit a file and pressed skip
#        else:
#            break
#
#    yield exit_port(0, "Success")
#    yield render_end_page()
#
