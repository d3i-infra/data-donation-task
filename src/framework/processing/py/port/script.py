import port.api.props as props
from port.api.commands import (CommandSystemDonate, CommandUIRender, CommandSystemExit)

import pandas as pd
from pyodide.http import open_url
import yaml

from port.logic import DDFactory
from port.logic import DDYoutube


def process(session_id: str):
    
    config = yaml.safe_load(open_url('../../config.yml'))

    dd_factory = DDFactory()

    # These are the available platforms
    dd_factory.register_dd('YouTube', DDYoutube)

    for platform in config:
        
        dd = dd_factory.create(platform, config.get(platform))

        # Start of the data donation flow
        while True:
            # Ask the participant to submit a file
            file_prompt = prompt_file(*dd.file_input())
            file_prompt_result = yield render_page(platform, file_prompt)

            # If the participant submitted a file: continue
            if file_prompt_result.__type__ == 'PayloadString':

                # Validate the file the participant submitted
                # In general this is wise to do 
                validation = dd.validate_zip(file_prompt_result.value)

                # Happy flow:
                # The file the participant submitted is valid
                if validation.status_code.id == 0:

                    # Extract the data you as a researcher are interested in, and put it in a pandas DataFrame
                    # Show this data to the participant in a table on screen
                    # The participant can now decide to donate
                    table_list = dd.extract(file_prompt_result.value, validation)
                    consent_prompt = prompt_consent(table_list)
                    consent_prompt_result = yield render_page(platform, consent_prompt)

                    # If the participant wants to donate the data gets donated
                    if consent_prompt_result.__type__ == "PayloadJSON":
                        yield donate(platform, consent_prompt_result.value)

                    break

                # Sad flow:
                # The data was not valid, ask the participant to retry
                if validation.status_code.id != 0:
                    retry_prompt = retry_confirmation(platform)
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

    yield exit(0, "Success")
    yield render_end_page()


####################################################################
# script.py helpers

def prompt_consent(table_list: list) -> props.PropsUIPromptConsentForm:
    """
    Assembles all donated data in consent form to be displayed
    """
    consent_form_tables = []
    for table in table_list:
        table_title = props.Translatable(table["title"])
        table_description = props.Translatable(table["description"])
        table = props.PropsUIPromptConsentFormTable(
            table["name"], 
            table_title, 
            table["df"], 
            table_description, 
            table["visualizations"]
        )
        consent_form_tables.append(table)
        
    return props.PropsUIPromptConsentForm(consent_form_tables, [])


def donate_logs(key):
    log_string = LOG_STREAM.getvalue()  # read the log stream
    if log_string:
        log_data = log_string.split("\n")
    else:
        log_data = ["no logs"]

    return donate(key, json.dumps(log_data))


def create_empty_table(platform_name: str) -> props.PropsUIPromptConsentFormTable:
    """
    Show something in case no data was extracted
    """
    title = props.Translatable({
       "en": "Nothing went wrong, but we couldn't find any data in your files",
       "nl": "Er ging niks mis, maar we konden geen gegevens in jouw data vinden",
    })
    df = pd.DataFrame(["No data found"], columns=["No data found"])
    table = props.PropsUIPromptConsentFormTable(f"{platform_name}_no_data_found", title, df)
    return table


def render_end_page():
    page = props.PropsUIPageEnd()
    return CommandUIRender(page)


def render_page(platform, body):
    header = props.PropsUIHeader(props.Translatable(
        {
            "en": platform, 
            "nl": "Je " + platform + " geschiedenis"
        }
    ))
    footer = props.PropsUIFooter()
    page = props.PropsUIPageDonation(platform, header, body, footer)
    return CommandUIRender(page)


def retry_confirmation(platform):
    text = props.Translatable(
        {
            "en": f"Unfortunately, we could not process your {platform} file. If you are sure that you selected the correct file, press Continue. To select a different file, press Try again.",
            "nl": f"Helaas, kunnen we je {platform} bestand niet verwerken. Weet je zeker dat je het juiste bestand hebt gekozen? Ga dan verder. Probeer opnieuw als je een ander bestand wilt kiezen."
        }
    )
    ok = props.Translatable({"en": "Try again", "nl": "Probeer opnieuw"})
    cancel = props.Translatable({"en": "Continue", "nl": "Verder"})
    return props.PropsUIPromptConfirm(text, ok, cancel)


def prompt_file(description, extensions):
    return props.PropsUIPromptFileInput(props.Translatable(description), extensions)


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)


def exit(code, info):
    return CommandSystemExit(code, info)


def donate_status(filename: str, message: str):
    return donate(filename, json.dumps({"status": message}))


def donate_dict(platform_name: str, d: dict):
    for k, v in d.items():
        donation_str = json.dumps({k: v})
        yield donate(f"{platform_name}_{k}", donation_str)