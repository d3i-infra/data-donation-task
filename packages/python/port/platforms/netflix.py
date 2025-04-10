"""
Netflix

This module provides an example flow of a Netflix data donation study
"""
import logging

import pandas as pd

import port.api.props as props
import port.api.d3i_props as d3i_props
import port.helpers.extraction_helpers as eh
import port.helpers.validate as validate
import port.helpers.port_helpers as ph
from port.platforms.flow_builder import FlowBuilder

from port.helpers.validate import (
    DDPCategory,
    DDPFiletype,
    Language,
)

logger = logging.getLogger(__name__)

DDP_CATEGORIES = [
    DDPCategory(
        id="csv",
        ddp_filetype=DDPFiletype.CSV,
        language=Language.EN,
        known_files=["MyList.csv", "ViewingActivity.csv", "SearchHistory.csv", "IndicatedPreferences.csv", "PlaybackRelatedEvents.csv", "InteractiveTitles.csv", "Ratings.csv", "GamePlaySession.txt", "IpAddressesLogin.csv", "IpAddressesAccountCreation.txt", "IpAddressesStreaming.csv", "Additional Information.pdf", "MessagesSentByNetflix.csv", "SocialMediaConnections.txt", "AccountDetails.csv", "ProductCancellationSurvey.txt", "CSContact.csv", "ChatTranscripts.csv", "Cover sheet.pdf", "Devices.csv", "ParentalControlsRestrictedTitles.txt", "AvatarHistory.csv", "Profiles.csv", "Clickstream.csv", "BillingHistory.csv"]
    )
]

def extract_users(netflix_zip) -> list[str]:
    """
    Extracts all users from a netflix csv file 
    This function expects all users to be present in the first column of a pd.DataFrame
    """

    b = eh.extract_file_from_zip(netflix_zip, "ViewingActivity.csv")
    df = eh.read_csv_from_bytes_to_df(b)
    out = []
    try:
        out: list[str] = df[df.columns[0]].unique().tolist()
        out.sort()
    except Exception as e:
        logger.error("Cannot extract users: %s", e)

    return out
    

def keep_user(df: pd.DataFrame, selected_user: str) -> pd.DataFrame:
    """
    Keep only the rows where the first column of df
    is equal to selected_user
    """
    try:
        df =  df.loc[df.iloc[:, 0] == selected_user].reset_index(drop=True)
    except Exception as e:  
        logger.info(e)

    return df

    
def netflix_to_df(netflix_zip: str, file_name: str, selected_user: str) -> pd.DataFrame:
    """
    netflix csv to df
    returns empty df in case of error
    """
    ratings_bytes = eh.extract_file_from_zip(netflix_zip, file_name)
    df = eh.read_csv_from_bytes_to_df(ratings_bytes)
    df = keep_user(df, selected_user)

    return df


def ratings_to_df(netflix_zip: str, selected_user: str)  -> pd.DataFrame:
    """
    Extract ratings from netflix zip to df
    Only keep the selected user
    """

    columns_to_keep = ["Title Name", "Thumbs Value", "Event Utc Ts"]
    columns_to_rename =  {
        "Title Name": "Titel",
        "Event Utc Ts": "Datum en tijd",
        "Thumbs Value": "Aantal duimpjes omhoog"
    }

    df = netflix_to_df(netflix_zip, "Ratings.csv", selected_user)

    # Extraction logic here
    try:
        if not df.empty:
            df = df[columns_to_keep]
            df = df.rename(columns=columns_to_rename) #pyright: ignore
    except Exception as e:
        logger.error("Data extraction error: %s", e)
        
    return df



def time_string_to_hours(time_str):
    try:
        # Split the time string into hours, minutes, and seconds
        hours, minutes, seconds = map(int, time_str.split(':'))

        # Convert each component to hours
        hours_in_seconds = hours * 3600
        minutes_in_seconds = minutes * 60

        # Sum up the converted values
        total_hours = (hours_in_seconds + minutes_in_seconds + seconds) / 3600
    except:
        return 0

    return round(total_hours, 3)


def viewing_activity_to_df(netflix_zip: str, selected_user: str)  -> pd.DataFrame:
    """
    Extract ViewingActivity from netflix zip to df
    Only keep the selected user
    """

    columns_to_keep = ["Start Time","Duration","Title","Supplemental Video Type"]
    columns_to_rename =  {
        "Start Time": "Start tijd",
        "Title": "Titel",
        "Supplemental Video Type": "Aanvullend informatie",
        "Duration": "Aantal uur gekeken"
    }

    df = netflix_to_df(netflix_zip, "ViewingActivity.csv", selected_user)
    remove_values = ["TEASER_TRAILER", "HOOK", "TRAILER", "CINEMAGRAPH"]

    try:
        if not df.empty:
            df = df[columns_to_keep]
            df = df[~df["Supplemental Video Type"].isin(remove_values)].reset_index(drop=True)
            df = df.rename(columns=columns_to_rename)

        df['Aantal uur gekeken'] = df['Aantal uur gekeken'].apply(time_string_to_hours)
        df = df.sort_values(by='Start tijd', ascending=True).reset_index(drop=True)
    except Exception as e:
        logger.error("Data extraction error: %s", e)
        
    return df



def extraction(netflix_zip: str, selected_user: str) -> list[d3i_props.PropsUIPromptConsentFormTableViz]:
    tables = [
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="netflix_ratings",
            data_frame=ratings_to_df(netflix_zip, selected_user),
            title=props.Translatable({
                "en": "Your ratings on Netflix",
                "nl": "Uw beoordelingen op Netflix"
            }),
            description=props.Translatable({
                "en": "Click 'Show Table' to view these ratings per row.", 
                "nl": "Klik op ‘Tabel tonen’ om deze beoordelingen per rij te bekijken."
            }),
            visualizations=[
                 {
                    "title": {
                        "en": "Titles rated by thumbs value", 
                        "nl": "Gekeken titles, grootte is gebasseerd op het aantal duimpjes omhoog"
                    },
                    "type": "wordcloud",
                    "textColumn": "Titel",
                    "valueColumn": "Aantal duimpjes omhoog",
                },
            ]
        ),
        d3i_props.PropsUIPromptConsentFormTableViz(
            id="netflix_viewing_activity",
            data_frame=viewing_activity_to_df(netflix_zip, selected_user),
            title= props.Translatable({
                "en": "What you watched",
                "nl": "Wanneer kijkt u Netflix"
            }),
            description=props.Translatable({
                "en": "This table shows what titles you watched when and for how long.", 
                "nl": "Klik op ‘Tabel tonen’ om voor elke keer dat u iets op Netflix heeft gekeken te zien welke serie of film dit was, wanneer u dit heeft gekeken, hoe lang u het heeft gekeken."
            }),
            visualizations=[
                {
                    "title": {
                        "en": "Total hours watched per month of the year", 
                        "nl": "Totaal aantal uren gekeken per maand van het jaar"
                    },
                    "type": "area",
                    "group": {
                        "column": "Start tijd",
                        "dateFormat": "month",
                        "label": "Month"
                    },
                    "values": [{
                        "column": "Aantal uur gekeken",
                        "aggregate": "sum",
                    }]
                },
                {
                    "title": {
                        "en": "Total hours watch by hour of the day",
                        "nl": "Totaal aantal uur gekeken op uur van de dag"
                    },
                    "type": "bar",
                    "group": {
                        "column": "Start tijd",
                        "dateFormat": "hour_cycle"
                    },
                    "values": [{
                        "column": "Aantal uur gekeken",
                        "aggregate": "sum",
                    }]
                }
            ]
        ),
    ]

    return [table for table in tables if not table.data_frame.empty]


class NetflixFlow(FlowBuilder):
    def __init__(self, session_id: int):
        super().__init__(session_id, "Netflix")
        
    def validate_file(self, file):
        return validate.validate_zip(DDP_CATEGORIES, file)
        
    def extract_data(self, file, validation):
        selected_user = ""
        users = extract_users(file)

        if len(users) == 1:
            selected_user = users[0]
            return extraction(file, selected_user)
        elif len(users) > 1:
            title = props.Translatable({
                "en": "Select your Netflix profile name",
                "nl": "Kies jouw Netflix profielnaam"
            })
            empty_text = props.Translatable({ "en": "", "nl": "" })
            radio_prompt = ph.generate_radio_prompt(title, empty_text, users)
            selection = yield ph.render_page(empty_text, radio_prompt)
            selected_user = selection.value
            print(selected_user)
            return extraction(file, selected_user)


def process(session_id):
    flow = NetflixFlow(session_id)
    return flow.start_flow()
