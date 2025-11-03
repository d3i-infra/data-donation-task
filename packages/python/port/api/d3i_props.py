from dataclasses import dataclass
from typing import Optional

import pandas as pd

import port.api.props as props

@dataclass
class PropsUIPromptConsentFormTableViz:
    """
    Table to be shown to the participant prior to donation.

    Attributes:
        id (str): A unique string to identify the table after donation.
        title (Translatable): Title of the table.
        data_frame (pd.DataFrame | Dict[str, Dict[str, Any]]): Table to be shown can be a pandas DataFrame or a dictionary.
        description (Optional[Translatable]): Optional description of the table.
        visualizations (Optional[list]): Optional visualizations to be shown.
        folded (Optional[bool]): Whether the table should be initially folded.
        delete_option (Optional[bool]): Whether to show a delete option for the table.

    Examples::

        data_frame_df = pd.DataFrame([
            {"column1": 1, "column2": 4},
            {"column1": 2, "column2": 5},
            {"column1": 3, "column2": 6},
        ])
        
        example1 = PropsUIPromptConsentFormTable(
            id="example1",
            title=Translatable("Table as DataFrame"),
            data_frame=data_frame_df,
        )

        data_frame_dict = {
            "column1": {"0": 1, "1": 4},
            "column2": {"0": 2, "1": 5},
            "column3": {"0": 3, "1": 6},
        }
        
        example2 = PropsUIPromptConsentFormTable(
            id="example2",
            title=Translatable("Table as Dictionary"),
            data_frame=data_frame_dict,
        )
    """
    id: str
    title: props.Translatable
    data_frame: pd.DataFrame
    description: Optional[props.Translatable] = None
    visualizations: Optional[list] = None
    folded: Optional[bool] = False
    delete_option: Optional[bool] = True

    def translate_data_frame(self):
        if isinstance(self.data_frame, pd.DataFrame):
            return self.data_frame.to_json()
        else:
            return self.data_frame

    def toDict(self):
        """
        Convert the object to a dictionary.

        Returns:
            dict: A dictionary representation of the object.
        """
        dict = {}
        dict["__type__"] = "PropsUIPromptConsentFormTableViz"
        dict["id"] = self.id
        dict["title"] = self.title.toDict()
        dict["data_frame"] = self.translate_data_frame()
        dict["description"] = self.description.toDict() if self.description else None
        dict["visualizations"] = self.visualizations if self.visualizations else None
        dict["folded"] = self.folded
        dict["delete_option"] = self.delete_option
        return dict


@dataclass
class PropsUIPromptConsentFormViz:
    """
    Tables to be shown to the participant prior to donation.

    Attributes:
        id (str): will be used as part of the filename when the data is stored
        tables (list[PropsUIPromptConsentFormTable]): A list of tables.
        description (Optional[Translatable]): Optional description of the consent form.
        donate_question (Optional[Translatable]): Optional donation question.
        donate_button (Optional[Translatable]): Optional text for the donate button.
    """
    tables: list[PropsUIPromptConsentFormTableViz]
    description: Optional[props.Translatable] = None
    donate_question: Optional[props.Translatable] = None
    donate_button: Optional[props.Translatable] = None

    def translate_tables(self):
        """
        Translate the tables to a list of dictionaries.

        Returns:
            list: A list of dictionaries representing the tables.
        """
        output = []
        for table in self.tables:
            output.append(table.toDict())
        return output

    def toDict(self):
        """
        Convert the object to a dictionary.

        Returns:
            dict: A dictionary representation of the object.
        """
        dict = {}
        dict["__type__"] = "PropsUIPromptConsentFormViz"
        dict["tables"] = self.translate_tables()
        dict["description"] = self.description and self.description.toDict()
        dict["donateQuestion"] = self.donate_question and self.donate_question.toDict()
        dict["donateButton"] = self.donate_button and self.donate_button.toDict()
        return dict


@dataclass
class PropsUIPromptFileInputMultiple:
    """
    Prompt the user to submit multiple files.

    Attributes:
        description (Translatable): Text with an explanation.
        extensions (str): Accepted mime types, example: "application/zip, text/plain".
    """
    description: props.Translatable
    extensions: str

    def toDict(self):
        """
        Convert the object to a dictionary.

        Returns:
            dict: A dictionary representation of the object.
        """
        dict = {}
        dict["__type__"] = "PropsUIPromptFileInputMultiple"
        dict["description"] = self.description.toDict()
        dict["extensions"] = self.extensions
        return dict


@dataclass
class PropsUIQuestionOpen:
    """
    Open-ended question.

    Attributes:
        id (int): Question ID.
        question (Translatable): The question text.
    """
    id: int
    question: props.Translatable

    def toDict(self):
        """
        Convert the object to a dictionary.

        Returns:
            dict: A dictionary representation of the object.
        """
        dict = {}
        dict["__type__"] = "PropsUIQuestionOpen"
        dict["id"] = self.id
        dict["question"] = self.question.toDict()
        return dict


@dataclass
class PropsUIQuestionMultipleChoiceCheckbox:
    """
    Multiple choice question with checkboxes.

    Attributes:
        id (int): Question ID.
        question (Translatable): The question text.
        choices (list[Translatable]): List of choices.
    """
    id: int
    question: props.Translatable
    choices: list[props.Translatable]

    def toDict(self):
        """
        Convert the object to a dictionary.

        Returns:
            dict: A dictionary representation of the object.
        """
        dict = {}
        dict["__type__"] = "PropsUIQuestionMultipleChoiceCheckbox"
        dict["id"] = self.id
        dict["question"] = self.question.toDict()
        dict["choices"] = [c.toDict() for c in self.choices]
        return dict


@dataclass
class PropsUIQuestionMultipleChoice:
    """
    Multiple choice question with radio buttons.

    Attributes:
        id (int): Question ID.
        question (Translatable): The question text.
        choices (list[Translatable]): List of choices.
    """
    id: int
    question: props.Translatable
    choices: list[props.Translatable]

    def toDict(self):
        """
        Convert the object to a dictionary.

        Returns:
            dict: A dictionary representation of the object.
        """
        dict = {}
        dict["__type__"] = "PropsUIQuestionMultipleChoice"
        dict["id"] = self.id
        dict["question"] = self.question.toDict()
        dict["choices"] = [c.toDict() for c in self.choices]
        return dict


@dataclass
class PropsUIPromptQuestionnaire:
    """
    Questionnaire containing multiple questions.

    Attributes:
        description (Translatable): Description of the questionnaire.
        questions (list[PropsUIQuestionMultipleChoice | PropsUIQuestionMultipleChoiceCheckbox | PropsUIQuestionOpen]):
            List of questions in the questionnaire.
    """
    description: props.Translatable
    questions: list[
        PropsUIQuestionMultipleChoice | 
        PropsUIQuestionMultipleChoiceCheckbox | 
        PropsUIQuestionOpen
    ]

    def toDict(self):
        """
        Convert the object to a dictionary.

        Returns:
            dict: A dictionary representation of the object.
        """
        dict = {}
        dict["__type__"] = "PropsUIPromptQuestionnaire"
        dict["description"] = self.description.toDict()
        dict["questions"] = [q.toDict() for q in self.questions]
        return dict


@dataclass
class PropsUIPromptRetry:
    """Retry submitting a file page

    Prompt the user if they want to submit a new file.
    This can be used in case a file could not be processed.

    Attributes:
        text: message to display
        ok: message to display if the user wants to try again
    """

    text: props.Translatable
    ok: props.Translatable

    def toDict(self):
        dict = {}
        dict["__type__"] = "PropsUIPromptRetry"
        dict["text"] = self.text.toDict()
        dict["ok"] = self.ok.toDict()
        return dict
