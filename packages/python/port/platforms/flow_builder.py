"""
This module contains a flow builder

The flow builder provides an interface to easily maintain the most commonly used data donation flows for various platforms
"""
from abc import abstractmethod
from typing import Generator
import json
import logging
import json

import port.api.props as props
import port.api.d3i_props as d3i_props
import port.helpers.port_helpers as ph
import port.helpers.validate as validate

logger = logging.getLogger(__name__)

class FlowBuilder:
    def __init__(self, session_id: int, platform_name: str):
        self.session_id = session_id
        self.platform_name = platform_name
        self.table_list = []
        
        self._initialize_ui_text()
        
    def _initialize_ui_text(self):
        """Initialize UI text based on platform name"""
        self.UI_TEXT = {
            "submit_file_header": props.Translatable({
                "en": f"Select your {self.platform_name} file", 
                "nl": f"Selecteer uw {self.platform_name} bestand"
            }),
            
            "review_data_header": props.Translatable({
                "en": f"Your {self.platform_name} data", 
                "nl": f"Uw {self.platform_name} gegevens"
            }),
            
            "retry_header": props.Translatable({
                "en": "Try again", 
                "nl": "Probeer opnieuw"
            }),
            
            "review_data_description": props.Translatable({
                "en": f"Below you will find a curated selection of {self.platform_name} data.",
                "nl": f"Below you will find a curated selection of {self.platform_name} data.",
            })
        }
        
    def start_flow(self):
        """
        Main processing loop for all platforms
        """
        while True:
            logger.info(f"Prompt for file for {self.platform_name}")
            file_prompt = self.generate_file_prompt()
            file_result = yield ph.render_page(self.UI_TEXT["submit_file_header"], file_prompt)
            
            if file_result.__type__ == "PayloadString":
                validation =self.validate_file(file_result.value)
                
                # Happy flow: Valid file
                if validation.get_status_code_id() == 0:
                    logger.info(f"Payload for {self.platform_name}")
                    self.table_list = self.extract_data(file_result.value, validation)
                    if isinstance(self.table_list, Generator):
                        self.table_list = yield from self.table_list

                    break
                    
                # Enter retry flow
                if validation.get_status_code_id() != 0:
                    logger.info(f"Not a valid {self.platform_name} file; No payload; prompt retry_confirmation")
                    retry_prompt = self.generate_retry_prompt()
                    retry_result = yield ph.render_page(self.UI_TEXT["retry_header"], retry_prompt)
                    if retry_result.__type__ == "PayloadTrue":
                        continue
                    else:
                        logger.info("Skipped during retry flow")
                        break
            else:
                logger.info("Skipped at file selection ending flow")
                break
                
        if self.table_list is not None:
            logger.info(f"Prompt consent; {self.platform_name}")
            review_data_prompt = self.generate_review_data_prompt()
            result = yield ph.render_page(self.UI_TEXT["review_data_header"], review_data_prompt)

            if result.__type__ == "PayloadJSON":
                reviewed_data = result.value
                yield ph.donate(f"{self.session_id}", reviewed_data)
            if result.__type__ == "PayloadFalse":
                value = json.dumps('{"status" : "data_submission declined"}')
                yield ph.donate(f"{self.session_id}", value)
            
        yield ph.exit(0, "Success")
    
    # Methods to be overridden by platform-specific implementations
    def generate_file_prompt(self):
        """Generate platform-specific file prompt"""
        return ph.generate_file_prompt("application/zip")

    @abstractmethod
    def validate_file(self, file: str) -> validate.ValidateInput:
        """Validate the file according to platform-specific rules"""
        raise NotImplementedError("Must be implemented by subclass")
        
    @abstractmethod
    def extract_data(self, file: str, validation: validate.ValidateInput) -> list[d3i_props.PropsUIPromptConsentFormTableViz]:
        """Extract data from file using platform-specific logic"""
        raise NotImplementedError("Must be implemented by subclass")
        
    def generate_retry_prompt(self):
        """Generate platform-specific retry prompt"""
        return ph.generate_retry_prompt(self.platform_name)
        
    def generate_review_data_prompt(self):
        """Generate platform-specific review data prompt"""
        return ph.generate_review_data_prompt(
            description=self.UI_TEXT["review_data_description"],
            table_list=self.table_list
        )


