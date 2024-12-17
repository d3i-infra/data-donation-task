"""
Flow Builder

This module contains an example flow of a ChatGPT data donation study
"""
import logging

import port.helpers.port_helpers as ph
import port.helpers.validate as validate

logger = logging.getLogger(__name__)


class DataDonationFlow:
    def __init__(self, platform_name, ddp_categories, texts, extraction_fun, session_id):
        self.name = platform_name,
        self.ddp_categories = ddp_categories
        self.texts = texts
        self.extraction = extraction_fun
        self.session_id = session_id
        self.steps = []

    def set_session_id(self, session_id):
        self.session_id = session_id

    def add_step(self, step_function):
        self.steps.append(step_function)
        return self

    def initialize_default_flow(self):
        self.add_step(prompt_file_and_validate_input)
        self.add_step(extract_and_review_data)
        self.add_step(exit_flow)
        return self

    def run(self):
        logger.info("Starting data donation flow for %s", self.name)
        
        data = None
        for  step in self.steps:
            data = yield from step(self, data)
        
        logger.info("Flow completed %s", self.name)
        return data


def prompt_file_and_validate_input(flow, _):
    logger.info("Prompt for file step for %s", flow.name)
    ddp_zip = None

    while True:
        file_prompt = ph.generate_file_prompt("application/zip")
        file_result = yield ph.render_page(flow.texts["submit_file_header"], file_prompt)

        validation = validate.validate_zip(flow.ddp_categories, file_result.value)

        # Happy flow: Valid DDP
        if validation.get_status_code_id() == 0:
            logger.info("Validation of DDP was succesfull for %s", flow.name)
            ddp_zip = file_result.value

            break

        # Enter retry flow
        if validation.get_status_code_id() != 0:
            logger.info("DDP did not pass validation; prompt retry_confirmation", flow.name)
            retry_prompt = ph.generate_retry_prompt(flow.name)
            retry_result = yield ph.render_page(flow.texts["retry_header"], retry_prompt)

            if retry_result.__type__ == "PayloadTrue":
                continue
            else:
                logger.info("Skipped during retry flow")
                break

    return ddp_zip


def extract_and_review_data(flow, zip):
    table_list = flow.extraction(zip)
    if table_list != None:
        logger.info("Ask participant to review data; %s", flow.name)
        review_data_prompt = ph.generate_review_data_prompt(f"{flow.session_id}-chatgpt", flow.texts["review_data_description"], table_list)
        yield ph.render_page(flow.texts["review_data_header"], review_data_prompt)
    else:
        logger.info("No data got extracted %s", flow.name)


def exit_flow(_, __):
    yield ph.exit(0, "Success")
    yield ph.render_end_page()

