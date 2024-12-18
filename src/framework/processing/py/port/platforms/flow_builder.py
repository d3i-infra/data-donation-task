"""
Flow Builder

This module contains tools to create data donation flows
"""

import logging
import json
import io

import port.helpers.port_helpers as ph
import port.helpers.validate as validate

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s --- %(name)s --- %(levelname)s --- %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

 
def should_yield(func):
    func.is_yieldable = True
    return func


def is_yieldable(func):
    return getattr(func, 'is_yieldable', False)


class DataDonationFlow:
    def __init__(self, platform_name, ddp_categories, texts, functions, session_id, is_donate_logs):
        self.name = platform_name
        self.ddp_categories = ddp_categories
        self.texts = texts
        self.functions = functions
        self.session_id = session_id
        self.is_donate_logs = is_donate_logs
        self.log_stream = io.StringIO()
        self.steps = []
        self._configure_logger()

    def _configure_logger(self):
        if self.is_donate_logs:
            handler_stream = self.log_stream
            logger.handlers = [] # clear handler
            handler = logging.StreamHandler(handler_stream)
            handler.setLevel(logging.INFO)
            handler.setFormatter(
                logging.Formatter(
                    fmt="%(asctime)s --- %(name)s --- %(levelname)s --- %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S%z"
                )
            )
            logger.addHandler(handler)

    def donate_logs(self):
        log_string = self.log_stream.getvalue()
        if log_string:
            log_data = log_string.split("\n")
        else:
            log_data = ["no logs"]

        return ph.donate(f"{self.session_id}-tracking.json", json.dumps(log_data))

    def add_step(self, step_function):
        self.steps.append(step_function)
        return self

    def initialize_default_flow(self):
        self.add_step(prompt_file_and_validate_input)
        self.add_step(extract_data)
        self.add_step(review_data)
        self.add_step(exit_flow)
        return self

    def run(self):
        logger.info("Starting data donation flow for %s", self.name)
        print(self.name)
        if self.is_donate_logs:
            yield self.donate_logs()

        data = None
        for  step in self.steps:
            if is_yieldable(step):
                data = yield from step(self, data)
            else:
                data = step(self, data)

            if self.is_donate_logs:
                yield self.donate_logs()


@should_yield
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


def extract_data(flow, zip):
    table_list = flow.functions["extraction"](zip)
    return table_list


@should_yield
def review_data(flow, table_list):
    if table_list != None:
        logger.info("Ask participant to review data; %s", flow.name)
        review_data_prompt = ph.generate_review_data_prompt(f"{flow.session_id}-chatgpt", flow.texts["review_data_description"], table_list)
        yield ph.render_page(flow.texts["review_data_header"], review_data_prompt)
    else:
        logger.info("No data got extracted %s", flow.name)


@should_yield
def exit_flow(_, __):
    yield ph.exit(0, "Success")
    yield ph.render_end_page()
