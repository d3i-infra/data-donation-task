"""FlowBuilder — shared per-platform donation flow orchestration.

Subclass this to implement a platform-specific donation flow.
Override validate_file() and extract_data(). Call start_flow()
as a generator from script.py via `yield from`.
"""
from abc import abstractmethod
from collections.abc import Generator
import json
import logging

import port.api.props as props
import port.api.d3i_props as d3i_props
from port.api.file_utils import SeekableBinaryReader
import port.helpers.port_helpers as ph
import port.helpers.validate as validate
import port.helpers.uploads as uploads

logger = logging.getLogger(__name__)


class TaskIncompleteError(Exception):
    """Flow ended without completion. ScriptWrapper maps this to a nonzero
    exit command so the host keeps the task pending (never completed).

    Raised with a reason key only: the fixed (code, info) pair comes from
    EXITS, so a raise site can never put exception or participant text on
    the bridge — exit info crosses it unconsented (ADR-0022/0023). Codes
    are a fork-local convention pending an agreed exit-code contract with
    Eyra; the host only distinguishes 0 from nonzero today (see ADR-0039).
    """

    EXITS = {
        "abandoned": (2, "Participant abandoned the task"),
        "donation_failed": (3, "Donation delivery failed"),
        "upload_rejected": (4, "Upload rejected"),
    }

    def __init__(self, reason: str):
        exit_code, exit_info = self.EXITS[reason]
        super().__init__(exit_info)
        self.reason = reason
        self.exit_code = exit_code
        self.exit_info = exit_info


class FlowBuilder:
    def __init__(self, session_id: str, platform_name: str):
        self.session_id = session_id
        self.platform_name = platform_name
        self._initialize_ui_text()

    def _initialize_ui_text(self):
        """Initialize UI text based on platform name."""
        self.UI_TEXT = {
            "submit_file_header": props.Translatable({
                "en": f"Select your {self.platform_name} file",
                "nl": f"Selecteer uw {self.platform_name} bestand",
                "de": f"Wählen Sie Ihre {self.platform_name}-Datei aus",
                "pl": f"Wybierz swój plik {self.platform_name}",
                "tr": f"{self.platform_name} dosyanı seç",
                "ar": f"اختر ملف {self.platform_name} الخاص بك",
                "ru": f"Выберите ваш файл {self.platform_name}",
                "it": f"Seleziona il tuo file {self.platform_name}",
                "ro": f"Selectează-ți fișierul {self.platform_name}",
                "es": f"Seleccione su archivo de {self.platform_name}",
                "sq": f"Zgjidh skedarin tënd {self.platform_name}",
            }),
            "review_data_header": props.Translatable({
                "en": f"Your {self.platform_name} data",
                "nl": f"Uw {self.platform_name} gegevens",
                "de": f" Ihre {self.platform_name}-Daten",
                "pl": f"Twoje dane {self.platform_name}",
                "tr": f"{self.platform_name} verilerin",
                "ar": f"بيانات {self.platform_name} الخاصة بك",
                "ru": f"Ваши данные {self.platform_name}",
                "it": f"I tuoi dati {self.platform_name}",
                "ro": f"Datele tale {self.platform_name}",
                "es": f"Sus datos de {self.platform_name}",
                "sq": f"Të dhënat e tua {self.platform_name}",
            }),
            "retry_header": props.Translatable({
                "en": "Try again",
                "nl": "Probeer opnieuw",
                "de": "Erneut versuchen",
                "pl": "Spróbuj ponownie",
                "tr": "Tekrar dene",
                "ar": "أعد المحاولة",
                "ru": "Повторить попытку",
                "it": "Riprova",
                "ro": "Încearcă din nou",
                "es": "Intentar de nuevo",
                "sq": "Provo përsëri",
            }),
            "review_data_description": props.Translatable({
                "en": f"Below you will find a curated selection of your {self.platform_name} data. You decide which of the displayed data you would like to share for research. Select the data you do not want to share and click 'Delete'.",

                "nl": f"Hieronder vindt u een zorgvuldig samengestelde selectie van uw {self.platform_name}-gegevens. U bepaalt zelf welke van de weergegeven gegevens u voor onderzoek wilt delen. Selecteer de gegevens die u niet wilt delen en klik op 'Verwijderen'.",

                "de": f"Nachfolgend finden Sie eine ausgewählte Zusammenstellung Ihrer {self.platform_name}-Daten. Sie entscheiden selbst, welche der angezeigten Daten Sie für die Forschung freigeben möchten. Setzen Sie den Haken bei den Daten, die Sie nicht freigeben möchten, und klicken Sie auf 'Löschen'.",

                "pl": f"Poniżej znajduje się wybrany zestaw Twoich danych z {self.platform_name}. Samodzielnie decydujesz, które z wyświetlanych danych chcesz udostępnić do celów badawczych. Zaznacz dane, których nie chcesz udostępniać, a następnie kliknij 'Usuń'.",

                "tr": f"Aşağıda {self.platform_name} verilerinizden seçilmiş bir derleme bulabilirsiniz. Görüntülenen verilerden hangilerini araştırma amacıyla paylaşmak istediğinize siz karar verirsiniz. Paylaşmak istemediğiniz verileri seçin ve 'Sil' düğmesine tıklayın.",

                "ar": f"ستجد أدناه مجموعة مختارة من بياناتك على {self.platform_name}. يمكنك أن تقرر بنفسك أي من البيانات المعروضة ترغب في مشاركتها لأغراض البحث. حدّد البيانات التي لا ترغب في مشاركتها ثم انقر على 'حذف'.",

                "ru": f"Ниже представлена отобранная подборка ваших данных {self.platform_name}. Вы сами решаете, какие из отображаемых данных хотите предоставить для исследования. Отметьте данные, которыми вы не хотите делиться, и нажмите «Удалить».",

                "it": f"Di seguito troverai una selezione dei tuoi dati di {self.platform_name}. Puoi decidere autonomamente quali dei dati visualizzati desideri condividere per la ricerca. Seleziona i dati che non desideri condividere e fai clic su 'Elimina'.",

                "ro": f"Mai jos găsiți o selecție a datelor dumneavoastră de pe {self.platform_name}. Dumneavoastră decideți ce date afișate doriți să puneți la dispoziție pentru cercetare. Selectați datele pe care nu doriți să le distribuiți și faceți clic pe 'Ștergeți'.",

                "es": f"A continuación encontrará una selección de sus datos de {self.platform_name}. Usted decide qué datos de los mostrados desea compartir con fines de investigación. Seleccione los datos que no desea compartir y haga clic en 'Eliminar'.",

                "sq": f"Më poshtë do të gjeni një përzgjedhje të të dhënave tuaja nga {self.platform_name}. Ju vendosni vetë se cilat nga të dhënat e shfaqura dëshironi të ndani për qëllime kërkimore. Përzgjidhni të dhënat që nuk dëshironi të ndani dhe klikoni 'Fshi'.",
            }),
        }

    def start_flow(self):
        """Main per-platform flow: file→materialize→safety→validate→retry→extract→consent→donate.

        This is a generator. script.py calls it via `yield from flow.start_flow()`.
        Control flow rules:
        - continue: retry upload only
        - break: successful extraction, proceed to consent
        - return: terminal paths that ARE completions (exit 0 at the host)
        - raise TaskIncompleteError: terminal paths that are NOT completions —
          ScriptWrapper shows the task-incomplete page and exits nonzero so
          the host keeps the task pending (ADR-0039)

        Flow milestones are sent to the host via explicit CommandSystemLog yields
        (through emit_log). These must be PII-free. Local logger keeps full
        diagnostic detail in browser console only.
        """
        while True:
            # 1. Render file prompt → receive payload
            logger.info("Prompt for file for %s", self.platform_name)
            file_prompt = self.generate_file_prompt()
            yield from ph.emit_log("info", f"[{self.platform_name}] Upload prompt sent")
            file_result = yield ph.render_page(self.UI_TEXT["submit_file_header"], file_prompt)

            # Skip: anything other than a PayloadFile. PayloadString/
            # WORKERFS support was retired with ADR-0026.
            # Distinguish the participant-skip case from an unexpected
            # payload type so a legacy/mismatched worker is observable.
            if file_result.__type__ != "PayloadFile":
                logger.info("Skipped at file selection for %s", self.platform_name)
                yield from ph.emit_log(
                    "info",
                    f"[{self.platform_name}] Upload skipped: type={file_result.__type__}",
                )
                raise TaskIncompleteError("abandoned")

            # AsyncFileAdapter — file-like, passed directly to validators
            # and extractors. Never materialized to a path. See ADR-0026.
            archive = file_result.value
            yield from ph.emit_log(
                "info",
                f"[{self.platform_name}] Upload received: size={archive.size}",
            )

            # 2. Safety check (size only — uses JS metadata, no read)
            try:
                uploads.check_payload_size(file_result)
            except (uploads.FileTooLargeError, uploads.ChunkedExportError) as e:
                logger.error("Safety check failed for %s: %s", self.platform_name, e)
                yield from ph.emit_log("info", f"[{self.platform_name}] Safety check failed: {type(e).__name__}")
                _ = yield ph.render_safety_error_page(self.platform_name, e)
                raise TaskIncompleteError("upload_rejected")

            # 3. Validate
            validation = self.validate_file(archive)
            status = validation.get_status_code_id()
            category = getattr(validation, "current_ddp_category", None)
            category_id = getattr(category, "id", "unknown") if category else "unknown"

            if status == 0:
                yield from ph.emit_log("info", f"[{self.platform_name}] Validation: valid ({category_id})")
            else:
                yield from ph.emit_log("info", f"[{self.platform_name}] Validation: invalid")

            # 4. If invalid → retry prompt
            if status != 0:
                logger.info("Invalid %s file; prompting retry", self.platform_name)
                retry_prompt = self.generate_retry_prompt()
                retry_result = yield ph.render_page(self.UI_TEXT["retry_header"], retry_prompt)
                if retry_result.__type__ == "PayloadTrue":
                    continue  # loop back to step 1
                yield from ph.emit_log("info", f"[{self.platform_name}] Retry declined")
                raise TaskIncompleteError("abandoned")

            # 5. Extract
            logger.info("Extracting data for %s", self.platform_name)
            raw_result = self.extract_data(archive, validation)
            if isinstance(raw_result, Generator):
                result = yield from raw_result
            else:
                result = raw_result

            # 6. Log extraction summary (PII-free: counts only)
            total_rows = sum(len(t.data_frame) for t in result.tables)
            if result.errors:
                error_summary = ", ".join(f"{k}×{v}" for k, v in result.errors.items())
                yield from ph.emit_log("info", f"[{self.platform_name}] Extraction complete: {len(result.tables)} tables, {total_rows} rows; errors: {error_summary}")
            else:
                yield from ph.emit_log("info", f"[{self.platform_name}] Extraction complete: {len(result.tables)} tables, {total_rows} rows; errors: none")

            # 7. If no tables → no-data page (clean empties only: zero tables
            # WITH extraction errors is an extraction failure, never presented
            # as "no data found" — the no-data/extraction-bug separation in
            # the no-data ADR. Raising routes it through the consent-gated
            # error flow, so the participant stays pending.)
            if not result.tables:
                if result.errors:
                    raise RuntimeError(
                        f"Extraction produced no tables with errors: "
                        f"{', '.join(f'{k}×{v}' for k, v in result.errors.items())}"
                    )
                logger.info("No data extracted for %s", self.platform_name)

                no_data_result = yield ph.render_no_data_page(
                    self.platform_name
                )

                if no_data_result.__type__ == "PayloadTrue":
                    continue  # Erneut versuchen → zurück zur Dateiauswahl

                return  # Fortfahren → Plattform-Flow beenden

            break  # proceed to consent

        # 8. Render consent form
        yield from ph.emit_log("info", f"[{self.platform_name}] Consent form shown")
        review_data_prompt = self.generate_review_data_prompt(result.tables)
        consent_result = yield ph.render_page(self.UI_TEXT["review_data_header"], review_data_prompt)

        # 9. Donate with per-platform key
        if consent_result.__type__ == "PayloadJSON":
            reviewed_data = consent_result.value
            yield from ph.emit_log("info", f"[{self.platform_name}] Consent: accepted")
        elif consent_result.__type__ == "PayloadFalse":
            reviewed_data = json.dumps({"status": "data_submission declined"})
            yield from ph.emit_log("info", f"[{self.platform_name}] Consent: declined")
        else:
            return

        donate_key = f"{self.session_id}-{self.platform_name.lower()}"
        is_decline = consent_result.__type__ == "PayloadFalse"
        yield from ph.emit_log("info", f"[{self.platform_name}] Donation started: payload size={len(reviewed_data)} bytes")
        donate_result = yield ph.donate(donate_key, reviewed_data)

        # 11. Inspect donate result
        # For declines, don't show failure UI — the participant chose not to donate,
        # so a failure to record that decision is invisible infrastructure, not their problem.
        if not ph.handle_donate_result(donate_result):
            if is_decline:
                logger.warning("Decline status donation failed for %s (silent)", self.platform_name)
                yield from ph.emit_log("info", f"[{self.platform_name}] Donation result: decline record failed (silent)")
                return
            logger.error("Donation failed for %s", self.platform_name)
            yield from ph.emit_log("info", f"[{self.platform_name}] Donation result: failed")
            _ = yield ph.render_donate_failure_page(self.platform_name)
            raise TaskIncompleteError("donation_failed")

        yield from ph.emit_log("info", f"[{self.platform_name}] Donation result: success")

    # Methods to be overridden by platform-specific implementations
    def generate_file_prompt(self):
        """Generate platform-specific file prompt."""
        return ph.generate_file_prompt("application/zip")

    @abstractmethod
    def validate_file(self, file: SeekableBinaryReader) -> validate.ValidateInput:
        """Validate the file according to platform-specific rules.

        `file` is the `AsyncFileAdapter` wrapping the browser upload — a
        seekable binary reader, never a path. See ADR-0026.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def extract_data(self, file: SeekableBinaryReader, validation: validate.ValidateInput) -> d3i_props.ExtractionResult:
        """Extract data from file using platform-specific logic.

        `file` is the `AsyncFileAdapter` wrapping the browser upload — a
        seekable binary reader, never a path. See ADR-0026.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def generate_retry_prompt(self):
        """Generate platform-specific retry prompt."""
        return ph.generate_retry_prompt(self.platform_name)

    def generate_review_data_prompt(self, table_list):
        """Generate platform-specific review data prompt."""
        return ph.generate_review_data_prompt(
            description=self.UI_TEXT["review_data_description"],
            table_list=table_list,
        )
