import logging

import port.api.d3i_props as d3i_props
import port.api.props as props
from port.api.commands import CommandSystemDonate, CommandSystemExit, CommandSystemLog, CommandUIRender

_logger = logging.getLogger(__name__)


def render_page(
    header_text: props.Translatable,
    body: (
        props.PropsUIPromptRadioInput
        | props.PropsUIPromptConsentForm
        | d3i_props.PropsUIPromptConsentFormViz
        | props.PropsUIPromptFileInput
        | d3i_props.PropsUIPromptFileInputMultiple
        | d3i_props.PropsUIPromptQuestionnaire
        | props.PropsUIPromptConfirm
    ),
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
            props.PropsUIPromptConfirm |
        ): The main content of the page. It must be compatible with `props.PropsUIPageDonation`.

    Returns:
        CommandUIRender: A render command object containing the fully assembled page. Must be yielded.
    """
    header = props.PropsUIHeader(header_text)
    page = props.PropsUIPageDataSubmission("does not matter", header, body)
    return CommandUIRender(page)


def generate_retry_prompt(platform_name: str, multiple: bool = False) -> props.PropsUIPromptConfirm:
    """
    Generate a multilingual retry prompt for file processing errors.

    Returns a PropsUIPromptConfirm with "Try again" (ok → PayloadTrue) and
    "Continue" (cancel → PayloadFalse) buttons. Using standard feldspar
    PropsUIPromptConfirm instead of d3i PropsUIPromptRetry which only
    renders a single button. See ADR-0016 for the broader
    decision on custom vs standard prompt components.

    Args:
        platform_name: The name of the platform whose file could not be processed.
        multiple (bool, optional): Whether the upload this retries is a
            multi-file (PayloadFiles) selection — mirrors generate_file_prompt's
            `multiple` flag. When True, the retry copy tells the participant to
            select ALL the files again, since a multi-part upload (e.g. Google
            Takeout) must be resubmitted as a complete set, not one part.
            Defaults to False.
    """

    if multiple:
        text = props.Translatable(
            {
                "en": f"Unfortunately, we cannot process your {platform_name} files. Continue, if you are sure that you selected the right files. Try again to select ALL the files.",
                "nl": f"Helaas, kunnen we uw {platform_name} bestanden niet verwerken. Weet u zeker dat u de juiste bestanden heeft gekozen? Ga dan verder. Probeer opnieuw om ALLE bestanden te selecteren.",
                "de": f"Leider können wir Ihre {platform_name}-Dateien nicht verarbeiten. Fahren Sie fort, wenn Sie sicher sind, dass Sie die richtigen Dateien ausgewählt haben. Versuchen Sie es erneut, um ALLE Dateien auszuwählen.",
                "it": f"Purtroppo non possiamo elaborare i suoi file di {platform_name}. Continui se è sicuro di aver selezionato i file giusti. Riprovi per selezionare TUTTI i file.",
                "es": f"Lamentablemente, no podemos procesar sus archivos de {platform_name}. Continúe si está seguro de que ha seleccionado los archivos correctos. Intente de nuevo para seleccionar TODOS los archivos.",
            }
        )
    else:
        text = props.Translatable(
            {
                "en": f"Unfortunately, we cannot process your {platform_name} file. Continue, if you are sure that you selected the right file. Try again to select a different file.",
                "nl": f"Helaas, kunnen we uw {platform_name} bestand niet verwerken. Weet u zeker dat u het juiste bestand heeft gekozen? Ga dan verder. Probeer opnieuw als u een ander bestand wilt kiezen.",
                "de": f"Leider können wir Ihre {platform_name}-Datei nicht verarbeiten. Fahren Sie fort, wenn Sie sicher sind, dass Sie die richtige Datei ausgewählt haben. Versuchen Sie es erneut, um eine andere Datei auszuwählen.",
                "it": f"Purtroppo non possiamo elaborare il suo file di {platform_name}. Continui se è sicuro di aver selezionato il file giusto. Riprovi per selezionare un file diverso.",
                "es": f"Lamentablemente, no podemos procesar su archivo de {platform_name}. Continúe si está seguro de que ha seleccionado el archivo correcto. Intente de nuevo para seleccionar un archivo diferente.",
            }
        )
    ok = props.Translatable(
        {"en": "Try again", "nl": "Probeer opnieuw", "de": "Erneut versuchen", "it": "Riprova", "es": "Intentar de nuevo"}
    )
    cancel = props.Translatable(
        {"en": "Continue", "nl": "Doorgaan", "de": "Weiter", "it": "Continua", "es": "Continuar"}
    )
    return props.PropsUIPromptConfirm(text, ok, cancel)


def generate_file_prompt(
    extensions: str, multiple: bool = False
) -> props.PropsUIPromptFileInput | d3i_props.PropsUIPromptFileInputMultiple:
    """
    Generates a file input prompt for selecting file(s) for a platform.

    Creates a multilingual file input prompt that instructs the user to select
    file(s) they've received from a platform and stored on their device.
    The returned prompt is rendered with: yield result = render_page(...)

    When returned, result.value contains the file handle(s):
    - If multiple=False: a single file, wrapped as PayloadFile
    - If multiple=True: a list of files, wrapped as PayloadFiles

    Args:
        extensions (str): A collection of allowed MIME types.
            For example: "application/zip, text/plain, application/json"
        multiple (bool, optional): Whether to allow multiple file selection.
            Defaults to False.

    Returns:
        props.PropsUIPromptFileInput | d3i_props.PropsUIPromptFileInputMultiple:
            A file input prompt object containing the description text and
            allowed file extensions. If multiple=True, returns a
            PropsUIPromptFileInputMultiple object for selecting multiple files.
    """
    # de/it/nl copy below: register standardized on formal Lei (it), Download-Anleitung
    # unified (de), Dutch compounds per Taalunie (nl) — reviewed 2026-08-27.
    description = props.Translatable(
        {
            "en": "Please follow the download instructions and choose the file that you stored on your device.",
            "nl": "Volg de downloadinstructies en kies het bestand dat u op uw apparaat heeft opgeslagen.",
            "de": "Bitte folgen Sie der Download-Anleitung und wählen Sie die Datei aus, die Sie auf Ihrem Gerät gespeichert haben.",
            "it": "Segua le istruzioni per il download e scelga il file che ha salvato sul suo dispositivo.",
            "es": "Siga las instrucciones de descarga y elija el archivo que ha guardado en su dispositivo.",
        }
    )
    if multiple:
        description = props.Translatable({
            "en": "Please follow the download instructions and select ALL the files you received — Google Takeout usually delivers several zip files that belong together.",
            "nl": "Volg de downloadinstructies en selecteer ALLE bestanden die u heeft ontvangen — Google Takeout levert meestal meerdere zipbestanden die bij elkaar horen.",
            "de": "Bitte folgen Sie der Download-Anleitung und wählen Sie ALLE erhaltenen Dateien aus — Google Takeout liefert meist mehrere zusammengehörige ZIP-Dateien.",
            "it": "Segua le istruzioni per il download e selezioni TUTTI i file ricevuti — Google Takeout di solito fornisce più file ZIP appartenenti alla stessa esportazione.",
            "es": "Siga las instrucciones de descarga y seleccione TODOS los archivos recibidos — Google Takeout suele entregar varios archivos zip que van juntos.",
        })
        # Keeps the filename portion identical across locales — only the
        # leading "Example"/"Voorbeeld"/... word is translated — matching
        # the Google Takeout chunked-export naming shape (see ADR-0040).
        example = props.Translatable({
            "en": "Example: takeout-...-1-001.zip, takeout-...-2-001.zip",
            "nl": "Voorbeeld: takeout-...-1-001.zip, takeout-...-2-001.zip",
            "de": "Beispiel: takeout-...-1-001.zip, takeout-...-2-001.zip",
            "it": "Esempio: takeout-...-1-001.zip, takeout-...-2-001.zip",
            "es": "Ejemplo: takeout-...-1-001.zip, takeout-...-2-001.zip",
        })
        return d3i_props.PropsUIPromptFileInputMultiple(description, extensions, example=example)

    return props.PropsUIPromptFileInput(description, extensions)


def generate_review_data_prompt(
    description: props.Translatable, table_list: list[d3i_props.PropsUIPromptConsentFormTableViz]
) -> d3i_props.PropsUIPromptConsentFormViz:
    """
    Generates a data review form with a list of tables and a description, including default donate question and button.
    The participant can review these tables before they will be send to the researcher. If the participant consents to sharing the data
    the data will be stored at the configured storage location.

    Args:
        table_list (list[props.PropsUIPromptConsentFormTableViz]): A list of consent form tables to be included in the prompt.
        description (props.Translatable): A translatable description text for the consent prompt.

    Returns:
        props.PropsUIPromptConsentForm: A structured consent form object containing the provided table list, description,
        and default values for donate question and button.
    """
    donate_question = props.Translatable(
        {
            "en": "Do you want to share this data for research?",
            "nl": "Wilt u deze gegevens delen voor onderzoek?",
            "de": "Möchten Sie diese Daten für die Forschung teilen?",
            "it": "Vuole condividere questi dati per la ricerca?",
            "es": "¿Desea compartir estos datos para la investigación?",
        }
    )

    donate_button = props.Translatable(
        {
            "en": "Yes, share for research",
            "nl": "Ja, deel voor onderzoek",
            "de": "Ja, für Forschung teilen",
            "it": "Sì, condividi per la ricerca",
            "es": "Sí, compartir para la investigación",
        }
    )

    return d3i_props.PropsUIPromptConsentFormViz(
        tables=table_list, description=description, donate_question=donate_question, donate_button=donate_button
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


def emit_log(level: str, message: str):
    """Yield a CommandSystemLog to the host via the command protocol.

    Use via `yield from emit_log(...)` in generators (FlowBuilder, script.py).
    The host receives the log immediately; the PayloadVoid response is discarded.

    Messages sent through this function reach mono's /api/feldspar/log.
    They MUST be PII-free — no file paths, exception text, or participant data.

    Examples::

        yield from emit_log("info", "[LinkedIn] Consent: accepted")
        yield from emit_log("info", "Starting platform: Facebook")
    """
    _ = yield CommandSystemLog(level=level, message=message)


def generate_radio_prompt(
    title: props.Translatable, description: props.Translatable, items: list[str]
) -> props.PropsUIPromptRadioInput:
    """
    General purpose prompt selection menu
    """
    radio_items: list[props.RadioItem] = [{"id": i, "value": item} for i, item in enumerate(items)]
    return props.PropsUIPromptRadioInput(title, description, radio_items)


def generate_questionnaire() -> d3i_props.PropsUIPromptQuestionnaire:
    """
    Administer a basic questionnaire in Port.

    This function generates a prompt which can be rendered with render_page().
    The questionnaire demonstrates all currently implemented question types.
    In the current implementation, all questions are optional.

    You can build in logic by:
    - Chaining questionnaires together
    - Using extracted data in your questionnaires

    Usage:
        prompt = generate_questionnaire()
        results = yield render_page(header_text, prompt)

    The results.value contains a JSON string with question answers that
    can then be donated with donate().
    """

    questionnaire_description = props.Translatable(
        translations={
            "en": "Customer Satisfaction Survey for our Online Store",
            "nl": "Klanttevredenheidsonderzoek voor onze Online Winkel",
            "de": "Kundenzufriedenheitsumfrage für unseren Online-Shop",
            "it": "Sondaggio sulla soddisfazione dei clienti per il nostro negozio online",
            "es": "Encuesta de satisfacción del cliente para nuestra tienda en línea",
        }
    )

    open_question = props.Translatable(
        translations={
            "en": "How can we improve our services?",
            "nl": "Hoe kunnen we onze diensten verbeteren?",
            "de": "Wie können wir unsere Dienstleistungen verbessern?",
            "it": "Come possiamo migliorare i nostri servizi?",
            "es": "¿Cómo podemos mejorar nuestros servicios?",
        }
    )

    mc_question = props.Translatable(
        translations={
            "en": "How would you rate your overall experience?",
            "nl": "Hoe zou u uw algemene ervaring beoordelen?",
            "de": "Wie würden Sie Ihre Gesamterfahrung bewerten?",
            "it": "Come valuterebbe la sua esperienza complessiva?",
            "es": "¿Cómo valoraría su experiencia general?",
        }
    )

    mc_choices = [
        props.Translatable(
            translations={"en": "Excellent", "nl": "Uitstekend", "de": "Ausgezeichnet", "it": "Eccellente", "es": "Excelente"}
        ),
        props.Translatable(translations={"en": "Good", "nl": "Goed", "de": "Gut", "it": "Buono", "es": "Bueno"}),
        props.Translatable(
            translations={"en": "Average", "nl": "Gemiddeld", "de": "Durchschnittlich", "it": "Nella media", "es": "Regular"}
        ),
        props.Translatable(translations={"en": "Poor", "nl": "Slecht", "de": "Schlecht", "it": "Scarso", "es": "Malo"}),
        props.Translatable(
            translations={"en": "Very Poor", "nl": "Zeer slecht", "de": "Sehr schlecht", "it": "Molto scarso", "es": "Muy malo"}
        ),
    ]

    checkbox_question = props.Translatable(
        translations={
            "en": "Which of our products have you purchased? (Select all that apply)",
            "nl": "Welke van onze producten heeft u gekocht? (Selecteer alle toepasselijke)",
            "de": "Welche unserer Produkte haben Sie gekauft? (Wählen Sie alle zutreffenden aus)",
            "it": "Quali dei nostri prodotti ha acquistato? (Selezioni tutte le opzioni pertinenti)",
            "es": "¿Cuáles de nuestros productos ha comprado? (Seleccione todas las opciones que correspondan)",
        }
    )

    checkbox_choices = [
        props.Translatable(
            translations={"en": "Electronics", "nl": "Elektronica", "de": "Elektronik", "it": "Elettronica", "es": "Electrónica"}
        ),
        props.Translatable(
            translations={"en": "Clothing", "nl": "Kleding", "de": "Kleidung", "it": "Abbigliamento", "es": "Ropa"}
        ),
        props.Translatable(
            translations={
                "en": "Home Goods",
                "nl": "Huishoudelijke artikelen",
                "de": "Haushaltswaren",
                "it": "Articoli per la casa",
                "es": "Artículos para el hogar",
            }
        ),
        props.Translatable(translations={"en": "Books", "nl": "Boeken", "de": "Bücher", "it": "Libri", "es": "Libros"}),
        props.Translatable(
            translations={
                "en": "Food Items",
                "nl": "Voedingsproducten",
                "de": "Lebensmittel",
                "it": "Alimentari",
                "es": "Alimentos",
            }
        ),
    ]

    open_ended_question = d3i_props.PropsUIQuestionOpen(id=1, question=open_question)

    multiple_choice_question = d3i_props.PropsUIQuestionMultipleChoice(id=2, question=mc_question, choices=mc_choices)

    checkbox_question_obj = d3i_props.PropsUIQuestionMultipleChoiceCheckbox(
        id=3, question=checkbox_question, choices=checkbox_choices
    )

    return d3i_props.PropsUIPromptQuestionnaire(
        description=questionnaire_description, questions=[multiple_choice_question, checkbox_question_obj, open_ended_question]
    )


def render_no_data_page(platform_name: str) -> CommandUIRender:
    """Render 'no relevant data found' with acknowledge button.

    Caller should yield and await response before returning.
    """
    header = props.PropsUIHeader(
        props.Translatable({
            "en": f"No data found",
            "nl": f"Geen gegevens gevonden",
            "de": "Keine Daten gefunden",
            "it": "Nessun dato trovato",
            "es": "No se han encontrado datos",
        })
    )
    body = props.PropsUIPromptConfirm(
        text=props.Translatable({
            "en": f"Unfortunately, no relevant data was found in your {platform_name} file.",
            "nl": f"Helaas zijn er geen relevante gegevens gevonden in uw {platform_name} bestand.",
            "de": f"Leider wurden in Ihrer {platform_name}-Datei keine relevanten Daten gefunden.",
            "it": f"Purtroppo non sono stati trovati dati rilevanti nel suo file di {platform_name}.",
            "es": f"Lamentablemente, no se han encontrado datos relevantes en su archivo de {platform_name}.",
        }),
        ok=props.Translatable({"en": "Continue", "nl": "Doorgaan", "de": "Weiter", "it": "Continua", "es": "Continuar"}),
        cancel=props.Translatable({"en": "Continue", "nl": "Doorgaan", "de": "Weiter", "it": "Continua", "es": "Continuar"}),
    )
    page = props.PropsUIPageDataSubmission(platform_name, header, body)
    return CommandUIRender(page)


def render_safety_error_page(platform_name: str, error: Exception) -> CommandUIRender:
    """Render file safety error page.

    Terminal page: FlowBuilder discards this Confirm's result and always
    raises TaskIncompleteError("upload_rejected") next, regardless of which
    button is pressed (start_flow's safety-check branch). A second button
    with the same effect would only invent a distinction that isn't there,
    so this is a single acknowledging button (no `cancel`) — see the
    task-incomplete page for the same pattern.

    Caller should yield and await response before returning.
    """
    header = props.PropsUIHeader(
        props.Translatable({
            "en": "File cannot be processed",
            "nl": "Bestand kan niet worden verwerkt",
            "de": "Datei kann nicht verarbeitet werden",
            "it": "Impossibile elaborare il file",
            "es": "No se puede procesar el archivo",
        })
    )
    body = props.PropsUIPromptConfirm(
        text=props.Translatable({
            "en": f"Your {platform_name} file could not be processed: {error}",
            "nl": f"Uw {platform_name} bestand kon niet worden verwerkt: {error}",
            "de": f"Ihre {platform_name}-Datei konnte nicht verarbeitet werden: {error}",
            "it": f"Non è stato possibile elaborare il suo file di {platform_name}: {error}",
            "es": f"No se ha podido procesar su archivo de {platform_name}: {error}",
        }),
        ok=props.Translatable({"en": "OK", "nl": "OK", "de": "OK", "it": "OK", "es": "OK"}),
    )
    page = props.PropsUIPageDataSubmission(platform_name, header, body)
    return CommandUIRender(page)


def render_task_incomplete_page(platform_name: str) -> CommandUIRender:
    """Render the terminal page of the error flow: the task was not completed
    and the participant can retry by refreshing the page.

    Shown after the consent-gated error report (or its skip) so the
    participant does not land on a stale error page when the flow exits
    nonzero (Issue #123). Caller should yield and await response before
    returning.
    """
    header = props.PropsUIHeader(
        props.Translatable({
            "en": "Task not completed",
            "nl": "Taak niet voltooid",
            "de": "Aufgabe nicht abgeschlossen",
            "it": "Attività non completata",
            "es": "Tarea no completada",
        })
    )
    body = props.PropsUIPromptConfirm(
        text=props.Translatable({
            "en": "This task could not be completed. You can try again by refreshing this page. If the problem persists, please contact the researcher.",
            "nl": "Deze taak kon niet worden voltooid. U kunt het opnieuw proberen door deze pagina te vernieuwen. Als het probleem aanhoudt, neem dan contact op met de onderzoeker.",
            "de": "Diese Aufgabe konnte nicht abgeschlossen werden. Sie können es erneut versuchen, indem Sie diese Seite aktualisieren. Wenn das Problem weiterhin besteht, wenden Sie sich bitte an den Forscher.",
            "it": "Non è stato possibile completare questa attività. Può riprovare aggiornando questa pagina. Se il problema persiste, contatti il ricercatore.",
            "es": "Esta tarea no se pudo completar. Puede intentarlo de nuevo actualizando esta página. Si el problema persiste, póngase en contacto con el investigador.",
        }),
        ok=props.Translatable({"en": "OK", "nl": "OK", "de": "OK", "it": "OK", "es": "OK"}),
    )
    page = props.PropsUIPageDataSubmission(platform_name, header, body)
    return CommandUIRender(page)


def render_donate_failure_page(platform_name: str) -> CommandUIRender:
    """Render donation failure page.

    Terminal page: FlowBuilder discards this Confirm's result and always
    raises TaskIncompleteError("donation_failed") next, regardless of which
    button is pressed (start_flow's donate-result branch) — donation is
    never retried from here. A second button with the same effect would
    only invent a distinction that isn't there, so this is a single
    acknowledging button (no `cancel`) — see the task-incomplete page for
    the same pattern.

    Caller should yield and await response before returning.
    """
    header = props.PropsUIHeader(
        props.Translatable({
            "en": "Data submission failed",
            "nl": "Gegevensinzending mislukt",
            "de": "Datenübermittlung fehlgeschlagen",
            "it": "Invio dei dati non riuscito",
            "es": "Error al enviar los datos",
        })
    )
    body = props.PropsUIPromptConfirm(
        text=props.Translatable({
            "en": f"Unfortunately, your {platform_name} data could not be submitted. Please try again later.",
            "nl": f"Helaas konden uw {platform_name} gegevens niet worden ingediend. Probeer het later opnieuw.",
            "de": f"Leider konnten Ihre {platform_name}-Daten nicht übermittelt werden. Bitte versuchen Sie es später erneut.",
            "it": f"Purtroppo non è stato possibile inviare i suoi dati di {platform_name}. Riprovi più tardi.",
            "es": f"Lamentablemente, no se han podido enviar sus datos de {platform_name}. Inténtelo de nuevo más tarde.",
        }),
        ok=props.Translatable({"en": "OK", "nl": "OK", "de": "OK", "it": "OK", "es": "OK"}),
    )
    page = props.PropsUIPageDataSubmission(platform_name, header, body)
    return CommandUIRender(page)


def render_protocol_error_page(platform_name: str) -> CommandUIRender:
    """Shown when the UI returned a payload type the flow cannot process —
    version skew between the study page and the flow, never participant data.

    Caller should yield and await response before returning. Distinct from
    the participant-skip case: a mismatched or unrecognized `__type__` is
    an observable protocol error, not a silent skip. See ADR-0018/0026 for
    the accepted upload payload shapes.

    Terminal page: FlowBuilder discards this Confirm's result and always
    raises TaskIncompleteError("upload_rejected") next, regardless of which
    button is pressed (start_flow's protocol-mismatch branch). A second
    button with the same effect would only invent a distinction that isn't
    there, so this is a single acknowledging button (no `cancel`) — see the
    task-incomplete page for the same pattern.
    """
    header = props.Translatable({
        "en": "Something went wrong",
        "nl": "Er ging iets mis",
        "de": "Etwas ist schiefgelaufen",
        "it": "Qualcosa è andato storto",
        "es": "Algo salió mal",
    })
    body = props.PropsUIPromptConfirm(
        text=props.Translatable({
            "en": f"The study page and the {platform_name} flow are out of sync. Please close this window and try again later.",
            "nl": f"De studiepagina en de {platform_name}-flow lopen niet gelijk. Sluit dit venster en probeer het later opnieuw.",
            "de": f"Die Studienseite und der {platform_name}-Ablauf sind nicht mehr synchron. Bitte schließen Sie dieses Fenster und versuchen Sie es später erneut.",
            "it": f"La pagina dello studio e il flusso di {platform_name} non sono sincronizzati. Chiuda questa finestra e riprovi più tardi.",
            "es": f"La página del estudio y el flujo de {platform_name} no están sincronizados. Cierre esta ventana e inténtelo de nuevo más tarde.",
        }),
        ok=props.Translatable({"en": "OK", "nl": "OK", "de": "OK", "it": "OK", "es": "OK"}),
    )
    return render_page(header, body)


def handle_donate_result(result) -> bool:
    """Inspect donate result. Returns True on success, False on failure.

    Both current bridges acknowledge a CommandSystemDonate with a structured
    result, so production and local dev alike reach Python as PayloadResponse:
    LiveBridge relays the host's reply, and FakeBridge returns the outcome of
    its own /data-submission POST. PayloadVoid arrives only from a bridge that
    resolves a donate without an acknowledgment (an older host, a stub bridge).

    PayloadResponse → check value.success (the path every current bridge takes)
    PayloadVoid / None → True (legacy no-acknowledgment shape)
    Anything else → log warning, return False
    """
    if result is None:
        return True

    result_type = getattr(result, "__type__", None)

    if result_type == "PayloadResponse":
        # value is { success: bool, key: str, status: int, error?: str }
        return bool(result.value.success)

    if result_type == "PayloadVoid":
        return True

    _logger.warning("Unexpected donate result type: %s", result_type)
    return False
