from port.helpers import port_helpers as ph


def test_multi_prompt_copy_covers_required_locales():
    prompt = ph.generate_file_prompt("application/zip", multiple=True)
    d = prompt.toDict()
    translations = d["description"]["translations"]
    assert set(translations) >= {"en", "nl", "de", "it", "es"}
    assert "file(s)" in translations["en"] or "files" in translations["en"]


def test_single_prompt_copy_unchanged():
    prompt = ph.generate_file_prompt("application/zip")
    assert prompt.toDict()["__type__"] == "PropsUIPromptFileInput"


def test_multi_prompt_includes_example_covering_required_locales():
    """generate_file_prompt(multiple=True) supplies the Takeout-shaped example
    (ITEM 1): PropsUIPromptFileInputMultiple.toDict() includes an "example"
    key when the field is set, covering all 5 locales."""
    prompt = ph.generate_file_prompt("application/zip", multiple=True)
    d = prompt.toDict()
    assert "example" in d
    translations = d["example"]["translations"]
    assert set(translations) >= {"en", "nl", "de", "it", "es"}


def test_multi_prompt_example_filename_identical_across_locales():
    """Only the leading word ("Example"/"Voorbeeld"/...) is translated; the
    Takeout filename shape itself must not vary by locale."""
    prompt = ph.generate_file_prompt("application/zip", multiple=True)
    translations = prompt.toDict()["example"]["translations"]
    filename_part = "takeout-...-1-001.zip, takeout-...-2-001.zip"
    for locale, text in translations.items():
        assert text.endswith(filename_part), f"locale {locale!r} filename part diverged: {text!r}"


def test_single_prompt_has_no_example_key():
    """The single-file prompt type carries no `example` concept at all —
    only PropsUIPromptFileInputMultiple gained the field."""
    prompt = ph.generate_file_prompt("application/zip")
    assert "example" not in prompt.toDict()


def test_protocol_error_page_covers_required_locales():
    page = ph.render_protocol_error_page("Instagram").toDict()["page"]
    body = page["body"][0]
    assert set(page["header"]["title"]["translations"]) >= {"en", "nl", "de", "it", "es"}
    assert set(body["text"]["translations"]) >= {"en", "nl", "de", "it", "es"}
    assert set(body["ok"]["translations"]) >= {"en", "nl", "de", "it", "es"}


def test_protocol_error_page_has_no_cancel_button():
    """ITEM 3: FlowBuilder discards this Confirm's result and always raises
    TaskIncompleteError("upload_rejected") next regardless of which button is
    pressed — a second identical button would invent a distinction that
    isn't there, so this is a single acknowledging button."""
    body = ph.render_protocol_error_page("Instagram").toDict()["page"]["body"][0]
    assert "cancel" not in body


def test_retry_prompt_single_file_wording_unchanged():
    prompt = ph.generate_retry_prompt("Instagram").toDict()
    assert "select a different file" in prompt["text"]["translations"]["en"]
    assert "ALL" not in prompt["text"]["translations"]["en"]


def test_retry_prompt_multiple_tells_participant_to_reselect_all_files():
    """ITEM 2: a multi-file (Google-Takeout-style) retry must ask the
    participant to select ALL the files, not just "a different file"."""
    prompt = ph.generate_retry_prompt("Google", multiple=True).toDict()
    translations = prompt["text"]["translations"]
    assert set(translations) >= {"en", "nl", "de", "it", "es"}
    assert "ALL" in translations["en"]
    assert "ALLE" in translations["nl"]


def test_retry_prompt_multiple_does_not_double_the_retry_adverb():
    """Copy-review fix: "Try again to select ALL the files again" (and the
    nl/de/it/es equivalents) doubled the retry adverb — say it once."""
    translations = ph.generate_retry_prompt("Google", multiple=True).toDict()["text"]["translations"]
    assert "again to select ALL the files again" not in translations["en"]
    assert "opnieuw om ALLE bestanden opnieuw" not in translations["nl"]
    assert "erneut, um ALLE Dateien erneut" not in translations["de"]
    assert "di nuovo TUTTI" not in translations["it"]
    assert "TODOS los archivos de nuevo" not in translations["es"]


def test_retry_prompt_multiple_ok_cancel_labels_unchanged():
    """Only the body text is multi-aware; the Try again / Continue button
    labels stay the same for both single- and multi-file retries."""
    single = ph.generate_retry_prompt("Instagram").toDict()
    multi = ph.generate_retry_prompt("Google", multiple=True).toDict()
    assert single["ok"]["translations"] == multi["ok"]["translations"]
    assert single["cancel"]["translations"] == multi["cancel"]["translations"]
