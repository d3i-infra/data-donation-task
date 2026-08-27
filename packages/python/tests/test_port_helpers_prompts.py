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


def test_protocol_error_page_covers_required_locales():
    page = ph.render_protocol_error_page("Instagram").toDict()["page"]
    body = page["body"][0]
    assert set(page["header"]["title"]["translations"]) >= {"en", "nl", "de", "it", "es"}
    assert set(body["text"]["translations"]) >= {"en", "nl", "de", "it", "es"}
    assert set(body["ok"]["translations"]) >= {"en", "nl", "de", "it", "es"}
    assert set(body["cancel"]["translations"]) >= {"en", "nl", "de", "it", "es"}
