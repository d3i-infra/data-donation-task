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
