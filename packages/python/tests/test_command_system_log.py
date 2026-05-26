from port.api.commands import CommandSystemLog
import json


def test_to_dict_has_all_fields():
    cmd = CommandSystemLog(level="warn", message="something happened")
    d = cmd.toDict()
    assert d["__type__"] == "CommandSystemLog"
    assert d["level"] == "warn"
    assert d["message"] == "something happened"
    # json_string for backwards compat with mono host
    payload = json.loads(d["json_string"])
    assert payload["level"] == "warn"
    assert payload["message"] == "something happened"


def test_level_and_message_match_json_string():
    """json_string must never diverge from the direct fields."""
    cmd = CommandSystemLog(level="error", message="boom")
    d = cmd.toDict()
    payload = json.loads(d["json_string"])
    assert payload["level"] == d["level"]
    assert payload["message"] == d["message"]
