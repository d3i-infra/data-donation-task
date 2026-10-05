"""Facebook posts carry their text in ``data[].post``; the table keeps it (synthetic data, ADR-0014)."""
import io
import json
import zipfile
from collections import Counter

from port.helpers.extraction_helpers import ZipArchiveReader
from port.helpers.validate import validate_zip
from port.platforms import facebook

POSTS = [
    {"timestamp": 1242868155, "title": "A. Example shared a link.",
     "data": [{"post": "I've been published!"}, {"update_timestamp": 1242868155}], "attachments": []},
    {"timestamp": 1246911014, "title": "A. Example was at Somewhere.", "data": [], "attachments": []},
]


def _archive() -> io.BytesIO:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("your_facebook_activity/posts/your_posts__check_ins__photos_and_videos_1.json", json.dumps(POSTS))
    buf.seek(0)
    return buf


def test_post_text_is_kept_and_missing_text_is_empty():
    archive = _archive()
    validation = validate_zip(facebook.DDP_CATEGORIES, archive)
    reader = ZipArchiveReader(archive, validation.archive_members, Counter())

    df = facebook.your_posts_check_ins_to_df(reader, Counter())

    assert list(df.columns) == ["Title", "Content", "Timestamp"]
    assert df.iloc[0]["Content"] == "I've been published!"
    assert df.iloc[1]["Content"] == ""
    assert df.iloc[0]["Timestamp"] == 1242868155
