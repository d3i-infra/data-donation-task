"""Tests for ValidateInput archive_members caching.

Per extraction/AD0007, validate_zip accepts a seekable binary file-like
(SeekableBinaryReader Protocol) — never a path string. Test fixtures use
io.BytesIO; production callers pass an AsyncFileAdapter directly.
"""
import io
import sys
import zipfile
from unittest.mock import MagicMock

sys.modules["js"] = MagicMock()

from port.helpers.validate import ValidateInput, validate_zip, DDPCategory, DDPFiletype, Language, StatusCode


def _build_archive(*entries: tuple[str, str | bytes]) -> io.BytesIO:
    """Build an in-memory zip with the given (name, content) entries."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in entries:
            zf.writestr(name, content)
    buf.seek(0)
    return buf


class TestArchiveMembers:
    def test_validate_zip_populates_archive_members(self):
        """validate_zip stores full member paths on ValidateInput."""
        archive = _build_archive(
            ("data/following.json", '{}'),
            ("data/posts.json", '{}'),
        )

        categories = [DDPCategory(
            id="test", ddp_filetype=DDPFiletype.JSON,
            language=Language.EN, known_files=["following.json", "posts.json"]
        )]
        result = validate_zip(categories, archive)
        assert "data/following.json" in result.archive_members
        assert "data/posts.json" in result.archive_members

    def test_archive_members_excluded_from_repr(self):
        """archive_members must not appear in repr (PII safety)."""
        archive = _build_archive(
            ("messages/inbox/contact_name_123/photo.jpg", b""),
            ("following.json", '{}'),
        )

        categories = [DDPCategory(
            id="test", ddp_filetype=DDPFiletype.JSON,
            language=Language.EN, known_files=["following.json"]
        )]
        result = validate_zip(categories, archive)
        assert "contact_name_123" not in repr(result)

    def test_archive_members_empty_on_bad_zip(self):
        """archive_members stays empty if archive is not a valid zip."""
        bad_archive = io.BytesIO(b"not a zip")

        categories = [DDPCategory(
            id="test", ddp_filetype=DDPFiletype.JSON,
            language=Language.EN, known_files=["file.json"]
        )]
        result = validate_zip(categories, bad_archive)
        assert result.archive_members == []

    def test_archive_members_default_empty(self):
        """archive_members defaults to empty list."""
        status_codes = [StatusCode(id=0, description="OK")]
        categories = [DDPCategory(
            id="test", ddp_filetype=DDPFiletype.JSON,
            language=Language.EN, known_files=["file.json"]
        )]
        v = ValidateInput(status_codes, categories)
        assert v.archive_members == []

    def test_validate_zip_detects_category_against_bytesio(self):
        """validate_zip correctly identifies the DDP category for a file-like archive."""
        archive = _build_archive(
            ("data/following.json", '{}'),
            ("data/posts.json", '{}'),
        )

        categories = [DDPCategory(
            id="test", ddp_filetype=DDPFiletype.JSON,
            language=Language.EN, known_files=["following.json", "posts.json"]
        )]
        result = validate_zip(categories, archive)
        assert result.current_ddp_category is not None
        assert result.current_ddp_category.id == "test"
