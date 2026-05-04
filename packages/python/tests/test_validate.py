"""Tests for ValidateInput archive_members caching."""
import io
import sys
import zipfile
from unittest.mock import MagicMock

sys.modules["js"] = MagicMock()

from port.helpers.validate import ValidateInput, validate_zip, DDPCategory, DDPFiletype, Language, StatusCode


class TestArchiveMembers:
    def test_validate_zip_populates_archive_members(self, tmp_path):
        """validate_zip stores full member paths on ValidateInput."""
        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data/following.json", '{}')
            zf.writestr("data/posts.json", '{}')

        categories = [DDPCategory(
            id="test", ddp_filetype=DDPFiletype.JSON,
            language=Language.EN, known_files=["following.json", "posts.json"]
        )]
        result = validate_zip(categories, str(zip_path))
        assert "data/following.json" in result.archive_members
        assert "data/posts.json" in result.archive_members

    def test_archive_members_excluded_from_repr(self, tmp_path):
        """archive_members must not appear in repr (PII safety)."""
        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("messages/inbox/contact_name_123/photo.jpg", b"")
            zf.writestr("following.json", '{}')

        categories = [DDPCategory(
            id="test", ddp_filetype=DDPFiletype.JSON,
            language=Language.EN, known_files=["following.json"]
        )]
        result = validate_zip(categories, str(zip_path))
        assert "contact_name_123" not in repr(result)

    def test_archive_members_empty_on_bad_zip(self, tmp_path):
        """archive_members stays empty if zip is invalid."""
        bad_path = tmp_path / "bad.zip"
        bad_path.write_bytes(b"not a zip")

        categories = [DDPCategory(
            id="test", ddp_filetype=DDPFiletype.JSON,
            language=Language.EN, known_files=["file.json"]
        )]
        result = validate_zip(categories, str(bad_path))
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


class TestFileLikeAcceptance:
    """validate_zip must accept a seekable binary file-like (e.g. AsyncFileAdapter).

    Per extraction/AD0007, the upload pipeline passes the AsyncFileAdapter
    directly so the zip is never materialized to a path.
    """

    def test_validate_zip_accepts_bytesio(self):
        """validate_zip works against an in-memory BytesIO archive."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("data/following.json", '{}')
            zf.writestr("data/posts.json", '{}')
        buf.seek(0)

        categories = [DDPCategory(
            id="test", ddp_filetype=DDPFiletype.JSON,
            language=Language.EN, known_files=["following.json", "posts.json"]
        )]
        result = validate_zip(categories, buf)
        assert "data/following.json" in result.archive_members
        assert "data/posts.json" in result.archive_members
        # Detected as the test category, not unknown.
        assert result.current_ddp_category is not None
        assert result.current_ddp_category.id == "test"
