"""Tests for upload safety checks and streaming invariant.

See extraction/AD0007 for the streaming invariant:
PayloadFile uploads must reach consumers (zipfile.ZipFile, validators,
extractors) without materialization, and size policy decisions must
use JS-reported metadata rather than reading the upload.
"""
import io
import sys
import zipfile
from unittest.mock import MagicMock

# Mock js module before any port imports (Pyodide-only at runtime).
sys.modules["js"] = MagicMock()

import pytest

from port.helpers.uploads import (
    check_payload_size,
    FileTooLargeError,
    ChunkedExportError,
    MAX_FILE_SIZE_BYTES,
    CHUNKED_EXPORT_SENTINEL_BYTES,
)


def _payload_file(size: int) -> MagicMock:
    """Build a PayloadFile-shaped mock with the given adapter size."""
    adapter = MagicMock()
    adapter.size = size
    payload = MagicMock()
    payload.__type__ = "PayloadFile"
    payload.value = adapter
    return payload


class TestCheckPayloadSize:
    def test_normal_size_passes(self):
        """Sub-limit PayloadFile passes without raising."""
        check_payload_size(_payload_file(100 * 1024))  # 100 KiB

    def test_just_below_limit_passes(self):
        """Exactly MAX_FILE_SIZE_BYTES - 1 passes."""
        check_payload_size(_payload_file(MAX_FILE_SIZE_BYTES - 1))

    def test_above_limit_raises_too_large(self):
        """Strictly above MAX_FILE_SIZE_BYTES raises FileTooLargeError."""
        with pytest.raises(FileTooLargeError):
            check_payload_size(_payload_file(MAX_FILE_SIZE_BYTES + 1))

    def test_exact_sentinel_raises_chunked_export(self):
        """Exactly CHUNKED_EXPORT_SENTINEL_BYTES raises ChunkedExportError."""
        with pytest.raises(ChunkedExportError):
            check_payload_size(_payload_file(CHUNKED_EXPORT_SENTINEL_BYTES))

    def test_payload_string_raises_type_error(self):
        """PayloadString is no longer accepted (SRC compat dropped per AD0007)."""
        payload = MagicMock()
        payload.__type__ = "PayloadString"
        payload.value = "/some/path"
        with pytest.raises(TypeError, match="Unsupported payload type"):
            check_payload_size(payload)

    def test_unknown_type_raises_type_error(self):
        """Any non-PayloadFile type raises TypeError."""
        payload = MagicMock()
        payload.__type__ = "PayloadJSON"
        with pytest.raises(TypeError, match="Unsupported payload type"):
            check_payload_size(payload)

    def test_does_not_read_adapter(self):
        """Size is taken from .size metadata; .read() is never called.

        This is the core invariant of AD0007 — verifying upload size
        must not trigger the failure mode it is meant to prevent.
        """
        adapter = MagicMock()
        adapter.size = MAX_FILE_SIZE_BYTES + 1
        payload = MagicMock()
        payload.__type__ = "PayloadFile"
        payload.value = adapter

        with pytest.raises(FileTooLargeError):
            check_payload_size(payload)

        adapter.read.assert_not_called()
        adapter.readSlice.assert_not_called()


class _TrackingAdapter(io.BytesIO):
    """A BytesIO-backed adapter that records every read() call.

    Behaves exactly like a seekable binary file-like (which is what
    zipfile.ZipFile expects) but logs the size argument of each read
    so tests can assert no full-file read occurs.
    """

    def __init__(self, content: bytes):
        super().__init__(content)
        self.read_calls: list[int] = []

    def read(self, size: int | None = -1) -> bytes:
        self.read_calls.append(size if size is not None else -1)
        return super().read(size)


class TestStreamingInvariant:
    """zipfile.ZipFile must never issue read(-1) against an upload-path
    file-like. A full-file read is what triggers
    FileReaderSync.readAsArrayBuffer above 2 GiB. See AD0007.
    """

    def _build_zip_bytes(self) -> bytes:
        """Build a small in-memory zip with several entries."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("a.json", '{"k": "v"}')
            zf.writestr("nested/b.json", '{"k2": "v2"}')
            zf.writestr("c.csv", "col1,col2\n1,2\n")
        return buf.getvalue()

    def test_zipfile_open_does_not_full_read(self):
        """Opening a ZipFile and listing members issues no read(-1)."""
        adapter = _TrackingAdapter(self._build_zip_bytes())
        with zipfile.ZipFile(adapter, "r") as zf:
            zf.namelist()
        assert -1 not in adapter.read_calls

    def test_zipfile_extract_does_not_full_read(self):
        """Reading a member never issues read(-1)."""
        adapter = _TrackingAdapter(self._build_zip_bytes())
        with zipfile.ZipFile(adapter, "r") as zf:
            data = zf.read("a.json")
        assert data == b'{"k": "v"}'
        assert -1 not in adapter.read_calls

    def test_zipfile_open_close_reuse(self):
        """Opening the same adapter in successive ZipFile contexts works.

        ZipArchiveReader._read_member_bytes opens a fresh ZipFile per
        member access. The adapter must survive being passed to
        successive contexts.
        """
        adapter = _TrackingAdapter(self._build_zip_bytes())
        with zipfile.ZipFile(adapter, "r") as zf:
            zf.namelist()
        with zipfile.ZipFile(adapter, "r") as zf:
            zf.read("nested/b.json")
        assert -1 not in adapter.read_calls


class TestAsyncFileAdapterContract:
    """Verify the real AsyncFileAdapter (not a mock) implements the
    file-like contract zipfile.ZipFile requires.
    """

    def test_adapter_provides_seek_tell_read_size(self):
        """Real AsyncFileAdapter has the methods/attributes streaming requires."""
        from port.api.file_utils import AsyncFileAdapter

        # Fake JS reader: returns the requested slice as a JS-array proxy.
        class FakeJsArray:
            def __init__(self, data: bytes):
                self._data = data

            def to_py(self) -> bytes:
                return self._data

        class FakeJsReader:
            def __init__(self, content: bytes):
                self._content = content
                self.size = len(content)
                self.name = "fake.zip"

            def readSlice(self, start: int, end: int):
                return FakeJsArray(self._content[start:end])

        content = b"abcdefghijklmnop"
        adapter = AsyncFileAdapter(FakeJsReader(content))

        assert adapter.size == len(content)
        assert adapter.readable()
        assert adapter.seekable()

        # Bounded read.
        assert adapter.read(4) == b"abcd"
        assert adapter.tell() == 4

        # Seek + bounded read.
        adapter.seek(8)
        assert adapter.read(4) == b"ijkl"

        # Seek-from-end.
        adapter.seek(-2, 2)
        assert adapter.read(2) == b"op"
