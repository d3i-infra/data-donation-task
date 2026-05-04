"""Upload safety checks.

Validates upload size against policy limits using metadata only —
the upload itself is never read into Pyodide's heap.

See extraction/AD0007 for the streaming invariant: PayloadFile uploads
must be passed directly to consumers (zipfile.ZipFile, validators,
extractors) without materialization. Reading the entire payload to
verify its size defeats this; the JS-reported `adapter.size` attribute
is the source of truth for size policy decisions.
"""
import logging

logger = logging.getLogger(__name__)

MAX_FILE_SIZE_BYTES = 2 * 1024 * 1024 * 1024  # 2 GiB
CHUNKED_EXPORT_SENTINEL_BYTES = MAX_FILE_SIZE_BYTES  # same value, distinct intent


class FileTooLargeError(Exception):
    """Raised when a file exceeds MAX_FILE_SIZE_BYTES."""


class ChunkedExportError(Exception):
    """Raised when a file is exactly CHUNKED_EXPORT_SENTINEL_BYTES (split export sentinel)."""


def check_payload_size(file_result) -> None:
    """Validate upload size from JS-reported metadata. No bytes read.

    Caller is expected to handle the exception and render a safety
    error page. FlowBuilder does this around step 1 of start_flow().

    Args:
        file_result: A PayloadFile-shaped object whose .value carries
            an AsyncFileAdapter (with a .size attribute populated from
            the JS reader at construction time).

    Raises:
        TypeError: If file_result is not a PayloadFile. PayloadString /
            WORKERFS support was retired with extraction/AD0007.
        ChunkedExportError: If size == CHUNKED_EXPORT_SENTINEL_BYTES
            (split export sentinel — incomplete multi-part download).
        FileTooLargeError: If size > MAX_FILE_SIZE_BYTES.
    """
    if file_result.__type__ != "PayloadFile":
        raise TypeError(
            f"Unsupported payload type: {file_result.__type__}. "
            "Only PayloadFile is accepted; PayloadString/WORKERFS support "
            "was retired in extraction/AD0007."
        )

    size = file_result.value.size  # JS metadata, no read
    if size == CHUNKED_EXPORT_SENTINEL_BYTES:
        raise ChunkedExportError(
            f"File is exactly {CHUNKED_EXPORT_SENTINEL_BYTES} bytes — "
            "likely a chunked export sentinel"
        )
    if size > MAX_FILE_SIZE_BYTES:
        raise FileTooLargeError(
            f"File is {size} bytes, exceeding limit of {MAX_FILE_SIZE_BYTES} bytes"
        )
