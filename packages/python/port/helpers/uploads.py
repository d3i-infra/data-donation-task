"""Upload safety checks.

Validates upload size against policy limits using metadata only —
the upload itself is never read into Pyodide's heap.

See ADR-0026 for the streaming invariant: PayloadFile uploads
must be passed directly to consumers (zipfile.ZipFile, validators,
extractors) without materialization. Reading the entire payload to
verify its size defeats this; the JS-reported `adapter.size` attribute
is the source of truth for size policy decisions.
"""
import logging

logger = logging.getLogger(__name__)

MAX_FILE_SIZE_BYTES = 2 * 1024 * 1024 * 1024  # 2 GiB

MAX_TOTAL_UPLOAD_BYTES = 10 * 1024**3  # 10 GiB aggregate cap for a PayloadFiles set
MAX_UPLOAD_FILES = 16  # max member count in a PayloadFiles set

# Caps per-member uncompressed size at materialization time (not here — this
# module is metadata-only). Enforced by the archive-set reader (Task 9).
MAX_MEMBER_UNCOMPRESSED_BYTES = 512 * 1024**2


class FileTooLargeError(Exception):
    """Raised when a file exceeds MAX_FILE_SIZE_BYTES."""


class TooManyFilesError(Exception):
    """Raised when a multi-file upload exceeds MAX_UPLOAD_FILES."""


def check_payload_size(file_result) -> None:
    """Validate upload size from JS-reported metadata. No bytes read.

    Accepts two payload shapes:
      - PayloadFile: a single upload. Checked against the per-file
        MAX_FILE_SIZE_BYTES cap only — there is no exact-size
        sentinel; a truncated archive fails zip validation downstream
        and routes to the retry prompt instead.
      - PayloadFiles: a multi-file set. The per-file cap does not
        apply here — multi-select is the supported path for chunked
        exports, so an individual member (e.g. a Google Takeout part)
        may legitimately exceed MAX_FILE_SIZE_BYTES. Instead the set
        is rejected if it is empty, if it has more than
        MAX_UPLOAD_FILES members, or if the members' sizes sum to
        more than MAX_TOTAL_UPLOAD_BYTES.

    Caller is expected to handle the exception and render a safety
    error page. FlowBuilder does this around step 1 of start_flow().

    Args:
        file_result: A PayloadFile- or PayloadFiles-shaped object.
            For PayloadFile, .value carries an AsyncFileAdapter (with
            a .size attribute populated from the JS reader at
            construction time). For PayloadFiles, .value carries a
            list of such adapters, each with its own .size.

    Raises:
        TypeError: If file_result is neither PayloadFile nor
            PayloadFiles, or if a PayloadFiles set carries no files.
            PayloadString / WORKERFS support was retired with
            ADR-0026.
        TooManyFilesError: For a PayloadFiles set, if it has more
            than MAX_UPLOAD_FILES members.
        FileTooLargeError: For a PayloadFile, if size >
            MAX_FILE_SIZE_BYTES. For a PayloadFiles set, if the
            combined size of all members exceeds
            MAX_TOTAL_UPLOAD_BYTES.
    """
    if getattr(file_result, "__type__", None) == "PayloadFiles":
        files = list(file_result.value)
        if not files:
            raise TypeError("PayloadFiles carried no files")
        if len(files) > MAX_UPLOAD_FILES:
            raise TooManyFilesError(
                f"{len(files)} files selected; at most {MAX_UPLOAD_FILES} are supported"
            )
        total = sum(f.size for f in files)
        if total > MAX_TOTAL_UPLOAD_BYTES:
            raise FileTooLargeError(
                f"Combined size is {total / 1024**2:.2f} MiB; "
                f"the limit is {MAX_TOTAL_UPLOAD_BYTES / 1024**2:.0f} MiB"
            )
        return

    if file_result.__type__ != "PayloadFile":
        raise TypeError(
            f"Unsupported payload type: {file_result.__type__}. "
            "Only PayloadFile is accepted; PayloadString/WORKERFS support "
            "was retired in ADR-0026."
        )

    size = file_result.value.size  # JS metadata, no read
    if size > MAX_FILE_SIZE_BYTES:
        raise FileTooLargeError(
            f"File is {size / (1024 ** 2):.2f} MiB, exceeding limit of {MAX_FILE_SIZE_BYTES / (1024 ** 2):.2f} MiB"
        )
