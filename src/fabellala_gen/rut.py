"""Chilean RUT primitives: check-digit math, canonical formatting, validation.

Pure, side-effect-free functions with no RNG and no I/O: generators.py uses
them to mint valid RUTs for synthetic clientes and to inject bad-check-digit
defects at the N_BAD_DV_RUT rate from config, while validate_outputs.py uses
is_valid_rut as a non-throwing predicate to detect those injected defects
deterministically (the checksum is exact ground truth, no fuzzy matching).

Canonical representation: "<body>-<DV>" with no thousands dots (banks store
RUT without dots; dots are display-only) and an uppercase DV, "0"-"9" or
"K". Parsing tolerates the dotted, spaced and dashless variants so format
dirt can be recognized later. RUT masking/pseudonymization under Ley N°
19.628 is deliberately deferred to the staging phase: this module models
what a source system holds, raw unmasked identifiers.
"""

DV_WEIGHTS = (2, 3, 4, 5, 6, 7)
_VALID_DV_CHARS = frozenset("0123456789K")


def compute_dv(body: int | str) -> str:
    """Compute the Chilean RUT check digit (dígito verificador) via módulo 11.

    The body's digits are traversed right-to-left and multiplied by the
    cycling weight sequence 2, 3, 4, 5, 6, 7 (DV_WEIGHTS). The remainder
    of the resulting sum divided by 11 maps to the check digit: 11 maps
    to "0", 10 maps to "K", and any other value maps to 11 - remainder.
    For example, the body 12345678 yields DV "5" and the single-digit
    body 6 yields "K" (6 * 2 = 12, 12 % 11 = 1, 11 - 1 = 10).

    Args:
        body: The RUT body (digits before the check digit), as an int or
            a string of digits. Leading zeros are ignored.

    Returns:
        The check digit as a single character, "0"-"9" or uppercase "K".

    Raises:
        ValueError: If body is negative or not a valid integer literal.
    """
    body_int = int(body)
    if body_int < 0:
        raise ValueError(f"RUT body must be non-negative, got {body!r}")
    total = 0
    for position, digit_char in enumerate(reversed(str(body_int))):
        total += int(digit_char) * DV_WEIGHTS[position % len(DV_WEIGHTS)]
    check_value = 11 - total % 11
    if check_value == 11:
        return "0"
    if check_value == 10:
        return "K"
    return str(check_value)


def format_rut(body: int | str, dv: str | None = None) -> str:
    """Return the canonical RUT string "<body>-<dv>" with no thousands dots.

    The check digit is uppercased ("k" becomes "K") but its checksum
    correctness is deliberately NOT verified here: format_rut validates
    the character only, while checksum correctness is is_valid_rut's
    concern. This split lets defect injection render an intentionally
    wrong check digit (e.g. "12345678-4") for the defect ledger's
    valor_inyectado column.

    Args:
        body: The RUT body, as an int or a string of digits. Leading
            zeros are ignored.
        dv: The check digit to render. Defaults to computing it from
            body via compute_dv.

    Returns:
        The canonical RUT string, e.g. "12345678-5" or "6-K".

    Raises:
        ValueError: If body is negative, or dv is not a single character
            in "0"-"9" or "K"/"k".
    """
    body_int = int(body)
    if body_int < 0:
        raise ValueError(f"RUT body must be non-negative, got {body!r}")
    if dv is None:
        dv_clean = compute_dv(body_int)
    else:
        dv_clean = dv.strip().upper()
        if len(dv_clean) != 1 or dv_clean not in _VALID_DV_CHARS:
            raise ValueError(
                f"RUT check digit must be one character in 0-9 or K, got {dv!r}"
            )
    return f"{body_int}-{dv_clean}"


def parse_rut(rut: str) -> tuple[int, str]:
    """Split a RUT string into its numeric body and check digit.

    Tolerates the common source-system variants so format inconsistencies
    can be recognized downstream: thousands dots ("12.345.678-5"), spaces
    ("12 345 678-5"), a lowercase "k", a missing dash ("123456785"), and
    leading zeros in the body. The dash, when present, must appear at
    most once; the check digit, exactly one character.

    Args:
        rut: The RUT string to parse.

    Returns:
        A (body, dv) tuple, e.g. (12345678, "5"). dv is a single
        uppercase character, "0"-"9" or "K".

    Raises:
        ValueError: If rut is empty, the body is not a digit string, or
            the check digit is not a single character in "0"-"9" or
            "K"/"k".
    """
    cleaned = rut.strip().replace(".", "").replace(" ", "").upper()
    if not cleaned:
        raise ValueError("RUT string is empty")

    if "-" in cleaned:
        body_part, dv_part = cleaned.split("-", 1)
    else:
        body_part, dv_part = cleaned[:-1], cleaned[-1:]

    if not (body_part.isascii() and body_part.isdigit()):
        raise ValueError(f"RUT body must be a digit string, got {body_part!r}")

    if len(dv_part) != 1 or dv_part not in _VALID_DV_CHARS:
        raise ValueError(
            f"RUT check digit must be one character in 0-9 or K, got {dv_part!r}"
        )
    return int(body_part), dv_part


def is_valid_rut(rut: str) -> bool:
    """Return True iff rut parses and its check digit matches módulo 11.

    Never raises: any structurally malformed string (empty, non-digit
    body, extra dashes, multi-character check digit) yields False, so
    data-quality checks and defect detection can call it as a predicate.

    Args:
        rut: The RUT string to validate, in any of the variants accepted
            by parse_rut.

    Returns:
        True if the check digit matches, False otherwise.
    """
    try:
        body, dv = parse_rut(rut)
    except ValueError:
        return False
    return dv == compute_dv(body)
