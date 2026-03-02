from __future__ import annotations

import base64
import binascii
import codecs
import html
import re
from urllib.parse import unquote

_WHITESPACE_RE = re.compile(r"\s+")
_BASE64_RE = re.compile(r"^[A-Za-z0-9+/=_-]+$")
_HEX_CHUNK_RE = re.compile(r"(?:0x)?([0-9a-fA-F]{2})")
_UNICODE_ESCAPE_RE = re.compile(r"(?:\\u[0-9a-fA-F]{4}|\\x[0-9a-fA-F]{2})")
_LETTER_RE = re.compile(r"[A-Za-z]")


# --- Pattern definitions ---
INJECTION_PATTERNS: dict[str, re.Pattern[str]] = {
    "override_instructions": re.compile(
        r"(?:ignore|disregard|bypass|override|forget)\s+(?:all\s+)?(?:previous\s+)?"
        r"(?:instructions|rules|guidelines|constraints|system\s+prompt)",
        re.IGNORECASE,
    ),
    "system_prompt_exfil": re.compile(
        r"(?:reveal|show|print|display|expose|output|repeat|echo)(?:\s+\w+){0,4}\s+"
        r"(?:your\s+)?(?:system\s+)?(?:prompt|instructions|rules|guidelines)",
        re.IGNORECASE,
    ),
    "explicit_jailbreak": re.compile(
        r"(?:prompt\s+injection|jailbreak|DAN\s+mode|do\s+anything\s+now|"
        r"developer\s+mode|unlocked\s+mode)",
        re.IGNORECASE,
    ),
    "tool_execution": re.compile(
        r"(?:run|execute|eval|spawn|invoke)\s+(?:shell|bash|python|curl|wget|"
        r"subprocess|os\.system|exec\()",
        re.IGNORECASE,
    ),
    "sql_injection": re.compile(
        r"(?:;\s*DROP\s+TABLE|;\s*DELETE\s+FROM|;\s*UPDATE\s+.*SET|"
        r";\s*INSERT\s+INTO|UNION\s+SELECT|OR\s+1\s*=\s*1|"
        r"--\s*$|/\*.*\*/)",
        re.IGNORECASE,
    ),
    "financial_manipulation": re.compile(
        r"(?:execute\s+trade|transfer\s+funds|place\s+order|"
        r"wire\s+transfer|send\s+money|modify\s+account)",
        re.IGNORECASE,
    ),
}


# --- Encoding decoders ---
def _normalize_text(text: str) -> str:
    return _WHITESPACE_RE.sub(" ", text.strip())


def _maybe_decode_base64(text: str) -> str | None:
    """Attempt base64 decode (standard and URL-safe). Return decoded text or None."""

    candidate = text.strip()
    if not candidate or len(candidate) < 8:
        return None
    if not _BASE64_RE.fullmatch(candidate):
        return None

    padding = "=" * ((4 - len(candidate) % 4) % 4)
    padded = candidate + padding

    for decoder in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            decoded = decoder(padded)
            decoded_text = decoded.decode("utf-8", errors="strict")
        except (binascii.Error, UnicodeDecodeError, ValueError):
            continue

        normalized = _normalize_text(decoded_text)
        if normalized and normalized != candidate:
            return normalized

    return None


def _maybe_decode_hex(text: str) -> str | None:
    """Detect and decode 0xNN hex-encoded payloads."""

    candidate = text.strip()
    if not candidate:
        return None

    tokens = _HEX_CHUNK_RE.findall(candidate)
    if len(tokens) < 4:
        return None

    joined = "".join(tokens)
    if len(joined) % 2 != 0:
        return None

    try:
        decoded = bytes.fromhex(joined).decode("utf-8", errors="strict")
    except ValueError:
        return None

    normalized = _normalize_text(decoded)
    return normalized if normalized and normalized != candidate else None


def _english_likelihood_score(text: str) -> float:
    letters = [char.lower() for char in text if char.isalpha()]
    if len(letters) < 8:
        return 0.0

    vowels = sum(1 for char in letters if char in "aeiou") / len(letters)
    common = sum(1 for char in letters if char in "etaoinshrdlu") / len(letters)
    spaces = text.count(" ") / max(len(text), 1)
    return vowels * 0.4 + common * 0.5 + spaces * 0.1


def _maybe_decode_rot13(text: str) -> str | None:
    """Detect ROT13-encoded text using letter frequency analysis."""

    candidate = text.strip()
    if not candidate or len(_LETTER_RE.findall(candidate)) < 8:
        return None

    decoded = codecs.decode(candidate, "rot_13")
    if decoded == candidate:
        return None

    decoded_score = _english_likelihood_score(decoded)
    original_score = _english_likelihood_score(candidate)

    if decoded_score <= original_score + 0.08:
        return None

    normalized = _normalize_text(decoded)
    return normalized if normalized else None


def _maybe_decode_unicode_escape(text: str) -> str | None:
    """Decode unicode escape sequences (\\uXXXX)."""

    candidate = text.strip()
    if not candidate or not _UNICODE_ESCAPE_RE.search(candidate):
        return None

    try:
        decoded = candidate.encode("utf-8").decode("unicode_escape")
    except UnicodeDecodeError:
        return None

    normalized = _normalize_text(decoded)
    return normalized if normalized and normalized != candidate else None


def _maybe_decode_url(text: str) -> str | None:
    """Decode percent-encoded (%XX) text, including double-encoding."""

    candidate = text.strip()
    if not candidate or "%" not in candidate:
        return None

    once = unquote(candidate)
    twice = unquote(once)

    if twice != candidate:
        normalized = _normalize_text(twice)
        return normalized if normalized else None

    return None


def _maybe_decode_html_entities(text: str) -> str | None:
    """Decode HTML entities (&amp;, &#xx;, etc.)."""

    candidate = text.strip()
    if not candidate:
        return None

    decoded = html.unescape(candidate)
    if decoded == candidate:
        return None

    normalized = _normalize_text(decoded)
    return normalized if normalized else None


_DECODERS = (
    _maybe_decode_base64,
    _maybe_decode_hex,
    _maybe_decode_rot13,
    _maybe_decode_unicode_escape,
    _maybe_decode_url,
    _maybe_decode_html_entities,
)


def _decoded_variants(normalized: str) -> set[str]:
    variants: set[str] = {normalized}
    queue = [normalized]

    while queue:
        current = queue.pop(0)

        for decoder in _DECODERS:
            decoded = decoder(current)
            if decoded and decoded not in variants:
                variants.add(decoded)
                queue.append(decoded)

        # Mixed payloads: decode likely encoded tokens inside plain text.
        parts = current.split(" ")
        for index, token in enumerate(parts):
            if len(token) < 8:
                continue
            for decoder in _DECODERS:
                decoded_token = decoder(token)
                if not decoded_token:
                    continue
                mixed = _normalize_text(
                    " ".join(parts[:index] + [decoded_token] + parts[index + 1 :])
                )
                if mixed and mixed not in variants:
                    variants.add(mixed)
                    queue.append(mixed)

    return variants


# --- Main detection functions ---
def detect_prompt_injection(instruction: str) -> list[str]:
    """Scan instruction text for injection patterns.

    Process:
    1. Normalize text (lowercase, collapse whitespace)
    2. Attempt all decodings (base64, hex, rot13, unicode, URL, HTML)
    3. Check original + all decoded variants against INJECTION_PATTERNS
    4. Return list of matched pattern names (empty = safe)

    Returns:
        List of reason strings (e.g., ["override_instructions", "sql_injection"]).
        Empty list means input is safe.
    """

    if not instruction:
        return []

    normalized = _normalize_text(instruction)
    if not normalized:
        return []

    variants = {variant.lower() for variant in _decoded_variants(normalized)}
    return [
        reason
        for reason, pattern in INJECTION_PATTERNS.items()
        if any(pattern.search(variant) for variant in variants)
    ]


DEFAULT_BLOCK_SUMMARY = "Unsafe instruction blocked by prompt-injection guard."


def check_and_block(instruction: str) -> tuple[bool, str | None]:
    """Convenience function: returns (is_safe, block_reason).

    is_safe=True, block_reason=None -> proceed with LLM call
    is_safe=False, block_reason="..." -> return block message to user
    """

    reasons = detect_prompt_injection(instruction)
    if not reasons:
        return True, None
    return False, f"{DEFAULT_BLOCK_SUMMARY} Reasons: {', '.join(reasons)}"


class PromptInjectionError(ValueError):
    """Raised when prompt injection is detected."""

    def __init__(self, reasons: list[str]):
        self.reasons = reasons
        super().__init__(f"Prompt injection detected: {', '.join(reasons)}")


def guard_chain_input(user_input: str) -> str:
    """Validate user input before passing to any LLM chain.

    Raises PromptInjectionError if injection detected.
    Returns sanitized input (stripped, normalized) if safe.
    """

    normalized = _normalize_text(user_input)
    reasons = detect_prompt_injection(normalized)
    if reasons:
        raise PromptInjectionError(reasons)
    return normalized
