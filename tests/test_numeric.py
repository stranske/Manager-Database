import pytest

from utils.numeric import parse_finite_float


def test_parse_finite_float_normalizes_overflow() -> None:
    with pytest.raises(ValueError, match="numeric value overflow"):
        parse_finite_float(10**1000, allow_none=False)
