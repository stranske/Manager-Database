"""Test file with intentional errors to validate the full autofix pipeline.

This file contains:
1. Formatting issues (ruff/black can fix) - cosmetic
2. Type errors (mypy will catch) - non-cosmetic, needs Codex
3. Test failures (pytest will catch) - non-cosmetic, needs Codex

The autofix system should:
1. First run basic autofix (ruff/black) to fix formatting
2. Gate should still fail due to mypy/pytest errors
3. Auto-escalate to Codex to fix the remaining issues
"""
import os
from typing import Dict,List,Optional
import sys

# Formatting issue 1: missing spaces around operators
x=1+2+3

# Formatting issue 2: multiple imports on one line (already above)

# Formatting issue 3: trailing whitespace (invisible but present)
y = 42   

# Formatting issue 4: inconsistent quotes
message = "hello"
other_message = 'world'

# Type error 1: wrong type assignment
def get_count() -> int:
    return "not an int"  # mypy error: returning str instead of int

# Type error 2: incompatible types
def process_items(items: List[str]) -> Dict[str, int]:
    result: Dict[str, int] = {}
    for item in items:
        result[item] = "count"  # mypy error: assigning str to int
    return result

# Type error 3: missing return
def calculate_total(values: List[int]) -> int:
    total = sum(values)
    # Missing return statement - mypy should catch this

# Unused import (ruff will catch this)
import json

# Test that will fail
def test_intentional_failure():
    """This test intentionally fails to trigger Codex escalation."""
    expected = 42
    actual = 41  # Wrong value
    assert actual == expected, f"Expected {expected}, got {actual}"

# Another failing test
def test_type_mismatch():
    """Test type handling with intentional failure."""
    result = get_count()
    assert isinstance(result, int), f"Expected int, got {type(result)}"

# Test with assertion error
def test_list_processing():
    """Test list processing with intentional failure."""
    items = ["a", "b", "c"]
    result = process_items(items)
    # This will fail because process_items has a bug
    assert all(isinstance(v, int) for v in result.values())
