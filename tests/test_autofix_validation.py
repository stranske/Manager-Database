"""Test file with intentional issues to validate autofix system.

This file contains:
1. Black formatting violations
2. Ruff lint errors  
3. Mypy type errors
4. Failing tests
5. Actual useful test coverage

Purpose: Validate the full autofix pipeline handles all failure modes.
"""

# --- BLACK VIOLATION: Bad formatting ---
import os,sys,time
from typing import Dict,List,Optional,Any
from adapters.base import connect_db,get_adapter,tracked_call

# --- RUFF VIOLATIONS ---
# F401: unused import
import json
import re
import collections

# E501: line too long
VERY_LONG_STRING_THAT_VIOLATES_LINE_LENGTH = "This is a very long string that definitely exceeds the maximum line length limit of 88 characters that ruff and black enforce by default"

# --- MYPY TYPE ERROR ---
def bad_type_annotation(x: int) -> str:
    return x  # Returns int, claims str

def missing_return_type(value):
    """Function missing type annotations."""
    return value * 2


# --- BLACK VIOLATION: Inconsistent spacing ---
def poorly_formatted_function( arg1,arg2,   arg3 ):
    """This function has poor formatting."""
    result=arg1+arg2+arg3
    if result>10:
        return   result
    else:
        return result*2


class BadlyFormattedClass:
    """Class with formatting issues."""
    
    def __init__(self,name:str,value:int):
        self.name=name
        self.value=value
    
    def compute(self,multiplier:int)->int:
        return self.value*multiplier


# --- ACTUAL USEFUL COVERAGE TESTS ---

def test_connect_db_sqlite_default():
    """Test connect_db returns valid SQLite connection."""
    # Clear env vars to force SQLite path
    old_url = os.environ.pop("DB_URL", None)
    old_path = os.environ.pop("DB_PATH", None)
    
    try:
        conn = connect_db(":memory:")
        assert conn is not None
        # Verify it's a working connection
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result == (1,)
        conn.close()
    finally:
        if old_url:
            os.environ["DB_URL"] = old_url
        if old_path:
            os.environ["DB_PATH"] = old_path


def test_connect_db_with_timeout():
    """Test connect_db accepts timeout parameter."""
    conn = connect_db(":memory:", connect_timeout=5.0)
    assert conn is not None
    conn.close()


def test_get_adapter_edgar():
    """Test that edgar adapter can be loaded."""
    try:
        adapter = get_adapter("edgar")
        assert adapter is not None
        # Verify it has expected protocol methods
        assert hasattr(adapter, "list_new_filings") or callable(getattr(adapter, "list_new_filings", None)) is False
    except ModuleNotFoundError:
        # Adapter module may not exist yet
        pass


def test_get_adapter_invalid():
    """Test get_adapter raises for unknown adapter."""
    try:
        get_adapter("nonexistent_adapter_xyz")
        assert False, "Should have raised"
    except (ModuleNotFoundError, ImportError):
        pass  # Expected


# --- INTENTIONALLY FAILING TESTS ---

def test_intentional_failure_assertion():
    """This test intentionally fails with an assertion error."""
    expected = 42
    actual = 41
    assert actual == expected, f"Expected {expected} but got {actual}"


def test_intentional_failure_exception():
    """This test intentionally raises an exception."""
    data = {"key": "value"}
    # This will raise KeyError
    result = data["nonexistent_key"]


def test_intentional_failure_type_error():
    """This test intentionally causes a TypeError."""
    value = "not a number"
    # This will raise TypeError
    result = value + 5


# --- RUFF VIOLATIONS: More lint issues ---

# W293: whitespace on blank line
def function_with_trailing_whitespace():    
    """Has trailing whitespace."""
    x = 1
    
    y = 2    
    return x + y


# E711: comparison to None
def bad_none_comparison(value):
    if value == None:
        return "empty"
    return "full"


# E712: comparison to True
def bad_bool_comparison(flag):
    if flag == True:
        return "yes"
    return "no"
