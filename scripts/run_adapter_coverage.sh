#!/usr/bin/env bash
set -euo pipefail

# Run adapter-focused tests while measuring coverage across the full adapters module.
pytest tests/test_adapter_base.py tests/test_adapter_registry.py tests/test_tracked_call.py \
  tests/test_uk_adapter.py tests/test_canada_adapter.py tests/test_edgar.py \
  tests/test_edgar_additional.py \
  --cov=adapters \
  --cov-report=term-missing \
  --cov-report=xml:coverage-adapters.xml \
  --cov-fail-under=80 \
  -m "not slow"
