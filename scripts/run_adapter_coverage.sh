#!/usr/bin/env bash
set -euo pipefail

pytest tests/test_adapter_base.py tests/test_tracked_call.py \
  --cov=adapters.base \
  --cov-report=term-missing \
  --cov-report=xml:coverage-adapters.xml \
  -m "not slow"
