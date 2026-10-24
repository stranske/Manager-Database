#!/usr/bin/env bash
# Emit the run-contract/v1 reference run consumed by backplane-conformance.yml.
set -euo pipefail

out_dir="${1:?usage: scripts/emit_reference_run.sh <output-dir>}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python "${repo_root}/scripts/emit_reference_run.py" "${out_dir}"
