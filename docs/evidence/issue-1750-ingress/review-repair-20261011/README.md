# PR1762 review repair

Baseline c6ad920d3c83ebe8739758cda4ac853255a7d848 still reproduced both active findings.
Four actual baseline failures in `tests/test_manager_ingress_review.py`: three real
`api.chat:app` requests (absent, malformed and underdeclared Content-Length) each
consumed a third chunk after the second exceeded the 20-byte cap; replay wrote a
false caller-integrity receipt but returned success. The repaired tests pass.

`api/managers.py` now enforces cumulative bytes during `request.stream()`, before
retaining each chunk. The response is 413 immediately after crossing the cap;
remaining ASGI chunks are not requested and storage is not accessed. Declared
oversize rejection remains. This bounds retained body bytes; the server's current
ASGI message may itself exceed the cap, and network/proxy limits are not claimed.

Replay writes its receipt, restores production bytes, and raises on caller
integrity failure before printing success. Its executable source.json is rebound
to current production. Historical validation archive and historical comparison
are unchanged; their percentages and old source bindings are not current coverage.

Validation on /opt/anaconda3/bin/python3 (Python3.12):

- `python -m pytest tests/test_manager_ingress_review.py -q -o addopts=`:
  before production repair **4 FAILED**, exactly the two reported failure modes.
- `python -m pytest tests/test_manager_ingress_boundaries.py tests/test_manager_ingress_evidence.py tests/test_manager_ingress_review.py -q -o addopts=`:
  **12 PASS** after repair.
- `python -m pytest tests/test_manager_bulk_api.py tests/test_openapi_schema.py tests/test_rate_limit_contract.py -q -o addopts=`:
  **88 PASS**.
- `python docs/evidence/issue-1750-ingress/replay_ingress_boundaries.py --output <new-directory>`:
  all four actual production mutations **RED exit1**, byte-identical restored
  production **GREEN exit0**; complete eight-phase receipt, unchanged callers.

Named-node JUnit, stdout/stderr, commands and source hashes are in validation.tar.gz;
manifest.json binds every member and current implementation/test/replay files.
No current full-suite/90%/provider/deployment claim. Source1750 remains open.
