# Adjacent-period new-buy proof

Strategy contract: `docs/backtest_signal_alpha_design.md`. The backtest enriches its own snapshot copy with the existing filing period key. It compares the latest visible period with the immediately preceding visible period and ranks only current-period values. `holdings_as_of` and its return contract, decision knowledge bounds, entry lag, holding period and benchmarks are unchanged.

`python -m pytest tests/test_backtest_flow.py tests/test_bitemporal_holdings.py -q`: 34 passed and one existing skip. Existing test cases were not modified. Added assertions cover A-B-A re-entry with its Q3 size, a larger historical size that must not outrank current positions, persisted Q3 entry date plus recomputed fixture-price return, and an invisible late amendment. The actual all-history `select_new_buys` body was temporarily restored: the named A-B-A regression failed with [] rather than AAA. Byte-identical restoration passed. Raw JUnit/logs and causal SHA256/exit receipt are included. Focused Mypy, Black and Ruff passed.

No live price-provider, hosted CI, deployment or original source-acceptance verdict is inferred from fixture prices. Keepalive/closer retain those lifecycle gates.
