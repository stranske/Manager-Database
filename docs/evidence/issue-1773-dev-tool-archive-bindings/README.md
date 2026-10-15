# Reviewed dashboard validation successor for dev-tool pins

Existing generated dependency delivery1772 atadafa19c393197ea2ea8ad9c540200063b1edbd6 changes Black26.10.0→26.10.1 and Ruff0.16.10→0.17.0. The historical review-recovery manifest pinned the entire then-current pyproject byte sequence, so these legitimate tool-only updates failed its current-input check. The authentic generated pyproject is retained with GitHub blob/head/SHA256/length provenance. Original checker:1FAIL; successor checker on the exact same bytes:1PASS.

The original issue1750 archives and manifests remain byte-identical. Historical pyproject and validator snapshots match their original manifest hashes. The successor binds the original manifest and new reviewed validator hash separately. All four product/replay inputs retain their exact historical current-input bindings. Current project semantics must match the historical input except the version values of exactly pinned Black/Ruff requirements; names/extras/markers, every other dependency and all other project/tool settings remain checked. Non-exact/wildcard formatter pins, unrelated project/dependency changes, or changed product/validator bytes fail.

Current main pyproject is unchanged. This does not rewrite historical experiments or claim that they used newer tool versions. The successor is a newly reviewed validation contract for the existing delivery prerequisite.

Validation:8focused successor controlsPASS; full dashboard evidence/replay/loader suite47PASS, including actual ten intended assertion RED/restored GREEN replay pairs. Original archive hash2204105d4a9239531d5d7c65c49fba86ce551e9d714d47e4d2f97c965e434dd1 verified unchanged. Black/Ruff/diff checks pass.

Commands: `/opt/anaconda3/bin/python3 -m pytest tests/test_dashboard_read_failure_evidence.py tests/test_dashboard_read_failure_replay.py tests/test_dashboard_loader_failures.py -q -o addopts=`. To replay the generated-input contrast, save main pyproject bytes, substitute only the retained authenticated generated pyproject, run the named recovery test, and restore the original bytes. Do not edit generated branch1772.

Hosted current-head CI/review, guarded source delivery, actual verification and source-owned Maint52/71 regeneration remain separate gates.
