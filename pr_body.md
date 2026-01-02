## Summary

Implement a production-ready rate limiter with Redis backend and comprehensive testing.

## Tasks

### Achievable by Codex
- [x] Implement sliding window rate limiting algorithm in `utils/rate_limiter.py`
- [x] Add `__init__` parameters for `max_requests` and `window_seconds`
- [ ] Add type hints to all methods
- [x] Create `tests/test_rate_limiter.py` with unit tests
- [ ] Add docstrings explaining the algorithm

### Requires External Setup (Codex cannot complete)
- [ ] Configure Redis connection pooling with production credentials
- [ ] Set up Datadog APM integration for rate limit metrics
- [ ] Deploy to staging environment and run load test
- [ ] Get security team sign-off on rate limiting thresholds

### Ambiguous Tasks
- [x] Ensure rate limiter handles edge cases properly
- [ ] Performance should be acceptable under load

<!-- keepalive:enabled -->
<!-- keepalive:max_iterations=3 -->
