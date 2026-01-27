# API Rate Limiting

This document describes rate limiting policies for the Manager-Database API and how to handle rate limit responses in your client application.

## Overview

The Manager-Database API implements rate limiting to ensure fair usage and system stability. Rate limits are applied per IP address and vary by endpoint based on the computational cost of each operation.

## Rate Limits by Endpoint

### Standard Endpoints

| Endpoint | Method | Rate Limit | Window |
|----------|--------|------------|--------|
| `/chat` | GET | 60 requests | per minute |
| `/managers` | GET | 100 requests | per minute |
| `/managers` | POST | 30 requests | per minute |
| `/managers/{id}` | GET | 100 requests | per minute |
| `/managers/bulk` | POST | 10 requests | per minute |
| `/api/data` | GET | 60 requests | per minute |

### Health Check Endpoints

Health check endpoints have higher limits to support frequent monitoring:

| Endpoint | Method | Rate Limit | Window |
|----------|--------|------------|--------|
| `/health` | GET | 300 requests | per minute |
| `/health/db` | GET | 300 requests | per minute |
| `/health/ready` | GET | 300 requests | per minute |
| `/health/detailed` | GET | 100 requests | per minute |
| `/healthz` | GET | 300 requests | per minute |
| `/livez` | GET | 300 requests | per minute |
| `/readyz` | GET | 300 requests | per minute |

## Rate Limit Calculation

Rate limits are calculated using a **sliding window** algorithm:

1. Each request is timestamped when received
2. The system counts requests from the past window period (e.g., 60 seconds)
3. If the count exceeds the limit, subsequent requests are rejected with HTTP 429
4. As old requests fall outside the window, new requests become available

This approach provides smoother rate limiting compared to fixed windows, preventing burst traffic at window boundaries.

## Response Headers

All API responses include rate limit information in the following headers:

### Rate Limit Headers

```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 87
X-RateLimit-Reset: 1738095600
```

| Header | Description |
|--------|-------------|
| `X-RateLimit-Limit` | Maximum number of requests allowed in the current window |
| `X-RateLimit-Remaining` | Number of requests remaining in the current window |
| `X-RateLimit-Reset` | Unix timestamp (seconds) when the rate limit window resets |

### Example Response

**Successful Request:**
```http
HTTP/1.1 200 OK
Content-Type: application/json
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 87
X-RateLimit-Reset: 1738095600

{
  "data": [...]
}
```

**Rate Limit Exceeded:**
```http
HTTP/1.1 429 Too Many Requests
Content-Type: application/json
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1738095600
Retry-After: 45

{
  "error": "Rate limit exceeded",
  "message": "You have exceeded the rate limit for this endpoint. Please retry after 45 seconds.",
  "limit": 100,
  "window": "60s",
  "retry_after": 45
}
```

## Handling 429 Responses

When your application receives a `429 Too Many Requests` response, implement the following strategies:

### 1. Respect the Retry-After Header

The `Retry-After` header indicates the number of seconds to wait before retrying:

```python
import time
import httpx

def fetch_with_retry(url: str, max_retries: int = 3) -> dict:
    """Fetch data with automatic retry on rate limit."""
    with httpx.Client() as client:
        for attempt in range(max_retries):
            response = client.get(url)
            
            if response.status_code == 200:
                return response.json()
            
            if response.status_code == 429:
                # Get retry delay from header or response body
                retry_after = int(response.headers.get("Retry-After", 60))
                
                if attempt < max_retries - 1:
                    print(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                else:
                    raise Exception("Max retries exceeded")
            
            # For other errors, raise immediately
            response.raise_for_status()

# Usage
try:
    data = fetch_with_retry("http://localhost:8000/managers")
    print(data)
except Exception as e:
    print(f"Error: {e}")
```

### 2. Implement Exponential Backoff

For more robust error handling, use exponential backoff:

```python
import time
import random
import httpx

def fetch_with_backoff(url: str, max_retries: int = 5) -> dict:
    """Fetch data with exponential backoff on rate limit."""
    with httpx.Client() as client:
        for attempt in range(max_retries):
            response = client.get(url)
            
            if response.status_code == 200:
                return response.json()
            
            if response.status_code == 429:
                if attempt < max_retries - 1:
                    # Use Retry-After if provided, otherwise exponential backoff
                    if "Retry-After" in response.headers:
                        delay = int(response.headers["Retry-After"])
                    else:
                        # Exponential backoff with jitter
                        delay = (2 ** attempt) + random.uniform(0, 1)
                    
                    print(f"Rate limited. Backing off for {delay:.2f} seconds...")
                    time.sleep(delay)
                    continue
                else:
                    raise Exception("Max retries exceeded")
            
            # For other errors, raise immediately
            response.raise_for_status()
```

### 3. Monitor Rate Limit Headers

Track remaining quota to avoid hitting limits:

```python
import httpx

class RateLimitAwareClient:
    """HTTP client that tracks rate limit status."""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.client = httpx.Client()
        self.rate_limit_remaining = None
        self.rate_limit_reset = None
    
    def close(self):
        """Close the HTTP client."""
        self.client.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def get(self, path: str) -> dict:
        """Make GET request and track rate limit headers."""
        response = self.client.get(f"{self.base_url}{path}")
        
        # Update rate limit tracking
        self.rate_limit_remaining = int(
            response.headers.get("X-RateLimit-Remaining", 0)
        )
        self.rate_limit_reset = int(
            response.headers.get("X-RateLimit-Reset", 0)
        )
        
        # Warn if approaching limit
        if self.rate_limit_remaining < 10:
            print(f"Warning: Only {self.rate_limit_remaining} requests remaining")
        
        if response.status_code == 429:
            raise Exception(
                f"Rate limit exceeded. Resets at {self.rate_limit_reset}"
            )
        
        response.raise_for_status()
        return response.json()
    
    def requests_remaining(self) -> int:
        """Get number of requests remaining in current window."""
        return self.rate_limit_remaining or 0

# Usage with context manager (recommended)
with RateLimitAwareClient("http://localhost:8000") as client:
    try:
        data = client.get("/managers")
        print(f"Requests remaining: {client.requests_remaining()}")
    except Exception as e:
        print(f"Error: {e}")
```

### 4. JavaScript/TypeScript Example

```javascript
async function fetchWithRetry(url, maxRetries = 3) {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const response = await fetch(url);
      
      // Check rate limit headers
      const remaining = response.headers.get('X-RateLimit-Remaining');
      const reset = response.headers.get('X-RateLimit-Reset');
      
      console.log(`Rate limit: ${remaining} requests remaining`);
      
      if (response.ok) {
        return await response.json();
      }
      
      if (response.status === 429) {
        const retryAfter = parseInt(response.headers.get('Retry-After') || '60');
        
        if (attempt < maxRetries - 1) {
          console.log(`Rate limited. Waiting ${retryAfter} seconds...`);
          await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
          continue;
        }
        
        throw new Error('Max retries exceeded');
      }
      
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    } catch (error) {
      if (attempt === maxRetries - 1) {
        throw error;
      }
    }
  }
}

// Usage
fetchWithRetry('http://localhost:8000/managers')
  .then(data => console.log(data))
  .catch(error => console.error('Error:', error));
```

## Best Practices

### 1. Cache Responses

Reduce API calls by caching responses locally:

```python
from functools import lru_cache
import time
import httpx

class CachedAPIClient:
    """API client with response caching."""
    
    def __init__(self, base_url: str, cache_ttl: int = 300):
        self.base_url = base_url
        self.cache_ttl = cache_ttl
        self._cache = {}
    
    def get(self, path: str, use_cache: bool = True) -> dict:
        """Make GET request with optional caching."""
        cache_key = f"{self.base_url}{path}"
        
        # Check cache
        if use_cache and cache_key in self._cache:
            data, timestamp = self._cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                print("Returning cached response")
                return data
        
        # Make request
        response = httpx.get(f"{self.base_url}{path}")
        response.raise_for_status()
        data = response.json()
        
        # Update cache
        self._cache[cache_key] = (data, time.time())
        return data
```

### 2. Implement Request Queuing

Queue requests to stay within rate limits:

```python
import time
from collections import deque
import httpx

class RateLimitedQueue:
    """Request queue that respects rate limits."""
    
    def __init__(self, requests_per_minute: int):
        self.requests_per_minute = requests_per_minute
        self.request_times = deque()
    
    def _wait_if_needed(self):
        """Wait if necessary to respect rate limit."""
        now = time.time()
        
        # Remove requests older than 60 seconds
        while self.request_times and now - self.request_times[0] > 60:
            self.request_times.popleft()
        
        # Wait if at capacity
        if len(self.request_times) >= self.requests_per_minute:
            sleep_time = 60 - (now - self.request_times[0])
            if sleep_time > 0:
                print(f"Rate limit reached. Waiting {sleep_time:.2f}s...")
                time.sleep(sleep_time)
                self._wait_if_needed()
    
    def request(self, url: str) -> dict:
        """Make rate-limited request."""
        self._wait_if_needed()
        
        response = httpx.get(url)
        self.request_times.append(time.time())
        
        response.raise_for_status()
        return response.json()

# Usage
queue = RateLimitedQueue(requests_per_minute=60)

# These requests will be automatically throttled
for i in range(100):
    data = queue.request("http://localhost:8000/managers")
    print(f"Request {i+1} completed")
```

### 3. Batch Operations

Use bulk endpoints when available to reduce request count:

```python
import httpx

# Assuming you have a list of managers to create
managers_list = [
    {"name": "Alice Smith", "role": "Director", "department": "Engineering"},
    {"name": "Bob Jones", "role": "Manager", "department": "Sales"},
    # ... more managers
]

# Instead of multiple individual POST requests
# BAD - Uses 10 requests (if managers_list has 10 items)
for manager in managers_list:
    httpx.post("http://localhost:8000/managers", json=manager)

# GOOD - Uses 1 request
httpx.post(
    "http://localhost:8000/managers/bulk",
    json={"managers": managers_list}
)
```

### 4. Monitor and Log Rate Limits

Track rate limit usage in your application:

```python
import logging
import httpx

logger = logging.getLogger(__name__)

def log_rate_limit_info(response: httpx.Response):
    """Log rate limit information from response headers."""
    limit = response.headers.get("X-RateLimit-Limit", "unknown")
    remaining = response.headers.get("X-RateLimit-Remaining", "unknown")
    reset = response.headers.get("X-RateLimit-Reset", "unknown")
    
    logger.info(
        f"Rate limit status - "
        f"Limit: {limit}, "
        f"Remaining: {remaining}, "
        f"Reset: {reset}"
    )
    
    if response.status_code == 429:
        logger.warning(
            f"Rate limit exceeded! "
            f"Retry after: {response.headers.get('Retry-After', 'unknown')}s"
        )
```

## Rate Limit Exemptions

Certain use cases may require higher rate limits:

1. **Internal Services**: Services within the same infrastructure may have higher limits
2. **Authenticated Users**: API keys or authentication tokens may provide elevated limits
3. **Enterprise Plans**: Contact the API administrator for custom rate limit tiers

## Troubleshooting

### Issue: Receiving 429 responses unexpectedly

**Solutions:**
- Check the `X-RateLimit-Reset` header to see when your quota resets
- Review your application's request patterns for excessive calls
- Implement caching to reduce duplicate requests
- Use bulk endpoints instead of individual requests

### Issue: Rate limit headers not present

**Solutions:**
- Ensure you're using the correct API base URL
- Check that your client is not stripping custom headers
- Verify your API version supports rate limit headers

### Issue: Rate limits reset but still receiving 429

**Solutions:**
- The rate limit may be calculated across multiple servers
- Check for concurrent requests from your application
- Verify your IP address hasn't changed (affects per-IP limits)

## Related Documentation

- [API Design Guidelines](api_design_guidelines.md) - General API design principles
- [API Changes](api_changes.md) - Historical API modifications
- [Health Check Runbook](runbooks/health-checks.md) - Monitoring API health

## Support

For questions about rate limits or to request limit increases, please:

1. Check this documentation first
2. Review the API response headers for specific limit information
3. Open an issue in the repository with details about your use case
4. Contact the API administrator for custom arrangements
