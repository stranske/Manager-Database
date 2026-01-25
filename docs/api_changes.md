# API Changes

## 2026-01-25

- **GET /managers**: The default pagination limit is now always set to 25 when the
  `limit` parameter is omitted.
  - Confirmation: Reviewed against docs/api_design_guidelines.md (Pagination defaults)
    on 2026-01-25; aligns with API design guidelines for list endpoints.
