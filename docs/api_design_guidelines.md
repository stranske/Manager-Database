# API Design Guidelines

## Pagination defaults

List endpoints should return a predictable default page size when clients omit
pagination parameters. The standard default limit is 25 records unless a
specific endpoint documents a different requirement.
