# API

## POST /managers

Create a manager record. Required fields are validated before persistence.

Request body:

```
{
  "name": "Alex Manager",
  "email": "alex@example.com",
  "department": "Operations"
}
```

Validation rules:

- `name` is required and cannot be blank.
- `email` must be a valid email address.
- `department` is required and cannot be blank.

Responses:

- `201 Created` with the stored manager record.
- `400 Bad Request` with `errors`, each including `field` and `message`.

Example error response:

```
{
  "errors": [
    {"field": "email", "message": "Email must be a valid email address."}
  ]
}
```
