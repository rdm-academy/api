# Gateway

The API gateway (or just gateway) provides a resource-centric HTTP interface for clients.

## Authentication

User accounts are created using authenticated third-party identity providers such as GitHub and Google. Clients must obtain a JWT by authorizing the application with a supported identity provider and authenticating with the provider. This enables the application to generate a JWT on behalf of the provider.

All endpoints that require authentication must include an `Authorization` header:

```
Authorization: Bearer <token>
```

Unless otherwise specified, all requests require this header.

## Accounts

### Register account

Update the user account information.

Note, this must be called at least once after an authorized JWT is obtained to register the account in the application.

```
PUT /account
{
  "email": "joe@example.com"
}
```

### Get account

Returns user account information.

```
GET /account
```

### Delete account

Deletes a user account

```
DELETE /account
```
