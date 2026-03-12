# User Settings – Frontend API Specification

Base URL: `{API_BASE}/api` (e.g. `https://your-backend.com/api`)

---

## Overview

Settings page includes:
- **Profile** – View and edit own profile (name, email)
- **Change password** – Update password
- **Company** – View/edit company details (admin only for edit)
- **Team** – Add users, assign roles (admin/user), remove users (admin only)

**Admin check:** Use `is_company_admin` from the JWT to show/hide admin-only UI.

---

## Endpoints

### 1. Get Profile

**GET** `/api/settings/profile/`

**Auth:** `Authorization: Bearer <access_token>`

**Access:** Any authenticated user

**Success (200):**
```json
{
  "data": {
    "id": 1,
    "email": "john@company.com",
    "first_name": "John",
    "last_name": "Doe",
    "company_id": 1,
    "company_name": "Acme Parts LLC",
    "is_company_admin": true
  }
}
```

---

### 2. Update Profile

**PUT** `/api/settings/profile/`

**Auth:** `Authorization: Bearer <access_token>`

**Request:**
```json
{
  "first_name": "John",
  "last_name": "Doe",
  "email": "john@company.com"
}
```

| Field | Type | Required |
|-------|------|----------|
| first_name | string | No |
| last_name | string | No |
| email | string | No |

At least one field required. Email must be unique.

**Success (200):**
```json
{
  "message": "Profile updated",
  "data": { ... }
}
```

**Errors:**
- `400` – "A user with this email already exists" or validation error

---

### 3. Change Password

**POST** `/api/auth/change-password/`

**Auth:** `Authorization: Bearer <access_token>`

**Request:**
```json
{
  "current_password": "oldpass123",
  "new_password": "newpass456"
}
```

**Success (201):**
```json
{
  "message": "Password changed successfully"
}
```

---

### 4. Get Company Settings

**GET** `/api/settings/company/`

**Auth:** `Authorization: Bearer <access_token>`

**Access:** Any company user

**Success (200):**
```json
{
  "data": {
    "id": 1,
    "name": "Acme Parts LLC",
    "slug": "acme-parts-abc123",
    "business_type": "retail_store",
    "country": "US",
    "state_province": "California",
    "tax_id": "12-3456789",
    "is_admin": true
  }
}
```

---

### 5. Update Company Settings

**PUT** `/api/settings/company/`

**Auth:** `Authorization: Bearer <access_token>`

**Access:** Company admin only

**Request:**
```json
{
  "name": "Acme Parts LLC",
  "business_type": "retail_store",
  "country": "US",
  "state_province": "California",
  "tax_id": "12-3456789"
}
```

| Field | Type | Required |
|-------|------|----------|
| name | string | No |
| business_type | string | No |
| country | string | No |
| state_province | string | No |
| tax_id | string | No |

**Success (200):**
```json
{
  "message": "Company updated",
  "data": { ... }
}
```

**Errors:**
- `403` – Not a company admin

---

### 6. List Company Team

**GET** `/api/settings/company/team/`

**Auth:** `Authorization: Bearer <access_token>`

**Access:** Any company user

**Success (200):**
```json
{
  "data": [
    {
      "id": 1,
      "email": "admin@company.com",
      "first_name": "John",
      "last_name": "Doe",
      "is_company_admin": true,
      "created_at": "2024-01-15T10:00:00"
    },
    {
      "id": 2,
      "email": "member@company.com",
      "first_name": "Jane",
      "last_name": "Smith",
      "is_company_admin": false,
      "created_at": "2024-01-20T14:30:00"
    }
  ]
}
```

---

### 7. Add User to Company

**POST** `/api/settings/company/team/`

**Auth:** `Authorization: Bearer <access_token>`

**Access:** Company admin only

**Request:**
```json
{
  "email": "newuser@company.com",
  "first_name": "New",
  "last_name": "User",
  "password": "securepass123",
  "is_company_admin": false
}
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| email | string | Yes | Must be unique |
| first_name | string | Yes | 1–150 chars |
| last_name | string | Yes | 1–150 chars |
| password | string | Yes | Min 8 chars |
| is_company_admin | boolean | No | Default: false |

**Success (201):**
```json
{
  "message": "User added",
  "data": {
    "id": 3,
    "email": "newuser@company.com",
    "first_name": "New",
    "last_name": "User",
    "is_company_admin": false
  }
}
```

**Errors:**
- `400` – "A user with this email already exists" or validation error
- `403` – Not a company admin

---

### 8. Update User Role

**PATCH** `/api/settings/company/team/<user_id>/`

**Auth:** `Authorization: Bearer <access_token>`

**Access:** Company admin only

**Request:**
```json
{
  "is_company_admin": true
}
```

**Success (200):**
```json
{
  "message": "User role updated",
  "data": {
    "id": 2,
    "is_company_admin": true
  }
}
```

**Errors:**
- `400` – "Cannot remove the last company admin" (when demoting)
- `403` – Not a company admin

---

### 9. Remove User from Company

**DELETE** `/api/settings/company/team/<user_id>/`

**Auth:** `Authorization: Bearer <access_token>`

**Access:** Company admin only

**Success (200):**
```json
{
  "message": "User removed from company"
}
```

**Errors:**
- `400` – "Cannot remove the last company admin"
- `403` – Not a company admin

---

## JWT / Login Updates

The JWT and login response now include `is_company_admin`:

**Login response:**
```json
{
  "message": "User logged in",
  "data": {
    "user_id": 1,
    "access_token": "eyJ...",
    "is_company_admin": true
  }
}
```

**Register response (Step 1):** The first user in a company is always admin. The response includes `access_token`; decode it to get `is_company_admin: true`.

---

## Settings Page Layout

| Section | Endpoint | Who |
|---------|----------|-----|
| **Profile** | GET/PUT `/api/settings/profile/` | All users |
| **Change password** | POST `/api/auth/change-password/` | All users |
| **Company details** | GET/PUT `/api/settings/company/` | All view; admin edit |
| **Team** | GET `/api/settings/company/team/` | All view |
| **Add user** | POST `/api/settings/company/team/` | Admin only |
| **Update role** | PATCH `/api/settings/company/team/<id>/` | Admin only |
| **Remove user** | DELETE `/api/settings/company/team/<id>/` | Admin only |

## UI Notes

1. **Profile tab:** Form with first_name, last_name, email. Save calls PUT profile.
2. **Password tab:** Form with current_password, new_password. Submit calls POST /auth/change-password/.
3. **Company tab:** Company details form. Admin can edit; others view-only.
4. **Team tab:** Table of users with role (Admin/User). Admin-only: Add user button, role toggle, remove button.
5. **Role display:** Show "Admin" or "User" based on `is_company_admin`.
6. **Self-actions:** Admin cannot remove themselves if they are the last admin. Disable remove/demote for current user when they are the only admin.
