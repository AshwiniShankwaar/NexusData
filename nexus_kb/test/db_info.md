# Database Topology (Dialect: SQLITE)

## Table: `users` (~3 rows)
> Stores information about users of the system.

**Columns:**
- `id` (INTEGER) [PK] — Unique identifier for each user.  *(ENUM — allowed values: 1, 2, 3)*  *(stats: min=1, max=3, avg=2.0)*
- `name` (TEXT) — Full name of the user.  *(ENUM — allowed values: 'Alice', 'Bob', 'Charlie')*
- `region` (TEXT) — Geographic region where the user is located.  *(ENUM — allowed values: 'UK', 'US')*
- `status` (TEXT) — Current activity status of the user (e.g., active, inactive).  *(ENUM — allowed values: 'active', 'inactive')*

---

## Table Relationships

- No relationships detected.
