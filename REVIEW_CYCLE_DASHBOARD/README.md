# Review Cycle Dashboard

Review Cycle views run **inside the unified OD Collection app** (`streamlit run tucson_CR.py` from the repo root). There is no separate login or standalone `app.py`.

## Configuration

Use the **repo root** `.env` (same Snowflake, S3, and MySQL variables as the OD dashboard).

```env
# Optional — defaults to REVIEW_CYCLE when omitted (same pattern as APP_CONFIG_SCHEMA)
REVIEW_CYCLE_SCHEMA=REVIEW_CYCLE_TEST   # local dev / isolated test data
# REVIEW_CYCLE_SCHEMA=REVIEW_CYCLE      # production (or omit the line entirely)

APP_CONFIG_SCHEMA=APP_CONFIG            # source of project definitions (shared with OD)
SNOWFLAKE_PRIVATE_KEY_PATH=path/to/key.p8
LOCAL_DATA_DIR=local_data
```

- **Local:** set `REVIEW_CYCLE_SCHEMA=REVIEW_CYCLE_TEST` and keep that schema empty in Snowflake. The first time you open Review Cycle, tables are created and projects are copied from `APP_CONFIG` automatically.
- **Live:** omit `REVIEW_CYCLE_SCHEMA` (or set `REVIEW_CYCLE`) — the app uses the production `REVIEW_CYCLE` schema.

Place the encrypted Snowflake private key at `path/to/key.p8` relative to the repo root.

## Access

After signing in at `/?page=login`, users with Review Cycle access choose **Review Cycle Dashboard** on the portal picker. Role mapping:

| OD role | RCD access |
|---------|------------|
| Super admin (3 emails) | Full (`admin`), including Sync & Admin |
| ADMIN | Manager pages (review, cleaning, flags, etc.) |
| CLEANING | Cleaning + history |
| USER | Field team |

Staff self-registration: `/?page=create_user` (default role **CLEANING**). Super admins manage roles in OD **Accounts Management**.

## Pipeline workspace

Pipeline runs use an **ephemeral OS temp directory** (`rcd_workspace` under the system temp folder), deleted automatically after each sync. Nothing is stored under `REVIEW_CYCLE_DASHBOARD/pipeline/workspace/` on the server.

## Shared sync with OD Collection

When the OD dashboard syncs, this app can detect newer `last_survey_date` values and offer **Pull latest records** on cleaning views.
