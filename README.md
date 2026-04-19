# Acorn Web

QBO data extraction and reporting tool by Oakbridge Finance.

## Stack
- **Backend:** FastAPI + Python
- **Frontend:** Single-page HTML/JS app
- **Database:** Supabase (PostgreSQL + Auth + Storage)
- **Hosting:** Railway (backend), Vercel (marketing site)
- **QBO Integration:** OAuth 2.0 + QuickBooks Online API

## Development
- `dev` branch for development
- `master` branch for production (auto-deploys to Railway)
- Run locally: `uvicorn backend.main:app --reload`

## Environment Variables
- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`
- `QBO_CLIENT_ID`
- `QBO_CLIENT_SECRET`
- `QBO_REDIRECT_URI`
- `CORS_ALLOWED_ORIGINS`
