# Acorn Web — Backend Security Audit

Scope: every file under `backend/`, plus cross-cutting checks (CORS, OAuth state, secrets in repo + git history, frontend exposure of the Supabase service-role key).

This is a read-only audit. No code has been changed.

Routers loaded by `backend/main.py`:
- `auth.py` (`/api/auth/*`)
- `qbo_oauth.py` (`/api/qbo/*`)
- `reports.py` (`/api/reports/*`)
- `mapping.py` (`/api/mapping/*`)

Other backend files: `jobs.py` (helper module — no routes), `portal_prep.py` (helper module — no routes), `billing.py` (empty file), `storage.py` (empty file), `worker.py` (empty file), `core/` (helper modules used by reports — no routes).

---

## 1. Endpoint inventory

Risk legend: **HIGH** = exploitable today, leaks/lets-attacker-control sensitive data. **MEDIUM** = weak control, hardening recommended. **LOW** = no realistic exploit observed.

### `backend/main.py`

| Method + path | File:line | Auth | Authz | Service role | Input validation | Risk |
|---|---|---|---|---|---|---|
| `GET /health` | main.py:58 | No (intentional) | N/A | No | — | LOW |
| `GET /` | main.py:62 | No (intentional) | N/A | No | None needed | LOW |
| `GET /login.html` | main.py:66 | No (intentional) | N/A | No | Hard-coded path | LOW |
| `GET /app.html` | main.py:70 | No (intentional) | N/A | No | Hard-coded path | LOW |
| `GET /style.css` | main.py:74 | No (intentional) | N/A | No | Hard-coded path | LOW |
| `GET /favicon.svg` | main.py:78 | No (intentional) | N/A | No | Hard-coded path | LOW |
| `GET /favicon.png` | main.py:82 | No (intentional) | N/A | No | Hard-coded path | LOW |

### `backend/auth.py`

| Method + path | File:line | Auth | Authz | Service role | Input validation | Risk |
|---|---|---|---|---|---|---|
| `POST /api/auth/signup` | auth.py:46 | Yes | Yes — requires `user.user_metadata.admin == true` (auth.py:50) | `get_supabase_admin()` is created in `get_current_user`; signup itself uses anon (auth.py:52) | Email/password are passed as-is to Supabase; Supabase enforces format | LOW |
| `POST /api/auth/login` | auth.py:63 | No (intentional — pre-auth) | N/A | No (anon) | Pydantic `email/password: str` only; Supabase enforces format | LOW |
| `POST /api/auth/refresh` | auth.py:84 | No (intentional — refresh flow) | Implicit via refresh token | No (anon) | Refresh token passed to Supabase | LOW |

Note on `get_current_user` (auth.py:36-43): uses the **service-role key** (`get_supabase_admin()`) just to call `auth.get_user(jwt)`. The anon key would be sufficient for token validation. Functionally correct but unnecessary privilege.

### `backend/qbo_oauth.py`

| Method + path | File:line | Auth | Authz | Service role | Input validation | Risk |
|---|---|---|---|---|---|---|
| `GET /api/qbo/auth-url` | qbo_oauth.py:38 | Yes | N/A — generates URL for the caller | No | None | LOW |
| `GET /api/qbo/callback` | qbo_oauth.py:52 | **No** | **No** — `user_id = state` (qbo_oauth.py:55) trusts the URL parameter as identity | Yes — upserts `qbo_tokens` (qbo_oauth.py:111-120) without any ownership check | `code`, `realmId`, `state` accepted as raw strings | **HIGH** |
| `GET /api/qbo/companies` | qbo_oauth.py:126 | Yes | Yes — `.eq("user_id", str(user.id))` (qbo_oauth.py:132) | Yes; ownership filter present | None needed | LOW |
| `DELETE /api/qbo/companies/{realm_id}` | qbo_oauth.py:136 | Yes | Yes — `.eq("user_id", str(user.id)).eq("realm_id", realm_id)` (qbo_oauth.py:140-142) | Yes; ownership filter present | `realm_id` not format-validated, but used only as Supabase filter (parameterized) | LOW |
| `POST /api/qbo/refresh-token/{realm_id}` | qbo_oauth.py:146 | Yes | Yes — same ownership filter (qbo_oauth.py:150-152, 190) | Yes; ownership filter present | Same as above | LOW |

`/api/qbo/companies` returns only `realm_id, company_name, updated_at` — never `access_token` or `refresh_token`. Refresh-token endpoint also does not echo tokens to the client. Good.

### `backend/reports.py`

| Method + path | File:line | Auth | Authz | Service role | Input validation | Risk |
|---|---|---|---|---|---|---|
| `POST /api/reports/generate` | reports.py:708 | Yes | Yes — `user_id=str(user.id)` is passed to `create_job` (reports.py:718) and the worker re-queries `qbo_tokens` with the same user_id (reports.py:51) | Yes; both queries scoped to user_id | `realm_id`, `start_date`, `end_date` are bare `str` (reports.py:24-27); no format check; sliced `[:7]` into a filename (reports.py:108) | **MEDIUM** |
| `GET /api/reports/job/{job_id}` | reports.py:737 | Yes | Yes — `if not job or job["user_id"] != str(user.id): 404` (reports.py:741) | Yes; ownership re-checked in app code | `job_id` used only as Supabase filter | LOW |
| `POST /api/reports/job/{job_id}/cancel` | reports.py:746 | Yes | Yes — same ownership check (reports.py:750) | Yes | Same | LOW |
| `GET /api/reports/history` | reports.py:756 | Yes | Yes — `get_user_jobs(str(user.id))` filters by user_id (jobs.py:49-51) | Yes | None needed | LOW |

Note on the worker (reports.py:35 `run_report_job`, called via `threading.Thread` from `/generate`): the worker re-fetches `qbo_tokens` filtered by the same `user_id`/`realm_id` it was given (reports.py:49-51) and writes the file to `f"{user_id}/{job_id}/{file_name}"` in Supabase storage (reports.py:685). The signed URL it returns is time-limited (3600s, reports.py:692-693). No leakage path observed.

### `backend/mapping.py`

| Method + path | File:line | Auth | Authz | Service role | Input validation | Risk |
|---|---|---|---|---|---|---|
| `GET /api/mapping/coa/{realm_id}` | mapping.py:41 | Yes | Yes — plan gate (mapping.py:44) + `get_tokens(str(user.id), realm_id)` returns 404 if no row (mapping.py:31-38) | Yes; ownership filter present | `realm_id` used in `f"{QBO_API_BASE}/v3/company/{realm_id}/query"` (mapping.py:59); not format-validated | LOW (QBO API will reject malformed IDs) |
| `GET /api/mapping/debug/{realm_id}` | mapping.py:98 | Yes | Yes — `.eq("user_id", str(user.id)).eq("realm_id", realm_id)` (mapping.py:103-105) | Yes | Same | LOW |
| `GET /api/mapping/{realm_id}` | mapping.py:127 | Yes | Yes — same ownership filter (mapping.py:132-134) | Yes | Same | LOW |
| `POST /api/mapping/{realm_id}` | mapping.py:144 | Yes | Yes — same ownership filter (mapping.py:149-151) | Yes; upsert is scoped | `account_maps` is `list[Any]` (mapping.py:141) — schema-less JSON stored as-is; only the owner can read/write it, but a malicious user could store arbitrary blobs that break their own report rendering | LOW |

Plan gating note (mapping.py:14-18, reports.py:712-715): plan is read from `user.user_metadata.plan`. By default, Supabase clients **can** call `auth.updateUser({ data: { plan: "admin" } })` and modify their own `user_metadata` unless explicitly restricted. See finding **F-3**.

---

## 2. Cross-cutting checks

### CORS (main.py:12-18) — verbatim

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

`allow_origins=["*"]` combined with `allow_credentials=True` is a misconfiguration. Per the CORS spec browsers refuse to send cookies/`Authorization` headers when the response is `Access-Control-Allow-Origin: *`, so the **practical** impact is reduced — but Starlette/FastAPI's `CORSMiddleware` echoes the requesting origin back when it sees `*` + credentials, which **does** pass the browser check. That makes any web origin able to issue credentialed requests to the API.

Today the frontend keeps the Supabase JWT in `localStorage` and sends it as a Bearer header (not a cookie), so the immediate CSRF surface is small (the attacker would need to read the victim's JWT, which is same-origin protected). But: any new feature that sets cookies, any browser-extension issued request, or any future change that puts the JWT in a cookie becomes immediately exploitable. This is an unsafe default.

### OAuth state validation (qbo_oauth.py:38-55) — verbatim

Generation:
```python
@router.get("/auth-url")
def get_auth_url(user=Depends(get_current_user)):
    params = {
        "client_id":     QBO_CLIENT_ID,
        "response_type": "code",
        "scope":         QBO_SCOPES,
        "redirect_uri":  QBO_REDIRECT_URI,
        "state":         str(user.id),
    }
```

Callback:
```python
@router.get("/callback")
async def qbo_callback(code: str, realmId: str, state: str = ""):
    """Handle QBO OAuth callback — exchange code for tokens and store in Supabase."""
    user_id = state  # we passed user_id as state
```

This is broken in two ways:

1. **State is not random.** It is the user's Supabase UUID, which is not secret — every authenticated user knows their own UUID, and any user shows up in shared logs, headers, JWTs, and `realm_id`-keyed paths.
2. **State is not verified.** The callback simply trusts whatever string the redirect carries and uses it as `user_id` for the row it upserts into `qbo_tokens`.

Combined with the callback being unauthenticated, this means an attacker who knows a victim's Supabase UUID (or guesses one — UUIDs are random so this is hard, but they leak from URLs, support emails, and so on) can:
- Initiate an OAuth flow using their own QBO sandbox, then arrive at `…/api/qbo/callback?code=…&realmId=ATTACKER_REALM&state=VICTIM_UUID`. The backend will store the attacker-controlled QBO connection on the victim's Acorn account. From that moment, when the victim runs a report, it pulls from the attacker's books — useful for phishing, fake invoices, or planted journal entries that the victim then reviews with their team.
- Mirror the attack the other direction: trick a victim through the legitimate flow but rewrite the `state` query parameter to the attacker's UUID, and the victim's QBO tokens land on the attacker's account, giving the attacker silent read access to the victim's books for the lifetime of the refresh token.

There is no nonce, no per-flow secret, no expiry, and no CSRF token.

### Hardcoded secrets and git history

- `.env` is in `.gitignore` (line 1).
- `git log --all -- ".env*"` shows two commits: `cb2c38e initial scaffold` and `bcc176d add user auth`. Both touch only `.env.example`, which contains empty placeholders (`SUPABASE_URL=`, `SUPABASE_SERVICE_KEY=`, `STRIPE_SECRET_KEY=`, `JWT_SECRET=`, etc., all blank). No real secret has been committed.
- Repo-wide grep for JWT-shaped strings (`eyJ…`) and Stripe live/test keys (`sk_live_`, `sk_test_`, `pk_live_`) returned no matches.
- All secrets in code are loaded via `os.getenv(...)`.

### Frontend exposure of `SUPABASE_SERVICE_KEY`

- Grep over `frontend/` for `SUPABASE_SERVICE`, `service_role`, `SERVICE_ROLE`, `client_secret`, `CLIENT_SECRET` — no matches.
- Frontend does not include any Supabase keys at all (no anon key either). It calls the backend with a Bearer JWT obtained from `POST /api/auth/login`.
- Confirmed clean.

---

## 3. Critical findings

### F-1 — CORS allows any origin to send credentialed requests **(HIGH)**

`backend/main.py:12-18`. `allow_origins=["*"]` together with `allow_credentials=True` permits any web page to fetch the API while passing the user's `Authorization` header through. Today the JWT is in `localStorage` so the realistic attack surface is narrow, but this is a permanent foot-gun and any future cookie-based session immediately becomes CSRF-able.

### F-2 — OAuth callback is unauthenticated and uses an unvalidated, predictable `state` **(HIGH)**

`backend/qbo_oauth.py:38-55, 110-120`. The QBO redirect handler:
- has no `Depends(get_current_user)`,
- treats the URL-supplied `state` as the user identity (`user_id = state`),
- writes the resulting QBO tokens into `qbo_tokens` for that user.

Because `state` is just the user's Supabase UUID (`str(user.id)` in `/auth-url`) and is never stored/verified in the callback, an attacker who knows a victim's UUID — or who controls the QBO consent leg — can graft an attacker-controlled QBO connection onto the victim's account or graft a victim's tokens onto an attacker's account. Either direction lets the attacker see (or substitute) financial data the victim trusts.

### F-3 — Plan check reads `user.user_metadata.plan`, which users may be able to self-update **(MEDIUM)**

`backend/auth.py:50` (admin gate on `/signup`), `backend/mapping.py:16-18` (`_require_mapping_plan`), `backend/reports.py:712-715` (admin-only Portal/GL detail).

By default Supabase Auth lets a logged-in client call `supabase.auth.updateUser({ data: { plan: "admin" } })` and persist arbitrary `user_metadata`. If the project hasn't explicitly locked this down (via a database trigger on `auth.users`, an "update user metadata" edge function with a service-role allow-list, or a separate `user_plans` table behind RLS), then any authenticated user can elevate themselves to `admin` and unlock paid features and the `/signup` admin endpoint. Worth verifying in the Supabase project before treating these gates as security boundaries.

### F-4 — `start_date` / `end_date` are unvalidated and flow into a storage path **(MEDIUM)**

`backend/reports.py:24-27, 108, 685`. The Pydantic model declares them as bare `str`. They are sliced (`start_date[:7]`) and concatenated into `file_name = f"{clean_name}_{start_date[:7]}_{end_date[:7]}.xlsx"`, which then becomes `storage_path = f"{user_id}/{job_id}/{file_name}"` for Supabase Storage. `clean_name` is sanitized (`re.sub(r'[^\w]', '_', company_name)`), but `start_date[:7]` is not — values like `../../x` or `..\\..\\` survive the slice.

The downstream `generate_lite()` call also receives the unsanitized strings as report parameters and feeds them to date math; malformed input will raise an unhandled exception inside the worker thread, marking the job failed. Not a remote-code-execution path, but it's an input-validation gap on a user-facing endpoint, and storage-key handling is an area where mistakes age badly.

### F-5 — `get_current_user` uses the service-role key just to validate tokens **(LOW)**

`backend/auth.py:36-43`. Functional, but the service-role client should not be the default tool for verifying user JWTs — the anon client is sufficient and limits the blast radius if the function is ever extended.

---

## 4. Recommended fixes

For each finding, the smallest change that closes the gap. **Not yet applied** — review and approve before I implement.

### F-1 — Restrict CORS

`backend/main.py`, replace lines 12-18:

```python
import os

allowed_origins = [
    o.strip()
    for o in os.getenv("CORS_ALLOWED_ORIGINS", "").split(",")
    if o.strip()
]
if not allowed_origins:
    raise RuntimeError("CORS_ALLOWED_ORIGINS must be set (comma-separated list)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)
```

Then set `CORS_ALLOWED_ORIGINS=https://<your-prod-host>,https://<your-staging-host>` in the deployment env. The hard failure on missing config is intentional — silent fallback to `*` is what got us here.

### F-2 — Real OAuth state

Two-part fix in `backend/qbo_oauth.py`:

1. Generate a random one-time state in `/auth-url` and bind it to `user.id` server-side. Since the project already uses Supabase, the cheapest store is a small `oauth_states` table:

   ```sql
   create table oauth_states (
     state       text primary key,
     user_id     uuid not null,
     created_at  timestamptz not null default now()
   );
   ```

   Then:

   ```python
   import secrets

   @router.get("/auth-url")
   def get_auth_url(user=Depends(get_current_user)):
       state = secrets.token_urlsafe(32)
       supabase = get_supabase()
       supabase.table("oauth_states").insert({
           "state": state,
           "user_id": str(user.id),
       }).execute()
       params = {
           "client_id":     QBO_CLIENT_ID,
           "response_type": "code",
           "scope":         QBO_SCOPES,
           "redirect_uri":  QBO_REDIRECT_URI,
           "state":         state,
       }
       return {"auth_url": QBO_AUTH_URL + "?" + urllib.parse.urlencode(params)}
   ```

2. Verify and consume the state in the callback (and reject anything older than ~10 minutes):

   ```python
   @router.get("/callback")
   async def qbo_callback(code: str, realmId: str, state: str = ""):
       supabase = get_supabase()
       row = supabase.table("oauth_states").select(
           "user_id, created_at"
       ).eq("state", state).execute()
       if not row.data:
           raise HTTPException(status_code=400, detail="Invalid or expired state")
       record = row.data[0]
       created = datetime.fromisoformat(record["created_at"].replace("Z", "+00:00"))
       if datetime.now(created.tzinfo) - created > timedelta(minutes=10):
           supabase.table("oauth_states").delete().eq("state", state).execute()
           raise HTTPException(status_code=400, detail="State expired")
       user_id = record["user_id"]
       supabase.table("oauth_states").delete().eq("state", state).execute()
       # …rest of callback unchanged…
   ```

   Add a periodic cleanup (cron or Supabase scheduled function) for stale `oauth_states` rows. The callback itself stays unauthenticated (Intuit redirects the browser without our session), but the state binding now provides the identity assertion.

### F-3 — Make plan tamper-resistant

Two options, in order of preference:

(a) Move `plan` out of `user_metadata` into a `user_plans` table (`user_id uuid pk references auth.users, plan text not null, updated_at timestamptz`) with RLS that only allows reads from the owning user and writes from the service role / Stripe webhook. Update `_require_mapping_plan`, the admin gate in `auth.py`, and the gates in `reports.py` to read from this table via the service-role client.

(b) If you want to keep `user_metadata.plan`, add a Postgres trigger on `auth.users` that resets `raw_user_meta_data->'plan'` to its previous value on any non-service-role update — and rely on it. A trigger is fragile compared to (a) but doesn't require schema changes outside `auth`.

Either way, confirm via a quick test that an authenticated user cannot self-promote: log in as a starter user, call `supabase.auth.updateUser({ data: { plan: 'admin' } })` from the browser console, then re-fetch `/api/auth/login`'s response and confirm `plan` did not change.

### F-4 — Validate dates

`backend/reports.py:24-33`, replace the model with:

```python
from datetime import date, datetime
from pydantic import BaseModel, field_validator

class GenerateRequest(BaseModel):
    realm_id:           str
    start_date:         str
    end_date:           str
    dimension:          str = "none"
    selected_maps:      list[str] = []
    include_gl_detail:  bool = False
    include_portal_data: bool = False
    include_ar_aging:   bool = False
    include_ap_aging:   bool = False

    @field_validator("start_date", "end_date")
    @classmethod
    def _yyyy_mm_dd(cls, v: str) -> str:
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError("must be YYYY-MM-DD")
        return v

    @field_validator("realm_id")
    @classmethod
    def _realm_id_digits(cls, v: str) -> str:
        if not v.isdigit() or not (1 <= len(v) <= 32):
            raise ValueError("realm_id must be a numeric string")
        return v
```

Pydantic will reject malformed input with a 422 before it reaches the worker, the filename composition, or the storage key. If you'd rather not break any in-flight clients, log first for a few days and then enforce.

### F-5 — Use the anon key in `get_current_user`

`backend/auth.py:36-43`:

```python
def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Validate JWT token and return user."""
    supabase = get_supabase_anon()
    try:
        result = supabase.auth.get_user(credentials.credentials)
        return result.user
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
```

The anon key has the right scope for `auth.get_user(jwt)` and removes an unnecessary use of the service role on every authenticated request.

---

## 5. Items reviewed and judged OK

- All authenticated routes verify ownership via `eq("user_id", str(user.id))` (or, for jobs, an explicit `job["user_id"] != str(user.id)` 404). No IDOR observed on the routes that exist today.
- `qbo_tokens` is never returned to the client. `/api/qbo/companies` selects only `realm_id, company_name, updated_at`. Refresh-token endpoint returns only `{refreshed, expires_at}`.
- Supabase queries are built via the Python SDK's filter chain, not string concatenation — no SQL injection surface in the routes.
- Static file routes hard-code their paths; no user input.
- `.env` is gitignored, no secrets in git history, frontend has no service-role key.
