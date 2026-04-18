# Acorn Web — Pre-Launch Code Review

**Reviewer:** Senior engineer audit (pre-production)
**Branch:** `dev`
**Date:** 2026-04-18
**Scope:** every file under `backend/` and `frontend/`
**Methodology:** read-only review. No code changes. Findings cite `file:line` where possible.

---

## Executive Summary

**Overall grade: C+ (conditional pass)**

The codebase is functionally solid and shows that the authors have thought about auth, plan gating, and OAuth state handling. Most of the security fundamentals are correct: bearer-token auth, service-role key kept server-side, CSRF-safe stateless auth, parameterized Supabase filters, user-scoped DB queries, OAuth state validation.

However, there is **one critical cross-user data-leakage bug** that must be fixed before production, and a cluster of hardening gaps (no login rate limit, in-memory rate limiter, orphaned debug endpoint, threading-based job runner with no cancellation) that should be closed before promoting widely.

### Top 5 launch-blockers

1. **`qbo_client._override_tokens` is a mutable module-level global used across threads** — two concurrent report jobs will cross-contaminate tokens and realm IDs. User A's download can contain User B's QBO data. (`backend/core/qbo_client.py:18-24`, used from the background thread in `backend/reports.py:130-134`.)
2. **`POST /api/auth/login` has no rate limit** — brute force on known emails is unthrottled. Only signup is limited. (`backend/auth.py:145-166`.)
3. **Rate limiter is in-memory (`_signup_attempts` dict)** — ineffective across multiple Uvicorn/Gunicorn workers or Railway replicas. (`backend/auth.py:72-83`.)
4. **`POST /api/reports/generate` spawns an unbounded `threading.Thread` per call** — no concurrency limit per user, no queue, no restart recovery. One user clicking "Generate" repeatedly can exhaust the worker process. The cancel endpoint does not actually stop the running thread. (`backend/reports.py:1691-1701`, cancel at `1713-1720`.)
5. **`GET /api/mapping/debug/{realm_id}` is a live debug endpoint** — comment says "Temporary debug" but it ships to production. Low data impact (returns user-scoped data only) but it is dead surface area that signals immaturity. (`backend/mapping.py:98-124`.)

### What's in good shape

- Auth: every sensitive endpoint requires `Depends(get_current_user)`.
- User scoping: every Supabase query on user-owned data filters by `user_id` drawn from the verified JWT, never from the request body.
- OAuth state: one-time tokens stored in DB, TTL'd at 10 min, bound to user, consumed on success. Good.
- XSS: almost every innerHTML in `app.html` routes user-controlled data through `escHtml()`.
- Auth surface for download: `/api/reports/download/{job_id}` re-checks ownership before streaming the file.
- No SQL injection in active code paths (Supabase PostgREST is parameterized; QBO `fetch_query` is SQL but it's unused).

---

## Critical Issues (launch-blockers)

### C-1. Cross-user token leakage via `qbo_client._override_tokens` (module-level mutable state)

**Files:** `backend/core/qbo_client.py:18-24, 73-76, 146-148, 186-188`; `backend/reports.py:129-136, 1651-1657, 1691-1701`

`qbo_client` holds the per-request QBO tokens in a **module-level global `_override_tokens`**. `reports.run_report_job()` runs in a background `threading.Thread` and calls `qbo_client.set_override_tokens({...})` to inject the authenticated user's tokens for the duration of the job. `generate_lite()` then calls back into `qbo_client.fetch_report()` / `fetch_accounts()` which read the same module global.

Threads share the module namespace. The following interleaving is possible:

```
T0  Thread A (User Alice)  set_override_tokens({alice_tokens, realm=111})
T1  Thread B (User Bob)    set_override_tokens({bob_tokens,   realm=222})
T2  Thread A               fetch_report() reads _override_tokens → {bob_tokens, realm=222}
T3  Thread A               GET https://quickbooks.api.intuit.com/v3/company/222/...
T4  generate_lite returns  results written to tempfile
T5  reports.py             uploads tempfile to storage at "{alice_user_id}/{job_id}/..."
```

Alice downloads a report containing Bob's company's data. This is a textbook multi-tenant data-leakage bug and a privacy-law incident if it happens in production with paying customers.

The `finally` block at `reports.py:1653-1657` (`set_override_tokens(None)`) only clears *after* the job finishes — it does nothing to prevent interleaving *during* the job.

**Fix:** stop using module-level globals. Pass tokens explicitly through `fetch_report(..., access_token=, realm_id=)` arguments all the way through `gl_extractor`. Alternatively, use `threading.local()` — but argument threading is clearer and easier to audit.

Until this is fixed, the system must not run more than one report generation concurrently. Today nothing enforces that.

### C-2. Concurrent report generation is completely unbounded

**File:** `backend/reports.py:1691-1701`

`generate_report` creates a new `threading.Thread(..., daemon=True).start()` for every request. There is:

- no per-user concurrency cap
- no global concurrency cap
- no queue / no backpressure
- no persistence — if the process restarts mid-job, the job row is stuck in `running` forever
- no retry path
- no actual cancellation (see C-3)

A user clicking "Generate" ten times queues ten concurrent QBO pulls against their realm. Under C-1, ten concurrent jobs across two users means guaranteed data corruption.

**Fix for launch:** wrap job submission in a `threading.Semaphore(1)` or `Semaphore(small_number)` at minimum; reject with 429 when full. Longer term, use a real task queue (RQ, Arq, Celery) with Supabase or Redis as broker.

### C-3. Cancel endpoint does not cancel

**File:** `backend/reports.py:1713-1720`

```python
@router.post("/job/{job_id}/cancel")
def cancel_job(job_id, user=...):
    ...
    update_job(job_id, status="failed", error="Cancelled by user")
```

This updates the DB row. The worker thread keeps running, keeps hitting QBO, and at line 1648 writes `status="complete"` on the same row, overwriting the cancel. The user sees the job flip from `failed` back to `complete`. The "Stop" button in the UI is a lie.

**Fix:** use a cancel flag in the DB that the worker checks at each progress step, or implement proper task cancellation.

---

## High Priority

### H-1. No rate limit on `/api/auth/login`

**File:** `backend/auth.py:145-166`

Only `/signup` is rate-limited (`auth.py:121-122`). Login has no throttling — an attacker can brute-force passwords on a known email address. Supabase itself may impose some limit but we should not rely on it.

**Fix:** add a per-IP and per-email limiter on login, and on `/refresh`.

### H-2. Rate limiter is in-memory — doesn't survive multiple workers

**File:** `backend/auth.py:72-83`

`_signup_attempts: dict[str, list[float]]` is process-local. Railway typically runs Gunicorn with multiple worker processes. An attacker distributes 5 attempts per worker and defeats the limit. Restarting the process also resets it.

**Fix:** move limits to Redis, or use Supabase as a shared store (a `rate_limits` table), or a proper middleware (`slowapi` with a Redis backend).

### H-3. `POST /api/reports/generate` does not verify the caller owns `realm_id` before spawning a thread

**File:** `backend/reports.py:1660-1701`

The endpoint calls `create_job` (which stores the realm_id) and starts a thread. The thread then queries `qbo_tokens` filtered by `(user_id, realm_id)` at line 81-84 and fails the job if no row exists. The check is *implicit and late* — it runs after allocating a thread, creating a DB row, and returning 200 to the client.

A malicious user can't read another user's data via this path (the token query is user-scoped), but they can:

- spawn an unbounded number of "failed" background jobs cheaply (DoS vector; compounds with C-2)
- pollute their own job history table

**Fix:** validate `qbo_tokens` ownership synchronously in `generate_report` before creating the job row. Return 404 if the user doesn't own the realm.

### H-4. Debug endpoint in production

**File:** `backend/mapping.py:98-124`

`GET /api/mapping/debug/{realm_id}` is labelled "Temporary debug — show raw mapping structure." It is scoped to the caller's user_id (so it doesn't leak across users) but it:

- returns internal structure the UI doesn't need
- is an unnecessary surface for future regressions
- signals that cleanup before launch was skipped

**Fix:** delete the endpoint.

### H-5. `escHtml` does not escape single quotes — self-XSS via onclick handlers

**File:** `frontend/app.html:386`, used in `581-589, 1285-1293, 1508-1533, 1526-1532`

```js
const escHtml = s => s ? String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;') : '';
```

`'` is not escaped. The output is embedded inside single-quoted JavaScript arguments on `onclick` attributes:

```js
onclick="selectClient('${escHtml(c.realm_id)}', '${escHtml(c.company_name)}')"
```

A value like `'); alert(1); //` in `company_name` breaks out of the JS string and executes arbitrary code when clicked. `company_name` is fetched from QBO (`qbo_oauth.py:197-201`) — the QBO account owner controls this, so it is user-provided data.

**Severity: self-XSS only.** Each user's app is isolated by bearer token; a user can only XSS *their own* session by naming their own QBO company maliciously. Not a cross-tenant attack. But combined with localStorage tokens (H-8), an attacker with *any* script-injection vector upgrades to token theft.

**Fix:** add `'` escaping to `escHtml`, or switch onclick bindings to `addEventListener` so the data never touches HTML parsing.

Same pattern appears for mapping names (`1285-1293`), account names (`1526-1532`), and group names.

### H-6. Threading runs full QBO fetch + Excel build in the request process

**File:** `backend/reports.py:1691-1701`

Already covered in C-2 for the concurrency risk. Additionally:

- a long-running CPU/IO thread blocks the process from graceful shutdown on deploy
- no structured logging per job; progress goes through `update_job(progress=...)` — readable, but worker state is invisible to ops
- tempdir lifecycle is tied to the thread; a process kill mid-run leaks the file and leaves `status=running`

**Fix:** real task queue. Short-term mitigation: add a startup hook that flips any `status in ('pending','running')` rows to `failed` with error "worker restart" so the UI doesn't hang.

### H-7. Signed URL is generated, stored, and then not used

**File:** `backend/reports.py:1641-1648` (generate and store), `frontend/app.html:973-1009` (only uses the URL to extract a filename — actual download goes through `/api/reports/download/{job_id}`)

The 1-hour signed URL is persisted in the `jobs` table but never used for download. Two problems:

- wasted work
- the URL sits in the DB for anyone with DB read access — stale, but for 1 hour it's a working bearer to the file. If a third party reads `jobs` rows (future feature, reporting, exports), they can download the file.

**Fix:** drop the signed-URL generation; just store the storage_path, have the download endpoint retrieve on demand (which is already what the code does).

### H-8. Tokens in `localStorage`

**File:** `frontend/app.html:126-132` (login), `359` (getter)

Access + refresh tokens in `localStorage`. Standard for SPAs, but vulnerable to any XSS. Given H-5 exists (self-XSS), this is less of a concern *today*, but an `httpOnly` cookie for the refresh token would be safer. Kept High because token theft is the highest-impact frontend outcome.

### H-9. Signup auto-verifies email

**File:** `backend/auth.py:127-136`

`"email_confirm": True` skips the email-ownership proof. Combined with 7-day Pro trial, this lets anyone create trials with `foo+1@gmail.com`, `foo+2@gmail.com`, etc. This is a business/abuse concern; it also means Acorn cannot contact users who typo'd their email.

**Fix:** set `email_confirm: False` and have the user click the Supabase-sent link. Pro trial starts once verified.

### H-10. `get_current_user` uses the **service-role** client to validate tokens

**File:** `backend/auth.py:88-103`

```python
def get_current_user(credentials=Depends(security)):
    supabase = get_supabase_anon()
    ...
```

Actually this is `get_supabase_anon()` — **good**, fine. (Earlier SECURITY_AUDIT.md flagged use of admin key here; current code is using anon. Confirming the prior finding has been fixed.)

However, `get_supabase_admin()` is used for user lookup in `qbo_oauth.py:133` which is legitimate for the OAuth callback (no user JWT available yet). OK.

### H-11. QBO OAuth state table: expired rows never cleaned

**File:** `backend/qbo_oauth.py:106-128`

Expired state rows are deleted only if a callback is attempted with them (line 126). States where the user never returned from Intuit accumulate forever. Not a security issue, but a growing table.

**Fix:** periodic cleanup job or a Supabase cron that deletes `created_at < now() - 1 hour`.

### H-12. No password reset flow

User who forgets password has no recovery path. Supabase supports this out of the box — wiring is missing.

---

## Medium Priority

### M-1. Empty source files checked in
- `backend/billing.py` (0 bytes)
- `backend/storage.py` (0 bytes)
- `backend/worker.py` (0 bytes)
- `frontend/index.html` (0 bytes)

Delete. They look like abandoned stubs.

### M-2. Unused code in `backend/core/qbo_client.py`
- `fetch_query()` (lines 137-177) — defined, never called. Builds SQL via f-string interpolation (`f"{sql} STARTPOSITION ..."`). Not exploitable today because the callers don't exist, but if it's reactivated for user-supplied filters it becomes an injection vector against the QBO Query API. Delete or lock down.
- `set_v2_test_mode` / `_v2_test_mode` (lines 29-35) — another module-level global. Used from `gl_extractor` but also racy for the same reason as C-1.

### M-3. Unused helper `_build_amount_row` in `backend/core/report_parser.py:37`

Dead code. Remove.

### M-4. Logging includes identifiers
- `backend/core/qbo_client.py:76` logs `realm_id`
- `backend/reports.py:90` logs `company_name`
- `backend/reports.py:166, 1647` log map names and URL key lists

These are not credentials, but they are PII-adjacent and will land in Railway logs, log aggregators, etc. Scrub or hash.

### M-5. `run_report_job` does its own QBO token refresh bypassing `qbo_client`

**File:** `backend/reports.py:96-127`

The background thread refreshes tokens inline using `httpx.post`. Meanwhile `qbo_client._override_tokens` is set once and never updated if the token expires mid-job. For long reports (multi-month GL pulls can take 5+ min) this is fragile.

Also, refresh failures silently fall through (line 126-127) and the job keeps running with potentially expired tokens.

### M-6. Password policy is length-only

**File:** `backend/auth.py:117-118`

`len >= 8` with no complexity requirement. For a B2B accounting tool handling financial data, this is weak.

### M-7. Signup error detection uses substring match

**File:** `backend/auth.py:139-142`

`"already been registered" in detail.lower()` — breaks silently if Supabase changes the message. Check Supabase exception class instead.

### M-8. `_signup_attempts` defaultdict grows forever

**File:** `backend/auth.py:72`

Each new IP creates a list. Over time the dict bloats. Small leak, but unbounded.

### M-9. Background thread redundantly mutates `sys.path`

**File:** `backend/reports.py:3, 77`

Two `sys.path.insert(0, ...)` calls for the same directory — once at module load, once inside `run_report_job`. Remove the duplicate.

### M-10. Frontend sends unencoded path parameters

**File:** `frontend/app.html:621` (`removeConnection`), `743` (`reconnectQBO`), `956` (`pollJob`), `1136` (cancel)

`fetch(API + '/api/qbo/companies/' + realmId, ...)` — no `encodeURIComponent`. Today `realm_id` and `job_id` are server-generated numeric/UUID strings so it works. Defensive fix: wrap in `encodeURIComponent`.

### M-11. `pollJob` polls every 3s with no backoff or stop condition on repeated errors

**File:** `frontend/app.html:951-1021`

If the server returns 500 every poll, the loop keeps going indefinitely, logging to console each time. Add exponential backoff and a max-retry.

### M-12. `refreshTokenIfNeeded` has 5-minute cooldown regardless of outcome

**File:** `frontend/app.html:389-430`

Even a 401 mid-request doesn't trigger a retry until 5 min after the last *attempt*. If login is fresh and a call 401s for some other reason, the user is forced to re-login. Consider splitting "last successful refresh" from "last attempt".

### M-13. No CSRF on state-changing `GET` endpoint `/api/qbo/callback`

**File:** `backend/qbo_oauth.py:106-220`

Callback is `GET` (required by OAuth spec). It mutates `qbo_tokens`. Standard OAuth; safe because the `state` token is bound to the user. OK. Listed for completeness.

### M-14. Plan gating trust model

**File:** `backend/reports.py:1674-1682`

Backend silently strips premium options when plan is insufficient (`body.dimension = "none"`, `body.include_gl_detail = False`, etc.) instead of returning 403. Silent success with downgraded output is confusing — basic users click "Include GL detail" and get a report without it with no feedback. The frontend disables these inputs (`app.html:1936-1972`) but a direct API caller gets no error.

**Fix:** return 403 with a specific message; let the frontend's own gating handle UI-side disabling.

### M-15. Download endpoint lists files then picks first

**File:** `backend/reports.py:1736-1740`

```python
files = supabase.storage.from_("reports").list(f"{user_id}/{job_id}")
... file_name = files[0]["name"]
```

If an earlier attempt left orphaned files in the same `job_id` prefix, you download an arbitrary file. Safer: persist the storage path on the job row and read that.

---

## Low Priority / Quick Cleanup

- `backend/reports.py` is 1760 lines — `run_report_job` alone is ~600 lines of Excel formatting logic. Extract the workbook-decoration into a separate module (`excel_postprocess.py`).
- Many inline `import` statements (`reports.py:76, 92-97, 176-179, 255-256, 1253-1257, 1334-1337, 1481-1484`). Most can be hoisted to the top.
- `reports.py` has repeated blocks that enforce "Arial 10" across the workbook (lines 1232-1241, 1314-1323, 1613-1624) — factor out.
- `load_dotenv()` called at module-load in six different files (`auth.py:12`, `jobs.py:6`, `mapping.py:11`, `portal_prep` n/a, `qbo_oauth.py:19`, `reports.py:16`). One top-level call in `main.py` is sufficient.
- `frontend/app.html` is 2037 lines with embedded CSS and inline styles. Long-term: extract JS to `app.js` and rely on `style.css`. Short-term: the duplicated `display:none;` on line 49 is a bug (harmless).
- `qbo_oauth.py:182, 213, 286` use `datetime.utcnow()` (naive) while callback handling uses `datetime.now(timezone.utc)` (aware). Pick one.
- `auth.py:126` uses `datetime.utcnow()` — also naive. Prefer aware UTC.
- Many `except Exception:` bare handlers that swallow errors: `qbo_oauth.py:135, 202-203`; `reports.py:126-127, 348-349, 528-533, 953-961, 1216-1218, 1327-1329, 1628-1631, 1656-1657`. Each hides a potentially-important signal in logs.
- `reports.py:138`: `file_name = f"{clean_name}_{start_date[:7]}_{end_date[:7]}.xlsx"` — `clean_name` is sanitized (line 138), but slicing `start_date[:7]` without verifying it's a 10-char ISO string is fragile. Dates *are* validated by `_parse_report_dates` earlier, so safe — but the coupling is brittle.
- `auth.py:108` — email regex is permissive (`[^@\s]+@[^@\s]+\.[^@\s]+`). Fine.
- `frontend/login.html:145-147` — if `acorn_access_token` is present, redirects to `app.html`, even if the token is expired. A revoked/expired token just gets the user stuck bouncing. Handle by calling `/api/auth/refresh` first.
- `frontend/login.html:150-152` — `keydown` listener fires `doAuth()` on *any* Enter keypress on the page, including in disabled state.
- `frontend/app.html:622-633, 730-731, 1015, 1075, 1834` — lots of `alert()`/`confirm()` for errors. Ok for MVP, replace before launch with toast UI.
- `README.md` is 220 bytes (placeholder).

---

## Findings by File

### `backend/main.py`
- `:9-10` — `logging.basicConfig(level=INFO)` clobbers any handler a hosting platform configured. Prefer `logging.getLogger()` without `basicConfig`.
- `:14-18` — CORS origins parse is fine; default to the production origin is good. No findings.
- `:29-56` — router loading wrapped in `try/except`: swallows import errors into log lines, letting the app boot without critical routers. On Railway the app would run with a silently-broken API. **Remove the try/except** — import failures must be fatal.
- `:88` — `/favicon.svg` references `favicon.svg` file that does not exist in `frontend/` (the dir has `.png` only). Returns 200 + empty file or 404; harmless but confusing.

### `backend/auth.py`
- `:72-83` — in-memory rate limit (H-2).
- `:111-142` — signup creates Pro trial + auto-verifies email (H-9).
- `:126` — naive `utcnow()` (Low).
- `:139-142` — substring error detection (M-7).
- `:145-166` — login has no rate limit (H-1).
- `:169-185` — refresh has no rate limit (H-1).
- `:88-103` — uses anon client for JWT validation. Correct.

### `backend/qbo_oauth.py`
- `:54-75` — company limit check is correct, but uses `hasattr(existing, "count")` — brittle if supabase-py changes. Consider a direct `len(data)` and document assumptions.
- `:106-220` — callback logic is OK end-to-end: state validated, TTL'd, bound to user, consumed. Good.
- `:113-128` — expired state rows not cleaned up globally (H-11). Only cleaned on attempted use.
- `:132-136` — bare `except Exception:` for admin user lookup; falls back to plan='basic' which is the safe default. OK.
- `:182, 213, 286` — naive `utcnow()` (Low).
- `:202-203` — swallowed exception when fetching company name. Acceptable fallback.
- `:233-240` — delete endpoint correctly scoped to `(user_id, realm_id)`.
- `:243-289` — refresh endpoint correctly scoped. 30s timeout is fine.

### `backend/mapping.py`
- `:14-18` — plan check. Good.
- `:41-95` — COA fetcher. Async + pagination, OK. **SQL is interpolated into a f-string for the QBO Query API (`query = f"... STARTPOSITION {start_position} MAXRESULTS {page_size}"`)** — the interpolated values are ints we control. Safe.
- `:98-124` — debug endpoint (H-4). Delete.
- `:127-137` — get_mapping. User-scoped. Good.
- `:140-155` — save_mapping. `account_maps: list[Any]` — accepts arbitrary JSON shape; relies on downstream code to validate. A malicious user can save enormous payloads; no size limit. Consider a max-bytes check (Medium, documented under "Quick Wins").
- `:144-155` — the POST does **not** verify the user owns `realm_id` before upserting. Upsert is scoped to `(user.id, realm_id)` so it can't overwrite another user's map, but a basic+ user can create garbage rows for non-existent realms. Add an existence check on `qbo_tokens`.

### `backend/reports.py`
- `:3, 77` — duplicate `sys.path` insert (M-9).
- `:26-52` — `_parse_report_dates`: clean, correct. Good.
- `:55-64` — request model uses bare `str` for `realm_id` etc. Could tighten with regex/UUID.
- `:66-1657` — giant `run_report_job` function. See C-1, C-2, C-3, H-7, M-5, M-9, and the module-size note under Low.
- `:96-127` — inline QBO refresh (M-5). Failure silently continues.
- `:129-135` — **C-1 root cause**: sets `qbo_client._override_tokens` on a shared module.
- `:141-144` — `progress_fn` filters out `[progress]`-prefixed msgs with no comment explaining why. Minor.
- `:147-161` — `tempfile.TemporaryDirectory` is correct; cleanup happens automatically.
- `:184-186` — `wb = openpyxl.load_workbook(file_path)` on every stage, saved and reloaded multiple times. Correct but inefficient — ~4× full workbook rewrites per report.
- `:1232-1241, 1314-1323, 1613-1624` — three near-identical "Arial 10" enforcement loops (Low).
- `:1634-1648` — uploads the file to Supabase storage keyed by `{user_id}/{job_id}/{file_name}`. Critical: **confirm the Supabase `reports` bucket has no public read policy**. If it does, storage path enumeration is a data leak. Audit the bucket manually.
- `:1641-1647` — signed URL generated and logged (H-7, M-4).
- `:1653-1657` — `set_override_tokens(None)` in `finally`. Insufficient; see C-1.
- `:1691-1701` — threading (C-2, C-3, H-6).
- `:1723-1752` — download endpoint does ownership re-check. Good. But uses `list()` + `[0]` (M-15).

### `backend/jobs.py`
- `:16-27` — `create_job` accepts inputs as-is. Relies on caller (reports.generate_report) to validate. OK.
- `:46-52` — `get_user_jobs` limits to 20. Good.

### `backend/portal_prep.py`
- Clean module, no findings. Pure computation on passed-in rows. Good.

### `backend/core/qbo_client.py`
- `:18-35` — module-level globals (C-1 and M-2).
- `:56-134` — `fetch_report`: good error handling for 401/403/non-JSON. The URL builder (lines 91-98) hand-rolls percent-encoding to preserve commas — non-standard but documented with a comment. OK.
- `:76` — logs realm_id (M-4).
- `:137-177` — **`fetch_query` unused dead code** (M-2). Remove.
- `:180-223` — `fetch_accounts`: good. Silent `break` on non-2xx (line 210) returns partial accounts rather than raising. Should raise or at least log.

### `backend/core/token_manager.py`
- File appears to be legacy from the desktop version — reads/writes `.env`. In the web deployment path, the web backend never calls `save_company` or `_save_tokens` (tokens live in Supabase `qbo_tokens` table). Confirm by searching for callers:
  - `get_company_tokens` — referenced by `qbo_client.fetch_report` only when `_override_tokens is None`, which in web mode is always set. So unused in practice.
  - `_save_tokens`, `save_company`, `list_companies` — unused in the web flow.
- **Finding:** `token_manager.py` is desktop-only legacy code that still ships. Not a security issue (no active web path uses it) but it's dead surface. Consider deleting or moving behind a `_legacy/` folder.

### `backend/core/report_parser.py`
- `:37-43` — `_build_amount_row` unused (M-3).
- No other findings.

### `backend/core/gl_extractor.py` (2401 lines)
- Ported from desktop. Most of it is pure data-processing on dict/list structures. A full line-by-line audit is outside scope. Delegated Explore-agent audit did not surface SQL injection, command injection, or unsafe deserialization. One flag:
  - Module relies on `qbo_client._override_tokens` being set before any call. If C-1 is fixed by switching to argument passing, gl_extractor will also need its signature updated.

### `frontend/login.html`
- `:126-132` — localStorage token storage (H-8).
- `:145-147` — auto-redirect to app.html if token exists, even if expired (Low).
- `:150-152` — global Enter-key listener triggers doAuth (Low).

### `frontend/app.html`
- `:359-370` — token retrieval + auth header, OK.
- `:386` — escHtml missing `'` (H-5).
- `:389-430` — refresh logic. See M-12.
- `:581-590` — onclick with embedded escHtml (H-5).
- `:621` — unencoded realmId in URL (M-10).
- `:720-732` — QBO auth redirect; `data.auth_url` comes from our own backend. Safe.
- `:894-948` — generate() submits form. Client-side date check, then POST. No duplicate-submit prevention — user can click "Generate" again while previous job is running (reinforces C-2).
- `:951-1021` — pollJob (M-11).
- `:973-1010` — job.file_url only used to derive filename (H-7).
- `:1133-1144` — stopReport fires `/cancel` but does not actually stop the server (C-3).
- `:1279-1295` — maps list with escHtml (self-XSS concern per H-5).
- `:1336-1373` — editor open retries COA fetch on empty state. Reasonable.
- `:1925-2034` — DOMContentLoaded handler wires plan gating into the UI correctly. Good. But a direct API caller bypasses it (M-14).

### `frontend/style.css`
- 1991 lines, not spot-audited for dead rules. Low-priority cleanup.

---

## Tier Enforcement Audit

Plan definitions:

| Plan  | Companies | Date range | Mapping | Class/Location | GL Detail | Portal tabs |
|-------|-----------|-----------:|:-------:|:--------------:|:---------:|:-----------:|
| basic | 1         | ≤92 days   | ✖       | ✖              | ✖         | ✖           |
| pro   | 5         | unlimited  | ✔       | ✔              | ✔         | ✖           |
| plus  | 25        | unlimited  | ✔       | ✔              | ✔         | ✖           |
| admin | ∞         | unlimited  | ✔       | ✔              | ✔         | ✔           |

### Endpoint-by-endpoint verification

| Endpoint | Auth? | User scoped? | Plan gated? | Enforced how | Verdict |
|---|---|---|---|---|---|
| `POST /api/auth/signup` | no (correct) | n/a | n/a | rate-limit only | OK (but see H-9) |
| `POST /api/auth/login` | no (correct) | n/a | n/a | — | **No rate limit (H-1)** |
| `POST /api/auth/refresh` | refresh token | n/a | n/a | — | No rate limit (H-1) |
| `GET /api/qbo/auth-url` | ✔ | n/a | ✔ company limit | `_check_company_limit` at `qbo_oauth.py:57-75` | OK |
| `GET /api/qbo/callback` | state | via state | ✔ company limit | inline at `qbo_oauth.py:131-150` | OK |
| `GET /api/qbo/companies` | ✔ | ✔ | ✖ | — | OK (no gate needed) |
| `DELETE /api/qbo/companies/{realm_id}` | ✔ | ✔ | ✖ | — | OK |
| `POST /api/qbo/refresh-token/{realm_id}` | ✔ | ✔ | ✖ | — | OK |
| `GET /api/mapping/coa/{realm_id}` | ✔ | ✔ (via tokens) | ✔ mapping plan | `_require_mapping_plan` | OK |
| `GET /api/mapping/debug/{realm_id}` | ✔ | ✔ | ✔ mapping plan | `_require_mapping_plan` | **Live debug endpoint (H-4)** |
| `GET /api/mapping/{realm_id}` | ✔ | ✔ | ✔ mapping plan | `_require_mapping_plan` | OK |
| `POST /api/mapping/{realm_id}` | ✔ | ✔ | ✔ mapping plan | `_require_mapping_plan` | OK (but no realm ownership check, see mapping.py notes) |
| `POST /api/reports/generate` | ✔ | ✔ (via thread) | ✔ feature flags silently stripped | lines 1665-1682 | **Silent downgrade (M-14); no realm ownership check (H-3); no concurrency limit (C-2)** |
| `GET /api/reports/job/{job_id}` | ✔ | ✔ | — | line 1708 | OK |
| `POST /api/reports/job/{job_id}/cancel` | ✔ | ✔ | — | line 1717 | **Cancel doesn't actually cancel (C-3)** |
| `GET /api/reports/download/{job_id}` | ✔ | ✔ | — | line 1728 | OK |
| `GET /api/reports/history` | ✔ | ✔ | — | | OK |
| `GET /health`, `/`, static pages | no (correct) | n/a | n/a | — | OK |

### Feature-flag enforcement verification

- **Mapping (pro+):** `_require_mapping_plan` in `backend/mapping.py:14-18` covers all `/api/mapping/*` endpoints. ✔
- **Class/Location (pro+):** enforced in `reports.generate_report` by forcing `dimension="none"` for basic (line 1676). ✔ but silent (M-14).
- **GL detail (pro+):** forced `False` for basic (line 1678). ✔ but silent.
- **Portal data (admin-only):** forced `False` for pro/plus (line 1681). ✔ but silent.
- **Company limit:** enforced on `/auth-url` and `/callback`. ✔.
- **Date range (≤92d for basic):** enforced at `reports.py:1668-1672`. ✔ returns 403. Good.
- **Trial expiry:** handled in `_effective_plan` (`auth.py:51-67`) — returns `basic` plan after expiry. ✔. The access token, however, still reflects the old plan in its `app_metadata` until refresh. So a logged-in trial user who lets their trial expire still sees Pro features until the next refresh. Given the 5-minute refresh cooldown (M-12) the window is small.

### Cross-user data access vectors

| Vector | Protected? | Evidence |
|---|---|---|
| Another user's QBO tokens | ✔ DB-side | `qbo_tokens` queries always `.eq("user_id", ...)` |
| Another user's mappings | ✔ DB-side | `mappings` queries always `.eq("user_id", ...)` |
| Another user's jobs | ✔ endpoint-side | `job["user_id"] != str(user.id)` check on every job route |
| Another user's report file | ✔ endpoint-side | download endpoint re-checks ownership before storage read |
| Supabase Storage direct access | **depends on bucket policy** — must verify `reports` bucket has **no anonymous read**. Tokens live in service-role backend only, but bucket-level policy is the actual guardrail. |
| **Cross-contamination via shared module state** | **✖ NOT protected** | C-1 (qbo_client._override_tokens) |

---

## Quick Wins (<15 min each)

1. Delete `backend/billing.py`, `backend/storage.py`, `backend/worker.py`, `frontend/index.html` (all empty).
2. Delete `GET /api/mapping/debug/{realm_id}` (H-4).
3. Add `.replace(/'/g,'&#39;')` to `frontend/app.html:386` escHtml (H-5).
4. Wrap path params in `encodeURIComponent` in `app.html` fetches (M-10).
5. Remove the redundant `sys.path.insert` at `reports.py:77` (M-9).
6. Remove `fetch_query()` and `_build_amount_row()` dead code (M-2, M-3).
7. Remove the `try/except ImportError` wrappers around router imports in `main.py`; let startup fail loudly.
8. Delete the unused signed-URL generation in `reports.py:1641-1648` (just persist the storage path) (H-7).
9. Stop double-logging realm_id/company_name to INFO (M-4).
10. Set `email_confirm: False` in signup (H-9) — *requires* adding a resend-verification UX; not a 15-min change in practice, but the config flip alone is trivial.
11. Remove duplicate `display:none;` at `app.html:49` (Low).
12. Add a process-startup hook that flips `status in ('pending','running')` jobs older than X minutes to `failed` (partial mitigation for H-6).
13. Replace naive `datetime.utcnow()` with `datetime.now(timezone.utc)` across `auth.py`, `qbo_oauth.py`, `reports.py` (consistency; Low).
14. Delete `backend/core/token_manager.py` if confirmed unused in the web path (Medium/cleanup).
15. Add an `encodeURIComponent` defense in `app.html:621, 743, 956, 1136` (M-10).
16. Add one line to `backend/reports.py:1684` to check `qbo_tokens` ownership synchronously before spawning a thread (H-3).
17. Add `addEventListener` for click on the login Enter key instead of global `keydown` (Low).

---

## Required before launch (summary)

Must fix:
- **C-1** — cross-user token leakage (architectural refactor: remove module globals in qbo_client).
- **C-2** — concurrency cap on report generation.
- **C-3** — real job cancellation (or at least stop lying to the user).
- **H-1** — login/refresh rate limiting.
- **H-2** — durable rate limit store (Redis or DB).
- **H-4** — delete debug endpoint.
- **H-5** — escape `'` in `escHtml`.

Should fix:
- **H-3, H-6, H-7, H-9, H-10** (already OK), **H-12** (password reset).

Nice-to-have before launch:
- M-1 through M-15, the Low list, Quick Wins.

---

*End of review.*
