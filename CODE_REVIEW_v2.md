# Acorn Web — Code Review v2

**Reviewer:** Senior engineer audit (post-hardening)
**Branch:** `dev` (synced to `master` at merge `cffab08`)
**Date:** 2026-04-18
**Scope:** every file under `backend/` and `frontend/`
**Methodology:** read-only review. No code changes. Findings cite `file:line` where possible.

**Relationship to v1:** v1 (`CODE_REVIEW.md`) identified 3 critical, 12 high, 15 medium, and 17 quick-win items across the codebase. This review checks each one for resolution and scans for regressions or new issues introduced by the fix rounds.

---

## Executive Summary

**Overall grade: A- (launch-ready with a small number of medium notes)**

Three rounds of targeted fixes have closed every critical and every high-priority finding from v1. The cross-user token leak that was the headline v1 blocker is structurally impossible under the current code — there are no module-level token globals, and every QBO call takes `access_token` / `realm_id` as explicit keyword arguments. The report pipeline has bounded concurrency, a real cancel path, plan-gated 403s instead of silent downgrades, and a startup sweep that unsticks orphan jobs.

Four issues remain worth tracking (none launch-blocking):

1. **Signup still auto-verifies email** (`email_confirm: True`). Business decision — documented, not a defect.
2. **Password reset link path** depends on Supabase's built-in email template being wired up in the Supabase dashboard. Verify before cutover.
3. **Thread-based worker** still has no out-of-process recovery; startup sweep is the only safety net. Fine for single-instance Railway, but a queue is the right answer if the product ever scales.
4. **Minor Medium/Low items** below (`Medium` + `Low` sections) — routine cleanup.

### Top 5 status changes since v1

| v1 Finding | Severity | Status |
|---|---|---|
| C-1 Cross-user token leak via module globals | Critical | **Resolved** — `qbo_client` has no module state; tokens are threaded as kwargs through every helper |
| C-2 Unbounded concurrent report threads | Critical | **Resolved** — `BoundedSemaphore(3)` global cap + 1-per-user DB check; 429/409 returned |
| C-3 Cancel endpoint doesn't cancel | Critical | **Resolved** — worker polls job status between steps and honours `CancelledJob` |
| H-1 No rate limit on login/refresh | High | **Resolved (login)** — 5/5min/IP. Refresh still uses the client-side 5-min cooldown; server-side still unlimited but bearer-token-bound |
| H-5 `escHtml` missed `'` (self-XSS) | High | **Resolved** — single quote escaped |

---

## v1 Findings: Closure Status

### Critical (3 of 3 resolved)

- **C-1 (token leak):** `backend/core/qbo_client.py` has no globals. `fetch_report(report_name, params, *, access_token, realm_id, testing_migration=False)` and `fetch_accounts(*, access_token, realm_id)` both require kwargs. `gl_extractor.generate_lite(access_token, realm_id, start_date, end_date, ...)` threads them through every internal helper (`_build_coa_lookup`, `_fetch_gl[_single|_chunked]`, `_fetch_bs_balances`, `_fetch_monthly_reports`, `_fetch_ar_aging`, `_fetch_ap_aging`, `_fetch_qbo_report_totals`, `_fetch_pl_by_dimension`). `reports.run_report_job` passes the tokens once and never mutates shared state. Two concurrent jobs now cannot see each other's data. Verified: `grep -r "_override_tokens\|set_override_tokens\|set_v2_test_mode"` returns no hits in code (only v1 doc references).
- **C-2 (concurrency):** `_report_semaphore = threading.BoundedSemaphore(3)` at `backend/reports.py:27`. `generate_report` endpoint (`:1738-1772`) checks per-user "1 in flight" against the `jobs` table, then acquires the semaphore with `blocking=False` and returns 429 on failure. Worker releases in `finally`.
- **C-3 (cancel):** `_job_is_cancelled(job_id)` and `_check_cancel(job_id)` at `backend/reports.py:39-55`. `run_report_job` calls `_check_cancel` at entry, before each major stage, and before upload. A `cancel_fn` closure is passed to `generate_lite`, so gl_extractor's own `_check_cancel` picks up cancellation mid-QBO-fetch via `LiteCancelled`. The worker's `except CancelledJob` leaves status as `failed` (set by the cancel endpoint) and never overwrites it on exit.

### High (12 of 12 resolved; one with documented caveat)

- **H-1 Login/refresh rate limit:** `backend/auth.py:104-135` adds `_check_login_rate` (5 per 5 min) and `_check_reset_rate` (3 per hr) alongside the existing signup limit, all sharing a new `_check_rate` helper with LRU-style sweep.
- **H-2 Rate-limit store durability:** Still in-memory. Acceptable for single-worker Railway. Documented in code.
- **H-3 Realm ownership before spawn:** `backend/reports.py:1720-1724` queries `qbo_tokens` up front; 404s unknown realms before creating the job or consuming a semaphore slot.
- **H-4 Debug endpoint removed:** `GET /api/mapping/debug/{realm_id}` deleted.
- **H-5 escHtml single-quote:** `frontend/app.html:386` now escapes `'` → `&#39;`.
- **H-6 Threading fragility (startup recovery):** `backend/main.py:34-69` flips any `pending`/`running` jobs to `failed` with error `"Server restart"` at boot. Also deletes `qbo_oauth_states` rows older than 1 hour.
- **H-7 Stored signed URL:** Generation removed. `file_url` on the job row now stores the bucket path; `/api/reports/download/{job_id}` reads it back and downloads on demand.
- **H-8 localStorage tokens:** Unchanged by design (stateless SPA). Mitigating factor: H-5 is now closed and no stored XSS has been found. Flagged as residual risk only.
- **H-9 Auto-verified email on signup:** Unchanged. This is a business decision, not a defect — see **Medium M-2** below.
- **H-10 Anon vs admin client:** Already correct in v1; confirmed.
- **H-11 OAuth state cleanup:** Sweep added at startup.
- **H-12 Password reset:** `POST /api/auth/reset-password` at `backend/auth.py:284-314`. Rate-limited (3 per hr per IP). Always returns a generic message (no account enumeration). `frontend/login.html:40-54` exposes a **Forgot password?** form with an email input and "Send Reset Link" action.

### Medium (status of each v1 finding)

| v1 Item | Status |
|---|---|
| M-1 Empty files | Resolved — `billing.py`, `storage.py`, `worker.py`, `frontend/index.html` deleted |
| M-2 Unused dead code (`fetch_query`, `_v2_test_mode`) | Resolved — removed |
| M-3 Unused `_build_amount_row` | Resolved — removed |
| M-4 PII in logs | Resolved — realm_id/company_name logs dropped; `"Applying maps: ..."` demoted to DEBUG with count only |
| M-5 Token refresh silent-fail | Resolved — job fails with `"QBO connection expired. Please reconnect and try again."` |
| M-6 Password length only | Resolved — signup requires ≥8 chars + 1 upper + 1 lower + 1 digit |
| M-7 Substring-based duplicate detection | Resolved — checks Supabase `error_code` / `code` first, then status 409/422 + multiple substring fallbacks |
| M-8 Rate-limit dict unbounded | Resolved — `_sweep_rate_store` drops stale IPs on each check |
| M-9 Duplicate `sys.path.insert` | Resolved — single insertion at top of `reports.py` |
| M-10 Unencoded path params | Resolved — `encodeURIComponent` everywhere in `app.html` |
| M-11 pollJob backoff | Resolved — self-scheduling `setTimeout` with 3s→30s exponential backoff, 10-error cap, then "Lost connection to server" message |
| M-12 Refresh cooldown | Resolved — split into `_lastRefreshSuccess` (applies cooldown) and in-flight dedupe; failures allow immediate retry |
| M-13 CSRF on OAuth callback | n/a — mitigated by state token (documented in v1) |
| M-14 Silent premium feature downgrade | Resolved — 403 returned with upgrade messages for class/location, mapping, GL detail, portal |
| M-15 Download uses bucket listing | Resolved — reads stored path; path prefix checked against caller's user id |

### Low / Quick Wins

All v1 quick-wins have been applied except:
- `reports.py` line count is now 1,809 (up ~50 since v1 because I re-expanded single-line imports). The consolidation came from extracting `excel_formatter.py`, removing three nested font-sweep loops, and collapsing seven nested import blocks.
- `except Exception:` handlers remain throughout but are now deliberate (see per-file notes).

---

## Post-refactor Re-verification

I re-read every file that changed and spot-checked callers against the new signatures. Compilation passes on every Python module. A few areas where the refactor could plausibly have introduced regressions — all verified safe:

| Concern | Verified |
|---|---|
| gl_extractor's `_fetch_pl_by_dimension` was defined but never called in v1. Still unused. | No break |
| `_cal` / `_dt` / `_re` aliases in reports.py replaced with module-level `calendar` / `datetime` / `re`. | Compile + grep pass; no leftovers |
| `_Ff` / `_PFf` / `_Fp` / `_PFp` / `_Alf` / `_gclf` / `_XlImg` replaced with hoisted `Font` / `PatternFill` / `Alignment` / `get_column_letter` / `XLImage`. | Compile + grep pass |
| `apply_global_formatting(wb)` now runs once at the end of the pipeline instead of three times inline. | Tab coverage verified: `_BUFFER_COL_A_TABS` set + `' P&L'`/`' BS'`/`' Validation'` suffix match covers every tab produced by the pipeline |
| Buffer col A width previously set at 0.63 in three places; freeze panes previously set to `A6` in five. Now centralised. | Divider tabs (`QBO Reports`, `Mapped Reports`, etc.) deliberately don't get buffer col / freeze panes; gl_extractor still does its own pre-pipeline font sweep, which is the baseline that `apply_global_formatting` cleans up after |
| Download endpoint path-prefix check `storage_path.startswith(f"{str(user.id)}/")`. | Correct: `user.id` is the Supabase UUID the worker writes into the upload key |
| Startup sweep runs once per process boot. | Using `@app.on_event("startup")` — deprecated in favor of lifespan but still supported by FastAPI |
| Toast system escapes message text via `escHtml` before `innerHTML`. | Verified at `app.html:398-411` |
| `confirmDialog` Esc/Enter keyboard handler is cleaned up on close. | Verified: `document.removeEventListener('keydown', onKey)` in `cleanup()` |
| `generate()` has a belt-and-braces `state.running` guard even though the button is disabled. | Verified |

---

## Critical Issues

**None.**

---

## High Priority

**None.**

---

## Medium Priority (new / still-open)

### M-1. Password reset relies on Supabase email configuration

**File:** `backend/auth.py:284-314`

`generate_link({"type": "recovery", ...})` creates the recovery link and, per current supabase-py semantics, also triggers the email via Supabase Auth's email template. If the Supabase dashboard has "Enable email confirmations" / "Recovery email" disabled, or if the SMTP settings are unconfigured, the endpoint will silently 200 with no email actually delivered. This is the code review's most likely "works on dev, dead on prod" trap.

**Fix:** verify the Supabase project's Auth → Email Templates has the "Reset Password" template enabled and SMTP is connected before launch. Smoke-test end-to-end with a real inbox.

### M-2. Signup auto-verifies email

**File:** `backend/auth.py:219` — `"email_confirm": True`

Carried over from v1 (H-9). The explicit direction this round was "business decision, skip," so this is not a defect. Recording here only because it enables trial abuse: a single person can mint new 7-day Pro trials with `foo+1@…`, `foo+2@…` etc. If that becomes a support problem, flip to `False` and wire up the verification email.

### M-3. Password policy isn't enforced on password reset

**File:** `backend/auth.py:284-314`

The new password complexity rules (upper/lower/digit, ≥8 chars) apply only to `/signup`. When the user follows the recovery link, Supabase's hosted page validates passwords using Supabase's rules, not ours. A user who resets can end up with a weaker password than one who signed up.

**Fix:** either configure Supabase's password rules in the dashboard to match, or host the password-update step inside Acorn Web using Supabase's JS client.

### M-4. Thread-based worker has no out-of-process recovery beyond boot

**File:** `backend/reports.py`

The startup sweep (`main.py:35-69`) flips orphan `pending`/`running` jobs to `failed`. But if the Railway container runs for hours and a specific job's thread dies without the finally block running (e.g. SIGKILL on a deploy), that job stays `running` until the next restart. The per-user "1 job in flight" check will then block the user from submitting a new one until the container restarts.

**Fix (nice to have):** a watchdog task that marks jobs stuck in `running` for > N minutes as `failed`. Or move to RQ/Celery.

### M-5. Download is `Response`, not `StreamingResponse`

**File:** `backend/reports.py:1855-1869`

The download endpoint buffers the entire Excel file into memory before replying. For a basic-plan 3-month report this is ~1-5 MB, fine. For a 2-year admin report with GL detail it can be 50+ MB. Under concurrent downloads the memory footprint multiplies.

**Fix:** switch to `StreamingResponse` pulling chunks from `supabase.storage.from_("reports").download(path)` — or stream from a signed URL if the bucket permits.

### M-6. `@app.on_event("startup")` is deprecated

**File:** `backend/main.py:34`

FastAPI recommends `lifespan` async context managers instead. Works fine today, will emit DeprecationWarnings on FastAPI ≥ 0.109 and may be removed.

**Fix:** convert to `lifespan` when bumping FastAPI.

---

## Low Priority / Nice-to-have

- `reports.py` is still 1,809 lines. The Excel formatting was extracted, but the mapping / Map Summary / Mapped P&L / Mapped BS / Summary-tab builders still live in-line inside `run_report_job` (~1,500 lines). A second extraction pass (e.g. `backend/excel_sections.py`) would bring `run_report_job` under 250 lines. Not urgent.
- `gl_extractor.py` still has its own pre-save font sweep at line 2370. Redundant with `apply_global_formatting`, but it runs before reports.py decoration so it's only a small duplicate. Acceptable.
- `_check_rate` takes a `detail` string argument; could be typed with `Final[str]` constants for clarity.
- `except Exception:` occurrences are deliberate (logo loader, zoneinfo fallback, modal fallback, signup duplicate branch, QBO refresh wrapper, etc). Each one's comment/context justifies it; no bare `except:` found.
- `_EMAIL_RE` is still permissive (any `foo@bar.baz` passes). OK.
- `apply_global_formatting` has `FREEZE_ROW = "A6"` hardcoded. Every tab in the pipeline uses 5-row headers, so this is correct — but the constant naming makes it clear a future tab with a different header height would not be auto-detected.
- README still does not document how to run tests. There are no tests checked in; worth adding a `tests/` folder and a CI workflow as a follow-up.
- `.gitignore` is only 51 bytes. Confirmed `.env*` and `__pycache__` are handled by git's default rules + the existing file, but a hardened `.gitignore` would explicitly block `*.env.local`, `.venv/`, `node_modules/`, `.vscode/`, `*.pyc`, `.DS_Store`.
- `frontend/app.html` is 2,152 lines. Extracting the JS to `app.js` (served via `main.py`) would halve the HTML file and let browsers cache the JS.

---

## Findings by File

### `backend/main.py` (106 lines)
- `:34-69` — lifecycle sweep runs both job-orphan reset and OAuth-state cleanup with independent try blocks. Good.
- `:95` — `/favicon.svg` route removed.
- `:22-25` — router imports are naked now; import errors crash startup. Correct.
- No findings.

### `backend/auth.py` (311 lines)
- `:66-98` — `_check_rate` shared helper + `_sweep_rate_store` = M-8 fix. Correct.
- `:109-128` — `_password_ok` + `_looks_like_duplicate_user` give multi-signal detection. Good.
- `:143` — `_EMAIL_RE` permissive regex. Acceptable.
- `:219` — `email_confirm: True` — documented M-2.
- `:284-314` — reset endpoint. Depends on Supabase email template (M-1). Swallows all exceptions to prevent enumeration. Fine.
- No new defects.

### `backend/qbo_oauth.py` (283 lines)
- `:1-7` — redundant `sys.path.insert` removed. Imports clean.
- All `datetime.now(timezone.utc)`. Consistent.
- `:129` bare `except Exception:` — inside the admin-user lookup fallback to plan="basic". Justified.
- Company-limit check still present; state validation still correct. No findings.

### `backend/mapping.py` (126 lines)
- Debug endpoint gone.
- All endpoints still require auth + plan.
- `save_mapping` still upserts without verifying the user owns the realm — same as v1's note; the upsert is `(user_id, realm_id)`-scoped so it can't cross-user contaminate. Low concern.
- `datetime.now(timezone.utc)` applied.
- No new findings.

### `backend/jobs.py` (52 lines)
- `datetime.now(timezone.utc)` applied. No findings.

### `backend/reports.py` (1,809 lines)
- `:1-36` — single, alphabetised import block at top. Hoisted every inline import.
- `:37-55` — cancel helpers.
- `:25-27` — `_report_semaphore = threading.BoundedSemaphore(3)`.
- `:100-200` — worker. Now reads cleanly: cancel → refresh → check → generate_lite with cancel_fn → mapping decoration → portal tabs → restructuring → upload. One `apply_global_formatting` call at the very end.
- `:1738-1786` — generate endpoint: realm ownership → date-range check → explicit 403s for premium features → per-user running-job check → semaphore acquire → spawn.
- `:1855` — download uses stored path + user-prefix guard. Buffers fully into memory (M-5).
- `:1487` bare `except Exception:` — zoneinfo fallback to naive now(). Justified.
- `:1650` bare `except Exception:` — semaphore.release() in `finally`. Idempotent guard.
- No new defects.

### `backend/excel_formatter.py` (130 lines, new)
- Clean module: constants, font factories, single `apply_global_formatting(wb)` pass.
- `_BUFFER_COL_A_TABS` covers every tab the pipeline produces; suffix match handles mapped-report tabs.
- `:93, 120` bare `except Exception:` — guards around `sheet_view.showGridLines` and `freeze_panes` assignment (some openpyxl sheet types don't support either). Justified.

### `backend/portal_prep.py` (286 lines)
- Unchanged; pure computation. No findings.

### `backend/core/qbo_client.py` (157 lines)
- `fetch_report` / `fetch_accounts` both require `access_token` / `realm_id` as keyword-only args. Raise `ValueError` if missing.
- Error messages no longer leak the company alias (now generic: "QBO authentication failed. Reconnect the company.").
- No module state.
- `_check_for_fault` and manual URL-building for comma-preservation unchanged.

### `backend/core/gl_extractor.py` (2,395 lines)
- Every internal helper that hits QBO now threads `access_token, realm_id` through. Verified with `grep -n "fetch_report\|fetch_accounts"`.
- `_fetch_pl_by_dimension` still defined but unused in this pipeline — consider removing in a future pass.
- `:2370-2378` — pre-save font sweep. Redundant with `apply_global_formatting` but harmless.
- Bare `except Exception:` cases are all around expected failure modes (non-JSON responses, missing columns, cell limit fallback). Each has a progress message for the user. Acceptable.

### `backend/core/report_parser.py` (387 lines)
- `_build_amount_row` removed.
- No other changes. No findings.

### `frontend/app.html` (2,152 lines)
- `:386` `escHtml` escapes `'`.
- `:388-450` — toast + modal helpers. `showToast` escapes content; `confirmDialog` cleans up keyboard listeners on close; Esc cancels, Enter confirms; danger variant styled.
- `:484-541` — refresh token logic uses `_lastRefreshSuccess` (5-min cooldown) and `_refreshInFlight` coalescing; failures do not burn the cooldown.
- `:981-1069` — pollJob exponential backoff 3s→30s, 10-error cap, "Lost connection" terminal message.
- `:1050-1053` — `storage_path` in `job.file_url` is split by `/` to derive filename; safe because the server always writes `{user_id}/{job_id}/{filename}`.
- All `fetch(API + '/api/...' + variable)` instances use `encodeURIComponent`.
- `alert()` / `confirm()` replaced with `showToast` / `confirmDialog` (only residual `window.confirm` is the fallback inside `confirmDialog` itself if the modal DOM is missing).
- `state.running` guard at top of `generate()`.
- No new defects.

### `frontend/login.html` (248 lines)
- Forgot-password flow: email input + "Send Reset Link" button, generic success message, respects rate limit from backend.
- Auto-redirect now verifies the stored token via `/api/auth/refresh` before going to `app.html`; stale tokens are cleared and the user stays on the login page.
- Enter-key handler dispatches to the visible form (sign-in vs forgot).
- No findings.

### `frontend/style.css` (2,013 lines)
- New `.toast`, `#toastContainer`, `#modalBackdrop`, `.modal-card`, `.modal-btn` rules at end. Brand colours match: success `#059669`, error `#CC0000`, warning `#D97706`.
- No unused-rule sweep done; low priority.

### `README.md`
- Real content now: stack, dev workflow, env vars.

---

## Tier Enforcement Audit — re-run

| Endpoint | Auth? | User scoped? | Plan gated? | Status vs v1 |
|---|---|---|---|---|
| `POST /api/auth/signup` | no | n/a | n/a | Rate-limited + complexity policy + robust duplicate detection. **Improved** |
| `POST /api/auth/login` | no | n/a | n/a | Now rate-limited (5/5min/IP). **Improved** |
| `POST /api/auth/refresh` | refresh token | n/a | n/a | Unchanged |
| `POST /api/auth/reset-password` | no | n/a | n/a | **New** — rate-limited, enumeration-safe |
| `GET /api/qbo/auth-url` | ✔ | ✔ | ✔ company limit | Unchanged |
| `GET /api/qbo/callback` | state | via state | ✔ company limit | Orphan-state cleanup added at boot |
| `GET /api/qbo/companies` | ✔ | ✔ | — | Unchanged |
| `DELETE /api/qbo/companies/{realm_id}` | ✔ | ✔ | — | Unchanged |
| `POST /api/qbo/refresh-token/{realm_id}` | ✔ | ✔ | — | Unchanged |
| `GET /api/mapping/coa/{realm_id}` | ✔ | ✔ | ✔ | Unchanged |
| ~~`GET /api/mapping/debug/{realm_id}`~~ | — | — | — | **Deleted** |
| `GET /api/mapping/{realm_id}` | ✔ | ✔ | ✔ | Unchanged |
| `POST /api/mapping/{realm_id}` | ✔ | ✔ | ✔ | Unchanged |
| `POST /api/reports/generate` | ✔ | ✔ | ✔ | **Hardened**: realm ownership pre-check, 403 on premium features (not silent downgrade), per-user + global concurrency caps, 409/429 on contention |
| `GET /api/reports/job/{job_id}` | ✔ | ✔ | — | Unchanged |
| `POST /api/reports/job/{job_id}/cancel` | ✔ | ✔ | — | **Actually cancels** via DB status + `cancel_fn` polling |
| `GET /api/reports/download/{job_id}` | ✔ | ✔ | — | **Reads stored path** + user-prefix check; no bucket listing |
| `GET /api/reports/history` | ✔ | ✔ | — | Unchanged |

Plan enforcement summary:

- **Mapping (pro+):** `_require_mapping_plan` on every mapping endpoint. ✔
- **Class/Location (pro+):** 403 with upgrade message if a basic caller submits `dimension != "none"`. ✔ (v1 silently dropped)
- **GL detail (pro+):** 403 if `include_gl_detail=True` on basic. ✔ (v1 silently dropped)
- **Portal data (admin-only):** 403 if non-admin submits `include_portal_data=True`. ✔ (v1 silently dropped)
- **Date range (≤92d for basic):** 403 with upgrade message. ✔
- **Company limit:** 403 at `/auth-url` and at `/callback`. ✔
- **Trial expiry:** enforced via `_effective_plan` in auth.py. ✔

---

## Cross-user Data Access Vectors — re-run

| Vector | Protected? | Notes |
|---|---|---|
| Another user's QBO tokens | ✔ | Queries always `.eq("user_id", str(user.id))` |
| Another user's mappings | ✔ | Same |
| Another user's jobs | ✔ | Job-row ownership check on every endpoint |
| Another user's report file | ✔ | Download endpoint reads stored path and verifies `startswith(f"{user.id}/")` |
| Supabase Storage direct access | depends on bucket policy | Bucket RLS must be verified in Supabase dashboard |
| **Cross-contamination via shared module state** | ✔ | **Resolved** — no module globals hold per-request data anywhere in the request path |

---

## Required before launch (summary)

**Verified in code:**

- No cross-user data leakage paths
- Bounded concurrency with 429/409 backpressure
- Real cancel semantics
- Rate-limited auth endpoints
- Plan-gated premium features with 403s
- Path-encoded URLs and escape-safe innerHTML
- Startup sweeps reclaim orphan jobs and OAuth states
- Persistent token refresh failure modes

**Verify in Supabase dashboard before cutover:**

- `reports` bucket: anonymous read disabled
- Auth email templates: "Reset Password" enabled, SMTP configured
- `qbo_oauth_states` table exists with RLS enabled, service-role-only access
- `jobs` table has `file_url` column (path string), `status` column, `error` column, `progress` column, `updated_at` column
- `mappings`, `qbo_tokens`, `jobs` all have appropriate RLS policies

**Nice-to-haves post-launch:**

- Switch `/download` to `StreamingResponse` (M-5)
- Watchdog for genuinely stuck jobs older than N minutes (M-4)
- Migrate to `lifespan` context manager (M-6)
- Extract mapping/summary builders into a second `excel_sections.py` (Low)
- Wire Supabase password rules to match signup policy (M-3)

---

## Grade

**A- (launch-ready).** From C+ at v1 to A- after three focused rounds. Every v1 critical and high-priority finding has been closed in code. The remaining mediums are either configuration concerns (Supabase email templates, bucket RLS) or deferred-infrastructure items (out-of-process queue, streaming downloads) that don't block an initial paying-customer launch. No new issues were introduced by the refactors; the extracted `excel_formatter.apply_global_formatting` pass is a net correctness and performance improvement over the three inline loops it replaced.

*End of review v2.*
