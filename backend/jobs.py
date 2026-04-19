import os
from datetime import datetime, timezone
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL         = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")


def get_supabase():
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def create_job(user_id: str, realm_id: str, start_date: str, end_date: str, dimension: str = "none") -> dict:
    """Create a new pending job and return it."""
    supabase = get_supabase()
    result = supabase.table("jobs").insert({
        "user_id":    user_id,
        "realm_id":   realm_id,
        "start_date": start_date,
        "end_date":   end_date,
        "dimension":  dimension,
        "status":     "pending",
    }).execute()
    return result.data[0]


def update_job(job_id: str, **kwargs):
    """Update a job's fields."""
    supabase = get_supabase()
    supabase.table("jobs").update({
        **kwargs,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }).eq("id", job_id).execute()


def get_job(job_id: str) -> dict | None:
    """Get a job by ID."""
    supabase = get_supabase()
    result = supabase.table("jobs").select("*").eq("id", job_id).execute()
    return result.data[0] if result.data else None


def get_user_jobs(user_id: str) -> list:
    """Get all jobs for a user, most recent first."""
    supabase = get_supabase()
    result = supabase.table("jobs").select("*").eq(
        "user_id", user_id
    ).order("created_at", desc=True).limit(20).execute()
    return result.data
