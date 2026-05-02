"""
stripe_webhook.py

Receives Stripe webhook events and reflects subscription state into
Supabase `auth.users.app_metadata.plan`. No auth dependency — Stripe
authenticates itself via the signature header, verified with
STRIPE_WEBHOOK_SECRET.
"""

import logging
import os

import stripe
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Request

from backend.auth import get_supabase_admin

load_dotenv()

logger = logging.getLogger(__name__)

router = APIRouter()

stripe.api_key  = os.getenv("STRIPE_SECRET_KEY")
WEBHOOK_SECRET  = os.getenv("STRIPE_WEBHOOK_SECRET")

# Map Stripe price IDs to internal plan names. Populated from env so the
# same code ships to test + production and only the price IDs differ.
PRICE_TO_PLAN = {
    os.getenv("STRIPE_PRO_PRICE_ID", ""):  "pro",
    os.getenv("STRIPE_PLUS_PRICE_ID", ""): "plus",
}


def _update_user_plan(email: str, plan: str) -> None:
    """Update the Supabase user's plan in app_metadata (service role only)."""
    if not email:
        return

    supabase = get_supabase_admin()

    target_user = None
    try:
        users = supabase.auth.admin.list_users()
        for u in users or []:
            if getattr(u, "email", None) == email:
                target_user = u
                break
    except Exception as e:
        logger.warning(f"Stripe webhook: list_users failed while resolving {email}: {e}")
        return

    if not target_user:
        logger.warning(f"Stripe webhook: no user found for {email}")
        return

    try:
        existing = dict(getattr(target_user, "app_metadata", None) or {})
        existing["plan"] = plan
        if plan in ("pro", "plus"):
            existing.pop("trial_expires", None)
        supabase.auth.admin.update_user_by_id(
            str(target_user.id),
            {"app_metadata": existing},
        )
        logger.info(f"Stripe webhook: updated {email} to plan={plan}")
    except Exception as e:
        logger.error(f"Stripe webhook: update_user_by_id failed for {email}: {e}")


def _customer_email(customer_id: str) -> str:
    """Look up a Stripe customer's email by id. Returns '' on failure."""
    if not customer_id:
        return ""
    try:
        customer = stripe.Customer.retrieve(customer_id)
        if isinstance(customer, dict):
            return customer.get("email", "") or ""
        return customer.email or ""
    except Exception as e:
        logger.warning(f"Stripe webhook: customer lookup failed for {customer_id}: {e}")
        return ""


@router.post("/api/stripe/webhook")
async def stripe_webhook(request: Request):
    payload    = await request.body()
    sig_header = request.headers.get("stripe-signature")

    if not WEBHOOK_SECRET:
        logger.error("STRIPE_WEBHOOK_SECRET not set")
        raise HTTPException(status_code=500, detail="Stripe webhook secret not configured")

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, WEBHOOK_SECRET)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid payload")
    except Exception as e:
        logger.error(f"Stripe signature verification failed: {e}")
        raise HTTPException(status_code=400, detail="Invalid signature")

    try:
        event_type = event["type"] if isinstance(event, dict) else getattr(event, "type", "")
        data_object = event["data"]["object"] if isinstance(event, dict) else event.data.object

        if event_type == "checkout.session.completed":
            customer_email = ""
            try:
                if isinstance(data_object, dict):
                    customer_email = (data_object.get("customer_details") or {}).get("email", "")
                else:
                    customer_email = data_object.customer_details.email or ""
            except Exception:
                pass

            mode = data_object["mode"] if isinstance(data_object, dict) else getattr(data_object, "mode", "")
            session_id = data_object["id"] if isinstance(data_object, dict) else data_object.id

            if mode == "subscription":
                try:
                    line_items = stripe.checkout.Session.list_line_items(session_id)
                    items_data = line_items["data"] if isinstance(line_items, dict) else line_items.data
                except Exception as e:
                    logger.warning(f"Stripe webhook: list_line_items failed: {e}")
                    items_data = []

                for item in items_data:
                    try:
                        if isinstance(item, dict):
                            price_id = (item.get("price") or {}).get("id", "")
                        else:
                            price_id = item.price.id
                    except Exception:
                        price_id = ""
                    plan = PRICE_TO_PLAN.get(price_id)
                    if plan and customer_email:
                        _update_user_plan(customer_email, plan)

        elif event_type == "customer.subscription.deleted":
            customer_id = data_object["customer"] if isinstance(data_object, dict) else getattr(data_object, "customer", "")
            email = _customer_email(customer_id)
            if email:
                _update_user_plan(email, "basic")

        elif event_type == "invoice.payment_failed":
            customer_id = data_object["customer"] if isinstance(data_object, dict) else getattr(data_object, "customer", "")
            email = _customer_email(customer_id)
            if email:
                _update_user_plan(email, "basic")

    except Exception as e:
        logger.error(f"Stripe webhook handler error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    return {"status": "ok"}
