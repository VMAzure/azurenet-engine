"""dmax_suspension_worker.py — Conferma differita della sospensione DealerMax.

Gira giornalmente. Legge da `utenti` i dealer con `dmax_suspension_pending_at`
scaduto (programmato dal webhook Stripe quando la subscription e' entrata in
stato `unpaid` / `canceled` / `incomplete_expired`), interroga Stripe per lo
stato attuale e decide:

- Sub oggi `active` / `trialing`     → azzera pending (pagamento arrivato,
                                       webhook di recovery mancato o in ritardo).
- Sub oggi `unpaid` / `canceled`
  / `incomplete_expired`             → conferma sospensione (flag + revoca
                                       API key pubblica + timestamp).
- Sub non esiste piu' su Stripe      → conferma sospensione (come sopra).
- Sub oggi `past_due` / `incomplete` → Stripe sta ancora ritentando, posticipa
                                       pending di altri GRACE_DAYS giorni.
- Errore Stripe (network / 5xx)      → lascia pending invariato, prossima run.

Motivo: SEPA Direct Debit puo' confermare il pagamento anche dopo 14 giorni
dall'addebito, ma Stripe marca la subscription `unpaid` molto prima (quando
esaurisce i retry). Sospendere all'istante del webhook generava downtime
ingiustificato per dealer che avevano gia' pagato. Ora il webhook solo PROGRAMMA
la sospensione; questo worker, dopo la grace window, verifica lo stato reale
su Stripe e sospende solo se necessario.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

import psycopg
import stripe

logger = logging.getLogger(__name__)

# Se past_due al momento del check, posticipo di questi giorni prima di
# riprovare (evito di sospendere mentre Stripe sta ancora tentando).
PAST_DUE_POSTPONE_DAYS = 7

# Batch limit per run (safety net: nella pratica < 10 in pipeline).
BATCH_SIZE = 500


def _get_db_url() -> str:
    return os.environ["DATABASE_URL"]


def _configure_stripe() -> str | None:
    key = os.getenv("STRIPE_SECRET_KEY")
    if key:
        stripe.api_key = key
    return stripe.api_key


def _fetch_due_dealers(conn: psycopg.Connection, limit: int) -> list[tuple[int, str | None]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, stripe_dmax_subscription_id
              FROM public.utenti
             WHERE dmax_suspension_pending_at IS NOT NULL
               AND dmax_suspension_pending_at <= NOW()
               AND COALESCE(dmax_billing_suspended, FALSE) = FALSE
               AND role = 'dealer'
             ORDER BY dmax_suspension_pending_at ASC
             LIMIT %s
            """,
            (limit,),
        )
        return [(row[0], row[1]) for row in cur.fetchall()]


def _confirm_suspension(conn: psycopg.Connection, user_id: int, new_status: str | None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.utenti
               SET dmax_billing_suspended = TRUE,
                   dmax_billing_suspended_at = NOW(),
                   dmax_suspension_pending_at = NULL,
                   dmax_subscription_status = COALESCE(%s, dmax_subscription_status)
             WHERE id = %s
            """,
            (new_status, user_id),
        )
        cur.execute(
            """
            UPDATE public.dealer_public_api_keys
               SET is_active = FALSE
             WHERE dealer_id = %s
               AND is_active = TRUE
            """,
            (user_id,),
        )


def _clear_pending(conn: psycopg.Connection, user_id: int, new_status: str | None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.utenti
               SET dmax_suspension_pending_at = NULL,
                   dmax_subscription_status = COALESCE(%s, dmax_subscription_status)
             WHERE id = %s
            """,
            (new_status, user_id),
        )


def _postpone_pending(conn: psycopg.Connection, user_id: int, days: int, new_status: str | None) -> None:
    new_due = datetime.now(timezone.utc) + timedelta(days=days)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.utenti
               SET dmax_suspension_pending_at = %s,
                   dmax_subscription_status = COALESCE(%s, dmax_subscription_status)
             WHERE id = %s
            """,
            (new_due, new_status, user_id),
        )


def dmax_suspension_worker() -> None:
    """Entry point APScheduler: processa la coda di sospensioni differite DMAX."""
    if not _configure_stripe():
        logger.warning("[DMAX SUSP] STRIPE_SECRET_KEY non configurata, skip run")
        return

    processed = 0
    confirmed = 0
    cleared = 0
    postponed = 0
    errors = 0

    with psycopg.connect(_get_db_url(), autocommit=False) as conn:
        due = _fetch_due_dealers(conn, BATCH_SIZE)
        if not due:
            logger.info("[DMAX SUSP] nessun dealer pending: idle")
            return

        for user_id, sub_id in due:
            processed += 1
            try:
                if not sub_id:
                    # Pending senza subscription id: Stripe non puo' piu' confermare.
                    # Conserviamo la sospensione (il dealer ha avuto la grace window).
                    _confirm_suspension(conn, user_id, None)
                    confirmed += 1
                    conn.commit()
                    logger.warning(
                        "[DMAX SUSP] user_id=%s sub_id missing → suspended", user_id
                    )
                    continue

                try:
                    sub = stripe.Subscription.retrieve(sub_id)
                    current_status = getattr(sub, "status", None)
                except stripe.error.InvalidRequestError:
                    # Subscription rimossa su Stripe → sospendi.
                    _confirm_suspension(conn, user_id, "canceled")
                    confirmed += 1
                    conn.commit()
                    logger.warning(
                        "[DMAX SUSP] user_id=%s sub_id=%s not_found on Stripe → suspended",
                        user_id,
                        sub_id,
                    )
                    continue
                except stripe.error.StripeError:
                    errors += 1
                    conn.rollback()
                    logger.exception(
                        "[DMAX SUSP] Stripe error for user_id=%s sub_id=%s, skip",
                        user_id,
                        sub_id,
                    )
                    continue

                if current_status in ("active", "trialing"):
                    _clear_pending(conn, user_id, current_status)
                    cleared += 1
                    conn.commit()
                    logger.info(
                        "[DMAX SUSP] user_id=%s sub=%s now %s → cleared pending",
                        user_id,
                        sub_id,
                        current_status,
                    )
                elif current_status in ("past_due", "incomplete"):
                    _postpone_pending(conn, user_id, PAST_DUE_POSTPONE_DAYS, current_status)
                    postponed += 1
                    conn.commit()
                    logger.info(
                        "[DMAX SUSP] user_id=%s sub=%s still %s → postponed %dd",
                        user_id,
                        sub_id,
                        current_status,
                        PAST_DUE_POSTPONE_DAYS,
                    )
                elif current_status in ("unpaid", "canceled", "incomplete_expired"):
                    _confirm_suspension(conn, user_id, current_status)
                    confirmed += 1
                    conn.commit()
                    logger.warning(
                        "[DMAX SUSP] user_id=%s sub=%s status=%s → suspended",
                        user_id,
                        sub_id,
                        current_status,
                    )
                else:
                    # Stati inattesi: log e posticipa per sicurezza.
                    _postpone_pending(conn, user_id, PAST_DUE_POSTPONE_DAYS, current_status)
                    postponed += 1
                    conn.commit()
                    logger.warning(
                        "[DMAX SUSP] user_id=%s sub=%s unexpected status=%s → postponed",
                        user_id,
                        sub_id,
                        current_status,
                    )
            except Exception:
                errors += 1
                try:
                    conn.rollback()
                except Exception:
                    pass
                logger.exception("[DMAX SUSP] user_id=%s unhandled error", user_id)

    logger.info(
        "[DMAX SUSP] run complete: processed=%d confirmed=%d cleared=%d postponed=%d errors=%d",
        processed,
        confirmed,
        cleared,
        postponed,
        errors,
    )
