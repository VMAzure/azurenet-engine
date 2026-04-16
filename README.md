# azurenet-engine
engie vehicle dertails
# azurenet-engine

Dedicated data synchronization engine for vehicle data.

## Responsibilities
- Sync auto nuove
- Sync auto usate
- Sync VIC nuovo
- Sync VIC usato
- Sync recensioni Google (`app/jobs/sync_google_reviews.py`): dealer con `google_place_id` e (**`dealer_public.is_active`** oppure almeno un **`dealer_site_public.is_active`**). CLI: `--all` (stesso criterio dello scheduler), `--all-with-place-id` (backfill tutti i place id). Job schedulato in `app/scheduler.py` (`schedule_reviews_jobs`, 03:30). Places API (New): **`X-Goog-FieldMask`** obbligatorio.

## Explicitly NOT included
- No API runtime
- No frontend
- No AI
- No images or videos
- No notifications
- No billing
- No SEO

## Architecture
- Shared PostgreSQL (same DB as CoreAPI)
- Process isolation
- Scheduler-based batch jobs only

This repository is part of the AZCORE ecosystem.
