# azurenet-engine
engie vehicle dertails
# azurenet-engine

Dedicated data synchronization engine for vehicle data.

## Responsibilities
- Sync auto nuove
- Sync auto usate
- Sync VIC nuovo
- Sync VIC usato
- Sync recensioni Google (`app/jobs/sync_google_reviews.py`): solo dealer con `dealer_public.google_place_id` valorizzato da DealerMax/admin; **nessuna** risoluzione automatica del place id. Job schedulato giornaliero in `app/scheduler.py`.

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
