import os

ENV = os.getenv("ENV", "local")

SCHEDULER_TIMEZONE = os.getenv("SCHEDULER_TIMEZONE", "Europe/Rome")

ENABLE_NUOVO_SYNC = os.getenv("ENABLE_NUOVO_SYNC", "true").lower() == "true"
ENABLE_USATO_SYNC = os.getenv("ENABLE_USATO_SYNC", "true").lower() == "true"
ENABLE_VIC_SYNC   = os.getenv("ENABLE_VIC_SYNC", "true").lower() == "true"
