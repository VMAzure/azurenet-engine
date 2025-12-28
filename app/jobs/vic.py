import logging
from app.database import DBSession

def sync_vic():
    logging.info("[VIC] sync started")
    with DBSession() as db:
        # TODO STEP SUCCESSIVO
        pass
    logging.info("[VIC] sync completed")
