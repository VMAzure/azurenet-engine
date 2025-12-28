import logging
from app.database import DBSession

def sync_nuovo():
    logging.info("[NUOVO] sync started")
    with DBSession() as db:
        # TODO STEP SUCCESSIVO
        pass
    logging.info("[NUOVO] sync completed")
