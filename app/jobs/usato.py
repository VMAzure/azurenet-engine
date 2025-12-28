import logging
from app.database import DBSession

def sync_usato():
    logging.info("[USATO] sync started")
    with DBSession() as db:
        # TODO STEP SUCCESSIVO
        pass
    logging.info("[USATO] sync completed")
