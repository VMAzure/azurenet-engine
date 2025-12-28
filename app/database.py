import os
import logging
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# DATABASE URL
# ============================================================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL non impostata")

def add_param(url: str, **params) -> str:
    u = urlparse(url)
    q = dict(parse_qsl(u.query))
    q.update(params)
    return urlunparse(u._replace(query=urlencode(q)))

DATABASE_URL = add_param(
    DATABASE_URL,
    application_name="azurenet_engine",
    options="-c idle_in_transaction_session_timeout=60000"
)

# ============================================================
# SQLALCHEMY ENGINE (BATCH SAFE)
# ============================================================
SQL_ECHO = os.getenv("SQL_ECHO", "false").lower() == "true"

engine = create_engine(
    DATABASE_URL,
    echo=SQL_ECHO,
    pool_size=5,            # batch engine → pool ridotto
    max_overflow=5,
    pool_pre_ping=True,
    pool_recycle=180,
    pool_timeout=30,
    connect_args={
        "sslmode": "require",
        "connect_timeout": 15,
        "options": "-c statement_timeout=30000",
    },
)

# ============================================================
# ORM BASE & SESSION
# ============================================================
Base = declarative_base()

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)

# ============================================================
# SESSION CONTEXT (BATCH)
# ============================================================
class DBSession:
    """
    Context manager per job batch.
    Commit esplicito, rollback sicuro.
    """
    def __enter__(self):
        self.db = SessionLocal()
        return self.db

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type:
                self.db.rollback()
                logging.exception("DB ERROR in azurenet-engine")
            else:
                self.db.commit()
        finally:
            self.db.close()

# ============================================================
# MODELS REGISTRATION
# ============================================================
from app.models import vehicle  # SOLO modelli veicolo
