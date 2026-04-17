from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.pool import NullPool

from app.core.config import settings


class Base(DeclarativeBase):
    pass


def _normalize_database_url(url: str) -> str:
    if url.startswith("postgres://"):
        return f"postgresql+psycopg://{url[len('postgres://'):]}"
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


database_url = _normalize_database_url(settings.database_url)
is_sqlite = database_url.startswith("sqlite")
is_supabase_pooler = "pooler.supabase.com" in database_url
is_supabase_transaction_pooler = is_supabase_pooler and ":6543/" in database_url

if is_sqlite:
    connect_args = {"check_same_thread": False}
    engine = create_engine(
        database_url,
        connect_args=connect_args,
        future=True,
    )
else:
    connect_args = {"connect_timeout": 10}
    engine_kwargs: dict = {
        "connect_args": connect_args,
        "pool_pre_ping": True,
        "future": True,
    }
    if is_supabase_pooler:
        # Supabase poolers already manage connections. NullPool avoids
        # app-side pooling conflicts and helps long-hanging checkouts.
        engine_kwargs["poolclass"] = NullPool
    if is_supabase_transaction_pooler:
        # Transaction poolers do not support prepared statements.
        connect_args["prepare_threshold"] = None
    engine = create_engine(database_url, **engine_kwargs)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
