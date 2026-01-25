"""
Database connection management for FDA Adverse Events Pipeline.

Supports both PostgreSQL (production) and SQLite (development/demo).
"""

import os
import logging
from dataclasses import dataclass
from typing import Optional
from contextlib import contextmanager

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
from dotenv import load_dotenv

from .models import Base

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    """Database configuration."""

    # PostgreSQL settings (production)
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "fda_adverse_events"
    postgres_user: str = "postgres"
    postgres_password: str = ""

    # SQLite settings (development)
    sqlite_path: str = "data/fda_adverse_events.db"

    # Connection settings
    use_sqlite: bool = True  # Default to SQLite for easy demo
    echo_sql: bool = False
    pool_size: int = 5

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Load configuration from environment variables."""
        return cls(
            postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_db=os.getenv("POSTGRES_DB", "fda_adverse_events"),
            postgres_user=os.getenv("POSTGRES_USER", "postgres"),
            postgres_password=os.getenv("POSTGRES_PASSWORD", ""),
            sqlite_path=os.getenv("SQLITE_PATH", "data/fda_adverse_events.db"),
            use_sqlite=os.getenv("USE_SQLITE", "true").lower() == "true",
            echo_sql=os.getenv("ECHO_SQL", "false").lower() == "true"
        )

    @property
    def connection_string(self) -> str:
        """Get the appropriate connection string."""
        if self.use_sqlite:
            return f"sqlite:///{self.sqlite_path}"
        else:
            return (
                f"postgresql://{self.postgres_user}:{self.postgres_password}"
                f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
            )


# Global engine and session factory
_engine = None
_SessionFactory = None


def get_engine(config: Optional[DatabaseConfig] = None):
    """
    Get or create the database engine.

    Args:
        config: Database configuration. If None, loads from environment.

    Returns:
        SQLAlchemy Engine instance
    """
    global _engine

    if _engine is None:
        config = config or DatabaseConfig.from_env()

        if config.use_sqlite:
            # Ensure directory exists for SQLite file
            sqlite_dir = os.path.dirname(config.sqlite_path)
            if sqlite_dir and not os.path.exists(sqlite_dir):
                os.makedirs(sqlite_dir, exist_ok=True)
                logger.info(f"Created directory for SQLite database: {sqlite_dir}")

            # SQLite-specific settings
            _engine = create_engine(
                config.connection_string,
                echo=config.echo_sql,
                connect_args={"check_same_thread": False},
                poolclass=StaticPool
            )

            # Enable foreign keys for SQLite
            @event.listens_for(_engine, "connect")
            def set_sqlite_pragma(dbapi_connection, connection_record):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()
        else:
            # PostgreSQL settings
            _engine = create_engine(
                config.connection_string,
                echo=config.echo_sql,
                pool_size=config.pool_size,
                max_overflow=10
            )

        logger.info(f"Database engine created: {'SQLite' if config.use_sqlite else 'PostgreSQL'}")

    return _engine


def get_session(config: Optional[DatabaseConfig] = None) -> Session:
    """
    Get a new database session.

    Args:
        config: Database configuration.

    Returns:
        SQLAlchemy Session instance
    """
    global _SessionFactory

    if _SessionFactory is None:
        engine = get_engine(config)
        _SessionFactory = sessionmaker(bind=engine)

    return _SessionFactory()


@contextmanager
def session_scope(config: Optional[DatabaseConfig] = None):
    """
    Provide a transactional scope around a series of operations.

    Usage:
        with session_scope() as session:
            session.add(obj)
            # Auto-commits on success, rollback on exception
    """
    session = get_session(config)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_database(config: Optional[DatabaseConfig] = None, drop_existing: bool = False):
    """
    Initialize the database schema.

    Creates all tables defined in the models.

    Args:
        config: Database configuration.
        drop_existing: If True, drops existing tables first.
    """
    engine = get_engine(config)

    if drop_existing:
        logger.warning("Dropping existing tables...")
        Base.metadata.drop_all(engine)

    logger.info("Creating database tables...")
    Base.metadata.create_all(engine)
    logger.info("Database initialized successfully")


def reset_connection():
    """Reset the global engine and session factory."""
    global _engine, _SessionFactory

    if _engine:
        _engine.dispose()

    _engine = None
    _SessionFactory = None
