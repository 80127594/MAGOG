import argparse
from contextlib import contextmanager
from pathlib import Path
from typing import Generator
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection

import logging
from . import db_schema

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, path: str):
        self.db_path = path
        self.engine: Engine = create_engine(f"sqlite:///{self.db_path}", future=True)
        self._init_pragma()
        logger.info(f"Database engine created for {self.db_path}")
        db_schema.ensure_schema(self.engine)

    def _init_pragma(self) -> None:
        with self.engine.connect() as conn:
            conn.exec_driver_sql("PRAGMA journal_mode=WAL;")
            conn.exec_driver_sql("PRAGMA busy_timeout=5000;")

    @contextmanager
    def connect(self) -> Generator[Connection, None, None]:
        with self.engine.begin() as conn:
            yield conn

    @contextmanager
    def connect_readonly(self) -> Generator[Connection, None, None]:
        with self.engine.connect() as conn:
            yield conn

if __name__ == "__main__":
    from . import log
    from . import config

    def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument(
            "--config",
            type=str,
            default="config.toml",
            help="Path to TOML config file (default: config.toml)",
        )
        args, _unknown = parser.parse_known_args(argv)
        return args


    _args = _parse_args()
    SETTINGS = config.load_config(_args.config)
    log.setup_logging(SETTINGS)
    logger = logging.getLogger(__name__)
    database_cfg = SETTINGS.get("database", {})
    db_path = Path(database_cfg.get("path", "data/catalog.db")).expanduser()
    logger.info(f"Using database path: {db_path}")
    dbase = Database(str(db_path))
    with dbase.connect() as conn:
        result = conn.execute(text("SELECT sqlite_version();"))
        version = result.scalar_one()
        logger.info(f"Connected to SQLite version {version}")


