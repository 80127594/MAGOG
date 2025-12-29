from collections.abc import Iterable
from typing import Optional, TypedDict

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.engine import Connection

from .db_schema import catalog_installers

class InstallerRow(TypedDict):
    id: int
    product_id: int
    installer_id: str
    language: Optional[str]
    os: Optional[str]
    version: Optional[str]


def upsert_installer(conn: Connection, row: InstallerRow) -> None:
    stmt = insert(catalog_installers).values(**row)
    stmt = stmt.on_conflict_do_update(
        index_elements=[catalog_installers.c.id],
        set_=row,
    )
    conn.execute(stmt)

def upsert_many(conn: Connection, rows: Iterable[InstallerRow]) -> None:
    for row in rows:
        upsert_installer(conn, row)
