from collections.abc import Iterable
from typing import Optional, TypedDict

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.engine import Connection

from .db_schema import catalog_builds


class BuildRow(TypedDict):
    id: int
    product_id: int
    date_published: str
    generation: int
    version: str
    legacy_build_id: Optional[int]
    os: Optional[str]


def upsert_build(conn: Connection, row: BuildRow) -> None:
    stmt = insert(catalog_builds).values(**row)
    stmt = stmt.on_conflict_do_update(
        index_elements=[catalog_builds.c.id],
        set_=row,
    )
    conn.execute(stmt)


def upsert_many(conn: Connection, rows: Iterable[BuildRow]) -> None:
    for row in rows:
        upsert_build(conn, row)


def get_latest_for_product(conn: Connection, product_id: int) -> Optional[BuildRow]:
    """
    helper for computing staleness
    """
    stmt = (
        select(catalog_builds)
        .where(catalog_builds.c.product_id == product_id)
        .order_by(catalog_builds.c.date_published.desc())
        .limit(1)
    )
    result = conn.execute(stmt).mappings().first()
    return None if result is None else BuildRow(**result)
