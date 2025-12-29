from collections.abc import Iterable
from typing import Optional, TypedDict

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.engine import Connection

from .db_schema import catalog_build_products


class BuildProductRow(TypedDict):
    build_id: int
    product_id: int
    product_name: Optional[str]
    temp_executable: Optional[str]

def upsert_build_product(conn: Connection, row: BuildProductRow) -> None:
    stmt = insert(catalog_build_products).values(**row)
    stmt = stmt.on_conflict_do_update(
        index_elements=[catalog_build_products.c.build_id, catalog_build_products.c.product_id],
        set_=row,
    )
    conn.execute(stmt)

def upsert_many(conn: Connection, rows: Iterable[BuildProductRow]) -> None:
    for row in rows:
        upsert_build_product(conn, row)

def get_by_build_id(conn: Connection, build_id: int) -> list[BuildProductRow]:
    stmt = select(catalog_build_products).where(catalog_build_products.c.build_id == build_id)
    results = conn.execute(stmt).mappings().all()
    return [BuildProductRow(**row) for row in results]


def get_by_product_id(conn: Connection, product_id: int) -> list[BuildProductRow]:
    stmt = select(catalog_build_products).where(catalog_build_products.c.product_id == product_id)
    results = conn.execute(stmt).mappings().all()
    return [BuildProductRow(**row) for row in results]


def get_by_id(conn: Connection, build_id: int, product_id: int) -> Optional[BuildProductRow]:
    stmt = select(catalog_build_products).where(
        (catalog_build_products.c.build_id == build_id) &
        (catalog_build_products.c.product_id == product_id)
    )
    result = conn.execute(stmt).mappings().first()
    return None if result is None else BuildProductRow(**result)