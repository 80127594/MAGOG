from collections.abc import Iterable
from typing import Optional, TypedDict

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.engine import Connection

from .db_schema import catalog_products


class ProductRow(TypedDict):
    id: int
    type: str
    slug: str
    title: str
    global_date: Optional[str]
    is_in_development: bool
    image_boxart: Optional[str]


def upsert_product(conn: Connection, row: ProductRow) -> None:
    stmt = insert(catalog_products).values(**row)
    stmt = stmt.on_conflict_do_update(
        index_elements=[catalog_products.c.id],
        set_=row,
    )
    conn.execute(stmt)


def upsert_many(conn: Connection, rows: Iterable[ProductRow]) -> None:
    for row in rows:
        upsert_product(conn, row)


def get_by_id(conn: Connection, product_id: int) -> Optional[ProductRow]:
    stmt = select(catalog_products).where(catalog_products.c.id == product_id)
    result = conn.execute(stmt).mappings().first()
    return None if result is None else ProductRow(**result)


def get_by_slug(conn: Connection, slug: str) -> Optional[ProductRow]:
    stmt = select(catalog_products).where(catalog_products.c.slug == slug)
    result = conn.execute(stmt).mappings().first()
    return None if result is None else ProductRow(**result)


def get_id_by_slug(conn: Connection, slug: str) -> Optional[int]:
    stmt = select(catalog_products.c.id).where(catalog_products.c.slug == slug)
    result = conn.execute(stmt).scalar_one_or_none()
    return result

def get_ids_by_slugs(conn: Connection, slugs: Iterable[str]) -> dict[str, list[int]]:
    slug_set = list(set(slugs))
    if not slug_set:
        return {}
    stmt = select(
        catalog_products.c.slug,
        catalog_products.c.id,
    ).where(catalog_products.c.slug.in_(slug_set))
    rows = conn.execute(stmt).mappings().all()
    mapping: dict[str, list[int]] = {}
    for row in rows:
        mapping.setdefault(row['slug'], []).append(row['id'])
    return mapping


def get_slugs_by_ids(conn: Connection, ids: Iterable[int]) -> dict[int, str]:
    id_set = list(set(ids))
    if not id_set:
        return {}
    stmt = select(
        catalog_products.c.id,
        catalog_products.c.slug,
    ).where(catalog_products.c.id.in_(id_set))
    rows = conn.execute(stmt).mappings().all()
    mapping: dict[int, str] = {}
    for row in rows:
        mapping[row['id']] = row['slug']
    return mapping