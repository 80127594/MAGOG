from collections.abc import Iterable
from typing import TypedDict

from sqlalchemy import select, delete, func
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.engine import Connection

from .db_schema import catalog_dlcs


class DlcRow(TypedDict):
    parent_id: int      # base game product_id
    dlc_id: int         # DLC product_id
    installer_qty: int  # number of installers for this DLC (0 = non-installable)


def update_dlc_link(conn: Connection, row: DlcRow) -> None:
    stmt = insert(catalog_dlcs).values(**row)
    stmt = stmt.on_conflict_do_update(
        index_elements=[catalog_dlcs.c.dlc_id],
        set_=row,
    )

    conn.execute(stmt)


def replace_for_parent(conn: Connection, parent_id: int, rows: Iterable[DlcRow]) -> None:
    """
    - First deletes existing rows for this parent.
    - Then inserts/updates only DLCs with installer_qty > 0
    """
    conn.execute(
        delete(catalog_dlcs).where(catalog_dlcs.c.parent_id == parent_id)
    )

    for row in rows:
        if row["installer_qty"] <= 0:
            # non-installable DLC: ignore
            continue

        stmt = insert(catalog_dlcs).values(**row)
        stmt = stmt.on_conflict_do_update(
            index_elements=[catalog_dlcs.c.dlc_id],
            set_=row,
        )

        conn.execute(stmt)


def count_installable_for_parent(conn: Connection, parent_id: int) -> int:
    """
    return the number of installable DLC SKUs for a given parent product.
    count of DLC entries with installer_qty > 0, not the sum of installer_qty.
    """
    stmt = (
        select(func.count())
        .select_from(catalog_dlcs)
        .where(
            catalog_dlcs.c.parent_id == parent_id,
            catalog_dlcs.c.installer_qty > 0,
        )
    )
    return int(conn.execute(stmt).scalar_one())


def get_installable_for_parent(conn: Connection, parent_id: int) -> list[DlcRow]:
    stmt = (
        select(catalog_dlcs)
        .where(
            catalog_dlcs.c.parent_id == parent_id,
            catalog_dlcs.c.installer_qty > 0,
        )
        .order_by(catalog_dlcs.c.dlc_id)
    )
    rows = conn.execute(stmt).mappings().all()
    return [DlcRow(**row) for row in rows]
