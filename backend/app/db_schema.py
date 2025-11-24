from sqlalchemy import (
    MetaData, Table, Column,
    Integer, String, Text, Boolean,
    ForeignKey, UniqueConstraint, Index,
)

metadata = MetaData()

def ensure_schema(engine) -> None:
    metadata.create_all(engine)

catalog_products = Table(
    "catalog_products",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("type", String, nullable=False),
    Column("slug", String, nullable=False),
    Column("title", String, nullable=False),
    Column("global_date", String, nullable=True),
    Column("is_in_development", Boolean, nullable=False),
    Column("image_boxart", String, nullable=True),
)

catalog_dlcs = Table(
    "catalog_dlcs",
    metadata,
    Column("parent_id", Integer, ForeignKey("catalog_products.id"), nullable=False),
    Column("dlc_id", Integer, ForeignKey("catalog_products.id"), primary_key=True),
    Column("installer_qty", Integer, nullable=False),
)

catalog_builds = Table(
    "catalog_builds",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("product_id", Integer, ForeignKey("catalog_products.id"), nullable=False),
    Column("date_published", String, nullable=False),
    Column("generation", Integer, nullable=False),
    Column("version", String, nullable=True),
    Column("legacy_build_id", Integer, nullable=True),
)

Index(
    "idx_catalog_builds_product_date",
    catalog_builds.c.product_id,
    catalog_builds.c.date_published.desc(),
)
Index("idx_catalog_products_slug", catalog_products.c.slug)
