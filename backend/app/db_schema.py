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
    Column("os", String, nullable=True),
)

catalog_installers = Table(
    "catalog_installers",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("product_id", Integer, ForeignKey("catalog_products.id"), nullable=False),
    Column("installer_id", String, nullable=False),
    Column("language", String, nullable=True),
    Column("os", String, nullable=True),
    Column("version", String, nullable=True),
)

Index(
    "idx_catalog_builds_product_date",
    catalog_builds.c.product_id,
    catalog_builds.c.date_published.desc(),
)
Index("idx_catalog_products_slug", catalog_products.c.slug)


library_stores = Table(
    "library_stores",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String, nullable=False, unique=True),
    Column("path", Text, nullable=False),
    Column("is_active", Boolean, nullable=False, default=True),
    Column("created_at", String, nullable=False),
    Column("updated_at", String, nullable=False),
)

library_products = Table(
    "library_products",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("store_id", Integer, ForeignKey("library_stores.id"), nullable=False),
    Column("product_id", Integer, ForeignKey("catalog_products.id"), nullable=False),
    Column("last_updated", String, nullable=True),
    UniqueConstraint("store_id", "product_id", name="uix_store_product")
)

artifact_fingerprints = Table(
    "artifact_fingerprints",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("hash_type", String, nullable=False),
    Column("hash_value", String, nullable=False),
    Column("exe_size_bytes", Integer, nullable=False),
    Column("pe_product_name", String, nullable=True),
    Column("pe_product_version", String, nullable=True),
    Column("sig_timestamp", String, nullable=True),
    UniqueConstraint("hash_type", "hash_value", name="uix_hash_type_value")
)
