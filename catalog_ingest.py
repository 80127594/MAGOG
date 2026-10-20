import argparse
import json
import os
import tarfile
import logging
from pathlib import Path
from typing import Iterable, Mapping, Any, Sequence

from sqlalchemy.engine import Connection

from . import db
from . import catalog_products as catalog_products_mgr
from . import catalog_builds as catalog_builds_mgr
from . import catalog_dlcs as catalog_dlcs_mgr
from . import catalog_installers as catalog_installers_mgr
from . import catalog_build_products as catalog_build_products_mgr
from .catalog_products import ProductRow
from .catalog_builds import BuildRow
from .catalog_installers import InstallerRow
from .catalog_build_products import BuildProductRow
from .catalog_dlcs import DlcRow

logger = logging.getLogger(__name__)

def _extract_product_row(data: Mapping[str, Any]) -> ProductRow | None:
    """
    Required keys:
      - id, type, slug, title
    Optional keys:
      - global_date, is_in_development, image_boxart
    """
    builds = data.get("builds") or []

    if data.get("store_state") == "coming-soon" or data.get("type") not in ("game", "dlc", "pack"):
        return None
    # if data.get("type") == 'game' and len(builds) == 0:
    #     # logger.debug(f"Ignoring game product ID {data.get('id')} with no builds")
    #     return None
    # if data.get("type") == "dlc" and data.get("dl_installer") in (None, []):
    #     # logger.debug(f"Ignoring non-installable DLC product ID {data.get('id')}")
    #     return None
    # if data.get("type") == "package" and data.get("dl_installer") in (None, []):
    #     # logger.debug(f"Ignoring non-installable package product ID {data.get('id')}")
    #     return None
    try:
        product_id = int(data["id"])
        product_type = str(data["type"])
        slug = str(data["slug"])
        title = str(data["title"])
    except KeyError as exc:
        raise ValueError(f"product.json missing required key: {exc}") from exc
    if slug.endswith("_demo"):
        # remember, no demos
        return None
    # Optional fields
    global_date = data.get("global_date")
    is_in_development = bool(data.get("is_in_development", False))
    image_boxart = data.get("image_boxart")

    product: ProductRow = {
        "id": product_id,
        "type": product_type,
        "slug": slug,
        "title": title,
        "global_date": global_date,
        "is_in_development": is_in_development,
        "image_boxart": image_boxart,
    }
    return product

def _extract_dlc_row(data: Mapping[str, Any]) -> DlcRow | None:
    """
    define DLC-parent links from DLC side
    installer_qty is len(dl_installer), counting available installers for this DLC.
    """

    try:
        product_id = int(data["id"])
    except KeyError as exc:
        raise ValueError(f"product.json missing required key: {exc}") from exc

    product_type = data.get("type")
    if product_type != "dlc":
        # Only DLC SKUs declare a parent via requires
        return None

    requires: Sequence[Any] = data.get("requires") or []
    if not requires:
        # dlc with no parent ???
        return None

    try:
        parent_id = int(requires[0])
    except (TypeError, ValueError):
        return None

    installer_qty = len(data.get("dl_installer") or [])

    return DlcRow(
        parent_id=parent_id,
        dlc_id=product_id,
        installer_qty=installer_qty,
    )


def _extract_build_rows(data: Mapping[str, Any]) -> list[BuildRow]:
    rows: list[BuildRow] = []
    builds = data.get("builds") or []
    for b in builds:
        # if b.get("os") != "windows":
        #     continue
        try:
            build_id = int(b["id"])
            product_id = int(b.get("product_id", data["id"]))
        except (KeyError, TypeError, ValueError):
            # skip malformed build entry
            continue

        legacy_raw = b.get("legacy_build_id")
        try:
            legacy_build_id = int(legacy_raw) if legacy_raw is not None else None
        except (TypeError, ValueError):
            legacy_build_id = None
        date_published = b.get("date_published")
        if not date_published:
            logger.warning(f"Skipping build {build_id} for product {product_id} with no date_published")
            continue
        rows.append(
            {
                "id": build_id,
                "product_id": product_id,
                "date_published": date_published,
                "generation": int(b.get("generation", 0)),
                "version": b.get("version"),
                "legacy_build_id": legacy_build_id,
                "os": b.get("os"),
            }
        )

    return rows

def _extract_installer_rows(data: Mapping[str, Any]) -> list[InstallerRow]:
    rows: list[InstallerRow] = []
    installers = data.get("dl_installer") or []
    for inst in installers:
        try:
            installer_id = str(inst["id"])
            product_id = int(data["id"])
        except (KeyError, TypeError, ValueError):
            # skip malformed installer entry
            continue
        language_data = inst.get("language") or {}
        language_code = language_data.get("code")
        os_field = inst.get("os")
        version = inst.get("version")
        rows.append(
            {
                "product_id": product_id,
                "installer_id": installer_id,
                "language": language_code,
                "os": os_field,
                "version": version,
            }
        )
    return rows


def _extract_build_product_rows(data: Mapping[str, Any]) -> list[BuildProductRow]:
    rows: list[BuildProductRow] = []
    
    try:
        build_id = int(data["buildId"])
    except (KeyError, TypeError, ValueError):
        logger.warning(f"Skipping gen2 build manifest - invalid or missing buildId: {data.get('buildId')}")
        return rows
    
    products = data.get("products") or []
    if not products:
        logger.debug(f"Gen2 build manifest {build_id} has no products")
        return rows
    
    for idx, prod in enumerate(products):
        try:
            product_id = int(prod["productId"])
        except (KeyError, TypeError, ValueError):
            logger.warning(f"Skipping product[{idx}] in build {build_id} - invalid or missing productId: {prod.get('productId')}")
            continue
        
        product_name = prod.get("name")
        temp_executable = prod.get("temp_executable")
        
        rows.append(
            {
                "build_id": build_id,
                "product_id": product_id,
                "product_name": product_name,
                "temp_executable": temp_executable,
            }
        )
    
    return rows


def import_build_data_gen2(conn: Connection, data: Mapping[str, Any]) -> None:
    rows = _extract_build_product_rows(data)
    for row in rows:
        catalog_build_products_mgr.upsert_build_product(conn, row)

def import_product_data(conn: Connection, data: Mapping[str, Any]) -> None:
    """Import a single product record from an already-parsed dict"""
    product = _extract_product_row(data)
    dlcLink = _extract_dlc_row(data)
    buildRows = _extract_build_rows(data)
    installerRows = _extract_installer_rows(data)
    if not product:
        return
    catalog_products_mgr.upsert_product(conn, product)
    if dlcLink is not None:
        catalog_dlcs_mgr.update_dlc_link(conn, dlcLink)
    for build in buildRows:
        catalog_builds_mgr.upsert_build(conn, build)
    for installer in installerRows:
        catalog_installers_mgr.upsert_installer(conn, installer)

def import_product_json(conn: Connection, json_path: Path) -> None:
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    logger.debug(f"Importing product ID {data.get('id')} from {json_path}")
    import_product_data(conn, data)

def import_multiple_products(conn: Connection, json_paths: Iterable[Path]) -> None:
    for path in json_paths:
        import_product_json(conn, path)


def import_archive(conn: Connection, archive_path: Path) -> None:
    """Import product.json and gen2 build manifest (17-digit buildID.json) files from a .tar.xz archive"""
    archive_path = archive_path.expanduser()
    with tarfile.open(archive_path, mode="r:xz") as tf:
        temp_v1_builds = 0
        for member in tf:
            if not member.isfile():
                continue
            
            basename = os.path.basename(member.name)
            f = tf.extractfile(member)
            if f is None:
                continue
            
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                # skip malformed JSON
                continue
            
            if basename == "product.json":
                import_product_data(conn, data)
            elif basename.endswith(".json"):
                name_without_ext = basename[:-5]  # .json
                if name_without_ext.isdigit():
                    version = data.get("version")
                    if version == 2:
                        # inject buildId from filename if missing in manifest
                        if "buildId" not in data:
                            data["buildId"] = int(name_without_ext)
                            logger.debug(f"Injected buildId {name_without_ext} from filename (source: {member.name})")
                        import_build_data_gen2(conn, data)
                    elif version == 1:
                        # TODO: implement gen1 build manifest import
                        temp_v1_builds += 1
        logger.debug(f"Skipped {temp_v1_builds} gen1 build manifests in {archive_path}")

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--config",
        type=str,
        default="config.toml",
        help="Path to TOML config file (default: config.toml)",
    )
    parser.add_argument(
        "sources",
        type=Path,
        nargs="+",
        help="One or more product.json files or .tar.xz archives to import",
    )
    args, _unknown = parser.parse_known_args(argv)
    return args


def cli(argv: list[str] | None = None) -> None:
    from . import config, log
    args = _parse_args(argv)
    SETTINGS = config.load_config(args.config)
    log.setup_logging(SETTINGS)
    logger = logging.getLogger(__name__)
    database_cfg = SETTINGS.get("database", {})
    db_path = Path(database_cfg.get("path", "data/catalog.db")).expanduser()
    dbase = db.Database(str(db_path))
    
    with dbase.connect() as conn:
        for path in args.sources:
            if not path.exists():
                raise FileNotFoundError(path)
            # Handle .tar.xz archives
            if "".join(path.suffixes[-2:]) == ".tar.xz":
                import_archive(conn, path)
            # Handle bare JSON files (product.json)
            elif path.suffix == ".json":
                import_product_json(conn, path)
            else:
                raise ValueError(f"Unsupported source type: {path}")

if __name__ == "__main__":
    cli()
