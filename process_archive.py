#!/usr/bin/env python3

import argparse
import json
import os
import subprocess
import sys
import tarfile
from typing import Any, Dict, List, Optional, Iterator

JQ_FILTER = (
    'select((.type == "game" or .type == "dlc") and .store_state != "coming-soon") '
    '| {type,title,slug,id,image_boxart,global_date,is_in_development,dl_installer,'
    'builds:[.builds[] | select(.os == "windows") '
    '| {id, date_published, version, generation}]}'
)


class UnsupportedModeError(RuntimeError):
    """unsupported combination of arguments"""

def process_archive(
    source: str,
    *,
    source_type: str = "path",
    mode: str = "json",
    jq_filter: str = JQ_FILTER,
) -> List[Dict[str, Any]]:
    if source_type != "path":
        raise UnsupportedModeError(
            f"source_type={source_type} is not supported (yet)"
        )
    if mode != "json":
        raise UnsupportedModeError(
            f"mode={mode} is not supported (yet)"
        )
    return list(
        iter_products(
            source,
            source_type=source_type,
            jq_filter=jq_filter,
        )
    )

def _run_jq_on_bytes(data: bytes, jq_filter: str) -> Optional[Dict[str, Any]]:
    try:
        data_str = data.decode("utf-8")
        proc = subprocess.run(
            ["jq", "-c", jq_filter],
            input=data_str,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            "jq not on PATH?"
        ) from exc


    # jq behavior:
    # - exit code 0, stdout="" when the object is filtered out
    # - exit code 0, stdout="<json>" when it matches
    # - non-zero exit code with non-empty stderr - treat as error.
    stdout = proc.stdout.strip()
    if proc.returncode != 0 and stdout:
        raise RuntimeError(
            f"jq failed with exit code {proc.returncode}: {proc.stderr.strip()}"
        )

    if not stdout:
        return None

    try:
        return json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Failed to parse jq output as JSON. Output was: {stdout}"
        ) from exc

def _iter_product_members(tf: tarfile.TarFile):
    for member in tf:
        if not member.isfile():
            continue
        if os.path.basename(member.name) == "product.json":
            yield member

def iter_products(
    source: str,
    *,
    source_type: str = "path",
    jq_filter: str = JQ_FILTER,
) -> Iterator[Dict[str, Any]]:
    if source_type != "path":
        raise UnsupportedModeError(
            f"source_type={source_type} is not supported yet"
        )

    with tarfile.open(source, mode="r:xz") as tf:
        for member in _iter_product_members(tf):
            f = tf.extractfile(member)
            if f is None:
                continue
            raw = f.read()
            record = _run_jq_on_bytes(raw, jq_filter=jq_filter)
            if record is not None:
                # yield as soon as we have a product
                yield record


def _cli(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="collate product.json from a GOGDB daily snapshot"
    )
    parser.add_argument(
        "source",
        help="Path to the .tar.xz archive (local file).",
    )
    # parser.add_argument("--mode", choices=["json", "sql"], default="json", ...)
    # parser.add_argument("--source-type", choices=["path", "url"], default="path", ...)
    args = parser.parse_args(argv)

    for product in iter_products(args.source, source_type="path"):
        json.dump(product, sys.stdout, ensure_ascii=False, separators=(',', ':'))
        sys.stdout.write('\n')
    return 0

if __name__ == "__main__":
    _cli()
