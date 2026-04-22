import json
from pathlib import Path
from typing import Any

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_STORE_MAPPING_PATH = BASE_DIR / "store_mapping_unified.json"


def load_store_mapping(mapping_path: str | Path | None = None) -> dict[str, str]:
    path = Path(mapping_path) if mapping_path else DEFAULT_STORE_MAPPING_PATH
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_store_query_names(store_name: Any, mapping_path: str | Path | None = None) -> list[str]:
    if store_name is None or (isinstance(store_name, float) and pd.isna(store_name)):
        return []

    name = str(store_name).strip()
    if not name:
        return []

    mapping = load_store_mapping(mapping_path)
    canonical_name = mapping.get(name, name)

    aliases = {canonical_name, name}
    for raw_name, normalized_name in mapping.items():
        if normalized_name == canonical_name:
            aliases.add(raw_name)

    return sorted(alias for alias in aliases if alias)


def normalize_store_name(raw_name: Any, store_mapping: dict[str, str] | None = None) -> str | None:
    if raw_name is None or (isinstance(raw_name, float) and pd.isna(raw_name)):
        return None
    name = str(raw_name).strip()
    if not name:
        return None
    mapping = store_mapping if store_mapping is not None else load_store_mapping()
    return mapping.get(name, name)


def normalize_store_series(series: pd.Series, store_mapping: dict[str, str] | None = None) -> pd.Series:
    mapping = store_mapping if store_mapping is not None else load_store_mapping()
    return series.apply(lambda value: normalize_store_name(value, mapping))
