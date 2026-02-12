from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import pandas as pd


SPECIAL_EMPTY = {"leer", "empty", "blank", "null", "none", ""}


def is_empty_value(x) -> bool:
    """True if x is None/NaN/empty-string/whitespace."""
    if x is None:
        return True
    if pd.isna(x):
        return True
    if isinstance(x, str) and x.strip() == "":
        return True
    return False


def normalize_str(x) -> str:
    """String-normalization used for comparisons."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    return str(x).strip()


def parse_filter_value(raw: str) -> str:
    """Return normalized filter value. Keeps special tokens like 'leer'."""
    return normalize_str(raw).lower()


def match_filter(series: pd.Series, filter_value_raw: str) -> pd.Series:
    """
    Build a boolean mask where the column matches the filter-value semantics.
    Special values:
      - 'leer'/'empty'/'blank'/... => column is empty (NaN or whitespace)
      - 'nichtleer'/'notempty' => column is NOT empty
      - otherwise: string-equality after normalize/strip
    """
    v = parse_filter_value(filter_value_raw)

    if v in SPECIAL_EMPTY:
        return series.apply(is_empty_value)

    if v in {"nichtleer", "notempty", "not_empty"}:
        return ~series.apply(is_empty_value)

    # default exact match (string-normalized)
    return series.map(normalize_str).str.lower().eq(v)

def excel_col_to_index(col: str) -> int:
    """
    Excel column letters -> zero-based index.
    A -> 0, B -> 1, Z -> 25, AA -> 26, AC -> 28, ...
    """
    col = col.strip().upper()
    if not col.isalpha():
        raise ValueError(f"Not an Excel column: {col}")

    idx = 0
    for ch in col:
        idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return idx - 1


def resolve_col(df_columns, token: str) -> str:
    """
    If token matches a real column name -> return it.
    Else if token looks like Excel letters -> map to df_columns[index].
    """
    if token in df_columns:
        return token

    t = token.strip()
    if t.isalpha():  # looks like Excel column letters
        i = excel_col_to_index(t)
        cols = list(df_columns)
        if i < 0 or i >= len(cols):
            raise KeyError(f"Excel column {token} out of range (max {len(cols)} columns).")
        return cols[i]

    raise KeyError(f"Column not found: {token}")



def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class FilterSpec:
    column: str
    value: str

    def describe(self) -> str:
        return f"{self.column} = {self.value}"
