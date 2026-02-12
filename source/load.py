from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Iterable
import pandas as pd


SUPPORTED_EXT = {".xlsx", ".xls", ".csv"}


@dataclass
class DataLoader:
    data_dir: Path

    def list_supported_files(self) -> list[Path]:
        """Return all supported files in data_dir, sorted by name."""
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data dir not found: {self.data_dir.resolve()}")

        files = [
            p for p in self.data_dir.iterdir()
            if p.is_file() and p.suffix.lower() in SUPPORTED_EXT
        ]
        files.sort(key=lambda p: p.name.lower())
        return files

    def resolve_files(self, filenames: Optional[Iterable[str]]) -> list[Path]:
        """
        If filenames is None -> return all supported files in data_dir (default behavior).
        If filenames provided -> return only those files (must exist).
        """
        if not filenames:
            files = self.list_supported_files()
            if not files:
                raise FileNotFoundError(f"No .xlsx/.xls/.csv found in {self.data_dir.resolve()}")
            return files

        resolved: list[Path] = []
        for name in filenames:
            p = self.data_dir / name
            if not p.exists():
                raise FileNotFoundError(f"File not found: {p.resolve()}")
            if p.suffix.lower() not in SUPPORTED_EXT:
                raise ValueError(f"Unsupported file extension: {p.name}")
            resolved.append(p)
        return resolved

    def load_path(self, path: Path, sheet: Optional[str] = None) -> pd.DataFrame:
        """Loads Excel/CSV given an absolute/relative path."""
        ext = path.suffix.lower()
        if ext in {".xlsx", ".xls"}:
            return pd.read_excel(path, sheet_name=sheet if sheet else 0, engine="openpyxl")
        if ext == ".csv":
            return pd.read_csv(path)
        raise ValueError(f"Unsupported extension: {ext}")
