from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Optional
import pandas as pd

from util import FilterSpec, match_filter, normalize_str


@dataclass
class ValueStats:
    value: str
    total: int
    matched: int

    @property
    def pct(self) -> float:
        return 0.0 if self.total == 0 else (100.0 * self.matched / self.total)


@dataclass
class ColumnAnalysis:
    column: str
    stats: list[ValueStats]  # sorted best-first


@dataclass
class ComboRow:
    columns: list[str]
    values: list[str]
    total: int
    matched: int

    @property
    def pct(self) -> float:
        return 0.0 if self.total == 0 else (100.0 * self.matched / self.total)

    def label(self) -> str:
        pairs = [f"{c}={v}" for c, v in zip(self.columns, self.values)]
        return " | ".join(pairs)


@dataclass
class ComboAnalysis:
    columns: list[str]             # first k columns used
    top: list[ComboRow]            # top-N combos


@dataclass
class AnalysisResult:
    file_rows: int
    filter_spec: FilterSpec
    filter_matched_total: int
    filter_matched_pct: float
    per_column: list[ColumnAnalysis]
    combos: list[ComboAnalysis]


class Analyzer:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def validate_columns(self, cols: Iterable[str]) -> None:
        missing = [c for c in cols if c not in self.df.columns]
        if missing:
            raise KeyError(f"Columns not found in data: {missing}")

    def build_filter_mask(self, spec: FilterSpec) -> pd.Series:
        if spec.column not in self.df.columns:
            raise KeyError(f"Filter column not found: {spec.column}")
        return match_filter(self.df[spec.column], spec.value)

    def analyze_columns(
        self,
        filter_spec: FilterSpec,
        analyze_cols: list[str],
        top_values_per_col: int = 30,
        min_group_size: int = 1,
    ) -> tuple[list[ColumnAnalysis], pd.Series]:
        """
        For each analyze column:
          value -> % where filter condition is true within that value-group
        """
        self.validate_columns(analyze_cols)
        mask = self.build_filter_mask(filter_spec)

        results: list[ColumnAnalysis] = []
        for col in analyze_cols:
            s = self.df[col].map(normalize_str).fillna("")
            tmp = pd.DataFrame({"value": s, "matched": mask.astype(int)})

            grp = tmp.groupby("value", dropna=False).agg(
                total=("matched", "count"),
                matched=("matched", "sum"),
            ).reset_index()

            # filter tiny groups if requested
            grp = grp[grp["total"] >= min_group_size].copy()
            grp["pct"] = (grp["matched"] / grp["total"]) * 100.0

            # Sort: highest pct first, then matched count
            grp = grp.sort_values(["pct", "matched", "total"], ascending=[False, False, False]).head(top_values_per_col)

            stats = [
                ValueStats(value=row["value"] if row["value"] != "" else "<EMPTY>",
                           total=int(row["total"]),
                           matched=int(row["matched"]))
                for _, row in grp.iterrows()
            ]
            results.append(ColumnAnalysis(column=col, stats=stats))

        return results, mask

    def analyze_combos(
        self,
        filter_mask: pd.Series,
        analyze_cols: list[str],
        top_n: int = 10,
        min_group_size: int = 1,
    ) -> list[ComboAnalysis]:
        """
        For k=1..len(analyze_cols): take first k columns and find top-N combinations
        by matched count (how often filter condition is true for that combo).
        """
        combos: list[ComboAnalysis] = []

        # Pre-normalize string versions for stable grouping
        norm_df = self.df[analyze_cols].map(normalize_str).fillna("")
        norm_df = norm_df.replace({"": "<EMPTY>"})
        norm_df["_matched"] = filter_mask.astype(int)

        for k in range(1, len(analyze_cols) + 1):
            cols_k = analyze_cols[:k]
            grp = norm_df.groupby(cols_k, dropna=False).agg(
                total=("_matched", "count"),
                matched=("_matched", "sum"),
            ).reset_index()

            grp = grp[grp["total"] >= min_group_size].copy()
            grp = grp.sort_values(["matched", "total"], ascending=[False, False]).head(top_n)

            rows: list[ComboRow] = []
            for _, row in grp.iterrows():
                values = [str(row[c]) for c in cols_k]
                rows.append(
                    ComboRow(columns=cols_k, values=values, total=int(row["total"]), matched=int(row["matched"]))
                )

            combos.append(ComboAnalysis(columns=cols_k, top=rows))

        return combos

    def run(
        self,
        filter_spec: FilterSpec,
        analyze_cols: list[str],
        top_values_per_col: int = 30,
        combo_top_n: int = 10,
        min_group_size: int = 1,
    ) -> AnalysisResult:
        per_col, mask = self.analyze_columns(
            filter_spec=filter_spec,
            analyze_cols=analyze_cols,
            top_values_per_col=top_values_per_col,
            min_group_size=min_group_size,
        )
        combos = self.analyze_combos(
            filter_mask=mask,
            analyze_cols=analyze_cols,
            top_n=combo_top_n,
            min_group_size=min_group_size,
        )

        total = len(self.df)
        matched_total = int(mask.sum())
        matched_pct = 0.0 if total == 0 else (100.0 * matched_total / total)

        return AnalysisResult(
            file_rows=total,
            filter_spec=filter_spec,
            filter_matched_total=matched_total,
            filter_matched_pct=matched_pct,
            per_column=per_col,
            combos=combos,
        )
