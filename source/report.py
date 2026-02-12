from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from util import ensure_dir
from analyze import AnalysisResult


@dataclass
class MarkdownReportWriter:
    output_path: Path

    def write(self, result: AnalysisResult, source_file: str) -> Path:
        ensure_dir(self.output_path.parent)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        lines: list[str] = []
        lines.append(f"# Business Central Analyse – leere/auffällige Beschreibung\n")
        lines.append(f"- **Zeitpunkt:** {now}")
        lines.append(f"- **Quelle:** `{source_file}`")
        lines.append(f"- **Zeilen gesamt:** {result.file_rows}")
        lines.append(f"- **Filter:** `{result.filter_spec.describe()}`")
        lines.append(f"- **Treffer im Filter:** {result.filter_matched_total} ({result.filter_matched_pct:.2f}%)")
        lines.append("")

        lines.append("## 1. Anteil Filter-Treffer je Spaltenwert\n")
        lines.append("Interpretation: Für jeden Wert einer Spalte steht dort, wie oft der Filter innerhalb dieser Wert-Gruppe zutrifft.\n")

        for col_res in result.per_column:
            lines.append(f"### Spalte: `{col_res.column}`\n")
            if not col_res.stats:
                lines.append("_Keine Daten / keine Gruppen (ggf. min_group_size zu hoch)._")
                lines.append("")
                continue

            lines.append("| Wert | Zeilen gesamt | Filter-Treffer | Anteil Filter |")
            lines.append("| --- | ---: | ---: | ---: |")
            for s in col_res.stats:
                lines.append(f"| {escape_md(s.value)} | {s.total} | {s.matched} | {s.pct:.2f}% |")
            lines.append("")

        lines.append("## 2. Häufigste Konstellationen (Top-Kombinationen)\n")
        lines.append("Sortierung: nach **Anzahl Filter-Treffer** (wie oft die Kombination im gefilterten Zustand vorkommt).\n")

        for combo in result.combos:
            lines.append(f"### Kombination: `{', '.join(combo.columns)}`\n")
            if not combo.top:
                lines.append("_Keine Kombinationen gefunden._\n")
                continue

            lines.append("| Kombination | Zeilen gesamt | Filter-Treffer | Anteil Filter |")
            lines.append("| --- | ---: | ---: | ---: |")
            for row in combo.top:
                lines.append(f"| {escape_md(row.label())} | {row.total} | {row.matched} | {row.pct:.2f}% |")
            lines.append("")

        self.output_path.write_text("\n".join(lines), encoding="utf-8")
        return self.output_path


def escape_md(s: str) -> str:
    """
    Escaping for Markdown tables:
    - '|' must be escaped to not break table columns
    - '<' and '>' should be escaped because some Markdown renderers treat them as HTML tags
    - '&' should be escaped first to avoid double-escaping issues
    """
    text = str(s)

    # HTML-escape first
    text = text.replace("&", "&amp;")
    text = text.replace("<", "&lt;")
    text = text.replace(">", "&gt;")

    # Markdown table escape
    text = text.replace("|", "\\|")

    return text
