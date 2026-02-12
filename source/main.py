from __future__ import annotations
import argparse
from pathlib import Path

from load import DataLoader
from analyze import Analyzer
from report import MarkdownReportWriter
from util import FilterSpec, resolve_col


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="BC Excel/CSV Analyse: Anteil Filter-Treffer je Spalte und Wert + Top-Kombinationen als Markdown Report"
    )

    p.add_argument("--data-dir", default="data", help="Datenverzeichnis (default: data)")
    p.add_argument(
        "--files",
        nargs="*",
        default=None,
        help="Optional: bestimmte Datei(en) in data-dir analysieren (z.B. --files A.xlsx B.xlsx). "
             "Wenn weggelassen: analysiert ALLE Dateien in data-dir."
    )
    p.add_argument("--sheet", default=None, help="Excel Sheet-Name oder Index (default: erstes Sheet)")

    p.add_argument("--filter-col", required=True, help="Spalte, die gefiltert wird (z.B. f)")
    p.add_argument("--filter-val", required=True, help="Wert für Filter (z.B. leer | nichtleer | 'XYZ')")

    p.add_argument(
        "--analyze-cols",
        required=True,
        nargs="+",
        help="Spalten, die analysiert werden sollen (Reihenfolge ist wichtig für Kombinationen!)",
    )

    p.add_argument("--top-values", type=int, default=30, help="Top-N Werte pro Spalte im Report (default: 30)")
    p.add_argument("--top-combos", type=int, default=10, help="Top-N Kombinationen pro Stufe (default: 10)")
    p.add_argument("--min-group-size", type=int, default=1, help="Minimale Gruppengröße (default: 1)")

    p.add_argument("--out-dir", default="output/report.md", help="Output Markdown Pfad (default: output/<inbound_file>_report.md)")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    data_dir = Path(args.data_dir)
    out_dir = Path(args.out_dir)
    loader = DataLoader(data_dir=data_dir)

    files = loader.resolve_files(args.files)

    for path in files:
        df = loader.load_path(path, sheet=args.sheet)

        # Spalten auflösen (unterstützt echten Header ODER Excel-Buchstaben)
        resolved_filter_col = resolve_col(df.columns, args.filter_col)
        resolved_analyze_cols = [resolve_col(df.columns, c) for c in args.analyze_cols]

        filter_spec = FilterSpec(column=resolved_filter_col, value=args.filter_val)

        analyzer = Analyzer(df)
        result = analyzer.run(
            filter_spec=filter_spec,
            analyze_cols=resolved_analyze_cols,
            top_values_per_col=args.top_values,
            combo_top_n=args.top_combos,
            min_group_size=args.min_group_size,
        )

        # Output-Dateiname pro Input
        report_name = f"{path.stem}_inbound_report.md"
        out_path = out_dir / report_name

        writer = MarkdownReportWriter(output_path=out_path)
        writer.write(result, source_file=str(path))

        print(f"Report geschrieben: {out_path.resolve()}")


if __name__ == "__main__":
    main()
