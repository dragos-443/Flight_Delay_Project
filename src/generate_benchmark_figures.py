from __future__ import annotations

import csv
import html
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BENCHMARKS_DIR = ROOT / "outputs" / "benchmarks"
FIGURES_DIR = ROOT / "reports" / "figures"
SUMMARY_PATH = BENCHMARKS_DIR / "benchmark_summary.csv"
SCALABILITY_SUMMARY_PATH = BENCHMARKS_DIR / "scalability_summary.csv"

ANALYSIS_LABELS = {
    "analysis_3_1": "Analisi 3.1",
    "analysis_3_2": "Analisi 3.2",
}
TECHNOLOGY_LABELS = {
    "spark_sql": "Spark SQL",
    "spark_core": "Spark Core",
    "hive": "Hive",
}
TECHNOLOGY_COLORS = {
    "spark_sql": "#2563eb",
    "spark_core": "#dc2626",
    "hive": "#16a34a",
}
RUN_SIZE_ORDER = ["100k", "500k", "half", "full"]
SCALE_ORDER = ["1x", "2x", "4x"]
COMBINED_ORDER = RUN_SIZE_ORDER + SCALE_ORDER
TECHNOLOGY_ORDER = ["spark_sql", "spark_core", "hive"]
ANALYSIS_ORDER = ["analysis_3_1", "analysis_3_2"]


def read_timings(timing_glob: str, run_size_order: list[str]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in sorted(BENCHMARKS_DIR.glob(timing_glob)):
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                rows.append(
                    {
                        "analysis": row["analysis"],
                        "technology": row["technology"],
                        "run_size": row["run_size"],
                        "input_path": row["input_path"],
                        "output_path": row["output_path"],
                        "execution_time_seconds": f"{float(row['execution_time_seconds']):.3f}",
                        "output_rows": row["output_rows"],
                        "run_timestamp": row["run_timestamp"],
                    }
                )

    return sorted(
        rows,
        key=lambda row: (
            ANALYSIS_ORDER.index(row["analysis"])
            if row["analysis"] in ANALYSIS_ORDER
            else len(ANALYSIS_ORDER),
            run_size_order.index(row["run_size"])
            if row["run_size"] in run_size_order
            else len(run_size_order),
            TECHNOLOGY_ORDER.index(row["technology"])
            if row["technology"] in TECHNOLOGY_ORDER
            else len(TECHNOLOGY_ORDER),
        ),
    )


def write_summary(rows: list[dict[str, str]], output_path: Path) -> None:
    BENCHMARKS_DIR.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "analysis",
        "technology",
        "run_size",
        "execution_time_seconds",
        "output_rows",
        "input_path",
        "output_path",
        "run_timestamp",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def svg_text(x: float, y: float, text: str, size: int = 13, anchor: str = "start", weight: str = "400") -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-family="Arial, sans-serif" '
        f'font-size="{size}" font-weight="{weight}" text-anchor="{anchor}" '
        f'fill="#111827">{html.escape(text)}</text>'
    )


def format_seconds(value: float) -> str:
    return f"{value:.1f}s"


def draw_line_chart(
    title: str,
    subtitle: str,
    groups: list[str],
    series: list[str],
    values: dict[tuple[str, str], float],
    output_path: Path,
    group_label_map: dict[str, str] | None = None,
    series_label_map: dict[str, str] | None = None,
    x_axis_label: str = "Dimensione input",
) -> None:
    width = 980
    height = 600
    margin_left = 82
    margin_right = 42
    margin_top = 96
    margin_bottom = 128
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    max_value = max(values.values()) if values else 1.0
    y_max = max_value * 1.18
    x_step = plot_width / max(len(groups) - 1, 1)

    def point_for(group_index: int, value: float) -> tuple[float, float]:
        x = margin_left + group_index * x_step if len(groups) > 1 else margin_left + plot_width / 2
        y = margin_top + plot_height - (value / y_max) * plot_height
        return x, y

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        svg_text(32, 38, title, size=24, weight="700"),
        svg_text(32, 64, subtitle, size=14),
    ]

    for index in range(6):
        ratio = index / 5
        y = margin_top + plot_height - ratio * plot_height
        value = y_max * ratio
        parts.append(f'<line x1="{margin_left}" y1="{y:.1f}" x2="{width - margin_right}" y2="{y:.1f}" stroke="#e5e7eb"/>')
        parts.append(svg_text(margin_left - 10, y + 4, format_seconds(value), size=12, anchor="end"))

    parts.append(
        f'<line x1="{margin_left}" y1="{margin_top + plot_height}" '
        f'x2="{width - margin_right}" y2="{margin_top + plot_height}" stroke="#9ca3af"/>'
    )
    parts.append(
        f'<line x1="{margin_left}" y1="{margin_top}" '
        f'x2="{margin_left}" y2="{margin_top + plot_height}" stroke="#9ca3af"/>'
    )

    for group_index, group in enumerate(groups):
        x = margin_left + group_index * x_step if len(groups) > 1 else margin_left + plot_width / 2
        label = group_label_map.get(group, group) if group_label_map else group
        parts.append(f'<line x1="{x:.1f}" y1="{margin_top + plot_height}" x2="{x:.1f}" y2="{margin_top + plot_height + 6}" stroke="#9ca3af"/>')
        parts.append(svg_text(x, margin_top + plot_height + 28, label, size=13, anchor="middle"))

    for series_index, item in enumerate(series):
        color = TECHNOLOGY_COLORS.get(item, "#64748b")
        points: list[tuple[float, float, float]] = []
        for group_index, group in enumerate(groups):
            value = values.get((group, item))
            if value is None:
                continue
            x, y = point_for(group_index, value)
            points.append((x, y, value))

        if not points:
            continue

        path_data = " ".join(
            f"{'M' if index == 0 else 'L'} {x:.1f} {y:.1f}"
            for index, (x, y, _) in enumerate(points)
        )
        parts.append(f'<path d="{path_data}" fill="none" stroke="{color}" stroke-width="3" stroke-linejoin="round" stroke-linecap="round"/>')

        for x, y, value in points:
            parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="#ffffff" stroke="{color}" stroke-width="3"/>')
            parts.append(svg_text(x, y - 12, format_seconds(value), size=11, anchor="middle"))

    legend_x = margin_left
    legend_y = height - 34
    for index, item in enumerate(series):
        x = legend_x + index * 150
        color = TECHNOLOGY_COLORS.get(item, "#64748b")
        label = series_label_map.get(item, item) if series_label_map else item
        parts.append(f'<line x1="{x}" y1="{legend_y - 7}" x2="{x + 16}" y2="{legend_y - 7}" stroke="{color}" stroke-width="3" stroke-linecap="round"/>')
        parts.append(f'<circle cx="{x + 8}" cy="{legend_y - 7}" r="4" fill="#ffffff" stroke="{color}" stroke-width="2"/>')
        parts.append(svg_text(x + 22, legend_y, label, size=13))

    parts.append(svg_text(width / 2, height - 64, x_axis_label, size=13, anchor="middle", weight="700"))
    parts.append(
        '<text x="18" y="300" font-family="Arial, sans-serif" font-size="13" '
        'font-weight="700" text-anchor="middle" fill="#111827" transform="rotate(-90 18 300)">Tempo esecuzione</text>'
    )
    parts.append("</svg>")

    output_path.write_text("\n".join(parts), encoding="utf-8")


def generate_figures(
    rows: list[dict[str, str]],
    groups: list[str],
    output_prefix: str,
    subtitle: str,
    x_axis_label: str,
) -> None:
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    for analysis in ANALYSIS_ORDER:
        filtered = [row for row in rows if row["analysis"] == analysis]
        values = {
            (row["run_size"], row["technology"]): float(row["execution_time_seconds"])
            for row in filtered
        }
        draw_line_chart(
            title=f"{ANALYSIS_LABELS[analysis]} - tempi di esecuzione",
            subtitle=subtitle,
            groups=groups,
            series=TECHNOLOGY_ORDER,
            values=values,
            output_path=FIGURES_DIR / f"{output_prefix}_{analysis}.svg",
            series_label_map=TECHNOLOGY_LABELS,
            x_axis_label=x_axis_label,
        )


def generate_combined_figures(
    benchmark_rows: list[dict[str, str]],
    scalability_rows: list[dict[str, str]],
) -> None:
    combined_rows = benchmark_rows + scalability_rows
    generate_figures(
        rows=combined_rows,
        groups=COMBINED_ORDER,
        output_prefix="combined",
        subtitle="Confronto unificato tra benchmark sample e dataset scalati",
        x_axis_label="Sample benchmark e fattore scala",
    )


def main() -> None:
    benchmark_rows = read_timings("analysis_3_*/**/timings.csv", RUN_SIZE_ORDER)
    if not benchmark_rows:
        raise SystemExit(f"Nessun timing CSV benchmark trovato in {BENCHMARKS_DIR}")

    write_summary(benchmark_rows, SUMMARY_PATH)
    generate_figures(
        rows=benchmark_rows,
        groups=RUN_SIZE_ORDER,
        output_prefix="benchmark",
        subtitle="Confronto tra Spark SQL, Spark Core e Hive sui run size disponibili",
        x_axis_label="Dimensione input",
    )
    print(f"Benchmark consolidati: {SUMMARY_PATH}")

    scalability_rows = read_timings("scalability/analysis_3_*/**/timings.csv", SCALE_ORDER)
    if scalability_rows:
        write_summary(scalability_rows, SCALABILITY_SUMMARY_PATH)
        generate_figures(
            rows=scalability_rows,
            groups=SCALE_ORDER,
            output_prefix="scalability",
            subtitle="Confronto tra Spark SQL, Spark Core e Hive sui dataset scalati",
            x_axis_label="Fattore scala",
        )
        print(f"Benchmark scalabilita consolidati: {SCALABILITY_SUMMARY_PATH}")
        generate_combined_figures(benchmark_rows, scalability_rows)

    print(f"Grafici generati in: {FIGURES_DIR}")


if __name__ == "__main__":
    main()
