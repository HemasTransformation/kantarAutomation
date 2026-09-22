"""
pulseplus_kantar_pipeline.py

Rebuilds the "KANTARLMRB" consolidated workbook automatically from the three
monthly PulsePlus crosstab exports, replacing the manual step where someone
currently assembles that file by hand. The rebuilt workbook is then uploaded
to a Databricks Unity Catalog volume.

Background / mapping (see the companion doc for the full write-up):

    A KANTARLMRB-style tab is column 1 (row label) followed by four
    19-market blocks, and each block is exactly one PulsePlus report's data:

        cols  2-20   "No of Households" block  <- PulsePlusReport0, cols 2-20
        cols 21-39   "HHP%" block               <- PulsePlusReport1, cols 2-20
                                                    (rows there are suffixed
                                                     "- Down %"; stripped here)
        cols 40-58   "Volume" block              <- PulsePlusReport2, cols 2-20
        cols 59-77   "Value" block               <- PulsePlusReport2, cols 21-39

    All three PulsePlus reports come from the same export tool at the same
    time, so (unlike the old hand-built Kantar file, which drifted out of
    sync with the category tree between vintages) their ~2,000 row labels
    match 1:1 in identical order - confirmed against a real sample. So this
    rebuild is a straight positional/label join across three files, no
    crosswalk needed. ROW_LABEL_MISMATCH_ALERT_THRESHOLD below exists purely
    as a safety net in case Kantar ever changes that.

    Every workbook has 6 tabs: Total, SEC_ A, SEC_ B, SEC_ C, SEC_ D, SEC_ E.
    In the rebuilt workbook these are labelled to match the original
    KANTARLMRB convention: "Total", "SEC: A", "SEC: B", ...

Usage:
    python pulseplus_kantar_pipeline.py --input-dir ./downloads --output-dir ./out

This script is intentionally split into small, independently-testable
functions so it can run either as a Databricks Job task (reading/writing
straight to a mounted /Volumes/... path), as the guts of an Azure Function
that Power Automate calls, or standalone against local files for testing.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path

import openpyxl
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pulseplus_kantar_pipeline")

TABS = ["Total", "SEC_ A", "SEC_ B", "SEC_ C", "SEC_ D", "SEC_ E"]

# How each PulsePlus tab name should be labelled in the rebuilt workbook,
# matching the convention of the original hand-built KANTARLMRB file.
KANTAR_TAB_LABEL = {
    "Total": "Total",
    "SEC_ A": "SEC: A",
    "SEC_ B": "SEC: B",
    "SEC_ C": "SEC: C",
    "SEC_ D": "SEC: D",
    "SEC_ E": "SEC: E",
}

# The four measure blocks, in the column order the rebuilt workbook uses,
# each 19 columns wide (All Island + 18 regions), with its row-1 header text.
MEASURE_BLOCKS = [
    ("HH_000s", "No of Households"),
    ("HHP_pct", "HHP%"),
    ("Volume_000", "Volume"),
    ("Value_Rs_mn", "Value"),
]

ROW_LABEL_MISMATCH_ALERT_THRESHOLD = 0.01  # 1% of rows not matching across the 3 reports


# --------------------------------------------------------------------------- #
# Stage 1: detect file kind and load raw grids
# --------------------------------------------------------------------------- #

@dataclass
class LoadedTab:
    file_kind: str  # "pulse_hh" | "pulse_hhp" | "pulse_volvalue"
    tab: str
    period: str
    grid: list[list]  # raw rows, 1 row = 1 list of cell values


def _sniff_period(ws) -> str:
    """PulsePlus files carry a 'Process Period' row near the top (e.g.
    '2026 Jul To 2026 Jul'), which becomes the partition/folder name for the
    rebuilt output in the volume."""
    for row in ws.iter_rows(min_row=1, max_row=10, max_col=2, values_only=True):
        if row[0] == "Process Period" and row[1]:
            return str(row[1])
    return ""


def _classify_tab(ws) -> str:
    row9 = ws.cell(row=9, column=1).value, ws.cell(row=9, column=2).value

    if row9[0] == "Measure" and row9[1]:
        measure_text = str(row9[1])
        if "Volumes in" in measure_text:
            return "pulse_volvalue"
        if "Households in" in measure_text:
            # Report0 vs Report1 both say "Households in 000s" in this header
            # cell; the real distinction only shows up in row labels further
            # down ("- Down %" suffix means it's the penetration-percent
            # report). Sample a data row to disambiguate.
            for r in range(15, 60):
                label = ws.cell(row=r, column=1).value
                # Skip the fixed base rows ("Universe (000s)", "TG Base
                # (000s)", "UNwtd Base", "Product Total (000s)") - only a
                # "[CODE] ..." category row reliably carries the suffix.
                if label and str(label).startswith("["):
                    return "pulse_hhp" if label.rstrip().endswith("- Down %") else "pulse_hh"
    raise ValueError(
        "Could not classify worksheet as a PulsePlus report - is this actually "
        "a KANTARLMRB-style consolidated file? That shape is no longer expected "
        "as an input; this pipeline now *produces* that shape as output."
    )


def detect_and_load(path: Path) -> list[LoadedTab]:
    """Opens a PulsePlus workbook and returns one LoadedTab per sheet."""
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    out = []
    for tab in TABS:
        if tab not in wb.sheetnames:
            log.warning("%s: tab %r missing, skipping", path.name, tab)
            continue
        ws = wb[tab]
        kind = _classify_tab(ws)
        period = _sniff_period(ws)
        grid = [list(row) for row in ws.iter_rows(values_only=True)]
        out.append(LoadedTab(file_kind=kind, tab=tab, period=period, grid=grid))
        log.info("%s [%s]: classified as %s", path.name, tab, kind)
    return out


def load_local_files(input_dir: Path) -> list[LoadedTab]:
    all_tabs = []
    for path in sorted(input_dir.glob("*.xlsx")):
        all_tabs.extend(detect_and_load(path))
    return all_tabs


# --------------------------------------------------------------------------- #
# Stage 2: melt each loaded tab into tidy long form
# --------------------------------------------------------------------------- #

def _header_row_index(grid: list[list]) -> int:
    """Row index (0-based) holding the market names ('ALL ISLAND', ...)."""
    for i, row in enumerate(grid[:20]):
        if row and row[0] is None and row[1] == "ALL ISLAND":
            return i
    raise ValueError("Could not find the market header row")


def _melt_block(grid: list[list], header_row: int, col_start: int, col_end: int,
                 measure: str, tab: str, period: str) -> pd.DataFrame:
    """col_start/col_end are 1-indexed inclusive column numbers within `grid`."""
    markets = grid[header_row][col_start - 1: col_end]
    records = []
    for row in grid[header_row + 1:]:
        if not row or row[0] is None:
            continue
        label = str(row[0]).strip()
        values = row[col_start - 1: col_end]
        for market, value in zip(markets, values):
            if value is None:
                continue
            records.append(
                {"period": period, "tab": tab, "product_label": label,
                 "market": market, "measure": measure, "value": value}
            )
    return pd.DataFrame.from_records(records)


def to_tidy(loaded: LoadedTab) -> pd.DataFrame:
    header_row = _header_row_index(loaded.grid)

    if loaded.file_kind == "pulse_hh":
        return _melt_block(loaded.grid, header_row, 2, 20, "HH_000s", loaded.tab, loaded.period)
    if loaded.file_kind == "pulse_hhp":
        df = _melt_block(loaded.grid, header_row, 2, 20, "HHP_pct", loaded.tab, loaded.period)
        df["product_label"] = df["product_label"].str.replace(r"\s*-\s*Down %$", "", regex=True)
        return df
    if loaded.file_kind == "pulse_volvalue":
        vol = _melt_block(loaded.grid, header_row, 2, 20, "Volume_000", loaded.tab, loaded.period)
        val = _melt_block(loaded.grid, header_row, 21, 39, "Value_Rs_mn", loaded.tab, loaded.period)
        return pd.concat([vol, val], ignore_index=True)
    raise ValueError(f"Unhandled file kind: {loaded.file_kind}")


# --------------------------------------------------------------------------- #
# Stage 3: join the three reports and rebuild the Kantar-shaped workbook
# --------------------------------------------------------------------------- #

def combine_pulse_reports(tidy_frames: list[pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Joins the tidy Report0/Report1/Report2 frames on (period, tab,
    product_label, market). Returns (complete, incomplete) - `incomplete`
    holds any row missing one of the four measures, which given the three
    reports come from the same export should be empty or near-empty; treat a
    non-trivial `incomplete` as a signal something about the export changed.
    """
    combined = pd.concat(tidy_frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["period", "tab", "product_label", "market", "measure"])

    pivot = combined.pivot_table(
        index=["period", "tab", "product_label", "market"],
        columns="measure", values="value", aggfunc="first",
    )
    expected = {m for m, _ in MEASURE_BLOCKS}
    complete_mask = pivot[list(expected)].notna().all(axis=1) if set(pivot.columns) >= expected else pd.Series(False, index=pivot.index)

    keyed = combined.set_index(["period", "tab", "product_label", "market"])
    complete = keyed.loc[keyed.index.isin(pivot.index[complete_mask])].reset_index()
    incomplete = keyed.loc[keyed.index.isin(pivot.index[~complete_mask])].reset_index()

    mismatch_rate = (~complete_mask).sum() / max(len(pivot.index), 1)
    if mismatch_rate > ROW_LABEL_MISMATCH_ALERT_THRESHOLD:
        log.warning(
            "%.1f%% of rows don't have all four measures across the three "
            "reports (expected ~0%% - they normally share an identical row "
            "list). Check whether Kantar changed the export/category tree "
            "this month before trusting the rebuilt workbook.",
            mismatch_rate * 100,
        )
    return complete, incomplete


def assemble_kantar_workbook(tidy_frames: list[pd.DataFrame]) -> tuple[openpyxl.Workbook, pd.DataFrame, str]:
    """Builds an openpyxl Workbook in the same 6-tab, 77-column layout as the
    original hand-built KANTARLMRB file, from the three PulsePlus reports'
    tidy data. Returns (workbook, incomplete_rows, period)."""
    complete, incomplete = combine_pulse_reports(tidy_frames)
    period = complete["period"].iloc[0] if not complete.empty else ""

    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    for tab in TABS:
        tab_df = complete[complete["tab"] == tab]
        if tab_df.empty:
            log.warning("No data for tab %r - skipping in rebuilt workbook", tab)
            continue
        ws = wb.create_sheet(tab)

        # Row order + market order exactly as they first appear in Report0's
        # export, so the rebuilt file reads the same way the manual one did.
        row_labels = list(dict.fromkeys(tab_df["product_label"]))
        markets = list(dict.fromkeys(tab_df["market"]))

        # Row 1: tab label + one header per measure block, spaced 19 apart.
        ws.cell(row=1, column=1, value=KANTAR_TAB_LABEL.get(tab, tab))
        for i, (_, header_text) in enumerate(MEASURE_BLOCKS):
            ws.cell(row=1, column=2 + i * 19, value=header_text)

        # Row 2: market names, repeated once per block.
        for i in range(len(MEASURE_BLOCKS)):
            for j, market in enumerate(markets):
                ws.cell(row=2, column=2 + i * 19 + j, value=market)

        pivot = tab_df.pivot_table(index="product_label", columns=["measure", "market"], values="value", aggfunc="first")

        for r, label in enumerate(row_labels, start=3):
            ws.cell(row=r, column=1, value=label)
            for i, (measure, _) in enumerate(MEASURE_BLOCKS):
                for j, market in enumerate(markets):
                    value = pivot.loc[label, (measure, market)] if (measure, market) in pivot.columns else None
                    if pd.notna(value):
                        ws.cell(row=r, column=2 + i * 19 + j, value=value)

        log.info("Rebuilt tab %r: %d rows x %d cols", tab, len(row_labels) + 2, 1 + 19 * len(MEASURE_BLOCKS))

    return wb, incomplete, period


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #

def validate_universe_consistency(tidy: pd.DataFrame, tolerance: float = 0.01) -> pd.DataFrame:
    """Flags (period, tab, market) combos where 'Universe (000s)' / 'TG Base
    (000s)' disagree by more than `tolerance` (fractional) across the three
    reports - these should be identical since they share one panel wave."""
    base_rows = tidy[tidy["product_label"].isin(["Universe (000s)", "TG Base (000s)"])]
    pivot = base_rows.pivot_table(
        index=["period", "tab", "market", "product_label"], columns="measure", values="value", aggfunc="first"
    )
    numeric = pivot.select_dtypes("number")
    if numeric.empty:
        return pd.DataFrame()
    spread = (numeric.max(axis=1) - numeric.min(axis=1)) / numeric.max(axis=1).replace(0, pd.NA)
    return spread[spread > tolerance].reset_index(name="relative_spread")


# --------------------------------------------------------------------------- #
# I/O: SharePoint pull and Databricks write (stubs with the intended shape)
# --------------------------------------------------------------------------- #

def pull_new_files_from_sharepoint(site_id: str, drive_id: str, folder_path: str,
                                    processed_log_path: Path, dest_dir: Path) -> list[Path]:
    """Downloads the three PulsePlus report files from a SharePoint/OneDrive
    folder via Microsoft Graph. Only needed if this pipeline pulls the files
    itself (e.g. running as a scheduled Databricks Job); if Power Automate
    hands the files to it instead (e.g. via an Azure Function payload), this
    isn't used at all - see the companion doc's Power Automate flow section.
    """
    raise NotImplementedError(
        "Wire up msal + Microsoft Graph here if this pipeline needs to pull "
        "the files itself rather than receiving them from Power Automate."
    )


def write_workbook_to_volume(wb: openpyxl.Workbook, volume_path: Path, filename: str) -> Path:
    """When running as a Databricks Job, a Unity Catalog volume is mounted at
    /Volumes/<catalog>/<schema>/<volume>/... and this is a normal file write.
    When running outside Databricks (e.g. in an Azure Function), replace this
    with a PUT to the Databricks Files REST API instead."""
    volume_path.mkdir(parents=True, exist_ok=True)
    out_path = volume_path / filename
    wb.save(out_path)
    log.info("Wrote rebuilt workbook to %s", out_path)
    return out_path


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #

def run(input_dir: Path, output_dir: Path) -> None:
    loaded = load_local_files(input_dir)
    if not loaded:
        raise SystemExit(f"No .xlsx files found in {input_dir}")

    tidy_frames = [to_tidy(t) for t in loaded]
    all_tidy = pd.concat(tidy_frames, ignore_index=True)

    spread_flags = validate_universe_consistency(all_tidy)
    if not spread_flags.empty:
        log.warning("Universe/base spread exceeds tolerance for %d group(s):\n%s",
                    len(spread_flags), spread_flags.to_string(index=False))

    wb, incomplete, period = assemble_kantar_workbook(tidy_frames)

    output_dir.mkdir(parents=True, exist_ok=True)
    out_name = f"KANTARLMRB_{period.replace(' ', '_') or 'unknown_period'}.xlsx"
    wb.save(output_dir / out_name)
    if not incomplete.empty:
        incomplete.to_csv(output_dir / "needs_review.csv", index=False)
        log.warning("%d rows need manual review - see needs_review.csv", len(incomplete))

    log.info("Done. Rebuilt %s for period %r.", out_name, period)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True,
                         help="Folder with the three PulsePlusReport .xlsx files")
    parser.add_argument("--output-dir", type=Path, required=True,
                         help="Where to write the rebuilt workbook locally")
    args = parser.parse_args()
    run(args.input_dir, args.output_dir)


if __name__ == "__main__":
    main()
