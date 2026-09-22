"""
Azure Function (Python v2 model, HTTP trigger) that Power Automate calls.

This rebuilds the KANTARLMRB-style workbook from the three PulsePlus reports
(replacing the manual assembly step) and uploads the result to a Databricks
Unity Catalog volume. Power Automate itself only needs to watch the delivery
folder, collect the three files, and call this endpoint - the header-block
detection, row join, and workbook rebuild happen here.

Reuses pulseplus_kantar_pipeline.py (the script from the mapping doc)
unchanged - deploy that file alongside this one in the Function App.

Request contract (what the Power Automate flow POSTs):
{
  "files": [
    {"filename": "PulsePlusReport0.xlsx", "content_base64": "..."},
    {"filename": "PulsePlusReport1.xlsx", "content_base64": "..."},
    {"filename": "PulsePlusReport2.xlsx", "content_base64": "..."}
  ]
}

Response:
{
  "status": "ok" | "needs_review" | "error",
  "period": "2026 Jul To 2026 Jul",
  "incomplete_rows": 0,
  "universe_spread_flags": 0,
  "volume_path": "/Volumes/hemas/market_research/pulseplus_kantar/KANTARLMRB_2026_Jul_To_2026_Jul.xlsx",
  "needs_review_path": null
}

Required Function App settings (Configuration -> Application settings; use
Key Vault references for the secret rather than plain text):
  DATABRICKS_HOST   e.g. https://adb-1234567890123456.7.azuredatabricks.net
  DATABRICKS_TOKEN  a Databricks PAT or Azure AD service-principal token with
                     WRITE VOLUME on the target path
  VOLUME_ROOT       e.g. /Volumes/hemas/market_research/pulseplus_kantar
"""

import base64
import json
import logging
import os
import tempfile
from pathlib import Path

import azure.functions as func
import pandas as pd
import requests

import pulseplus_kantar_pipeline as pipeline

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

log = logging.getLogger("databricks_upload_function")


def _databricks_put_file(local_path: Path, remote_path: str) -> None:
    """Uploads one file to a Unity Catalog volume via the Databricks Files
    REST API (PUT /api/2.0/fs/files/{path}). `databricks-sdk`'s
    WorkspaceClient().files.upload(...) is a fine drop-in replacement if
    you'd rather use the SDK instead of a raw request."""
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]
    url = f"{host}/api/2.0/fs/files{remote_path}"
    with open(local_path, "rb") as fh:
        resp = requests.put(url, headers={"Authorization": f"Bearer {token}"}, data=fh, timeout=120)
    resp.raise_for_status()


@app.route(route="pulseplus-kantar", methods=["POST"])
def pulseplus_kantar(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(json.dumps({"status": "error", "message": "invalid JSON body"}),
                                  status_code=400, mimetype="application/json")

    files = body.get("files", [])
    if len(files) < 3:
        return func.HttpResponse(
            json.dumps({"status": "error", "message": f"expected 3 PulsePlus report files, got {len(files)}"}),
            status_code=400, mimetype="application/json",
        )

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)

        # 1) Decode each incoming file back to .xlsx on local disk.
        for f in files:
            (tmp_dir / f["filename"]).write_bytes(base64.b64decode(f["content_base64"]))

        # 2) Read + classify + melt each report (same logic as the standalone script).
        try:
            loaded = pipeline.load_local_files(tmp_dir)
        except ValueError as exc:
            return func.HttpResponse(json.dumps({"status": "error", "message": str(exc)}),
                                      status_code=400, mimetype="application/json")

        if not loaded:
            return func.HttpResponse(json.dumps({"status": "error", "message": "no readable .xlsx files"}),
                                      status_code=400, mimetype="application/json")

        tidy_frames = [pipeline.to_tidy(t) for t in loaded]
        all_tidy = pd.concat(tidy_frames, ignore_index=True)

        # 3) Sanity-check the shared base before trusting the rebuild.
        spread_flags = pipeline.validate_universe_consistency(all_tidy)

        # 4) Rebuild the Kantar-shaped workbook and write it locally.
        wb, incomplete, period = pipeline.assemble_kantar_workbook(tidy_frames)

        out_dir = tmp_dir / "out"
        out_dir.mkdir()
        out_name = f"KANTARLMRB_{period.replace(' ', '_') or 'unknown_period'}.xlsx"
        local_path = out_dir / out_name
        wb.save(local_path)

        # 5) Upload the rebuilt workbook to the volume.
        volume_root = os.environ["VOLUME_ROOT"].rstrip("/")
        remote_path = f"{volume_root}/{out_name}"
        _databricks_put_file(local_path, remote_path)

        review_uploaded = False
        if not incomplete.empty:
            review_path = out_dir / "needs_review.csv"
            incomplete.to_csv(review_path, index=False)
            remote_review_path = f"{volume_root}/needs_review_{period.replace(' ', '_')}.csv"
            _databricks_put_file(review_path, remote_review_path)
            review_uploaded = True

        status = "needs_review" if (review_uploaded or not spread_flags.empty) else "ok"
        result = {
            "status": status,
            "period": period,
            "incomplete_rows": int(len(incomplete)),
            "universe_spread_flags": int(len(spread_flags)),
            "volume_path": remote_path,
            "needs_review_path": (remote_review_path if review_uploaded else None),
        }
        log.info("Rebuilt Kantar workbook for period %s: %s", period, result)
        return func.HttpResponse(json.dumps(result), status_code=200, mimetype="application/json")