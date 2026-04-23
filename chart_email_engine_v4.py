"""
╔══════════════════════════════════════════════════════════════════════╗
║         BigQuery Chart Engine  —  v4                                ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  AUTH    : Service Account JSON key file                             ║
║                                                                      ║
║  READS   : email_body    — html_body with {{PLACEHOLDERS}}           ║
║            chart_config  — one row per chart placeholder             ║
║                                                                      ║
║  WRITES  : email_output  — final rendered HTML pushed back to BQ     ║
║                                                                      ║
║  email_output schema:                                                ║
║    email_id        STRING   — from email_body                        ║
║    sender_email    STRING   — from email_body                        ║
║    recipient_email STRING   — from email_body                        ║
║    subject         STRING   — from email_body                        ║
║    final_html      STRING   — html_body with charts embedded         ║
║    charts_injected INT64    — how many charts were replaced          ║
║    status          STRING   — SUCCESS | PARTIAL | FAILED             ║
║    error_message   STRING   — blank on SUCCESS, reason on FAILED     ║
║    processed_at    TIMESTAMP — UTC timestamp of this run             ║
║                                                                      ║
║  WRITE MODES (configurable):                                         ║
║    APPEND   — always add new rows (keeps history across runs)        ║
║    TRUNCATE — wipe table then insert (latest run only)               ║
║                                                                      ║
║  FLOW:                                                               ║
║    1. Load chart_config → config_map                                 ║
║    2. Load email_body rows                                           ║
║    3. For each email:                                                ║
║         a. Scan html_body for {{PLACEHOLDERS}}                       ║
║         b. Build SELECT from bq_table + columns + filters            ║
║         c. Run query → DataFrame → render chart → base64 PNG         ║
║         d. Replace placeholder with <img> block                      ║
║         e. Wrap in Outlook-safe envelope                             ║
║         f. Save HTML file locally                                    ║
║         g. INSERT row into email_output                              ║
║    4. Print summary                                                  ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝

QUICK START
───────────
  pip install matplotlib pandas google-cloud-bigquery pyarrow

  1. Edit the CONFIG block below.
  2. Run create_tables.sql in BigQuery console to create all 3 tables.
  3. python chart_email_engine_v4.py
"""

import io
import os
import re
import json
import base64
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker


# ══════════════════════════════════════════════════════════════════════
# ▶  CONFIG  —  edit before running
# ══════════════════════════════════════════════════════════════════════

SERVICE_ACCOUNT_KEY_FILE = "service_account_key.json"   # path to JSON key
PROJECT_ID               = "your-gcp-project-id"

EMAIL_BODY_VIEW     = "your_project.your_dataset.email_body"
CHART_CONFIG_TABLE  = "your_project.your_dataset.chart_config"
EMAIL_OUTPUT_TABLE  = "your_project.your_dataset.email_output"  # ← new

# APPEND  → keeps history of every run (recommended for auditing)
# TRUNCATE → overwrites table with latest run only
WRITE_MODE = "APPEND"

OUTPUT_DIR = "output_emails"   # local folder for .html files

USE_MOCK = True   # ← set False for live BigQuery


# ══════════════════════════════════════════════════════════════════════
# 1.  BIGQUERY CLIENT  —  Service Account Auth
# ══════════════════════════════════════════════════════════════════════

class BigQueryClient:
    """
    Handles both READ (query) and WRITE (insert to email_output) operations.

    Service Account IAM roles required:
      - roles/bigquery.dataViewer   (read email_body, chart_config, chart data)
      - roles/bigquery.dataEditor   (insert rows into email_output)
      - roles/bigquery.jobUser      (run query and load jobs)
    """

    def __init__(self):
        self.client = None

        if USE_MOCK:
            print("  [MODE] Mock — skipping BigQuery connection.")
            return

        key_path = SERVICE_ACCOUNT_KEY_FILE
        if not os.path.isfile(key_path):
            raise FileNotFoundError(
                f"Service account key not found: '{key_path}'\n"
                "Download it from: GCP Console → IAM & Admin → "
                "Service Accounts → Keys → Add Key → JSON."
            )

        from google.oauth2 import service_account
        from google.cloud import bigquery

        credentials = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        self.client = bigquery.Client(
            project=PROJECT_ID,
            credentials=credentials,
        )

        with open(key_path) as f:
            meta = json.load(f)
        print(f"  [AUTH] Service account : {meta.get('client_email', '?')}")
        print(f"  [AUTH] Project          : {meta.get('project_id', '?')}")

    # ── READ ──────────────────────────────────────────────────────────
    def query(self, sql: str) -> pd.DataFrame:
        if USE_MOCK:
            return _mock_data(sql)
        return self.client.query(sql).to_dataframe()

    # ── WRITE ─────────────────────────────────────────────────────────
    def insert_rows(self, table_id: str, rows: list[dict],
                    write_mode: str = "APPEND") -> None:
        """
        Insert a list of dicts into a BigQuery table.

        Args:
            table_id   : Full table path  e.g. project.dataset.table
            rows       : List of dicts — keys must match table column names
            write_mode : "APPEND" or "TRUNCATE"
        """
        if USE_MOCK:
            print(f"  [MOCK WRITE] Would insert {len(rows)} row(s) → {table_id}")
            for r in rows:
                # Print everything except the full HTML (too long)
                preview = {k: (v[:80] + "…" if isinstance(v, str) and len(v) > 80 else v)
                           for k, v in r.items()}
                print(f"    {preview}")
            return

        from google.cloud import bigquery

        df = pd.DataFrame(rows)

        # Map Python types to BQ schema
        schema = [
            bigquery.SchemaField("email_id",        "STRING"),
            bigquery.SchemaField("sender_email",     "STRING"),
            bigquery.SchemaField("recipient_email",  "STRING"),
            bigquery.SchemaField("subject",          "STRING"),
            bigquery.SchemaField("final_html",       "STRING"),
            bigquery.SchemaField("charts_injected",  "INTEGER"),
            bigquery.SchemaField("status",           "STRING"),
            bigquery.SchemaField("error_message",    "STRING"),
            bigquery.SchemaField("processed_at",     "TIMESTAMP"),
        ]

        disposition_map = {
            "APPEND":   bigquery.WriteDisposition.WRITE_APPEND,
            "TRUNCATE": bigquery.WriteDisposition.WRITE_TRUNCATE,
        }
        disposition = disposition_map.get(
            write_mode.upper(), bigquery.WriteDisposition.WRITE_APPEND
        )

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=disposition,
        )

        job = self.client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()   # wait for completion
        print(f"  [BQ WRITE] {len(rows)} row(s) → {table_id}  "
              f"(mode={write_mode})")


# ══════════════════════════════════════════════════════════════════════
# 2.  MOCK DATA  (used when USE_MOCK = True)
# ══════════════════════════════════════════════════════════════════════

def _mock_data(sql: str) -> pd.DataFrame:
    u = sql.upper()

    if "EMAIL_BODY" in u:
        return pd.DataFrame([{
            "email_id":        "E001",
            "sender_email":    "reports@company.com",
            "subject":         "Monthly Performance Report — April 2026",
            "recipient_email": "leadership@company.com",
            "html_body": (
                "<h2 style='font-family:Arial;color:#1A2332;'>"
                "Monthly Performance Report</h2>"
                "<p style='font-family:Arial;'>Dear Team,</p>"
                "<p style='font-family:Arial;'>Please find this month's "
                "key metrics below.</p>"
                "<p style='font-family:Arial;font-weight:bold;'>"
                "Revenue Trend</p>"
                "{{MONTHLY_REVENUE_CHART}}"
                "<p style='font-family:Arial;font-weight:bold;'>"
                "Regional Split</p>"
                "{{REGION_PIE_CHART}}"
                "<p style='font-family:Arial;font-weight:bold;'>"
                "Category Performance</p>"
                "{{CATEGORY_BAR_CHART}}"
                "<p style='font-family:Arial;font-weight:bold;'>"
                "Market Share</p>"
                "{{MARKET_DONUT_CHART}}"
                "<p style='font-family:Arial;'>Regards,<br/>"
                "Analytics Team</p>"
            ),
        }])

    if "CHART_CONFIG" in u:
        return pd.DataFrame([
            {
                "variable_name": "MONTHLY_REVENUE_CHART",
                "chart_type":    "line",
                "bq_table":      "my_project.analytics.monthly_sales",
                "filters":       "year = 2026",
                "x_column":      "Month",
                "y_columns":     "Revenue,Target",
                "legend":        "yes",
                "title":         "Monthly Revenue vs Target",
                "color_theme":   "blue",
                "show_values":   "no",
                "sort_order":    "none",
                "width_px":      600,
                "height_px":     310,
            },
            {
                "variable_name": "REGION_PIE_CHART",
                "chart_type":    "pie",
                "bq_table":      "my_project.analytics.region_revenue",
                "filters":       "",
                "x_column":      "Region",
                "y_columns":     "Revenue",
                "legend":        "yes",
                "title":         "Revenue by Region",
                "color_theme":   "rainbow",
                "show_values":   "yes",
                "sort_order":    "desc",
                "width_px":      500,
                "height_px":     350,
            },
            {
                "variable_name": "CATEGORY_BAR_CHART",
                "chart_type":    "bar",
                "bq_table":      "my_project.analytics.category_quarterly",
                "filters":       "fiscal_year = 2026",
                "x_column":      "Category",
                "y_columns":     "Q1,Q2,Q3,Q4",
                "legend":        "yes",
                "title":         "Quarterly Sales by Category",
                "color_theme":   "teal",
                "show_values":   "yes",
                "sort_order":    "none",
                "width_px":      600,
                "height_px":     330,
            },
            {
                "variable_name": "MARKET_DONUT_CHART",
                "chart_type":    "donut",
                "bq_table":      "my_project.analytics.market_share",
                "filters":       "report_date = '2026-04-01'",
                "x_column":      "Segment",
                "y_columns":     "Share",
                "legend":        "yes",
                "title":         "Market Share by Segment",
                "color_theme":   "purple",
                "show_values":   "yes",
                "sort_order":    "desc",
                "width_px":      500,
                "height_px":     350,
            },
        ])

    if "MONTHLY_SALES" in u:
        return pd.DataFrame({
            "Month":   ["Jan","Feb","Mar","Apr","May","Jun",
                        "Jul","Aug","Sep","Oct","Nov","Dec"],
            "Revenue": [42000,47500,53200,49800,61000,67300,
                        72100,68900,74500,81200,78600,91000],
            "Target":  [45000,48000,52000,55000,60000,65000,
                        70000,72000,75000,80000,82000,88000],
        })
    if "REGION_REVENUE" in u:
        return pd.DataFrame({
            "Region":  ["North","South","East","West","Central"],
            "Revenue": [31200, 24500, 18900, 27800, 15600],
        })
    if "CATEGORY_QUARTERLY" in u:
        return pd.DataFrame({
            "Category": ["Electronics","Clothing","Food","Books","Sports"],
            "Q1": [12000, 8500, 6200, 3400, 5100],
            "Q2": [14500, 9200, 7800, 4100, 6300],
            "Q3": [13200,11000, 8100, 3800, 7200],
            "Q4": [18900,14500, 9300, 4600, 8800],
        })
    if "MARKET_SHARE" in u:
        return pd.DataFrame({
            "Segment": ["Enterprise","SMB","Startup","Consumer","Government"],
            "Share":   [38.5, 24.2, 15.8, 12.1, 9.4],
        })

    return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════
# 3.  SQL BUILDER
# ══════════════════════════════════════════════════════════════════════

def build_select(cfg: dict) -> str:
    """
    SELECT {x_column}, {y_col1}, {y_col2}
    FROM   `{bq_table}`
    WHERE  {filters}    ← omitted when filters is blank
    """
    cols    = ", ".join([cfg["x_column"]] + cfg["y_columns"])
    sql     = f"SELECT {cols}\nFROM   `{cfg['bq_table']}`"
    filters = cfg.get("filters", "").strip()
    if filters:
        sql += f"\nWHERE  {filters}"
    return sql


# ══════════════════════════════════════════════════════════════════════
# 4.  CONFIG PARSER
# ══════════════════════════════════════════════════════════════════════

def parse_config(row: dict) -> dict:
    y_raw  = str(row.get("y_columns", ""))
    y_cols = [c.strip() for c in y_raw.split(",") if c.strip()]
    return {
        "variable_name": str(row.get("variable_name", "")).strip(),
        "chart_type":    str(row.get("chart_type",    "bar")).lower().strip(),
        "bq_table":      str(row.get("bq_table",      "")).strip(),
        "filters":       str(row.get("filters",       "")).strip(),
        "x_column":      str(row.get("x_column",      "")).strip(),
        "y_columns":     y_cols,
        "legend":        str(row.get("legend",        "yes")).lower() == "yes",
        "title":         str(row.get("title",         "")).strip(),
        "color_theme":   str(row.get("color_theme",   "default")).lower().strip(),
        "show_values":   str(row.get("show_values",   "no")).lower()  == "yes",
        "sort_order":    str(row.get("sort_order",    "none")).lower().strip(),
        "width_px":      int(row.get("width_px",  580)),
        "height_px":     int(row.get("height_px", 300)),
    }


# ══════════════════════════════════════════════════════════════════════
# 5.  COLOUR THEMES & CHART STYLE
# ══════════════════════════════════════════════════════════════════════

_THEMES = {
    "default": ["#1E6FD9","#F0A500","#2CB67D","#E05252","#9B59B6","#0097A7"],
    "blue":    ["#1E6FD9","#5B9BD5","#2B9AF3","#0D4EA6","#8AB4E8","#C7DEFF"],
    "green":   ["#2CB67D","#3ECFA0","#1A8A5A","#6DD9A8","#0D5C3A","#A8F0D0"],
    "orange":  ["#F0A500","#F7C542","#D47F00","#FFD97D","#A85C00","#FFE9A0"],
    "red":     ["#E05252","#F07070","#B83232","#F9A8A8","#7A1A1A","#FDD0D0"],
    "purple":  ["#9B59B6","#C390D4","#7D3C98","#DDA0F0","#4A235A","#EDD6F8"],
    "teal":    ["#0097A7","#4DD0E1","#006064","#80DEEA","#004D40","#B2EBF2"],
    "rainbow": ["#1E6FD9","#2CB67D","#F0A500","#E05252","#9B59B6","#0097A7"],
}
_BG   = "#FFFFFF"
_GRID = "#EBEBEB"
_TEXT = "#1A2332"
_DPI  = 150

def _clrs(theme: str, n: int) -> list:
    p = _THEMES.get(theme, _THEMES["default"])
    return [p[i % len(p)] for i in range(n)]

def _inches(w, h):
    return (w / _DPI, h / _DPI)

def _ax_style(ax):
    ax.set_facecolor(_BG)
    ax.yaxis.grid(True, color=_GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    for s in ["top","right","left"]:
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color(_GRID)
    ax.tick_params(colors=_TEXT, labelsize=9)

def _to_b64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=_DPI,
                bbox_inches="tight", facecolor=_BG)
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode()
    plt.close(fig)
    return f"data:image/png;base64,{b64}"


# ══════════════════════════════════════════════════════════════════════
# 6.  CHART RENDERERS
# ══════════════════════════════════════════════════════════════════════

def _line(df: pd.DataFrame, cfg: dict) -> str:
    fig, ax = plt.subplots(figsize=_inches(cfg["width_px"], cfg["height_px"]))
    fig.patch.set_facecolor(_BG)
    _ax_style(ax)
    x    = range(len(df))
    cols = cfg["y_columns"]
    clrs = _clrs(cfg["color_theme"], len(cols))
    for i, col in enumerate(cols):
        if col not in df.columns:
            continue
        c = clrs[i]
        ax.plot(x, df[col], color=c, linewidth=2.5,
                marker="o", markersize=5, label=col, zorder=3)
        ax.fill_between(x, df[col], alpha=0.07, color=c)
        if cfg["show_values"]:
            for xi, yi in zip(x, df[col]):
                ax.annotate(f"{yi:,.0f}", (xi, yi),
                            textcoords="offset points", xytext=(0,7),
                            ha="center", fontsize=7, color=c)
    ax.set_xticks(list(x))
    ax.set_xticklabels(df[cfg["x_column"]].tolist(),
                       fontsize=9, color=_TEXT)
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda v,_: f"{v:,.0f}"))
    if cfg["legend"]:
        ax.legend(fontsize=8.5, frameon=False, loc="upper left")
    ax.set_title(cfg["title"], fontsize=12, fontweight="bold",
                 color=_TEXT, pad=10)
    fig.tight_layout()
    return _to_b64(fig)


def _bar(df: pd.DataFrame, cfg: dict) -> str:
    fig, ax = plt.subplots(figsize=_inches(cfg["width_px"], cfg["height_px"]))
    fig.patch.set_facecolor(_BG)
    _ax_style(ax)
    cols = cfg["y_columns"]
    clrs = _clrs(cfg["color_theme"], len(cols))
    x    = np.arange(len(df))
    n    = len(cols)
    w    = 0.72 / n
    for i, col in enumerate(cols):
        if col not in df.columns:
            continue
        offsets = x + (i - n/2 + 0.5) * w
        bars = ax.bar(offsets, df[col], width=w*0.9,
                      color=clrs[i], label=col, zorder=3)
        if cfg["show_values"]:
            for bar in bars:
                h = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2,
                        h + df[col].max()*0.015,
                        f"{h:,.0f}", ha="center", va="bottom",
                        fontsize=7, color=clrs[i])
    ax.set_xticks(x)
    ax.set_xticklabels(df[cfg["x_column"]].tolist(), fontsize=9,
                       color=_TEXT, rotation=15 if len(df)>5 else 0)
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda v,_: f"{v:,.0f}"))
    if cfg["legend"] and n > 1:
        ax.legend(fontsize=8.5, frameon=False)
    ax.set_title(cfg["title"], fontsize=12, fontweight="bold",
                 color=_TEXT, pad=10)
    fig.tight_layout()
    return _to_b64(fig)


def _pie_donut(df: pd.DataFrame, cfg: dict, donut: bool) -> str:
    fig, ax = plt.subplots(figsize=_inches(cfg["width_px"], cfg["height_px"]))
    fig.patch.set_facecolor(_BG)
    val_col = cfg["y_columns"][0]
    lbl_col = cfg["x_column"]
    if cfg["sort_order"] == "desc":
        df = df.sort_values(val_col, ascending=False).reset_index(drop=True)
    elif cfg["sort_order"] == "asc":
        df = df.sort_values(val_col, ascending=True).reset_index(drop=True)
    labels  = df[lbl_col].tolist()
    values  = df[val_col].tolist()
    clrs    = _clrs(cfg["color_theme"], len(labels))
    explode = [0.03] * len(labels)
    wkw     = {"linewidth": 1.8, "edgecolor": "white"}
    if donut:
        wkw["width"] = 0.48
    pct = "%1.1f%%" if cfg["show_values"] else ""
    wedges, _, ats = ax.pie(
        values, labels=None, colors=clrs, explode=explode,
        autopct=pct, pctdistance=0.80, startangle=140,
        wedgeprops=wkw,
    )
    for at in ats:
        at.set_fontsize(8); at.set_color("white"); at.set_fontweight("bold")
    if donut:
        total = sum(values)
        fmt   = f"{total:,.1f}" if isinstance(total, float) else f"{int(total):,}"
        ax.text(0,  0.10, fmt,     ha="center", va="center",
                fontsize=14, fontweight="bold", color=_TEXT)
        ax.text(0, -0.16, "Total", ha="center", va="center",
                fontsize=8.5, color="#6B7A8D")
    if cfg["legend"]:
        ax.legend(wedges, labels, loc="lower center",
                  bbox_to_anchor=(0.5, -0.10),
                  ncol=min(3, len(labels)),
                  fontsize=8.5, frameon=False)
    ax.set_title(cfg["title"], fontsize=12, fontweight="bold",
                 color=_TEXT, pad=10)
    fig.tight_layout()
    return _to_b64(fig)

def _pie(df, cfg):   return _pie_donut(df, cfg, donut=False)
def _donut(df, cfg): return _pie_donut(df, cfg, donut=True)


_RENDERERS = {"line": _line, "bar": _bar, "pie": _pie, "donut": _donut}

def render_chart(df: pd.DataFrame, cfg: dict) -> Optional[str]:
    fn = _RENDERERS.get(cfg["chart_type"])
    if fn is None:
        print(f"    [WARN] Unknown chart_type='{cfg['chart_type']}' — skipping.")
        return None
    if df.empty:
        print(f"    [WARN] Empty data for '{cfg['variable_name']}' — skipping.")
        return None
    return fn(df, cfg)


# ══════════════════════════════════════════════════════════════════════
# 7.  PLACEHOLDER SCANNER
# ══════════════════════════════════════════════════════════════════════

_RE = re.compile(r"\{\{([A-Z0-9_]+)\}\}")

def find_placeholders(html: str) -> list:
    seen, out = set(), []
    for m in _RE.finditer(html):
        n = m.group(1)
        if n not in seen:
            seen.add(n); out.append(n)
    return out


# ══════════════════════════════════════════════════════════════════════
# 8.  HTML HELPERS
# ══════════════════════════════════════════════════════════════════════

def _img_block(b64: str, cfg: dict) -> str:
    return (
        f'<div style="margin:12px 0 20px 0;">'
        f'<!--[if mso]><table><tr><td><![endif]-->'
        f'<img src="{b64}" alt="{cfg["title"]}" width="{cfg["width_px"]}"'
        f' style="display:block;max-width:100%;height:auto;'
        f'border:1px solid #E0E4EA;border-radius:4px;"/>'
        f'<!--[if mso]></td></tr></table><![endif]-->'
        f'</div>'
    )

def _envelope(body: str, row: dict) -> str:
    subject = row.get("subject", "Report")
    return f"""<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:o="urn:schemas-microsoft-com:office:office">
<head>
  <meta charset="UTF-8"/>
  <meta http-equiv="X-UA-Compatible" content="IE=edge"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <!--[if mso]>
  <xml><o:OfficeDocumentSettings>
    <o:AllowPNG/><o:PixelsPerInch>96</o:PixelsPerInch>
  </o:OfficeDocumentSettings></xml>
  <![endif]-->
  <title>{subject}</title>
</head>
<body style="margin:0;padding:24px;background:#F4F6F9;
             font-family:Arial,sans-serif;color:{_TEXT};">
  <table width="660" cellpadding="0" cellspacing="0" border="0" align="center"
         style="max-width:660px;width:100%;background:#fff;border-radius:6px;
                padding:28px 32px;border:1px solid #E0E4EA;">
    <tr><td>{body}</td></tr>
    <tr><td style="padding-top:16px;font-size:11px;color:#9AA3AF;
                   border-top:1px solid #E8ECF0;">
      Charts are embedded as images for compatibility across all email clients.
    </td></tr>
  </table>
</body>
</html>"""


# ══════════════════════════════════════════════════════════════════════
# 9.  OUTPUT ROW BUILDER
#     Constructs the dict that goes into email_output
# ══════════════════════════════════════════════════════════════════════

def build_output_row(email_row: dict,
                     final_html: str,
                     charts_injected: int,
                     total_placeholders: int,
                     error_message: str = "") -> dict:
    """
    Builds one row for email_output table.

    status logic:
      SUCCESS  → all placeholders were replaced with charts
      PARTIAL  → some placeholders replaced, some skipped
      FAILED   → error_message is set (exception was caught)
    """
    if error_message:
        status = "FAILED"
    elif charts_injected == 0:
        status = "FAILED"
        error_message = "No charts were injected."
    elif charts_injected < total_placeholders:
        status = "PARTIAL"
        error_message = (
            f"{charts_injected}/{total_placeholders} chart(s) injected. "
            "Some placeholders had no matching chart_config row or empty data."
        )
    else:
        status = "SUCCESS"

    return {
        "email_id":        str(email_row.get("email_id", "")),
        "sender_email":    str(email_row.get("sender_email", "")),
        "recipient_email": str(email_row.get("recipient_email", "")),
        "subject":         str(email_row.get("subject", "")),
        "final_html":      final_html,
        "charts_injected": int(charts_injected),
        "status":          status,
        "error_message":   error_message,
        "processed_at":    datetime.now(timezone.utc).isoformat(),
    }


# ══════════════════════════════════════════════════════════════════════
# 10. MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════

def process_emails(bq: BigQueryClient) -> list:
    """
    End-to-end pipeline. Returns list of output row dicts.

    For each email:
      ① Scan html_body for {{PLACEHOLDERS}}
      ② Look up each in chart_config
      ③ Build SELECT → run → render → base64 → inject
      ④ Wrap in email envelope
      ⑤ Save .html locally
      ⑥ Push row to email_output in BigQuery
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ── Load all chart configs once ────────────────────────────────
    print("► Loading chart_config...")
    cfg_df     = bq.query(f"SELECT * FROM `{CHART_CONFIG_TABLE}`")
    config_map = {}
    for _, r in cfg_df.iterrows():
        cfg = parse_config(r.to_dict())
        config_map[cfg["variable_name"]] = cfg
    print(f"  {len(config_map)} config(s) : {list(config_map.keys())}")

    # ── Load emails ────────────────────────────────────────────────
    print("\n► Loading email_body...")
    emails_df = bq.query(f"SELECT * FROM `{EMAIL_BODY_VIEW}`")
    print(f"  {len(emails_df)} email(s) found.\n")

    output_rows = []   # will be bulk-inserted once all emails are processed

    for _, email_row in emails_df.iterrows():
        email_id  = email_row["email_id"]
        html_body = str(email_row["html_body"])
        error_msg = ""

        print(f"{'─'*62}")
        print(f"  email_id  : {email_id}")
        print(f"  subject   : {email_row.get('subject','')}")
        print(f"  sender    : {email_row.get('sender_email','')}")
        print(f"  recipient : {email_row.get('recipient_email','')}")

        placeholders = find_placeholders(html_body)
        total        = len(placeholders)
        injected     = 0
        print(f"  Placeholders ({total}): {placeholders}")

        try:
            for var in placeholders:
                token = f"{{{{{var}}}}}"

                if var not in config_map:
                    print(f"\n  [SKIP] '{var}' — not in chart_config.")
                    continue

                cfg = config_map[var]
                print(f"\n  ► '{var}'  [{cfg['chart_type']}]")

                sql = build_select(cfg)
                print(f"    SQL ↓\n      " + sql.replace("\n", "\n      "))

                df = bq.query(sql)
                if df.empty:
                    print(f"    [WARN] 0 rows — skipping.")
                    continue
                print(f"    Rows: {len(df)}  Cols: {list(df.columns)}")

                b64 = render_chart(df, cfg)
                if b64 is None:
                    continue

                html_body = html_body.replace(token, _img_block(b64, cfg))
                injected += 1
                print(f"    ✓ Injected (base64: {len(b64):,} chars)")

        except Exception as exc:
            error_msg = str(exc)
            print(f"\n  [ERROR] {error_msg}")

        # ── Wrap in Outlook envelope ───────────────────────────────
        final_html = _envelope(html_body, email_row.to_dict())

        # ── Save local HTML file ───────────────────────────────────
        out_path = os.path.join(OUTPUT_DIR, f"email_{email_id}.html")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(final_html)
        print(f"\n  ✓ HTML saved → {out_path}")

        # ── Build output row ───────────────────────────────────────
        row = build_output_row(
            email_row     = email_row.to_dict(),
            final_html    = final_html,
            charts_injected    = injected,
            total_placeholders = total,
            error_message = error_msg,
        )
        output_rows.append(row)
        print(f"  ✓ Output row built  "
              f"[status={row['status']}, "
              f"charts={row['charts_injected']}/{total}]")

    # ── Push ALL rows to BigQuery email_output in one batch ────────
    if output_rows:
        print(f"\n{'─'*62}")
        print(f"► Writing {len(output_rows)} row(s) → {EMAIL_OUTPUT_TABLE}")
        print(f"  Write mode : {WRITE_MODE}")
        bq.insert_rows(EMAIL_OUTPUT_TABLE, output_rows, write_mode=WRITE_MODE)

    return output_rows


# ══════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 62)
    print("  BigQuery Chart Engine  v4  —  with email_output write-back")
    print("=" * 62)
    print(f"  Key file    : {SERVICE_ACCOUNT_KEY_FILE}")
    print(f"  Project     : {PROJECT_ID}")
    print(f"  Mode        : {'MOCK' if USE_MOCK else 'LIVE BigQuery'}")
    print(f"  Write mode  : {WRITE_MODE}")
    print(f"  Output table: {EMAIL_OUTPUT_TABLE}")
    print()

    bq   = BigQueryClient()
    rows = process_emails(bq)

    print(f"\n{'='*62}")
    print(f"  Done — {len(rows)} email(s) processed and written to BQ.")
    for r in rows:
        status_icon = {"SUCCESS":"✓","PARTIAL":"⚠","FAILED":"✗"}.get(r["status"],"?")
        print(f"  {status_icon} [{r['email_id']}]  "
              f"status={r['status']}  "
              f"charts={r['charts_injected']}  "
              f"to={r['recipient_email']}")
        if r["error_message"]:
            print(f"      msg: {r['error_message']}")
    print(f"  Local files : {os.path.abspath(OUTPUT_DIR)}/")
    print("=" * 62)
