from __future__ import annotations

"""
Rewritten version of the Sales & Marketing Action Center Streamlit app
with a modern dark‑mode UI. The business logic, data connections and
validation have been preserved from the original application. This
version injects custom CSS directly via ``st.markdown`` so no
external ``config.toml`` is required. The UI is organized into
semantic cards/containers and uses a gradient header. Radio buttons are
styled as horizontal chips and the primary action button uses the
brand green colour.
"""

import hashlib
import hmac
import io
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st
from zoneinfo import ZoneInfo

import dropbox
import gspread
from dropbox.exceptions import ApiError, AuthError
from dropbox.sharing import RequestedVisibility, SharedLinkSettings
from google.oauth2.service_account import Credentials

# Optional libs (Excel Export / AgGrid / Plotly)
try:
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter
    from openpyxl.utils.dataframe import dataframe_to_rows

    HAS_OPENPYXL = True
except Exception:
    HAS_OPENPYXL = False

try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

    HAS_AGGRID = True
except Exception:
    HAS_AGGRID = False

try:
    import plotly.express as px

    HAS_PLOTLY = True
except Exception:
    HAS_PLOTLY = False

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app_rewrite")

# Page config – set dark mode layout
st.set_page_config(page_title="Sales & Marketing Action Center", page_icon="🚀", layout="wide")

# -----------------------------------------------------------------------------
# Custom CSS – inserted at the top of the script. This stylesheet hides the
# default Streamlit header and footer, imports the Poppins font from Google
# Fonts and defines classes for the header container, cards, chip radios and
# primary buttons. Colours are inspired by the Mentari Sejuk logo: dark
# charcoal backgrounds, fresh green primary accents and teal/blue secondary
# accents. See documentation for Streamlit theming【368938812334752†L170-L209】.
custom_css = """
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;500;600;700&display=swap" rel="stylesheet">

<style>
    /* Hide Streamlit's default menu, header and footer */
    #MainMenu, header, footer {visibility: hidden;}

    /* Global font */
    html, body, [class*="css"] {
        font-family: 'Poppins', sans-serif;
    }

    /* App background and default text colours */
    .stApp {
        background-color: #1E1E1E;
        color: #FAFAFA;
    }

    /* Header container with a green‑to‑dark gradient */
    .header-container {
        background: linear-gradient(90deg, #006E3D, #002915);
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        margin-bottom: 24px;
    }
    .header-container h1 {
        color: #FAFAFA;
        margin: 0;
        font-weight: 600;
        font-size: 32px;
    }
    .header-container p {
        color: #CCCCCC;
        margin-top: 4px;
        font-size: 14px;
    }

    /* Card styling: slightly lighter dark background, rounded corners and shadow */
    .card {
        background-color: #262730;
        border-radius: 12px;
        padding: 20px 24px;
        margin-bottom: 24px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }

    /* Radio buttons as horizontal chips */
    div[role="radiogroup"] {
        flex-direction: row;
        gap: 10px;
    }
    div[role="radiogroup"] label > div:first-child {
        display: none;
    }
    input[type="radio"] + div {
        background-color: #1F7742;
        color: #FAFAFA;
        border-radius: 24px;
        padding: 8px 16px;
        font-size: 14px;
        transition: background-color 0.2s ease;
    }
    input[type="radio"][tabindex="0"] + div {
        background-color: #58A157;
        color: #FFFFFF;
    }
    input[type="radio"][tabindex="0"] + div p {
        color: #FFFFFF;
    }
    input[type="radio"] + div:hover {
        background-color: #29B765;
    }

    /* Primary button styling */
    button[kind="primary"] {
        background-color: #58A157;
        color: #FFFFFF;
        border-radius: 8px;
        border: none;
        width: 100%;
        padding: 10px 0;
        font-size: 16px;
        font-weight: 600;
        transition: background-color 0.2s ease;
    }
    button[kind="primary"]:hover {
        background-color: #32C978;
        color: #FFFFFF;
    }

    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: #1B1D23;
        color: #FAFAFA;
        border-right: 1px solid #333333;
    }

    /* Selected option in sidebar radio */
    [data-testid="stSidebar"] div[role="radiogroup"] > label[data-baseweb="radio"] > div[tabindex="0"] {
        background-color: #58A157;
        color: #FFFFFF;
    }

</style>
"""

st.markdown(custom_css, unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Timezone helpers
try:
    TZ_JKT = ZoneInfo("Asia/Jakarta")
except Exception:
    TZ_JKT = None  # fallback


def _now() -> datetime:
    return datetime.now(tz=TZ_JKT) if TZ_JKT else datetime.now()


def now_ts_str() -> str:
    """Consistent timestamp string for logs."""
    return _now().strftime("%d-%m-%Y %H:%M:%S")


def today_jkt() -> date:
    return _now().date()

# -----------------------------------------------------------------------------
# Constants and column names (unchanged from original). These variables control
# worksheet names, column headings and database configuration. Keeping these
# definitions intact ensures backend compatibility.
NAMA_GOOGLE_SHEET = "Laporan Kegiatan Harian"
FOLDER_DROPBOX = "/Laporan_Kegiatan_Harian"

# Sheet Names
SHEET_CONFIG_NAMA = "Config_Staf"
SHEET_TARGET_TEAM = "Target_Team_Checklist"
SHEET_TARGET_INDIVIDU = "Target_Individu_Checklist"
SHEET_CONFIG_TEAM = "Config_Team"
SHEET_CLOSING_DEAL = "Closing_Deal"
SHEET_PEMBAYARAN = "Pembayaran_DP"

# Column definitions for daily reports
COL_TIMESTAMP = "Timestamp"
COL_NAMA = "Nama"
COL_TEMPAT = "Tempat Dikunjungi"
COL_DESKRIPSI = "Deskripsi"
COL_LINK_FOTO = "Link Foto"
COL_LINK_SOSMED = "Link Sosmed"
COL_KESIMPULAN = "Kesimpulan"
COL_KENDALA = "Kendala"
COL_KENDALA_KLIEN = "Kendala Klien"
COL_PENDING = "Next Plan (Pending)"
COL_FEEDBACK = "Feedback Lead"
COL_INTEREST = "Interest (%)"
COL_NAMA_KLIEN = "Nama Klien"
COL_KONTAK_KLIEN = "No HP/WA"

NAMA_KOLOM_STANDAR = [
    COL_TIMESTAMP,
    COL_NAMA,
    COL_TEMPAT,
    COL_DESKRIPSI,
    COL_LINK_FOTO,
    COL_LINK_SOSMED,
    COL_KESIMPULAN,
    COL_KENDALA,
    COL_KENDALA_KLIEN,
    COL_PENDING,
    COL_FEEDBACK,
    COL_INTEREST,
    COL_NAMA_KLIEN,
    COL_KONTAK_KLIEN,
]

# Audit columns
COL_TS_UPDATE = "Timestamp Update (Log)"
COL_UPDATED_BY = "Updated By"

# Team config columns
TEAM_COL_NAMA_TEAM = "Nama Team"
TEAM_COL_POSISI = "Posisi"
TEAM_COL_ANGGOTA = "Nama Anggota"
TEAM_COLUMNS = [TEAM_COL_NAMA_TEAM, TEAM_COL_POSISI, TEAM_COL_ANGGOTA]

# Closing deal columns
COL_GROUP = "Nama Group"
COL_MARKETING = "Nama Marketing"
COL_TGL_EVENT = "Tanggal Event"
COL_BIDANG = "Bidang"
COL_NILAI_KONTRAK = "Nilai Kontrak"
CLOSING_COLUMNS = [COL_GROUP, COL_MARKETING, COL_TGL_EVENT, COL_BIDANG, COL_NILAI_KONTRAK]

# Checklist columns
TEAM_CHECKLIST_COLUMNS = [
    "Misi",
    "Tgl_Mulai",
    "Tgl_Selesai",
    "Status",
    "Bukti/Catatan",
    COL_TS_UPDATE,
    COL_UPDATED_BY,
]
INDIV_CHECKLIST_COLUMNS = [
    "Nama",
    "Target",
    "Tgl_Mulai",
    "Tgl_Selesai",
    "Status",
    "Bukti/Catatan",
    COL_TS_UPDATE,
    COL_UPDATED_BY,
]

# Pembayaran columns
COL_TS_BAYAR = "Timestamp Input"
COL_JENIS_BAYAR = "Jenis Pembayaran"
COL_NOMINAL_BAYAR = "Nominal Pembayaran"
COL_JATUH_TEMPO = "Batas Waktu Bayar"
COL_STATUS_BAYAR = "Status Pembayaran"
COL_BUKTI_BAYAR = "Bukti Pembayaran"
COL_CATATAN_BAYAR = "Catatan"

PAYMENT_COLUMNS = [
    COL_TS_BAYAR,
    COL_GROUP,
    COL_MARKETING,
    COL_TGL_EVENT,
    COL_JENIS_BAYAR,
    COL_NOMINAL_BAYAR,
    COL_JATUH_TEMPO,
    COL_STATUS_BAYAR,
    COL_BUKTI_BAYAR,
    COL_CATATAN_BAYAR,
    COL_TS_UPDATE,
    COL_UPDATED_BY,
]

# -----------------------------------------------------------------------------
# Helper functions for cleaning and normalising inputs
def safe_str(x: Any, default: str = "") -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return default
        s = str(x)
        if s.lower() in {"nan", "none"}:
            return default
        return s
    except Exception:
        return default


def normalize_bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    s = safe_str(x, "").strip().upper()
    return s == "TRUE"


def normalize_date(x: Any) -> Optional[date]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    try:
        dt = pd.to_datetime(x, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.date()
    except Exception:
        return None


def get_actor_fallback(default: str = "-") -> str:
    for k in ["pelapor_main", "sidebar_user", "payment_editor_name"]:
        if k in st.session_state and safe_str(st.session_state.get(k), "").strip():
            return safe_str(st.session_state.get(k)).strip()
    return default

# -----------------------------------------------------------------------------
# Password verification for admin
def verify_admin_password(pwd_input: str) -> bool:
    pwd_input = safe_str(pwd_input, "").strip()
    if not pwd_input:
        return False

    hash_secret = None
    try:
        hash_secret = st.secrets.get("password_admin_hash", None)
    except Exception:
        hash_secret = None

    if hash_secret and safe_str(hash_secret, "").strip():
        try:
            digest = hashlib.sha256(pwd_input.encode("utf-8")).hexdigest()
            return hmac.compare_digest(digest, safe_str(hash_secret, "").strip())
        except Exception:
            return False

    plain_secret = None
    try:
        plain_secret = st.secrets.get("password_admin", None)
    except Exception:
        plain_secret = None

    if plain_secret and safe_str(plain_secret, "").strip():
        return hmac.compare_digest(pwd_input, safe_str(plain_secret, "").strip())

    return False


def admin_secret_configured() -> bool:
    try:
        return bool(
            safe_str(st.secrets.get("password_admin_hash", ""), "").strip()
            or safe_str(st.secrets.get("password_admin", ""), "").strip()
        )
    except Exception:
        return False

# -----------------------------------------------------------------------------
# Connections (cache_resource for stability)
@dataclass
class Connections:
    spreadsheet: Optional[gspread.Spreadsheet]
    dbx: Optional[dropbox.Dropbox]
    gs_ok: bool
    dbx_ok: bool
    gs_error: str = ""
    dbx_error: str = ""


@st.cache_resource(ttl=3600, show_spinner=False)
def init_connections() -> Connections:
    spreadsheet = None
    dbx = None
    gs_ok = False
    dbx_ok = False
    gs_error = ""
    dbx_error = ""

    # Google Sheets
    try:
        if "gcp_service_account" in st.secrets:
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
            creds_dict = dict(st.secrets["gcp_service_account"])
            if "private_key" in creds_dict:
                creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            gc = gspread.authorize(creds)
            spreadsheet = gc.open(NAMA_GOOGLE_SHEET)
            gs_ok = True
        else:
            gs_error = "Kredensial gcp_service_account tidak ditemukan di secrets."
    except Exception as e:
        gs_error = str(e)

    # Dropbox
    try:
        if "dropbox" in st.secrets and "access_token" in st.secrets["dropbox"]:
            dbx = dropbox.Dropbox(st.secrets["dropbox"]["access_token"])
            dbx.users_get_current_account()
            dbx_ok = True
        else:
            dbx_error = "Token Dropbox tidak ditemukan di secrets."
    except AuthError:
        dbx_error = "Token Autentikasi Dropbox tidak valid."
    except Exception as e:
        dbx_error = str(e)

    return Connections(
        spreadsheet=spreadsheet,
        dbx=dbx,
        gs_ok=gs_ok,
        dbx_ok=dbx_ok,
        gs_error=gs_error,
        dbx_error=dbx_error,
    )


CONN = init_connections()
spreadsheet = CONN.spreadsheet
dbx = CONN.dbx
KONEKSI_GSHEET_BERHASIL = CONN.gs_ok
KONEKSI_DROPBOX_BERHASIL = CONN.dbx_ok

if not KONEKSI_GSHEET_BERHASIL:
    st.error(f"Database Error (Google Sheets): {CONN.gs_error or 'Unknown error'}")
    st.stop()

if not KONEKSI_DROPBOX_BERHASIL:
    st.warning("⚠️ Dropbox non-aktif. Fitur upload foto/bukti dimatikan.")

# -----------------------------------------------------------------------------
# Rupiah parser and formatter
def parse_rupiah_to_int(value: Any) -> Optional[int]:
    if value is None:
        return None

    if isinstance(value, (int, float)) and not pd.isna(value):
        try:
            return int(round(float(value)))
        except Exception:
            return None

    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "-", "null"}:
        return None

    s_lower = re.sub(r"\s+", "", s.lower())
    s_lower = s_lower.replace("idr", "").replace("rp", "")

    multiplier = 1
    if "miliar" in s_lower or "milyar" in s_lower:
        multiplier = 1_000_000_000
    elif "jt" in s_lower or "juta" in s_lower:
        multiplier = 1_000_000
    elif "rb" in s_lower or "ribu" in s_lower:
        multiplier = 1_000

    s_num = re.sub(r"(miliar|milyar|juta|jt|ribu|rb)", "", s_lower)
    s_num = re.sub(r"[^0-9.,]", "", s_num)
    if not s_num:
        return None

    def to_float_locale(num_str: str) -> float:
        if "." in num_str and "," in num_str:
            if num_str.rfind(",") > num_str.rfind("."):
                cleaned = num_str.replace(".", "").replace(",", ".")
            else:
                cleaned = num_str.replace(",", "")
            return float(cleaned)

        if "," in num_str:
            if num_str.count(",") > 1:
                return float(num_str.replace(",", ""))
            after = num_str.split(",")[1]
            if len(after) == 3:
                return float(num_str.replace(",", ""))
            return float(num_str.replace(",", "."))

        if "." in num_str:
            if num_str.count(".") > 1:
                return float(num_str.replace(".", ""))
            after = num_str.split(".")[1]
            if len(after) == 3:
                return float(num_str.replace(".", ""))
            return float(num_str)

        return float(num_str)

    try:
        base = to_float_locale(s_num)
    except Exception:
        digits = re.sub(r"\D", "", s_num)
        return int(digits) if digits else None

    if multiplier != 1:
        if base >= multiplier:
            return int(round(base))
        return int(round(base * multiplier))

    return int(round(base))


def format_rupiah_display(amount: Any) -> str:
    try:
        if amount is None or pd.isna(amount):
            return ""
        n = int(amount)
        return "Rp " + f"{n:,}".replace(",", ".")
    except Exception:
        return str(amount)

# -----------------------------------------------------------------------------
# Audit log helpers (Pembayaran)
def parse_payment_log_lines(log_text: str) -> List[str]:
    log_text = safe_str(log_text, "").strip()
    if not log_text:
        return []

    raw_lines = [ln.rstrip() for ln in log_text.splitlines() if ln.strip()]
    out: List[str] = []

    for ln in raw_lines:
        mnum = re.match(r"^\s*\d+\.\s*(.*)$", ln)
        if mnum:
            ln = mnum.group(1).rstrip()

        m = re.match(r"^\[(.*?)\]\s*\((.*?)\)\s*(.*)$", ln)
        if m:
            ts, actor, rest = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
            prefix = f"[{ts}] ({actor})"
            if rest:
                parts = [p.strip() for p in rest.split(";") if p.strip()]
                if parts:
                    out.append(f"{prefix} {parts[0]}")
                    for p in parts[1:]:
                        out.append(f" {p}")
                else:
                    out.append(prefix)
            else:
                out.append(prefix)
        else:
            out.append(ln)

    return out


def build_numbered_log(lines: Sequence[str]) -> str:
    lines2 = [str(l).rstrip() for l in (lines or []) if safe_str(l, "").strip()]
    return "\n".join([f"{i}. {line}" for i, line in enumerate(lines2, 1)]).strip()


def _fmt_payment_val_for_log(col_name: str, v: Any) -> str:
    if col_name == COL_NOMINAL_BAYAR:
        x = parse_rupiah_to_int(v)
        return format_rupiah_display(x) if x is not None else "-"
    if col_name == COL_STATUS_BAYAR:
        return "✅ Dibayar" if normalize_bool(v) else "⏳ Belum"
    if col_name in {COL_JATUH_TEMPO, COL_TGL_EVENT}:
        d = normalize_date(v)
        return d.strftime("%Y-%m-%d") if d else "-"
    s = safe_str(v, "-").replace("\n", " ").strip()
    return s if s else "-"


def append_payment_ts_update(existing_log: str, ts: str, actor: str, changes: Sequence[str]) -> str:
    lines = parse_payment_log_lines(existing_log)
    changes2 = [safe_str(c, "").strip() for c in (changes or []) if safe_str(c, "").strip()]
    if not changes2:
        return build_numbered_log(lines)

    actor = safe_str(actor, "-").strip() or "-"
    ts = safe_str(ts, now_ts_str()).strip() or now_ts_str()

    lines.append(f"[{ts}] ({actor}) {changes2[0]}")
    for c in changes2[1:]:
        lines.append(f" {c}")

    return build_numbered_log(lines)

# -----------------------------------------------------------------------------
# UI display helpers for payment
def payment_df_for_display(df: pd.DataFrame) -> pd.DataFrame:
    dfv = df.copy()
    if dfv is None or dfv.empty:
        return dfv
    if COL_NOMINAL_BAYAR in dfv.columns:
        dfv[COL_NOMINAL_BAYAR] = dfv[COL_NOMINAL_BAYAR].apply(
            lambda x: "" if x is None or pd.isna(x) else format_rupiah_display(x)
        )
    return dfv


def on_change_pay_nominal():
    raw = st.session_state.get("pay_nominal", "")
    val = parse_rupiah_to_int(raw)
    if val is not None:
        st.session_state["pay_nominal"] = format_rupiah_display(val)


def reset_payment_form_state():
    keys = [
        "pay_group",
        "pay_marketing",
        "pay_event_date",
        "pay_jenis_opt",
        "pay_jenis_custom",
        "pay_nominal",
        "pay_due_date",
        "pay_status",
        "pay_note",
        "pay_file",
    ]
    for k in keys:
        try:
            if k == "pay_event_date":
                st.session_state[k] = today_jkt()
            elif k == "pay_due_date":
                st.session_state[k] = today_jkt() + timedelta(days=7)
            elif k == "pay_jenis_opt":
                st.session_state[k] = "Down Payment (DP)"
            elif k == "pay_status":
                st.session_state[k] = False
            else:
                st.session_state[k] = ""
        except Exception:
            pass

# -----------------------------------------------------------------------------
# Excel export helper
def df_to_excel_bytes(
    df: pd.DataFrame,
    sheet_name: str = "Sheet1",
    col_widths: Optional[Dict[str, int]] = None,
    wrap_cols: Optional[Sequence[str]] = None,
    right_align_cols: Optional[Sequence[str]] = None,
    number_format_cols: Optional[Dict[str, str]] = None,
) -> Optional[bytes]:
    if not HAS_OPENPYXL:
        return None

    df_export = df.copy().where(pd.notna(df), None)

    output = io.BytesIO()
    wb = Workbook()
    ws = wb.active
    ws.title = (sheet_name or "Sheet1")[:31]

    for r in dataframe_to_rows(df_export, index=False, header=True):
        ws.append(r)

    header_fill = PatternFill("solid", fgColor="E6E6E6")
    header_font = Font(bold=True)

    for cell in ws[1]:
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    ws.freeze_panes = "A2"

    wrap_cols = set(wrap_cols or [])
    right_align_cols = set(right_align_cols or [])
    number_format_cols = dict(number_format_cols or {})
    col_widths = dict(col_widths or {})

    cols = list(df_export.columns)

    for i, col_name in enumerate(cols, 1):
        col_letter = get_column_letter(i)

        if col_name in col_widths:
            ws.column_dimensions[col_letter].width = col_widths[col_name]
        else:
            max_len = len(str(col_name))
            try:
                for v in df_export[col_name]:
                    v_str = "" if v is None else str(v)
                    max_len = max(max_len, len(v_str))
            except Exception:
                pass
            ws.column_dimensions[col_letter].width = min(max(10, max_len + 2), 60)

        for cell in ws[col_letter][1:]:
            wrap = col_name in wrap_cols
            horiz = "right" if col_name in right_align_cols else "left"
            cell.alignment = Alignment(vertical="top", horizontal=horiz, wrap_text=wrap)
            if col_name in number_format_cols:
                cell.number_format = number_format_cols[col_name]

    wb.save(output)
    return output.getvalue()

# -----------------------------------------------------------------------------
# Google Sheets formatting helpers
def _build_currency_number_format_rupiah() -> Dict[str, Any]:
    return {"type": "CURRENCY", "pattern": '"Rp" #,##0'}


def auto_format_sheet(worksheet: gspread.Worksheet) -> None:
    """Auto-format sheet columns and headers."""
    try:
        sheet_id = worksheet.id
        all_values = worksheet.get_all_values()
        if not all_values:
            return

        headers = all_values[0]
        data_row_count = len(all_values)
        formatting_row_count = max(worksheet.row_count, data_row_count)

        requests: List[Dict[str, Any]] = []
        default_body_format = {"verticalAlignment": "TOP", "wrapStrategy": "CLIP"}

        requests.append(
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": formatting_row_count},
                    "cell": {"userEnteredFormat": default_body_format},
                    "fields": "userEnteredFormat(verticalAlignment,wrapStrategy)",
                }
            }
        )

        long_text_cols = {
            "Misi",
            "Target",
            "Deskripsi",
            "Bukti/Catatan",
            "Link Foto",
            "Link Sosmed",
            "Tempat Dikunjungi",
            "Kesimpulan",
            "Kendala",
            "Next Plan (Pending)",
            "Feedback Lead",
            COL_KENDALA_KLIEN,
            COL_NAMA_KLIEN,
            TEAM_COL_NAMA_TEAM,
            TEAM_COL_POSISI,
            TEAM_COL_ANGGOTA,
            COL_GROUP,
            COL_MARKETING,
            COL_BIDANG,
            COL_JENIS_BAYAR,
            COL_BUKTI_BAYAR,
            COL_CATATAN_BAYAR,
            COL_TS_UPDATE,
        }

        for i, col_name in enumerate(headers):
            col_index = i
            cell_format_override: Dict[str, Any] = {}
            width = 100

            if col_name in long_text_cols:
                width = 360 if col_name == COL_TS_UPDATE else 300
                cell_format_override["wrapStrategy"] = "WRAP"
            elif col_name in {"Tgl_Mulai", "Tgl_Selesai", "Timestamp", COL_TGL_EVENT, COL_JATUH_TEMPO, COL_TS_BAYAR}:
                width = 160 if col_name in {"Timestamp", COL_TS_BAYAR} else 120
                cell_format_override["horizontalAlignment"] = "CENTER"
            elif col_name in {"Status", "Done?", COL_STATUS_BAYAR}:
                width = 130 if col_name == COL_STATUS_BAYAR else 80
                cell_format_override["horizontalAlignment"] = "CENTER"
            elif col_name == "Nama":
                width = 160
            elif col_name == COL_UPDATED_BY:
                width = 160
            elif col_name == COL_INTEREST:
                width = 140
                cell_format_override["horizontalAlignment"] = "CENTER"
            elif col_name == COL_KONTAK_KLIEN:
                width = 150
                cell_format_override["horizontalAlignment"] = "CENTER"
            elif col_name in {COL_NILAI_KONTRAK, COL_NOMINAL_BAYAR}:
                width = 180
                cell_format_override["horizontalAlignment"] = "RIGHT"
                cell_format_override["numberFormat"] = _build_currency_number_format_rupiah()

            requests.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": col_index,
                            "endIndex": col_index + 1,
                        },
                        "properties": {"pixelSize": width},
                        "fields": "pixelSize",
                    }
                }
            )

            if cell_format_override:
                fields = ",".join(cell_format_override.keys())
                requests.append(
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": 1,
                                "endRowIndex": formatting_row_count,
                                "startColumnIndex": col_index,
                                "endColumnIndex": col_index + 1,
                            },
                            "cell": {"userEnteredFormat": cell_format_override},
                            "fields": f"userEnteredFormat({fields})",
                        }
                    }
                )

        requests.append(
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {"bold": True},
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE",
                            "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
                            "wrapStrategy": "WRAP",
                        }
                    },
                    "fields": "userEnteredFormat(textFormat,horizontalAlignment,verticalAlignment,backgroundColor,wrapStrategy)",
                }
            }
        )

        requests.append(
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                    "fields": "gridProperties.frozenRowCount",
                }
            }
        )

        worksheet.spreadsheet.batch_update({"requests": requests})
    except Exception as e:
        log.warning("Format Error: %s", e)


def ensure_headers(worksheet: gspread.Worksheet, desired_headers: Sequence[str]) -> None:
    """Ensure worksheet has the desired headers in correct order."""
    try:
        if worksheet.col_count < len(desired_headers):
            worksheet.resize(cols=len(desired_headers))

        headers = worksheet.row_values(1)
        need_reset = (
            not headers
            or (len(headers) < len(desired_headers))
            or (headers[: len(desired_headers)] != list(desired_headers))
        )
        if need_reset:
            worksheet.update(range_name="A1", values=[list(desired_headers)], value_input_option="USER_ENTERED")
            auto_format_sheet(worksheet)
    except Exception as e:
        log.warning("Ensure Header Error: %s", e)


def get_ws(sheet_name: str, headers: Sequence[str], rows: int = 300) -> gspread.Worksheet:
    """Open or create worksheet and ensure headers."""
    try:
        ws = spreadsheet.worksheet(sheet_name)
        ensure_headers(ws, headers)
        return ws
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=sheet_name, rows=max(rows, 100), cols=len(headers))
        ws.append_row(list(headers), value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return ws


@st.cache_resource(ttl=60, show_spinner=False)
def get_or_create_worksheet(nama_worksheet: str) -> Optional[gspread.Worksheet]:
    """Sheet laporan harian per staf."""
    try:
        return get_ws(nama_worksheet, NAMA_KOLOM_STANDAR, rows=200)
    except Exception:
        return None

# -----------------------------------------------------------------------------
# Staf list management
@st.cache_data(ttl=60, show_spinner=False)
def get_daftar_staf_terbaru() -> List[str]:
    default_staf = ["Saya"]
    try:
        ws = get_ws(SHEET_CONFIG_NAMA, ["Daftar Nama Staf"], rows=100)
        nama_list = ws.col_values(1)
        if nama_list and nama_list[0] == "Daftar Nama Staf":
            nama_list.pop(0)
        return nama_list if nama_list else default_staf
    except Exception:
        return default_staf


def tambah_staf_baru(nama_baru: str) -> Tuple[bool, str]:
    try:
        ws = get_ws(SHEET_CONFIG_NAMA, ["Daftar Nama Staf"], rows=100)
        existing = set(ws.col_values(1))
        if nama_baru in existing:
            return False, "Nama sudah ada!"
        ws.append_row([nama_baru], value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return True, "Berhasil tambah tim!"
    except Exception as e:
        return False, str(e)

# -----------------------------------------------------------------------------
# Team configuration
@st.cache_data(ttl=60, show_spinner=False)
def load_team_config() -> pd.DataFrame:
    try:
        ws = get_ws(SHEET_CONFIG_TEAM, TEAM_COLUMNS, rows=300)
        data = ws.get_all_records()
        df = pd.DataFrame(data).fillna("")
        for c in TEAM_COLUMNS:
            if c not in df.columns:
                df[c] = ""
        return df[TEAM_COLUMNS].copy()
    except Exception:
        return pd.DataFrame(columns=TEAM_COLUMNS)


def tambah_team_baru(nama_team: str, posisi: str, anggota_list: Sequence[str]) -> Tuple[bool, str]:
    try:
        nama_team = str(nama_team).strip()
        posisi = str(posisi).strip()
        anggota_list2 = [str(a).strip() for a in anggota_list if str(a).strip()]

        if not nama_team or not posisi or not anggota_list2:
            return False, "Nama team, posisi, dan minimal 1 anggota wajib diisi."

        ws = get_ws(SHEET_CONFIG_TEAM, TEAM_COLUMNS, rows=300)

        existing: set[Tuple[str, str, str]] = set()
        try:
            for r in ws.get_all_records():
                key = (
                    str(r.get(TEAM_COL_NAMA_TEAM, "")).strip(),
                    str(r.get(TEAM_COL_POSISI, "")).strip(),
                    str(r.get(TEAM_COL_ANGGOTA, "")).strip(),
                )
                existing.add(key)
        except Exception:
            pass

        rows_to_add = []
        for anggota in anggota_list2:
            key = (nama_team, posisi, anggota)
            if key not in existing:
                rows_to_add.append([nama_team, posisi, anggota])

        if not rows_to_add:
            return False, "Semua anggota sudah terdaftar di team tersebut."

        ws.append_rows(rows_to_add, value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return True, f"Berhasil tambah team '{nama_team}' ({len(rows_to_add)} anggota)."
    except Exception as e:
        return False, str(e)

# -----------------------------------------------------------------------------
# Dropbox upload
def upload_ke_dropbox(file_obj, nama_staf: str, kategori: str = "Umum") -> str:
    if not KONEKSI_DROPBOX_BERHASIL or dbx is None:
        return "Koneksi Dropbox Error"

    try:
        file_data = file_obj.getvalue()
        ts = _now().strftime("%Y%m%d_%H%M%S")

        clean_filename = "".join([c for c in file_obj.name if c.isalnum() or c in (".", "_")])
        clean_user_folder = "".join([c for c in nama_staf if c.isalnum() or c in (" ", "_")]).replace(" ", "_")
        clean_kategori = "".join([c for c in kategori if c.isalnum() or c in (" ", "_")]).replace(" ", "_")

        path = f"{FOLDER_DROPBOX}/{clean_user_folder}/{clean_kategori}/{ts}_{clean_filename}"
        dbx.files_upload(file_data, path, mode=dropbox.files.WriteMode.add)

        settings = SharedLinkSettings(requested_visibility=RequestedVisibility.public)
        try:
            link = dbx.sharing_create_shared_link_with_settings(path, settings=settings)
        except ApiError as e:
            if e.error.is_shared_link_already_exists():
                link = dbx.sharing_list_shared_links(path, direct_only=True).links[0]
            else:
                return "-"

        return link.url.replace("?dl=0", "?raw=1")
    except Exception:
        return "-"

# -----------------------------------------------------------------------------
# Checklist helpers
def clean_bulk_input(text_input: str) -> List[str]:
    lines = (text_input or "").split("\n")
    cleaned_targets: List[str] = []
    for line in lines:
        cleaned = re.sub(r"^[\d\.\-\*\s]+", "", line).strip()
        if cleaned:
            cleaned_targets.append(cleaned)
    return cleaned_targets


@st.cache_data(ttl=60, show_spinner=False)
def load_checklist(sheet_name: str, columns: Sequence[str]) -> pd.DataFrame:
    try:
        ws = get_ws(sheet_name, list(columns), rows=250)
        data = ws.get_all_records()
        df = pd.DataFrame(data).fillna("")

        for col in columns:
            if col not in df.columns:
                df[col] = False if col == "Status" else ""

        if "Status" in df.columns:
            df["Status"] = df["Status"].apply(lambda x: True if str(x).upper() == "TRUE" else False)

        return df[list(columns)].copy()
    except Exception:
        return pd.DataFrame(columns=list(columns))


def save_checklist(sheet_name: str, df: pd.DataFrame, columns: Sequence[str]) -> bool:
    try:
        ws = get_ws(sheet_name, list(columns), rows=max(250, len(df) + 10))
        ensure_headers(ws, list(columns))
        ws.clear()

        rows_needed = len(df) + 1
        if ws.row_count < rows_needed:
            ws.resize(rows=rows_needed)

        df_save = df.copy().fillna("")
        for c in columns:
            if c not in df_save.columns:
                df_save[c] = ""

        if "Status" in df_save.columns:
            df_save["Status"] = df_save["Status"].apply(lambda x: "TRUE" if bool(x) else "FALSE")

        df_save = df_save[list(columns)].astype(str)
        data_to_save = [df_save.columns.values.tolist()] + df_save.values.tolist()

        ws.update(range_name="A1", values=data_to_save, value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return True
    except Exception:
        return False


def apply_audit_checklist_changes(
    df_before: pd.DataFrame, df_after: pd.DataFrame, key_cols: Sequence[str], actor: str
) -> pd.DataFrame:
    if df_after is None or df_after.empty:
        return df_after

    actor = safe_str(actor, "-").strip() or "-"
    before = df_before.copy() if df_before is not None else pd.DataFrame()
    after = df_after.copy()

    for c in [COL_TS_UPDATE, COL_UPDATED_BY]:
        if c not in after.columns:
            after[c] = ""

    def make_key_row(r: pd.Series) -> Tuple[str, ...]:
        return tuple(safe_str(r.get(k, "")).strip() for k in key_cols)

    before_map: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    if not before.empty:
        for _, r in before.iterrows():
            before_map[make_key_row(r)] = r.to_dict()

    ts = now_ts_str()
    watched_cols = {"Status", "Bukti/Catatan"}

    for idx, r in after.iterrows():
        k = make_key_row(r)
        prev = before_map.get(k, None)
        if prev is None:
            after.at[idx, COL_TS_UPDATE] = ts
            after.at[idx, COL_UPDATED_BY] = actor
            continue

        changed = False
        for col in watched_cols:
            if col not in after.columns:
                continue
            oldv = prev.get(col, "")
            newv = r.get(col, "")
            if col == "Status":
                if normalize_bool(oldv) != normalize_bool(newv):
                    changed = True
            else:
                if safe_str(oldv, "").strip() != safe_str(newv, "").strip():
                    changed = True

        if changed:
            after.at[idx, COL_TS_UPDATE] = ts
            after.at[idx, COL_UPDATED_BY] = actor

    return after


def add_bulk_targets(sheet_name: str, base_row_data: Sequence[str], targets_list: Sequence[str]) -> bool:
    try:
        columns = TEAM_CHECKLIST_COLUMNS if sheet_name == SHEET_TARGET_TEAM else INDIV_CHECKLIST_COLUMNS
        ws = get_ws(sheet_name, columns, rows=300)

        actor = get_actor_fallback(default="Admin")
        ts = now_ts_str()

        rows_to_add = []
        for t in targets_list:
            new_row = [""] * len(columns)
            row_vals = list(base_row_data) if base_row_data else []
            for i in range(min(len(row_vals), len(columns))):
                new_row[i] = row_vals[i]

            if sheet_name == SHEET_TARGET_TEAM:
                new_row[0] = t
            else:
                new_row[1] = t

            if COL_TS_UPDATE in columns:
                new_row[columns.index(COL_TS_UPDATE)] = ts
            if COL_UPDATED_BY in columns:
                new_row[columns.index(COL_UPDATED_BY)] = actor

            rows_to_add.append(new_row)

        ws.append_rows(rows_to_add, value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return True
    except Exception:
        return False


def update_evidence_row(
    sheet_name: str,
    target_name: str,
    note: str,
    file_obj,
    user_folder_name: str,
    kategori_folder: str,
) -> Tuple[bool, str]:
    try:
        columns = TEAM_CHECKLIST_COLUMNS if sheet_name == SHEET_TARGET_TEAM else INDIV_CHECKLIST_COLUMNS
        ws = get_ws(sheet_name, columns, rows=300)

        df = pd.DataFrame(ws.get_all_records()).fillna("")
        col_target_key = "Misi" if sheet_name == SHEET_TARGET_TEAM else "Target"
        if col_target_key not in df.columns:
            return False, "Kolom kunci error."

        matches = df.index[df[col_target_key] == target_name].tolist()
        if not matches:
            return False, "Target tidak ditemukan."

        row_idx_pandas = matches[0]
        row_idx_gsheet = row_idx_pandas + 2

        link_bukti = ""
        if file_obj:
            link_bukti = upload_ke_dropbox(file_obj, user_folder_name, kategori=kategori_folder)

        catatan_lama = str(df.at[row_idx_pandas, "Bukti/Catatan"]) if "Bukti/Catatan" in df.columns else ""
        if catatan_lama in {"-", "nan"}:
            catatan_lama = ""

        ts_update = now_ts_str()
        actor = safe_str(user_folder_name, "-").strip() or "-"

        update_text = f"[{ts_update}] "
        if note:
            update_text += f"{note}. "
        if link_bukti and link_bukti != "-":
            update_text += f"[FOTO: {link_bukti}]"

        final_note = f"{catatan_lama}\n{update_text}" if catatan_lama.strip() else update_text
        final_note = final_note.strip() if final_note.strip() else "-"

        headers = ws.row_values(1)
        if "Bukti/Catatan" not in headers:
            return False, "Kolom Bukti error."

        col_idx_gsheet = headers.index("Bukti/Catatan") + 1
        cell_address = gspread.utils.rowcol_to_a1(row_idx_gsheet, col_idx_gsheet)
        ws.update(range_name=cell_address, values=[[final_note]], value_input_option="USER_ENTERED")

        if COL_TS_UPDATE in headers:
            col_ts = headers.index(COL_TS_UPDATE) + 1
            cell_ts = gspread.utils.rowcol_to_a1(row_idx_gsheet, col_ts)
            ws.update(range_name=cell_ts, values=[[ts_update]], value_input_option="USER_ENTERED")

        if COL_UPDATED_BY in headers:
            col_by = headers.index(COL_UPDATED_BY) + 1
            cell_by = gspread.utils.rowcol_to_a1(row_idx_gsheet, col_by)
            ws.update(range_name=cell_by, values=[[actor]], value_input_option="USER_ENTERED")

        auto_format_sheet(ws)
        return True, "Berhasil update!"
    except Exception as e:
        return False, f"Error: {e}"

# -----------------------------------------------------------------------------
# Feedback + daily report functions
def kirim_feedback_admin(nama_staf: str, timestamp_key: str, isi_feedback: str) -> Tuple[bool, str]:
    """Append feedback rather than overwriting existing content."""
    try:
        ws = get_or_create_worksheet(nama_staf)
        if ws is None:
            return False, "Worksheet staf tidak ditemukan."

        ensure_headers(ws, NAMA_KOLOM_STANDAR)

        headers = ws.row_values(1)
        if COL_FEEDBACK not in headers:
            ws.update_cell(1, len(headers) + 1, COL_FEEDBACK)
            headers.append(COL_FEEDBACK)
            auto_format_sheet(ws)

        all_timestamps = ws.col_values(1)

        def clean_ts(text: Any) -> str:
            return "".join(filter(str.isdigit, str(text)))

        target_clean = clean_ts(timestamp_key)
        found_row = None
        for idx, val in enumerate(all_timestamps):
            if clean_ts(val) == target_clean:
                found_row = idx + 1
                break

        if not found_row:
            return False, "Data tidak ditemukan."

        col_idx = headers.index(COL_FEEDBACK) + 1
        existing = safe_str(ws.cell(found_row, col_idx).value, "").strip()
        ts = now_ts_str()
        actor = get_actor_fallback(default="Admin")
        new_line = f"[{ts}] ({actor}) {isi_feedback}".strip()

        merged = f"{existing}\n{new_line}".strip() if existing else new_line
        ws.update_cell(found_row, col_idx, merged)
        return True, "Feedback terkirim!"
    except Exception as e:
        return False, f"Error: {e}"


def simpan_laporan_harian_batch(list_of_rows: Sequence[Sequence[Any]], nama_staf: str) -> bool:
    try:
        ws = get_or_create_worksheet(nama_staf)
        if ws is None:
            return False
        ensure_headers(ws, NAMA_KOLOM_STANDAR)
        ws.append_rows(list(list_of_rows), value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return True
    except Exception as e:
        log.warning("Error saving daily report batch: %s", e)
        return False


@st.cache_data(ttl=30, show_spinner=False)
def get_reminder_pending(nama_staf: str) -> Optional[str]:
    try:
        ws = get_or_create_worksheet(nama_staf)
        if not ws:
            return None
        all_vals = ws.get_all_records()
        if not all_vals:
            return None
        last_row = all_vals[-1]
        pending_task = last_row.get(COL_PENDING, "")
        if pending_task and str(pending_task).strip() not in {"-", ""}:
            return pending_task
        return None
    except Exception:
        return None


@st.cache_data(ttl=60, show_spinner=False)
def load_all_reports(daftar_staf: Sequence[str]) -> pd.DataFrame:
    all_data: List[Dict[str, Any]] = []
    for nama in daftar_staf:
        try:
            ws = get_or_create_worksheet(nama)
            if ws:
                d = ws.get_all_records()
                if d:
                    all_data.extend(d)
        except Exception:
            pass
    return pd.DataFrame(all_data) if all_data else pd.DataFrame(columns=NAMA_KOLOM_STANDAR)


def render_hybrid_table(df_data: pd.DataFrame, unique_key: str, main_text_col: str) -> pd.DataFrame:
    use_aggrid_attempt = HAS_AGGRID
    if use_aggrid_attempt:
        try:
            df_grid = df_data.copy().reset_index(drop=True)
            gb = GridOptionsBuilder.from_dataframe(df_grid)

            if "Status" in df_grid.columns:
                gb.configure_column("Status", editable=True, width=90)

            if main_text_col in df_grid.columns:
                gb.configure_column(main_text_col, wrapText=True, autoHeight=True, width=400, editable=False)

            if "Bukti/Catatan" in df_grid.columns:
                gb.configure_column(
                    "Bukti/Catatan",
                    wrapText=True,
                    autoHeight=True,
                    editable=True,
                    cellEditor="agLargeTextCellEditor",
                    width=300,
                )

            if COL_TS_UPDATE in df_grid.columns:
                gb.configure_column(COL_TS_UPDATE, editable=False, width=420)
            if COL_UPDATED_BY in df_grid.columns:
                gb.configure_column(COL_UPDATED_BY, editable=False, width=160)

            gb.configure_default_column(editable=False)
            gridOptions = gb.build()

            grid_response = AgGrid(
                df_grid,
                gridOptions=gridOptions,
                update_mode=GridUpdateMode.MODEL_CHANGED,
                fit_columns_on_grid_load=True,
                height=420,
                theme="streamlit",
                key=f"aggrid_{unique_key}",
            )
            return pd.DataFrame(grid_response["data"])
        except Exception:
            use_aggrid_attempt = False

    column_config: Dict[str, Any] = {}
    if "Status" in df_data.columns:
        column_config["Status"] = st.column_config.CheckboxColumn("Done?", width="small")
    if main_text_col in df_data.columns:
        column_config[main_text_col] = st.column_config.TextColumn(main_text_col, disabled=True, width="large")
    if "Bukti/Catatan" in df_data.columns:
        column_config["Bukti/Catatan"] = st.column_config.TextColumn("Bukti/Note", width="large")
    if COL_TS_UPDATE in df_data.columns:
        column_config[COL_TS_UPDATE] = st.column_config.TextColumn(COL_TS_UPDATE, disabled=True, width="large")
    if COL_UPDATED_BY in df_data.columns:
        column_config[COL_UPDATED_BY] = st.column_config.TextColumn(COL_UPDATED_BY, disabled=True, width="medium")

    return st.data_editor(
        df_data,
        column_config=column_config,
        hide_index=True,
        key=f"editor_native_{unique_key}",
        use_container_width=True,
    )

# -----------------------------------------------------------------------------
# Closing deal functions
@st.cache_data(ttl=60, show_spinner=False)
def load_closing_deal() -> pd.DataFrame:
    try:
        ws = get_ws(SHEET_CLOSING_DEAL, CLOSING_COLUMNS, rows=300)
        data = ws.get_all_records()
        df = pd.DataFrame(data)

        for c in CLOSING_COLUMNS:
            if c not in df.columns:
                df[c] = ""

        if COL_NILAI_KONTRAK in df.columns:
            parsed = df[COL_NILAI_KONTRAK].apply(parse_rupiah_to_int)
            df[COL_NILAI_KONTRAK] = pd.Series(parsed, dtype="Int64")

        for c in [COL_GROUP, COL_MARKETING, COL_TGL_EVENT, COL_BIDANG]:
            if c in df.columns:
                df[c] = df[c].fillna("").astype(str)

        return df[CLOSING_COLUMNS].copy()
    except Exception:
        return pd.DataFrame(columns=CLOSING_COLUMNS)


def tambah_closing_deal(
    nama_group: str,
    nama_marketing: str,
    tanggal_event: date,
    bidang: str,
    nilai_kontrak_input: str,
) -> Tuple[bool, str]:
    try:
        nama_group = str(nama_group).strip() if nama_group is not None else ""
        nama_marketing = str(nama_marketing).strip() if nama_marketing is not None else ""
        bidang = str(bidang).strip() if bidang is not None else ""

        if not nama_group:
            nama_group = "-"

        if not nama_marketing or not tanggal_event or not bidang or not str(nilai_kontrak_input).strip():
            return False, "Field wajib: Nama Marketing, Tanggal Event, Bidang, dan Nilai Kontrak."

        nilai_int = parse_rupiah_to_int(nilai_kontrak_input)
        if nilai_int is None:
            return False, "Nilai Kontrak tidak valid. Contoh: 15jt / 15.000.000 / Rp 15.000.000 / 15,5jt"

        ws = get_ws(SHEET_CLOSING_DEAL, CLOSING_COLUMNS, rows=300)
        tgl_str = tanggal_event.strftime("%Y-%m-%d")
        ws.append_row([nama_group, nama_marketing, tgl_str, bidang, int(nilai_int)], value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return True, "Closing deal berhasil disimpan!"
    except Exception as e:
        return False, str(e)

# -----------------------------------------------------------------------------
# Pembayaran functions
@st.cache_data(ttl=60, show_spinner=False)
def load_pembayaran_dp() -> pd.DataFrame:
    try:
        ws = get_ws(SHEET_PEMBAYARAN, PAYMENT_COLUMNS, rows=600)
        data = ws.get_all_records()
        df = pd.DataFrame(data)

        for c in PAYMENT_COLUMNS:
            if c not in df.columns:
                df[c] = ""

        if COL_NOMINAL_BAYAR in df.columns:
            parsed = df[COL_NOMINAL_BAYAR].apply(parse_rupiah_to_int)
            df[COL_NOMINAL_BAYAR] = pd.Series(parsed, dtype="Int64")

        if COL_STATUS_BAYAR in df.columns:
            df[COL_STATUS_BAYAR] = df[COL_STATUS_BAYAR].apply(lambda x: True if str(x).upper() == "TRUE" else False)

        if COL_JATUH_TEMPO in df.columns:
            df[COL_JATUH_TEMPO] = pd.to_datetime(df[COL_JATUH_TEMPO], errors="coerce").dt.date

        for c in [
            COL_TS_BAYAR,
            COL_GROUP,
            COL_MARKETING,
            COL_TGL_EVENT,
            COL_JENIS_BAYAR,
            COL_BUKTI_BAYAR,
            COL_CATATAN_BAYAR,
            COL_TS_UPDATE,
            COL_UPDATED_BY,
        ]:
            if c in df.columns:
                df[c] = df[c].fillna("").astype(str)

        if COL_TS_UPDATE in df.columns:
            df[COL_TS_UPDATE] = df[COL_TS_UPDATE].apply(lambda x: build_numbered_log(parse_payment_log_lines(x)))

        if COL_TS_BAYAR in df.columns and COL_TS_UPDATE in df.columns:
            def _fix_empty_log(row):
                logv = safe_str(row.get(COL_TS_UPDATE, ""), "").strip()
                if logv:
                    return logv
                ts_in = safe_str(row.get(COL_TS_BAYAR, ""), "").strip()
                return build_numbered_log([safe_str(ts_in, now_ts_str())]) if ts_in else ""

            df[COL_TS_UPDATE] = df.apply(_fix_empty_log, axis=1)

        return df[PAYMENT_COLUMNS].copy()
    except Exception:
        return pd.DataFrame(columns=PAYMENT_COLUMNS)


def save_pembayaran_dp(df: pd.DataFrame) -> bool:
    try:
        ws = get_ws(SHEET_PEMBAYARAN, PAYMENT_COLUMNS, rows=max(600, len(df) + 10))
        ensure_headers(ws, PAYMENT_COLUMNS)

        ws.clear()
        rows_needed = len(df) + 1
        if ws.row_count < rows_needed:
            ws.resize(rows=rows_needed)

        df_save = df.copy()
        for c in PAYMENT_COLUMNS:
            if c not in df_save.columns:
                df_save[c] = ""

        df_save[COL_STATUS_BAYAR] = df_save[COL_STATUS_BAYAR].apply(lambda x: "TRUE" if bool(x) else "FALSE")

        def _to_int_or_blank(x):
            if x is None or pd.isna(x):
                return ""
            val = parse_rupiah_to_int(x)
            return "" if val is None else int(val)

        df_save[COL_NOMINAL_BAYAR] = df_save[COL_NOMINAL_BAYAR].apply(_to_int_or_blank)

        def _fmt_date(d):
            if d is None or pd.isna(d):
                return ""
            if hasattr(d, "strftime"):
                return d.strftime("%Y-%m-%d")
            s = str(d).strip()
            return s if s and s.lower() not in {"nan", "none"} else ""

        df_save[COL_JATUH_TEMPO] = df_save[COL_JATUH_TEMPO].apply(_fmt_date)
        df_save[COL_TS_UPDATE] = df_save[COL_TS_UPDATE].apply(lambda x: build_numbered_log(parse_payment_log_lines(x)))
        df_save[COL_UPDATED_BY] = df_save[COL_UPDATED_BY].apply(lambda x: safe_str(x, "-").strip() or "-")

        df_save = df_save[PAYMENT_COLUMNS].fillna("")
        data_to_save = [df_save.columns.values.tolist()] + df_save.values.tolist()
        ws.update(range_name="A1", values=data_to_save, value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return True
    except Exception:
        return False


def apply_audit_payments_changes(df_before: pd.DataFrame, df_after: pd.DataFrame, actor: str) -> pd.DataFrame:
    if df_after is None or df_after.empty:
        return df_after

    actor = safe_str(actor, "-").strip() or "-"
    before = df_before.copy() if df_before is not None else pd.DataFrame()
    after = df_after.copy()

    for c in [COL_TS_UPDATE, COL_UPDATED_BY]:
        if c not in after.columns:
            after[c] = ""

    if before.empty or COL_TS_BAYAR not in before.columns or COL_TS_BAYAR not in after.columns:
        ts = now_ts_str()
        for i in range(len(after)):
            oldlog = after.at[i, COL_TS_UPDATE] if COL_TS_UPDATE in after.columns else ""
            after.at[i, COL_TS_UPDATE] = append_payment_ts_update(oldlog, ts, actor, ["Data diperbarui (fallback)"])
            after.at[i, COL_UPDATED_BY] = actor
        return after

    before_idx = before.set_index(COL_TS_BAYAR, drop=False)
    after_idx = after.set_index(COL_TS_BAYAR, drop=False)

    watched_cols = [
        COL_JENIS_BAYAR,
        COL_NOMINAL_BAYAR,
        COL_JATUH_TEMPO,
        COL_STATUS_BAYAR,
        COL_BUKTI_BAYAR,
        COL_CATATAN_BAYAR,
    ]
    ts = now_ts_str()

    for key, row in after_idx.iterrows():
        if key not in before_idx.index:
            oldlog = safe_str(row.get(COL_TS_UPDATE, ""), "")
            if not safe_str(oldlog, "").strip():
                oldlog = build_numbered_log([safe_str(row.get(COL_TS_BAYAR, ts), ts)])
            after_idx.at[key, COL_TS_UPDATE] = oldlog
            after_idx.at[key, COL_UPDATED_BY] = actor
            continue

        prev = before_idx.loc[key]
        if isinstance(prev, pd.DataFrame):
            prev = prev.iloc[0]

        changes: List[str] = []
        for col in watched_cols:
            if col not in after_idx.columns or col not in before_idx.columns:
                continue

            oldv = prev[col]
            newv = row[col]

            if col == COL_STATUS_BAYAR:
                if normalize_bool(oldv) != normalize_bool(newv):
                    changes.append(f"Status Pembayaran: {_fmt_payment_val_for_log(col, oldv)} → {_fmt_payment_val_for_log(col, newv)}")
            elif col == COL_JATUH_TEMPO:
                if normalize_date(oldv) != normalize_date(newv):
                    changes.append(f"Jatuh Tempo: {_fmt_payment_val_for_log(col, oldv)} → {_fmt_payment_val_for_log(col, newv)}")
            elif col == COL_NOMINAL_BAYAR:
                if parse_rupiah_to_int(oldv) != parse_rupiah_to_int(newv):
                    changes.append(f"Nominal: {_fmt_payment_val_for_log(col, oldv)} → {_fmt_payment_val_for_log(col, newv)}")
            else:
                if safe_str(oldv, "").strip() != safe_str(newv, "").strip():
                    changes.append(f"{col}: {_fmt_payment_val_for_log(col, oldv)} → {_fmt_payment_val_for_log(col, newv)}")

        if changes:
            oldlog = safe_str(prev.get(COL_TS_UPDATE, ""), "")
            newlog = append_payment_ts_update(oldlog, ts, actor, changes)
            after_idx.at[key, COL_TS_UPDATE] = newlog
            after_idx.at[key, COL_UPDATED_BY] = actor

    return after_idx.reset_index(drop=True)


def tambah_pembayaran_dp(
    nama_group: str,
    nama_marketing: str,
    tanggal_event: Optional[date],
    jenis_bayar: str,
    nominal_input: str,
    jatuh_tempo: date,
    status_bayar: bool,
    bukti_file,
    catatan: str,
) -> Tuple[bool, str]:
    try:
        nama_group = str(nama_group).strip() if nama_group else "-"
        nama_marketing = str(nama_marketing).strip() if nama_marketing else ""
        jenis_bayar = str(jenis_bayar).strip() if jenis_bayar else "Down Payment (DP)"
        catatan = str(catatan).strip() if catatan else "-"

        if not nama_marketing or not str(nominal_input).strip() or not jatuh_tempo:
            return False, "Field wajib: Nama Marketing, Nominal, dan Batas Waktu Bayar."

        nominal_int = parse_rupiah_to_int(nominal_input)
        if nominal_int is None:
            return False, "Nominal tidak valid. Contoh: 5jt / 5.000.000 / Rp 5.000.000 / 5,5jt"

        link_bukti = "-"
        if bukti_file and KONEKSI_DROPBOX_BERHASIL:
            link_bukti = upload_ke_dropbox(bukti_file, nama_marketing, kategori="Bukti_Pembayaran")

        ws = get_ws(SHEET_PEMBAYARAN, PAYMENT_COLUMNS, rows=600)

        tgl_event_str = tanggal_event.strftime("%Y-%m-%d") if tanggal_event else "-"
        jatuh_tempo_str = jatuh_tempo.strftime("%Y-%m-%d")

        ts_input = now_ts_str()
        actor0 = nama_marketing or "-"
        ts_update_log = build_numbered_log([ts_input])

        ws.append_row(
            [
                ts_input,
                nama_group,
                nama_marketing,
                tgl_event_str,
                jenis_bayar,
                int(nominal_int),
                jatuh_tempo_str,
                "TRUE" if bool(status_bayar) else "FALSE",
                link_bukti,
                catatan if catatan else "-",
                ts_update_log,
                actor0,
            ],
            value_input_option="USER_ENTERED",
        )
        auto_format_sheet(ws)
        return True, "Pembayaran berhasil disimpan!"
    except Exception as e:
        return False, str(e)


def build_alert_pembayaran(df: pd.DataFrame, days_due_soon: int = 3) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df is None or df.empty:
        return (
            pd.DataFrame(columns=df.columns if df is not None else PAYMENT_COLUMNS),
            pd.DataFrame(columns=df.columns if df is not None else PAYMENT_COLUMNS),
        )

    today = today_jkt()
    df2 = df.copy()

    if COL_STATUS_BAYAR in df2.columns:
        df2 = df2[df2[COL_STATUS_BAYAR] == False].copy()

    if COL_JATUH_TEMPO in df2.columns:
        df2 = df2[pd.notna(df2[COL_JATUH_TEMPO])].copy()
    else:
        return (
            pd.DataFrame(columns=df.columns),
            pd.DataFrame(columns=df.columns),
        )

    overdue = df2[df2[COL_JATUH_TEMPO] < today].copy()
    due_soon = df2[(df2[COL_JATUH_TEMPO] >= today) & (df2[COL_JATUH_TEMPO] <= (today + timedelta(days=days_due_soon)))].copy()
    return overdue, due_soon


def update_bukti_pembayaran_by_index(row_index_0based: int, file_obj, nama_marketing: str, actor: str = "-") -> Tuple[bool, str]:
    if not KONEKSI_DROPBOX_BERHASIL:
        return False, "Dropbox non-aktif. Upload bukti dimatikan."
    if file_obj is None:
        return False, "File bukti belum dipilih."

    try:
        ws = get_ws(SHEET_PEMBAYARAN, PAYMENT_COLUMNS, rows=600)
        link = upload_ke_dropbox(file_obj, nama_marketing or "Unknown", kategori="Bukti_Pembayaran")
        if not link or link == "-":
            return False, "Gagal upload ke Dropbox."

        headers = ws.row_values(1)
        row_gsheet = row_index_0based + 2

        if COL_BUKTI_BAYAR not in headers:
            return False, "Kolom 'Bukti Pembayaran' tidak ditemukan."
        col_bukti = headers.index(COL_BUKTI_BAYAR) + 1

        old_bukti = safe_str(ws.cell(row_gsheet, col_bukti).value, "")
        cell_bukti = gspread.utils.rowcol_to_a1(row_gsheet, col_bukti)
        ws.update(range_name=cell_bukti, values=[[link]], value_input_option="USER_ENTERED")

        ts = now_ts_str()
        actor_final = safe_str(actor, "-").strip() or "-"

        if COL_TS_UPDATE in headers:
            col_ts = headers.index(COL_TS_UPDATE) + 1
            old_log = safe_str(ws.cell(row_gsheet, col_ts).value, "")
            new_log = append_payment_ts_update(
                old_log,
                ts,
                actor_final,
                [f"{COL_BUKTI_BAYAR}: {_fmt_payment_val_for_log(COL_BUKTI_BAYAR, old_bukti)} → {_fmt_payment_val_for_log(COL_BUKTI_BAYAR, link)}"],
            )
            cell_ts = gspread.utils.rowcol_to_a1(row_gsheet, col_ts)
            ws.update(range_name=cell_ts, values=[[new_log]], value_input_option="USER_ENTERED")

        if COL_UPDATED_BY in headers:
            col_by = headers.index(COL_UPDATED_BY) + 1
            cell_by = gspread.utils.rowcol_to_a1(row_gsheet, col_by)
            ws.update(range_name=cell_by, values=[[actor_final]], value_input_option="USER_ENTERED")

        auto_format_sheet(ws)
        return True, "Bukti pembayaran berhasil di-update!"
    except Exception as e:
        return False, f"Error: {e}"


def _save_catatan_for_rows(df_pay: pd.DataFrame, edited_view: pd.DataFrame, actor_final: str) -> bool:
    df_new = df_pay.copy()
    ts_now = now_ts_str()

    for _, r in edited_view.iterrows():
        k = safe_str(r.get(COL_TS_BAYAR, "")).strip()
        if not k:
            continue
        mask = df_new[COL_TS_BAYAR].astype(str) == k
        if mask.any():
            old_note = safe_str(df_new.loc[mask, COL_CATATAN_BAYAR].values[0], "")
            new_note = safe_str(r.get(COL_CATATAN_BAYAR, ""), "")

            df_new.loc[mask, COL_CATATAN_BAYAR] = new_note

            old_log = safe_str(df_new.loc[mask, COL_TS_UPDATE].values[0], "")
            df_new.loc[mask, COL_TS_UPDATE] = append_payment_ts_update(
                old_log,
                ts_now,
                actor_final,
                [f"{COL_CATATAN_BAYAR}: {_fmt_payment_val_for_log(COL_CATATAN_BAYAR, old_note)} → {_fmt_payment_val_for_log(COL_CATATAN_BAYAR, new_note)}"],
            )
            df_new.loc[mask, COL_UPDATED_BY] = actor_final

    return save_pembayaran_dp(df_new)

# -----------------------------------------------------------------------------
# Main application UI

def main():
    """Main entrypoint for the app. Contains navigation, forms and dashboards."""
    # Sidebar navigation and admin login
    with st.sidebar:
        st.header("Navigasi")

        st.session_state.setdefault("is_admin", False)

        opsi_menu = ["📝 Laporan & Target"]
        if st.session_state["is_admin"]:
            opsi_menu.append("📊 Dashboard Admin")
        menu_nav = st.radio("Pilih Menu:", opsi_menu)

        st.divider()
        if not st.session_state["is_admin"]:
            with st.expander("🔐 Akses Khusus Admin"):
                if not admin_secret_configured():
                    st.warning("Admin login belum aktif: set `password_admin_hash` (disarankan) atau `password_admin` di Streamlit Secrets.")
                pwd = st.text_input("Password:", type="password", key="input_pwd")
                if st.button("Login Admin"):
                    if verify_admin_password(pwd):
                        st.session_state["is_admin"] = True
                        st.rerun()
                    else:
                        st.error("Password salah / belum dikonfigurasi!")
        else:
            if st.button("🔓 Logout Admin"):
                st.session_state["is_admin"] = False
                st.rerun()

        st.divider()
        st.header("🎯 Manajemen Target")

        tab_team, tab_individu, tab_admin = st.tabs(["Team", "Pribadi", "Admin"])

        with tab_team:
            st.caption("Bulk Input Target Team")
            with st.form("add_team_goal", clear_on_submit=True):
                goal_team_text = st.text_area("Target Team (Satu per baris)", height=100)
                c1, c2 = st.columns(2)
                today = today_jkt()
                start_d = c1.date_input("Mulai", value=today, key="start_team")
                end_d = c2.date_input("Selesai", value=today + timedelta(days=30), key="end_team")
                if st.form_submit_button("➕ Tambah"):
                    targets = clean_bulk_input(goal_team_text)
                    if targets:
                        if add_bulk_targets(SHEET_TARGET_TEAM, ["", str(start_d), str(end_d), "FALSE", "-"], targets):
                            st.success(f"{len(targets)} target ditambahkan!")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("Gagal.")

        with tab_individu:
            st.caption("Bulk Input Target Pribadi")
            NAMA_STAF = get_daftar_staf_terbaru()
            pilih_nama = st.selectbox("Siapa Anda?", NAMA_STAF, key="sidebar_user")

            with st.form("add_indiv_goal", clear_on_submit=True):
                goal_indiv_text = st.text_area("Target Mingguan (Satu per baris)", height=100)
                c1, c2 = st.columns(2)
                today = today_jkt()
                start_i = c1.date_input("Mulai", value=today, key="start_indiv")
                end_i = c2.date_input("Selesai", value=today + timedelta(days=7), key="end_indiv")
                if st.form_submit_button("➕ Tambah"):
                    targets = clean_bulk_input(goal_indiv_text)
                    if targets:
                        if add_bulk_targets(
                            SHEET_TARGET_INDIVIDU,
                            [pilih_nama, "", str(start_i), str(end_i), "FALSE", "-"],
                            targets,
                        ):
                            st.success(f"{len(targets)} target ditambahkan!")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("Gagal.")

        with tab_admin:
            with st.expander("➕ Tambah Karyawan"):
                with st.form("add_staff", clear_on_submit=True):
                    new_name = st.text_input("Nama")
                    new_role = st.text_input("Jabatan")
                    if st.form_submit_button("Tambah"):
                        if new_name and new_role:
                            res, msg = tambah_staf_baru(f"{new_name} ({new_role})")
                            if res:
                                st.success(msg)
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error(msg)
                        else:
                            st.error("Nama dan jabatan wajib diisi.")

            with st.expander("👥 Tambah Team (Admin)"):
                with st.form("add_team_admin", clear_on_submit=True):
                    team_name = st.text_input("Nama Team", placeholder="Contoh: Team Sales A")
                    team_posisi = st.text_input("Posisi/Divisi", placeholder="Contoh: Sales Lapangan / Digital Marketing")
                    anggota_text = st.text_area("Nama Anggota (satu per baris)", height=120, placeholder="Contoh:\nAndi\nBudi\nSusi")
                    if st.form_submit_button("Tambah Team"):
                        anggota_list = clean_bulk_input(anggota_text)
                        res, msg = tambah_team_baru(team_name, team_posisi, anggota_list)
                        if res:
                            st.success(msg)
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error(msg)

            with st.expander("📋 Lihat Daftar Team"):
                df_team_cfg = load_team_config()
                if not df_team_cfg.empty:
                    st.dataframe(df_team_cfg, use_container_width=True, hide_index=True)
                else:
                    st.info("Belum ada team yang tersimpan.")

        # Closing Deal section in sidebar
        st.divider()
        st.header("🤝 Closing Deal")

        with st.expander("➕ Input Closing Deal", expanded=False):
            with st.form("form_closing_deal", clear_on_submit=True):
                cd_group = st.text_input("Nama Group (Opsional)", placeholder="Kosongkan jika tidak ada")
                cd_marketing = st.text_input("Nama Marketing", placeholder="Contoh: Andi")
                cd_tgl = st.date_input("Tanggal Event", value=today_jkt(), key="closing_event_date")
                cd_bidang = st.text_input("Bidang (Manual)", placeholder="Contoh: F&B / Properti / Pendidikan")
                cd_nilai = st.text_input("Nilai Kontrak (Input bebas)", placeholder="Contoh: 15jt / 15.000.000 / Rp 15.000.000 / 15,5jt")

                if st.form_submit_button("✅ Simpan Closing Deal"):
                    res, msg = tambah_closing_deal(cd_group, cd_marketing, cd_tgl, cd_bidang, cd_nilai)
                    if res:
                        st.success(msg)
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(msg)

        with st.expander("📋 Data Closing Deal", expanded=False):
            df_cd = load_closing_deal()
            if not df_cd.empty:
                df_cd_display = df_cd.copy()
                df_cd_display[COL_NILAI_KONTRAK] = df_cd_display[COL_NILAI_KONTRAK].apply(lambda x: "" if pd.isna(x) else format_rupiah_display(x))
                st.dataframe(df_cd_display, use_container_width=True, hide_index=True)

                if HAS_OPENPYXL:
                    df_export = df_cd.copy()
                    df_export[COL_NILAI_KONTRAK] = df_export[COL_NILAI_KONTRAK].apply(lambda x: None if pd.isna(x) else int(x))
                    excel_bytes = df_to_excel_bytes(
                        df_export,
                        sheet_name="Closing_Deal",
                        col_widths={COL_GROUP: 25, COL_MARKETING: 20, COL_TGL_EVENT: 16, COL_BIDANG: 25, COL_NILAI_KONTRAK: 18},
                        wrap_cols=[COL_GROUP, COL_BIDANG],
                        right_align_cols=[COL_NILAI_KONTRAK],
                        number_format_cols={COL_NILAI_KONTRAK: '"Rp" #,##0'},
                    )
                    if excel_bytes:
                        st.download_button(
                            "⬇️ Download Excel Closing Deal (Rapi + Rupiah)",
                            data=excel_bytes,
                            file_name="closing_deal.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                        )

                csv_cd = df_cd.to_csv(index=False).encode("utf-8")
                st.download_button("⬇️ Download CSV Closing Deal", data=csv_cd, file_name="closing_deal.csv", mime="text/csv", use_container_width=True)
            else:
                st.info("Belum ada data closing deal.")

        # Pembayaran (DP/Termin/Pelunasan) section in sidebar
        st.divider()
        st.header("💳 Pembayaran (DP / Termin / Pelunasan)")

        with st.expander("➕ Input Pembayaran", expanded=False):
            p_group = st.text_input("Nama Group (Opsional)", placeholder="Kosongkan jika tidak ada", key="pay_group")
            p_marketing = st.text_input("Nama Marketing (Wajib)", placeholder="Contoh: Andi", key="pay_marketing")
            p_tgl_event = st.date_input("Tanggal Event (Opsional)", value=st.session_state.get("pay_event_date", today_jkt()), key="pay_event_date")

            jenis_opts = ["Down Payment (DP)", "Termin", "Pelunasan", "Lainnya"]
            current_jenis = st.session_state.get("pay_jenis_opt", "Down Payment (DP)")
            idx_jenis = jenis_opts.index(current_jenis) if current_jenis in jenis_opts else 0

            p_jenis_opt = st.selectbox("Jenis Pembayaran", jenis_opts, index=idx_jenis, key="pay_jenis_opt")

            p_jenis_custom = ""
            if p_jenis_opt == "Lainnya":
                p_jenis_custom = st.text_input(
                    "Tulis Jenis Pembayaran (Custom) *wajib*",
                    placeholder="Contoh: Cicilan 1 / Fee Admin / Refund / dll",
                    key="pay_jenis_custom",
                )

            p_jenis_final = p_jenis_opt if p_jenis_opt != "Lainnya" else (p_jenis_custom or "").strip()

            p_nominal = st.text_input(
                "Nominal Pembayaran (Input bebas)",
                placeholder="Contoh: 5000000 / 5jt / Rp 5.000.000 / 5,5jt",
                key="pay_nominal",
                on_change=on_change_pay_nominal,
            )

            nom_preview = parse_rupiah_to_int(p_nominal)
            st.caption(f"Preview: **{format_rupiah_display(nom_preview)}**" if nom_preview is not None else "Preview: -")

            p_jatuh_tempo = st.date_input("Batas Waktu Bayar (Jatuh Tempo)", value=st.session_state.get("pay_due_date", today_jkt() + timedelta(days=7)), key="pay_due_date")
            p_status = st.checkbox("✅ Sudah Dibayar?", value=bool(st.session_state.get("pay_status", False)), key="pay_status")
            p_catatan = st.text_area("Catatan (Opsional)", height=90, placeholder="Contoh: DP untuk booking tanggal event...", key="pay_note")

            p_bukti = st.file_uploader("Upload Bukti Pembayaran (Foto/Screenshot/PDF)", key="pay_file", disabled=not KONEKSI_DROPBOX_BERHASIL)

            if st.button("✅ Simpan Pembayaran", use_container_width=True, key="btn_save_payment"):
                if p_jenis_opt == "Lainnya" and not p_jenis_final:
                    st.error("Karena memilih 'Lainnya', jenis pembayaran custom wajib diisi.")
                else:
                    res, msg = tambah_pembayaran_dp(
                        nama_group=p_group,
                        nama_marketing=p_marketing,
                        tanggal_event=p_tgl_event,
                        jenis_bayar=p_jenis_final,
                        nominal_input=p_nominal,
                        jatuh_tempo=p_jatuh_tempo,
                        status_bayar=p_status,
                        bukti_file=p_bukti,
                        catatan=p_catatan,
                    )
                    if res:
                        st.success(msg)
                        reset_payment_form_state()
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(msg)

        with st.expander("📋 Data Pembayaran + Alert Jatuh Tempo", expanded=False):
            df_pay = load_pembayaran_dp()
            if df_pay.empty:
                st.info("Belum ada data pembayaran.")
            else:
                default_actor = get_actor_fallback(default="-")
                staff_opts = get_daftar_staf_terbaru()
                cA, cB = st.columns([2, 1])

                with cA:
                    actor_select = st.selectbox(
                        "Nama Editor (untuk log perubahan)",
                        options=staff_opts,
                        index=staff_opts.index(default_actor) if default_actor in staff_opts else 0,
                        key="payment_editor_name",
                    )
                with cB:
                    actor_manual = st.text_input("Atau tulis manual", placeholder="Opsional", key="payment_editor_manual")

                actor_final = (actor_manual.strip() if safe_str(actor_manual, "").strip() else safe_str(actor_select, "-").strip()) or "-"

                overdue_df, due_soon_df = build_alert_pembayaran(df_pay, days_due_soon=3)

                cols_alert = [
                    COL_TS_BAYAR,
                    COL_GROUP,
                    COL_MARKETING,
                    COL_JENIS_BAYAR,
                    COL_NOMINAL_BAYAR,
                    COL_JATUH_TEMPO,
                    COL_STATUS_BAYAR,
                    COL_BUKTI_BAYAR,
                    COL_CATATAN_BAYAR,
                    COL_TS_UPDATE,
                    COL_UPDATED_BY,
                ]

                def _alert_editor(title: str, df_alert: pd.DataFrame, key: str) -> Optional[pd.DataFrame]:
                    if df_alert.empty:
                        return None
                    st.info(title)
                    cols = [c for c in cols_alert if c in df_alert.columns]
                    view = payment_df_for_display(df_alert[cols])
                    edited = st.data_editor(
                        view,
                        column_config={
                            COL_STATUS_BAYAR: st.column_config.CheckboxColumn("Sudah Dibayar?", width="small"),
                            COL_JATUH_TEMPO: st.column_config.DateColumn("Jatuh Tempo", width="medium"),
                            COL_JENIS_BAYAR: st.column_config.TextColumn("Jenis Pembayaran", width="medium"),
                            COL_NOMINAL_BAYAR: st.column_config.TextColumn("Nominal", disabled=True, width="medium"),
                            COL_BUKTI_BAYAR: st.column_config.TextColumn("Bukti (Link)", width="large"),
                            COL_CATATAN_BAYAR: st.column_config.TextColumn("Catatan", width="large"),
                            COL_TS_UPDATE: st.column_config.TextColumn(COL_TS_UPDATE, disabled=True, width="large"),
                            COL_UPDATED_BY: st.column_config.TextColumn("Updated By", disabled=True, width="medium"),
                        },
                        disabled=[c for c in cols if c != COL_CATATAN_BAYAR],
                        hide_index=True,
                        use_container_width=True,
                        key=key,
                    )
                    return edited

                if not overdue_df.empty:
                    st.error(f"⛔ Overdue: {len(overdue_df)} pembayaran melewati jatuh tempo!")
                    edited_overdue = _alert_editor("Edit catatan overdue (yang boleh hanya Catatan):", overdue_df, "overdue_editor_catatan")
                    if st.button("💾 Simpan Catatan Overdue", use_container_width=True):
                        if edited_overdue is not None and _save_catatan_for_rows(df_pay, edited_overdue, actor_final):
                            st.toast("Catatan overdue tersimpan!", icon="✅")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("Gagal menyimpan catatan overdue.")

                if not due_soon_df.empty:
                    st.warning(f"⚠️ Jatuh tempo ≤ 3 hari: {len(due_soon_df)} pembayaran belum dibayar.")
                    edited_due = _alert_editor("Edit catatan due‑soon (yang boleh hanya Catatan):", due_soon_df, "due_soon_editor_catatan")
                    if st.button("💾 Simpan Catatan Due Soon", use_container_width=True):
                        if edited_due is not None and _save_catatan_for_rows(df_pay, edited_due, actor_final):
                            st.toast("Catatan due‑soon tersimpan!", icon="✅")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("Gagal menyimpan catatan due‑soon.")

                st.caption(
                    "Edit yang diizinkan di tabel utama: **Jenis Pembayaran**, **Status Pembayaran**, "
                    "**Jatuh Tempo**, **Catatan**. Semua perubahan masuk ke "
                    f"**{COL_TS_UPDATE}** dan **{COL_UPDATED_BY}**."
                )

                editable_cols = {COL_STATUS_BAYAR, COL_JATUH_TEMPO, COL_CATATAN_BAYAR, COL_JENIS_BAYAR}
                disabled_cols = [c for c in PAYMENT_COLUMNS if c not in editable_cols]

                df_pay_view = payment_df_for_display(df_pay)
                edited_pay_view = st.data_editor(
                    df_pay_view,
                    column_config={
                        COL_STATUS_BAYAR: st.column_config.CheckboxColumn("Sudah Dibayar?", width="small"),
                        COL_JATUH_TEMPO: st.column_config.DateColumn("Jatuh Tempo", width="medium"),
                        COL_JENIS_BAYAR: st.column_config.TextColumn("Jenis Pembayaran", width="medium"),
                        COL_NOMINAL_BAYAR: st.column_config.TextColumn("Nominal", disabled=True, width="medium"),
                        COL_BUKTI_BAYAR: st.column_config.TextColumn("Bukti (Link)", width="large"),
                        COL_CATATAN_BAYAR: st.column_config.TextColumn("Catatan", width="large"),
                        COL_TS_UPDATE: st.column_config.TextColumn(COL_TS_UPDATE, disabled=True, width="large"),
                        COL_UPDATED_BY: st.column_config.TextColumn("Updated By", disabled=True, width="medium"),
                    },
                    disabled=disabled_cols,
                    hide_index=True,
                    use_container_width=True,
                    key="editor_payments",
                )

                if st.button("💾 Simpan Perubahan Pembayaran", use_container_width=True):
                    df_after = df_pay.copy().set_index(COL_TS_BAYAR, drop=False)
                    ed = edited_pay_view.copy().set_index(COL_TS_BAYAR, drop=False)

                    for c in [COL_STATUS_BAYAR, COL_JATUH_TEMPO, COL_CATATAN_BAYAR, COL_JENIS_BAYAR]:
                        if c in ed.columns:
                            df_after.loc[ed.index, c] = ed[c]

                    df_after = df_after.reset_index(drop=True)
                    df_to_save = apply_audit_payments_changes(df_pay, df_after, actor=actor_final)

                    if save_pembayaran_dp(df_to_save):
                        st.toast("Tersimpan!", icon="✅")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("Gagal menyimpan perubahan.")

                st.divider()
                with st.expander("📎 Update Bukti Pembayaran (untuk data yang sudah ada)", expanded=False):
                    df_pay_reset = df_pay.reset_index(drop=True)

                    def _label_payment_idx(i: int) -> str:
                        r = df_pay_reset.iloc[i]
                        nominal_disp = format_rupiah_display(r.get(COL_NOMINAL_BAYAR))
                        due_disp = r.get(COL_JATUH_TEMPO, "")
                        status_disp = "✅ Dibayar" if bool(r.get(COL_STATUS_BAYAR)) else "⏳ Belum"
                        return (
                            f"{i+1}. {r.get(COL_MARKETING, '-')}"
                            f" | {r.get(COL_JENIS_BAYAR, '-')}"
                            f" | {nominal_disp}"
                            f" | Due: {due_disp}"
                            f" | {status_disp}"
                        )

                    if len(df_pay_reset) == 0:
                        st.info("Belum ada data pembayaran.")
                    else:
                        selected_idx = st.selectbox(
                            "Pilih record yang mau di-update buktinya:",
                            options=list(range(len(df_pay_reset))),
                            format_func=_label_payment_idx,
                            key="pay_select_update_bukti_idx",
                        )
                        file_new = st.file_uploader("Upload bukti baru:", key="pay_file_update_bukti", disabled=not KONEKSI_DROPBOX_BERHASIL)

                        if st.button("⬆️ Update Bukti", use_container_width=True):
                            marketing_name = str(df_pay_reset.iloc[selected_idx].get(COL_MARKETING, "Unknown"))
                            ok, msg = update_bukti_pembayaran_by_index(selected_idx, file_new, marketing_name, actor=actor_final)
                            if ok:
                                st.success(msg)
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error(msg)

    # -------------------------------------------------------------------------
    # Main Page content: header and alerts
    col_kiri, col_tengah, col_kanan = st.columns([1, 5, 1])
    with col_kiri:
        try:
            st.image("assets/logo traine.png", use_container_width=True)
        except Exception:
            st.write("")  # fallback if asset missing

    with col_tengah:
        st.markdown(
            f"""
            <div class='header-container'>
                <h1>Sales &amp; Marketing Action Center</h1>
                <p>Realtime: {_now().strftime('%d %B %Y %H:%M:%S')}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col_kanan:
        try:
            st.image("assets/log EO.png", use_container_width=True)
        except Exception as e:
            st.error(f"Gagal: {e}")

    try:
        df_pay_main = load_pembayaran_dp()
        if not df_pay_main.empty:
            overdue_main, due_soon_main = build_alert_pembayaran(df_pay_main, days_due_soon=3)
            if len(overdue_main) > 0:
                st.error(f"⛔ Alert Pembayaran: {len(overdue_main)} pembayaran OVERDUE!")
            elif len(due_soon_main) > 0:
                st.warning(f"⚠️ Alert Pembayaran: {len(due_soon_main)} pembayaran jatuh tempo ≤ 3 hari.")
    except Exception:
        pass

    # -------------------------------------------------------------------------
    # Menu: Laporan & Target
    if menu_nav == "📝 Laporan & Target":
        st.subheader("📊 Checklist Target (Result KPI)")
        col_dash_1, col_dash_2 = st.columns(2)

        df_team = load_checklist(SHEET_TARGET_TEAM, TEAM_CHECKLIST_COLUMNS)
        df_indiv_all = load_checklist(SHEET_TARGET_INDIVIDU, INDIV_CHECKLIST_COLUMNS)

        with col_dash_1:
            st.markdown("#### 🏆 Target Team")
            if not df_team.empty:
                done = len(df_team[df_team["Status"] == True])
                st.progress(done / len(df_team) if len(df_team) > 0 else 0, text=f"Pencapaian: {done}/{len(df_team)}")
                edited_team = render_hybrid_table(df_team, "team_table", "Misi")

                if st.button("💾 Simpan Team", use_container_width=True):
                    actor = get_actor_fallback(default="Admin")
                    df_to_save = apply_audit_checklist_changes(df_team, edited_team, key_cols=["Misi"], actor=actor)
                    if save_checklist(SHEET_TARGET_TEAM, df_to_save, TEAM_CHECKLIST_COLUMNS):
                        st.toast("Tersimpan!", icon="✅")
                        st.cache_data.clear()
                        st.rerun()

                with st.expander("📂 Update Bukti (Team)"):
                    pilih_misi = st.selectbox("Misi:", df_team["Misi"].tolist())
                    note_misi = st.text_area("Catatan")
                    file_misi = st.file_uploader("Bukti", key="up_team", disabled=not KONEKSI_DROPBOX_BERHASIL)
                    if st.button("Update Team"):
                        pelapor = get_actor_fallback(default="Admin")
                        sukses, msg = update_evidence_row(SHEET_TARGET_TEAM, pilih_misi, note_misi, file_misi, pelapor, "Target_Team")
                        if sukses:
                            st.success("Updated!")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error(msg)
            else:
                st.info("Belum ada target team.")

        with col_dash_2:
            st.markdown("#### ⚡ Target Individu")
            filter_nama = st.selectbox("Filter:", get_daftar_staf_terbaru(), index=0)

            if not df_indiv_all.empty:
                df_user = df_indiv_all[df_indiv_all["Nama"] == filter_nama]
                if not df_user.empty:
                    done = len(df_user[df_user["Status"] == True])
                    st.progress(done / len(df_user) if len(df_user) > 0 else 0, text=f"Progress: {done}/{len(df_user)}")
                    edited_indiv = render_hybrid_table(df_user, f"indiv_{filter_nama}", "Target")

                    if st.button(f"💾 Simpan {filter_nama}", use_container_width=True):
                        df_all_upd = df_indiv_all.copy()
                        df_all_upd.update(edited_indiv)

                        actor = filter_nama
                        df_all_upd = apply_audit_checklist_changes(df_indiv_all, df_all_upd, key_cols=["Nama", "Target"], actor=actor)

                        if save_checklist(SHEET_TARGET_INDIVIDU, df_all_upd, INDIV_CHECKLIST_COLUMNS):
                            st.toast("Tersimpan!", icon="✅")
                            st.cache_data.clear()
                            st.rerun()

                    with st.expander(f"📂 Update Bukti ({filter_nama})"):
                        pilih_target = st.selectbox("Target:", df_user["Target"].tolist())
                        note_target = st.text_area("Catatan", key="note_indiv")
                        file_target = st.file_uploader("Bukti", key="up_indiv", disabled=not KONEKSI_DROPBOX_BERHASIL)
                        if st.button("Update Pribadi"):
                            sukses, msg = update_evidence_row(SHEET_TARGET_INDIVIDU, pilih_target, note_target, file_target, filter_nama, "Target_Individu")
                            if sukses:
                                st.success("Updated!")
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error(msg)
                else:
                    st.info("Belum ada target.")
            else:
                st.info("Data kosong.")

        # Input Harian Section
        st.divider()
        # Container for Input Laporan Harian
        with st.container():
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            st.markdown("<h3>📝 Input Laporan Harian (Activity)</h3>", unsafe_allow_html=True)

            c_nama, c_reminder = st.columns([1, 2])
            with c_nama:
                nama_pelapor = st.selectbox("Nama Pelapor", get_daftar_staf_terbaru(), key="pelapor_main")

            # Reminder of last pending item or feedback
            try:
                df_user_only = load_all_reports([nama_pelapor])
                if not df_user_only.empty and COL_FEEDBACK in df_user_only.columns:
                    df_with_feed = df_user_only[df_user_only[COL_FEEDBACK].astype(str).str.strip() != ""]
                    if not df_with_feed.empty:
                        last_feed = df_with_feed.iloc[-1]
                        st.info(
                            f"💌 **Pesan Terbaru Team Lead (Laporan {last_feed[COL_TIMESTAMP]}):**\n\n"
                            f"\"{last_feed[COL_FEEDBACK]}\""
                        )
            except Exception:
                pass

            with c_reminder:
                pending_msg = get_reminder_pending(nama_pelapor)
                if pending_msg:
                    st.warning(f"🔔 **Reminder:** Kamu punya pendingan kemarin: '{pending_msg}'")
                else:
                    st.caption("Tidak ada pendingan dari laporan terakhir.")

            kategori_aktivitas = st.radio(
                "Jenis Aktivitas:",
                ["🚗 Sales (Kunjungan Lapangan)", "💻 Digital Marketing / Konten / Ads", "📞 Telesales / Follow Up", "🏢 Lainnya"],
                horizontal=True,
            )

            is_kunjungan = kategori_aktivitas.startswith("🚗")

            c1, c2 = st.columns(2)
            with c1:
                today_now = today_jkt()
                st.markdown(f"**Tanggal:** `{today_now.strftime('%d-%m-%Y')}`")
                sosmed_link = ""
                if "Digital Marketing" in kategori_aktivitas:
                    sosmed_link = st.text_input("Link Konten / Ads / Drive (Wajib jika ada)")

            with c2:
                if is_kunjungan:
                    lokasi_input = st.text_input("📍 Nama Klien / Lokasi Kunjungan (Wajib)")
                else:
                    lokasi_input = st.text_input("Jenis Tugas (Otomatis)", value=kategori_aktivitas, disabled=True)

                fotos = st.file_uploader(
                    "Upload Bukti (Foto/Screenshot/Dokumen)",
                    accept_multiple_files=True,
                    disabled=not KONEKSI_DROPBOX_BERHASIL,
                )

            deskripsi_map: Dict[str, str] = {}
            main_deskripsi = ""

            if fotos:
                st.info("📸 **Detail Bukti:** Berikan keterangan spesifik untuk setiap file:")
                for i, f in enumerate(fotos):
                    with st.container():
                        col_img, col_desc = st.columns([1, 3])
                        with col_img:
                            if f.type.startswith("image"):
                                st.image(f, width=150)
                            else:
                                st.markdown(f"📄 **{f.name}**")
                        with col_desc:
                            deskripsi_map[f.name] = st.text_area(
                                f"Ket. File: {f.name}",
                                height=70,
                                key=f"desc_{i}",
                                placeholder="Jelaskan aktivitas terkait file ini...",
                            )
            else:
                placeholder_text = "Jelaskan hasil kunjungan..." if is_kunjungan else "Jelaskan konten/ads/calls yang dikerjakan..."
                main_deskripsi = st.text_area("Deskripsi Aktivitas", placeholder=placeholder_text)

            st.markdown("</div>", unsafe_allow_html=True)  # Close card for input laporan

        # Container for Kesimpulan Harian
        with st.container():
            st.markdown("<div class='card'>", unsafe_allow_html=True)
            st.markdown("<h3>🏁 Kesimpulan Harian</h3>", unsafe_allow_html=True)
            st.caption("Bagian ini penting agar progress besok lebih terarah.")

            col_ref_1, col_ref_2, col_ref_3 = st.columns(3)
            with col_ref_1:
                input_kesimpulan = st.text_area("💡 Kesimpulan / Apa yang dicapai hari ini?", height=110)
            with col_ref_2:
                input_kendala = st.text_area("🚧 Kendala / Masalah (Internal)?", height=110)
            with col_ref_3:
                input_kendala_klien = st.text_area("🧑‍💼 Kendala dari Klien?", height=110)

            input_interest = st.radio(
                "📈 Tingkat Interest (Presentase)",
                ["Under 50% (A)", "50-75% (B)", "75%-100%"],
                horizontal=True,
                key="interest_persen",
            )

            c_lead1, c_lead2 = st.columns(2)
            with c_lead1:
                input_nama_klien = st.text_input("👤 Nama Klien yang Dihubungi", key="nama_klien_input")
            with c_lead2:
                input_kontak_klien = st.text_input("📞 No HP/WA Klien", key="kontak_klien_input")

            input_pending = st.text_input("📌 Next Plan / Pending Item (Akan jadi Reminder Besok)")

            if st.button("✅ Submit Laporan", type="primary"):
                valid = True
                if is_kunjungan and not str(lokasi_input).strip():
                    st.error("Untuk Sales (Kunjungan), Lokasi Wajib Diisi!")
                    valid = False
                if (not fotos) and (not str(main_deskripsi).strip()):
                    st.error("Deskripsi Wajib Diisi!")
                    valid = False

                if valid:
                    with st.spinner("Menyimpan dan memformat database..."):
                        rows = []
                        ts = now_ts_str()
                        final_lokasi = lokasi_input if is_kunjungan else kategori_aktivitas

                        val_kesimpulan = input_kesimpulan.strip() if str(input_kesimpulan).strip() else "-"
                        val_kendala = input_kendala.strip() if str(input_kendala).strip() else "-"
                        val_kendala_klien = input_kendala_klien.strip() if str(input_kendala_klien).strip() else "-"
                        val_pending = input_pending.strip() if str(input_pending).strip() else "-"
                        val_feedback = ""
                        val_interest = input_interest if input_interest else "-"
                        val_nama_klien = input_nama_klien.strip() if str(input_nama_klien).strip() else "-"
                        val_kontak_klien = input_kontak_klien.strip() if str(input_kontak_klien).strip() else "-"

                        if fotos and KONEKSI_DROPBOX_BERHASIL:
                            for f in fotos:
                                url = upload_ke_dropbox(f, nama_pelapor, "Laporan_Harian")
                                desc = deskripsi_map.get(f.name, "-")
                                rows.append(
                                    [
                                        ts,
                                        nama_pelapor,
                                        final_lokasi,
                                        desc,
                                        url,
                                        sosmed_link if sosmed_link else "-",
                                        val_kesimpulan,
                                        val_kendala,
                                        val_kendala_klien,
                                        val_pending,
                                        val_feedback,
                                        val_interest,
                                        val_nama_klien,
                                        val_kontak_klien,
                                    ]
                                )
                        else:
                            rows.append(
                                [
                                    ts,
                                    nama_pelapor,
                                    final_lokasi,
                                    main_deskripsi,
                                    "-",
                                    sosmed_link if sosmed_link else "-",
                                    val_kesimpulan,
                                    val_kendala,
                                    val_kendala_klien,
                                    val_pending,
                                    val_feedback,
                                    val_interest,
                                    val_nama_klien,
                                    val_kontak_klien,
                                ]
                            )

                        if simpan_laporan_harian_batch(rows, nama_pelapor):
                            st.success(f"Laporan Tersimpan! Reminder besok: {val_pending}")
                            st.balloons()
                            st.cache_data.clear()
                        else:
                            st.error("Gagal simpan.")

            st.markdown("</div>", unsafe_allow_html=True)  # Close card for kesimpulan

        with st.expander("📂 Log Data Mentah"):
            if st.button("🔄 Refresh"):
                st.cache_data.clear()
                st.rerun()

            df_log = load_all_reports(get_daftar_staf_terbaru())
            if not df_log.empty:
                st.dataframe(df_log, use_container_width=True, hide_index=True)
            else:
                st.info("Kosong")

    # -------------------------------------------------------------------------
    # Menu: Dashboard Admin
    elif menu_nav == "📊 Dashboard Admin":
        st.header("📊 Dashboard Produktivitas")
        st.info("Dashboard ini memisahkan analisa antara Sales dan Marketing.")

        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()

        df_log = load_all_reports(get_daftar_staf_terbaru())
        if df_log.empty:
            st.info("Belum ada data laporan.")
        else:
            try:
                df_log[COL_TIMESTAMP] = pd.to_datetime(df_log[COL_TIMESTAMP], format="%d-%m-%Y %H:%M:%S", errors="coerce")
                df_log["Tanggal"] = df_log[COL_TIMESTAMP].dt.date
            except Exception:
                df_log["Tanggal"] = today_jkt()

            keywords_digital = ["Digital", "Marketing", "Konten", "Ads", "Telesales", "Admin", "Follow"]

            def get_category(val: Any) -> str:
                val_str = str(val)
                if any(k in val_str for k in keywords_digital):
                    return "Digital/Internal"
                return "Kunjungan Lapangan"

            df_log["Kategori"] = df_log[COL_TEMPAT].apply(get_category)

            days = st.selectbox("Rentang Waktu:", [7, 14, 30], index=0)
            start_date = date.today() - timedelta(days=days)
            df_filt = df_log[df_log["Tanggal"] >= start_date]

            tab_sales, tab_marketing, tab_review, tab_galeri = st.tabs(
                ["🚗 Sales (Lapangan)", "💻 Marketing (Digital)", "📝 Review & Feedback", "🖼️ Galeri Bukti"]
            )

            with tab_sales:
                df_sales = df_filt[df_filt["Kategori"] == "Kunjungan Lapangan"]
                col1, col2 = st.columns(2)
                col1.metric("Total Kunjungan", len(df_sales))
                col2.metric("Sales Aktif", df_sales[COL_NAMA].nunique())
                if not df_sales.empty:
                    st.subheader("Top Visiting Sales")
                    st.bar_chart(df_sales[COL_NAMA].value_counts())
                    st.subheader("Lokasi Paling Sering Dikunjungi")
                    st.dataframe(df_sales[COL_TEMPAT].value_counts().head(5), use_container_width=True)
                else:
                    st.info("Tidak ada data kunjungan lapangan.")

            with tab_marketing:
                df_mkt = df_filt[df_filt["Kategori"] == "Digital/Internal"]
                col1, col2 = st.columns(2)
                col1.metric("Total Output", len(df_mkt))
                col2.metric("Marketer Aktif", df_mkt[COL_NAMA].nunique())
                if not df_mkt.empty:
                    st.subheader("Produktivitas Tim Digital")
                    if HAS_PLOTLY:
                        fig = px.pie(df_mkt, names=COL_NAMA, title="Distribusi Beban Kerja Digital")
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.bar_chart(df_mkt[COL_NAMA].value_counts())
                    st.subheader("Jenis Tugas Digital")
                    st.bar_chart(df_mkt[COL_TEMPAT].value_counts())
                else:
                    st.info("Tidak ada data aktivitas digital.")

            with tab_review:
                st.subheader("📝 Review Catatan Harian & Feedback")
                st.caption("Monitoring kendala dan memberikan feedback langsung per individu.")

                with st.expander("📇 Tarik Data Nama & No HP per Tingkat Interest", expanded=True):
                    if COL_INTEREST not in df_filt.columns:
                        st.warning("Kolom Interest (%) belum ada di data.")
                    else:
                        st.session_state.setdefault("filter_interest_admin", "Under 50% (A)")
                        b1, b2, b3 = st.columns(3)
                        if b1.button("Tarik Under 50% (A)"):
                            st.session_state["filter_interest_admin"] = "Under 50% (A)"
                        if b2.button("Tarik 50-75% (B)"):
                            st.session_state["filter_interest_admin"] = "50-75% (B)"
                        if b3.button("Tarik 75%-100%"):
                            st.session_state["filter_interest_admin"] = "75%-100%"

                        selected_interest = st.session_state["filter_interest_admin"]
                        st.info(f"Filter aktif: **{selected_interest}**")

                        df_tmp = df_filt.copy()
                        for c in [COL_NAMA_KLIEN, COL_KONTAK_KLIEN, COL_INTEREST]:
                            if c not in df_tmp.columns:
                                df_tmp[c] = ""

                        df_tmp[COL_INTEREST] = df_tmp[COL_INTEREST].astype(str).fillna("").str.strip()
                        df_interest = df_tmp[df_tmp[COL_INTEREST] == selected_interest].copy()

                        cols_out = [c for c in [COL_TIMESTAMP, COL_NAMA, COL_NAMA_KLIEN, COL_KONTAK_KLIEN, COL_INTEREST, COL_TEMPAT, COL_DESKRIPSI, COL_KENDALA_KLIEN] if c in df_interest.columns]
                        df_out = df_interest[cols_out].copy() if cols_out else df_interest.copy()
                        st.dataframe(df_out, use_container_width=True, hide_index=True)

                        if HAS_OPENPYXL:
                            df_export = df_out.copy()
                            if COL_TIMESTAMP in df_export.columns and pd.api.types.is_datetime64_any_dtype(df_export[COL_TIMESTAMP]):
                                df_export[COL_TIMESTAMP] = df_export[COL_TIMESTAMP].dt.strftime("%d-%m-%Y %H:%M:%S")

                            excel_bytes = df_to_excel_bytes(df_export, sheet_name="Data_Interest", wrap_cols=[COL_DESKRIPSI, COL_TEMPAT, COL_KENDALA_KLIEN])
                            safe_name = selected_interest.replace("%", "").replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")
                            if excel_bytes:
                                st.download_button(
                                    "⬇️ Download Excel (sesuai filter)",
                                    data=excel_bytes,
                                    file_name=f"data_klien_{safe_name}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    use_container_width=True,
                                )

                        df_export_csv = df_out.copy()
                        if COL_TIMESTAMP in df_export_csv.columns and pd.api.types.is_datetime64_any_dtype(df_export_csv[COL_TIMESTAMP]):
                            df_export_csv[COL_TIMESTAMP] = df_export_csv[COL_TIMESTAMP].dt.strftime("%d-%m-%Y %H:%M:%S")

                        csv_data = df_export_csv.to_csv(index=False).encode("utf-8")
                        safe_name = selected_interest.replace("%", "").replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")
                        st.download_button(
                            "⬇️ Download CSV (sesuai filter)",
                            data=csv_data,
                            file_name=f"data_klien_{safe_name}.csv",
                            mime="text/csv",
                            use_container_width=True,
                        )

                df_review = df_filt.sort_values(by=COL_TIMESTAMP, ascending=False)
                if df_review.empty:
                    st.info("Belum ada data laporan pada rentang waktu ini.")
                else:
                    for _, row in df_review.iterrows():
                        with st.container():
                            c_head1, c_head2 = st.columns([3, 1])
                            with c_head1:
                                st.markdown(f"### 👤 {row.get(COL_NAMA, '-')}")
                                st.caption(f"📅 {row.get(COL_TIMESTAMP, '-')} | 🏷️ {row.get('Kategori', '-')}")

                            c_body, c_img = st.columns([3, 1])
                            with c_body:
                                st.markdown(f"**📍 Aktivitas/Lokasi:** {row.get(COL_TEMPAT, '-')}")
                                st.markdown(f"**📝 Deskripsi:** {row.get(COL_DESKRIPSI, '-')}")

                                nama_klien_val = row.get(COL_NAMA_KLIEN, "-") or "-"
                                kontak_klien_val = row.get(COL_KONTAK_KLIEN, "-") or "-"
                                interest_val = row.get(COL_INTEREST, "-") or "-"

                                st.markdown(f"**👤 Klien:** {nama_klien_val}  |  **📞 No HP/WA:** {kontak_klien_val}")
                                st.markdown(f"**📈 Interest:** {interest_val}")

                                st.divider()
                                col_a, col_b, col_c, col_d = st.columns(4)
                                with col_a:
                                    st.info(f"💡 **Hasil/Kesimpulan:**\n\n{row.get(COL_KESIMPULAN, '-')}")
                                with col_b:
                                    st.warning(f"🚧 **Kendala (Internal):**\n\n{row.get(COL_KENDALA, '-')}")
                                with col_c:
                                    st.warning(f"🧑‍💼 **Kendala Klien:**\n\n{row.get(COL_KENDALA_KLIEN, '-')}")
                                with col_d:
                                    st.error(f"📌 **Next Plan:**\n\n{row.get(COL_PENDING, '-')}")

                                st.divider()
                                existing_feed = row.get(COL_FEEDBACK, "") or ""
                                with st.expander(f"💬 Beri Feedback untuk {row.get(COL_NAMA, '-')}", expanded=False):
                                    unique_key = f"feed_{row.get(COL_NAMA, '-')}_{row.get(COL_TIMESTAMP, '-')}
                                    input_feed = st.text_area("Tulis Masukan/Arahan:", value=str(existing_feed), key=unique_key)

                                    if st.button("Kirim Feedback 🚀", key=f"btn_{unique_key}"):
                                        if input_feed:
                                            ts_val = row.get(COL_TIMESTAMP)
                                            ts_str = ts_val.strftime("%d-%m-%Y %H:%M:%S") if hasattr(ts_val, "strftime") else str(ts_val)
                                            res, msg = kirim_feedback_admin(row.get(COL_NAMA, ""), ts_str, input_feed)
                                            if res:
                                                st.toast("Feedback terkirim!", icon="✅")
                                                st.cache_data.clear()
                                                st.rerun()
                                            else:
                                                st.error(msg)

                            with c_img:
                                link_foto = str(row.get(COL_LINK_FOTO, ""))
                                if "http" in link_foto:
                                    url_asli = link_foto
                                    direct_url = url_asli.replace("www.dropbox.com", "dl.dropboxusercontent.com").replace("?dl=0", "")
                                    try:
                                        st.image(direct_url, use_container_width=True)
                                        st.caption("Bukti Foto")
                                    except Exception:
                                        st.caption("Gagal load foto")

            with tab_galeri:
                st.caption("Menampilkan bukti foto/dokumen terbaru")
                if COL_LINK_FOTO in df_filt.columns:
                    df_foto = df_filt[df_filt[COL_LINK_FOTO].astype(str).str.contains("http", na=False, case=False)].sort_values(by=COL_TIMESTAMP, ascending=False).head(12)
                else:
                    df_foto = pd.DataFrame()

                if df_foto.empty:
                    st.info("Belum ada bukti yang terupload.")
                else:
                    data_dict = df_foto.to_dict("records")
                    cols = st.columns(4)
                    for idx, row in enumerate(data_dict):
                        with cols[idx % 4]:
                            with st.container():
                                url_asli = str(row.get(COL_LINK_FOTO, ""))
                                nama = row.get(COL_NAMA, "-")
                                tempat = row.get(COL_TEMPAT, "-")
                                direct_url = url_asli.replace("www.dropbox.com", "dl.dropboxusercontent.com").replace("?dl=0", "")
                                try:
                                    st.image(direct_url, use_container_width=True)
                                    st.markdown(f"**{nama}**")
                                    st.caption(f"📍 {tempat}")
                                except Exception:
                                    st.error("Gagal load gambar")
                                    st.link_button("Buka Link", url_asli)


if __name__ == "__main__":
    main()
