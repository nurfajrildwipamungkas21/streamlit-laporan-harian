# app.py
# =========================================================
# Sales & Marketing Action Center (Streamlit)
# Merged Version: Full Features + Pro UI/UX
# =========================================================

from __future__ import annotations
import hashlib
import hmac
import io
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
import dropbox
import gspread
from dropbox.exceptions import ApiError, AuthError
from dropbox.sharing import RequestedVisibility, SharedLinkSettings
from google.oauth2.service_account import Credentials

# =========================================================
# OPTIONAL LIBS (Excel Export / AgGrid / Plotly)
# =========================================================
try:
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter
    from openpyxl.utils.dataframe import dataframe_to_rows
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False

try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
    HAS_AGGRID = True
except ImportError:
    HAS_AGGRID = False

try:
    import plotly.express as px
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

# =========================================================
# LOGGING SETUP
# =========================================================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")

# =========================================================
# PAGE CONFIG & CUSTOM CSS (UI/UX PROPER)
# =========================================================
st.set_page_config(
    page_title="Sales & Marketing Action Center",
    page_icon="🚀",
    layout="wide"
)

# Custom CSS untuk mempercantik UI
st.markdown("""
    <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        
        /* Typography */
        h1, h2, h3 {
            font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
            color: #2C3E50;
        }
        
        /* Input Fields Styling */
        .stTextInput input, .stTextArea textarea, .stDateInput input, .stSelectbox div[data-baseweb="select"] {
            border-radius: 8px;
            border: 1px solid #E0E0E0;
            padding: 10px;
        }
        
        /* Button Styling */
        .stButton button {
            border-radius: 8px;
            font-weight: 600;
            transition: all 0.3s ease;
        }
        
        /* Card/Expander Styling Override */
        div[data-testid="stExpander"] {
            background-color: #ffffff;
            border-radius: 8px;
            border: 1px solid #F0F2F6;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }
        
        /* Status Badges in Tables */
        div[data-testid="stDataFrame"] {
            font-size: 14px;
        }
    </style>
""", unsafe_allow_html=True)

# =========================================================
# TIMEZONE & CONSTANTS
# =========================================================
try:
    TZ_JKT = ZoneInfo("Asia/Jakarta")
except Exception:
    TZ_JKT = None  # Fallback system tz

def _now() -> datetime:
    return datetime.now(tz=TZ_JKT) if TZ_JKT else datetime.now()

def now_ts_str() -> str:
    """Timestamp akurat (WIB) untuk semua perubahan."""
    return _now().strftime("%d-%m-%Y %H:%M:%S")

# --- SHEET NAMES ---
NAMA_GOOGLE_SHEET = "Laporan Kegiatan Harian"
FOLDER_DROPBOX = "/Laporan_Kegiatan_Harian"

SHEET_CONFIG_NAMA = "Config_Staf"
SHEET_TARGET_TEAM = "Target_Team_Checklist"
SHEET_TARGET_INDIVIDU = "Target_Individu_Checklist"
SHEET_CONFIG_TEAM = "Config_Team"
SHEET_CLOSING_DEAL = "Closing_Deal"
SHEET_PEMBAYARAN = "Pembayaran_DP"

# --- COLUMN NAMES ---
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
    COL_TIMESTAMP, COL_NAMA, COL_TEMPAT, COL_DESKRIPSI,
    COL_LINK_FOTO, COL_LINK_SOSMED,
    COL_KESIMPULAN, COL_KENDALA, COL_KENDALA_KLIEN,
    COL_PENDING, COL_FEEDBACK, COL_INTEREST,
    COL_NAMA_KLIEN, COL_KONTAK_KLIEN
]

# Audit & Logic Columns
COL_TS_UPDATE = "Timestamp Update (Log)"
COL_UPDATED_BY = "Updated By"
TEAM_COL_NAMA_TEAM = "Nama Team"
TEAM_COL_POSISI = "Posisi"
TEAM_COL_ANGGOTA = "Nama Anggota"
TEAM_COLUMNS = [TEAM_COL_NAMA_TEAM, TEAM_COL_POSISI, TEAM_COL_ANGGOTA]

# Closing Deal
COL_GROUP = "Nama Group"
COL_MARKETING = "Nama Marketing"
COL_TGL_EVENT = "Tanggal Event"
COL_BIDANG = "Bidang"
COL_NILAI_KONTRAK = "Nilai Kontrak"
CLOSING_COLUMNS = [COL_GROUP, COL_MARKETING, COL_TGL_EVENT, COL_BIDANG, COL_NILAI_KONTRAK]

# Checklist
TEAM_CHECKLIST_COLUMNS = ["Misi", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan", COL_TS_UPDATE, COL_UPDATED_BY]
INDIV_CHECKLIST_COLUMNS = ["Nama", "Target", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan", COL_TS_UPDATE, COL_UPDATED_BY]

# Payment
COL_TS_BAYAR = "Timestamp Input"
COL_JENIS_BAYAR = "Jenis Pembayaran"
COL_NOMINAL_BAYAR = "Nominal Pembayaran"
COL_JATUH_TEMPO = "Batas Waktu Bayar"
COL_STATUS_BAYAR = "Status Pembayaran"
COL_BUKTI_BAYAR = "Bukti Pembayaran"
COL_CATATAN_BAYAR = "Catatan"

PAYMENT_COLUMNS = [
    COL_TS_BAYAR, COL_GROUP, COL_MARKETING, COL_TGL_EVENT,
    COL_JENIS_BAYAR, COL_NOMINAL_BAYAR, COL_JATUH_TEMPO,
    COL_STATUS_BAYAR, COL_BUKTI_BAYAR, COL_CATATAN_BAYAR,
    COL_TS_UPDATE, COL_UPDATED_BY
]

# =========================================================
# HELPERS (Safe Str, Dates, Auth, Parsing)
# =========================================================
def safe_str(x, default="") -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return default
        s = str(x)
        if s.lower() in {"nan", "none"}:
            return default
        return s
    except Exception:
        return default

def normalize_bool(x) -> bool:
    if isinstance(x, bool):
        return x
    s = safe_str(x, "").strip().upper()
    return True if s == "TRUE" else False

def normalize_date(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    try:
        return pd.to_datetime(x, errors="coerce").date()
    except Exception:
        return None

def get_actor_fallback(default="-") -> str:
    for k in ["pelapor_main", "sidebar_user"]:
        if k in st.session_state and safe_str(st.session_state.get(k), "").strip():
            return safe_str(st.session_state.get(k)).strip()
    return default

def verify_admin_password(pwd_input: str) -> bool:
    pwd_input = safe_str(pwd_input, "").strip()
    if not pwd_input:
        return False
    
    # Mode Hash
    try:
        hash_secret = st.secrets.get("password_admin_hash", None)
        if hash_secret and safe_str(hash_secret, "").strip():
            digest = hashlib.sha256(pwd_input.encode("utf-8")).hexdigest()
            return hmac.compare_digest(digest, safe_str(hash_secret, "").strip())
    except Exception:
        pass

    # Mode Plain
    try:
        plain_secret = st.secrets.get("password_admin", None)
        if plain_secret and safe_str(plain_secret, "").strip():
            return hmac.compare_digest(pwd_input, safe_str(plain_secret, "").strip())
    except Exception:
        pass

    return False

def admin_secret_configured() -> bool:
    try:
        return bool(
            safe_str(st.secrets.get("password_admin_hash", ""), "").strip()
            or safe_str(st.secrets.get("password_admin", ""), "").strip()
        )
    except Exception:
        return False

# --- RUPIAH PARSER ---
def parse_rupiah_to_int(value) -> Optional[int]:
    if value is None: return None
    if isinstance(value, (int, float)) and not pd.isna(value):
        try:
            return int(round(float(value)))
        except: return None
    
    s = str(value).strip().lower()
    if s in {"nan", "none", "-", "null"}: return None
    
    s = re.sub(r"\s+", "", s).replace("idr", "").replace("rp", "")
    multiplier = 1
    if "miliar" in s or "milyar" in s: multiplier = 1_000_000_000
    elif "jt" in s or "juta" in s: multiplier = 1_000_000
    elif "rb" in s or "ribu" in s: multiplier = 1_000
    
    s_num = re.sub(r"(miliar|milyar|juta|jt|ribu|rb)", "", s)
    s_num = re.sub(r"[^0-9.,]", "", s_num)
    if not s_num: return None

    try:
        if "," in s_num and "." in s_num:
            if s_num.rfind(",") > s_num.rfind("."): # 1.000,50
                cleaned = s_num.replace(".", "").replace(",", ".")
            else: # 1,000.50
                cleaned = s_num.replace(",", "")
            base = float(cleaned)
        elif "," in s_num:
            if s_num.count(",") > 1: base = float(s_num.replace(",", ""))
            elif len(s_num.split(",")[1]) == 3: base = float(s_num.replace(",", ""))
            else: base = float(s_num.replace(",", "."))
        elif "." in s_num:
            if s_num.count(".") > 1: base = float(s_num.replace(".", ""))
            elif len(s_num.split(".")[1]) == 3: base = float(s_num.replace(".", ""))
            else: base = float(s_num)
        else:
            base = float(s_num)
    except:
        digits = re.sub(r"\D", "", s_num)
        return int(digits) if digits else None

    return int(round(base * multiplier))

def format_rupiah_display(amount) -> str:
    try:
        if amount is None or pd.isna(amount): return ""
        n = int(amount)
        return "Rp " + f"{n:,}".replace(",", ".")
    except: return str(amount)

# --- AUDIT LOG HELPERS ---
def parse_payment_log_lines(log_text: str):
    log_text = safe_str(log_text, "").strip()
    if not log_text: return []
    raw_lines = [ln.rstrip() for ln in log_text.splitlines() if ln.strip()]
    out = []
    for ln in raw_lines:
        mnum = re.match(r"^\s*\d+\.\s*(.*)$", ln)
        if mnum: ln = mnum.group(1).rstrip()
        
        m = re.match(r"^\[(.*?)\]\s*\((.*?)\)\s*(.*)$", ln)
        if m:
            ts, actor, rest = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
            prefix = f"[{ts}] ({actor})"
            if rest:
                parts = [p.strip() for p in rest.split(";") if p.strip()]
                if parts:
                    out.append(f"{prefix} {parts[0]}")
                    for p in parts[1:]: out.append(f" {p}")
                else: out.append(prefix)
            else: out.append(prefix)
        else:
            out.append(ln)
    return out

def build_numbered_log(lines):
    lines = [str(l).rstrip() for l in (lines or []) if safe_str(l, "").strip()]
    return "\n".join([f"{i}. {line}" for i, line in enumerate(lines, 1)]).strip()

def _fmt_payment_val_for_log(col_name: str, v):
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

def append_payment_ts_update(existing_log: str, ts: str, actor: str, changes):
    lines = parse_payment_log_lines(existing_log)
    changes = [safe_str(c, "").strip() for c in (changes or []) if safe_str(c, "").strip()]
    if not changes: return build_numbered_log(lines)
    
    actor = safe_str(actor, "-").strip() or "-"
    ts = safe_str(ts, now_ts_str()).strip() or now_ts_str()
    
    lines.append(f"[{ts}] ({actor}) {changes[0]}")
    for c in changes[1:]: lines.append(f" {c}")
    
    return build_numbered_log(lines)

# --- EXCEL EXPORT ---
def df_to_excel_bytes(df: pd.DataFrame, sheet_name="Sheet1", col_widths=None, wrap_cols=None, right_align_cols=None, number_format_cols=None):
    if not HAS_OPENPYXL: return None
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
            ws.column_dimensions[col_letter].width = 20
            
        for cell in ws[col_letter][1:]:
            wrap = col_name in wrap_cols
            horiz = "right" if col_name in right_align_cols else "left"
            cell.alignment = Alignment(vertical="top", horizontal=horiz, wrap_text=wrap)
            if col_name in number_format_cols:
                cell.number_format = number_format_cols[col_name]
                
    wb.save(output)
    return output.getvalue()

# =========================================================
# CONNECTIONS (GSheet & Dropbox)
# =========================================================
KONEKSI_GSHEET_BERHASIL = False
KONEKSI_DROPBOX_BERHASIL = False
spreadsheet = None
dbx = None

# 1) Google Sheets
try:
    if "gcp_service_account" in st.secrets:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds_dict = dict(st.secrets["gcp_service_account"])
        if "private_key" in creds_dict:
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open(NAMA_GOOGLE_SHEET)
        KONEKSI_GSHEET_BERHASIL = True
    else:
        st.error("GSheet Error: Secret 'gcp_service_account' tidak ditemukan.")
except Exception as e:
    log.error(f"GSheet Connection Error: {e}")
    st.error(f"GSheet Error: {e}")

# 2) Dropbox
try:
    if "dropbox" in st.secrets and "access_token" in st.secrets["dropbox"]:
        dbx = dropbox.Dropbox(st.secrets["dropbox"]["access_token"])
        dbx.users_get_current_account()
        KONEKSI_DROPBOX_BERHASIL = True
except AuthError:
    st.error("Dropbox Error: Token Autentikasi tidak valid.")
except Exception as e:
    log.error(f"Dropbox Connection Error: {e}")

# =========================================================
# SHEET LOGIC & FORMATTING
# =========================================================
def _build_currency_number_format_rupiah():
    return {"type": "CURRENCY", "pattern": '"Rp" #,##0'}

def auto_format_sheet(worksheet):
    try:
        sheet_id = worksheet.id
        all_values = worksheet.get_all_values()
        if not all_values: return
        
        headers = all_values[0]
        rows = max(worksheet.row_count, len(all_values))
        requests = []
        
        # Reset Base
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": rows},
                "cell": {"userEnteredFormat": {"verticalAlignment": "TOP", "wrapStrategy": "CLIP"}},
                "fields": "userEnteredFormat(verticalAlignment,wrapStrategy)"
            }
        })
        
        # Columns Config
        long_text_cols = {
            "Misi", "Target", "Deskripsi", "Bukti/Catatan", "Link Foto", "Link Sosmed",
            "Tempat Dikunjungi", "Kesimpulan", "Kendala", "Next Plan (Pending)", "Feedback Lead",
            COL_KENDALA_KLIEN, COL_NAMA_KLIEN, TEAM_COL_NAMA_TEAM, TEAM_COL_POSISI, TEAM_COL_ANGGOTA,
            COL_GROUP, COL_MARKETING, COL_BIDANG, COL_JENIS_BAYAR, COL_BUKTI_BAYAR, COL_CATATAN_BAYAR, COL_TS_UPDATE
        }
        
        for i, col_name in enumerate(headers):
            width = 100
            fmt = {}
            
            if col_name in long_text_cols:
                width = 360 if col_name == COL_TS_UPDATE else 300
                fmt["wrapStrategy"] = "WRAP"
            elif col_name in {"Tgl_Mulai", "Tgl_Selesai", "Timestamp", COL_TGL_EVENT, COL_JATUH_TEMPO, COL_TS_BAYAR}:
                width = 160 if col_name in {"Timestamp", COL_TS_BAYAR} else 120
                fmt["horizontalAlignment"] = "CENTER"
            elif col_name in {"Status", "Done?", COL_STATUS_BAYAR}:
                width = 130 if col_name == COL_STATUS_BAYAR else 80
                fmt["horizontalAlignment"] = "CENTER"
            elif col_name in {COL_NILAI_KONTRAK, COL_NOMINAL_BAYAR}:
                width = 180
                fmt["horizontalAlignment"] = "RIGHT"
                fmt["numberFormat"] = _build_currency_number_format_rupiah()
            
            requests.append({
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i+1},
                    "properties": {"pixelSize": width},
                    "fields": "pixelSize"
                }
            })
            
            if fmt:
                fields = ",".join(fmt.keys())
                requests.append({
                    "repeatCell": {
                        "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": rows, "startColumnIndex": i, "endColumnIndex": i+1},
                        "cell": {"userEnteredFormat": fmt},
                        "fields": f"userEnteredFormat({fields})"
                    }
                })
        
        # Header Style
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {
                    "textFormat": {"bold": True},
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE",
                    "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
                    "wrapStrategy": "WRAP"
                }},
                "fields": "userEnteredFormat(textFormat,horizontalAlignment,verticalAlignment,backgroundColor,wrapStrategy)"
            }
        })
        
        # Freeze Header
        requests.append({
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount"
            }
        })
        
        worksheet.spreadsheet.batch_update({"requests": requests})
    except Exception as e:
        log.warning(f"Formatting warning: {e}")

def ensure_headers(worksheet, desired_headers):
    try:
        if worksheet.col_count < len(desired_headers):
            worksheet.resize(cols=len(desired_headers))
        headers = worksheet.row_values(1)
        if not headers or len(headers) < len(desired_headers) or headers[:len(desired_headers)] != desired_headers:
            worksheet.update(range_name="A1", values=[desired_headers], value_input_option="USER_ENTERED")
            auto_format_sheet(worksheet)
    except Exception as e:
        log.error(f"Header Ensure Error: {e}")

@st.cache_resource(ttl=60)
def get_or_create_worksheet(nama_worksheet):
    try:
        ws = spreadsheet.worksheet(nama_worksheet)
        ensure_headers(ws, NAMA_KOLOM_STANDAR)
        return ws
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=nama_worksheet, rows=100, cols=len(NAMA_KOLOM_STANDAR))
        ws.append_row(NAMA_KOLOM_STANDAR, value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return ws
    except Exception:
        return None

# =========================================================
# DATA LOADERS & ACTIONS
# =========================================================

# --- STAFF ---
@st.cache_data(ttl=60)
def get_daftar_staf_terbaru():
    default_staf = ["Saya"]
    if not KONEKSI_GSHEET_BERHASIL: return default_staf
    try:
        try:
            ws = spreadsheet.worksheet(SHEET_CONFIG_NAMA)
        except:
            ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_NAMA, rows=100, cols=1)
            ws.append_row(["Daftar Nama Staf"], value_input_option="USER_ENTERED")
            ws.append_row(["Saya"], value_input_option="USER_ENTERED")
            auto_format_sheet(ws)
            return default_staf
        
        nama_list = ws.col_values(1)
        if nama_list and nama_list[0] == "Daftar Nama Staf": nama_list.pop(0)
        return nama_list if nama_list else default_staf
    except: return default_staf

def tambah_staf_baru(nama_baru):
    try:
        try: ws = spreadsheet.worksheet(SHEET_CONFIG_NAMA)
        except: ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_NAMA, rows=100, cols=1)
        
        if nama_baru in ws.col_values(1): return False, "Nama sudah ada!"
        ws.append_row([nama_baru], value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return True, "Berhasil tambah tim!"
    except Exception as e: return False, str(e)

# --- TEAM CONFIG ---
@st.cache_data(ttl=60)
def load_team_config():
    if not KONEKSI_GSHEET_BERHASIL: return pd.DataFrame(columns=TEAM_COLUMNS)
    try:
        try: ws = spreadsheet.worksheet(SHEET_CONFIG_TEAM)
        except:
            ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_TEAM, rows=300, cols=len(TEAM_COLUMNS))
            ws.append_row(TEAM_COLUMNS, value_input_option="USER_ENTERED")
            auto_format_sheet(ws)
            return pd.DataFrame(columns=TEAM_COLUMNS)
        
        data = ws.get_all_records()
        df = pd.DataFrame(data).fillna("")
        for c in TEAM_COLUMNS:
            if c not in df.columns: df[c] = ""
        return df[TEAM_COLUMNS].copy()
    except: return pd.DataFrame(columns=TEAM_COLUMNS)

def tambah_team_baru(nama_team, posisi, anggota_list):
    if not KONEKSI_GSHEET_BERHASIL: return False, "Koneksi Error"
    try:
        ws = spreadsheet.worksheet(SHEET_CONFIG_TEAM)
    except:
        ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_TEAM, rows=300, cols=len(TEAM_COLUMNS))
        ws.append_row(TEAM_COLUMNS, value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
    
    rows_to_add = [[nama_team, posisi, a] for a in anggota_list]
    ws.append_rows(rows_to_add, value_input_option="USER_ENTERED")
    auto_format_sheet(ws)
    return True, f"Team '{nama_team}' ditambahkan."

# --- DROPBOX ---
def upload_ke_dropbox(file_obj, nama_staf, kategori="Umum"):
    if not KONEKSI_DROPBOX_BERHASIL or dbx is None: return "-"
    try:
        file_data = file_obj.getvalue()
        ts = _now().strftime("%Y%m%d_%H%M%S")
        clean_filename = "".join([c for c in file_obj.name if c.isalnum() or c in (".", "_")])
        clean_user = "".join([c for c in nama_staf if c.isalnum() or c in (" ", "_")]).replace(" ", "_")
        clean_cat = "".join([c for c in kategori if c.isalnum() or c in (" ", "_")]).replace(" ", "_")
        
        path = f"{FOLDER_DROPBOX}/{clean_user}/{clean_cat}/{ts}_{clean_filename}"
        dbx.files_upload(file_data, path, mode=dropbox.files.WriteMode.add)
        
        settings = SharedLinkSettings(requested_visibility=RequestedVisibility.public)
        try:
            link = dbx.sharing_create_shared_link_with_settings(path, settings=settings)
        except ApiError as e:
            if e.error.is_shared_link_already_exists():
                link = dbx.sharing_list_shared_links(path, direct_only=True).links[0]
            else: return "-"
        return link.url.replace("?dl=0", "?raw=1")
    except Exception: return "-"

# --- CHECKLIST ---
def load_checklist(sheet_name, columns):
    try:
        try: ws = spreadsheet.worksheet(sheet_name)
        except:
            ws = spreadsheet.add_worksheet(title=sheet_name, rows=200, cols=len(columns))
            ws.append_row(columns, value_input_option="USER_ENTERED")
            auto_format_sheet(ws)
            return pd.DataFrame(columns=columns)
        
        ensure_headers(ws, columns)
        data = ws.get_all_records()
        df = pd.DataFrame(data).fillna("")
        for col in columns:
            if col not in df.columns:
                df[col] = False if col == "Status" else ""
        if "Status" in df.columns:
            df["Status"] = df["Status"].apply(lambda x: True if str(x).upper() == "TRUE" else False)
        return df[columns].copy()
    except: return pd.DataFrame(columns=columns)

def save_checklist(sheet_name, df, columns):
    try:
        ws = spreadsheet.worksheet(sheet_name)
        ensure_headers(ws, columns)
        ws.clear()
        
        rows_needed = len(df) + 1
        if ws.row_count < rows_needed: ws.resize(rows=rows_needed)
        
        df_save = df.copy().fillna("")
        for c in columns:
            if c not in df_save.columns: df_save[c] = ""
        if "Status" in df_save.columns:
            df_save["Status"] = df_save["Status"].apply(lambda x: "TRUE" if bool(x) else "FALSE")
            
        data_to_save = [df_save.columns.values.tolist()] + df_save[columns].astype(str).values.tolist()
        ws.update(range_name="A1", values=data_to_save, value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return True
    except: return False

def apply_audit_checklist_changes(df_before, df_after, key_cols, actor):
    if df_after is None or df_after.empty: return df_after
    actor = safe_str(actor, "-").strip() or "-"
    before = df_before.copy() if df_before is not None else pd.DataFrame()
    after = df_after.copy()
    
    for c in [COL_TS_UPDATE, COL_UPDATED_BY]:
        if c not in after.columns: after[c] = ""
        
    def make_key(r): return tuple(safe_str(r.get(k, "")).strip() for k in key_cols)
    before_map = {make_key(r): r.to_dict() for _, r in before.iterrows()}
    
    ts = now_ts_str()
    for idx, r in after.iterrows():
        k = make_key(r)
        prev = before_map.get(k, None)
        if prev is None:
            after.at[idx, COL_TS_UPDATE] = ts
            after.at[idx, COL_UPDATED_BY] = actor
            continue
            
        changed = False
        if normalize_bool(prev.get("Status")) != normalize_bool(r.get("Status")): changed = True
        if safe_str(prev.get("Bukti/Catatan"), "").strip() != safe_str(r.get("Bukti/Catatan"), "").strip(): changed = True
        
        if changed:
            after.at[idx, COL_TS_UPDATE] = ts
            after.at[idx, COL_UPDATED_BY] = actor
            
    return after

def add_bulk_targets(sheet_name, base_row_data, targets_list):
    try:
        try: ws = spreadsheet.worksheet(sheet_name)
        except: return False
        
        columns = TEAM_CHECKLIST_COLUMNS if sheet_name == SHEET_TARGET_TEAM else INDIV_CHECKLIST_COLUMNS
        ensure_headers(ws, columns)
        
        actor = get_actor_fallback("Admin")
        ts = now_ts_str()
        rows = []
        
        for t in targets_list:
            new_row = list(base_row_data) + [""] * (len(columns) - len(base_row_data))
            if sheet_name == SHEET_TARGET_TEAM: new_row[0] = t
            else: new_row[1] = t
            
            if COL_TS_UPDATE in columns: new_row[columns.index(COL_TS_UPDATE)] = ts
            if COL_UPDATED_BY in columns: new_row[columns.index(COL_UPDATED_BY)] = actor
            rows.append(new_row[:len(columns)])
            
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return True
    except: return False

def update_evidence_row(sheet_name, target_name, note, file_obj, user_folder, category):
    try:
        ws = spreadsheet.worksheet(sheet_name)
        columns = TEAM_CHECKLIST_COLUMNS if sheet_name == SHEET_TARGET_TEAM else INDIV_CHECKLIST_COLUMNS
        ensure_headers(ws, columns)
        
        df = pd.DataFrame(ws.get_all_records()).fillna("")
        key_col = "Misi" if sheet_name == SHEET_TARGET_TEAM else "Target"
        matches = df.index[df[key_col] == target_name].tolist()
        if not matches: return False, "Target tidak ditemukan."
        
        row_idx = matches[0]
        row_gsheet = row_idx + 2
        
        link = upload_ke_dropbox(file_obj, user_folder, category) if file_obj else ""
        old_note = str(df.at[row_idx, "Bukti/Catatan"]) if "Bukti/Catatan" in df.columns else ""
        if old_note in {"-", "nan"}: old_note = ""
        
        ts = now_ts_str()
        update_txt = f"[{ts}] {note}."
        if link and link != "-": update_txt += f" [FOTO: {link}]"
        
        final_note = f"{old_note}\n{update_txt}".strip() if old_note.strip() else update_txt
        
        headers = ws.row_values(1)
        col_idx = headers.index("Bukti/Catatan") + 1
        ws.update_cell(row_gsheet, col_idx, final_note)
        
        if COL_TS_UPDATE in headers:
            ws.update_cell(row_gsheet, headers.index(COL_TS_UPDATE)+1, ts)
        if COL_UPDATED_BY in headers:
            ws.update_cell(row_gsheet, headers.index(COL_UPDATED_BY)+1, user_folder)
            
        auto_format_sheet(ws)
        return True, "Berhasil update!"
    except Exception as e: return False, str(e)

# --- DAILY REPORTS ---
def simpan_laporan_harian_batch(rows, nama_staf):
    try:
        ws = get_or_create_worksheet(nama_staf)
        if not ws: return False
        ensure_headers(ws, NAMA_KOLOM_STANDAR)
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return True
    except Exception as e:
        log.error(f"Daily Report Error: {e}")
        return False

@st.cache_data(ttl=60)
def load_all_reports(daftar_staf):
    all_data = []
    for nama in daftar_staf:
        try:
            ws = get_or_create_worksheet(nama)
            if ws:
                d = ws.get_all_records()
                if d: all_data.extend(d)
        except: pass
    return pd.DataFrame(all_data) if all_data else pd.DataFrame(columns=NAMA_KOLOM_STANDAR)

@st.cache_data(ttl=30)
def get_reminder_pending(nama_staf):
    try:
        ws = get_or_create_worksheet(nama_staf)
        if not ws: return None
        vals = ws.get_all_records()
        if not vals: return None
        pending = vals[-1].get(COL_PENDING, "")
        return pending if str(pending).strip() not in {"-", ""} else None
    except: return None

def kirim_feedback_admin(nama_staf, timestamp_key, isi_feedback):
    try:
        ws = spreadsheet.worksheet(nama_staf)
        ensure_headers(ws, NAMA_KOLOM_STANDAR)
        headers = ws.row_values(1)
        if COL_FEEDBACK not in headers:
            return False, "Kolom feedback error."
            
        all_ts = ws.col_values(1)
        clean_target = "".join(filter(str.isdigit, str(timestamp_key)))
        row_found = None
        for i, val in enumerate(all_ts):
            if "".join(filter(str.isdigit, str(val))) == clean_target:
                row_found = i + 1
                break
        
        if not row_found: return False, "Data tidak ditemukan."
        
        ts = now_ts_str()
        actor = get_actor_fallback("Admin")
        ws.update_cell(row_found, headers.index(COL_FEEDBACK)+1, f"[{ts}] ({actor}) {isi_feedback}")
        return True, "Feedback terkirim!"
    except Exception as e: return False, str(e)

# --- CLOSING DEAL ---
@st.cache_data(ttl=60)
def load_closing_deal():
    if not KONEKSI_GSHEET_BERHASIL: return pd.DataFrame(columns=CLOSING_COLUMNS)
    try:
        try: ws = spreadsheet.worksheet(SHEET_CLOSING_DEAL)
        except:
            ws = spreadsheet.add_worksheet(title=SHEET_CLOSING_DEAL, rows=300, cols=len(CLOSING_COLUMNS))
            ws.append_row(CLOSING_COLUMNS, value_input_option="USER_ENTERED")
            auto_format_sheet(ws)
            return pd.DataFrame(columns=CLOSING_COLUMNS)
        
        ensure_headers(ws, CLOSING_COLUMNS)
        df = pd.DataFrame(ws.get_all_records())
        for c in CLOSING_COLUMNS:
            if c not in df.columns: df[c] = ""
            
        if COL_NILAI_KONTRAK in df.columns:
            df[COL_NILAI_KONTRAK] = pd.Series(df[COL_NILAI_KONTRAK].apply(parse_rupiah_to_int), dtype="Int64")
            
        return df[CLOSING_COLUMNS].copy()
    except: return pd.DataFrame(columns=CLOSING_COLUMNS)

def tambah_closing_deal(group, marketing, tgl, bidang, nilai):
    if not KONEKSI_GSHEET_BERHASIL: return False, "Koneksi Error"
    try:
        ws = spreadsheet.worksheet(SHEET_CLOSING_DEAL)
    except:
        ws = spreadsheet.add_worksheet(title=SHEET_CLOSING_DEAL, rows=300, cols=len(CLOSING_COLUMNS))
        ws.append_row(CLOSING_COLUMNS, value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        
    ensure_headers(ws, CLOSING_COLUMNS)
    val_int = parse_rupiah_to_int(nilai)
    if val_int is None: return False, "Format nilai salah (Contoh: 15jt)"
    
    tgl_str = tgl.strftime("%Y-%m-%d") if hasattr(tgl, "strftime") else str(tgl)
    ws.append_row([group or "-", marketing, tgl_str, bidang, int(val_int)], value_input_option="USER_ENTERED")
    auto_format_sheet(ws)
    return True, "Closing Deal Disimpan!"

# --- PEMBAYARAN (DP/TERMIN) ---
@st.cache_data(ttl=60)
def load_pembayaran_dp():
    if not KONEKSI_GSHEET_BERHASIL: return pd.DataFrame(columns=PAYMENT_COLUMNS)
    try:
        try: ws = spreadsheet.worksheet(SHEET_PEMBAYARAN)
        except:
            ws = spreadsheet.add_worksheet(title=SHEET_PEMBAYARAN, rows=500, cols=len(PAYMENT_COLUMNS))
            ws.append_row(PAYMENT_COLUMNS, value_input_option="USER_ENTERED")
            auto_format_sheet(ws)
            return pd.DataFrame(columns=PAYMENT_COLUMNS)
        
        ensure_headers(ws, PAYMENT_COLUMNS)
        df = pd.DataFrame(ws.get_all_records())
        for c in PAYMENT_COLUMNS:
            if c not in df.columns: df[c] = ""
            
        if COL_NOMINAL_BAYAR in df.columns:
            df[COL_NOMINAL_BAYAR] = pd.Series(df[COL_NOMINAL_BAYAR].apply(parse_rupiah_to_int), dtype="Int64")
        if COL_STATUS_BAYAR in df.columns:
            df[COL_STATUS_BAYAR] = df[COL_STATUS_BAYAR].apply(lambda x: True if str(x).upper() == "TRUE" else False)
        if COL_JATUH_TEMPO in df.columns:
            df[COL_JATUH_TEMPO] = pd.to_datetime(df[COL_JATUH_TEMPO], errors="coerce").dt.date
            
        # Parse audit log
        if COL_TS_UPDATE in df.columns:
            df[COL_TS_UPDATE] = df[COL_TS_UPDATE].apply(lambda x: build_numbered_log(parse_payment_log_lines(x)))
            
        return df[PAYMENT_COLUMNS].copy()
    except: return pd.DataFrame(columns=PAYMENT_COLUMNS)

def save_pembayaran_dp(df: pd.DataFrame) -> bool:
    try:
        ws = spreadsheet.worksheet(SHEET_PEMBAYARAN)
        ensure_headers(ws, PAYMENT_COLUMNS)
        ws.clear()
        
        rows_needed = len(df) + 1
        if ws.row_count < rows_needed: ws.resize(rows=rows_needed)
        
        df_save = df.copy()
        for c in PAYMENT_COLUMNS:
            if c not in df_save.columns: df_save[c] = ""
            
        df_save[COL_STATUS_BAYAR] = df_save[COL_STATUS_BAYAR].apply(lambda x: "TRUE" if bool(x) else "FALSE")
        df_save[COL_NOMINAL_BAYAR] = df_save[COL_NOMINAL_BAYAR].apply(lambda x: int(parse_rupiah_to_int(x)) if parse_rupiah_to_int(x) is not None else "")
        df_save[COL_JATUH_TEMPO] = df_save[COL_JATUH_TEMPO].apply(lambda d: d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else (str(d) if pd.notna(d) else ""))
        df_save[COL_TS_UPDATE] = df_save[COL_TS_UPDATE].apply(lambda x: build_numbered_log(parse_payment_log_lines(x)))
        
        data_to_save = [df_save.columns.values.tolist()] + df_save[PAYMENT_COLUMNS].fillna("").values.tolist()
        ws.update(range_name="A1", values=data_to_save, value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return True
    except: return False

def tambah_pembayaran_dp(group, marketing, tgl, jenis, nominal, jatuh_tempo, status, file, catatan):
    if not KONEKSI_GSHEET_BERHASIL: return False, "Koneksi Error"
    try:
        ws = spreadsheet.worksheet(SHEET_PEMBAYARAN)
    except:
        ws = spreadsheet.add_worksheet(title=SHEET_PEMBAYARAN, rows=500, cols=len(PAYMENT_COLUMNS))
        ws.append_row(PAYMENT_COLUMNS, value_input_option="USER_ENTERED")
        
    ensure_headers(ws, PAYMENT_COLUMNS)
    
    val_int = parse_rupiah_to_int(nominal)
    if val_int is None: return False, "Format nominal salah."
    
    link = upload_ke_dropbox(file, marketing, "Bukti_Pembayaran") if file else "-"
    ts_in = now_ts_str()
    
    ws.append_row([
        ts_in, group or "-", marketing,
        tgl.strftime("%Y-%m-%d"), jenis, int(val_int), jatuh_tempo.strftime("%Y-%m-%d"),
        "TRUE" if status else "FALSE", link, catatan or "-",
        build_numbered_log([ts_in]), marketing or "-"
    ], value_input_option="USER_ENTERED")
    
    auto_format_sheet(ws)
    return True, "Pembayaran Tersimpan!"

def apply_audit_payments_changes(df_before, df_after, actor):
    if df_after is None or df_after.empty: return df_after
    actor = safe_str(actor, "-").strip() or "-"
    before_idx = df_before.set_index(COL_TS_BAYAR, drop=False) if not df_before.empty else pd.DataFrame()
    after_idx = df_after.set_index(COL_TS_BAYAR, drop=False)
    
    watched = [COL_JENIS_BAYAR, COL_NOMINAL_BAYAR, COL_JATUH_TEMPO, COL_STATUS_BAYAR, COL_BUKTI_BAYAR, COL_CATATAN_BAYAR]
    ts = now_ts_str()
    
    for key, row in after_idx.iterrows():
        if key not in before_idx.index: continue
        prev = before_idx.loc[key]
        if isinstance(prev, pd.DataFrame): prev = prev.iloc[0]
        
        changes = []
        for col in watched:
            oldv = prev[col]
            newv = row[col]
            
            if col == COL_STATUS_BAYAR:
                if normalize_bool(oldv) != normalize_bool(newv):
                    changes.append(f"Status: {_fmt_payment_val_for_log(col, oldv)} → {_fmt_payment_val_for_log(col, newv)}")
            elif col == COL_NOMINAL_BAYAR:
                if parse_rupiah_to_int(oldv) != parse_rupiah_to_int(newv):
                    changes.append(f"Nominal: {_fmt_payment_val_for_log(col, oldv)} → {_fmt_payment_val_for_log(col, newv)}")
            elif col == COL_JATUH_TEMPO:
                if normalize_date(oldv) != normalize_date(newv):
                    changes.append(f"Due Date: {_fmt_payment_val_for_log(col, oldv)} → {_fmt_payment_val_for_log(col, newv)}")
            else:
                if safe_str(oldv, "").strip() != safe_str(newv, "").strip():
                    changes.append(f"{col}: {_fmt_payment_val_for_log(col, oldv)} → {_fmt_payment_val_for_log(col, newv)}")
                    
        if changes:
            old_log = safe_str(prev.get(COL_TS_UPDATE, ""), "")
            new_log = append_payment_ts_update(old_log, ts, actor, changes)
            after_idx.at[key, COL_TS_UPDATE] = new_log
            after_idx.at[key, COL_UPDATED_BY] = actor
            
    return after_idx.reset_index(drop=True)

def update_bukti_pembayaran_by_index(row_idx, file_obj, nama_marketing, actor):
    try:
        ws = spreadsheet.worksheet(SHEET_PEMBAYARAN)
        link = upload_ke_dropbox(file_obj, nama_marketing, "Bukti_Pembayaran")
        if not link or link == "-": return False, "Gagal Upload"
        
        ensure_headers(ws, PAYMENT_COLUMNS)
        row_gsheet = row_idx + 2
        
        headers = ws.row_values(1)
        col_bukti = headers.index(COL_BUKTI_BAYAR) + 1
        old_bukti = ws.cell(row_gsheet, col_bukti).value or ""
        ws.update_cell(row_gsheet, col_bukti, link)
        
        ts = now_ts_str()
        if COL_TS_UPDATE in headers:
            col_ts = headers.index(COL_TS_UPDATE) + 1
            old_log = ws.cell(row_gsheet, col_ts).value or ""
            new_log = append_payment_ts_update(old_log, ts, actor, [f"Update Bukti: {old_bukti} -> {link}"])
            ws.update_cell(row_gsheet, col_ts, new_log)
        
        if COL_UPDATED_BY in headers:
            ws.update_cell(row_gsheet, headers.index(COL_UPDATED_BY)+1, actor)
            
        auto_format_sheet(ws)
        return True, "Bukti Terupdate"
    except Exception as e: return False, str(e)

# --- UI RENDERER ---
def render_hybrid_table(df_data, key, text_col):
    if HAS_AGGRID:
        gb = GridOptionsBuilder.from_dataframe(df_data)
        if "Status" in df_data.columns: gb.configure_column("Status", editable=True, width=90)
        if "Bukti/Catatan" in df_data.columns: gb.configure_column("Bukti/Catatan", editable=True, width=300)
        gb.configure_default_column(editable=False, wrapText=True, autoHeight=True)
        return pd.DataFrame(AgGrid(df_data, gridOptions=gb.build(), fit_columns_on_grid_load=True, key=f"ag_{key}")["data"])
    else:
        cfg = {}
        if "Status" in df_data.columns: cfg["Status"] = st.column_config.CheckboxColumn("Done?")
        return st.data_editor(df_data, column_config=cfg, hide_index=True, key=f"de_{key}", use_container_width=True)

def payment_df_for_display(df):
    d = df.copy()
    if COL_NOMINAL_BAYAR in d.columns:
        d[COL_NOMINAL_BAYAR] = d[COL_NOMINAL_BAYAR].apply(lambda x: "" if pd.isna(x) else format_rupiah_display(x))
    return d

# =========================================================
# MAIN APP UI
# =========================================================
if not KONEKSI_GSHEET_BERHASIL:
    st.error("❌ Koneksi Database Gagal. Periksa Secrets.")
    st.stop()

# --- SIDEBAR ---
with st.sidebar:
    st.header("Navigasi")
    if "is_admin" not in st.session_state: st.session_state["is_admin"] = False
    
    opsi = ["📝 Laporan & Target"]
    if st.session_state["is_admin"]: opsi.append("📊 Dashboard Admin")
    menu = st.radio("Pilih Menu:", opsi)
    
    st.divider()
    if not st.session_state["is_admin"]:
        with st.expander("🔐 Login Admin"):
            pwd = st.text_input("Password:", type="password")
            if st.button("Login"):
                if verify_admin_password(pwd):
                    st.session_state["is_admin"] = True
                    st.rerun()
                else: st.error("Salah password")
    else:
        if st.button("🔓 Logout"):
            st.session_state["is_admin"] = False
            st.rerun()

    st.divider()
    st.markdown("### 🎯 Quick Add Targets")
    
    tab_t, tab_i, tab_adm = st.tabs(["Team", "Pribadi", "Admin"])
    
    with tab_t:
        with st.form("add_team_t"):
            txt_t = st.text_area("Target Team (per baris)")
            d1, d2 = st.columns(2)
            s_d = d1.date_input("Start", value=today_jkt(), key="sd_t")
            e_d = d2.date_input("End", value=today_jkt()+timedelta(days=30), key="ed_t")
            if st.form_submit_button("Tambah"):
                targets = [t.strip() for t in txt_t.split("\n") if t.strip()]
                if add_bulk_targets(SHEET_TARGET_TEAM, ["", str(s_d), str(e_d), "FALSE", "-"], targets):
                    st.success(f"{len(targets)} added!")
                    st.cache_data.clear()
    
    with tab_i:
        me = st.selectbox("Saya:", get_daftar_staf_terbaru(), key="sidebar_user")
        with st.form("add_indiv_t"):
            txt_i = st.text_area("Target Pribadi (per baris)")
            d1, d2 = st.columns(2)
            s_i = d1.date_input("Start", value=today_jkt(), key="sd_i")
            e_i = d2.date_input("End", value=today_jkt()+timedelta(days=7), key="ed_i")
            if st.form_submit_button("Tambah"):
                targets = [t.strip() for t in txt_i.split("\n") if t.strip()]
                if add_bulk_targets(SHEET_TARGET_INDIVIDU, [me, "", str(s_i), str(e_i), "FALSE", "-"], targets):
                    st.success(f"{len(targets)} added!")
                    st.cache_data.clear()
                    
    with tab_adm:
        with st.expander("Staff & Teams"):
            new_staf = st.text_input("Nama Baru")
            if st.button("Add Staff"):
                ok, msg = tambah_staf_baru(new_staf)
                if ok: st.success(msg); st.cache_data.clear()
                else: st.error(msg)
                
    st.divider()
    st.markdown("### 🤝 Closing Deal")
    with st.expander("Input Closing"):
        with st.form("cd_form"):
            c_grp = st.text_input("Group")
            c_mkt = st.text_input("Marketing")
            c_tgl = st.date_input("Tgl Event")
            c_bid = st.text_input("Bidang")
            c_val = st.text_input("Nilai (Rp)")
            if st.form_submit_button("Simpan"):
                ok, msg = tambah_closing_deal(c_grp, c_mkt, c_tgl, c_bid, c_val)
                if ok: st.success(msg); st.cache_data.clear()
                else: st.error(msg)
                
    st.divider()
    st.markdown("### 💳 Pembayaran")
    with st.expander("Input Pembayaran"):
        with st.form("pay_form"):
            p_grp = st.text_input("Group", key="p_grp")
            p_mkt = st.text_input("Marketing", key="p_mkt")
            p_tgl = st.date_input("Event", key="p_tgl")
            p_jns = st.selectbox("Jenis", ["DP", "Termin", "Pelunasan", "Lainnya"])
            p_nom = st.text_input("Nominal", key="p_nom")
            p_due = st.date_input("Jatuh Tempo", key="p_due")
            p_sts = st.checkbox("Lunas?", key="p_sts")
            p_fil = st.file_uploader("Bukti", key="p_fil")
            p_cat = st.text_area("Catatan", key="p_cat")
            
            if st.form_submit_button("Simpan"):
                ok, msg = tambah_pembayaran_dp(p_grp, p_mkt, p_tgl, p_jns, p_nom, p_due, p_sts, p_fil, p_cat)
                if ok: st.success(msg); st.cache_data.clear()
                else: st.error(msg)

# --- MAIN PAGE CONTENT ---
st.title("🚀 Sales & Marketing Action Center")
st.caption(f"Realtime: {now_ts_str()}")

# ALERTS
try:
    df_pay_main = load_pembayaran_dp()
    if not df_pay_main.empty:
        today = datetime.now().date()
        overdue = df_pay_main[(df_pay_main[COL_STATUS_BAYAR]==False) & (df_pay_main[COL_JATUH_TEMPO] < today)]
        soon = df_pay_main[(df_pay_main[COL_STATUS_BAYAR]==False) & (df_pay_main[COL_JATUH_TEMPO] >= today) & (df_pay_main[COL_JATUH_TEMPO] <= today+timedelta(days=3))]
        
        if not overdue.empty: st.error(f"⛔ Alert: {len(overdue)} Pembayaran OVERDUE!")
        elif not soon.empty: st.warning(f"⚠️ Alert: {len(soon)} Pembayaran Jatuh Tempo ≤ 3 Hari.")
except: pass

if menu == "📝 Laporan & Target":
    
    # 1. KPI TARGETS
    st.subheader("📊 Checklist Target (Result KPI)")
    c1, c2 = st.columns(2)
    
    df_team = load_checklist(SHEET_TARGET_TEAM, TEAM_CHECKLIST_COLUMNS)
    df_indiv = load_checklist(SHEET_TARGET_INDIVIDU, INDIV_CHECKLIST_COLUMNS)
    
    with c1:
        with st.container(border=True):
            st.markdown("#### 🏆 Target Team")
            if not df_team.empty:
                done = len(df_team[df_team["Status"]==True])
                st.progress(done/len(df_team), f"{done}/{len(df_team)}")
                ed_team = render_hybrid_table(df_team, "team", "Misi")
                
                if st.button("💾 Simpan Team", use_container_width=True):
                    df_sv = apply_audit_checklist_changes(df_team, ed_team, ["Misi"], get_actor_fallback("Admin"))
                    if save_checklist(SHEET_TARGET_TEAM, df_sv, TEAM_CHECKLIST_COLUMNS):
                        st.success("Tersimpan!"); st.cache_data.clear(); st.rerun()
                        
                with st.expander("Update Bukti Team"):
                    sel_m = st.selectbox("Misi", df_team["Misi"].unique())
                    n_m = st.text_area("Note Team")
                    f_m = st.file_uploader("File Team")
                    if st.button("Up Team"):
                        ok, msg = update_evidence_row(SHEET_TARGET_TEAM, sel_m, n_m, f_m, get_actor_fallback(), "Team")
                        if ok: st.success("Updated!"); st.rerun()
            else: st.info("No Data")

    with c2:
        with st.container(border=True):
            st.markdown("#### ⚡ Target Individu")
            filter_u = st.selectbox("Filter User:", get_daftar_staf_terbaru())
            df_u = df_indiv[df_indiv["Nama"] == filter_u] if not df_indiv.empty else pd.DataFrame()
            
            if not df_u.empty:
                done = len(df_u[df_u["Status"]==True])
                st.progress(done/len(df_u), f"{done}/{len(df_u)}")
                ed_u = render_hybrid_table(df_u, "indiv", "Target")
                
                if st.button("💾 Simpan Pribadi", use_container_width=True):
                    df_full = df_indiv.copy()
                    df_full.update(ed_u)
                    df_sv = apply_audit_checklist_changes(df_indiv, df_full, ["Nama", "Target"], filter_u)
                    if save_checklist(SHEET_TARGET_INDIVIDU, df_sv, INDIV_CHECKLIST_COLUMNS):
                        st.success("Tersimpan!"); st.cache_data.clear(); st.rerun()
                        
                with st.expander("Update Bukti Pribadi"):
                    sel_i = st.selectbox("Target", df_u["Target"].unique())
                    n_i = st.text_area("Note Indiv")
                    f_i = st.file_uploader("File Indiv")
                    if st.button("Up Indiv"):
                        ok, msg = update_evidence_row(SHEET_TARGET_INDIVIDU, sel_i, n_i, f_i, filter_u, "Indiv")
                        if ok: st.success("Updated!"); st.rerun()
            else: st.info("No Data")

    # 2. DAILY REPORT
    st.divider()
    with st.container(border=True):
        st.subheader("📝 Input Laporan Harian")
        
        c_nm, c_rem = st.columns([1,2])
        pelapor = c_nm.selectbox("Nama Pelapor", get_daftar_staf_terbaru(), key="pelapor_main")
        
        # Reminder Logic
        pend = get_reminder_pending(pelapor)
        if pend: c_rem.warning(f"🔔 Reminder: {pend}")
        
        # Input Form
        cat = st.radio("Aktivitas:", ["🚗 Sales (Lapangan)", "💻 Digital/Office", "📞 Telesales", "🏢 Lainnya"], horizontal=True)
        is_visit = "Sales" in cat
        
        c1, c2 = st.columns(2)
        c1.markdown(f"**Tanggal:** `{today_jkt()}`")
        link_sos = c1.text_input("Link Sosmed/Drive (Jika ada)")
        loc = c2.text_input("📍 Lokasi / Klien (Wajib)" if is_visit else "Detail Tugas", value="" if is_visit else cat.split(" ")[1])
        
        files = st.file_uploader("Upload Foto", accept_multiple_files=True)
        desc_map = {}
        
        if files:
            for f in files:
                c_img, c_desc = st.columns([1,3])
                c_img.markdown(f"📄 **{f.name}**")
                desc_map[f.name] = c_desc.text_input(f"Ket {f.name}")
        
        main_desc = st.text_area("Deskripsi Aktivitas") if not files else ""
        
        st.divider()
        st.markdown("#### 🏁 Kesimpulan & Kendala")
        
        k1, k2, k3 = st.columns(3)
        kesimpulan = k1.text_area("💡 Kesimpulan")
        kendala = k2.text_area("🚧 Kendala Internal")
        kendala_klien = k3.text_area("🧑‍💼 Kendala Klien")
        
        intr = st.radio("Interest:", ["< 50%", "50-75%", "> 75%"], horizontal=True)
        
        l1, l2 = st.columns(2)
        nm_klien = l1.text_input("Nama Klien")
        no_klien = l2.text_input("Kontak Klien")
        
        next_plan = st.text_input("📌 Next Plan (Pending Item)")
        
        if st.button("✅ Submit Laporan", type="primary", use_container_width=True):
            if is_visit and not loc: st.error("Lokasi Wajib!")
            elif not files and not main_desc: st.error("Deskripsi Wajib!")
            else:
                with st.spinner("Saving..."):
                    ts = now_ts_str()
                    rows = []
                    
                    base_row = [
                        ts, pelapor, loc, "", "", link_sos or "-", 
                        kesimpulan or "-", kendala or "-", kendala_klien or "-",
                        next_plan or "-", "", intr, nm_klien or "-", no_klien or "-"
                    ]
                    
                    if files:
                        for f in files:
                            url = upload_ke_dropbox(f, pelapor, "Daily")
                            r = base_row[:]
                            r[3] = desc_map.get(f.name, "-") # Deskripsi
                            r[4] = url # Link Foto
                            rows.append(r)
                    else:
                        r = base_row[:]
                        r[3] = main_desc
                        r[4] = "-"
                        rows.append(r)
                        
                    if simpan_laporan_harian_batch(rows, pelapor):
                        st.success("Tersimpan!")
                        st.balloons()
                        st.cache_data.clear()
                    else: st.error("Gagal.")

elif menu == "📊 Dashboard Admin":
    st.header("📊 Dashboard Produktivitas")
    if st.button("Refresh"): st.cache_data.clear(); st.rerun()
    
    df_logs = load_all_reports(get_daftar_staf_terbaru())
    if not df_logs.empty:
        df_logs["DateObj"] = pd.to_datetime(df_logs[COL_TIMESTAMP], dayfirst=True, errors="coerce").dt.date
        days = st.selectbox("Rentang:", [7, 14, 30])
        df_filt = df_logs[df_logs["DateObj"] >= date.today()-timedelta(days=days)]
        
        t1, t2, t3, t4 = st.tabs(["Sales", "Digital", "Review", "Payment"])
        
        with t1:
            st.metric("Total Aktivitas", len(df_filt))
            st.bar_chart(df_filt[COL_NAMA].value_counts())
            
        with t2:
            if HAS_PLOTLY:
                fig = px.pie(df_filt, names=COL_NAMA, title="Distribusi")
                st.plotly_chart(fig)
                
        with t3:
            st.subheader("Review Detail")
            for _, r in df_filt.sort_values(COL_TIMESTAMP, ascending=False).iterrows():
                with st.expander(f"{r[COL_NAMA]} | {r[COL_TIMESTAMP]}"):
                    st.write(f"**Tempat:** {r[COL_TEMPAT]}")
                    st.write(f"**Desc:** {r[COL_DESKRIPSI]}")
                    st.info(f"**Kesimpulan:** {r[COL_KESIMPULAN]}")
                    st.warning(f"**Kendala Klien:** {r.get(COL_KENDALA_KLIEN, '-')}")
                    
                    feed_key = f"feed_{r[COL_TIMESTAMP]}"
                    prev_feed = r.get(COL_FEEDBACK, "")
                    feed_in = st.text_area("Feedback Lead:", value=prev_feed, key=feed_key)
                    if st.button("Kirim Feedback", key=f"btn_{feed_key}"):
                        ok, msg = kirim_feedback_admin(r[COL_NAMA], r[COL_TIMESTAMP], feed_in)
                        if ok: st.success("Terkirim!")
                        
        with t4:
            st.subheader("Manajemen Pembayaran")
            df_pay = load_pembayaran_dp()
            if not df_pay.empty:
                actor = st.text_input("Nama Admin (Audit)", "Admin")
                df_disp = payment_df_for_display(df_pay)
                
                cols_cfg = {
                    COL_STATUS_BAYAR: st.column_config.CheckboxColumn("Lunas?"),
                    COL_NOMINAL_BAYAR: st.column_config.TextColumn("Nominal", disabled=True),
                    COL_TS_UPDATE: st.column_config.TextColumn("Log", disabled=True, width="large"),
                    COL_UPDATED_BY: st.column_config.TextColumn("By", disabled=True)
                }
                
                ed_pay = st.data_editor(
                    df_disp, column_config=cols_cfg, hide_index=True, use_container_width=True,
                    disabled=[COL_GROUP, COL_MARKETING, COL_TS_BAYAR, COL_NOMINAL_BAYAR, COL_BUKTI_BAYAR]
                )
                
                if st.button("Simpan Perubahan Pembayaran"):
                    df_res = apply_audit_payments_changes(df_pay, ed_pay, actor)
                    if save_pembayaran_dp(df_res):
                        st.success("Database Updated!")
                        st.cache_data.clear()
                        st.rerun()
                
                st.divider()
                st.write("### Update Bukti Bayar")
                idx = st.number_input("Index Baris (Mulai dari 0)", min_value=0, step=1)
                f_up = st.file_uploader("File Bukti Baru")
                if st.button("Upload Bukti"):
                    if idx < len(df_pay):
                        mkt = df_pay.iloc[idx][COL_MARKETING]
                        ok, msg = update_bukti_pembayaran_by_index(int(idx), f_up, mkt, actor)
                        if ok: st.success(msg); st.rerun()
                        else: st.error(msg)
    else: st.info("Belum ada data.")
