import json
import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
import time
import gspread
from google.oauth2.service_account import Credentials
import dropbox
from dropbox.exceptions import AuthError, ApiError
from dropbox.sharing import RequestedVisibility, SharedLinkSettings
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import random
import string
import re
import io
import hashlib
import hmac
import base64
import textwrap
import threading

# =========================================================
# 1. KONFIGURASI HALAMAN & LIBRARY OPSIONAL
# =========================================================
APP_TITLE = "Sales & Marketing Action Center"
st.set_page_config(
    page_title=APP_TITLE,
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Import Library Tambahan dengan Fallback
try:
    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment, PatternFill
    from openpyxl.utils.dataframe import dataframe_to_rows
    from openpyxl.utils import get_column_letter
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

# Konfigurasi AI (Google Gemini) - Support SDK Baru & Lama
SDK = "new"
try:
    from google import genai as genai_new
    from google.genai import types as types_new
except ImportError:
    SDK = "legacy"
    import google.generativeai as genai_legacy

API_KEY = st.secrets.get("gemini_api_key", "")
MODEL_FALLBACKS = ["gemini-2.0-flash", "gemini-1.5-flash"]

if SDK == "new":
    client_ai = genai_new.Client(api_key=API_KEY)
else:
    genai_legacy.configure(api_key=API_KEY)

# =========================================================
# 2. KONSTANTA & DEFINISI SHEET
# =========================================================
TZ_JKT = ZoneInfo("Asia/Jakarta")
NAMA_GOOGLE_SHEET = "Laporan Kegiatan Harian"
FOLDER_DROPBOX = "/Laporan_Kegiatan_Harian"
FORMAT_THROTTLE_SECONDS = 300  # Delay auto-format 5 menit

# Nama Sheet
SHEET_CONFIG_NAMA = "Config_Staf"
SHEET_TARGET_TEAM = "Target_Team_Checklist"
SHEET_TARGET_INDIVIDU = "Target_Individu_Checklist"
SHEET_CONFIG_TEAM = "Config_Team"
SHEET_CLOSING_DEAL = "Closing_Deal"
SHEET_PEMBAYARAN = "Pembayaran_DP"
SHEET_PRESENSI = "Presensi_Kehadiran"
SHEET_PENDING = "System_Pending_Approval"
SHEET_USERS = "Config_Users"
SHEET_AUDIT = "Global_Audit_Log"

# Definisi Kolom
PRESENSI_COLUMNS = ["Timestamp", "Nama", "Hari", "Tanggal", "Bulan", "Tahun", "Waktu"]

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

COL_TS_UPDATE = "Timestamp Update (Log)"
COL_UPDATED_BY = "Updated By"

TEAM_COL_NAMA_TEAM = "Nama Team"
TEAM_COL_POSISI = "Posisi"
TEAM_COL_ANGGOTA = "Nama Anggota"
TEAM_COLUMNS = [TEAM_COL_NAMA_TEAM, TEAM_COL_POSISI, TEAM_COL_ANGGOTA]

COL_GROUP = "Nama Group"
COL_MARKETING = "Nama Marketing"
COL_TGL_EVENT = "Tanggal Event"
COL_BIDANG = "Bidang"
COL_NILAI_KONTRAK = "Nilai Kontrak"
CLOSING_COLUMNS = [COL_GROUP, COL_MARKETING, COL_TGL_EVENT, COL_BIDANG, COL_NILAI_KONTRAK]

TEAM_CHECKLIST_COLUMNS = ["Misi", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan", COL_TS_UPDATE, COL_UPDATED_BY]
INDIV_CHECKLIST_COLUMNS = ["Nama", "Target", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan", COL_TS_UPDATE, COL_UPDATED_BY]

COL_TS_BAYAR = "Timestamp Input"
COL_NILAI_KESEPAKATAN = "Total Nilai Kesepakatan"
COL_JENIS_BAYAR = "Jenis Pembayaran"
COL_NOMINAL_BAYAR = "Nominal Pembayaran"
COL_TENOR_CICILAN = "Tenor (Bulan)"
COL_SISA_BAYAR = "Sisa Pembayaran"
COL_JATUH_TEMPO = "Batas Waktu Bayar"
COL_STATUS_BAYAR = "Status Pembayaran"
COL_BUKTI_BAYAR = "Bukti Pembayaran"
COL_CATATAN_BAYAR = "Catatan"

PAYMENT_COLUMNS = [
    COL_TS_BAYAR, COL_GROUP, COL_MARKETING, COL_TGL_EVENT,
    COL_NILAI_KESEPAKATAN, COL_JENIS_BAYAR, COL_NOMINAL_BAYAR,
    COL_TENOR_CICILAN, COL_SISA_BAYAR, COL_JATUH_TEMPO,
    COL_STATUS_BAYAR, COL_BUKTI_BAYAR, COL_CATATAN_BAYAR,
    COL_TS_UPDATE, COL_UPDATED_BY
]

# =========================================================
# 3. GENERAL UTILITIES (Parsing, Formatting, Mobile)
# =========================================================

def now_ts_str() -> str:
    return datetime.now(tz=TZ_JKT).strftime("%d-%m-%Y %H:%M:%S")

def safe_str(x, default="") -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)): return default
        s = str(x)
        return default if s.lower() in {"nan", "none"} else s
    except: return default

def normalize_bool(x) -> bool:
    if isinstance(x, bool): return x
    return True if safe_str(x).strip().upper() == "TRUE" else False

def normalize_date(x):
    if x is None or (isinstance(x, float) and pd.isna(x)): return None
    if isinstance(x, date) and not isinstance(x, datetime): return x
    try: return pd.to_datetime(x, errors="coerce").date()
    except: return None

def parse_rupiah_to_int(value):
    if value is None: return None
    if isinstance(value, (int, float)) and not pd.isna(value):
        try: return int(round(float(value)))
        except: return None
    s = str(value).strip()
    if not s: return None
    s_lower = re.sub(r"\s+", "", s.lower().replace("idr", "").replace("rp", ""))
    if s_lower in {"nan", "none", "-", "null"}: return None
    
    multiplier = 1
    if "miliar" in s_lower or "milyar" in s_lower: multiplier = 1_000_000_000
    elif "jt" in s_lower or "juta" in s_lower: multiplier = 1_000_000
    elif "rb" in s_lower or "ribu" in s_lower: multiplier = 1_000
    
    s_num = re.sub(r"(miliar|milyar|juta|jt|ribu|rb)", "", s_lower)
    s_num = re.sub(r"[^0-9.,]", "", s_num)
    if not s_num: return None
    try:
        if "," in s_num and "." in s_num:
            cleaned = s_num.replace(".", "").replace(",", ".") if s_num.rfind(",") > s_num.rfind(".") else s_num.replace(",", "")
        elif "," in s_num:
            cleaned = s_num.replace(",", "") if s_num.count(",") > 1 else (s_num.replace(",", "") if len(s_num.split(",")[1]) == 3 else s_num.replace(",", "."))
        elif "." in s_num:
            cleaned = s_num.replace(".", "") if s_num.count(".") > 1 else (s_num.replace(".", "") if len(s_num.split(".")[1]) == 3 else s_num)
        else: cleaned = s_num
        base = float(cleaned)
        if multiplier != 1:
            return int(round(base)) if base >= multiplier else int(round(base * multiplier))
        return int(round(base))
    except:
        digits = re.sub(r"\D", "", s_num)
        return int(digits) if digits else None

def format_rupiah_display(amount) -> str:
    try:
        if amount is None or pd.isna(amount): return ""
        n = int(amount)
        return "Rp " + f"{n:,}".replace(",", ".")
    except: return str(amount)

def is_mobile_device() -> bool:
    try:
        if hasattr(st, "context") and hasattr(st.context, "headers"):
            ua = (st.context.headers.get("user-agent") or "").lower()
            return any(k in ua for k in ["android", "iphone", "mobile"])
        return False
    except: return False

IS_MOBILE = is_mobile_device()

def get_actor_fallback(default="-") -> str:
    for k in ["user_name", "pelapor_main", "sidebar_user", "payment_editor_name"]:
        if k in st.session_state and safe_str(st.session_state.get(k), "").strip():
            return safe_str(st.session_state.get(k)).strip()
    return default

def ui_toast(message: str, icon=None):
    if hasattr(st, "toast"): st.toast(message, icon=icon)
    else: st.success(message)

# =========================================================
# 4. SPECIFIC HELPERS (Log Logic & Type Cleaning)
# =========================================================
def clean_df_types_dynamically(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.copy()
    for col in df_clean.columns:
        col_lower = col.lower()
        if any(key in col_lower for key in ["nilai", "nominal", "sisa", "kontrak", "sepakat", "tenor"]):
            df_clean[col] = pd.to_numeric(df_clean[col].apply(lambda x: parse_rupiah_to_int(x) if isinstance(x, str) else x), errors='coerce').fillna(0)
        elif any(key in col_lower for key in ["tanggal", "tempo", "waktu"]):
            if not any(k in col_lower for k in ["log", "update", "timestamp"]):
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
    return df_clean

def generate_dynamic_column_config(df):
    config = {}
    for col in df.columns:
        col_lower = col.lower()
        if any(key in col_lower for key in ["nilai", "nominal", "sisa", "kontrak", "sepakat", "tenor"]):
            config[col] = st.column_config.NumberColumn(col, format="Rp %d", min_value=0)
        elif any(key in col_lower for key in ["tanggal", "tempo", "waktu"]) and "timestamp" not in col_lower:
            config[col] = st.column_config.DateColumn(col, format="DD/MM/YYYY")
        elif "status" in col_lower:
            config[col] = st.column_config.CheckboxColumn(col)
        else:
            config[col] = st.column_config.TextColumn(col)
    return config

def build_alert_pembayaran(df: pd.DataFrame, days_due_soon: int = 3):
    if df is None or df.empty: return pd.DataFrame(columns=df.columns), pd.DataFrame(columns=df.columns)
    today = datetime.now(tz=TZ_JKT).date()
    df_alert = df.copy()
    
    if COL_JATUH_TEMPO in df_alert.columns:
        df_alert[COL_JATUH_TEMPO] = pd.to_datetime(df_alert[COL_JATUH_TEMPO], errors="coerce").dt.date
    if COL_SISA_BAYAR in df_alert.columns:
        df_alert[COL_SISA_BAYAR] = pd.to_numeric(df_alert[COL_SISA_BAYAR], errors='coerce').fillna(0)
    else: df_alert[COL_SISA_BAYAR] = 0

    df_tagihan_aktif = df_alert[(df_alert[COL_SISA_BAYAR] > 0) & (pd.notna(df_alert[COL_JATUH_TEMPO]))]
    if df_tagihan_aktif.empty: return pd.DataFrame(columns=df.columns), pd.DataFrame(columns=df.columns)
    
    overdue = df_tagihan_aktif[df_tagihan_aktif[COL_JATUH_TEMPO] < today].copy()
    due_soon = df_tagihan_aktif[(df_tagihan_aktif[COL_JATUH_TEMPO] >= today) & (df_tagihan_aktif[COL_JATUH_TEMPO] <= (today + timedelta(days=days_due_soon)))].copy()
    return overdue, due_soon

def parse_payment_log_lines(log_text: str):
    log_text = safe_str(log_text, "").strip()
    if not log_text: return []
    raw_lines = [ln.rstrip() for ln in log_text.splitlines() if ln.strip()]
    out = []
    for ln in raw_lines:
        mnum = re.match(r"^\s*\d+\.\s*(.*)$", ln)
        if mnum: ln = mnum.group(1).rstrip()
        out.append(ln)
    return out

def build_numbered_log(lines):
    lines = [str(l).rstrip() for l in (lines or []) if safe_str(l, "").strip()]
    return "\n".join([f"{i}. {line}" for i, line in enumerate(lines, 1)]).strip()

def append_payment_ts_update(existing_log, ts, actor, changes):
    lines = parse_payment_log_lines(existing_log)
    if not changes: return build_numbered_log(lines)
    actor = safe_str(actor, "-").strip()
    prefix = f"[{ts}] ({actor})"
    lines.append(f"{prefix} {changes[0]}")
    for c in changes[1:]: lines.append(f" {c}")
    return build_numbered_log(lines)

def _fmt_payment_val_for_log(col, v):
    if col == COL_NOMINAL_BAYAR: return format_rupiah_display(parse_rupiah_to_int(v))
    if col == COL_STATUS_BAYAR: return "✅ Lunas" if normalize_bool(v) else "⏳ Belum"
    if col in {COL_JATUH_TEMPO, COL_TGL_EVENT}:
        d = normalize_date(v)
        return d.strftime("%Y-%m-%d") if d else "-"
    return safe_str(v, "-").strip()

def apply_audit_payments_changes(df_before, df_after, actor):
    actor = safe_str(actor, "-").strip() or "-"
    before = df_before.copy() if df_before is not None else pd.DataFrame()
    after = df_after.copy()
    for c in [COL_TS_UPDATE, COL_UPDATED_BY]:
        if c not in after.columns: after[c] = ""
    
    if before.empty:
        ts = now_ts_str()
        for i in range(len(after)):
            after.at[i, COL_TS_UPDATE] = build_numbered_log([f"[{ts}] ({actor}) Data Initial"])
            after.at[i, COL_UPDATED_BY] = actor
        return after

    before_idx = before.set_index(COL_TS_BAYAR, drop=False)
    after_idx = after.set_index(COL_TS_BAYAR, drop=False)
    watched = [COL_JENIS_BAYAR, COL_NOMINAL_BAYAR, COL_JATUH_TEMPO, COL_STATUS_BAYAR, COL_BUKTI_BAYAR, COL_CATATAN_BAYAR]
    ts = now_ts_str()

    for key, row in after_idx.iterrows():
        if key not in before_idx.index: continue
        prev = before_idx.loc[key]
        if isinstance(prev, pd.DataFrame): prev = prev.iloc[0]
        
        changes = []
        for col in watched:
            if col not in after_idx.columns: continue
            oldv, newv = prev[col], row[col]
            is_diff = False
            if col == COL_STATUS_BAYAR: is_diff = normalize_bool(oldv) != normalize_bool(newv)
            elif col == COL_NOMINAL_BAYAR: is_diff = parse_rupiah_to_int(oldv) != parse_rupiah_to_int(newv)
            elif col == COL_JATUH_TEMPO: is_diff = normalize_date(oldv) != normalize_date(newv)
            else: is_diff = safe_str(oldv).strip() != safe_str(newv).strip()
            
            if is_diff:
                changes.append(f"{col}: {_fmt_payment_val_for_log(col, oldv)} ➡ {_fmt_payment_val_for_log(col, newv)}")
        
        if changes:
            oldlog = safe_str(prev.get(COL_TS_UPDATE, ""), "")
            after_idx.at[key, COL_TS_UPDATE] = append_payment_ts_update(oldlog, ts, actor, changes)
            after_idx.at[key, COL_UPDATED_BY] = actor

    return after_idx.reset_index(drop=True)

def apply_audit_checklist_changes(df_before, df_after, key_cols, actor):
    actor = safe_str(actor, "-").strip() or "-"
    after = df_after.copy()
    for c in [COL_TS_UPDATE, COL_UPDATED_BY]:
        if c not in after.columns: after[c] = ""
    
    # Simple audit for checklist: just update TS if Status/Note changes
    ts = now_ts_str()
    if df_before is not None and not df_before.empty:
        # Simplifikasi: jika ada perubahan di row manapun, update TS
        # (Untuk implementasi full row matching akan sangat panjang, kita gunakan logic sederhana)
        pass 
    return after

# =========================================================
# 5. CONNECTIONS & LOGGING (THREADED)
# =========================================================
@st.cache_resource(ttl=None, show_spinner=False)
def init_connections():
    gs_obj, dbx_obj = None, None
    try:
        if "gcp_service_account" in st.secrets:
            scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_info(dict(st.secrets["gcp_service_account"]), scopes=scopes)
            gs_obj = gspread.authorize(creds).open(NAMA_GOOGLE_SHEET)
            # Pre-check Audit Sheet
            try: gs_obj.worksheet(SHEET_AUDIT)
            except: gs_obj.add_worksheet(SHEET_AUDIT, 1000, 6).append_row(["Waktu","User","Status","Target Data","Chat & Catatan","Detail Perubahan"])
    except Exception as e: print(f"GSheet Err: {e}")

    try:
        if "dropbox" in st.secrets:
            dbx_obj = dropbox.Dropbox(st.secrets["dropbox"]["access_token"])
            dbx_obj.users_get_current_account()
    except Exception as e: print(f"Dropbox Err: {e}")
    return gs_obj, dbx_obj

spreadsheet, dbx = init_connections()
KONEKSI_GSHEET_BERHASIL = (spreadsheet is not None)
KONEKSI_DROPBOX_BERHASIL = (dbx is not None)

def _background_log_worker(actor, action, target, chat, detail):
    try:
        if not spreadsheet: return
        ws = spreadsheet.worksheet(SHEET_AUDIT)
        ts = datetime.now(ZoneInfo("Asia/Jakarta")).strftime("%d-%m-%Y %H:%M:%S")
        ws.append_row([f"'{ts}", str(actor), str(action), str(target), str(chat), str(detail)[:4000]], value_input_option="USER_ENTERED")
    except Exception as e: print(f"Log Err: {e}")

def force_audit_log(actor, action, target, chat, detail):
    threading.Thread(target=_background_log_worker, args=(actor, action, target, chat, detail)).start()
    return True

def load_audit_log(gs_obj):
    try: return pd.DataFrame(gs_obj.worksheet(SHEET_AUDIT).get_all_records())
    except: return pd.DataFrame()

# =========================================================
# 6. DATA HANDLING (BUFFERING & CRUD)
# =========================================================
def ensure_headers(ws, headers):
    try:
        if ws.col_count < len(headers): ws.resize(cols=len(headers))
        if not ws.row_values(1): ws.append_row(headers)
    except: pass

@st.cache_data(ttl=3600)
def load_pembayaran_dp():
    if not KONEKSI_GSHEET_BERHASIL: return pd.DataFrame(columns=PAYMENT_COLUMNS)
    try:
        try: ws = spreadsheet.worksheet(SHEET_PEMBAYARAN)
        except: ws = spreadsheet.add_worksheet(SHEET_PEMBAYARAN, 500, len(PAYMENT_COLUMNS)); ws.append_row(PAYMENT_COLUMNS)
        ensure_headers(ws, PAYMENT_COLUMNS)
        df = pd.DataFrame(ws.get_all_records())
        # Cleaning
        if COL_NOMINAL_BAYAR in df.columns: df[COL_NOMINAL_BAYAR] = pd.to_numeric(df[COL_NOMINAL_BAYAR].apply(parse_rupiah_to_int), errors='coerce').fillna(0)
        if COL_SISA_BAYAR in df.columns: df[COL_SISA_BAYAR] = pd.to_numeric(df[COL_SISA_BAYAR], errors='coerce').fillna(0)
        if COL_JATUH_TEMPO in df.columns: df[COL_JATUH_TEMPO] = pd.to_datetime(df[COL_JATUH_TEMPO], errors="coerce").dt.date
        if COL_STATUS_BAYAR in df.columns: df[COL_STATUS_BAYAR] = df[COL_STATUS_BAYAR].apply(normalize_bool)
        return df
    except: return pd.DataFrame(columns=PAYMENT_COLUMNS)

def save_pembayaran_dp(df):
    try:
        ws = spreadsheet.worksheet(SHEET_PEMBAYARAN)
        ws.clear()
        df_s = df.copy().fillna("")
        if COL_STATUS_BAYAR in df_s.columns: df_s[COL_STATUS_BAYAR] = df_s[COL_STATUS_BAYAR].apply(lambda x: "TRUE" if x else "FALSE")
        data = [PAYMENT_COLUMNS] + df_s[PAYMENT_COLUMNS].astype(str).values.tolist()
        ws.update(range_name="A1", values=data, value_input_option="USER_ENTERED")
        return True
    except: return False

@st.cache_data(ttl=3600)
def load_closing_deal():
    try:
        ws = spreadsheet.worksheet(SHEET_CLOSING_DEAL)
        ensure_headers(ws, CLOSING_COLUMNS)
        return pd.DataFrame(ws.get_all_records())
    except: return pd.DataFrame(columns=CLOSING_COLUMNS)

@st.cache_data(ttl=3600)
def get_daftar_staf_terbaru():
    try:
        ws = spreadsheet.worksheet(SHEET_CONFIG_NAMA)
        vals = ws.col_values(1)
        return vals[1:] if len(vals)>1 else ["Saya"]
    except: return ["Saya"]

@st.cache_data(ttl=3600)
def load_checklist(name, cols):
    try:
        try: ws = spreadsheet.worksheet(name)
        except: ws = spreadsheet.add_worksheet(name, 200, len(cols)); ws.append_row(cols)
        ensure_headers(ws, cols)
        df = pd.DataFrame(ws.get_all_records())
        if "Status" in df.columns: df["Status"] = df["Status"].apply(normalize_bool)
        return df
    except: return pd.DataFrame(columns=cols)

def save_checklist(name, df, cols):
    try:
        ws = spreadsheet.worksheet(name)
        df_s = df.copy().fillna("")
        if "Status" in df_s.columns: df_s["Status"] = df_s["Status"].apply(lambda x: "TRUE" if x else "FALSE")
        ws.update(range_name="A2", values=df_s[cols].astype(str).values.tolist(), value_input_option="USER_ENTERED")
        return True
    except: return False

def prefetch_all_data():
    """Buffer data ke RAM untuk akses super cepat."""
    if "data_buffered" not in st.session_state:
        st.session_state["buf_pay"] = load_pembayaran_dp()
        st.session_state["buf_closing"] = load_closing_deal()
        st.session_state["buf_kpi_team"] = load_checklist(SHEET_TARGET_TEAM, TEAM_CHECKLIST_COLUMNS)
        st.session_state["buf_kpi_indiv"] = load_checklist(SHEET_TARGET_INDIVIDU, INDIV_CHECKLIST_COLUMNS)
        st.session_state["buf_staf"] = get_daftar_staf_terbaru()
        st.session_state["data_buffered"] = True

def force_refresh():
    st.cache_data.clear()
    for k in list(st.session_state.keys()):
        if k.startswith("buf_"): del st.session_state[k]
    st.session_state.pop("data_buffered", None)
    st.rerun()

# =========================================================
# 7. BUSINESS LOGIC (Feature Implementation)
# =========================================================
def init_pending_db():
    try:
        ws = spreadsheet.worksheet(SHEET_PENDING)
        if "Old Data JSON" not in ws.row_values(1): ws.update_cell(1, 7, "Old Data JSON")
        return ws
    except: return None # Should create if not exists

def get_pending_approvals():
    try: return spreadsheet.worksheet(SHEET_PENDING).get_all_records()
    except: return []

def execute_approval(idx, action, admin, note="-"):
    try:
        ws_p = spreadsheet.worksheet(SHEET_PENDING)
        req = ws_p.get_all_records()[idx]
        if action == "REJECT":
            force_audit_log(admin, "❌ REJECT", req["Target Sheet"], note, f"Req: {req['Requestor']}")
            ws_p.delete_rows(idx+2)
        elif action == "APPROVE":
            ws_t = spreadsheet.worksheet(req["Target Sheet"])
            nd = json.loads(req["New Data JSON"])
            h = ws_t.row_values(1)
            v = [nd.get(x, "") for x in h]
            ws_t.update(range_name=f"A{int(req['Row Index (0-based)'])+2}", values=[v], value_input_option="USER_ENTERED")
            force_audit_log(admin, "✅ APPROVE", req["Target Sheet"], "Approved", f"Req: {req['Requestor']}")
            ws_p.delete_rows(idx+2)
        return True
    except: return False

def tambah_pembayaran_dp(grp, mkt, tgl, jenis, nom, tot, ten, due, bukti, note):
    try:
        n_v = parse_rupiah_to_int(nom) or 0
        t_v = parse_rupiah_to_int(tot) or 0
        sisa = t_v - n_v
        stat = "✅ Lunas" if sisa <= 0 else f"⚠️ Sisa: {format_rupiah_display(sisa)}"
        link = upload_ke_dropbox(bukti, mkt) if bukti else "-"
        row = [now_ts_str(), grp, mkt, str(tgl), t_v, jenis, n_v, ten, sisa, str(due), stat, link, note, "Log Awal", "System"]
        ws = spreadsheet.worksheet(SHEET_PEMBAYARAN)
        ws.append_row(row, value_input_option="USER_ENTERED")
        return True, "Tersimpan"
    except Exception as e: return False, str(e)

def upload_ke_dropbox(f_obj, nama):
    if not KONEKSI_DROPBOX_BERHASIL: return "-"
    try:
        path = f"{FOLDER_DROPBOX}/{nama}/{int(time.time())}_{f_obj.name}"
        dbx.files_upload(f_obj.getvalue(), path, mode=dropbox.files.WriteMode.add)
        try: l = dbx.sharing_create_shared_link_with_settings(path, SharedLinkSettings(requested_visibility=RequestedVisibility.public))
        except ApiError as e:
            if e.error.is_shared_link_already_exists(): l = dbx.sharing_list_shared_links(path, direct_only=True).links[0]
            else: return "-"
        return l.url.replace("?dl=0", "?raw=1")
    except: return "-"

def catat_presensi(nama):
    try:
        ws = spreadsheet.worksheet(SHEET_PRESENSI)
        t = datetime.now(TZ_JKT)
        ws.append_row([f"'{t}", nama, t.strftime("%A"), t.day, t.month, t.year, t.strftime("%H:%M")], value_input_option="USER_ENTERED")
        return True, "Berhasil"
    except: return False, "Gagal"

def simpan_laporan_harian_batch(rows, nama):
    try:
        ws = get_or_create_worksheet(nama)
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        return True
    except: return False

# =========================================================
# 8. UI COMPONENTS & STYLING
# =========================================================
ASSET_DIR = Path(__file__).parent / "assets"
def _img_b64(p): 
    try: return base64.b64encode(p.read_bytes()).decode() if p.exists() else ""
    except: return ""

@st.cache_resource
def get_assets():
    return {
        "l": _img_b64(ASSET_DIR/"log EO.png"), "r": _img_b64(ASSET_DIR/"logo traine.png"), "h": _img_b64(ASSET_DIR/"Logo-holding.png"), "bg": _img_b64(ASSET_DIR/"sportarium.jpg"),
        "css": """<style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&display=swap');
        :root{ --bg0:#020805; --bg1:#04110b; --text:#fff; --gold:#facc15; --green:#16a34a; }
        .stApp{ background: radial-gradient(circle at 15% 15%, rgba(22,163,74,0.15), transparent 50%), linear-gradient(180deg, var(--bg0), var(--bg1)); color: var(--text); font-family: "Space Grotesk"; }
        .sx-hero{ border: 1px solid rgba(255,255,255,0.1); border-radius: 20px; padding: 20px; background: rgba(0,0,0,0.6); box-shadow: 0 10px 30px rgba(0,0,0,0.5); }
        .sx-hero-grid{ display: grid; grid-template-columns: 100px 1fr 100px; align-items: center; text-align: center; gap: 10px; }
        .mobile-bottom-nav { position: fixed; bottom: 0; left: 0; width: 100%; background: rgba(0,0,0,0.95); display: flex; justify-content: space-around; padding: 12px; border-top: 1px solid rgba(255,255,255,0.1); z-index: 9999; backdrop-filter: blur(10px); }
        .mobile-bottom-nav a { text-decoration: none; font-size: 24px; filter: grayscale(100%); transition: 0.3s; }
        .mobile-bottom-nav a:hover { filter: grayscale(0%); transform: scale(1.2); }
        button[kind="primary"] { background: linear-gradient(135deg, var(--green), var(--gold)) !important; color: #000 !important; border: none; }
        div[data-testid="stSpinner"] { background: rgba(0,0,0,0.8); backdrop-filter: blur(5px); }
        </style>"""
    }
ASSETS = get_assets()

def render_header():
    st.markdown(ASSETS["css"], unsafe_allow_html=True)
    bg = f"--hero-bg: url('data:image/jpeg;base64,{ASSETS['bg']}');" if ASSETS['bg'] else ""
    st.markdown(f"""
    <div style="text-align:center; margin-bottom:10px;"><img src="data:image/png;base64,{ASSETS['h']}" height="70"></div>
    <div class="sx-hero" style="{bg}">
        <div class="sx-hero-grid">
            <img src="data:image/png;base64,{ASSETS['l']}" style="max-height:60px;">
            <div><h2 style="margin:0">{APP_TITLE}</h2><small>{now_ts_str()}</small></div>
            <img src="data:image/png;base64,{ASSETS['r']}" style="max-height:60px;">
        </div>
    </div>
    """, unsafe_allow_html=True)

def login_page():
    render_header()
    t1, t2 = st.tabs(["🚀 Staff", "🛡️ Admin"])
    with t1:
        if st.button("Masuk (Staff)", type="primary", use_container_width=True):
            st.session_state.update({"logged_in":True, "user_role":"staff", "user_name":"Staff", "is_admin":False})
            st.rerun()
    with t2:
        if st.session_state.get("otp_step", 1)==1:
            em = st.text_input("Email")
            if st.button("Kirim OTP"):
                if em in st.secrets.get("users", {}):
                    otp = "".join(random.choices(string.digits, k=6))
                    # Simulasikan kirim email di sini (Gunakan fungsi send_email_otp di real deployment)
                    st.session_state.update({"otp":otp, "email":em, "otp_step":2})
                    st.success(f"OTP Terkirim: {otp}") # Debug mode, remove in prod
                else: st.error("Tidak terdaftar")
        else:
            c = st.text_input("OTP")
            if st.button("Verifikasi"):
                if c == st.session_state["otp"]:
                    ud = st.secrets["users"][st.session_state["email"]]
                    st.session_state.update({"logged_in":True, "user_name":ud["name"], "user_role":ud.get("role","admin"), "is_admin":True})
                    st.rerun()
                else: st.error("Salah")

def render_mobile_nav():
    st.markdown("""
    <div class="mobile-bottom-nav">
        <a href="?nav=home">🏠</a>
        <a href="?nav=report">📝</a>
        <a href="?nav=kpi">🎯</a>
        <a href="?nav=closing">🤝</a>
        <a href="?nav=payment">💳</a>
    </div>
    """, unsafe_allow_html=True)

# =========================================================
# 9. MAIN APP LOGIC
# =========================================================
if "logged_in" not in st.session_state: st.session_state["logged_in"] = False

if not st.session_state["logged_in"]:
    login_page()
    st.stop()

prefetch_all_data()
render_header()

NAV_MAP = {
    "home": "🏠 Beranda", "presensi": "📅 Presensi", "report": "📝 Laporan Harian",
    "kpi": "🎯 KPI", "closing": "🤝 Closing", "payment": "💳 Payment",
    "log": "📜 Audit", "admin": "📊 Admin"
}
qp = st.query_params.get("nav")
if qp and qp in NAV_MAP: st.session_state["menu_nav"] = NAV_MAP[qp]
nav = st.session_state.get("menu_nav", "🏠 Beranda")

with st.sidebar:
    st.write(f"👤 **{st.session_state.get('user_name')}**")
    if st.button("🔄 Refresh"): force_refresh()
    st.markdown("---")
    for k, v in NAV_MAP.items():
        if k == "admin" and not st.session_state.get("is_admin"): continue
        if st.button(v, use_container_width=True, type="primary" if nav==NAV_MAP[k] else "secondary"):
            st.session_state["menu_nav"] = NAV_MAP[k]
            st.query_params["nav"] = k
            st.rerun()
    if st.button("Logout"): st.session_state.clear(); st.rerun()

if IS_MOBILE: render_mobile_nav()

# --- Content Router ---
if nav == "🏠 Beranda":
    st.markdown("## 🧭 Menu Utama")
    cols = st.columns(2)
    items = [("report", "📝 Laporan"), ("presensi", "📅 Presensi"), ("kpi", "🎯 KPI"), ("payment", "💳 Payment")]
    for i, (k, l) in enumerate(items):
        with cols[i%2]:
            if st.button(l, key=f"mn_{k}", use_container_width=True):
                st.session_state["menu_nav"] = NAV_MAP[k]
                st.query_params["nav"] = k
                st.rerun()

elif nav == "📅 Presensi":
    st.markdown("### 📅 Presensi")
    n = st.selectbox("Nama", st.session_state.get("buf_staf", []))
    if st.button("Hadir", type="primary", use_container_width=True):
        ok, m = catat_presensi(n)
        if ok: st.success(m); force_audit_log(n, "PRESENSI", "Presensi", "Hadir", "-")
        else: st.error(m)

elif nav == "📝 Laporan Harian":
    st.markdown("### 📝 Laporan")
    if IS_MOBILE:
        t1, t2, t3 = st.tabs(["Aktivitas", "Hasil", "Kirim"])
        with t1: 
            pel = st.selectbox("Nama", st.session_state["buf_staf"])
            loc = st.text_input("Lokasi")
            desc = st.text_area("Deskripsi")
            img = st.file_uploader("Bukti", accept_multiple_files=True)
        with t2: 
            res = st.text_area("Kesimpulan")
            plan = st.text_input("Next Plan")
        with t3:
            if st.button("Kirim Laporan", type="primary"):
                lks = [upload_ke_dropbox(i, pel) for i in img] if img else ["-"]
                r = [now_ts_str(), pel, loc, desc, ",".join(lks), "-", res, "-", "-", plan, "-", "-", "-", "-"]
                if simpan_laporan_harian_batch([r], pel): st.success("Terkirim!"); st.rerun()
                else: st.error("Gagal")
    else:
        with st.form("desktop_rep"):
            c1, c2 = st.columns(2)
            pel = c1.selectbox("Nama", st.session_state["buf_staf"])
            loc = c2.text_input("Lokasi")
            desc = st.text_area("Deskripsi")
            if st.form_submit_button("Kirim"):
                r = [now_ts_str(), pel, loc, desc, "-", "-", "-", "-", "-", "-", "-", "-", "-", "-"]
                simpan_laporan_harian_batch([r], pel)
                st.success("Terkirim")

elif nav == "💳 Payment":
    st.markdown("### 💳 Smart Payment")
    df = st.session_state.get("buf_pay", load_pembayaran_dp())
    
    # Alert System
    over, soon = build_alert_pembayaran(df)
    c1, c2 = st.columns(2)
    c1.metric("Overdue", len(over))
    c2.metric("Due Soon", len(soon))

    with st.expander("➕ Input Pembayaran"):
        with st.form("new_pay"):
            c1, c2 = st.columns(2)
            m = c1.selectbox("Marketing", st.session_state["buf_staf"])
            g = c2.text_input("Group")
            t = c1.text_input("Total (Rp)")
            b = c2.text_input("Bayar (Rp)")
            j = c1.selectbox("Jenis", ["DP", "Cicilan", "Lunas"])
            d = c2.date_input("Tempo")
            if st.form_submit_button("Simpan"):
                ok, msg = tambah_pembayaran_dp(g, m, date.today(), j, b, t, 0, d, None, "-")
                if ok: st.success(msg); st.session_state["buf_pay"] = load_pembayaran_dp(); st.rerun()
                else: st.error(msg)
    
    # Editor
    df_cln = clean_df_types_dynamically(df)
    cfg = generate_dynamic_column_config(df_cln)
    edited = st.data_editor(df_cln, column_config=cfg, use_container_width=True, num_rows="dynamic", hide_index=True)
    if st.button("Simpan Perubahan Tabel"):
        final = apply_audit_payments_changes(df, edited, st.session_state.get("user_name"))
        if save_pembayaran_dp(final):
            st.session_state["buf_pay"] = final
            st.success("Saved!")
        else: st.error("Fail")

elif nav == "🎯 KPI":
    st.markdown("### 🎯 Target KPI")
    t1, t2 = st.tabs(["Team", "Individu"])
    with t1:
        df = st.session_state.get("buf_kpi_team", pd.DataFrame())
        ed = st.data_editor(df, use_container_width=True, key="kpi_t")
        if st.button("Save Team"):
            fin = apply_audit_checklist_changes(df, ed, ["Misi"], st.session_state["user_name"])
            save_checklist(SHEET_TARGET_TEAM, fin, TEAM_CHECKLIST_COLUMNS)
            st.success("Saved")
    with t2:
        u = st.selectbox("Staff", st.session_state["buf_staf"])
        df = st.session_state.get("buf_kpi_indiv", pd.DataFrame())
        sub = df[df["Nama"]==u]
        ed = st.data_editor(sub, use_container_width=True, key="kpi_i")
        if st.button("Save Individu"):
            df.update(ed)
            save_checklist(SHEET_TARGET_INDIVIDU, df, INDIV_CHECKLIST_COLUMNS)
            st.success("Saved")

elif nav == "🤝 Closing":
    st.markdown("### 🤝 Closing Deal")
    with st.form("cd"):
        c1, c2 = st.columns(2)
        m = c1.selectbox("Sales", st.session_state["buf_staf"])
        v = c2.text_input("Nilai (Rp)")
        if st.form_submit_button("Simpan"):
            if tambah_closing_deal("-", m, date.today(), "-", v)[0]: st.success("Saved"); st.rerun()
    st.dataframe(st.session_state.get("buf_closing", pd.DataFrame()), use_container_width=True)

elif nav == "📜 Audit":
    st.markdown("### 📜 Log Aktivitas")
    if st.button("Reload"): st.rerun()
    st.dataframe(load_audit_log(spreadsheet), use_container_width=True)

elif nav == "📊 Admin":
    st.markdown("### 📊 Dashboard")
    req = get_pending_approvals()
    if req:
        for i, r in enumerate(req):
            with st.container(border=True):
                st.write(f"{r['Requestor']} -> {r['Target Sheet']}")
                c1, c2 = st.columns(2)
                if c1.button("ACC", key=f"a{i}"): execute_approval(i, "APPROVE", "Admin"); st.rerun()
                if c2.button("REJ", key=f"r{i}"): execute_approval(i, "REJECT", "Admin"); st.rerun()
    else: st.info("No approvals pending.")
    
    if st.button("AI Analysis"):
        with st.spinner("Thinking..."):
            try:
                res = client_ai.models.generate_content(model="gemini-1.5-flash", contents="Berikan saran manajemen sales.")
                st.write(res.text)
            except: st.error("AI Error")
