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
import re
import io
import hashlib
import hmac
import base64
import textwrap

# =========================================================
# OPTIONAL LIBS (Excel Export / AgGrid / Plotly)
# =========================================================
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


# =========================================================
# PAGE CONFIG
# =========================================================
APP_TITLE = "Sales & Marketing Action Center"
st.set_page_config(
    page_title=APP_TITLE,
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =========================================================
# GLOBAL STYLE (SpaceX x Muhammadiyah — Elegant, International)
# =========================================================
def inject_global_css():
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700;800&display=swap');

        :root{
            --bg0:#020805;
            --bg1:#04110b;
            --bg2:#062015;
            --cardA: rgba(255,255,255,0.06);
            --cardB: rgba(255,255,255,0.045);
            --border: rgba(255,255,255,0.10);
            --text: rgba(255,255,255,0.92);
            --muted: rgba(255,255,255,0.70);
        }

        /* ---------- App background ---------- */
        .stApp {
            background:
                radial-gradient(circle at 14% 12%, rgba(22, 163, 74, 0.20) 0%, rgba(22, 163, 74, 0.0) 46%),
                radial-gradient(circle at 84% 14%, rgba(250, 204, 21, 0.16) 0%, rgba(250, 204, 21, 0.0) 42%),
                radial-gradient(circle at 18% 92%, rgba(20, 184, 166, 0.12) 0%, rgba(20, 184, 166, 0.0) 40%),
                linear-gradient(180deg, var(--bg0) 0%, var(--bg1) 55%, var(--bg2) 100%);
            color: var(--text);
        }

        /* Subtle starfield overlay */
        .stApp::before {
            content: "";
            position: fixed;
            inset: 0;
            pointer-events: none;
            background: radial-gradient(rgba(255,255,255,0.18) 0.8px, transparent 0.8px);
            background-size: 68px 68px;
            opacity: 0.10;
            mask-image: radial-gradient(circle at 50% 15%, rgba(0,0,0,1) 0%, rgba(0,0,0,0.0) 70%);
        }

        /* Hide Footer & MainMenu (titik tiga kanan atas), TAPI Header jangan di-hidden total */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        
        /* =========================
           CUSTOM HEADER & TOGGLE BUTTON
           ========================= */
        
        /* Buat Header Transparan (supaya background app tetap terlihat) */
        header[data-testid="stHeader"] {
            background-color: transparent !important;
            border-bottom: none !important;
            z-index: 99999; /* Pastikan selalu di atas */
        }

        /* Styling Tombol Hamburger (☰) biar kayak tombol Pop-Up Keren */
        header[data-testid="stHeader"] > button[title="View fullscreen"]{
             display: none; /* Sembunyikan tombol fullscreen bawaan biar bersih */
        }

        /* Tombol Navigasi (Hamburger) */
        button[kind="header"] {
            background-color: rgba(255, 255, 255, 0.05) !important;
            border: 1px solid rgba(255, 255, 255, 0.15) !important;
            color: #ffffff !important;
            border-radius: 8px !important;
            transition: all 0.3s ease;
            margin-top: 10px; /* Sedikit turun biar gak nempel atas */
        }

        button[kind="header"]:hover {
            background-color: rgba(22, 163, 74, 0.6) !important; /* Warna hijau saat hover */
            border-color: rgba(22, 163, 74, 0.8) !important;
            transform: scale(1.1);
        }
        
        /* Ikon panah (chevron) saat sidebar terbuka */
        section[data-testid="stSidebar"] button[kind="header"] {
             margin-top: 10px;
        }

        /* Typography */
        h1, h2, h3, h4, h5, h6, p, label, span, div {
            font-family: "Space Grotesk", sans-serif;
        }

        /* Sidebar polish */
        section[data-testid="stSidebar"] > div {
            background: linear-gradient(180deg, rgba(0,0,0,0.95) 0%, rgba(3,10,6,0.95) 60%, rgba(4,16,11,0.95) 100%);
            border-right: 1px solid rgba(255,255,255,0.10);
        }
        section[data-testid="stSidebar"] * {
            color: var(--text) !important;
        }
        section[data-testid="stSidebar"] hr {
            border-color: rgba(255,255,255,0.10);
        }

        /* Cards */
        div[data-testid="stVerticalBlockBorderWrapper"] > div {
            background: linear-gradient(180deg, var(--cardA) 0%, var(--cardB) 100%);
            border: 1px solid var(--border);
            border-radius: 18px;
            padding: 1.05rem;
            backdrop-filter: blur(10px);
            box-shadow: 0 16px 46px rgba(0,0,0,0.42);
        }

        /* Buttons Standard */
        .stButton>button, .stDownloadButton>button {
            border-radius: 12px !important;
            border: 1px solid rgba(255,255,255,0.14) !important;
            background: rgba(255,255,255,0.05) !important;
            color: var(--text) !important;
            transition: all 0.15s ease-in-out;
        }
        .stButton>button:hover, .stDownloadButton>button:hover {
            transform: translateY(-1px);
            border-color: rgba(250,204,21,0.35) !important;
            background: rgba(255,255,255,0.08) !important;
        }

        /* Primary button */
        button[kind="primary"] {
            background: linear-gradient(135deg, rgba(22,163,74,0.95), rgba(245,158,11,0.92)) !important;
            color: rgba(6, 26, 17, 0.95) !important;
            border: none !important;
        }
        button[kind="primary"]:hover {
            filter: brightness(1.05);
        }

        /* Inputs */
        .stTextInput input, .stTextArea textarea, .stNumberInput input, .stDateInput input, .stSelectbox div[data-baseweb="select"] > div {
            border-radius: 12px !important;
        }
        
        /* Dataframes */
        div[data-testid="stDataFrame"] {
            border-radius: 14px;
            overflow: hidden;
            border: 1px solid rgba(255,255,255,0.10);
        }

        /* HERO & LOGO Styles (Dari update sebelumnya) */
        .sx-hero{
            position: relative;
            border-radius: 20px;
            border: 1px solid rgba(255,255,255,0.12);
            overflow: hidden;
            padding: 18px 18px;
            background:
                radial-gradient(circle at 50% 0%, rgba(255,255,255,0.08) 0%, rgba(255,255,255,0.0) 52%),
                linear-gradient(90deg, rgba(0,0,0,0.55) 0%, rgba(0,0,0,0.25) 50%, rgba(0,0,0,0.55) 100%);
            box-shadow: 0 18px 60px rgba(0,0,0,0.45);
        }
        .sx-hero::before{
            content:""; position:absolute; inset:0; 
            background-image: var(--hero-bg); background-repeat:no-repeat;
            background-position: var(--hero-bg-pos, 50% 72%);
            background-size: var(--hero-bg-size, 140%);
            opacity: 0.28; filter: saturate(1.05) contrast(1.08); pointer-events:none;
        }
        .sx-hero-grid{
            position: relative; display: grid; grid-template-columns: 240px 1fr 240px;
            align-items: center; gap: 14px;
        }
        .sx-hero-grid > * { min-width: 0; }
        @media (max-width: 1100px){ .sx-hero-grid{ grid-template-columns: 200px 1fr 200px; } }
        @media (max-width: 860px){ .sx-hero-grid{ grid-template-columns: 1fr; text-align:center; } }
        
        .sx-logo-card{
            background: rgba(255,255,255,0.92); border: 1px solid rgba(0,0,0,0.06);
            border-radius: 16px; width: 100%; max-width: 240px; 
            height: clamp(120px, 12vw, 160px); padding: 10px;
            display:flex; align-items:center; justify-content:center;
            box-shadow: 0 10px 26px rgba(0,0,0,0.28);
        }
        .sx-logo-card img{
            width: 100%; height: 100%; object-fit: contain; object-position: center; display: block;
        }
        .sx-hero-center{ text-align: center; }
        .sx-title{ font-size: 2.05rem; font-weight: 800; line-height: 1.12; letter-spacing: 0.06em; text-transform: uppercase; margin: 0; }
        .sx-subrow{ margin-top: 0.45rem; display:flex; gap: 0.55rem; flex-wrap: wrap; justify-content: center; align-items: center; color: rgba(255,255,255,0.78); font-size: 0.95rem; }
        .sx-pill{ display:inline-flex; align-items:center; gap: 0.35rem; padding: 0.22rem 0.60rem; border-radius: 999px; border: 1px solid rgba(255,255,255,0.14); background: rgba(255,255,255,0.06); color: rgba(255,255,255,0.88); font-size: 0.80rem; }
        .sx-pill.on{ border-color: rgba(34,197,94,0.55); box-shadow: 0 0 0 2px rgba(34,197,94,0.10) inset; }
        .sx-pill.off{ border-color: rgba(239,68,68,0.55); box-shadow: 0 0 0 2px rgba(239,68,68,0.10) inset; }
        .sx-dot{ width: 8px; height: 8px; border-radius: 999px; display:inline-block; background: rgba(255,255,255,0.55); }
        .sx-pill.on .sx-dot{ background: rgba(34,197,94,0.95); }
        .sx-pill.off .sx-dot{ background: rgba(239,68,68,0.95); }

        /* Sidebar Nav Buttons */
        .sx-nav{ margin-top: 0.25rem; }
        .sx-nav button{ width: 100% !important; text-align: left !important; border-radius: 12px !important; padding: 0.60rem 0.80rem !important; text-transform: uppercase !important; letter-spacing: 0.10em !important; font-size: 0.78rem !important; }
        .sx-nav button[kind="primary"]{ background: linear-gradient(90deg, rgba(22,163,74,0.95), rgba(245,158,11,0.90)) !important; color: rgba(6,26,17,0.95) !important; }
        .sx-section-title{ font-size: 0.82rem; letter-spacing: 0.12em; text-transform: uppercase; color: rgba(255,255,255,0.70); }

        </style>
        """,
        unsafe_allow_html=True
    )

# =========================================================
# COMPAT HELPERS (toast / link button)
# =========================================================
def ui_toast(message: str, icon=None):
    """Streamlit toast (fallback ke success jika tidak tersedia)."""
    if hasattr(st, "toast"):
        try:
            st.toast(message, icon=icon)
            return
        except Exception:
            pass
    st.success(message)


# =========================================================
# CONSTANTS
# =========================================================
NAMA_GOOGLE_SHEET = "Laporan Kegiatan Harian"
FOLDER_DROPBOX = "/Laporan_Kegiatan_Harian"

# Sheet Names
SHEET_CONFIG_NAMA = "Config_Staf"
SHEET_TARGET_TEAM = "Target_Team_Checklist"
SHEET_TARGET_INDIVIDU = "Target_Individu_Checklist"
SHEET_CONFIG_TEAM = "Config_Team"
SHEET_CLOSING_DEAL = "Closing_Deal"
SHEET_PEMBAYARAN = "Pembayaran_DP"

# Kolom laporan harian
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
    COL_PENDING,
    COL_FEEDBACK,
    COL_INTEREST,
    COL_NAMA_KLIEN,
    COL_KONTAK_KLIEN
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
COL_NILAI_KONTRAK = "Nilai Kontrak"  # disimpan sebagai angka (int)

CLOSING_COLUMNS = [COL_GROUP, COL_MARKETING, COL_TGL_EVENT, COL_BIDANG, COL_NILAI_KONTRAK]

# Target/checklist columns
TEAM_CHECKLIST_COLUMNS = ["Misi", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan", COL_TS_UPDATE, COL_UPDATED_BY]
INDIV_CHECKLIST_COLUMNS = ["Nama", "Target", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan", COL_TS_UPDATE, COL_UPDATED_BY]

# Pembayaran Columns
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
    COL_UPDATED_BY
]

TZ_JKT = ZoneInfo("Asia/Jakarta")

# Formatting throttling (avoid heavy batch formatting too frequently)
FORMAT_THROTTLE_SECONDS = 300  # 5 minutes


# =========================================================
# SMALL HELPERS
# =========================================================
def now_ts_str() -> str:
    """Timestamp akurat (WIB) untuk semua perubahan."""
    return datetime.now(tz=TZ_JKT).strftime("%d-%m-%Y %H:%M:%S")


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
    """Return datetime.date or None."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    try:
        return pd.to_datetime(x, errors="coerce").date()
    except Exception:
        return None


def get_actor_fallback(default="-") -> str:
    """
    Ambil 'actor' (siapa yang mengedit) dari session_state yang tersedia.
    Jika tidak ada, fallback ke default.
    """
    for k in ["pelapor_main", "sidebar_user", "payment_editor_name"]:
        if k in st.session_state and safe_str(st.session_state.get(k), "").strip():
            return safe_str(st.session_state.get(k)).strip()
    return default


# =========================================================
# ADMIN PASSWORD HELPERS
# =========================================================
def verify_admin_password(pwd_input: str) -> bool:
    """
    - Support 2 mode:
      (A) st.secrets["password_admin_hash"] = SHA256 hex dari password
      (B) st.secrets["password_admin"] = password plain (legacy)
    """
    pwd_input = safe_str(pwd_input, "").strip()
    if not pwd_input:
        return False

    # Mode hash (disarankan)
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

    # Mode plain (legacy)
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


# =========================================================
# CONNECTIONS
# =========================================================
KONEKSI_GSHEET_BERHASIL = False
KONEKSI_DROPBOX_BERHASIL = False
spreadsheet = None
dbx = None

# 1) Google Sheets
try:
    if "gcp_service_account" in st.secrets:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds_dict = dict(st.secrets["gcp_service_account"])
        if "private_key" in creds_dict:
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open(NAMA_GOOGLE_SHEET)
        KONEKSI_GSHEET_BERHASIL = True
    else:
        st.error("GSheet Error: Kredensial tidak ditemukan.")
except Exception as e:
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
    st.error(f"Dropbox Error: {e}")


# =========================================================
# RUPIAH PARSER (input bebas -> int Rupiah)
# =========================================================
def parse_rupiah_to_int(value):
    """Parser Rupiah yang lebih pintar."""
    if value is None:
        return None

    # jika sudah numeric
    if isinstance(value, (int, float)) and not pd.isna(value):
        try:
            return int(round(float(value)))
        except Exception:
            return None

    s = str(value).strip()
    if not s:
        return None

    s_lower = s.lower().strip()
    if s_lower in {"nan", "none", "-", "null"}:
        return None

    # hilangkan spasi + penanda mata uang
    s_lower = re.sub(r"\\s+", "", s_lower)
    s_lower = s_lower.replace("idr", "").replace("rp", "")

    # deteksi satuan
    multiplier = 1
    if "miliar" in s_lower or "milyar" in s_lower:
        multiplier = 1_000_000_000
    elif "jt" in s_lower or "juta" in s_lower:
        multiplier = 1_000_000
    elif "rb" in s_lower or "ribu" in s_lower:
        multiplier = 1_000

    # buang kata satuan dari string angka
    s_num = re.sub(r"(miliar|milyar|juta|jt|ribu|rb)", "", s_lower)

    # sisakan digit + pemisah ribuan/desimal
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
        digits = re.sub(r"\\D", "", s_num)
        return int(digits) if digits else None

    if multiplier != 1:
        if base >= multiplier:
            return int(round(base))
        return int(round(base * multiplier))

    return int(round(base))


def format_rupiah_display(amount) -> str:
    """Hanya untuk display di UI (bukan untuk disimpan)."""
    try:
        if amount is None or pd.isna(amount):
            return ""
        n = int(amount)
        return "Rp " + f"{n:,}".replace(",", ".")
    except Exception:
        return str(amount)


# =========================================================
# AUDIT LOG HELPERS (PEMBAYARAN)
# =========================================================
def parse_payment_log_lines(log_text: str):
    """
    Normalisasi log lama/baru menjadi list baris TANPA nomor.
    - Kalau log sudah bernomor "1. ..." => nomor dihapus dulu.
    - Kalau format lama pakai ';' dalam satu baris => dipecah jadi multiline.
    - Baris tambahan dalam satu event dibuat indent (diawali spasi).
    """
    log_text = safe_str(log_text, "").strip()
    if not log_text:
        return []

    raw_lines = [ln.rstrip() for ln in log_text.splitlines() if ln.strip()]
    out = []

    for ln in raw_lines:
        # hapus numbering lama kalau ada: "12. ...."
        mnum = re.match(r"^\\s*\\d+\\.\\s*(.*)$", ln)
        if mnum:
            ln = mnum.group(1).rstrip()

        # kalau format: "[ts] (actor) ...."
        m = re.match(r"^\\[(.*?)\\]\\s*\\((.*?)\\)\\s*(.*)$", ln)
        if m:
            ts, actor, rest = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
            prefix = f"[{ts}] ({actor})"

            if rest:
                parts = [p.strip() for p in rest.split(";") if p.strip()]
                if parts:
                    out.append(f"{prefix} {parts[0]}")
                    for p in parts[1:]:
                        out.append(f" {p}")  # indent
                else:
                    out.append(prefix)
            else:
                out.append(prefix)
        else:
            out.append(ln)

    return out


def build_numbered_log(lines):
    """Buat output bernomor 1..N dari list baris (tanpa nomor)."""
    lines = [str(l).rstrip() for l in (lines or []) if safe_str(l, "").strip()]
    return "\\n".join([f"{i}. {line}" for i, line in enumerate(lines, 1)]).strip()


def _fmt_payment_val_for_log(col_name: str, v):
    """Format nilai agar enak dibaca di log."""
    if col_name == COL_NOMINAL_BAYAR:
        x = parse_rupiah_to_int(v)
        return format_rupiah_display(x) if x is not None else "-"
    if col_name == COL_STATUS_BAYAR:
        return "✅ Dibayar" if normalize_bool(v) else "⏳ Belum"
    if col_name in {COL_JATUH_TEMPO, COL_TGL_EVENT}:
        d = normalize_date(v)
        return d.strftime("%Y-%m-%d") if d else "-"
    s = safe_str(v, "-").replace("\\n", " ").strip()
    return s if s else "-"


def append_payment_ts_update(existing_log: str, ts: str, actor: str, changes):
    """
    Append perubahan ke log dengan format rapih & bernomor.
    """
    lines = parse_payment_log_lines(existing_log)
    changes = [safe_str(c, "").strip() for c in (changes or []) if safe_str(c, "").strip()]
    if not changes:
        return build_numbered_log(lines)

    actor = safe_str(actor, "-").strip() or "-"
    ts = safe_str(ts, now_ts_str()).strip() or now_ts_str()

    # baris pertama event
    lines.append(f"[{ts}] ({actor}) {changes[0]}")

    # baris selanjutnya: indent (tanpa ulang prefix)
    for c in changes[1:]:
        lines.append(f" {c}")

    return build_numbered_log(lines)


# =========================================================
# UI DISPLAY HELPERS (RUPIAH)
# =========================================================
def payment_df_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """Untuk tampilan UI saja."""
    dfv = df.copy()
    if dfv is None or dfv.empty:
        return dfv
    if COL_NOMINAL_BAYAR in dfv.columns:
        dfv[COL_NOMINAL_BAYAR] = dfv[COL_NOMINAL_BAYAR].apply(
            lambda x: "" if x is None or pd.isna(x) else format_rupiah_display(x)
        )
    return dfv


def on_change_pay_nominal():
    """Auto-format input nominal ke 'Rp 15.000.000' (untuk UI)."""
    raw = st.session_state.get("pay_nominal", "")
    val = parse_rupiah_to_int(raw)
    if val is not None:
        st.session_state["pay_nominal"] = format_rupiah_display(val)


def reset_payment_form_state():
    """Reset field input pembayaran (agar terasa seperti clear_on_submit)."""
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
                st.session_state[k] = datetime.now(tz=TZ_JKT).date()
            elif k == "pay_due_date":
                st.session_state[k] = datetime.now(tz=TZ_JKT).date() + timedelta(days=7)
            elif k == "pay_jenis_opt":
                st.session_state[k] = "Down Payment (DP)"
            elif k == "pay_status":
                st.session_state[k] = False
            else:
                st.session_state[k] = ""
        except Exception:
            pass


# =========================================================
# EXCEL EXPORT
# =========================================================
def df_to_excel_bytes(
    df: pd.DataFrame,
    sheet_name="Sheet1",
    col_widths=None,
    wrap_cols=None,
    right_align_cols=None,
    number_format_cols=None
):
    """Export dataframe ke .xlsx rapi."""
    if not HAS_OPENPYXL:
        return None

    df_export = df.copy()
    df_export = df_export.where(pd.notna(df_export), None)

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


# =========================================================
# GOOGLE SHEETS FORMATTING
# =========================================================
def _build_currency_number_format_rupiah():
    return {"type": "CURRENCY", "pattern": '"Rp" #,##0'}


def maybe_auto_format_sheet(worksheet, force: bool = False):
    """Throttled formatting: avoid calling heavy formatting too often."""
    try:
        if worksheet is None:
            return
        if "_fmt_sheet_last" not in st.session_state:
            st.session_state["_fmt_sheet_last"] = {}

        now = time.time()
        key = str(getattr(worksheet, "id", "unknown"))
        last = float(st.session_state["_fmt_sheet_last"].get(key, 0))
        if force or (now - last) > FORMAT_THROTTLE_SECONDS:
            auto_format_sheet(worksheet)
            st.session_state["_fmt_sheet_last"][key] = now
    except Exception:
        # Never break app due to formatting.
        pass


def auto_format_sheet(worksheet):
    """Auto-format Google Sheet."""
    try:
        sheet_id = worksheet.id
        all_values = worksheet.get_all_values()
        if not all_values:
            return

        headers = all_values[0]
        data_row_count = len(all_values)
        formatting_row_count = max(worksheet.row_count, data_row_count)

        requests = []
        default_body_format = {"verticalAlignment": "TOP", "wrapStrategy": "CLIP"}

        # 1) Reset body base style
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": formatting_row_count},
                "cell": {"userEnteredFormat": default_body_format},
                "fields": "userEnteredFormat(verticalAlignment,wrapStrategy)"
            }
        })

        # 2) Column sizing + per-column overrides
        for i, col_name in enumerate(headers):
            col_index = i
            cell_format_override = {}
            width = 100

            long_text_cols = {
                "Misi", "Target", "Deskripsi", "Bukti/Catatan", "Link Foto", "Link Sosmed",
                "Tempat Dikunjungi", "Kesimpulan", "Kendala", "Next Plan (Pending)", "Feedback Lead",
                COL_KENDALA_KLIEN,
                COL_NAMA_KLIEN,
                TEAM_COL_NAMA_TEAM, TEAM_COL_POSISI, TEAM_COL_ANGGOTA,
                COL_GROUP, COL_MARKETING, COL_BIDANG,
                COL_JENIS_BAYAR, COL_BUKTI_BAYAR, COL_CATATAN_BAYAR,
                COL_TS_UPDATE,
            }

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

            # Set width
            requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": col_index,
                        "endIndex": col_index + 1
                    },
                    "properties": {"pixelSize": width},
                    "fields": "pixelSize"
                }
            })

            # Apply per-column format
            if cell_format_override:
                fields = ",".join(cell_format_override.keys())
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "endRowIndex": formatting_row_count,
                            "startColumnIndex": col_index,
                            "endColumnIndex": col_index + 1
                        },
                        "cell": {"userEnteredFormat": cell_format_override},
                        "fields": f"userEnteredFormat({fields})"
                    }
                })

        # 3) Header style
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {
                    "textFormat": {"bold": True},
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE",
                    "backgroundColor": {"red": 0.90, "green": 0.92, "blue": 0.96},
                    "wrapStrategy": "WRAP"
                }},
                "fields": "userEnteredFormat(textFormat,horizontalAlignment,verticalAlignment,backgroundColor,wrapStrategy)"
            }
        })

        # 4) Freeze header
        requests.append({
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount"
            }
        })

        worksheet.spreadsheet.batch_update({"requests": requests})
    except Exception as e:
        print(f"Format Error: {e}")


def ensure_headers(worksheet, desired_headers):
    """
    Pastikan header sesuai urutan standar.
    """
    try:
        if worksheet.col_count < len(desired_headers):
            worksheet.resize(cols=len(desired_headers))

        headers = worksheet.row_values(1)
        need_reset = (
            not headers
            or (len(headers) < len(desired_headers))
            or (headers[:len(desired_headers)] != desired_headers)
        )
        if need_reset:
            worksheet.update(range_name="A1", values=[desired_headers], value_input_option="USER_ENTERED")
            maybe_auto_format_sheet(worksheet, force=True)
    except Exception as e:
        print(f"Ensure Header Error: {e}")


# =========================================================
# WORKSHEET GET/CREATE + STAFF LIST
# =========================================================
@st.cache_resource(ttl=600)
def _get_or_create_ws_cached(nama_worksheet: str):
    """Get/create worksheet object (cached)."""
    try:
        ws = spreadsheet.worksheet(nama_worksheet)
        return ws
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=nama_worksheet, rows=200, cols=len(NAMA_KOLOM_STANDAR))
        ws.append_row(NAMA_KOLOM_STANDAR, value_input_option="USER_ENTERED")
        maybe_auto_format_sheet(ws, force=True)
        return ws
    except Exception:
        return None


def get_or_create_worksheet(nama_worksheet):
    """
    Pastikan header selalu up-to-date.
    """
    ws = _get_or_create_ws_cached(nama_worksheet)
    if ws is not None:
        ensure_headers(ws, NAMA_KOLOM_STANDAR)
    return ws


@st.cache_data(ttl=120)
def get_daftar_staf_terbaru():
    default_staf = ["Saya"]
    if not KONEKSI_GSHEET_BERHASIL:
        return default_staf

    try:
        try:
            ws = spreadsheet.worksheet(SHEET_CONFIG_NAMA)
        except Exception:
            ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_NAMA, rows=100, cols=1)
            ws.append_row(["Daftar Nama Staf"], value_input_option="USER_ENTERED")
            ws.append_row(["Saya"], value_input_option="USER_ENTERED")
            maybe_auto_format_sheet(ws, force=True)
            return default_staf

        nama_list = ws.col_values(1)
        if nama_list and nama_list[0] == "Daftar Nama Staf":
            nama_list.pop(0)

        return nama_list if nama_list else default_staf
    except Exception:
        return default_staf


def tambah_staf_baru(nama_baru):
    try:
        try:
            ws = spreadsheet.worksheet(SHEET_CONFIG_NAMA)
        except Exception:
            ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_NAMA, rows=100, cols=1)

        if nama_baru in ws.col_values(1):
            return False, "Nama sudah ada!"

        ws.append_row([nama_baru], value_input_option="USER_ENTERED")
        maybe_auto_format_sheet(ws)
        return True, "Berhasil tambah tim!"
    except Exception as e:
        return False, str(e)


# =========================================================
# TEAM CONFIG
# =========================================================
@st.cache_data(ttl=120)
def load_team_config():
    if not KONEKSI_GSHEET_BERHASIL:
        return pd.DataFrame(columns=TEAM_COLUMNS)

    try:
        try:
            ws = spreadsheet.worksheet(SHEET_CONFIG_TEAM)
        except Exception:
            ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_TEAM, rows=300, cols=len(TEAM_COLUMNS))
            ws.append_row(TEAM_COLUMNS, value_input_option="USER_ENTERED")
            maybe_auto_format_sheet(ws, force=True)
            return pd.DataFrame(columns=TEAM_COLUMNS)

        data = ws.get_all_records()
        df = pd.DataFrame(data).fillna("")
        for c in TEAM_COLUMNS:
            if c not in df.columns:
                df[c] = ""
        return df[TEAM_COLUMNS].copy()
    except Exception:
        return pd.DataFrame(columns=TEAM_COLUMNS)


def tambah_team_baru(nama_team, posisi, anggota_list):
    if not KONEKSI_GSHEET_BERHASIL:
        return False, "Koneksi GSheet belum aktif."

    try:
        nama_team = str(nama_team).strip()
        posisi = str(posisi).strip()
        anggota_list = [str(a).strip() for a in anggota_list if str(a).strip()]

        if not nama_team or not posisi or not anggota_list:
            return False, "Nama team, posisi, dan minimal 1 anggota wajib diisi."

        try:
            ws = spreadsheet.worksheet(SHEET_CONFIG_TEAM)
        except Exception:
            ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_TEAM, rows=300, cols=len(TEAM_COLUMNS))
            ws.append_row(TEAM_COLUMNS, value_input_option="USER_ENTERED")
            maybe_auto_format_sheet(ws, force=True)

        existing = set()
        try:
            for r in ws.get_all_records():
                key = (
                    str(r.get(TEAM_COL_NAMA_TEAM, "")).strip(),
                    str(r.get(TEAM_COL_POSISI, "")).strip(),
                    str(r.get(TEAM_COL_ANGGOTA, "")).strip()
                )
                existing.add(key)
        except Exception:
            pass

        rows_to_add = []
        for anggota in anggota_list:
            key = (nama_team, posisi, anggota)
            if key not in existing:
                rows_to_add.append([nama_team, posisi, anggota])

        if not rows_to_add:
            return False, "Semua anggota sudah terdaftar di team tersebut."

        ws.append_rows(rows_to_add, value_input_option="USER_ENTERED")
        maybe_auto_format_sheet(ws)
        return True, f"Berhasil tambah team '{nama_team}' ({len(rows_to_add)} anggota)."
    except Exception as e:
        return False, str(e)


# =========================================================
# DROPBOX UPLOAD
# =========================================================
def upload_ke_dropbox(file_obj, nama_staf, kategori="Umum"):
    if not KONEKSI_DROPBOX_BERHASIL or dbx is None:
        return "Koneksi Dropbox Error"

    try:
        file_data = file_obj.getvalue()
        ts = datetime.now(tz=TZ_JKT).strftime("%Y%m%d_%H%M%S")

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


# =========================================================
# TARGET / CHECKLIST HELPERS
# =========================================================
def clean_bulk_input(text_input):
    lines = (text_input or "").split("\\n")
    cleaned_targets = []
    for line in lines:
        cleaned = re.sub(r"^[\\d\\.\\-\\*\\s]+", "", line).strip()
        if cleaned:
            cleaned_targets.append(cleaned)
    return cleaned_targets


@st.cache_data(ttl=120)
def load_checklist(sheet_name, columns):
    try:
        try:
            ws = spreadsheet.worksheet(sheet_name)
        except Exception:
            ws = spreadsheet.add_worksheet(title=sheet_name, rows=200, cols=len(columns))
            ws.append_row(columns, value_input_option="USER_ENTERED")
            maybe_auto_format_sheet(ws, force=True)
            return pd.DataFrame(columns=columns)

        ensure_headers(ws, columns)

        data = ws.get_all_records()
        df = pd.DataFrame(data).fillna("")

        for col in columns:
            if col not in df.columns:
                if col == "Status":
                    df[col] = False
                else:
                    df[col] = ""

        if "Status" in df.columns:
            df["Status"] = df["Status"].apply(lambda x: True if str(x).upper() == "TRUE" else False)

        return df[columns].copy()
    except Exception:
        return pd.DataFrame(columns=columns)


def save_checklist(sheet_name, df, columns):
    try:
        ws = spreadsheet.worksheet(sheet_name)
        ensure_headers(ws, columns)

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

        df_save = df_save[columns].astype(str)
        data_to_save = [df_save.columns.values.tolist()] + df_save.values.tolist()

        ws.update(range_name="A1", values=data_to_save, value_input_option="USER_ENTERED")
        maybe_auto_format_sheet(ws)
        return True
    except Exception:
        return False


def apply_audit_checklist_changes(df_before: pd.DataFrame, df_after: pd.DataFrame, key_cols, actor: str):
    """Update audit columns hanya untuk baris yang benar-benar berubah."""
    if df_after is None or df_after.empty:
        return df_after

    actor = safe_str(actor, "-").strip() or "-"

    before = df_before.copy() if df_before is not None else pd.DataFrame()
    after = df_after.copy()

    for c in [COL_TS_UPDATE, COL_UPDATED_BY]:
        if c not in after.columns:
            after[c] = ""

    def make_key_row(r):
        return tuple(safe_str(r.get(k, "")).strip() for k in key_cols)

    before_map = {}
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


def add_bulk_targets(sheet_name, base_row_data, targets_list):
    try:
        try:
            ws = spreadsheet.worksheet(sheet_name)
        except Exception:
            return False

        columns = TEAM_CHECKLIST_COLUMNS if sheet_name == SHEET_TARGET_TEAM else INDIV_CHECKLIST_COLUMNS
        ensure_headers(ws, columns)

        actor = get_actor_fallback(default="Admin")
        ts = now_ts_str()

        rows_to_add = []
        for t in targets_list:
            row_vals = list(base_row_data) if base_row_data else []
            new_row = [""] * len(columns)

            for i in range(min(len(row_vals), len(columns))):
                new_row[i] = row_vals[i]

            if sheet_name == SHEET_TARGET_TEAM:
                new_row[0] = t
            elif sheet_name == SHEET_TARGET_INDIVIDU:
                new_row[1] = t

            if COL_TS_UPDATE in columns:
                new_row[columns.index(COL_TS_UPDATE)] = ts
            if COL_UPDATED_BY in columns:
                new_row[columns.index(COL_UPDATED_BY)] = actor

            rows_to_add.append(new_row)

        ws.append_rows(rows_to_add, value_input_option="USER_ENTERED")
        maybe_auto_format_sheet(ws)
        return True
    except Exception:
        return False


def update_evidence_row(sheet_name, target_name, note, file_obj, user_folder_name, kategori_folder):
    """
    Update bukti/catatan untuk checklist (Team/Individu).
    ✅ Optimasi: gunakan batch_update untuk mengurangi jumlah API call.
    """
    try:
        ws = spreadsheet.worksheet(sheet_name)

        columns = TEAM_CHECKLIST_COLUMNS if sheet_name == SHEET_TARGET_TEAM else INDIV_CHECKLIST_COLUMNS
        ensure_headers(ws, columns)

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

        final_note = f"{catatan_lama}\\n{update_text}" if catatan_lama.strip() else update_text
        final_note = final_note.strip() if final_note.strip() else "-"
        final_note = final_note.strip() if final_note.strip() else "-"

        headers = ws.row_values(1)
        if "Bukti/Catatan" not in headers:
            return False, "Kolom Bukti error."

        updates = []

        # Bukti/Catatan
        col_idx_gsheet = headers.index("Bukti/Catatan") + 1
        cell_address = gspread.utils.rowcol_to_a1(row_idx_gsheet, col_idx_gsheet)
        updates.append({"range": cell_address, "values": [[final_note]]})

        # Timestamp Update
        if COL_TS_UPDATE in headers:
            col_ts = headers.index(COL_TS_UPDATE) + 1
            cell_ts = gspread.utils.rowcol_to_a1(row_idx_gsheet, col_ts)
            updates.append({"range": cell_ts, "values": [[ts_update]]})

        # Updated By
        if COL_UPDATED_BY in headers:
            col_by = headers.index(COL_UPDATED_BY) + 1
            cell_by = gspread.utils.rowcol_to_a1(row_idx_gsheet, col_by)
            updates.append({"range": cell_by, "values": [[actor]]})

        ws.batch_update(updates, value_input_option="USER_ENTERED")

        maybe_auto_format_sheet(ws)
        return True, "Berhasil update!"
    except Exception as e:
        return False, f"Error: {e}"


# =========================================================
# FEEDBACK + DAILY REPORT
# =========================================================
def kirim_feedback_admin(nama_staf, timestamp_key, isi_feedback):
    try:
        ws = spreadsheet.worksheet(nama_staf)

        if ws.col_count < len(NAMA_KOLOM_STANDAR):
            ws.resize(cols=len(NAMA_KOLOM_STANDAR))

        headers = ws.row_values(1)
        if COL_FEEDBACK not in headers:
            ws.update_cell(1, len(headers) + 1, COL_FEEDBACK)
            headers.append(COL_FEEDBACK)
            maybe_auto_format_sheet(ws, force=True)

        all_timestamps = ws.col_values(1)

        def clean_ts(text):
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

        ts = now_ts_str()
        actor = get_actor_fallback(default="Admin")
        feedback_text = f"[{ts}] ({actor}) {isi_feedback}"

        ws.update_cell(found_row, col_idx, feedback_text)
        return True, "Feedback terkirim!"
    except Exception as e:
        return False, f"Error: {e}"


def simpan_laporan_harian_batch(list_of_rows, nama_staf):
    try:
        ws = get_or_create_worksheet(nama_staf)
        if ws is None:
            return False

        ensure_headers(ws, NAMA_KOLOM_STANDAR)
        ws.append_rows(list_of_rows, value_input_option="USER_ENTERED")

        # ✅ Optimasi: jangan format tiap submit (throttled)
        maybe_auto_format_sheet(ws)

        return True
    except Exception as e:
        print(f"Error saving daily report batch: {e}")
        return False


@st.cache_data(ttl=45)
def get_reminder_pending(nama_staf):
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


@st.cache_data(ttl=120)
def load_all_reports(daftar_staf):
    all_data = []
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


def render_hybrid_table(df_data, unique_key, main_text_col):
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
                    width=300
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
                key=f"aggrid_{unique_key}"
            )
            return pd.DataFrame(grid_response["data"])
        except Exception:
            use_aggrid_attempt = False

    column_config = {}
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
        use_container_width=True
    )


# =========================================================
# CLOSING DEAL
# =========================================================
@st.cache_data(ttl=120)
def load_closing_deal():
    if not KONEKSI_GSHEET_BERHASIL:
        return pd.DataFrame(columns=CLOSING_COLUMNS)

    try:
        try:
            ws = spreadsheet.worksheet(SHEET_CLOSING_DEAL)
        except Exception:
            ws = spreadsheet.add_worksheet(title=SHEET_CLOSING_DEAL, rows=300, cols=len(CLOSING_COLUMNS))
            ws.append_row(CLOSING_COLUMNS, value_input_option="USER_ENTERED")
            maybe_auto_format_sheet(ws, force=True)
            return pd.DataFrame(columns=CLOSING_COLUMNS)

        ensure_headers(ws, CLOSING_COLUMNS)

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


def tambah_closing_deal(nama_group, nama_marketing, tanggal_event, bidang, nilai_kontrak_input):
    if not KONEKSI_GSHEET_BERHASIL:
        return False, "Koneksi GSheet belum aktif."

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
            return False, "Nilai Kontrak tidak valid. Contoh: 15000000 / 15.000.000 / Rp 15.000.000 / 15jt / 15,5jt"

        try:
            ws = spreadsheet.worksheet(SHEET_CLOSING_DEAL)
        except Exception:
            ws = spreadsheet.add_worksheet(title=SHEET_CLOSING_DEAL, rows=300, cols=len(CLOSING_COLUMNS))
            ws.append_row(CLOSING_COLUMNS, value_input_option="USER_ENTERED")

        ensure_headers(ws, CLOSING_COLUMNS)

        tgl_str = tanggal_event.strftime("%Y-%m-%d") if hasattr(tanggal_event, "strftime") else str(tanggal_event)

        ws.append_row([nama_group, nama_marketing, tgl_str, bidang, int(nilai_int)], value_input_option="USER_ENTERED")

        maybe_auto_format_sheet(ws)
        return True, "Closing deal berhasil disimpan!"
    except Exception as e:
        return False, str(e)


# =========================================================
# PEMBAYARAN
# =========================================================
@st.cache_data(ttl=120)
def load_pembayaran_dp():
    if not KONEKSI_GSHEET_BERHASIL:
        return pd.DataFrame(columns=PAYMENT_COLUMNS)

    try:
        try:
            ws = spreadsheet.worksheet(SHEET_PEMBAYARAN)
        except Exception:
            ws = spreadsheet.add_worksheet(title=SHEET_PEMBAYARAN, rows=500, cols=len(PAYMENT_COLUMNS))
            ws.append_row(PAYMENT_COLUMNS, value_input_option="USER_ENTERED")
            maybe_auto_format_sheet(ws, force=True)
            return pd.DataFrame(columns=PAYMENT_COLUMNS)

        ensure_headers(ws, PAYMENT_COLUMNS)

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

        for c in [COL_TS_BAYAR, COL_GROUP, COL_MARKETING, COL_TGL_EVENT, COL_JENIS_BAYAR,
                  COL_BUKTI_BAYAR, COL_CATATAN_BAYAR, COL_TS_UPDATE, COL_UPDATED_BY]:
            if c in df.columns:
                df[c] = df[c].fillna("").astype(str)

        # rapihkan log agar tampil bernomor & multiline
        if COL_TS_UPDATE in df.columns:
            df[COL_TS_UPDATE] = df[COL_TS_UPDATE].apply(lambda x: build_numbered_log(parse_payment_log_lines(x)))

        # fallback: kalau log kosong tapi ada timestamp input
        if COL_TS_BAYAR in df.columns and COL_TS_UPDATE in df.columns:
            def _fix_empty_log(row):
                logv = safe_str(row.get(COL_TS_UPDATE, ""), "").strip()
                if logv:
                    return logv
                ts_in = safe_str(row.get(COL_TS_BAYAR, ""), "").strip()
                return build_numbered_log([ts_in]) if ts_in else ""
            df[COL_TS_UPDATE] = df.apply(_fix_empty_log, axis=1)

        return df[PAYMENT_COLUMNS].copy()
    except Exception:
        return pd.DataFrame(columns=PAYMENT_COLUMNS)


def save_pembayaran_dp(df: pd.DataFrame) -> bool:
    try:
        ws = spreadsheet.worksheet(SHEET_PEMBAYARAN)
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
        maybe_auto_format_sheet(ws)
        return True
    except Exception:
        return False


def apply_audit_payments_changes(df_before: pd.DataFrame, df_after: pd.DataFrame, actor: str):
    """Update Timestamp Update (Log) & Updated By hanya untuk baris yang berubah."""
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

        changes = []

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
    nama_group,
    nama_marketing,
    tanggal_event,
    jenis_bayar,
    nominal_input,
    jatuh_tempo,
    status_bayar,
    bukti_file,
    catatan
):
    """Tambah 1 record pembayaran."""
    if not KONEKSI_GSHEET_BERHASIL:
        return False, "Koneksi GSheet belum aktif."

    try:
        nama_group = str(nama_group).strip() if nama_group else "-"
        nama_marketing = str(nama_marketing).strip() if nama_marketing else ""
        jenis_bayar = str(jenis_bayar).strip() if jenis_bayar else "Down Payment (DP)"
        catatan = str(catatan).strip() if catatan else "-"

        if not nama_marketing or not str(nominal_input).strip() or not jatuh_tempo:
            return False, "Field wajib: Nama Marketing, Nominal, dan Batas Waktu Bayar."

        nominal_int = parse_rupiah_to_int(nominal_input)
        if nominal_int is None:
            return False, "Nominal tidak valid. Contoh: 5000000 / 5jt / Rp 5.000.000 / 5,5jt"

        link_bukti = "-"
        if bukti_file and KONEKSI_DROPBOX_BERHASIL:
            link_bukti = upload_ke_dropbox(bukti_file, nama_marketing, kategori="Bukti_Pembayaran")

        try:
            ws = spreadsheet.worksheet(SHEET_PEMBAYARAN)
        except Exception:
            ws = spreadsheet.add_worksheet(title=SHEET_PEMBAYARAN, rows=500, cols=len(PAYMENT_COLUMNS))
            ws.append_row(PAYMENT_COLUMNS, value_input_option="USER_ENTERED")

        ensure_headers(ws, PAYMENT_COLUMNS)

        tgl_event_str = tanggal_event.strftime("%Y-%m-%d") if hasattr(tanggal_event, "strftime") else (str(tanggal_event) if tanggal_event else "-")
        jatuh_tempo_str = jatuh_tempo.strftime("%Y-%m-%d") if hasattr(jatuh_tempo, "strftime") else str(jatuh_tempo)

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
                actor0
            ],
            value_input_option="USER_ENTERED"
        )

        maybe_auto_format_sheet(ws)
        return True, "Pembayaran berhasil disimpan!"
    except Exception as e:
        return False, str(e)


def build_alert_pembayaran(df: pd.DataFrame, days_due_soon: int = 3):
    if df is None or df.empty:
        return (pd.DataFrame(columns=df.columns if df is not None else PAYMENT_COLUMNS),
                pd.DataFrame(columns=df.columns if df is not None else PAYMENT_COLUMNS))

    today = datetime.now(tz=TZ_JKT).date()

    df2 = df.copy()
    if COL_STATUS_BAYAR in df2.columns:
        df2 = df2[df2[COL_STATUS_BAYAR] == False].copy()

    if COL_JATUH_TEMPO in df2.columns:
        df2 = df2[pd.notna(df2[COL_JATUH_TEMPO])].copy()
    else:
        return (pd.DataFrame(columns=df.columns), pd.DataFrame(columns=df.columns))

    overdue = df2[df2[COL_JATUH_TEMPO] < today].copy()
    due_soon = df2[(df2[COL_JATUH_TEMPO] >= today) & (df2[COL_JATUH_TEMPO] <= (today + timedelta(days=days_due_soon)))].copy()

    return overdue, due_soon


def update_bukti_pembayaran_by_index(row_index_0based: int, file_obj, nama_marketing: str, actor: str = "-"):
    if not KONEKSI_GSHEET_BERHASIL:
        return False, "Koneksi GSheet belum aktif."
    if not KONEKSI_DROPBOX_BERHASIL:
        return False, "Dropbox non-aktif. Upload bukti dimatikan."
    if file_obj is None:
        return False, "File bukti belum dipilih."

    try:
        ws = spreadsheet.worksheet(SHEET_PEMBAYARAN)
        ensure_headers(ws, PAYMENT_COLUMNS)

        link = upload_ke_dropbox(file_obj, nama_marketing or "Unknown", kategori="Bukti_Pembayaran")
        if not link or link == "-":
            return False, "Gagal upload ke Dropbox."

        headers = ws.row_values(1)
        row_gsheet = row_index_0based + 2

        if COL_BUKTI_BAYAR not in headers:
            return False, "Kolom 'Bukti Pembayaran' tidak ditemukan."
        col_bukti = headers.index(COL_BUKTI_BAYAR) + 1

        old_bukti = ""
        try:
            old_bukti = ws.cell(row_gsheet, col_bukti).value
        except Exception:
            old_bukti = ""

        cell_bukti = gspread.utils.rowcol_to_a1(row_gsheet, col_bukti)

        ts = now_ts_str()
        actor_final = safe_str(actor, "-").strip() or "-"

        updates = [{"range": cell_bukti, "values": [[link]]}]

        if COL_TS_UPDATE in headers:
            col_ts = headers.index(COL_TS_UPDATE) + 1
            old_log = ""
            try:
                old_log = ws.cell(row_gsheet, col_ts).value
            except Exception:
                old_log = ""
            new_log = append_payment_ts_update(
                old_log,
                ts,
                actor_final,
                [f"{COL_BUKTI_BAYAR}: {_fmt_payment_val_for_log(COL_BUKTI_BAYAR, old_bukti)} → {_fmt_payment_val_for_log(COL_BUKTI_BAYAR, link)}"]
            )
            cell_ts = gspread.utils.rowcol_to_a1(row_gsheet, col_ts)
            updates.append({"range": cell_ts, "values": [[new_log]]})

        if COL_UPDATED_BY in headers:
            col_by = headers.index(COL_UPDATED_BY) + 1
            cell_by = gspread.utils.rowcol_to_a1(row_gsheet, col_by)
            updates.append({"range": cell_by, "values": [[actor_final]]})

        ws.batch_update(updates, value_input_option="USER_ENTERED")
        maybe_auto_format_sheet(ws)
        return True, "Bukti pembayaran berhasil di-update!"
    except Exception as e:
        return False, f"Error: {e}"


# =========================================================
# HEADER (LOGO LEFT/RIGHT + HOLDING BACKGROUND)
# =========================================================
ASSET_DIR = Path(__file__).parent / "assets"
LOGO_LEFT = ASSET_DIR / "log EO.png"
LOGO_RIGHT = ASSET_DIR / "logo traine.png"

# Logo holding tetap dipakai, tapi jadi logo mandiri di atas judul
LOGO_HOLDING = ASSET_DIR / "Logo-holding.png"

# Background hero diganti jadi sportarium
HERO_BG = ASSET_DIR / "sportarium.jpg"

def _img_to_base64(path: Path) -> str:
    try:
        if path and path.exists():
            return base64.b64encode(path.read_bytes()).decode("utf-8")
        return ""
    except Exception:
        return ""

def render_header():
    ts_now = datetime.now(tz=TZ_JKT).strftime("%d %B %Y %H:%M:%S")

    left_b64 = _img_to_base64(LOGO_LEFT)
    right_b64 = _img_to_base64(LOGO_RIGHT)
    holding_b64 = _img_to_base64(LOGO_HOLDING) # Logo UMB
    bg_b64 = _img_to_base64(HERO_BG)

    g_on = bool(KONEKSI_GSHEET_BERHASIL)
    d_on = bool(KONEKSI_DROPBOX_BERHASIL)

    def pill(label: str, on: bool):
        cls = "sx-pill on" if on else "sx-pill off"
        return f"<span class='{cls}'><span class='sx-dot'></span>{label}</span>"

    # Style background hero (Sportarium)
    hero_style = (
        f"--hero-bg: url('data:image/jpeg;base64,{bg_b64}'); "
        f"--hero-bg-pos: 50% 72%; "
        f"--hero-bg-size: 140%;"
    ) if bg_b64 else "--hero-bg: none;"

    # Logo Kiri & Kanan (Mentari Sejuk)
    left_html = f"<img src='data:image/png;base64,{left_b64}' alt='Logo EO' />" if left_b64 else ""
    right_html = f"<img src='data:image/png;base64,{right_b64}' alt='Logo Training' />" if right_b64 else ""

    # --- BAGIAN BARU: Logo Holding di Paling Atas ---
    # Kita buat div terpisah di luar card utama
    top_logo_html = ""
    if holding_b64:
        top_logo_html = f"""
        <div style="display: flex; justify-content: center; margin-bottom: 25px; padding-top: 10px;">
            <img src='data:image/png;base64,{holding_b64}' 
                 alt='Holding Logo'
                 style="height: 100px; width: auto; object-fit: contain; filter: drop-shadow(0 5px 15px rgba(0,0,0,0.5));" />
        </div>
        """

    # Susunan HTML: Logo Atas -> Baru kemudian Hero Card
    html = f"""
{top_logo_html}
<div class="sx-hero" style="{hero_style}">
<div class="sx-hero-grid">
<div class="sx-logo-card">{left_html}</div>
<div class="sx-hero-center">
<div class="sx-title">🚀 {APP_TITLE}</div>
<div class="sx-subrow">
<span>Realtime: {ts_now}</span>
{pill('GSheet: ON' if g_on else 'GSheet: OFF', g_on)}
{pill('Dropbox: ON' if d_on else 'Dropbox: OFF', d_on)}
</div>
</div>
<div class="sx-logo-card">{right_html}</div>
</div>
</div>
    """
    
    st.markdown(html, unsafe_allow_html=True)



# =========================================================
# APP UI
# =========================================================
if not KONEKSI_GSHEET_BERHASIL:
    st.error("Database Error.")
    st.stop()

# Small banner for Dropbox status
if not KONEKSI_DROPBOX_BERHASIL:
    st.warning("⚠️ Dropbox non-aktif. Fitur upload foto/bukti dimatikan.")

# Session defaults
if "is_admin" not in st.session_state:
    st.session_state["is_admin"] = False

if "menu_nav" not in st.session_state:
    st.session_state["menu_nav"] = "📝 Laporan Harian"

# Render header
render_header()

# =========================================================
# SIDEBAR (SpaceX-inspired)
# =========================================================
with st.sidebar:
    st.markdown("<div class='sx-section-title'>Navigation</div>", unsafe_allow_html=True)

    menu_items = [
        "📝 Laporan Harian",
        "🎯 Target & KPI",
        "🤝 Closing Deal",
        "💳 Pembayaran",
    ]
    if st.session_state["is_admin"]:
        menu_items.append("📊 Dashboard Admin")

    # SpaceX-like nav buttons
    st.markdown("<div class='sx-nav'>", unsafe_allow_html=True)
    for i, item in enumerate(menu_items):
        active = (st.session_state.get("menu_nav") == item)
        btype = "primary" if active else "secondary"
        if st.button(item, use_container_width=True, type=btype, key=f"nav_{i}"):
            st.session_state["menu_nav"] = item
            st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

    st.divider()

    # Admin login
    if not st.session_state["is_admin"]:
        with st.expander("🔐 Akses Khusus Admin", expanded=False):
            if not admin_secret_configured():
                st.warning("Admin login belum aktif: set `password_admin_hash` (disarankan) atau `password_admin` di Streamlit Secrets.")
            pwd = st.text_input("Password:", type="password", key="input_pwd")
            if st.button("Login Admin", use_container_width=True):
                if verify_admin_password(pwd):
                    st.session_state["is_admin"] = True
                    # Jika baru login, refresh menu agar Dashboard muncul.
                    st.rerun()
                else:
                    st.error("Password salah / belum dikonfigurasi!")
    else:
        if st.button("🔓 Logout Admin", use_container_width=True):
            st.session_state["is_admin"] = False
            # Kalau sedang di dashboard, pindahkan ke laporan harian.
            if st.session_state.get("menu_nav") == "📊 Dashboard Admin":
                st.session_state["menu_nav"] = "📝 Laporan Harian"
            st.rerun()

    st.divider()

    # Quick stats (lightweight)
    try:
        df_pay_sidebar = load_pembayaran_dp()
        overdue_s, due_soon_s = build_alert_pembayaran(df_pay_sidebar, days_due_soon=3) if not df_pay_sidebar.empty else (pd.DataFrame(), pd.DataFrame())
        st.markdown("<div class='sx-section-title'>Quick Stats</div>", unsafe_allow_html=True)
        st.metric("Overdue Payment", int(len(overdue_s)) if overdue_s is not None else 0)
        st.metric("Due ≤ 3 hari", int(len(due_soon_s)) if due_soon_s is not None else 0)
    except Exception:
        pass

    st.divider()
    st.caption("Tip: navigasi ala SpaceX → ringkas, jelas, fokus.")


menu_nav = st.session_state.get("menu_nav", "📝 Laporan Harian")


# =========================================================
# MENU: LAPORAN HARIAN
# =========================================================
if menu_nav == "📝 Laporan Harian":
    staff_list = get_daftar_staf_terbaru()

    # Top cards: Reminder & last feedback
    col_a, col_b = st.columns([1, 1])
    with col_a:
        with st.container(border=True):
            st.markdown("#### 👤 Pelapor & Reminder")
            nama_pelapor = st.selectbox("Nama Pelapor", staff_list, key="pelapor_main")
            pending_msg = get_reminder_pending(nama_pelapor)
            if pending_msg:
                st.warning(f"🔔 Pending terakhir: **{pending_msg}**")
            else:
                st.info("Tidak ada pendingan dari laporan terakhir.")
    with col_b:
        with st.container(border=True):
            st.markdown("#### 💌 Feedback Team Lead (Terakhir)")
            try:
                df_user_only = load_all_reports([nama_pelapor])
                if not df_user_only.empty and COL_FEEDBACK in df_user_only.columns:
                    df_with_feed = df_user_only[df_user_only[COL_FEEDBACK].astype(str).str.strip() != ""]
                    if not df_with_feed.empty:
                        last_feed = df_with_feed.iloc[-1]
                        st.info(f"{last_feed.get(COL_FEEDBACK, '-')}")
                    else:
                        st.caption("Belum ada feedback.")
                else:
                    st.caption("Belum ada feedback.")
            except Exception:
                st.caption("Belum ada feedback.")

    st.divider()

    # Daily input form (using form helps smoothness)
    with st.container(border=True):
        st.markdown("### 📝 Input Laporan Harian (Activity)")
        st.caption("Gunakan form ini untuk mencatat aktivitas harian. Data akan tersimpan ke Google Sheet dan bukti (opsional) ke Dropbox.")

        with st.form("form_laporan_harian", clear_on_submit=False):
            kategori_aktivitas = st.radio(
                "Jenis Aktivitas:",
                ["🚗 Sales (Kunjungan Lapangan)", "💻 Digital Marketing / Konten / Ads", "📞 Telesales / Follow Up", "🏢 Lainnya"],
                horizontal=True
            )

            is_kunjungan = kategori_aktivitas.startswith("🚗")

            c1, c2 = st.columns(2)
            with c1:
                today_now = datetime.now(tz=TZ_JKT).date()
                st.markdown(f"**Tanggal:** `{today_now.strftime('%d-%m-%Y')}`")

                sosmed_link = ""
                if "Digital Marketing" in kategori_aktivitas:
                    sosmed_link = st.text_input("Link Konten / Ads / Drive (Opsional)")

            with c2:
                if is_kunjungan:
                    lokasi_input = st.text_input("📍 Nama Klien / Lokasi Kunjungan (Wajib)")
                else:
                    lokasi_input = st.text_input("Jenis Tugas", value=kategori_aktivitas.split(" ")[1], disabled=True)

                fotos = st.file_uploader(
                    "Upload Bukti (Foto/Screenshot/Dokumen) - Opsional",
                    accept_multiple_files=True,
                    disabled=not KONEKSI_DROPBOX_BERHASIL
                )

            deskripsi_map = {}
            main_deskripsi = ""

            if fotos:
                st.info("📸 Tambahkan keterangan singkat untuk setiap file bukti (membantu tracking).")
                for i, f in enumerate(fotos):
                    with st.container(border=True):
                        col_img, col_desc = st.columns([1, 3])
                        with col_img:
                            if getattr(f, "type", "").startswith("image"):
                                st.image(f, width=140)
                            else:
                                st.markdown(f"📄 **{f.name}**")
                        with col_desc:
                            deskripsi_map[f.name] = st.text_area(
                                f"Ket. File: {f.name}",
                                height=70,
                                key=f"desc_{i}",
                                placeholder="Jelaskan aktivitas terkait file ini..."
                            )
            else:
                placeholder_text = "Jelaskan hasil kunjungan..." if is_kunjungan else "Jelaskan konten/ads/calls yang dikerjakan..."
                main_deskripsi = st.text_area("Deskripsi Aktivitas (Wajib)", placeholder=placeholder_text, height=120)

            st.divider()
            st.markdown("### 🏁 Kesimpulan Harian")
            st.caption("Ringkas agar progress besok lebih terarah.")

            col_ref_1, col_ref_2, col_ref_3 = st.columns(3)
            with col_ref_1:
                input_kesimpulan = st.text_area(
                    "💡 Kesimpulan / Apa yang dicapai hari ini?",
                    height=110,
                    placeholder="Contoh: Klien setuju, tapi minta diskon. / Konten sudah jadi 3 feeds."
                )
            with col_ref_2:
                input_kendala = st.text_area(
                    "🚧 Kendala / Masalah (Internal)?",
                    height=110,
                    placeholder="Contoh: Hujan deras jadi telat. / Laptop agak lemot render video."
                )
            with col_ref_3:
                input_kendala_klien = st.text_area(
                    "🧑‍💼 Kendala dari Klien?",
                    height=110,
                    placeholder="Contoh: Klien minta revisi berkali-kali / Budget dipotong / Minta tempo pembayaran."
                )

            input_interest = st.radio(
                "📈 Tingkat Interest (Presentase)",
                ["Under 50% (A)", "50-75% (B)", "75%-100%"],
                horizontal=True,
                key="interest_persen"
            )

            c_lead1, c_lead2 = st.columns(2)
            with c_lead1:
                input_nama_klien = st.text_input("👤 Nama Klien yang Dihubungi", placeholder="Contoh: Bu Susi / Pak Andi", key="nama_klien_input")
            with c_lead2:
                input_kontak_klien = st.text_input("📞 No HP/WA Klien", placeholder="Contoh: 08xxxxxxxxxx", key="kontak_klien_input")

            input_pending = st.text_input(
                "📌 Next Plan / Pending Item (Akan jadi Reminder Besok)",
                placeholder="Contoh: Follow up Bu Susi jam 10 pagi. / Revisi desain banner."
            )

            submitted = st.form_submit_button("✅ Submit Laporan", type="primary", use_container_width=True)

        if submitted:
            valid = True

            if is_kunjungan and not str(lokasi_input).strip():
                st.error("Untuk Sales (Kunjungan), Lokasi Wajib Diisi!")
                valid = False

            if (not fotos) and (not str(main_deskripsi).strip()):
                st.error("Deskripsi Wajib Diisi!")
                valid = False

            if valid:
                with st.spinner("Menyimpan laporan..."):
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
                            rows.append([
                                ts, nama_pelapor, final_lokasi, desc,
                                url, sosmed_link if sosmed_link else "-",
                                val_kesimpulan, val_kendala, val_kendala_klien,
                                val_pending,
                                val_feedback, val_interest,
                                val_nama_klien, val_kontak_klien
                            ])
                    else:
                        rows.append([
                            ts, nama_pelapor, final_lokasi, main_deskripsi,
                            "-", sosmed_link if sosmed_link else "-",
                            val_kesimpulan, val_kendala, val_kendala_klien,
                            val_pending,
                            val_feedback, val_interest,
                            val_nama_klien, val_kontak_klien
                        ])

                    if simpan_laporan_harian_batch(rows, nama_pelapor):
                        st.success(f"✅ Laporan tersimpan! Reminder besok: **{val_pending}**")
                        ui_toast("Laporan tersimpan!", icon="✅")
                        st.cache_data.clear()
                    else:
                        st.error("Gagal simpan.")

    # Raw log
    with st.container(border=True):
        st.markdown("### 📂 Log Data Mentah")
        c1, c2 = st.columns([1, 3])
        with c1:
            if st.button("🔄 Refresh Data", use_container_width=True):
                st.cache_data.clear()
                st.rerun()
        with c2:
            st.caption("Tip: gunakan filter browser (Ctrl+F) atau download di Dashboard Admin untuk analisa lebih lanjut.")

        df_log = load_all_reports(get_daftar_staf_terbaru())
        if not df_log.empty:
            st.dataframe(df_log, use_container_width=True, hide_index=True)
        else:
            st.info("Kosong")


# =========================================================
# MENU: TARGET & KPI
# =========================================================
elif menu_nav == "🎯 Target & KPI":
    st.markdown("## 🎯 Checklist Target (Result KPI)")
    st.caption("Kelola target Team dan Individu. Update Status + Bukti/Catatan akan tercatat di kolom audit.")

    tab_team, tab_individu, tab_admin = st.tabs(["🏆 Team", "⚡ Individu", "⚙️ Admin Setup"])

    with tab_team:
        with st.container(border=True):
            st.markdown("### 🏆 Target Team")
            df_team = load_checklist(SHEET_TARGET_TEAM, TEAM_CHECKLIST_COLUMNS)

            if not df_team.empty:
                done = len(df_team[df_team["Status"] == True])
                progress_val = (done / len(df_team)) if len(df_team) > 0 else 0
                st.progress(progress_val)
                st.caption(f"Pencapaian: {done}/{len(df_team)}")

                edited_team = render_hybrid_table(df_team, "team_table", "Misi")

                csave, cexp = st.columns([1, 2])
                with csave:
                    if st.button("💾 Simpan Perubahan Team", use_container_width=True):
                        actor = get_actor_fallback(default="Admin")
                        df_to_save = apply_audit_checklist_changes(df_team, edited_team, key_cols=["Misi"], actor=actor)
                        if save_checklist(SHEET_TARGET_TEAM, df_to_save, TEAM_CHECKLIST_COLUMNS):
                            ui_toast("Tersimpan!", icon="✅")
                            st.cache_data.clear()
                            st.rerun()
                with cexp:
                    st.caption("Anda bisa update bukti file via panel 'Update Bukti (Team)' di bawah.")
            else:
                st.info("Belum ada target team.")

        with st.container(border=True):
            st.markdown("### ➕ Bulk Input Target Team")
            with st.form("add_team_goal", clear_on_submit=True):
                goal_team_text = st.text_area("Target Team (Satu per baris)", height=100)
                c1, c2 = st.columns(2)
                today_ = datetime.now(tz=TZ_JKT).date()
                start_d = c1.date_input("Mulai", value=today_, key="start_team")
                end_d = c2.date_input("Selesai", value=today_ + timedelta(days=30), key="end_team")
                if st.form_submit_button("➕ Tambah", use_container_width=True):
                    targets = clean_bulk_input(goal_team_text)
                    if targets:
                        if add_bulk_targets(SHEET_TARGET_TEAM, ["", str(start_d), str(end_d), "FALSE", "-"], targets):
                            st.success(f"{len(targets)} target ditambahkan!")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("Gagal.")

        with st.container(border=True):
            st.markdown("### 📂 Update Bukti (Team)")
            df_team = load_checklist(SHEET_TARGET_TEAM, TEAM_CHECKLIST_COLUMNS)
            if df_team.empty:
                st.info("Belum ada target team.")
            else:
                pilih_misi = st.selectbox("Misi:", df_team["Misi"].tolist())
                note_misi = st.text_area("Catatan (Opsional)")
                file_misi = st.file_uploader("Bukti", key="up_team", disabled=not KONEKSI_DROPBOX_BERHASIL)
                if st.button("Update Team", use_container_width=True):
                    pelapor = get_actor_fallback(default="Admin")
                    sukses, msg = update_evidence_row(SHEET_TARGET_TEAM, pilih_misi, note_misi, file_misi, pelapor, "Target_Team")
                    if sukses:
                        st.success("Updated!")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(msg)

    with tab_individu:
        with st.container(border=True):
            st.markdown("### ⚡ Target Individu")
            df_indiv_all = load_checklist(SHEET_TARGET_INDIVIDU, INDIV_CHECKLIST_COLUMNS)
            staff = get_daftar_staf_terbaru()
            filter_nama = st.selectbox("Filter Nama:", staff, index=0)

            if not df_indiv_all.empty:
                df_user = df_indiv_all[df_indiv_all["Nama"] == filter_nama]
                if not df_user.empty:
                    done = len(df_user[df_user["Status"] == True])
                    progress_val = (done / len(df_user)) if len(df_user) > 0 else 0
                    st.progress(progress_val)
                    st.caption(f"Progress: {done}/{len(df_user)}")
                    edited_indiv = render_hybrid_table(df_user, f"indiv_{filter_nama}", "Target")

                    if st.button(f"💾 Simpan {filter_nama}", use_container_width=True):
                        df_all_upd = df_indiv_all.copy()
                        df_all_upd.update(edited_indiv)

                        actor = filter_nama
                        df_all_upd = apply_audit_checklist_changes(df_indiv_all, df_all_upd, key_cols=["Nama", "Target"], actor=actor)

                        if save_checklist(SHEET_TARGET_INDIVIDU, df_all_upd, INDIV_CHECKLIST_COLUMNS):
                            ui_toast("Tersimpan!", icon="✅")
                            st.cache_data.clear()
                            st.rerun()

                    with st.expander(f"📂 Update Bukti ({filter_nama})", expanded=False):
                        pilih_target = st.selectbox("Target:", df_user["Target"].tolist())
                        note_target = st.text_area("Catatan", key="note_indiv")
                        file_target = st.file_uploader("Bukti", key="up_indiv", disabled=not KONEKSI_DROPBOX_BERHASIL)
                        if st.button("Update Pribadi", use_container_width=True):
                            sukses, msg = update_evidence_row(SHEET_TARGET_INDIVIDU, pilih_target, note_target, file_target, filter_nama, "Target_Individu")
                            if sukses:
                                st.success("Updated!")
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error(msg)
                else:
                    st.info("Belum ada target untuk user ini.")
            else:
                st.info("Data kosong.")

        with st.container(border=True):
            st.markdown("### ➕ Bulk Input Target Pribadi")
            with st.form("add_indiv_goal", clear_on_submit=True):
                goal_indiv_text = st.text_area("Target Mingguan (Satu per baris)", height=100)
                c1, c2 = st.columns(2)
                today_ = datetime.now(tz=TZ_JKT).date()
                start_i = c1.date_input("Mulai", value=today_, key="start_indiv")
                end_i = c2.date_input("Selesai", value=today_ + timedelta(days=7), key="end_indiv")
                if st.form_submit_button("➕ Tambah", use_container_width=True):
                    targets = clean_bulk_input(goal_indiv_text)
                    if targets:
                        if add_bulk_targets(SHEET_TARGET_INDIVIDU, [filter_nama, "", str(start_i), str(end_i), "FALSE", "-"], targets):
                            st.success(f"{len(targets)} target ditambahkan!")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("Gagal.")

    with tab_admin:
        # Staff management
        with st.container(border=True):
            st.markdown("### ➕ Tambah Karyawan")
            with st.form("add_staff", clear_on_submit=True):
                new_name = st.text_input("Nama")
                new_role = st.text_input("Jabatan")
                if st.form_submit_button("Tambah", use_container_width=True):
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

        with st.container(border=True):
            st.markdown("### 👥 Tambah Team (Config)")
            with st.form("add_team_admin", clear_on_submit=True):
                team_name = st.text_input("Nama Team", placeholder="Contoh: Team Sales A")
                team_posisi = st.text_input("Posisi/Divisi", placeholder="Contoh: Sales Lapangan / Digital Marketing")
                anggota_text = st.text_area("Nama Anggota (satu per baris)", height=120, placeholder="Contoh:\\nAndi\\nBudi\\nSusi")
                if st.form_submit_button("Tambah Team", use_container_width=True):
                    anggota_list = clean_bulk_input(anggota_text)
                    res, msg = tambah_team_baru(team_name, team_posisi, anggota_list)
                    if res:
                        st.success(msg)
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(msg)

        with st.container(border=True):
            st.markdown("### 📋 Daftar Team")
            df_team_cfg = load_team_config()
            if not df_team_cfg.empty:
                st.dataframe(df_team_cfg, use_container_width=True, hide_index=True)
            else:
                st.info("Belum ada team yang tersimpan.")


# =========================================================
# MENU: CLOSING DEAL
# =========================================================
elif menu_nav == "🤝 Closing Deal":
    st.markdown("## 🤝 Closing Deal")
    st.caption("Catat closing deal dan export data dalam format Excel/CSV.")

    with st.container(border=True):
        st.markdown("### ➕ Input Closing Deal")
        with st.form("form_closing_deal", clear_on_submit=True):
            cd_group = st.text_input("Nama Group (Opsional)", placeholder="Kosongkan jika tidak ada")
            cd_marketing = st.text_input("Nama Marketing", placeholder="Contoh: Andi")
            cd_tgl = st.date_input("Tanggal Event", value=datetime.now(tz=TZ_JKT).date(), key="closing_event_date")
            cd_bidang = st.text_input("Bidang (Manual)", placeholder="Contoh: F&B / Properti / Pendidikan")
            cd_nilai = st.text_input(
                "Nilai Kontrak (Input bebas)",
                placeholder="Contoh: 15000000 / 15.000.000 / Rp 15.000.000 / 15jt / 15,5jt"
            )

            if st.form_submit_button("✅ Simpan Closing Deal", use_container_width=True):
                res, msg = tambah_closing_deal(cd_group, cd_marketing, cd_tgl, cd_bidang, cd_nilai)
                if res:
                    st.success(msg)
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(msg)

    with st.container(border=True):
        st.markdown("### 📋 Data Closing Deal")
        df_cd = load_closing_deal()

        if not df_cd.empty:
            # Quick summary
            total_kontrak = int(df_cd[COL_NILAI_KONTRAK].fillna(0).sum()) if COL_NILAI_KONTRAK in df_cd.columns else 0
            c1, c2, c3 = st.columns(3)
            c1.metric("Jumlah Deal", int(len(df_cd)))
            c2.metric("Total Nilai Kontrak", format_rupiah_display(total_kontrak))
            c3.metric("Marketing Aktif", int(df_cd[COL_MARKETING].nunique()) if COL_MARKETING in df_cd.columns else 0)

            df_cd_display = df_cd.copy()
            df_cd_display[COL_NILAI_KONTRAK] = df_cd_display[COL_NILAI_KONTRAK].apply(
                lambda x: "" if pd.isna(x) else format_rupiah_display(x)
            )
            st.dataframe(df_cd_display, use_container_width=True, hide_index=True)

            cexp1, cexp2 = st.columns(2)

            with cexp1:
                if HAS_OPENPYXL:
                    col_widths = {
                        COL_GROUP: 25,
                        COL_MARKETING: 20,
                        COL_TGL_EVENT: 16,
                        COL_BIDANG: 25,
                        COL_NILAI_KONTRAK: 18
                    }

                    df_export = df_cd.copy()
                    df_export[COL_NILAI_KONTRAK] = df_export[COL_NILAI_KONTRAK].apply(
                        lambda x: None if pd.isna(x) else int(x)
                    )

                    excel_bytes = df_to_excel_bytes(
                        df_export,
                        sheet_name="Closing_Deal",
                        col_widths=col_widths,
                        wrap_cols=[COL_GROUP, COL_BIDANG],
                        right_align_cols=[COL_NILAI_KONTRAK],
                        number_format_cols={COL_NILAI_KONTRAK: '"Rp" #,##0'}
                    )
                    st.download_button(
                        "⬇️ Download Excel (Rapi + Rupiah)",
                        data=excel_bytes,
                        file_name="closing_deal.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                else:
                    st.warning("openpyxl belum tersedia. Download Excel dinonaktifkan.")

            with cexp2:
                csv_cd = df_cd.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "⬇️ Download CSV",
                    data=csv_cd,
                    file_name="closing_deal.csv",
                    mime="text/csv",
                    use_container_width=True
                )
        else:
            st.info("Belum ada data closing deal.")


# =========================================================
# MENU: PEMBAYARAN
# =========================================================
elif menu_nav == "💳 Pembayaran":
    st.markdown("## 💳 Pembayaran (DP / Termin / Pelunasan)")
    st.caption("Input pembayaran, monitoring jatuh tempo, dan audit log otomatis.")

    # Input pembayaran
    with st.container(border=True):
        st.markdown("### ➕ Input Pembayaran")

        p_group = st.text_input("Nama Group (Opsional)", placeholder="Kosongkan jika tidak ada", key="pay_group")
        p_marketing = st.text_input("Nama Marketing (Wajib)", placeholder="Contoh: Andi", key="pay_marketing")
        p_tgl_event = st.date_input(
            "Tanggal Event (Opsional)",
            value=st.session_state.get("pay_event_date", datetime.now(tz=TZ_JKT).date()),
            key="pay_event_date"
        )

        p_jenis_opt = st.selectbox(
            "Jenis Pembayaran",
            ["Down Payment (DP)", "Termin", "Pelunasan", "Lainnya"],
            key="pay_jenis_opt"
        )

        p_jenis_custom = ""
        if p_jenis_opt == "Lainnya":
            p_jenis_custom = st.text_input(
                "Tulis Jenis Pembayaran (Custom) *wajib*",
                placeholder="Contoh: Cicilan 1 / Cicilan 2 / Fee Admin / Refund / dll",
                key="pay_jenis_custom"
            )

        p_jenis_final = p_jenis_opt if p_jenis_opt != "Lainnya" else (p_jenis_custom or "").strip()

        p_nominal = st.text_input(
            "Nominal Pembayaran (Input bebas)",
            placeholder="Contoh: 5000000 / 5jt / Rp 5.000.000 / 5,5jt",
            key="pay_nominal",
            on_change=on_change_pay_nominal
        )

        nom_preview = parse_rupiah_to_int(p_nominal)
        st.caption(f"Preview nominal: **{format_rupiah_display(nom_preview) if nom_preview is not None else '-'}**")

        p_jatuh_tempo = st.date_input(
            "Batas Waktu Bayar (Jatuh Tempo)",
            value=st.session_state.get("pay_due_date", datetime.now(tz=TZ_JKT).date() + timedelta(days=7)),
            key="pay_due_date"
        )

        p_status = st.checkbox("✅ Sudah Dibayar?", value=bool(st.session_state.get("pay_status", False)), key="pay_status")

        p_catatan = st.text_area(
            "Catatan (Opsional)",
            height=90,
            placeholder="Contoh: DP untuk booking tanggal event...",
            key="pay_note"
        )

        p_bukti = st.file_uploader(
            "Upload Bukti Pembayaran (Foto/Screenshot/PDF)",
            key="pay_file",
            disabled=not KONEKSI_DROPBOX_BERHASIL
        )

        if st.button("✅ Simpan Pembayaran", type="primary", use_container_width=True, key="btn_save_payment"):
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
                    catatan=p_catatan
                )

                if res:
                    st.success(msg)
                    reset_payment_form_state()
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(msg)

    # Data pembayaran + alert
    with st.container(border=True):
        st.markdown("### 📋 Data Pembayaran + Alert Jatuh Tempo")
        df_pay = load_pembayaran_dp()

        if df_pay.empty:
            st.info("Belum ada data pembayaran.")
        else:
            default_actor = get_actor_fallback(default="-")
            staff_opts = get_daftar_staf_terbaru()
            editor_cols = st.columns([2, 1])
            with editor_cols[0]:
                actor_select = st.selectbox(
                    "Nama Editor (untuk log perubahan)",
                    options=staff_opts,
                    index=staff_opts.index(default_actor) if default_actor in staff_opts else 0,
                    key="payment_editor_name"
                )
            with editor_cols[1]:
                actor_manual = st.text_input("Atau tulis manual", placeholder="Opsional", key="payment_editor_manual")

            actor_final = (actor_manual.strip() if safe_str(actor_manual, "").strip() else safe_str(actor_select, "-").strip()) or "-"

            overdue_df, due_soon_df = build_alert_pembayaran(df_pay, days_due_soon=3)

            # Alerts
            a1, a2 = st.columns(2)
            with a1:
                st.metric("⛔ Overdue", int(len(overdue_df)))
            with a2:
                st.metric("⚠️ Due ≤ 3 hari", int(len(due_soon_df)))

            if len(overdue_df) > 0:
                st.error(f"⛔ Overdue: {len(overdue_df)} pembayaran melewati jatuh tempo!")

            if len(due_soon_df) > 0:
                st.warning(f"⚠️ Jatuh tempo ≤ 3 hari: {len(due_soon_df)} pembayaran belum dibayar.")

            st.caption(
                "Kolom yang bisa diedit: **Jenis Pembayaran**, **Status Pembayaran**, "
                "**Jatuh Tempo**, **Catatan**. Semua perubahan otomatis tercatat pada log."
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
                key="editor_payments"
            )

            if st.button("💾 Simpan Perubahan Pembayaran", use_container_width=True):
                editable_cols_list = [COL_STATUS_BAYAR, COL_JATUH_TEMPO, COL_CATATAN_BAYAR, COL_JENIS_BAYAR]

                df_after = df_pay.copy().set_index(COL_TS_BAYAR, drop=False)
                ed = edited_pay_view.copy().set_index(COL_TS_BAYAR, drop=False)

                for c in editable_cols_list:
                    if c in ed.columns:
                        df_after.loc[ed.index, c] = ed[c]

                df_after = df_after.reset_index(drop=True)

                df_to_save = apply_audit_payments_changes(df_pay, df_after, actor=actor_final)
                if save_pembayaran_dp(df_to_save):
                    ui_toast("Tersimpan!", icon="✅")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error("Gagal menyimpan perubahan.")

            st.divider()
            with st.expander("📎 Update Bukti Pembayaran (data existing)", expanded=False):
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

                selected_idx = st.selectbox(
                    "Pilih record yang mau di-update buktinya:",
                    options=list(range(len(df_pay_reset))),
                    format_func=_label_payment_idx,
                    key="pay_select_update_bukti_idx"
                )

                file_new = st.file_uploader(
                    "Upload bukti baru:",
                    key="pay_file_update_bukti",
                    disabled=not KONEKSI_DROPBOX_BERHASIL
                )

                if st.button("⬆️ Update Bukti", use_container_width=True):
                    marketing_name = str(df_pay_reset.iloc[selected_idx].get(COL_MARKETING, "Unknown"))
                    ok, msg = update_bukti_pembayaran_by_index(
                        selected_idx,
                        file_new,
                        marketing_name,
                        actor=actor_final
                    )
                    if ok:
                        st.success(msg)
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(msg)


# =========================================================
# MENU: DASHBOARD ADMIN
# =========================================================
elif menu_nav == "📊 Dashboard Admin":
    if not st.session_state.get("is_admin", False):
        st.warning("Akses Dashboard Admin memerlukan login admin.")
        st.stop()

    st.markdown("## 📊 Dashboard Produktivitas")
    st.info("Dashboard ini memisahkan analisa antara Sales dan Marketing.")

    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    staff_list = get_daftar_staf_terbaru()
    df_log = load_all_reports(staff_list)

    if df_log.empty:
        st.info("Belum ada data laporan.")
        st.stop()

    # Parse time
    try:
        df_log[COL_TIMESTAMP] = pd.to_datetime(df_log[COL_TIMESTAMP], format="%d-%m-%Y %H:%M:%S", errors="coerce")
        df_log["Tanggal"] = df_log[COL_TIMESTAMP].dt.date
    except Exception:
        df_log["Tanggal"] = datetime.now(tz=TZ_JKT).date()

    keywords_digital = ["Digital", "Marketing", "Konten", "Ads", "Telesales", "Admin", "Follow"]

    def get_category(val):
        val_str = str(val)
        if any(k in val_str for k in keywords_digital):
            return "Digital/Internal"
        return "Kunjungan Lapangan"

    df_log["Kategori"] = df_log[COL_TEMPAT].apply(get_category)

    # Filters
    with st.container(border=True):
        st.markdown("### 🔎 Filter")
        c1, c2, c3 = st.columns([1, 1, 2])
        days = c1.selectbox("Rentang (hari):", [7, 14, 30, 60], index=0)
        start_date = date.today() - timedelta(days=days)
        df_filt = df_log[df_log["Tanggal"] >= start_date].copy()
        c2.metric("Total Aktivitas", int(len(df_filt)))
        c3.metric("Staf Aktif", int(df_filt[COL_NAMA].nunique()) if COL_NAMA in df_filt.columns else 0)

    tab_sales, tab_marketing, tab_review, tab_galeri = st.tabs(
        ["🚗 Sales (Lapangan)", "💻 Marketing (Digital)", "📝 Review & Feedback", "🖼️ Galeri Bukti"]
    )

    with tab_sales:
        with st.container(border=True):
            df_sales = df_filt[df_filt["Kategori"] == "Kunjungan Lapangan"]
            c1, c2 = st.columns(2)
            c1.metric("Total Kunjungan", int(len(df_sales)))
            c2.metric("Sales Aktif", int(df_sales[COL_NAMA].nunique()) if COL_NAMA in df_sales.columns else 0)
            if not df_sales.empty:
                st.markdown("#### Top Visiting Sales")
                st.bar_chart(df_sales[COL_NAMA].value_counts())
                st.markdown("#### Lokasi Paling Sering Dikunjungi")
                st.dataframe(df_sales[COL_TEMPAT].value_counts().head(10), use_container_width=True)
            else:
                st.info("Tidak ada data kunjungan lapangan.")

    with tab_marketing:
        with st.container(border=True):
            df_mkt = df_filt[df_filt["Kategori"] == "Digital/Internal"]
            c1, c2 = st.columns(2)
            c1.metric("Total Output", int(len(df_mkt)))
            c2.metric("Marketer Aktif", int(df_mkt[COL_NAMA].nunique()) if COL_NAMA in df_mkt.columns else 0)
            if not df_mkt.empty:
                st.markdown("#### Produktivitas Tim Digital")
                if HAS_PLOTLY:
                    fig = px.pie(df_mkt, names=COL_NAMA, title="Distribusi Beban Kerja Digital")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.bar_chart(df_mkt[COL_NAMA].value_counts())
                st.markdown("#### Jenis Tugas Digital")
                st.bar_chart(df_mkt[COL_TEMPAT].value_counts())
            else:
                st.info("Tidak ada data aktivitas digital.")

    with tab_review:
        with st.container(border=True):
            st.markdown("### 📝 Review Catatan Harian & Feedback")
            st.caption("Monitoring kendala dan memberikan feedback langsung per individu.")

            # Limit cards for smoothness
            max_cards = st.slider("Jumlah kartu yang ditampilkan", min_value=10, max_value=200, value=50, step=10)

            # Interest export section
            with st.expander("📇 Tarik Data Nama & No HP per Tingkat Interest", expanded=True):
                if COL_INTEREST not in df_filt.columns:
                    st.warning("Kolom Interest (%) belum ada di data.")
                else:
                    if "filter_interest_admin" not in st.session_state:
                        st.session_state["filter_interest_admin"] = "Under 50% (A)"

                    b1, b2, b3 = st.columns(3)
                    if b1.button("Tarik Under 50% (A)", use_container_width=True):
                        st.session_state["filter_interest_admin"] = "Under 50% (A)"
                    if b2.button("Tarik 50-75% (B)", use_container_width=True):
                        st.session_state["filter_interest_admin"] = "50-75% (B)"
                    if b3.button("Tarik 75%-100%", use_container_width=True):
                        st.session_state["filter_interest_admin"] = "75%-100%"

                    selected_interest = st.session_state["filter_interest_admin"]
                    st.info(f"Filter aktif: **{selected_interest}**")

                    df_tmp = df_filt.copy()
                    for c in [COL_NAMA_KLIEN, COL_KONTAK_KLIEN, COL_INTEREST, COL_KENDALA_KLIEN]:
                        if c not in df_tmp.columns:
                            df_tmp[c] = ""

                    df_tmp[COL_INTEREST] = df_tmp[COL_INTEREST].astype(str).fillna("").str.strip()
                    df_interest = df_tmp[df_tmp[COL_INTEREST] == selected_interest].copy()

                    cols_out = []
                    for c in [COL_TIMESTAMP, COL_NAMA, COL_NAMA_KLIEN, COL_KONTAK_KLIEN, COL_INTEREST, COL_TEMPAT, COL_DESKRIPSI, COL_KENDALA_KLIEN]:
                        if c in df_interest.columns:
                            cols_out.append(c)

                    df_out = df_interest[cols_out].copy() if cols_out else df_interest.copy()
                    st.dataframe(df_out, use_container_width=True, hide_index=True)

                    if HAS_OPENPYXL:
                        df_export = df_out.copy()
                        if COL_TIMESTAMP in df_export.columns and pd.api.types.is_datetime64_any_dtype(df_export[COL_TIMESTAMP]):
                            df_export[COL_TIMESTAMP] = df_export[COL_TIMESTAMP].dt.strftime("%d-%m-%Y %H:%M:%S")

                        excel_bytes = df_to_excel_bytes(
                            df_export,
                            sheet_name="Data_Interest",
                            wrap_cols=[COL_DESKRIPSI, COL_TEMPAT, COL_KENDALA_KLIEN],
                        )
                        safe_name = selected_interest.replace("%", "").replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")
                        st.download_button(
                            "⬇️ Download Excel (sesuai filter)",
                            data=excel_bytes,
                            file_name=f"data_klien_{safe_name}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
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
                        use_container_width=True
                    )

            # Review cards
            df_review = df_filt.sort_values(by=COL_TIMESTAMP, ascending=False).head(max_cards)

            if df_review.empty:
                st.info("Belum ada data laporan pada rentang waktu ini.")
            else:
                for _, row in df_review.iterrows():
                    with st.container(border=True):
                        c_head1, c_head2 = st.columns([3, 1])
                        with c_head1:
                            st.markdown(f"### 👤 {row.get(COL_NAMA, '-')}")
                            tsv = row.get(COL_TIMESTAMP, "-")
                            tss = tsv.strftime("%d-%m-%Y %H:%M:%S") if hasattr(tsv, "strftime") else str(tsv)
                            st.caption(f"📅 {tss} | 🏷️ {row.get('Kategori', '-')}")

                        with c_head2:
                            st.markdown("")

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
                                st.info(f"💡 **Hasil/Kesimpulan:**\\n\\n{row.get(COL_KESIMPULAN, '-')}")
                            with col_b:
                                st.warning(f"🚧 **Kendala (Internal):**\\n\\n{row.get(COL_KENDALA, '-')}")
                            with col_c:
                                st.warning(f"🧑‍💼 **Kendala Klien:**\\n\\n{row.get(COL_KENDALA_KLIEN, '-')}")
                            with col_d:
                                st.error(f"📌 **Next Plan:**\\n\\n{row.get(COL_PENDING, '-')}")

                            st.divider()
                            existing_feed = row.get(COL_FEEDBACK, "") or ""

                            with st.expander("💬 Beri Feedback", expanded=False):
                                unique_key = f"feed_{row.get(COL_NAMA, '-')}_{tss}"
                                input_feed = st.text_area("Tulis Masukan/Arahan:", value=str(existing_feed), key=unique_key)

                                if st.button("Kirim Feedback 🚀", key=f"btn_{unique_key}", use_container_width=True):
                                    if input_feed:
                                        res, msg = kirim_feedback_admin(row.get(COL_NAMA, ""), tss, input_feed)
                                        if res:
                                            ui_toast("Feedback terkirim!", icon="✅")
                                        else:
                                            st.error(msg)

                        with c_img:
                            link_foto = str(row.get(COL_LINK_FOTO, ""))
                            if "http" in link_foto:
                                url_asli = link_foto
                                direct_url = url_asli.replace("www.dropbox.com", "dl.dropboxusercontent.com").replace("?dl=0", "")
                                try:
                                    st.image(direct_url, use_container_width=True)
                                    st.caption("Bukti")
                                except Exception:
                                    st.caption("Gagal load bukti")
                            else:
                                st.caption("Tidak ada bukti")

    with tab_galeri:
        with st.container(border=True):
            st.caption("Menampilkan bukti foto/dokumen terbaru")
            if COL_LINK_FOTO in df_filt.columns:
                df_foto = df_filt[
                    df_filt[COL_LINK_FOTO].astype(str).str.contains("http", na=False, case=False)
                ].sort_values(by=COL_TIMESTAMP, ascending=False).head(12)
            else:
                df_foto = pd.DataFrame()

            if not df_foto.empty:
                data_dict = df_foto.to_dict("records")
                cols = st.columns(4)
                for idx, row in enumerate(data_dict):
                    with cols[idx % 4]:
                        with st.container(border=True):
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
                                st.markdown(f"[Buka Link]({url_asli})")
            else:
                st.info("Belum ada bukti yang terupload.")
