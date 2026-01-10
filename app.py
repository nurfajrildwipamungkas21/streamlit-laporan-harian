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

from audit_service import log_admin_action, compare_and_get_changes

import json

# --- HELPER: APPROVAL SYSTEM ---
# Nama sheet untuk menampung request perubahan yang menunggu persetujuan
SHEET_PENDING = "System_Pending_Approval"

def init_pending_db():
    """Memastikan sheet pending approval ada dengan kolom untuk DATA LAMA."""
    try:
        try:
            ws = spreadsheet.worksheet(SHEET_PENDING)
            # Cek apakah header sudah update (punya Old Data JSON)
            headers = ws.row_values(1)
            if "Old Data JSON" not in headers:
                # PERBAIKAN: Resize sheet dulu sebelum update cell di kolom baru
                current_cols = ws.col_count
                new_col_idx = len(headers) + 1
                if current_cols < new_col_idx:
                    ws.resize(cols=new_col_idx) # Tambah kolom jika kurang
                
                ws.update_cell(1, new_col_idx, "Old Data JSON")
        except gspread.WorksheetNotFound:
            # Header Baru: Tambah "Old Data JSON"
            # Pastikan cols=7 agar cukup menampung kolom baru
            ws = spreadsheet.add_worksheet(title=SHEET_PENDING, rows=1000, cols=7)
            headers = ["Timestamp", "Requestor", "Target Sheet", "Row Index (0-based)", "New Data JSON", "Reason", "Old Data JSON"]
            ws.append_row(headers, value_input_option="USER_ENTERED")
            maybe_auto_format_sheet(ws, force=True)
        return ws
    except Exception as e:
        # Tampilkan error di terminal untuk debugging jika terjadi lagi
        print(f"Error init_pending_db: {e}") 
        return None

def submit_change_request(target_sheet, row_idx_0based, new_df_row, old_df_row, reason, requestor):
    """
    UPDATE: Sekarang menerima old_df_row untuk perbandingan.
    """
    ws = init_pending_db()
    if not ws: return False, "DB Error"
    
    # Konversi row dataframe ke dictionary -> JSON string
    row_dict_new = new_df_row.astype(str).to_dict()
    json_new = json.dumps(row_dict_new)

    # Proses Data Lama
    row_dict_old = old_df_row.astype(str).to_dict() if old_df_row is not None else {}
    json_old = json.dumps(row_dict_old)
    
    ts = now_ts_str()
    
    # Simpan request lengkap
    ws.append_row(
        [ts, requestor, target_sheet, row_idx_0based, json_new, reason, json_old],
        value_input_option="USER_ENTERED"
    )
    return True, "Permintaan perubahan terkirim ke Manager!"

def get_pending_approvals():
    """Fungsi untuk Manager mengambil semua daftar request yang pending."""
    ws = init_pending_db()
    if not ws: return []
    return ws.get_all_records()

import json

def execute_approval(request_index_0based, action, admin_name="Manager", rejection_note="-"):
    """
    Fungsi Eksekusi Approval oleh Manager.
    Menggabungkan logika update data dan pencatatan Audit Log (Approve/Reject).
    """
    try:
        # Inisialisasi koneksi ke sheet pending (pastikan fungsi init_pending_db tersedia di scope global)
        ws_pending = init_pending_db()
        
        # Ambil data terbaru dari sheet pending
        all_requests = ws_pending.get_all_records()
        
        if request_index_0based >= len(all_requests):
            return False, "Data tidak ditemukan (mungkin sudah diproses)."
            
        req = all_requests[request_index_0based]
        target_sheet_name = req["Target Sheet"]
        row_target_idx = int(req["Row Index (0-based)"])
        
        # --- PERSIAPAN LOGGING (Diff Checker) ---
        # Ambil detail perubahan untuk Log (Membandingkan Old Data vs New Data)
        old_data_str = req.get("Old Data JSON", "{}")
        new_data_str = req.get("New Data JSON", "{}")
        
        # Buat String Diff Sederhana untuk Audit Log
        diff_str = ""
        try:
            old_d = json.loads(old_data_str)
            new_d = json.loads(new_data_str)
            diff_list = []
            for k, v in new_d.items():
                old_v = old_d.get(k, "")
                # Bandingkan nilai lama dan baru
                if str(old_v) != str(v):
                    diff_list.append(f"{k}: {old_v} -> {v}")
            diff_str = "\n".join(diff_list)
        except Exception as e_json:
            diff_str = f"Data parsing error: {e_json}"

        # --- ACTION: REJECT ---
        if action == "REJECT":
            # 1. Catat ke Audit Log bahwa Manager MENOLAK
            # (Pastikan fungsi log_admin_action tersedia di scope global/main script)
            log_admin_action(
                spreadsheet=spreadsheet,
                actor=admin_name,
                role="Manager",
                feature="Approval System",
                target_sheet=target_sheet_name,
                row_idx=row_target_idx + 2,
                action="REJECTED", 
                reason=f"Ditolak Manager. Alasan: {rejection_note}",
                changes_dict={"Diff": diff_str}
            )
            
            # 2. Hapus request dari sheet pending
            # Row index di GSheet = index list + 2 (header di baris 1)
            ws_pending.delete_rows(request_index_0based + 2)
            
            return True, "Permintaan ditolak. Alasan telah dicatat di Log."
            
        # --- ACTION: APPROVE ---
        elif action == "APPROVE":
            # 1. Parsing Data Baru
            new_data_dict = json.loads(req["New Data JSON"])
            
            # 2. Update Sheet Target Asli
            ws_target = spreadsheet.worksheet(target_sheet_name)
            
            # Susun data sesuai urutan header di sheet target
            headers = ws_target.row_values(1)
            row_values = []
            for h in headers:
                # Ambil nilai dari JSON berdasarkan nama kolom header
                val = new_data_dict.get(h, "") 
                row_values.append(val)
                
            # Update Cell di Sheet Target
            # Baris GSheet = index 0-based + 2
            gsheet_row = row_target_idx + 2
            cell_range = f"A{gsheet_row}"
            
            # Lakukan update ke Google Sheet
            ws_target.update(range_name=cell_range, values=[row_values], value_input_option="USER_ENTERED")
            
            # 3. Catat ke Audit Log bahwa Manager MENYETUJUI
            log_admin_action(
                spreadsheet=spreadsheet,
                actor=admin_name,
                role="Manager",
                feature="Approval System",
                target_sheet=target_sheet_name,
                row_idx=gsheet_row,
                action="APPROVED",
                reason=req.get("Reason", "Approved by Manager"), 
                changes_dict={"Diff": diff_str}
            )

            # 4. Hapus Request dari Sheet Pending setelah berhasil
            ws_pending.delete_rows(request_index_0based + 2)
            
            return True, "Perubahan disetujui, diterapkan ke Database, dan dicatat di Log."
            
    except Exception as e:
        return False, f"Error: {e}"

# --- BAGIAN IMPORT OPTIONAL LIBS JANGAN DIHAPUS (Excel/AgGrid/Plotly) ---
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
# SYSTEM LOGIN OTP VIA EMAIL
# =========================================================

def send_email_otp(target_email, otp_code):
    """Mengirim kode OTP ke email target menggunakan SMTP Gmail"""
    smtp_config = st.secrets["smtp"]
    sender_email = smtp_config["sender_email"]
    sender_password = smtp_config["sender_password"]
    
    subject = "Kode Login - Sales Action Center"
    body = f"""
    <html>
      <body style="font-family: Arial, sans-serif;">
        <h2 style="color: #2e7d32;">Sales & Marketing Action Center</h2>
        <p>Halo,</p>
        <p>Gunakan kode berikut untuk masuk ke aplikasi:</p>
        <div style="background-color: #f1f8e9; padding: 15px; border-radius: 8px; display: inline-block;">
            <h1 style="color: #1b5e20; letter-spacing: 5px; margin: 0;">{otp_code}</h1>
        </div>
        <p>Kode ini berlaku untuk satu kali login. Jangan berikan kepada siapapun.</p>
        <hr>
        <small>Pesan otomatis dari Sistem Laporan Harian.</small>
      </body>
    </html>
    """

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = target_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))

    try:
        # Menggunakan SSL (Port 465)
        with smtplib.SMTP_SSL(smtp_config["smtp_server"], smtp_config["smtp_port"]) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, target_email, msg.as_string())
        return True
    except Exception as e:
        st.error(f"Gagal kirim email: {e}")
        return False

def generate_otp():
    return ''.join(random.choices(string.digits, k=6))

# =========================================================
# SYSTEM LOGIN (MODIFIED: Direct Staff Access)
# =========================================================

# =========================================================
# SYSTEM LOGIN (MODIFIED: Direct Staff Access & Role Check)
# =========================================================

def login_page():
    st.markdown("<br><br>", unsafe_allow_html=True)
    st.markdown("<h1 style='text-align: center;'>🔐 Access Portal</h1>", unsafe_allow_html=True)
    st.markdown(f"<p style='text-align: center;'>{APP_TITLE}</p>", unsafe_allow_html=True)
    st.divider()

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        # MEMBUAT TABS: Staff (Langsung) vs Admin (OTP)
        tab_staff, tab_admin = st.tabs(["🚀 Akses Staff", "🛡️ Login Admin"])
        
        # --- TAB 1: AKSES STAFF (LANGSUNG) ---
        with tab_staff:
            st.markdown("### 👋 Halo, Team!")
            st.info("Klik tombol di bawah untuk masuk dan mulai membuat laporan.")
            
            if st.button("Masuk Aplikasi (Staff)", type="primary", use_container_width=True):
                # SET SESSION STAFF (GENERIC)
                st.session_state["logged_in"] = True
                st.session_state["user_email"] = "staff_entry" 
                st.session_state["user_name"] = "Staff Member" # Nama spesifik nanti dipilih di dalam form
                st.session_state["user_role"] = "staff" 
                st.session_state["is_admin"] = False    # KUNCI: Staff tidak bisa akses dashboard admin
                
                st.success("Berhasil masuk! Mengalihkan...")
                time.sleep(0.5)
                st.rerun()

        # --- TAB 2: LOGIN ADMIN (EMAIL & OTP) ---
        with tab_admin:
            # Step 1: Input Email
            if st.session_state.get("otp_step", 1) == 1:
                with st.form("email_form"):
                    st.caption("Khusus Admin & Manager (via Email Terdaftar)")
                    email_input = st.text_input("Email Address")
                    
                    if st.form_submit_button("Kirim Kode OTP", use_container_width=True):
                        # Cek whitelist di secrets.toml
                        users_db = st.secrets.get("users", {})
                        # Normalisasi email (hilangkan spasi & lowercase)
                        email_clean = email_input.strip().lower()
                        
                        if email_clean in users_db:
                            otp = generate_otp()
                            # Kirim Email OTP
                            if send_email_otp(email_clean, otp):
                                st.session_state["generated_otp"] = otp
                                st.session_state["temp_email"] = email_clean
                                st.session_state["otp_step"] = 2
                                st.success("OTP Terkirim ke email!")
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("Gagal kirim email (Cek Config SMTP).")
                        else:
                            st.error("⛔ Akses Ditolak: Email tidak terdaftar sebagai Admin/Manager.")
            
            # Step 2: Input OTP
            elif st.session_state.get("otp_step") == 2:
                st.info(f"Kode dikirim ke: **{st.session_state['temp_email']}**")
                
                with st.form("otp_form"):
                    otp_input = st.text_input("Kode OTP (6 Digit)", max_chars=6)
                    c_back, c_ok = st.columns(2)
                    
                    # Tombol Kembali
                    if c_back.form_submit_button("⬅️ Ganti Email"):
                        st.session_state["otp_step"] = 1
                        st.rerun()
                        
                    # Tombol Verifikasi
                    if c_ok.form_submit_button("Verifikasi ✅", type="primary"):
                        if otp_input == st.session_state["generated_otp"]:
                            # LOGIN ADMIN/MANAGER SUKSES
                            email_fix = st.session_state["temp_email"]
                            user_info = st.secrets["users"][email_fix]
                            
                            st.session_state["logged_in"] = True
                            st.session_state["user_email"] = email_fix
                            st.session_state["user_name"] = user_info["name"]
                            
                            # --- DETEKSI ROLE ---
                            # Ambil role dari secrets.toml, default ke 'staff' jika tidak ada
                            role_str = str(user_info.get("role", "staff")).lower()
                            st.session_state["user_role"] = role_str
                            
                            # Tentukan Flag Admin: True jika role adalah 'admin' ATAU 'manager'
                            # Ini memberikan akses ke menu Dashboard Admin untuk kedua role tersebut
                            if role_str in ["admin", "manager"]:
                                st.session_state["is_admin"] = True
                            else:
                                st.session_state["is_admin"] = False
                            
                            st.success(f"Login Berhasil! Selamat datang {role_str.upper()}.")
                            time.sleep(0.5)
                            st.rerun()
                        else:
                            st.error("Kode OTP Salah.")
                        
# =========================================================
# HELPER: DATABASE STAFF (GOOGLE SHEET)
# =========================================================
SHEET_USERS = "Config_Users"

def init_user_db():
    """Memastikan sheet user ada."""
    try:
        try:
            ws = spreadsheet.worksheet(SHEET_USERS)
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=SHEET_USERS, rows=100, cols=4)
            ws.append_row(["Username", "Password", "Nama", "Role"], value_input_option="USER_ENTERED")
            maybe_auto_format_sheet(ws, force=True)
        return ws
    except Exception:
        return None

def check_staff_login(username, password):
    """Cek login untuk staff biasa via GSheet."""
    ws = init_user_db()
    if not ws: return None
    
    # Ambil semua data
    records = ws.get_all_records()
    for user in records:
        u_db = str(user.get("Username", "")).strip()
        p_db = str(user.get("Password", "")).strip()
        if u_db == username and p_db == password:
            return user 
    return None

def add_staff_account(username, password, nama):
    """Admin menambah akun staff."""
    ws = init_user_db()
    if not ws: return False, "DB Error"
    
    existing_users = ws.col_values(1)
    if username in existing_users:
        return False, "Username sudah dipakai!"
        
    ws.append_row([username, password, nama, "staff"], value_input_option="USER_ENTERED")
    return True, "Akun berhasil dibuat."

def update_staff_account(username_lama, new_password=None, new_name=None):
    """Fitur Edit Akun Staff (Ganti Password / Nama)."""
    ws = init_user_db()
    if not ws: return False, "DB Error"

    try:
        cell = ws.find(username_lama)
        row = cell.row
        if new_password and new_password.strip():
            ws.update_cell(row, 2, new_password) # Kolom 2 = Password
        if new_name and new_name.strip():
            ws.update_cell(row, 3, new_name) # Kolom 3 = Nama
        return True, f"Data user {username_lama} berhasil diperbarui."
    except gspread.exceptions.CellNotFound:
        return False, "Username tidak ditemukan."
    except Exception as e:
        return False, str(e)

def delete_staff_account(username):
    """Admin menghapus akun staff."""
    ws = init_user_db()
    if not ws: return False, "DB Error"
    
    try:
        cell = ws.find(username)
        ws.delete_rows(cell.row)
        return True, f"User {username} dihapus."
    except gspread.exceptions.CellNotFound:
        return False, "Username tidak ditemukan."

# =========================================================
# MAIN FLOW CHECK
# =========================================================
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False

if not st.session_state["logged_in"]:
    login_page()
    st.stop() # Berhenti disini jika belum login

# =========================================================
# USER INFO SETELAH LOGIN (Variabel Global)
# =========================================================
# Variabel ini akan dipakai di seluruh aplikasi
user_email = st.session_state["user_email"]
user_name = st.session_state["user_name"]
user_role = st.session_state["user_role"]

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

            --green:#16a34a;
            --green2:#22c55e;
            --teal:#14b8a6;
            --gold:#facc15;
            --amber:#f59e0b;
            --danger:#ef4444;

            /* Beri tahu browser bahwa UI ini dark theme */
            color-scheme: dark;
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

        /* Subtle starfield overlay (Space vibe) */
        .stApp::before {
            content: "";
            position: fixed;
            inset: 0;
            pointer-events: none;
            background:
                radial-gradient(rgba(255,255,255,0.18) 0.8px, transparent 0.8px);
            background-size: 68px 68px;
            opacity: 0.10;
            -webkit-mask-image: radial-gradient(circle at 50% 15%, rgba(0,0,0,1) 0%, rgba(0,0,0,0.0) 70%);
            mask-image: radial-gradient(circle at 50% 15%, rgba(0,0,0,1) 0%, rgba(0,0,0,0.0) 70%);
        }

        /* Hide Streamlit default UI chrome (we use custom header) */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}

        /* Typography */
        h1, h2, h3, h4, h5, h6, p, label, span, div {
            font-family: "Space Grotesk", ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, "Helvetica Neue", "Noto Sans", "Liberation Sans", sans-serif;
        }

        /* =========================
        Text selection (blok teks)
        ========================= */
        .stApp ::selection{
            color: #ffffff !important;
            background: rgba(22,163,74,0.35) !important;
        }
        .stApp ::-moz-selection{
            color: #ffffff !important;
            background: rgba(22,163,74,0.35) !important;
        }

        /* Sidebar polish (SpaceX-like) */
        section[data-testid="stSidebar"] > div {
            background: linear-gradient(180deg, rgba(0,0,0,0.92) 0%, rgba(3,10,6,0.92) 60%, rgba(4,16,11,0.92) 100%);
            border-right: 1px solid rgba(255,255,255,0.10);
        }
        section[data-testid="stSidebar"] * {
            color: var(--text) !important;
        }
        section[data-testid="stSidebar"] hr {
            border-color: rgba(255,255,255,0.10);
        }

        /* Card styling for containers with border=True */
        div[data-testid="stVerticalBlockBorderWrapper"] > div {
            background: linear-gradient(180deg, var(--cardA) 0%, var(--cardB) 100%);
            border: 1px solid var(--border);
            border-radius: 18px;
            padding: 1.05rem 1.05rem 0.75rem 1.05rem;
            box-shadow: 0 16px 46px rgba(0,0,0,0.42);
            backdrop-filter: blur(10px);
        }

        /* Buttons */
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

        /* Primary button (type=primary) */
        button[kind="primary"] {
            background: linear-gradient(135deg, rgba(22,163,74,0.95), rgba(245,158,11,0.92)) !important;
            color: rgba(6, 26, 17, 0.95) !important;
            border: none !important;
        }
        button[kind="primary"]:hover {
            filter: brightness(1.05);
        }

        /* Inputs */
        .stTextInput input, .stTextArea textarea, .stNumberInput input {
            border-radius: 12px !important;
        }
        .stDateInput input {
            border-radius: 12px !important;
        }
        .stSelectbox div[data-baseweb="select"] > div {
            border-radius: 12px !important;
        }

        /* Dataframes / tables */
        div[data-testid="stDataFrame"] {
            border-radius: 14px;
            overflow: hidden;
            border: 1px solid rgba(255,255,255,0.10);
        }

        /* =========================
           HERO HEADER (Custom)
           ========================= */
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
            content:"";
            position:absolute;
            inset:0;
            background-image: var(--hero-bg);
            background-repeat:no-repeat;
            background-position: var(--hero-bg-pos, 50% 72%);
            background-size: var(--hero-bg-size, 140%);
            opacity: 0.28;
            filter: saturate(1.05) contrast(1.08);
            pointer-events:none;
        }
        .sx-holding-logo{
            display:block;
            margin: 0 auto 10px auto;
            width: clamp(90px, 10vw, 140px);
            height: auto;
            opacity: 0.95;
            filter: drop-shadow(0 10px 22px rgba(0,0,0,0.45));
        }
        .sx-hero::after{
            content:"";
            position:absolute;
            inset:0;
            background:
                linear-gradient(180deg, rgba(2,8,5,0.15) 0%, rgba(2,8,5,0.52) 100%);
            pointer-events:none;
        }

        .sx-hero-grid{
            position: relative;
            display: grid;
            grid-template-columns: 240px 1fr 240px;
            align-items: center;
            gap: 14px;
        }

        .sx-hero-grid > * { min-width: 0; }

        @media (max-width: 1100px){
            .sx-hero-grid{ grid-template-columns: 200px 1fr 200px; }
        }
        @media (max-width: 860px){
            .sx-hero-grid{ grid-template-columns: 1fr; text-align:center; }
        }

        *, *::before, *::after { box-sizing: border-box; }

        .sx-logo-card{
            background: rgba(255,255,255,0.92);
            border: 1px solid rgba(0,0,0,0.06);
            border-radius: 16px;
            width: 100%;
            max-width: 240px;
            height: clamp(120px, 12vw, 160px);
            padding: 10px;
            display:flex;
            align-items:center;
            justify-content:center;
            box-shadow: 0 10px 26px rgba(0,0,0,0.28);
        }

        .sx-logo-card img{
            width: 100%;
            height: 100%;
            max-width: 220px;
            max-height: 100%;
            object-fit: contain;
            object-position: center;
            display: block;
        }

        .sx-hero-center{
            text-align: center;
        }
        .sx-title{
            font-size: 2.05rem;
            font-weight: 800;
            line-height: 1.12;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            margin: 0;
        }
        .sx-subrow{
            margin-top: 0.45rem;
            display:flex;
            gap: 0.55rem;
            flex-wrap: wrap;
            justify-content: center;
            align-items: center;
            color: rgba(255,255,255,0.78);
            font-size: 0.95rem;
        }
        .sx-pill{
            display:inline-flex;
            align-items:center;
            gap: 0.35rem;
            padding: 0.22rem 0.60rem;
            border-radius: 999px;
            border: 1px solid rgba(255,255,255,0.14);
            background: rgba(255,255,255,0.06);
            color: rgba(255,255,255,0.88);
            font-size: 0.80rem;
        }
        .sx-pill.on{
            border-color: rgba(34,197,94,0.55);
            box-shadow: 0 0 0 2px rgba(34,197,94,0.10) inset;
        }
        .sx-pill.off{
            border-color: rgba(239,68,68,0.55);
            box-shadow: 0 0 0 2px rgba(239,68,68,0.10) inset;
        }
        .sx-dot{
            width: 8px; height: 8px; border-radius: 999px; display:inline-block;
            background: rgba(255,255,255,0.55);
        }
        .sx-pill.on .sx-dot{ background: rgba(34,197,94,0.95); }
        .sx-pill.off .sx-dot{ background: rgba(239,68,68,0.95); }

        /* =========================
           Sidebar Nav (SpaceX-like)
           ========================= */
        .sx-nav{
            margin-top: 0.25rem;
        }
        .sx-nav button{
            width: 100% !important;
            text-align: left !important;
            border-radius: 12px !important;
            padding: 0.60rem 0.80rem !important;
            text-transform: uppercase !important;
            letter-spacing: 0.10em !important;
            font-size: 0.78rem !important;
        }
        .sx-nav button[kind="primary"]{
            background: linear-gradient(90deg, rgba(22,163,74,0.95), rgba(245,158,11,0.90)) !important;
            color: rgba(6,26,17,0.95) !important;
        }

        .sx-section-title{
            font-size: 0.82rem;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            color: rgba(255,255,255,0.70);
        }

        /* ==================================================
           MOBILE ONLY (<=768px) - tidak mengubah desktop
           ================================================== */
        @media (max-width: 768px){
          /* Sidebar disembunyikan di HP */
          section[data-testid="stSidebar"] { display: none !important; }

          /* Padding konten + ruang untuk bottom nav */
          .block-container {
            padding-left: 1rem !important;
            padding-right: 1rem !important;
            padding-bottom: 80px !important; /* biar konten tidak ketutup bottom nav */
          }

          /* Hero dibuat lebih ringkas */
          .sx-title { font-size: 1.35rem !important; }
          .sx-hero-grid { grid-template-columns: 1fr !important; }

          /* Logo kiri/kanan dimatikan di HP biar tidak makan tempat */
          .sx-logo-card { display:none !important; }

          .mobile-bottom-nav{
            position: fixed;
            left: 0; right: 0; bottom: 0;
            padding: 10px 12px;
            background: rgba(0,0,0,0.75);
            border-top: 1px solid rgba(255,255,255,0.12);
            display: flex;
            justify-content: space-around;
            gap: 8px;
            z-index: 9999;
            backdrop-filter: blur(10px);
          }
          .mobile-bottom-nav a{
            text-decoration:none;
            color: rgba(255,255,255,0.92);
            padding: 8px 10px;
            border-radius: 12px;
            border: 1px solid rgba(255,255,255,0.12);
            background: rgba(255,255,255,0.06);
            font-size: 14px;
          }

          /* Kurangi efek blur di HP (card form Closing Deal) */
          div[data-testid="stVerticalBlockBorderWrapper"] > div {
            backdrop-filter: none !important;
            background: linear-gradient(
                180deg,
                rgba(6, 36, 22, 0.96),
                rgba(5, 25, 17, 0.98)
            ) !important;
          }
        }

        /* =========================================
           PATCH KONTRAS TEKS & LOGO (MOBILE + DESKTOP)
           ========================================= */

        /* 1. Warna label & teks kecil di dalam form Closing Deal */
        div[data-testid="stForm"] label,
        div[data-testid="stForm"] p {
            color: rgba(255, 255, 255, 0.9) !important;
        }

        /* 2. Warna teks judul field di dalam kartu form (jaga-jaga) */
        div[data-testid="stVerticalBlockBorderWrapper"] label,
        div[data-testid="stVerticalBlockBorderWrapper"] p {
            color: rgba(255, 255, 255, 0.88) !important;
        }

        /* 3. Biar icon / logo tidak nyaru di navbar / header custom */
        .sx-nav button,
        .sx-nav svg,
        .sx-nav span {
            color: #ffffff !important;
            fill: #ffffff !important;
        }

        /* =========================================
           PATCH LANJUTAN – KONTRAS TEKS DI DALAM CARD
           (Riwayat Closing, dst)
           ========================================= */

        /* Semua teks di dalam card ber-border */
        div[data-testid="stVerticalBlockBorderWrapper"],
        div[data-testid="stVerticalBlockBorderWrapper"] p,
        div[data-testid="stVerticalBlockBorderWrapper"] span,
        div[data-testid="stVerticalBlockBorderWrapper"] small,
        div[data-testid="stVerticalBlockBorderWrapper"] li {
            color: rgba(255, 255, 255, 0.90) !important;
        }

        /* Teks yang berasal dari st.markdown / st.write */
        div[data-testid="stMarkdown"],
        div[data-testid="stMarkdown"] p,
        div[data-testid="stMarkdown"] span,
        div[data-testid="stMarkdown"] li,
        div[data-testid="stMarkdown"] small,
        div[data-testid="stMarkdownContainer"],
        div[data-testid="stMarkdownContainer"] p,
        div[data-testid="stMarkdownContainer"] span,
        div[data-testid="stMarkdownContainer"] li,
        div[data-testid="stMarkdownContainer"] small {
            color: rgba(255, 255, 255, 0.90) !important;
        }

        /* =========================================
           FIX KONTRAS METRIC (Total Nilai, Overdue, dll)
           ========================================= */

        /* Container metric */
        div[data-testid="stMetric"] {
            color: var(--text) !important;
        }

        /* Label kecil di atas angka */
        div[data-testid="stMetricLabel"],
        div[data-testid="stMetric"] label {
            color: rgba(255,255,255,0.80) !important;
            font-weight: 500 !important;
        }

        /* Angka besar (nilai utama metric) */
        div[data-testid="stMetricValue"] {
            color: var(--gold) !important;  /* ganti ke var(--text) kalau mau putih */
            font-weight: 700 !important;
        }

        /* Delta metric (jika dipakai) */
        div[data-testid="stMetricDelta"] {
            color: var(--green2) !important;
            font-weight: 600 !important;
        }

        /* =========================================
           LOADING SPINNER OVERLAY (FIXED & FULLSCREEN)
           ========================================= */
        /* Container utama spinner: dibuat memenuhi satu layar penuh */
        div[data-testid="stSpinner"] {
            position: fixed !important;
            top: 0 !important;
            left: 0 !important;
            width: 100vw !important;
            height: 100vh !important;
            z-index: 999999 !important; /* Pastikan di paling depan */
            
            /* Background Gelap Transparan (Glassmorphism) */
            background: rgba(0, 0, 0, 0.85) !important; 
            backdrop-filter: blur(8px); /* Efek blur latar belakang */
            
            /* Posisi konten di tengah */
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            gap: 20px;
            
            /* Reset style bawaan yang mengganggu */
            transform: none !important;
            border: none !important;
            box-shadow: none !important;
        }

        /* Teks pesan loading (misal: "Sedang menyimpan...") */
        div[data-testid="stSpinner"] > div {
            color: #ffffff !important;
            font-size: 1.1rem !important;
            font-weight: 500 !important;
            letter-spacing: 0.05em;
            text-shadow: 0 2px 4px rgba(0,0,0,0.5);
        }
        
        /* Icon Lingkaran Berputar (Spinner) */
        /* Target elemen SVG atau div lingkaran di dalam spinner */
        div[data-testid="stSpinner"] > div > div {
            border-top-color: var(--gold) !important;    /* Warna Emas */
            border-right-color: var(--green) !important; /* Warna Hijau */
            border-bottom-color: var(--gold) !important; /* Warna Emas */
            border-left-color: transparent !important;
            width: 3.5rem !important;  /* Ukuran icon lebih besar */
            height: 3.5rem !important;
            border-width: 4px !important; /* Ketebalan garis */
        }

        </style>
        """,
        unsafe_allow_html=True
    )

inject_global_css()




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
# MOBILE DETECTION (safe, tidak mengubah desktop)
# =========================================================
def is_mobile_device() -> bool:
    """
    Deteksi via User-Agent. Hanya dipakai untuk membedakan UI HP vs Desktop.
    Jika st.context tidak tersedia, fallback = False (anggap desktop).
    """
    try:
        ua = ""
        if hasattr(st, "context") and hasattr(st.context, "headers"):
            headers = st.context.headers
            ua = (headers.get("user-agent") or headers.get("User-Agent") or "").lower()
        return any(k in ua for k in ["android", "iphone", "ipad", "mobile"])
    except Exception:
        return False

IS_MOBILE = is_mobile_device()



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
        # =========================================================
        # [BARU] AUTO-CREATE AUDIT SHEET SAAT STARTUP
        # =========================================================
        # Tambahkan blok ini agar sheet otomatis dibuat saat aplikasi dibuka
        from audit_service import ensure_audit_sheet
        try:
            ensure_audit_sheet(spreadsheet)
            # print("Audit sheet ready.") # Opsional untuk debug console
        except Exception as e:
            st.error(f"⚠️ Sistem Error: Gagal membuat Sheet Audit otomatis. Pesan: {e}")
            # Ini akan memunculkan kotak merah di layar jika gagal,
            # jadi admin langsung tahu ada yang salah.
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


@st.cache_data(ttl=3600)
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
        # maybe_auto_format_sheet(ws)
        return True, "Berhasil tambah tim!"
    except Exception as e:
        return False, str(e)


# =========================================================
# TEAM CONFIG
# =========================================================
@st.cache_data(ttl=3600)
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
        # maybe_auto_format_sheet(ws)
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


@st.cache_data(ttl=3600)
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
        # maybe_auto_format_sheet(ws)
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
        # maybe_auto_format_sheet(ws)
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

        # maybe_auto_format_sheet(ws)
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
        # maybe_auto_format_sheet(ws)

        return True
    except Exception as e:
        print(f"Error saving daily report batch: {e}")
        return False


@st.cache_data(ttl=3600)
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


@st.cache_data(ttl=3600)
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


def render_laporan_harian_mobile():
    st.markdown("## 📝 Laporan Harian")

    # tombol balik
    if st.button("⬅️ Kembali ke Beranda", use_container_width=True):
        set_nav("home")

    staff_list = get_daftar_staf_terbaru()

    # tetap pakai key pelapor_main agar actor log tetap konsisten
    nama_pelapor = st.selectbox("Nama Pelapor", staff_list, key="pelapor_main")

    pending_msg = get_reminder_pending(nama_pelapor)
    if pending_msg:
        st.warning(f"🔔 Pending terakhir: **{pending_msg}**")

    tab1, tab2, tab3, tab4 = st.tabs(["📌 Aktivitas", "🏁 Kesimpulan", "📇 Kontak", "✅ Submit"])

    # ===== TAB 1: Aktivitas =====
    with tab1:
        kategori_aktivitas = st.radio(
            "Jenis Aktivitas",
            ["🚗 Sales (Kunjungan Lapangan)", "💻 Digital Marketing / Konten / Ads", "📞 Telesales / Follow Up", "🏢 Lainnya"],
            horizontal=False,
            key="m_kategori"
        )
        is_kunjungan = kategori_aktivitas.startswith("🚗")

        if "Digital Marketing" in kategori_aktivitas:
            st.text_input("Link Konten / Ads / Drive (Opsional)", key="m_sosmed")

        if is_kunjungan:
            st.text_input("📍 Nama Klien / Lokasi Kunjungan (Wajib)", key="m_lokasi")
        else:
            st.text_input("Jenis Tugas", value=kategori_aktivitas, disabled=True, key="m_tugas")

        fotos = st.file_uploader(
            "Upload Bukti (opsional)",
            accept_multiple_files=True,
            disabled=not KONEKSI_DROPBOX_BERHASIL,
            key="m_fotos"
        )

        # 1 deskripsi saja agar ringkas (bisa detail per file via expander)
        st.text_area("Deskripsi Aktivitas (Wajib)", height=120, key="m_deskripsi")

        with st.expander("Detail deskripsi per file (opsional)", expanded=False):
            if fotos:
                for i, f in enumerate(fotos):
                    st.text_input(f"Ket. {f.name}", key=f"m_desc_{i}")

    # ===== TAB 2: Kesimpulan =====
    with tab2:
        st.text_area("💡 Kesimpulan hari ini", height=100, key="m_kesimpulan")
        st.text_area("🚧 Kendala internal", height=90, key="m_kendala")
        st.text_area("🧑‍💼 Kendala klien", height=90, key="m_kendala_klien")

    # ===== TAB 3: Kontak =====
    with tab3:
        st.radio(
            "📈 Tingkat Interest",
            ["Under 50% (A)", "50-75% (B)", "75%-100%"],
            horizontal=False,
            key="interest_persen"
        )
        st.text_input("👤 Nama Klien", key="nama_klien_input")
        st.text_input("📞 No HP/WA Klien", key="kontak_klien_input")
        st.text_input("📌 Next Plan / Pending (Reminder Besok)", key="m_pending")

# ===== TAB 4: Submit =====
    with tab4:
        st.caption("Pastikan data sudah benar, lalu submit.")

        if st.button("✅ Submit Laporan", type="primary", use_container_width=True):
            
            # --- 1. SIAPKAN VARIABEL DATA ---
            kategori_aktivitas = st.session_state.get("m_kategori", "")
            is_kunjungan = str(kategori_aktivitas).startswith("🚗")
            lokasi_input = st.session_state.get("m_lokasi", "") if is_kunjungan else kategori_aktivitas
            main_deskripsi = st.session_state.get("m_deskripsi", "")
            sosmed_link = st.session_state.get("m_sosmed", "") if "Digital Marketing" in str(kategori_aktivitas) else ""
            fotos = st.session_state.get("m_fotos", None)

            # --- 2. VALIDASI INPUT ---
            if is_kunjungan and not str(lokasi_input).strip():
                st.error("Lokasi kunjungan wajib diisi.")
                st.stop()
            
            if (not fotos) and (not str(main_deskripsi).strip()):
                st.error("Deskripsi wajib diisi.")
                st.stop()

            # --- 3. PERSIAPAN PROGRESS BAR ---
            # Container kosong untuk menaruh loading bar
            progress_placeholder = st.empty()
            
            # Hitung total langkah (Jumlah Foto + 1 langkah simpan ke Excel/GSheet)
            jml_foto = len(fotos) if fotos else 0
            total_steps = jml_foto + 1 
            current_step = 0

            # Tampilkan Bar Awal (0%)
            my_bar = progress_placeholder.progress(0, text="🚀 Memulai proses...")

            try:
                # Siapkan data timestamp & string lain
                ts = now_ts_str()
                val_kesimpulan = (st.session_state.get("m_kesimpulan") or "-").strip() or "-"
                val_kendala = (st.session_state.get("m_kendala") or "-").strip() or "-"
                val_kendala_klien = (st.session_state.get("m_kendala_klien") or "-").strip() or "-"
                val_pending = (st.session_state.get("m_pending") or "-").strip() or "-"
                val_feedback = ""
                val_interest = st.session_state.get("interest_persen") or "-"
                val_nama_klien = (st.session_state.get("nama_klien_input") or "-").strip() or "-"
                val_kontak_klien = (st.session_state.get("kontak_klien_input") or "-").strip() or "-"

                rows = []
                final_lokasi = lokasi_input if is_kunjungan else kategori_aktivitas

                # --- 4. PROSES UPLOAD FOTO (LOOPING) ---
                if fotos and KONEKSI_DROPBOX_BERHASIL:
                    for i, f in enumerate(fotos):
                        # Update Persentase Progress Bar
                        # (Contoh: Foto 1 dari 3 => 33%)
                        pct = float(current_step / total_steps)
                        # Pastikan pct tidak lebih dari 1.0
                        if pct > 1.0: pct = 1.0
                        
                        my_bar.progress(pct, text=f"📤 Mengupload foto ke-{i+1} dari {jml_foto}...")

                        # Eksekusi Upload (Berat)
                        url = upload_ke_dropbox(f, nama_pelapor, "Laporan_Harian")
                        
                        # Ambil deskripsi per foto jika ada
                        desc = st.session_state.get(f"m_desc_{i}", "") or main_deskripsi or "-"
                        
                        # Masukkan ke list rows
                        rows.append([
                            ts, nama_pelapor, final_lokasi, desc,
                            url, sosmed_link if sosmed_link else "-",
                            val_kesimpulan, val_kendala, val_kendala_klien,
                            val_pending, val_feedback, val_interest,
                            val_nama_klien, val_kontak_klien
                        ])
                        
                        # Tambah counter langkah
                        current_step += 1
                else:
                    # Jika tidak ada foto, langsung siapkan 1 baris
                    rows.append([
                        ts, nama_pelapor, final_lokasi, main_deskripsi,
                        "-", sosmed_link if sosmed_link else "-",
                        val_kesimpulan, val_kendala, val_kendala_klien,
                        val_pending, val_feedback, val_interest,
                        val_nama_klien, val_kontak_klien
                    ])

                # --- 5. PROSES SIMPAN KE DATABASE (GSHEET) ---
                # Update bar ke langkah terakhir sebelum selesai
                pct_save = float(current_step / total_steps)
                if pct_save > 0.95: pct_save = 0.95 # Biarkan sisa sedikit untuk efek selesai
                
                my_bar.progress(pct_save, text="💾 Menyimpan data ke Database...")
                
                # Eksekusi Simpan (Berat)
                ok = simpan_laporan_harian_batch(rows, nama_pelapor)

                # --- 6. FINISHING ---
                # Set bar ke 100%
                my_bar.progress(1.0, text="✅ Selesai!")
                time.sleep(0.8) # Jeda sebentar agar user lihat status 100%
                progress_placeholder.empty() # Hapus bar agar bersih

                if ok:
                    st.success(f"✅ Laporan tersimpan! Reminder: **{val_pending}**")
                    ui_toast("Laporan tersimpan!", icon="✅")
                    
                    # Clear cache & Navigasi
                    st.cache_data.clear()
                    time.sleep(1) 
                    set_nav("home")
                else:
                    st.error("Gagal menyimpan ke Database (GSheet).")

            except Exception as e:
                # Jika error, hapus bar dan tampilkan error
                progress_placeholder.empty()
                st.error(f"Terjadi kesalahan: {e}")


# =========================================================
# CLOSING DEAL
# =========================================================
@st.cache_data(ttl=3600)
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

        # maybe_auto_format_sheet(ws)
        return True, "Closing deal berhasil disimpan!"
    except Exception as e:
        return False, str(e)


# =========================================================
# PEMBAYARAN
# =========================================================
@st.cache_data(ttl=3600)
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
        # maybe_auto_format_sheet(ws)
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

        # maybe_auto_format_sheet(ws)
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
        # maybe_auto_format_sheet(ws)
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

def render_section_watermark():
    """
    Menampilkan watermark Sportarium di bagian bawah halaman/tab.
    Menggunakan file HERO_BG (sportarium.jpg) dengan style CSS .sx-section-watermark.
    """
    # Menggunakan aset global HERO_BG yang sudah didefinisikan di atas
    if not HERO_BG or not HERO_BG.exists():
        return

    b64 = _img_to_base64(HERO_BG)
    if not b64:
        return

    # Render HTML dengan class CSS yang sudah ada di inject_global_css
    html = f"""
    <div class="sx-section-watermark">
        <img src="data:image/jpeg;base64,{b64}" alt="Sportarium Watermark" />
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

def render_home_mobile():
    st.markdown("## 🧭 Menu Utama")
    st.caption("Pilih fitur seperti shortcut ala aplikasi mobile.")

    features = [
        {"key": "report",  "icon": "📝", "title": "Laporan Harian", "sub": "Input aktivitas + reminder"},
        {"key": "kpi",     "icon": "🎯", "title": "Target & KPI",   "sub": "Checklist team & individu"},
        {"key": "closing", "icon": "🤝", "title": "Closing Deal",   "sub": "Catat deal + export"},
        {"key": "payment", "icon": "💳", "title": "Pembayaran",     "sub": "DP/Termin/Pelunasan + jatuh tempo"},
        {"key": "log",     "icon": "📜", "title": "Global Audit Log", "sub": "Riwayat perubahan data (Super Admin)"},
        {"key": "admin",   "icon": "🔐", "title": "Akses Admin",    "sub": "Dashboard + kontrol (butuh login)"},
    ]

    cols = st.columns(2, gap="medium")
    for i, f in enumerate(features):
        with cols[i % 2]:
            with st.container(border=True):
                st.markdown(f"### {f['icon']} {f['title']}")
                st.caption(f["sub"])
                if st.button("Buka", use_container_width=True, key=f"home_open_{f['key']}"):
                    set_nav(f["key"])



# =========================================================
# APP UI
# =========================================================
if not KONEKSI_GSHEET_BERHASIL:
    st.error("Database Error.")
    st.stop()

# Small banner for Dropbox status
if not KONEKSI_DROPBOX_BERHASIL:
    st.warning("⚠️ Dropbox non-aktif. Fitur upload foto/bukti dimatikan.")

# =========================================================
# ROUTER NAV (untuk mobile ala "Facebook shortcut")
# =========================================================
HOME_NAV = "🏠 Beranda"

NAV_MAP = {
    "home": HOME_NAV,
    "report": "📝 Laporan Harian",
    "kpi": "🎯 Target & KPI",
    "closing": "🤝 Closing Deal",
    "payment": "💳 Pembayaran",
    "log": "📜 Global Audit Log",
    "admin": "📊 Dashboard Admin",
}

def _get_query_nav():
    try:
        # streamlit baru
        if hasattr(st, "query_params"):
            v = st.query_params.get("nav", None)
            # ✅ normalisasi: kalau list, ambil elemen pertama
            if isinstance(v, (list, tuple)):
                return v[0] if v else None
            return v

        # streamlit lama
        qp = st.experimental_get_query_params()
        return (qp.get("nav", [None])[0])
    except Exception:
        return None

def set_nav(nav_key: str):
    nav_key = nav_key if nav_key in NAV_MAP else "home"
    try:
        if hasattr(st, "query_params"):
            st.query_params["nav"] = [nav_key]   # ✅ konsisten dengan format list
        else:
            st.experimental_set_query_params(nav=nav_key)
    except Exception:
        pass
    st.session_state["menu_nav"] = NAV_MAP[nav_key]
    st.rerun()


# Session defaults
if "is_admin" not in st.session_state:
    st.session_state["is_admin"] = False

if "menu_nav" not in st.session_state:
    # Mobile masuk Beranda, Desktop tetap ke Laporan Harian (tidak berubah)
    st.session_state["menu_nav"] = HOME_NAV if IS_MOBILE else "📝 Laporan Harian"

# Sinkronkan kalau URL ada ?nav=...
nav_from_url = _get_query_nav()
if nav_from_url in NAV_MAP:
    st.session_state["menu_nav"] = NAV_MAP[nav_from_url]


# Render header
render_header()

# MOBILE: tampilkan Beranda sebagai landing page
menu_nav = st.session_state.get("menu_nav", HOME_NAV if IS_MOBILE else "📝 Laporan Harian")

if IS_MOBILE and menu_nav == HOME_NAV:
    render_home_mobile()
    st.stop()


# =========================================================
# SIDEBAR (SpaceX-inspired)
# =========================================================
with st.sidebar:
    if st.button("🔄 Refresh Data", type="primary", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.markdown("<div class='sx-section-title'>Navigation</div>", unsafe_allow_html=True)

    menu_items = [
        "📝 Laporan Harian",
        "🎯 Target & KPI",
        "🤝 Closing Deal",
        "💳 Pembayaran",
        "📜 Global Audit Log",
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

    # -----------------------------------------------------------
    # PROFIL USER (OTP LOGIN)
    # -----------------------------------------------------------
    st.divider()
    
    col_p1, col_p2 = st.columns([1, 3])
    
    with col_p1:
        st.markdown("👤") # Icon default karena OTP tidak ambil foto profil Google
            
    with col_p2:
        st.caption("Login sebagai:")
        st.markdown(f"**{st.session_state.get('user_name', 'User')}**")
        
        role_now = st.session_state.get("user_role", "user")
        role_color = "red" if role_now == "admin" else "blue"
        st.markdown(f":{role_color}[{role_now.upper()}]")

    # Tombol Logout Manual (Reset State)
    if st.button("🚪 Sign Out / Logout", use_container_width=True):
        # Reset semua variabel sesi yang penting
        st.session_state["logged_in"] = False
        st.session_state["user_email"] = None
        st.session_state["user_name"] = None
        st.session_state["user_role"] = None
        st.session_state["is_admin"] = False
        
        # Reset step OTP agar kembali ke input email saat login ulang
        st.session_state["otp_step"] = 1 
        st.session_state["temp_email"] = ""
        st.session_state["generated_otp"] = ""
        
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

menu_nav = st.session_state.get("menu_nav", "📝 Laporan Harian")

# [MULAI KODE TAMBAHAN: FIX NAVIGASI MOBILE]
# Ini akan memunculkan tombol Back & Menu Bawah untuk Closing, KPI, Payment, dll.
if IS_MOBILE and menu_nav != "📝 Laporan Harian":
    # 1. Tombol Kembali ke Beranda
    if st.button("⬅️ Kembali ke Beranda", use_container_width=True, key="global_mobile_back"):
        set_nav("home")
    
    # 2. Bottom Navigation Bar (Menu Bawah)
    # Perbaikan: Menambahkan link nav=log dan merapikan tag HTML
    st.markdown("""
    <div class="mobile-bottom-nav">
      <a href="?nav=home">🏠</a>
      <a href="?nav=report">📝</a>
      <a href="?nav=kpi">🎯</a>
      <a href="?nav=closing">🤝</a>
      <a href="?nav=payment">💳</a>
      <a href="?nav=log">📜</a>
    </div>
    """, unsafe_allow_html=True)
    
    st.divider()


# =========================================================
# FUNGSI RENDER MOBILE PER FITUR (BARU)
# =========================================================
def render_kpi_mobile():
    st.markdown("### 🎯 Target & KPI (Full Mobile)")

    # Gunakan Tabs seperti Desktop agar fitur lengkap
    tab1, tab2, tab3 = st.tabs(["🏆 Team", "⚡ Individu", "⚙️ Admin"])

    # --- TAB 1: TEAM ---
    with tab1:
        st.caption("Checklist & Upload Bukti Team")
        df_team = load_checklist(SHEET_TARGET_TEAM, TEAM_CHECKLIST_COLUMNS)
        
        if not df_team.empty:
            # 1. Editor (Bisa Edit Status/Text)
            edited_team = render_hybrid_table(df_team, "mob_team_tbl", "Misi")
            
            # Tombol Simpan
            if st.button("💾 Simpan Perubahan (Team)", use_container_width=True, key="mob_btn_save_team"):
                actor = get_actor_fallback(default="Admin")
                final_df = apply_audit_checklist_changes(df_team, edited_team, ["Misi"], actor)
                if save_checklist(SHEET_TARGET_TEAM, final_df, TEAM_CHECKLIST_COLUMNS):
                    st.success("Tersimpan!")
                    st.rerun()

            st.divider()
            
            # 2. Upload Bukti (Fitur Desktop dibawa ke HP)
            with st.expander("📂 Upload Bukti / Catatan"):
                sel_misi = st.selectbox("Pilih Misi", df_team["Misi"].unique(), key="mob_sel_misi")
                note_misi = st.text_area("Catatan", key="mob_note_misi")
                file_misi = st.file_uploader("File", key="mob_file_misi")
                
                if st.button("Update Bukti", use_container_width=True, key="mob_upd_team"):
                    actor = get_actor_fallback()
                    res, msg = update_evidence_row(SHEET_TARGET_TEAM, sel_misi, note_misi, file_misi, actor, "Team")
                    if res: st.success("Updated!"); st.rerun()
                    else: st.error(msg)
        else:
            st.info("Belum ada target team.")

    # --- TAB 2: INDIVIDU ---
    with tab2:
        st.caption("Target Individu")
        staff = get_daftar_staf_terbaru()
        filter_nama = st.selectbox("Filter Nama:", staff, key="mob_indiv_filter")
        
        df_indiv_all = load_checklist(SHEET_TARGET_INDIVIDU, INDIV_CHECKLIST_COLUMNS)
        df_user = df_indiv_all[df_indiv_all["Nama"] == filter_nama]
        
        if not df_user.empty:
            edited_indiv = render_hybrid_table(df_user, f"mob_indiv_{filter_nama}", "Target")
            
            if st.button(f"💾 Simpan ({filter_nama})", use_container_width=True, key="mob_save_indiv"):
                df_merged = df_indiv_all.copy()
                df_merged.update(edited_indiv)
                final_df = apply_audit_checklist_changes(df_indiv_all, df_merged, ["Nama", "Target"], filter_nama)
                save_checklist(SHEET_TARGET_INDIVIDU, final_df, INDIV_CHECKLIST_COLUMNS)
                st.success("Tersimpan!")
                st.rerun()
            
            # Upload Bukti Individu
            with st.expander(f"📂 Update Bukti ({filter_nama})"):
                pilih_target = st.selectbox("Target:", df_user["Target"].tolist(), key="mob_sel_indiv")
                note_target = st.text_area("Catatan", key="mob_note_indiv")
                file_target = st.file_uploader("File", key="mob_file_indiv")
                if st.button("Update Pribadi", use_container_width=True, key="mob_upd_indiv"):
                    res, msg = update_evidence_row(SHEET_TARGET_INDIVIDU, pilih_target, note_target, file_target, filter_nama, "Individu")
                    if res: st.success("Updated!"); st.rerun()
                    else: st.error(msg)
        else:
            st.info("Kosong.")

    # --- TAB 3: ADMIN (Fitur Tambah Target) ---
    with tab3:
        st.markdown("#### ➕ Tambah Target Baru")
        jenis_t = st.radio("Jenis", ["Team", "Individu"], horizontal=True, key="mob_jenis_target")
        
        with st.form("mob_add_kpi"):
            target_text = st.text_area("Isi Target (1 per baris)", height=100)
            c1, c2 = st.columns(2)
            t_mulai = c1.date_input("Mulai", value=datetime.now())
            t_selesai = c2.date_input("Selesai", value=datetime.now()+timedelta(days=30))
            
            nama_target = ""
            if jenis_t == "Individu":
                nama_target = st.selectbox("Staf:", get_daftar_staf_terbaru(), key="mob_add_staf_target")
            
            if st.form_submit_button("Tambah Target", use_container_width=True):
                targets = clean_bulk_input(target_text)
                sheet = SHEET_TARGET_TEAM if jenis_t == "Team" else SHEET_TARGET_INDIVIDU
                base = ["", str(t_mulai), str(t_selesai), "FALSE", "-"]
                if jenis_t == "Individu": base = [nama_target] + base
                
                if add_bulk_targets(sheet, base, targets):
                    st.success("Berhasil!")
                    st.rerun()
                else:
                    st.error("Gagal.")

def render_closing_mobile():
    st.markdown("### 🤝 Closing Deal (Full Mobile)")
    
    # Form Input Tetap Sama
    with st.expander("➕ Input Deal Baru", expanded=False):
        with st.form("mob_form_closing"):
            cd_group = st.text_input("Nama Group (Opsional)")
            cd_marketing = st.selectbox("Nama Marketing", get_daftar_staf_terbaru())
            cd_tgl = st.date_input("Tanggal Event")
            cd_bidang = st.text_input("Bidang", placeholder="F&B / Wedding")
            cd_nilai = st.text_input("Nilai (Rp)", placeholder="Contoh: 15jt")
            
            if st.form_submit_button("Simpan Deal", type="primary", use_container_width=True):
                res, msg = tambah_closing_deal(cd_group, cd_marketing, cd_tgl, cd_bidang, cd_nilai)
                if res:
                    st.success(msg)
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error(msg)
    
    st.divider()
    st.markdown("#### 📋 Riwayat Lengkap & Download")
    
    df_cd = load_closing_deal()
    
    if not df_cd.empty:
        # 1. Tampilkan Statistik Singkat
        tot = df_cd[COL_NILAI_KONTRAK].sum() if COL_NILAI_KONTRAK in df_cd.columns else 0
        st.metric("Total Closing", format_rupiah_display(tot))
        
        # 2. Tampilkan Semua Data (Tanpa batasan .head)
        st.dataframe(df_cd, use_container_width=True, hide_index=True)
        
        # 3. Fitur Download (Excel & CSV) - Diaktifkan di Mobile
        c1, c2 = st.columns(2)
        with c1:
            if HAS_OPENPYXL:
                xb = df_to_excel_bytes(df_cd, sheet_name="Closing")
                if xb:
                    st.download_button("⬇️ Excel", data=xb, file_name="closing_mob.xlsx", 
                                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
                                       use_container_width=True)
        with c2:
            csv = df_cd.to_csv(index=False).encode('utf-8')
            st.download_button("⬇️ CSV", data=csv, file_name="closing_mob.csv", 
                               mime="text/csv", use_container_width=True)

        # 4. Grafik (Jika ada Plotly)
        if HAS_PLOTLY:
            with st.expander("📊 Lihat Grafik Performance"):
                try:
                    df_plot = df_cd.copy()
                    df_plot[COL_NILAI_KONTRAK] = df_plot[COL_NILAI_KONTRAK].fillna(0).astype(int)
                    fig = px.bar(df_plot, x=COL_MARKETING, y=COL_NILAI_KONTRAK, color=COL_BIDANG,
                                 title="Total per Marketing")
                    st.plotly_chart(fig, use_container_width=True)
                except: pass
    else:
        st.info("Belum ada data.")

def render_payment_mobile():
    st.markdown("### 💳 Pembayaran (Full Mobile)")
    
    # Input Baru
    with st.expander("➕ Input Pembayaran Baru", expanded=False):
        with st.form("mob_form_pay"):
            p_group = st.text_input("Group")
            p_marketing = st.selectbox("Marketing", get_daftar_staf_terbaru())
            p_nominal = st.text_input("Nominal (Rp)")
            p_jenis = st.selectbox("Jenis", ["DP", "Pelunasan", "Termin"])
            p_jatuh_tempo = st.date_input("Jatuh Tempo")
            p_status = st.checkbox("Sudah Dibayar?")
            p_bukti = st.file_uploader("Upload Bukti", disabled=not KONEKSI_DROPBOX_BERHASIL)
            
            if st.form_submit_button("Simpan", type="primary", use_container_width=True):
                res, msg = tambah_pembayaran_dp(p_group, p_marketing, datetime.now(), p_jenis, p_nominal, p_jatuh_tempo, p_status, p_bukti, "-")
                if res:
                    st.success(msg)
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(msg)

    st.divider()
    
    # Load Data
    df_pay = load_pembayaran_dp()
    
    if not df_pay.empty:
        # Alert System
        overdue, due_soon = build_alert_pembayaran(df_pay)
        if not overdue.empty: st.error(f"⛔ {len(overdue)} Overdue!")
        if not due_soon.empty: st.warning(f"⚠️ {len(due_soon)} Jatuh Tempo Dekat")
        
        # --- FITUR EDIT DATA DI MOBILE ---
        st.markdown("#### 📋 Edit Data & Cek Status")
        st.caption("Anda bisa mengubah Status 'Lunas' atau 'Jatuh Tempo' langsung di sini.")
        
        # Siapkan kolom yang bisa diedit
        df_view = payment_df_for_display(df_pay)
        editable_cols = {COL_STATUS_BAYAR, COL_JATUH_TEMPO, COL_CATATAN_BAYAR}
        disabled_cols = [c for c in PAYMENT_COLUMNS if c not in editable_cols]

        edited_pay_mob = st.data_editor(
            df_view,
            column_config={
                COL_STATUS_BAYAR: st.column_config.CheckboxColumn("Lunas?", width="small"),
                COL_JATUH_TEMPO: st.column_config.DateColumn("Jatuh Tempo", width="medium"),
                COL_NOMINAL_BAYAR: st.column_config.TextColumn("Nominal", disabled=True),
                COL_BUKTI_BAYAR: st.column_config.LinkColumn("Bukti"),
                COL_TS_UPDATE: st.column_config.TextColumn("Log", disabled=True)
            },
            disabled=disabled_cols,
            hide_index=True,
            use_container_width=True,
            key="editor_pay_mobile"
        )
        
        # Tombol Simpan Perubahan
        if st.button("💾 Simpan Perubahan Data", use_container_width=True, key="btn_save_pay_mob"):
            df_after = df_pay.copy().set_index(COL_TS_BAYAR, drop=False)
            ed = edited_pay_mob.copy().set_index(COL_TS_BAYAR, drop=False)
            
            # Apply changes
            for c in [COL_STATUS_BAYAR, COL_JATUH_TEMPO, COL_CATATAN_BAYAR]:
                if c in ed.columns: df_after.loc[ed.index, c] = ed[c]
            
            df_after = df_after.reset_index(drop=True)
            actor = get_actor_fallback(default="Mobile User")
            
            final_df = apply_audit_payments_changes(df_pay, df_after, actor=actor)
            if save_pembayaran_dp(final_df):
                st.success("Data Terupdate!")
                st.rerun()
            else:
                st.error("Gagal simpan.")

        st.divider()
        
        # --- FITUR UPLOAD SUSULAN DI MOBILE ---
        with st.expander("📎 Upload Bukti (Susulan)"):
            df_pay_reset = df_pay.reset_index(drop=True)
            opts = [f"{i}. {r[COL_MARKETING]} ({format_rupiah_display(r[COL_NOMINAL_BAYAR])})" for i, r in df_pay_reset.iterrows()]
            sel_idx = st.selectbox("Pilih Data:", range(len(opts)), format_func=lambda x: opts[x], key="mob_sel_upd_bukti")
            
            file_susulan = st.file_uploader("Upload File Baru", key="mob_file_susulan")
            
            if st.button("⬆️ Upload Bukti", use_container_width=True, key="mob_btn_upd_bukti"):
                if file_susulan:
                    mk_name = df_pay_reset.iloc[sel_idx][COL_MARKETING]
                    ok, msg = update_bukti_pembayaran_by_index(sel_idx, file_susulan, mk_name, actor="Mobile User")
                    if ok:
                        st.success("Berhasil!")
                        st.rerun()
                    else: st.error(msg)
    else:
        st.info("Data kosong.")

def render_admin_mobile():
    st.markdown("### 🔐 Admin Dashboard (Full Mobile)")
    
    # 1. Cek Login
    if not st.session_state["is_admin"]:
        pwd = st.text_input("Password Admin", type="password", key="mob_adm_pwd")
        if st.button("Login", use_container_width=True, key="mob_adm_login"):
            if verify_admin_password(pwd):
                st.session_state["is_admin"] = True
                st.rerun()
            else:
                st.error("Password salah.")
        return # Stop disini kalau belum login

    # 2. Jika Sudah Login -> Tampilkan Dashboard Penuh
    if st.button("🔓 Logout", use_container_width=True, key="mob_adm_logout"):
        st.session_state["is_admin"] = False
        st.rerun()

    # --- CODE DASHBOARD ADMIN (Versi Compact untuk Mobile) ---
    
    # Helper sederhana (copy dari desktop logic)
    def get_cat(val):
        s = str(val)
        if any(k in s for k in ["Digital", "Ads", "Konten"]): return "Digital"
        return "Sales"

    staff_list = get_daftar_staf_terbaru()
    df_all = load_all_reports(staff_list)
    
    if not df_all.empty:
        try:
            df_all[COL_TIMESTAMP] = pd.to_datetime(df_all[COL_TIMESTAMP], format="%d-%m-%Y %H:%M:%S", errors="coerce")
            df_all["Tgl"] = df_all[COL_TIMESTAMP].dt.date
            df_all["Kat"] = df_all[COL_TEMPAT].apply(get_cat)
        except: pass

    # TABS NAVIGATION MOBILE
    tab_prod, tab_leads, tab_data, tab_cfg = st.tabs(["📈 Grafik", "🧲 Leads", "📦 Data", "⚙️ Config"])

    # A. Tab Produktivitas
    with tab_prod:
        st.caption("Analisa Kinerja")
        if not df_all.empty:
            days = st.selectbox("Hari Terakhir:", [7, 30, 90], key="mob_adm_days")
            start_d = datetime.now(tz=TZ_JKT).date() - timedelta(days=days)
            df_f = df_all[df_all["Tgl"] >= start_d].copy()
            
            st.metric("Total Laporan", len(df_f))
            st.bar_chart(df_f[COL_NAMA].value_counts())
        else: st.info("No data")

    # B. Tab Leads (Download Excel)
    with tab_leads:
        st.caption("Filter & Download Leads")
        sel_int = st.selectbox("Interest:", ["Under 50% (A)", "50-75% (B)", "75%-100%"], key="mob_adm_int")
        if not df_all.empty and COL_INTEREST in df_all.columns:
            df_leads = df_all[df_all[COL_INTEREST].astype(str).str.strip() == sel_int]
            st.dataframe(df_leads[[COL_NAMA_KLIEN, COL_KONTAK_KLIEN]], use_container_width=True)
            
            if HAS_OPENPYXL:
                xb = df_to_excel_bytes(df_leads, sheet_name="Leads")
                if xb:
                    st.download_button("⬇️ Excel Leads", data=xb, file_name=f"leads_{sel_int}.xlsx", 
                                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
                                       use_container_width=True)

    # C. Tab Data Master
    with tab_data:
        st.caption("Master Data Laporan")
        if st.button("Refresh Data", use_container_width=True, key="mob_ref_data"):
            st.cache_data.clear(); st.rerun()
            
        st.dataframe(df_all, use_container_width=True)
        
        if HAS_OPENPYXL:
            xb = df_to_excel_bytes(df_all, sheet_name="Master")
            if xb: st.download_button("⬇️ Download Full Excel", data=xb, file_name="master.xlsx", use_container_width=True)

    # D. Tab Config (Tambah Staf)
    with tab_cfg:
        st.caption("Kelola Staf & Tim")
        with st.form("mob_add_staff"):
            new_st = st.text_input("Nama Staf Baru")
            if st.form_submit_button("Simpan"):
                tambah_staf_baru(new_st)
                st.success("Tersimpan")
                st.cache_data.clear(); st.rerun()

def render_audit_mobile():
    st.markdown("### 📜 Global Audit Log (Mobile)")
    st.caption("Rekaman jejak perubahan data admin.")

    # Import fungsi load
    from audit_service import load_audit_log
    
    # Tombol Refresh
    if st.button("🔄 Refresh", use_container_width=True, key="mob_refresh_log"):
        st.cache_data.clear()
        st.rerun()

    # Load Data
    df_log = load_audit_log(spreadsheet)

    if not df_log.empty:
        # Sortir data terbaru diatas
        try:
            col_waktu = "Waktu & Tanggal"
            df_log[col_waktu] = pd.to_datetime(df_log[col_waktu], format="%d-%m-%Y %H:%M:%S", errors="coerce")
            df_log = df_log.sort_values(by=col_waktu, ascending=False)
        except:
            pass

        # Tampilan Mobile (Card View Sederhana)
        # Kita ambil 10 data terbaru saja biar ringan di HP
        st.markdown("#### 🕒 10 Aktivitas Terakhir")
        for i, row in df_log.head(10).iterrows():
            with st.container(border=True):
                # Baris 1: Siapa & Kapan
                st.markdown(f"**{row.get('Pelaku (User)', '-')}**")
                st.caption(f"📅 {row.get('Waktu & Tanggal')} | 🔧 {row.get('Aksi Dilakukan')}")
                
                # Baris 2: Apa yang diubah
                st.text(f"Data: {row.get('Nama Data / Sheet')}")
                st.info(f"📝 {row.get('Alasan Perubahan', '-')}")
                
                # Expander untuk detail teknis
                with st.expander("Lihat Detail Perubahan"):
                    st.code(row.get('Rincian (Sebelum ➡ Sesudah)', '-'), language="text")

        # Tombol Download Excel (Penting buat admin cek di HP)
        if HAS_OPENPYXL:
            xb = df_to_excel_bytes(df_log, sheet_name="Audit_Log")
            if xb:
                st.download_button("⬇️ Download Full Log (Excel)", data=xb, file_name="audit_log_full.xlsx", use_container_width=True)
    else:
        st.info("Belum ada data log.")

# =========================================================
# MAIN ROUTER LOGIC (REVISI TOTAL)
# =========================================================

if menu_nav == "📝 Laporan Harian":
    if IS_MOBILE:
        render_laporan_harian_mobile()
    else:
        # --- DESKTOP FULL FORM ---
        st.markdown("## 📝 Laporan Kegiatan Harian")
        
        # Header Info
        c1, c2 = st.columns([1, 2])
        with c1:
            pelapor = st.selectbox("Nama Pelapor", get_daftar_staf_terbaru(), key="pelapor_desk")
        with c2:
            pending = get_reminder_pending(pelapor)
            if pending: st.warning(f"🔔 Reminder Pending: {pending}")
            
        with st.container(border=True):
            with st.form("daily_report_desk", clear_on_submit=False):
                st.markdown("### 📌 Detail Aktivitas")
                col_kiri, col_kanan = st.columns(2)
                
                with col_kiri:
                    kategori = st.radio("Kategori", ["🚗 Sales Lapangan", "💻 Digital/Kantor", "📞 Telesales", "🏢 Lainnya"])
                    lokasi = st.text_input("Lokasi / Nama Klien / Jenis Tugas", placeholder="Wajib diisi...")
                    deskripsi = st.text_area("Deskripsi Detail", height=150)
                    foto = st.file_uploader("Upload Bukti", accept_multiple_files=True, disabled=not KONEKSI_DROPBOX_BERHASIL)
                
                with col_kanan:
                    st.markdown("### 📊 Hasil & Follow Up")
                    kesimpulan = st.text_area("Kesimpulan / Hasil", height=80)
                    kendala = st.text_area("Kendala Internal/Lapangan", height=60)
                    next_plan = st.text_input("Next Plan / Pending (Reminder)")
                    
                    st.markdown("### 👤 Data Klien (Jika ada)")
                    cl_nama = st.text_input("Nama Klien")
                    cl_kontak = st.text_input("No HP/WA")
                    cl_interest = st.selectbox("Interest Level", ["-", "Under 50%", "50-75%", "75-100%"])

                st.divider()
                if st.form_submit_button("✅ KIRIM LAPORAN", type="primary", use_container_width=True):
                    if not lokasi or not deskripsi:
                        st.error("Lokasi dan Deskripsi wajib diisi!")
                    else:
                        with st.spinner("Mengirim laporan..."):
                            # Logic Upload & Save mirip mobile
                            ts = now_ts_str()
                            final_link = "-"
                            if foto and KONEKSI_DROPBOX_BERHASIL:
                                links = []
                                for f in foto:
                                    l = upload_ke_dropbox(f, pelapor, "Laporan_Harian")
                                    links.append(l)
                                final_link = ", ".join(links)
                            
                            row_data = [
                                ts, pelapor, lokasi, deskripsi, final_link, "-", 
                                kesimpulan, kendala, "-", next_plan, "-", 
                                cl_interest, cl_nama, cl_kontak
                            ]
                            
                            if simpan_laporan_harian_batch([row_data], pelapor):
                                st.success("Laporan Terkirim!")
                                st.cache_data.clear()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("Gagal simpan ke GSheet.")

elif menu_nav == "🎯 Target & KPI":
    if IS_MOBILE:
        render_kpi_mobile()
    else:
        # --- DESKTOP LENGKAP ---
        st.markdown("## 🎯 Manajemen Target & KPI")
        tab1, tab2, tab3 = st.tabs(["🏆 Target Team", "⚡ Target Individu", "⚙️ Admin Setup"])
        
        # TAB 1: TEAM
        with tab1:
            st.caption("Checklist target bersama. Centang 'Status' jika selesai.")
            df_team = load_checklist(SHEET_TARGET_TEAM, TEAM_CHECKLIST_COLUMNS)
            
            if not df_team.empty:
                # Progress Bar
                done = len(df_team[df_team["Status"]==True])
                st.progress(done/len(df_team))
                st.caption(f"Progress: {done} / {len(df_team)}")
                
                # Hybrid Editor
                edited_team = render_hybrid_table(df_team, "team_desk", "Misi")
                
                c_save, c_upload = st.columns([1, 2])
                with c_save:
                    if st.button("💾 Simpan Perubahan Team", use_container_width=True):
                        actor = get_actor_fallback()
                        final_df = apply_audit_checklist_changes(df_team, edited_team, ["Misi"], actor)
                        save_checklist(SHEET_TARGET_TEAM, final_df, TEAM_CHECKLIST_COLUMNS)
                        st.success("Tersimpan!")
                        st.cache_data.clear()
                        st.rerun()
                
                with c_upload:
                    with st.expander("📂 Upload Bukti / Catatan (Per Item)"):
                        sel_misi = st.selectbox("Pilih Misi", df_team["Misi"].unique())
                        note_misi = st.text_area("Catatan Tambahan")
                        file_misi = st.file_uploader("Bukti", key="up_team_desk")
                        if st.button("Update Bukti Team"):
                            res, msg = update_evidence_row(SHEET_TARGET_TEAM, sel_misi, note_misi, file_misi, actor, "Team")
                            if res: st.success("Updated!"); st.rerun()
                            else: st.error(msg)
            else:
                st.info("Belum ada target team.")

        # TAB 2: INDIVIDU
        with tab2:
            st.caption("Monitoring target perorangan.")
            staff = get_daftar_staf_terbaru()
            pilih_staf = st.selectbox("Pilih Nama Staf:", staff)
            
            df_indiv_all = load_checklist(SHEET_TARGET_INDIVIDU, INDIV_CHECKLIST_COLUMNS)
            df_user = df_indiv_all[df_indiv_all["Nama"] == pilih_staf]
            
            if not df_user.empty:
                edited_indiv = render_hybrid_table(df_user, f"indiv_{pilih_staf}", "Target")
                if st.button(f"💾 Simpan Target {pilih_staf}"):
                    # Update logic complex (merge back to main df)
                    df_merged = df_indiv_all.copy()
                    df_merged.update(edited_indiv) # Simple update based on index
                    
                    final_df = apply_audit_checklist_changes(df_indiv_all, df_merged, ["Nama", "Target"], pilih_staf)
                    save_checklist(SHEET_TARGET_INDIVIDU, final_df, INDIV_CHECKLIST_COLUMNS)
                    st.success("Tersimpan!")
                    st.cache_data.clear()
                    st.rerun()
            else:
                st.info(f"Tidak ada target aktif untuk {pilih_staf}")

        # TAB 3: ADMIN (ADD TARGET)
        with tab3:
            st.markdown("### ➕ Tambah Target Baru")
            jenis_t = st.radio("Jenis Target", ["Team", "Individu"], horizontal=True)
            
            with st.form("add_kpi_desk"):
                target_text = st.text_area("Isi Target (Bisa banyak baris)", height=100)
                tgl_mulai = st.date_input("Mulai", value=datetime.now())
                tgl_selesai = st.date_input("Selesai", value=datetime.now()+timedelta(days=30))
                
                nama_target = ""
                if jenis_t == "Individu":
                    nama_target = st.selectbox("Untuk Staf:", get_daftar_staf_terbaru())
                
                if st.form_submit_button("Tambah Target"):
                    targets = clean_bulk_input(target_text)
                    sheet = SHEET_TARGET_TEAM if jenis_t == "Team" else SHEET_TARGET_INDIVIDU
                    base_row = ["", str(tgl_mulai), str(tgl_selesai), "FALSE", "-"] 
                    if jenis_t == "Individu":
                        base_row = [nama_target] + base_row
                    
                    if add_bulk_targets(sheet, base_row, targets):
                        st.success("Berhasil ditambahkan!")
                        st.cache_data.clear()
                        st.rerun()

# =========================================================
# MENU: CLOSING DEAL (FIXED & RESTORED)
# =========================================================
elif menu_nav == "🤝 Closing Deal":
    # --- LOGIKA MOBILE (HP) ---
    if IS_MOBILE:
        st.markdown("### 🤝 Closing Deal (Mobile)")
        
        with st.expander("➕ Input Deal Baru", expanded=True):
            with st.form("mob_form_closing"):
                # Tambahkan Input Group yang hilang sebelumnya
                cd_group = st.text_input("Nama Group (Opsional)", placeholder="Kosongkan jika tidak ada")
                cd_marketing = st.selectbox("Nama Marketing", get_daftar_staf_terbaru())
                cd_tgl = st.date_input("Tanggal Event")
                cd_bidang = st.text_input("Bidang", placeholder="F&B / Wedding / dll")
                cd_nilai = st.text_input("Nilai (Rp)", placeholder="Contoh: 15jt")
                
                if st.form_submit_button("Simpan Deal", type="primary", use_container_width=True):
                    with st.spinner("Menyimpan data..."):
                        # Panggil fungsi dengan 5 argumen lengkap
                        res, msg = tambah_closing_deal(cd_group, cd_marketing, cd_tgl, cd_bidang, cd_nilai)
                        
                    if res:
                        st.success(msg)
                        st.cache_data.clear()
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(msg)
        
        st.divider()
        st.markdown("#### 📋 Riwayat Closing (5 Terakhir)")
        
        df_cd = load_closing_deal()
        
        if not df_cd.empty:
            df_display = df_cd.sort_index(ascending=False).head(5) 
            
            for _, row in df_display.iterrows():
                with st.container(border=True):
                    val_disp = format_rupiah_display(row.get(COL_NILAI_KONTRAK))
                    st.markdown(f"💰 **{val_disp}**")
                    st.caption(f"👤 {row.get(COL_MARKETING,'-')} | 📅 {row.get(COL_TGL_EVENT,'-')}")
                    st.text(f"Group: {row.get(COL_GROUP,'-')} | Bidang: {row.get(COL_BIDANG,'-')}")
            
            sisa_data = len(df_cd) - 5
            if sisa_data > 0:
                st.caption(f"ℹ️ {sisa_data} data lama disembunyikan. Buka di Laptop untuk lihat semua.")
        else:
            st.info("Belum ada data.")

    # --- LOGIKA DESKTOP (PC/LAPTOP) ---
    else:
        st.markdown("## 🤝 Closing Deal")
        st.caption("Pencatatan sales closing (Kontrak/Event).")

        # 1. FORM INPUT LENGKAP (5 KOLOM)
        with st.container(border=True):
            st.markdown("### ➕ Input Closing Deal")
            with st.form("form_closing_desk_full", clear_on_submit=True):
                # Baris 1: Group, Marketing, Tanggal
                c1, c2, c3 = st.columns(3)
                with c1:
                    inp_group = st.text_input("Nama Group (Opsional)", placeholder="Kosongkan jika personal")
                with c2:
                    inp_marketing = st.text_input("Nama Marketing (Wajib)", placeholder="Nama Sales")
                with c3:
                    inp_tgl_event = st.date_input("Tanggal Event", value=datetime.now(tz=TZ_JKT).date())
                
                # Baris 2: Bidang, Nilai
                c4, c5 = st.columns([1, 1])
                with c4:
                    inp_bidang = st.text_input("Bidang / Jenis Event", placeholder="Contoh: Gathering / Training / Wedding")
                with c5:
                    inp_nilai = st.text_input("Nilai Kontrak (Rupiah)", placeholder="Contoh: 15.000.000 / 15jt")
                
                if st.form_submit_button("✅ Simpan Closing Deal", type="primary", use_container_width=True):
                    if not inp_marketing or not inp_nilai:
                        st.error("Nama Marketing dan Nilai Kontrak wajib diisi!")
                    else:
                        # Panggil fungsi save
                        res, msg = tambah_closing_deal(inp_group, inp_marketing, inp_tgl_event, inp_bidang, inp_nilai)
                        if res:
                            st.success(msg)
                            ui_toast("Closing deal tersimpan!", icon="✅")
                            st.cache_data.clear()
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(msg)
        
        # 2. TABEL DATA & EXPORT
        st.divider()
        st.markdown("### 📋 Riwayat Closing Deal")
        
        df_cd = load_closing_deal()
        
        if not df_cd.empty:
            # Summary Metrics di atas tabel
            tot_nilai = df_cd[COL_NILAI_KONTRAK].sum() if COL_NILAI_KONTRAK in df_cd.columns else 0
            
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Closing (Deal)", len(df_cd))
            m2.metric("Total Nilai Kontrak", format_rupiah_display(tot_nilai))
            m3.metric("Bulan Ini", f"{len(df_cd)} deal") # Placeholder logic sederhana

            # Tampilkan Tabel
            df_show = df_cd.copy()
            if COL_NILAI_KONTRAK in df_show.columns:
                df_show[COL_NILAI_KONTRAK] = df_show[COL_NILAI_KONTRAK].apply(lambda x: format_rupiah_display(x))
            
            st.dataframe(
                df_show, 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    COL_NILAI_KONTRAK: st.column_config.TextColumn("Nilai Kontrak", width="medium"),
                    COL_TGL_EVENT: st.column_config.DateColumn("Tanggal Event", format="DD/MM/YYYY")
                }
            )
            
            # Tombol Download Excel/CSV
            col_ex, col_csv = st.columns([1, 1])
            with col_ex:
                if HAS_OPENPYXL:
                    xbytes = df_to_excel_bytes(
                        df_cd, 
                        sheet_name="Closing_Deal",
                        right_align_cols=[COL_NILAI_KONTRAK],
                        number_format_cols={COL_NILAI_KONTRAK: '"Rp" #,##0'}
                    )
                    if xbytes:
                        st.download_button(
                            "⬇️ Download Excel (Closing)", 
                            data=xbytes, 
                            file_name="closing_deal.xlsx", 
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
                            use_container_width=True
                        )
            
            with col_csv:
                csv_data = df_cd.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "⬇️ Download CSV", 
                    data=csv_data, 
                    file_name="closing_deal.csv", 
                    mime="text/csv", 
                    use_container_width=True
                )

            # Chart Sederhana (Jika Plotly ada)
            if HAS_PLOTLY and not df_cd.empty:
                try:
                    st.markdown("#### 📊 Grafik Performa")
                    df_plot = df_cd.copy()
                    df_plot[COL_NILAI_KONTRAK] = df_plot[COL_NILAI_KONTRAK].fillna(0).astype(int)
                    
                    fig = px.bar(
                        df_plot, 
                        x=COL_MARKETING, 
                        y=COL_NILAI_KONTRAK, 
                        color=COL_BIDANG, 
                        title="Nilai Kontrak per Marketing (Breakdown Bidang)",
                        labels={COL_NILAI_KONTRAK: "Total Nilai (Rp)", COL_MARKETING: "Nama Marketing"}
                    )
                    st.plotly_chart(fig, use_container_width=True)
                except Exception:
                    pass

        else:
            st.info("Belum ada data closing deal yang tersimpan.")
            
        render_section_watermark()

# =========================================================
# MENU: PEMBAYARAN (FULL FEATURE RESTORED)
# =========================================================
elif menu_nav == "💳 Pembayaran":
    if IS_MOBILE:
        render_payment_mobile()
    else:
        # --- TAMPILAN DESKTOP LENGKAP ---
        st.markdown("## 💳 Pembayaran (DP / Termin / Pelunasan)")
        st.caption("Input pembayaran, monitoring jatuh tempo, dan audit log otomatis.")

        # 1. FORM INPUT PEMBAYARAN
        with st.container(border=True):
            st.markdown("### ➕ Input Pembayaran")
            with st.form("form_pay_desk_full", clear_on_submit=True):
                col_a, col_b = st.columns(2)
                with col_a:
                    p_group = st.text_input("Nama Group (Opsional)", placeholder="Kosongkan jika tidak ada")
                    p_marketing = st.text_input("Nama Marketing (Wajib)", placeholder="Contoh: Andi")
                    p_tgl_event = st.date_input("Tanggal Event", value=datetime.now(tz=TZ_JKT).date())
                    p_jenis_opt = st.selectbox("Jenis Pembayaran", ["Down Payment (DP)", "Termin", "Pelunasan", "Lainnya"])
                    
                with col_b:
                    p_nominal = st.text_input("Nominal (Rp)", placeholder="Contoh: 5.000.000")
                    p_jatuh_tempo = st.date_input("Batas Waktu Bayar (Jatuh Tempo)", value=datetime.now(tz=TZ_JKT).date() + timedelta(days=7))
                    p_status = st.checkbox("✅ Sudah Dibayar?", value=False)
                    p_catatan = st.text_area("Catatan", height=100, placeholder="Keterangan transfer...")
                    p_bukti = st.file_uploader("Upload Bukti", disabled=not KONEKSI_DROPBOX_BERHASIL)

                # Logic Custom Jenis Bayar
                p_jenis_final = p_jenis_opt
                if p_jenis_opt == "Lainnya":
                    p_jenis_final = st.text_input("Tulis Jenis Pembayaran Lainnya:", placeholder="Misal: Refund")

                if st.form_submit_button("✅ Simpan Pembayaran", type="primary", use_container_width=True):
                    if not p_marketing or not p_nominal:
                        st.error("Nama Marketing dan Nominal wajib diisi!")
                    else:
                        res, msg = tambah_pembayaran_dp(
                            p_group, p_marketing, p_tgl_event, p_jenis_final, 
                            p_nominal, p_jatuh_tempo, p_status, p_bukti, p_catatan
                        )
                        if res:
                            st.success(msg)
                            st.cache_data.clear()
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(msg)

        # 2. TABEL DATA & EDIT (AUDIT LOG)
        with st.container(border=True):
            st.markdown("### 📋 Data Pembayaran + Audit Log")
            df_pay = load_pembayaran_dp()

            if df_pay.empty:
                st.info("Belum ada data pembayaran.")
            else:
                # Alert System
                overdue, due_soon = build_alert_pembayaran(df_pay)
                c1, c2 = st.columns(2)
                c1.metric("⛔ Overdue", len(overdue))
                c2.metric("⚠️ Due Soon (3 Hari)", len(due_soon))

                if not overdue.empty:
                    st.error(f"Ada {len(overdue)} pembayaran jatuh tempo yang belum lunas!")

                # Editor Data (Fitur Edit Langsung di Tabel)
                st.caption("Edit data di bawah ini (Status, Jatuh Tempo, Catatan) lalu klik Simpan.")
                
                # Setup Editor Actor
                current_user = get_actor_fallback(default="Admin")
                
                df_view = payment_df_for_display(df_pay)
                
                # Config kolom agar user tidak edit sembarangan
                disabled_cols = [c for c in df_view.columns if c not in [COL_STATUS_BAYAR, COL_JATUH_TEMPO, COL_CATATAN_BAYAR, COL_JENIS_BAYAR]]

                edited_pay = st.data_editor(
                    df_view,
                    disabled=disabled_cols,
                    column_config={
                        COL_STATUS_BAYAR: st.column_config.CheckboxColumn("Lunas?", width="small"),
                        COL_JATUH_TEMPO: st.column_config.DateColumn("Jatuh Tempo"),
                        COL_NOMINAL_BAYAR: st.column_config.TextColumn("Nominal", disabled=True),
                        COL_TS_UPDATE: st.column_config.TextColumn("Log Perubahan", width="large", disabled=True),
                    },
                    use_container_width=True,
                    hide_index=True,
                    key="editor_pay_desktop"
                )

                if st.button("💾 Simpan Perubahan Data", use_container_width=True):
                    # Logic Simpan Perubahan ke GSheet + Audit Log
                    df_after = df_pay.copy()
                    # Mapping perubahan dari editor kembali ke format asli
                    # (Simplified logic for brevity - assumes direct mapping works or use previous generic update logic)
                    # Agar aman, kita pakai logic apply_audit_payments_changes yang sudah dibuat
                    
                    # Reconstruct df_after from edited_pay view (careful with types)
                    # Karena data_editor return dataframe visual, kita hanya ambil kolom yg diedit
                    for idx, row in edited_pay.iterrows():
                        real_idx = idx # Assuming index aligns
                        if COL_STATUS_BAYAR in row: df_after.at[real_idx, COL_STATUS_BAYAR] = row[COL_STATUS_BAYAR]
                        if COL_JATUH_TEMPO in row: df_after.at[real_idx, COL_JATUH_TEMPO] = row[COL_JATUH_TEMPO]
                        if COL_CATATAN_BAYAR in row: df_after.at[real_idx, COL_CATATAN_BAYAR] = row[COL_CATATAN_BAYAR]
                        if COL_JENIS_BAYAR in row: df_after.at[real_idx, COL_JENIS_BAYAR] = row[COL_JENIS_BAYAR]

                    final_df = apply_audit_payments_changes(df_pay, df_after, actor=current_user)
                    
                    if save_pembayaran_dp(final_df):
                        st.success("Data berhasil diperbarui!")
                        st.cache_data.clear()
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Gagal menyimpan.")

        # 3. UPDATE BUKTI (Expander)
        with st.expander("📎 Update Bukti Pembayaran (Susulan)", expanded=False):
            st.info("Gunakan fitur ini jika ingin upload bukti bayar untuk data yang sudah ada.")
            df_pay_reset = df_pay.reset_index(drop=True)
            
            opts = [f"{i+1}. {r[COL_MARKETING]} - {format_rupiah_display(r[COL_NOMINAL_BAYAR])}" for i, r in df_pay_reset.iterrows()]
            sel_idx = st.selectbox("Pilih Data", range(len(opts)), format_func=lambda x: opts[x])
            
            file_susulan = st.file_uploader("Upload Bukti Baru", key="pay_susulan")
            if st.button("⬆️ Upload Bukti Susulan"):
                if file_susulan:
                    mk_name = df_pay_reset.iloc[sel_idx][COL_MARKETING]
                    ok, msg = update_bukti_pembayaran_by_index(sel_idx, file_susulan, mk_name, actor="Admin")
                    if ok:
                        st.success("Bukti terupload!")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(msg)
                else:
                    st.error("Pilih file dulu.")

# =========================================================
# MENU: GLOBAL AUDIT LOG
# =========================================================
elif menu_nav == "📜 Global Audit Log":
    if IS_MOBILE:
        render_audit_mobile()
    else:
        # --- LOGIC DESKTOP ---
        st.markdown("## 📜 Global Audit Log")
        st.caption("Rekaman jejak perubahan data yang dilakukan oleh Admin (Super Editor). Transparansi data.")

        # Load Data dari Service
        from audit_service import load_audit_log
        
        # Tombol Refresh
        if st.button("🔄 Refresh Log", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        with st.spinner("Memuat data log..."):
            df_log = load_audit_log(spreadsheet)

        if not df_log.empty:
            # Konversi kolom Waktu agar bisa di-sort
            try:
                col_waktu = "Waktu & Tanggal" 
                df_log[col_waktu] = pd.to_datetime(df_log[col_waktu], format="%d-%m-%Y %H:%M:%S", errors="coerce")
                df_log = df_log.sort_values(by=col_waktu, ascending=False)
            except Exception:
                pass 

            # --- FITUR FILTERING ---
            with st.expander("🔍 Filter Pencarian"):
                c1, c2 = st.columns(2)
                with c1:
                    filter_user = st.multiselect("Pilih Pelaku (User)", df_log["Pelaku (User)"].unique())
                with c2:
                    filter_sheet = st.multiselect("Pilih Sheet/Data", df_log["Nama Data / Sheet"].unique())
            
            # Terapkan Filter
            df_show = df_log.copy()
            if filter_user:
                df_show = df_show[df_show["Pelaku (User)"].isin(filter_user)]
            if filter_sheet:
                df_show = df_show[df_show["Nama Data / Sheet"].isin(filter_sheet)]

            # --- TAMPILAN DATA ---
            st.markdown(f"**Total Record:** {len(df_show)}")
            
            st.dataframe(
                df_show, 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "Waktu & Tanggal": st.column_config.DatetimeColumn("Waktu", format="D MMM YYYY, HH:mm:ss"),
                    "Rincian (Sebelum ➡ Sesudah)": st.column_config.TextColumn("Detail Perubahan", width="large"),
                    "Alasan Perubahan": st.column_config.TextColumn("Alasan", width="medium"),
                }
            )

            # Download Button
            if HAS_OPENPYXL:
                xb = df_to_excel_bytes(df_show, sheet_name="Audit_Log")
                if xb:
                    st.download_button("⬇️ Download Log (Excel)", data=xb, file_name="global_audit_log.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.info("Belum ada riwayat perubahan data.")

        render_section_watermark()

# =========================================================
# MENU: DASHBOARD ADMIN (MIGRATED & UPGRADED)
# =========================================================
elif menu_nav == "📊 Dashboard Admin":
    if IS_MOBILE:
        render_admin_mobile()
    else:
        # --- LOGIC DESKTOP ---
        st.markdown("## 📊 Dashboard Admin")

        # 1. Cek Login Session
        if not st.session_state.get("is_admin"):
            # Tampilan Login (Jika belum login / sesi habis)
            c_login1, c_login2, c_login3 = st.columns([1, 1, 1])
            with c_login2:
                with st.container(border=True):
                    st.markdown("### 🔐 Login Admin")
                    # (Opsional: Bisa dihapus jika Login utama sudah via OTP di awal)
                    # Tapi dibiarkan sebagai fallback re-login tanpa refresh halaman
                    pwd = st.text_input("Password", type="password", key="desk_adm_pwd")
                    if st.button("Login Masuk", use_container_width=True, type="primary"):
                        if verify_admin_password(pwd):
                            st.session_state["is_admin"] = True
                            st.rerun()
                        else:
                            st.error("Password salah.")
        else:
            # =========================================================
            # DASHBOARD ADMIN DESKTOP (INTEGRASI FULL FITUR)
            # =========================================================
            
            # Helper untuk kategori aktivitas (Lokal Dashboard)
            def get_category_activity(val):
                val_str = str(val)
                keywords_digital = ["Digital", "Marketing", "Konten", "Ads", "Telesales", "Admin", "Follow"]
                if any(k in val_str for k in keywords_digital):
                    return "Digital/Internal"
                return "Kunjungan Lapangan"

            # 1. Load Data Utama
            staff_list = get_daftar_staf_terbaru()
            df_all = load_all_reports(staff_list)

            # 2. Pre-processing Data
            if not df_all.empty:
                try:
                    # Convert timestamp ke datetime objects
                    df_all[COL_TIMESTAMP] = pd.to_datetime(df_all[COL_TIMESTAMP], format="%d-%m-%Y %H:%M:%S", errors="coerce")
                    df_all["Tanggal_Date"] = df_all[COL_TIMESTAMP].dt.date
                    df_all["Kategori"] = df_all[COL_TEMPAT].apply(get_category_activity)
                except Exception:
                    df_all["Tanggal_Date"] = datetime.now(tz=TZ_JKT).date()
                    df_all["Kategori"] = "Umum"

            # 3. Navigasi Tab Dashboard (DINAMIS SESUAI ROLE)
            # -----------------------------------------------------------
            
            # Daftar Tab Standar
            tabs_labels = ["📈 Produktivitas", "🧲 Leads & Interest", "💬 Review & Feedback", "🖼️ Galeri Bukti", "📦 Master Data", "⚙️ Config Staff", "👥 Akun Staff", "⚡ SUPER EDITOR"]
            
            # Cek Role (Manager vs Admin)
            is_manager = (st.session_state.get("user_role") == "manager")
            
            # Jika Manager, sisipkan Tab Approval di paling depan
            if is_manager:
                tabs_labels.insert(0, "🔔 APPROVAL (ACC)")
            
            # Render Tabs
            all_tabs = st.tabs(tabs_labels)
            
            # Mapping Variabel Tab
            if is_manager:
                tab_acc = all_tabs[0]       # Index 0 = Approval
                tab_prod = all_tabs[1]
                tab_leads = all_tabs[2]
                tab_review = all_tabs[3]
                tab_galeri = all_tabs[4]
                tab_data = all_tabs[5]
                tab_config = all_tabs[6]
                tab_users = all_tabs[7]
                tab_super = all_tabs[8]     # Super Editor di akhir
            else:
                # Admin Biasa (Tanpa Approval Tab)
                tab_prod = all_tabs[0]
                tab_leads = all_tabs[1]
                tab_review = all_tabs[2]
                tab_galeri = all_tabs[3]
                tab_data = all_tabs[4]
                tab_config = all_tabs[5]
                tab_users = all_tabs[6]
                tab_super = all_tabs[7]

            # -----------------------------------------------------------
            # TAB KHUSUS: APPROVAL SYSTEM (Hanya Manager)
            # -----------------------------------------------------------
            if is_manager:
                with tab_acc:
                    st.markdown("### 🔔 Pusat Persetujuan (Manager)")
                    st.caption("Review detail perubahan (Sebelum vs Sesudah). Gunakan fitur Tolak dengan catatan jika perlu.")
                    
                    pending_data = get_pending_approvals()
                    
                    if not pending_data:
                        st.info("✅ Tidak ada permintaan pending.")
                    else:
                        # REJECTION DIALOG (Membutuhkan Streamlit versi baru st.experimental_dialog atau st.dialog)
                        # Jika versi lama, gunakan st.expander di dalam loop. 
                        # Di sini saya gunakan pendekatan session_state expander agar aman untuk semua versi.

                        for i, req in enumerate(pending_data):
                            with st.container(border=True):
                                # Header Card
                                c_h1, c_h2 = st.columns([3, 1])
                                with c_h1:
                                    st.markdown(f"👤 **{req['Requestor']}** | 📂 Sheet: `{req['Target Sheet']}`")
                                    st.text(f"📝 Alasan Admin: {req['Reason']}")
                                with c_h2:
                                    st.caption(f"🕒 {req['Timestamp']}")

                                st.divider()
                                
                                # --- LOGIC DIFF (DATA LAMA VS BARU) ---
                                try:
                                    old_d = json.loads(req.get("Old Data JSON", "{}"))
                                    new_d = json.loads(req.get("New Data JSON", "{}"))
                                    
                                    # Cari kolom yang berubah saja
                                    changes_table = []
                                    for k, v_new in new_d.items():
                                        v_old = old_d.get(k, "")
                                        # Normalisasi string biar gak false alarm
                                        if str(v_new).strip() != str(v_old).strip():
                                            changes_table.append({
                                                "Kolom": k,
                                                "🔴 Data Lama": str(v_old),
                                                "🟢 Data Baru": str(v_new)
                                            })
                                    
                                    if changes_table:
                                        st.markdown("**Detail Perubahan:**")
                                        st.table(pd.DataFrame(changes_table))
                                    else:
                                        st.warning("⚠️ Tidak terdeteksi perubahan data (mungkin hanya re-save).")
                                        # Tampilkan raw jika diff kosong (fallback)
                                        with st.expander("Lihat Data Mentah (JSON)"):
                                            st.json(new_d)
                                except Exception as e:
                                    st.error("Gagal memproses data JSON.")

                                # --- ACTION BUTTONS ---
                                col_act_space, col_act_btn = st.columns([3, 2])
                                
                                with col_act_btn:
                                    b_col1, b_col2 = st.columns(2)
                                    
                                    # TOMBOL TOLAK (Memicu Expander/Input)
                                    key_reject = f"btn_reject_show_{i}"
                                    if b_col1.button("❌ Tolak", key=key_reject, use_container_width=True):
                                        st.session_state[f"show_reject_input_{i}"] = True

                                    # TOMBOL ACC
                                    if b_col2.button("✅ ACC", key=f"btn_acc_{i}", type="primary", use_container_width=True):
                                        ok, msg = execute_approval(i, "APPROVE", admin_name=st.session_state["user_name"])
                                        if ok:
                                            st.success(msg)
                                            time.sleep(1)
                                            st.rerun()
                                        else:
                                            st.error(msg)
                                
                                # --- AREA INPUT ALASAN PENOLAKAN (Muncul jika tombol Tolak ditekan) ---
                                if st.session_state.get(f"show_reject_input_{i}", False):
                                    st.markdown("---")
                                    st.warning("Anda akan menolak permintaan ini.")
                                    with st.form(key=f"form_reject_{i}"):
                                        note = st.text_area("Catatan Penolakan (Opsional, tapi disarankan):", placeholder="Misal: Nominal salah, tolong cek lagi.")
                                        
                                        c_batal, c_confirm = st.columns(2)
                                        if c_batal.form_submit_button("Batal"):
                                            st.session_state[f"show_reject_input_{i}"] = False
                                            st.rerun()
                                            
                                        if c_confirm.form_submit_button("🚫 Konfirmasi Tolak", type="primary"):
                                            note_final = note if note.strip() else "Tidak ada catatan."
                                            ok, msg = execute_approval(i, "REJECT", admin_name=st.session_state["user_name"], rejection_note=note_final)
                                            if ok:
                                                st.success(f"Ditolak: {note_final}")
                                                st.session_state[f"show_reject_input_{i}"] = False
                                                time.sleep(1)
                                                st.rerun()

            # --- TAB 1: PRODUKTIVITAS ---
            with tab_prod:
                st.markdown("### 🚀 Analisa Produktivitas")
                
                if df_all.empty:
                    st.info("Belum ada data laporan.")
                else:
                    # Filter Rentang Waktu
                    c_filter1, c_filter2 = st.columns([1, 3])
                    with c_filter1:
                        days_opt = st.selectbox("Rentang Waktu:", [7, 14, 30, 60, 90], index=0)
                    
                    start_date = datetime.now(tz=TZ_JKT).date() - timedelta(days=days_opt)
                    df_filt = df_all[df_all["Tanggal_Date"] >= start_date].copy()

                    # Split Sales vs Digital
                    df_sales = df_filt[df_filt["Kategori"] == "Kunjungan Lapangan"]
                    df_digital = df_filt[df_filt["Kategori"] == "Digital/Internal"]

                    # 1. SALES STATS
                    with st.container(border=True):
                        st.markdown("#### 🚗 Performance Sales (Lapangan)")
                        k1, k2, k3 = st.columns(3)
                        k1.metric("Total Kunjungan", len(df_sales))
                        k2.metric("Sales Aktif", df_sales[COL_NAMA].nunique())
                        k3.metric("Rata-rata/Hari", f"{len(df_sales)/days_opt:.1f}")
                        
                        if not df_sales.empty:
                            st.bar_chart(df_sales[COL_NAMA].value_counts(), color="#16a34a") # Green

                    # 2. DIGITAL STATS
                    with st.container(border=True):
                        st.markdown("#### 💻 Performance Digital & Internal")
                        d1, d2, d3 = st.columns(3)
                        d1.metric("Total Output", len(df_digital))
                        d2.metric("Staf Aktif", df_digital[COL_NAMA].nunique())
                        d3.metric("Rata-rata/Hari", f"{len(df_digital)/days_opt:.1f}")

                        if not df_digital.empty:
                            if HAS_PLOTLY:
                                try:
                                    fig = px.pie(df_digital, names=COL_NAMA, title="Distribusi Beban Kerja Digital", hole=0.4)
                                    st.plotly_chart(fig, use_container_width=True)
                                except:
                                    st.bar_chart(df_digital[COL_NAMA].value_counts(), color="#facc15") # Yellow
                            else:
                                st.bar_chart(df_digital[COL_NAMA].value_counts(), color="#facc15") # Yellow

            # --- TAB 2: LEADS & INTEREST ---
            with tab_leads:
                st.markdown("### 🧲 Filter Data Klien (Leads)")
                st.caption("Download data klien berdasarkan tingkat ketertarikan (Interest).")

                if df_all.empty:
                    st.info("Data kosong.")
                else:
                    if COL_INTEREST not in df_all.columns:
                        st.warning("Kolom Interest belum tersedia di database.")
                    else:
                        st.session_state.setdefault("filter_interest_admin", "Under 50% (A)")
                        
                        # Button Filter
                        b1, b2, b3 = st.columns(3)
                        if b1.button("Tarik: Under 50% (A)", use_container_width=True):
                            st.session_state["filter_interest_admin"] = "Under 50% (A)"
                        if b2.button("Tarik: 50-75% (B)", use_container_width=True):
                            st.session_state["filter_interest_admin"] = "50-75% (B)"
                        if b3.button("Tarik: 75%-100%", use_container_width=True):
                            st.session_state["filter_interest_admin"] = "75%-100%"
                        
                        sel_interest = st.session_state["filter_interest_admin"]
                        st.success(f"📂 Menampilkan Filter: **{sel_interest}**")

                        # Filtering logic
                        df_leads = df_all.copy()
                        df_leads[COL_INTEREST] = df_leads[COL_INTEREST].astype(str).fillna("").str.strip()
                        df_filtered = df_leads[df_leads[COL_INTEREST] == sel_interest].copy()

                        with st.container(border=True):
                            st.success(f"📂 Menampilkan Data: **{sel_interest}** (Total: {len(df_leads)})")
                            
                            cols_display = [c for c in [COL_TIMESTAMP, COL_NAMA, COL_NAMA_KLIEN, COL_KONTAK_KLIEN, COL_TEMPAT, COL_DESKRIPSI, COL_KENDALA_KLIEN] if c in df_leads.columns]
                            st.dataframe(df_leads[cols_display], use_container_width=True, hide_index=True)

                            # Export Buttons
                            c_ex, c_csv = st.columns(2)
                            safe_name = sel_interest.replace("%", "").replace(" ", "_").replace("/", "")
                            
                            with c_ex:
                                if HAS_OPENPYXL:
                                    xb = df_to_excel_bytes(df_filtered[cols_display], sheet_name="Leads", wrap_cols=[COL_DESKRIPSI, COL_TEMPAT])
                                    if xb:
                                        st.download_button(f"⬇️ Excel ({sel_interest})", data=xb, file_name=f"leads_{safe_name}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
                            with c_csv:
                                csv_data = df_filtered[cols_display].to_csv(index=False).encode('utf-8')
                                st.download_button(f"⬇️ Download CSV ({sel_interest})", data=csv_data, file_name=f"leads_{safe_name}.csv", mime="text/csv", use_container_width=True)

            # --- TAB 3: REVIEW & FEEDBACK ---
            with tab_review:
                st.markdown("### 📝 Review Laporan & Kirim Feedback")
                st.caption("Monitoring detail kendala dan memberikan feedback langsung per laporan.")

                if df_all.empty:
                    st.info("Data kosong.")
                else:
                    # Sort by newest
                    df_rev = df_all.sort_values(by=COL_TIMESTAMP, ascending=False).head(50) # Limit 50 terbaru agar ringan
                    
                    for i, row in df_rev.iterrows():
                        with st.container(border=True):
                            # Header Card
                            c_head1, c_head2 = st.columns([4, 1])
                            with c_head1:
                                st.markdown(f"**{row.get(COL_NAMA, '-')}** | 📅 {row.get(COL_TIMESTAMP, '-')}")
                                st.caption(f"📍 {row.get(COL_TEMPAT, '-')} ({row.get('Kategori', '-')})")
                            with c_head2:
                                # Tampilkan Interest sebagai badge jika ada
                                intr = row.get(COL_INTEREST, "-")
                                if intr and intr != "-" and intr != "":
                                    st.markdown(f"🔥 `{intr}`")

                            st.markdown(f"📄 **Deskripsi:** {row.get(COL_DESKRIPSI, '-')}")
                            
                            # Info Klien
                            if row.get(COL_NAMA_KLIEN) not in ["-", ""]:
                                st.markdown(f"👤 **Klien:** {row.get(COL_NAMA_KLIEN)} | 📞 {row.get(COL_KONTAK_KLIEN)}")

                            st.divider()
                            
                            # 3 Kolom detail
                            rc1, rc2, rc3 = st.columns(3)
                            with rc1:
                                st.info(f"💡 **Hasil:**\n\n{row.get(COL_KESIMPULAN, '-')}")
                            with rc2:
                                st.warning(f"🚧 **Kendala:**\n\n{row.get(COL_KENDALA, '-')}")
                            with rc3:
                                st.error(f"📌 **Pending/Next:**\n\n{row.get(COL_PENDING, '-')}")

                            # Foto Bukti
                            link_foto = str(row.get(COL_LINK_FOTO, ""))
                            if "http" in link_foto:
                                with st.expander("🖼️ Lihat Bukti Foto"):
                                    direct_url = link_foto.replace("www.dropbox.com", "dl.dropboxusercontent.com").replace("?dl=0", "")
                                    st.image(direct_url, width=300)
                                    st.caption(f"Link: {link_foto}")

                            # Form Feedback
                            existing_fb = row.get(COL_FEEDBACK, "")
                            with st.expander(f"💬 Beri Feedback ({row.get(COL_NAMA)})", expanded=False):
                                uk = f"fb_desk_{i}_{row.get(COL_TIMESTAMP)}"
                                fb_in = st.text_area("Tulis Masukan/Arahan:", value=str(existing_fb), key=uk)
                                if st.button("Kirim Feedback 🚀", key=f"btn_{uk}"):
                                    ts_val = row.get(COL_TIMESTAMP)
                                    ts_str = ts_val.strftime("%d-%m-%Y %H:%M:%S") if hasattr(ts_val, "strftime") else str(ts_val)
                                    ok, msg = kirim_feedback_admin(row.get(COL_NAMA), ts_str, fb_in)
                                    if ok:
                                        st.success("Terkirim!")
                                        st.cache_data.clear()
                                    else:
                                        st.error(msg)

            # --- TAB 4: GALERI ---
            with tab_galeri:
                st.markdown("### 🖼️ Galeri Aktivitas Terbaru")
                if df_all.empty or COL_LINK_FOTO not in df_all.columns:
                    st.info("Belum ada foto.")
                else:
                    # Filter link http valid
                    df_foto = df_all[df_all[COL_LINK_FOTO].astype(str).str.contains("http", na=False, case=False)].sort_values(by=COL_TIMESTAMP, ascending=False).head(12)
                    
                    if df_foto.empty:
                        st.warning("Tidak ada data foto valid.")
                    else:
                        cols = st.columns(4)
                        for idx, row in enumerate(df_foto.to_dict("records")):
                            with cols[idx % 4]:
                                with st.container(border=True):
                                    url_asli = str(row.get(COL_LINK_FOTO, ""))
                                    direct_url = url_asli.replace("www.dropbox.com", "dl.dropboxusercontent.com").replace("?dl=0", "")
                                    try:
                                        st.image(direct_url, use_container_width=True)
                                        st.caption(f"**{row.get(COL_NAMA)}**\n\n{row.get(COL_TEMPAT)}")
                                        st.link_button("🔗 Buka", url_asli)
                                    except:
                                        st.error("Gagal load")

            # --- TAB 5: MASTER DATA ---
            with tab_data:
                st.markdown("### 📦 Data Mentah")
                if st.button("🔄 Refresh Data", key="refresh_master"):
                    st.cache_data.clear()
                    st.rerun()
                
                st.dataframe(df_all, use_container_width=True, hide_index=True)
                
                if HAS_OPENPYXL:
                    xb = df_to_excel_bytes(df_all, sheet_name="All_Reports")
                    if xb:
                        st.download_button("⬇️ Download Full Excel", data=xb, file_name="master_laporan.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

            # --- TAB 6: CONFIG & STAFF ---
            with tab_config:
                c_conf1, c_conf2 = st.columns(2)
                
                with c_conf1:
                    st.markdown("#### 👥 Manajemen Staf")
                    staff_df = pd.DataFrame({"Nama Staf": staff_list})
                    st.dataframe(staff_df, hide_index=True, use_container_width=True)
                    
                    with st.form("add_staff_admin"):
                        new_staff = st.text_input("Tambah Staf Baru")
                        if st.form_submit_button("Simpan Staf"):
                            if new_staff:
                                tambah_staf_baru(new_staff)
                                st.success("Tersimpan")
                                st.cache_data.clear()
                                st.rerun()
                
                with c_conf2:
                    st.markdown("#### ⚙️ Config Team")
                    df_team_cfg = load_team_config()
                    st.dataframe(df_team_cfg, hide_index=True, use_container_width=True)
                    
                    with st.form("add_team_admin"):
                        tm_name = st.text_input("Nama Team")
                        tm_pos = st.text_input("Posisi")
                        tm_mem = st.text_area("Anggota (1 per baris)")
                        if st.form_submit_button("Simpan Team"):
                            mem_list = [x.strip() for x in tm_mem.splitlines() if x.strip()]
                            ok, msg = tambah_team_baru(tm_name, tm_pos, mem_list)
                            if ok: 
                                st.success(msg)
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error(msg)
                                
            # --- TAB 7: AKUN STAFF (Username & Password - Legacy) ---
            with tab_users:
                st.markdown("### 👥 Manajemen Akun Staff")
                st.caption("Fitur legacy. Staff sekarang bisa masuk langsung tanpa password.")
                
                # Menampilkan tabel user hanya untuk referensi
                ws_u = init_user_db()
                if ws_u:
                    all_users = ws_u.get_all_records()
                    df_users = pd.DataFrame(all_users)
                    if "Password" in df_users.columns:
                        df_users["Password"] = "••••••"
                    st.dataframe(df_users, use_container_width=True, hide_index=True)
                else:
                    st.info("Tidak ada data akun.")

            # --- TAB 8: SUPER ADMIN EDITOR (FITUR KHUSUS ADMIN) ---
            with tab_super:
                st.markdown("### ⚡ Super Admin Data Editor")
                
                # 1. Pilih Sheet yang mau diedit
                sheet_options = {
                    "Laporan Harian": "Laporan Kegiatan Harian", 
                    "Target Team": SHEET_TARGET_TEAM,
                    "Target Individu": SHEET_TARGET_INDIVIDU,
                    "Closing Deal": SHEET_CLOSING_DEAL,
                    "Pembayaran": SHEET_PEMBAYARAN,
                    "📜 Global Audit Log": "Global_Audit_Log"
                }
                
                staff_list = get_daftar_staf_terbaru()
                for s in staff_list:
                    sheet_options[f"Laporan: {s}"] = s

                selected_label = st.selectbox("Pilih Data / Sheet:", list(sheet_options.keys()))
                target_sheet_name = sheet_options[selected_label]

                # 2. Load Data Existing
                if st.button("📂 Load Data", key="btn_load_super"):
                    st.session_state["super_df_old"] = None 
                    
                    try:
                        ws = spreadsheet.worksheet(target_sheet_name)
                        data = ws.get_all_records()
                        df = pd.DataFrame(data)
                        st.session_state["super_df_old"] = df.copy()
                        st.session_state["super_sheet_target"] = target_sheet_name
                    except Exception as e:
                        st.error(f"Gagal load sheet: {e}")

                # 3. Editor Interface & NOTIFIKASI STATUS
                if "super_df_old" in st.session_state and st.session_state["super_df_old"] is not None:
                    df_old = st.session_state["super_df_old"]
                    target_s = st.session_state["super_sheet_target"]
                    
                    # --- CEK STATUS TERAKHIR DARI AUDIT LOG ---
                    try:
                        from audit_service import load_audit_log
                        logs = load_audit_log(spreadsheet)
                        
                        status_alert = None
                        if not logs.empty:
                            # Filter log khusus sheet ini
                            logs_sheet = logs[logs["Nama Data / Sheet"] == target_s]
                            if not logs_sheet.empty:
                                # Ambil log paling baru
                                last_action = logs_sheet.iloc[0] 
                                action_type = str(last_action.get("Aksi Dilakukan", "")).upper()
                                reason_log = last_action.get("Alasan Perubahan", "-")
                                actor_log = last_action.get("Pelaku (User)", "Manager")
                                
                                if "REJECTED" in action_type:
                                     status_alert = {
                                         "type": "error",
                                         "msg": f"⛔ Perubahan terakhir DITOLAK oleh {actor_log}.",
                                         "detail": f"Catatan: {reason_log}"
                                     }
                                elif "APPROVED" in action_type:
                                     status_alert = {
                                         "type": "success",
                                         "msg": "✅ Perubahan terakhir SUDAH DISETUJUI.",
                                         "detail": f"Oleh: {actor_log}"
                                     }
                                elif "System_Pending" in str(target_s): 
                                     status_alert = {"type": "info", "msg": "Sedang Menunggu Persetujuan...", "detail": ""}
                    except ImportError:
                        status_alert = None 
                        pass

                    # TAMPILKAN ALERT STATUS DI ATAS EDITOR
                    if status_alert:
                        if status_alert["type"] == "error":
                            st.error(f"{status_alert['msg']}\n\n{status_alert['detail']}")
                        elif status_alert["type"] == "success":
                            st.success(f"{status_alert['msg']}")
                        elif status_alert["type"] == "info":
                             st.info(f"{status_alert['msg']}")
                    else:
                        st.info(f"Mengedit Sheet: **{st.session_state['super_sheet_target']}** ({len(df_old)} baris)")

                    # --- EDITOR ---
                    edit_reason = st.text_input("📝 Alasan Perubahan (Wajib diisi):", placeholder="Contoh: Koreksi typo nominal")

                    edited_df = st.data_editor(df_old, use_container_width=True, num_rows="dynamic", key="super_editor")

                    # 4. Tombol Simpan
                    current_role = st.session_state.get("user_role", "admin")
                    
                    if current_role == "manager":
                        btn_label = "💾 SIMPAN PERUBAHAN (LANGSUNG)"
                        btn_type = "primary"
                    else:
                        btn_label = "📤 AJUKAN PERUBAHAN (REQUEST ACC)"
                        btn_type = "primary"

                    if st.button(btn_label, type=btn_type, use_container_width=True):
                        if not edit_reason:
                            st.error("❌ Alasan perubahan wajib diisi!")
                        else:
                            # Deteksi Perubahan
                            changes = compare_and_get_changes(df_old, edited_df)
                            
                            if not changes:
                                st.warning("Tidak ada perubahan data yang terdeteksi.")
                            else:
                                # SKENARIO 1: MANAGER (Langsung Simpan)
                                if current_role == "manager":
                                    with st.spinner("Menyimpan ke Google Sheets & Mencatat Audit..."):
                                        try:
                                            # Update Google Sheets
                                            ws = spreadsheet.worksheet(st.session_state["super_sheet_target"])
                                            ws.clear()
                                            params = [edited_df.columns.values.tolist()] + edited_df.astype(str).values.tolist()
                                            ws.update(range_name="A1", values=params, value_input_option="USER_ENTERED")
                                            
                                            # Catat Log Audit
                                            real_actor = st.session_state.get("user_name") or "Manager"
                                            success_log = 0
                                            for chg in changes:
                                                real_row = chg['row_idx'] + 2 
                                                log_admin_action(
                                                    spreadsheet=spreadsheet,
                                                    actor=real_actor,
                                                    role="Manager (Super)",
                                                    feature="Super Editor",
                                                    target_sheet=st.session_state["super_sheet_target"],
                                                    row_idx=real_row,
                                                    action="UPDATE",
                                                    reason=edit_reason,
                                                    changes_dict=chg['diff']
                                                )
                                                success_log += 1
                                            
                                            st.success(f"✅ Berhasil! {success_log} baris data diperbarui.")
                                            st.session_state["super_df_old"] = edited_df.copy() # Update state
                                            
                                        except Exception as e:
                                            st.error(f"Terjadi kesalahan saat menyimpan: {e}")

                                # SKENARIO 2: ADMIN (Ajukan Request ke Pending List)
                                else:
                                    with st.spinner("Mengirim permintaan persetujuan ke Manager..."):
                                        success_count = 0
                                        for chg in changes:
                                            r_idx = chg['row_idx']
                                            
                                            # Data Baru (dari editor)
                                            new_row_data = edited_df.iloc[r_idx]

                                            # Data Lama (dari state awal) -- [PENGGABUNGAN CODE KEDUA]
                                            old_row_data = df_old.iloc[r_idx]
                                            
                                            # Kirim ke helper submit_change_request
                                            ok, msg = submit_change_request(
                                                target_sheet=st.session_state["super_sheet_target"],
                                                row_idx_0based=r_idx,
                                                new_df_row=new_row_data,
                                                old_df_row=old_row_data, # <--- Parameter tambahan
                                                reason=edit_reason,
                                                requestor=st.session_state["user_name"]
                                            )
                                            if ok: success_count += 1
                                        
                                        if success_count > 0:
                                            st.success(f"✅ {success_count} Perubahan berhasil diajukan! Menunggu ACC Manager.")
                                        else:
                                            st.error("Gagal mengajukan perubahan.")

            # Render watermark di luar tabs
            render_section_watermark()
