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

from audit_service import log_admin_action, compare_and_get_changes

# =========================================================
# [BARU] SISTEM LOGGING LANGSUNG (ANTI-GAGAL)
# =========================================================
# Ganti fungsi force_audit_log dengan ini


import threading

def _background_log_worker(actor, action, target_sheet, chat_msg, details_input):
    """Worker yang berjalan di background tanpa mengganggu UI."""
    try:
        ws = spreadsheet.worksheet("Global_Audit_Log")
        ts = datetime.now(ZoneInfo("Asia/Jakarta")).strftime("%d-%m-%Y %H:%M:%S")
        
        # Format detail
        final_details = str(details_input)[:4000] # Cegah error teks terlalu panjang
        if isinstance(details_input, dict):
            final_details = "\n".join([f"• {k}: {v}" for k, v in details_input.items()])

        row = [f"'{ts}", str(actor), str(action), str(target_sheet), str(chat_msg), final_details]
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:
        print(f"Log Error: {e}")

import threading

def _background_log_worker(actor, action, target_sheet, chat_msg, details_input):
    """Worker berjalan di background thread."""
    try:
        if not spreadsheet: return
        ws = spreadsheet.worksheet("Global_Audit_Log")
        ts = datetime.now(ZoneInfo("Asia/Jakarta")).strftime("%d-%m-%Y %H:%M:%S")
        final_details = str(details_input)[:4000] 
        if isinstance(details_input, dict):
            final_details = "\n".join([f"• {k}: {v}" for k, v in details_input.items()])
        row = [f"'{ts}", str(actor), str(action), str(target_sheet), str(chat_msg), final_details]
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:
        print(f"Log Background Error: {e}")

def force_audit_log(actor, action, target_sheet, chat_msg, details_input):
    """Langsung return True agar UI instan, log jalan sendiri."""
    threading.Thread(
        target=_background_log_worker, 
        args=(actor, action, target_sheet, chat_msg, details_input)
    ).start()
    return True


# =========================================================
# ANCHOR: HELPER APPROVAL (AMBIL DARI CODE KEDUA)
# =========================================================
SHEET_PENDING = "System_Pending_Approval"


def init_pending_db():
    try:
        try:
            ws = spreadsheet.worksheet(SHEET_PENDING)
            headers = ws.row_values(1)
            if "Old Data JSON" not in headers:
                current_cols = ws.col_count
                new_col_idx = len(headers) + 1
                if current_cols < new_col_idx:
                    ws.resize(cols=new_col_idx)
                ws.update_cell(1, new_col_idx, "Old Data JSON")
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(
                title=SHEET_PENDING, rows=1000, cols=7)
            headers = ["Timestamp", "Requestor", "Target Sheet",
                       "Row Index (0-based)", "New Data JSON", "Reason", "Old Data JSON"]
            ws.append_row(headers, value_input_option="USER_ENTERED")
            maybe_auto_format_sheet(ws, force=True)
        return ws
    except Exception as e:
        print(f"Error init_pending_db: {e}")
        return None


def submit_change_request(target_sheet, row_idx_0based, new_df_row, old_df_row, reason, requestor):
    ws = init_pending_db()
    if not ws:
        return False, "DB Error"
    row_dict_new = new_df_row.astype(str).to_dict()
    json_new = json.dumps(row_dict_new)
    row_dict_old = old_df_row.astype(
        str).to_dict() if old_df_row is not None else {}
    json_old = json.dumps(row_dict_old)
    ts = now_ts_str()
    ws.append_row([ts, requestor, target_sheet, row_idx_0based,
                  json_new, reason, json_old], value_input_option="USER_ENTERED")

    diff_log = {}
    for k, v_new in row_dict_new.items():
        v_old = row_dict_old.get(k, "")
        if str(v_new).strip() != str(v_old).strip():
            diff_log[k] = f"{v_old} ➡ {v_new}"
    diff_str = "\n".join(
        [f"{k}: {v}" for k, v in diff_log.items()]) if diff_log else "Re-save data."

    force_audit_log(actor=requestor, action="⏳ PENDING", target_sheet=target_sheet,
                    chat_msg=f"🙋‍♂️ [ADMIN]: {reason}", details_input=diff_str)
    return True, "Permintaan terkirim!"


def execute_approval(request_index_0based, action, admin_name="Manager", rejection_note="-"):
    try:
        ws_pending = init_pending_db()
        all_requests = ws_pending.get_all_records()
        if request_index_0based >= len(all_requests):
            return False, "Data tidak ditemukan."
        req = all_requests[request_index_0based]

        if action == "REJECT":
            force_audit_log(actor=admin_name, action="❌ DITOLAK",
                            target_sheet=req["Target Sheet"], chat_msg=f"⛔ [MANAGER]: {rejection_note}", details_input=f"Pengaju: {req['Requestor']}")
            ws_pending.delete_rows(request_index_0based + 2)
            return True, "Ditolak."

        elif action == "APPROVE":
            new_data_dict = json.loads(req["New Data JSON"])
            ws_target = spreadsheet.worksheet(req["Target Sheet"])
            headers = ws_target.row_values(1)
            row_values = [new_data_dict.get(h, "") for h in headers]
            gsheet_row = int(req["Row Index (0-based)"]) + 2
            ws_target.update(range_name=f"A{gsheet_row}", values=[
                             row_values], value_input_option="USER_ENTERED")
            force_audit_log(actor=admin_name, action="✅ SUKSES/ACC",
                            target_sheet=req["Target Sheet"], chat_msg="✅ [MANAGER]: Disetujui.", details_input=f"Pengaju: {req['Requestor']}")
            ws_pending.delete_rows(request_index_0based + 2)
            return True, "Disetujui."
    except Exception as e:
        return False, str(e)


def init_pending_db():
    """Memastikan sheet pending approval ada dengan kolom untuk DATA LAMA."""
    try:
        # Blok Try Dalam (Mencoba ambil worksheet)
        try:
            ws = spreadsheet.worksheet(SHEET_PENDING)
            # Cek apakah header sudah update (punya Old Data JSON)
            headers = ws.row_values(1)
            if "Old Data JSON" not in headers:
                # PERBAIKAN: Resize sheet dulu sebelum update cell di kolom baru
                current_cols = ws.col_count
                new_col_idx = len(headers) + 1
                if current_cols < new_col_idx:
                    ws.resize(cols=new_col_idx)  # Tambah kolom jika kurang

                ws.update_cell(1, new_col_idx, "Old Data JSON")

        except gspread.WorksheetNotFound:
            # Jika tidak ada, buat baru
            ws = spreadsheet.add_worksheet(
                title=SHEET_PENDING, rows=1000, cols=7)
            headers = ["Timestamp", "Requestor", "Target Sheet",
                       "Row Index (0-based)", "New Data JSON", "Reason", "Old Data JSON"]
            ws.append_row(headers, value_input_option="USER_ENTERED")
            maybe_auto_format_sheet(ws, force=True)

        return ws

    except Exception as e:  # <--- PASTIKAN BAGIAN INI ADA DAN SEJAJAR DENGAN TRY PERTAMA
        # Tampilkan error di terminal untuk debugging jika terjadi lagi
        print(f"Error init_pending_db: {e}")
        return None


def submit_change_request(target_sheet, row_idx_0based, new_df_row, old_df_row, reason, requestor):
    """
    UPDATE: Menggabungkan logic penyimpanan pending data dan pencatatan log (Audit Trail).
    Status: PENDING | Detail: List Perubahan (String) | Alasan: Format Chat Admin.
    """
    # --- 1. Inisialisasi Database Pending ---
    ws = init_pending_db()
    if not ws:
        return False, "DB Error"

    # --- 2. Persiapan Data (Konversi ke JSON) ---
    # Konversi row dataframe baru ke dictionary
    row_dict_new = new_df_row.astype(str).to_dict()
    json_new = json.dumps(row_dict_new)

    # Konversi row dataframe lama ke dictionary
    row_dict_old = old_df_row.astype(
        str).to_dict() if old_df_row is not None else {}
    json_old = json.dumps(row_dict_old)

    ts = now_ts_str()

    # --- 3. Simpan ke System_Pending_Approval ---
    # (Data ini wajib disimpan agar Manager bisa melihat data asli vs baru saat approval)
    ws.append_row(
        [ts, requestor, target_sheet, row_idx_0based, json_new, reason, json_old],
        value_input_option="USER_ENTERED"
    )

    # --- 4. Hitung Perbedaan (Diff Logic) ---
    diff_log = {}
    for k, v_new in row_dict_new.items():
        v_old = row_dict_old.get(k, "")
        # Normalisasi string agar tidak false alarm (spasi, dll)
        if str(v_new).strip() != str(v_old).strip():
            diff_log[k] = f"{v_old} ➡ {v_new}"

    # Flatten Dictionary ke String (agar muncul rapi di kolom 'Detail Perubahan')
    if not diff_log:
        diff_str = "Tidak ada perubahan data terdeteksi (Re-save)."
    else:
        # Join setiap item dengan enter (\n) agar rapi list ke bawah
        diff_str = "\n".join([f"{k}: {v}" for k, v in diff_log.items()])

    # --- 5. Format Chat & Catat Log (Revisi dari Code Kedua) ---

    # Format Chat Admin agar lebih interaktif di UI
    final_chat = f"🙋‍♂️ [ADMIN]: {reason}" if reason else "🙋‍♂️ [ADMIN]: Request Update Data."

    # Panggil fungsi logging yang baru
    force_audit_log(
        actor=requestor,
        action="⏳ PENDING",       # Status Jelas
        target_sheet=target_sheet,
        chat_msg=final_chat,       # Masuk ke kolom "Chat & Catatan"
        details_input=diff_str     # Masuk ke kolom "Detail Perubahan"
    )

    return True, "Permintaan terkirim & Log tercatat!"


def get_pending_approvals():
    """Fungsi untuk Manager mengambil semua daftar request yang pending."""
    ws = init_pending_db()
    if not ws:
        return []
    return ws.get_all_records()


def execute_approval(request_index_0based, action, admin_name="Manager", rejection_note="-"):
    """
    Eksekusi Approval dengan perbaikan Logging agar kolom Detail & Chat terisi lengkap.
    """
    try:
        ws_pending = init_pending_db()
        if not ws_pending:
            return False, "DB Error: Sheet Pending tidak ditemukan."

        all_requests = ws_pending.get_all_records()
        if request_index_0based >= len(all_requests):
            return False, "Data tidak ditemukan (mungkin sudah diproses)."

        req = all_requests[request_index_0based]
        target_sheet_name = req["Target Sheet"]
        row_target_idx = int(req["Row Index (0-based)"])
        requestor_name = req.get("Requestor", "Unknown")

        # --- [FIX] MENYUSUN ULANG DETAIL PERUBAHAN (DIFF) ---
        # Kita baca ulang JSON dari request pending agar log Manager memiliki detail data
        diff_str_log = ""
        try:
            raw_old = req.get("Old Data JSON", "{}")
            raw_new = req.get("New Data JSON", "{}")
            old_d = json.loads(raw_old) if raw_old else {}
            new_d = json.loads(raw_new) if raw_new else {}

            diff_list = []
            for k, v in new_d.items():
                old_v = old_d.get(k, "")
                if str(old_v).strip() != str(v).strip():
                    diff_list.append(f"• {k}: '{old_v}' ➡ '{v}'")

            diff_str_log = "\n".join(
                diff_list) if diff_list else "Re-save (Tanpa Perubahan Nilai)."
        except:
            diff_str_log = "Detail perubahan tidak terbaca."

        # --- ACTION: REJECT (DITOLAK) ---
        if action == "REJECT":
            final_reason = str(rejection_note).strip()
            # Jika admin tidak menulis alasan, beri default
            if not final_reason or final_reason in ["-", ""]:
                final_reason = "Data perlu direvisi."

            # LOG BARU: Menggunakan parameter 'chat_msg' dan 'details_input'
            force_audit_log(
                actor=admin_name,
                action="❌ DITOLAK",
                target_sheet=target_sheet_name,
                chat_msg=f"⛔ [MANAGER]: {final_reason}",
                details_input=f"Pengaju: {requestor_name}\n(Data dikembalikan ke Admin)"
            )

            # Hapus dari daftar pending karena sudah diproses
            ws_pending.delete_rows(request_index_0based + 2)
            return True, f"DITOLAK. Alasan: {final_reason}"

        # --- ACTION: APPROVE (DI ACC) ---
        elif action == "APPROVE":
            # 1. EKSEKUSI UPDATE DATA KE SHEET TARGET (Logika Asli)
            new_data_dict = json.loads(req["New Data JSON"])
            ws_target = spreadsheet.worksheet(target_sheet_name)
            headers = ws_target.row_values(1)

            # Mapping data baru sesuai urutan header di sheet target
            row_values = [new_data_dict.get(h, "") for h in headers]

            # Update baris di Google Sheet (Write)
            gsheet_row = row_target_idx + 2
            ws_target.update(range_name=f"A{gsheet_row}", values=[
                             row_values], value_input_option="USER_ENTERED")

            # 2. LOG BARU: Mencatat Sukses dengan detail perubahan
            force_audit_log(
                actor=admin_name,
                action="✅ SUKSES/ACC",
                target_sheet=target_sheet_name,
                chat_msg="✅ [MANAGER]: Disetujui & Data Terupdate.",
                details_input=f"Pengaju: {requestor_name}\n---\n{diff_str_log}"
            )

            # Hapus dari daftar pending karena sudah diproses
            ws_pending.delete_rows(request_index_0based + 2)
            return True, "DISETUJUI & Database Terupdate."

    except Exception as e:
        return False, f"System Error: {e}"


# --- BAGIAN IMPORT OPTIONAL LIBS JANGAN DIHAPUS (Excel/AgGrid/Plotly) ---
# Bagian ini dipertahankan dari Code Pertama untuk menjaga kompatibilitas arsitektur
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
    st.markdown("<h1 style='text-align: center;'>🔐 Access Portal</h1>",
                unsafe_allow_html=True)
    st.markdown(
        f"<p style='text-align: center;'>{APP_TITLE}</p>", unsafe_allow_html=True)
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
                # Nama spesifik nanti dipilih di dalam form
                st.session_state["user_name"] = "Staff Member"
                st.session_state["user_role"] = "staff"
                # KUNCI: Staff tidak bisa akses dashboard admin
                st.session_state["is_admin"] = False

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
                                st.error(
                                    "Gagal kirim email (Cek Config SMTP).")
                        else:
                            st.error(
                                "⛔ Akses Ditolak: Email tidak terdaftar sebagai Admin/Manager.")

            # Step 2: Input OTP
            elif st.session_state.get("otp_step") == 2:
                st.info(
                    f"Kode dikirim ke: **{st.session_state['temp_email']}**")

                with st.form("otp_form"):
                    otp_input = st.text_input(
                        "Kode OTP (6 Digit)", max_chars=6)
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
                            role_str = str(user_info.get(
                                "role", "staff")).lower()
                            st.session_state["user_role"] = role_str

                            # Tentukan Flag Admin: True jika role adalah 'admin' ATAU 'manager'
                            # Ini memberikan akses ke menu Dashboard Admin untuk kedua role tersebut
                            if role_str in ["admin", "manager"]:
                                st.session_state["is_admin"] = True
                            else:
                                st.session_state["is_admin"] = False

                            st.success(
                                f"Login Berhasil! Selamat datang {role_str.upper()}.")
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
            ws.append_row(["Username", "Password", "Nama", "Role"],
                          value_input_option="USER_ENTERED")
            maybe_auto_format_sheet(ws, force=True)
        return ws
    except Exception:
        return None


def check_staff_login(username, password):
    """Cek login untuk staff biasa via GSheet."""
    ws = init_user_db()
    if not ws:
        return None

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
    if not ws:
        return False, "DB Error"

    existing_users = ws.col_values(1)
    if username in existing_users:
        return False, "Username sudah dipakai!"

    ws.append_row([username, password, nama, "staff"],
                  value_input_option="USER_ENTERED")
    return True, "Akun berhasil dibuat."


def update_staff_account(username_lama, new_password=None, new_name=None):
    """Fitur Edit Akun Staff (Ganti Password / Nama)."""
    ws = init_user_db()
    if not ws:
        return False, "DB Error"

    try:
        cell = ws.find(username_lama)
        row = cell.row
        if new_password and new_password.strip():
            ws.update_cell(row, 2, new_password)  # Kolom 2 = Password
        if new_name and new_name.strip():
            ws.update_cell(row, 3, new_name)  # Kolom 3 = Nama
        return True, f"Data user {username_lama} berhasil diperbarui."
    except gspread.exceptions.CellNotFound:
        return False, "Username tidak ditemukan."
    except Exception as e:
        return False, str(e)


def delete_staff_account(username):
    """Admin menghapus akun staff."""
    ws = init_user_db()
    if not ws:
        return False, "DB Error"

    try:
        cell = ws.find(username)
        ws.delete_rows(cell.row)
        return True, f"User {username} dihapus."
    except gspread.exceptions.CellNotFound:
        return False, "Username tidak ditemukan."


# =========================================================
# 1. DEFINISI FUNGSI KONEKSI & ASSETS (TARUH PALING ATAS)
# =========================================================

@st.cache_resource(ttl=None, show_spinner=False)
def init_connections():
    """Inisialisasi koneksi berat hanya SEKALI saat server start."""
    gs_obj = None
    dbx_obj = None
    
    # Setup Google Sheets
    try:
        if "gcp_service_account" in st.secrets:
            scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            creds_dict = dict(st.secrets["gcp_service_account"])
            if "private_key" in creds_dict:
                creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            gc = gspread.authorize(creds)
            gs_obj = gc.open(NAMA_GOOGLE_SHEET)
    except Exception as e:
        print(f"⚠️ GSheet Init Error: {e}")

    # Setup Dropbox
    try:
        if "dropbox" in st.secrets and "access_token" in st.secrets["dropbox"]:
            dbx_obj = dropbox.Dropbox(st.secrets["dropbox"]["access_token"])
            dbx_obj.users_get_current_account()
    except Exception as e:
        print(f"⚠️ Dropbox Init Error: {e}")
        
    return gs_obj, dbx_obj

@st.cache_data(ttl=None, show_spinner=False)
def load_data_ke_ram(sheet_name):
    """Mengambil data dari GSheet dan menguncinya di RAM."""
    try:
        if spreadsheet:
            ws = spreadsheet.worksheet(sheet_name)
            return pd.DataFrame(ws.get_all_records())
    except Exception as e:
        print(f"Gagal Load {sheet_name} ke RAM: {e}")
    return pd.DataFrame()

def prefetch_all_data_to_state():
    """Memindahkan data dari RAM ke Session State setelah login sukses."""
    if "data_loaded" not in st.session_state:
        st.session_state["df_payment"] = load_data_ke_ram(SHEET_PEMBAYARAN)
        st.session_state["df_closing"] = load_data_ke_ram(SHEET_CLOSING_DEAL)
        st.session_state["df_staf"] = get_daftar_staf_terbaru()
        st.session_state["data_loaded"] = True

# =========================================================
# 2. EKSEKUSI KONEKSI GLOBAL
# =========================================================

# Jalankan koneksi sekarang agar variabel 'spreadsheet' tersedia untuk fungsi lain
spreadsheet, dbx = init_connections()
KONEKSI_GSHEET_BERHASIL = (spreadsheet is not None)
KONEKSI_DROPBOX_BERHASIL = (dbx is not None)

# =========================================================
# 3. LOGIKA LOGIN & ALUR UTAMA (MAIN FLOW)
# =========================================================

if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False

# Halaman Login
if not st.session_state["logged_in"]:
    login_page()
    st.stop() 

# --- JIKA SUDAH LOGIN, KODE DI BAWAH INI AKAN BERJALAN ---

# 1. Kunci data ke RAM (Instan)
prefetch_all_data_to_state()

# 2. Suntik CSS dan Tampilkan Header (Pastikan fungsi ini sudah didefinisikan di atas)
inject_global_css_fast() 
render_header()

# 3. Inisialisasi Variabel User Global
user_email = st.session_state["user_email"]
user_name = st.session_state["user_name"]
user_role = st.session_state["user_role"]

# =========================================================
# 4. OPTIONAL LIBRARIES (LOAD SETELAH LOGIN AGAR RINGAN)
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
            color: var(--gold) !important;  /* ganti ke var(--text) kalau mau putih */
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
            border-top-color: var(--gold) !important;    /* Warna Emas */
            border-right-color: var(--green) !important; /* Warna Hijau */
            border-bottom-color: var(--gold) !important; /* Warna Emas */
            border-left-color: transparent !important;
            width: 3.5rem !important;  /* Ukuran icon lebih besar */
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
SHEET_PRESENSI = "Presensi_Kehadiran"
PRESENSI_COLUMNS = ["Timestamp", "Nama", "Hari",
                    "Tanggal", "Bulan", "Tahun", "Waktu"]


def init_presensi_db():
    """Memastikan sheet presensi tersedia."""
    try:
        try:
            ws = spreadsheet.worksheet(SHEET_PRESENSI)
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(
                title=SHEET_PRESENSI, rows=2000, cols=len(PRESENSI_COLUMNS))
            ws.append_row(PRESENSI_COLUMNS, value_input_option="USER_ENTERED")
            maybe_auto_format_sheet(ws, force=True)
        return ws
    except Exception:
        return None


def catat_presensi(nama_staf):
    """Logika utama presensi: Otomatis, Real-time, No-edit."""
    ws = init_presensi_db()
    if not ws:
        return False, "Database Presensi Error"

    # 1. Ambil Waktu Real-Time (WIB)
    now = datetime.now(TZ_JKT)

    # Mapping Hari Indonesia
    hari_map = {
        "Monday": "Senin", "Tuesday": "Selasa", "Wednesday": "Rabu",
        "Thursday": "Kamis", "Friday": "Jumat", "Saturday": "Sabtu", "Sunday": "Minggu"
    }

    ts_full = now.strftime("%d-%m-%Y %H:%M:%S")
    hari = hari_map.get(now.strftime("%A"), now.strftime("%A"))
    tanggal = now.strftime("%d")
    bulan = now.strftime("%B")
    tahun = now.strftime("%Y")
    waktu = now.strftime("%H:%M:%S")

    # 2. Cek Duplikasi (Opsional: Cegah absen 2x di hari yang sama)
    # Jika ingin membolehkan absen berkali-kali, bagian ini bisa dihapus
    records = ws.get_all_records()
    today_str = now.strftime("%d-%m-%Y")
    for r in records:
        if str(r.get("Nama")) == nama_staf and today_str in str(r.get("Timestamp")):
            return False, f"Anda sudah melakukan presensi hari ini pada {r.get('Waktu')}."

    # 3. Masukkan Data (User tidak input manual, semua dari sistem)
    row = [f"'{ts_full}", nama_staf, hari, tanggal, bulan, tahun, waktu]
    ws.append_row(row, value_input_option="USER_ENTERED")

    return True, f"Berhasil! Presensi tercatat pukul {waktu} WIB."


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

# =========================================================
# DEFINISI KOLOM DATA (DATABASE STRUCTURE)
# =========================================================

# --- 1. Closing Deal Columns ---
COL_GROUP = "Nama Group"
COL_MARKETING = "Nama Marketing"
COL_TGL_EVENT = "Tanggal Event"
COL_BIDANG = "Bidang"
COL_NILAI_KONTRAK = "Nilai Kontrak"  # disimpan sebagai angka (int)

CLOSING_COLUMNS = [
    COL_GROUP, 
    COL_MARKETING, 
    COL_TGL_EVENT, 
    COL_BIDANG, 
    COL_NILAI_KONTRAK
]

# --- 2. Target / Checklist Columns ---
TEAM_CHECKLIST_COLUMNS = [
    "Misi", "Tgl_Mulai", "Tgl_Selesai", "Status", 
    "Bukti/Catatan", COL_TS_UPDATE, COL_UPDATED_BY
]
INDIV_CHECKLIST_COLUMNS = [
    "Nama", "Target", "Tgl_Mulai", "Tgl_Selesai", "Status", 
    "Bukti/Catatan", COL_TS_UPDATE, COL_UPDATED_BY
]

# --- 3. Smart Pembayaran Columns (Update Khusus) ---
COL_TS_BAYAR = "Timestamp Input"
COL_NILAI_KESEPAKATAN = "Total Nilai Kesepakatan" # [NEW]
COL_JENIS_BAYAR = "Jenis Pembayaran"             # DP, Cicilan, atau Cash
COL_NOMINAL_BAYAR = "Nominal Pembayaran"         # Nominal yang masuk saat ini
COL_TENOR_CICILAN = "Tenor (Bulan)"              # [NEW]
COL_SISA_BAYAR = "Sisa Pembayaran"               # [NEW] Kalkulator Otomatis
COL_JATUH_TEMPO = "Batas Waktu Bayar"
COL_STATUS_BAYAR = "Status Pembayaran"           # Deskripsi status (Lunas/Belum)
COL_BUKTI_BAYAR = "Bukti Pembayaran"
COL_CATATAN_BAYAR = "Catatan"

PAYMENT_COLUMNS = [
    COL_TS_BAYAR,
    COL_GROUP,
    COL_MARKETING,
    COL_TGL_EVENT,
    COL_NILAI_KESEPAKATAN, # Letakkan total di awal agar alur logika jelas
    COL_JENIS_BAYAR,
    COL_NOMINAL_BAYAR,
    COL_TENOR_CICILAN,
    COL_SISA_BAYAR,
    COL_JATUH_TEMPO,
    COL_STATUS_BAYAR,
    COL_BUKTI_BAYAR,
    COL_CATATAN_BAYAR,
    COL_TS_UPDATE,
    COL_UPDATED_BY
]

# --- 4. System Config ---
TZ_JKT = ZoneInfo("Asia/Jakarta")

# Formatting throttling (menghindari API overload saat batch update)
FORMAT_THROTTLE_SECONDS = 300  # 5 minutes

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
            ua = (headers.get("user-agent")
                  or headers.get("User-Agent") or "").lower()
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

# =========================================================
# [MIGRASI] PEMBAYARAN LOGIC HELPERS
# =========================================================
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
        else: out.append(ln)
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
    actor, ts = (safe_str(actor, "-").strip() or "-"), (safe_str(ts, now_ts_str()).strip() or now_ts_str())
    lines.append(f"[{ts}] ({actor}) {changes[0]}")
    for c in changes[1:]: lines.append(f" {c}")
    return build_numbered_log(lines)


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


def dynamic_column_mapper(df):
    mapping = {}
    # Tambahkan keyword yang mungkin ada di GSheet Anda
    keywords = {
        "Waktu": "Waktu",
        "Pelaku": "User",
        "User": "User",
        "Aksi": "Status",     # Menangkap "Aksi Dilakukan"
        "Status": "Status",   # Menangkap "Status"
        "Nama Data": "Target Data",
        "Target": "Target Data",
        "Sheet": "Target Data",
        "Alasan": "Chat & Catatan",
        "Chat": "Chat & Catatan",
        "Rincian": "Detail Perubahan",
        "Detail": "Detail Perubahan"
    }

    for col in df.columns:
        for key, standard_name in keywords.items():
            if key.lower() in str(col).lower():
                mapping[col] = standard_name
                break

    return df.rename(columns=mapping)

# =========================================================
# [BARU] DYNAMIC UI HELPERS (Anti-Crash & Auto-Type)
# =========================================================

def clean_df_types_dynamically(df: pd.DataFrame) -> pd.DataFrame:
    """
    Versi Perbaikan: Menggunakan datetime64[ns] dan sinkronisasi keyword 
    untuk menghindari StreamlitAPIException akibat ketidakcocokan tipe data.
    """
    df_clean = df.copy()
    for col in df_clean.columns:
        col_lower = col.lower()
        
        # 1. Kolom Numerik: Pastikan murni angka dan isi NaN dengan 0
        # Menambahkan 'tenor' agar konsisten dengan NumberColumn
        if any(key in col_lower for key in ["nilai", "nominal", "sisa", "kontrak", "sepakat", "tenor"]):
            # Konversi rupiah string ke int, lalu paksa ke numeric murni
            df_clean[col] = pd.to_numeric(
                df_clean[col].apply(lambda x: parse_rupiah_to_int(x) if isinstance(x, str) else x), 
                errors='coerce'
            ).fillna(0)
            
        # 2. Kolom Tanggal: Gunakan tipe datetime pandas asli
        elif any(key in col_lower for key in ["tanggal", "tempo", "waktu"]):
            if not any(k in col_lower for k in ["log", "update", "timestamp"]):
                # PERBAIKAN: Hapus .dt.date. 
                # Pandas menyimpan .dt.date sebagai tipe 'object', yang memicu error di st.data_editor.
                # Biarkan tetap bertipe datetime64[ns].
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
                
    return df_clean

def generate_dynamic_column_config(df):
    """
    Versi Perbaikan: Sinkron dengan fungsi clean agar tipe data kompatibel.
    """
    config = {}
    for col in df.columns:
        col_lower = col.lower()
        
        # Numerik: Gunakan NumberColumn untuk kolom yang sudah di-clean jadi angka
        if any(key in col_lower for key in ["nilai", "nominal", "sisa", "kontrak", "sepakat", "tenor"]):
            config[col] = st.column_config.NumberColumn(col, format="Rp %d", min_value=0)
            
        # Tanggal: Gunakan DateColumn untuk kolom datetime64[ns]
        elif any(key in col_lower for key in ["tanggal", "tempo", "waktu"]):
            if "timestamp" not in col_lower:
                config[col] = st.column_config.DateColumn(col, format="DD/MM/YYYY")
            else:
                config[col] = st.column_config.TextColumn(col, disabled=True)
                
        # Status: Gunakan CheckboxColumn jika data berisi Boolean (True/False)
        elif "status" in col_lower:
            config[col] = st.column_config.CheckboxColumn(col)
            
        else:
            config[col] = st.column_config.TextColumn(col)
            
    return config


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
# CONNECTIONS (PERSISTENT IN RAM)
# =========================================================
@st.cache_resource(ttl=None, show_spinner=False) # Simpan selamanya di RAM
def init_connections():
    """Inisialisasi koneksi berat hanya SEKALI saat server start."""
    gs_obj = None
    dbx_obj = None
    
    # 1. Setup Google Sheets
    try:
        if "gcp_service_account" in st.secrets:
            scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            creds_dict = dict(st.secrets["gcp_service_account"])
            if "private_key" in creds_dict:
                creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            gc = gspread.authorize(creds)
            gs_obj = gc.open(NAMA_GOOGLE_SHEET)
            
            # Pre-load Audit Sheet agar tidak perlu dicek lagi nanti
            from audit_service import ensure_audit_sheet
            try: ensure_audit_sheet(gs_obj)
            except: pass
    except Exception as e:
        print(f"⚠️ GSheet Init Error: {e}")

    # 2. Setup Dropbox
    try:
        if "dropbox" in st.secrets and "access_token" in st.secrets["dropbox"]:
            dbx_obj = dropbox.Dropbox(st.secrets["dropbox"]["access_token"])
            dbx_obj.users_get_current_account()
    except Exception as e:
        print(f"⚠️ Dropbox Init Error: {e}")
        
    return gs_obj, dbx_obj

# Load Global Connections dari Cache
KONEKSI_GSHEET_BERHASIL = (spreadsheet is not None)
KONEKSI_DROPBOX_BERHASIL = (dbx is not None)

# === Konfigurasi AI Robust (Tiruan Proyek Telesales) ===
SDK = "new"
try:
    from google import genai as genai_new
    from google.genai import types as types_new
except Exception:
    SDK = "legacy"
    import google.generativeai as genai_legacy

# === Konfigurasi AI Robust (Tiruan Proyek Telesales) ===
SDK = "new"
try:
    from google import genai as genai_new
    from google.genai import types as types_new
except Exception:
    SDK = "legacy"
    import google.generativeai as genai_legacy

# AMBIL DARI SECRETS (SANGAT AMAN)
API_KEY = st.secrets.get("gemini_api_key", "")

# Daftar model cadangan
MODEL_FALLBACKS = ["gemini-2.0-flash", "gemini-1.5-flash"]

if SDK == "new":
    client_ai = genai_new.Client(api_key=API_KEY)
else:
    genai_legacy.configure(api_key=API_KEY)
# Daftar model cadangan agar tidak muncul pesan "berhalangan" jika satu model error
MODEL_FALLBACKS = ["gemini-2.5-flash", "gemini-2.0-flash"]

if SDK == "new":
    client_ai = genai_new.Client(api_key=API_KEY)
else:
    genai_legacy.configure(api_key=API_KEY)

def prefetch_all_data():
    """
    Memuat SEMUA data sheet ke RAM (Session State) sekaligus.
    Hanya jalan 1x saat login. Menu lain tinggal baca RAM (0 detik).
    """
    if "data_buffered" not in st.session_state:
        placeholder = st.empty()
        placeholder.info("🚀 Sedang menyiapkan Database ke RAM (High Speed Mode)...")
        
        # Load Payment
        st.session_state["buf_payment"] = load_pembayaran_dp()
        # Load Closing
        st.session_state["buf_closing"] = load_closing_deal()
        # Load KPI
        st.session_state["buf_kpi_team"] = load_checklist(SHEET_TARGET_TEAM, TEAM_CHECKLIST_COLUMNS)
        st.session_state["buf_kpi_indiv"] = load_checklist(SHEET_TARGET_INDIVIDU, INDIV_CHECKLIST_COLUMNS)
        # Load Staf
        st.session_state["buf_staf"] = get_daftar_staf_terbaru()
        
        st.session_state["data_buffered"] = True
        placeholder.empty()

def force_refresh_buffer():
    """Hapus buffer untuk paksa download ulang."""
    keys = ["buf_payment", "buf_closing", "buf_kpi_team", "buf_kpi_indiv", "buf_staf", "data_buffered"]
    for k in keys:
        if k in st.session_state: del st.session_state[k]
    st.cache_data.clear()
    st.rerun()

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
            ts, actor, rest = m.group(1).strip(), m.group(
                2).strip(), m.group(3).strip()
            prefix = f"[{ts}] ({actor})"

            if rest:
                parts = [p.strip() for p in rest.split(";") if p.strip()]
                if parts:
                    out.append(f"{prefix} {parts[0]}")
                    for p in parts[1:]:
                        out.append(f" {p}")  # indent
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
    changes = [safe_str(c, "").strip()
               for c in (changes or []) if safe_str(c, "").strip()]
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
            lambda x: "" if x is None or pd.isna(
                x) else format_rupiah_display(x)
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
                st.session_state[k] = datetime.now(
                    tz=TZ_JKT).date() + timedelta(days=7)
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
        cell.alignment = Alignment(
            horizontal="center", vertical="center", wrap_text=True)

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
            ws.column_dimensions[col_letter].width = min(
                max(10, max_len + 2), 60)

        for cell in ws[col_letter][1:]:
            wrap = col_name in wrap_cols
            horiz = "right" if col_name in right_align_cols else "left"
            cell.alignment = Alignment(
                vertical="top", horizontal=horiz, wrap_text=wrap)

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
    """
    Throttled formatting: Mencegah pemanggilan fungsi formatting yang berat 
    terlalu sering untuk menghemat kuota API dan menjaga performa.
    """
    try:
        # 1. Validasi keberadaan worksheet
        if worksheet is None:
            return
            
        # 2. Inisialisasi penyimpanan waktu terakhir format di session state
        if "_fmt_sheet_last" not in st.session_state:
            st.session_state["_fmt_sheet_last"] = {}

        now = time.time()
        
        # 3. Identifikasi sheet menggunakan ID unik (atau 'unknown')
        # gspread worksheet object memiliki atribut 'id'
        key = str(getattr(worksheet, "id", "unknown"))
        
        # Ambil waktu terakhir sheet ini diformat
        last = float(st.session_state["_fmt_sheet_last"].get(key, 0))
        
        # 4. Cek apakah harus menjalankan auto_format
        # Dijalankan jika: dipaksa (force=True) ATAU selisih waktu > batas throttle
        if force or (now - last) > FORMAT_THROTTLE_SECONDS:
            auto_format_sheet(worksheet)
            
            # Update timestamp terakhir kali berhasil format
            st.session_state["_fmt_sheet_last"][key] = now
            
    except Exception:
        # Fitur pengaman: Error pada formatting tidak boleh membuat 
        # seluruh aplikasi crash/berhenti.
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
        default_body_format = {
            "verticalAlignment": "TOP", "wrapStrategy": "CLIP"}

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
                cell_format_override["numberFormat"] = _build_currency_number_format_rupiah(
                )

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
            worksheet.update(range_name="A1", values=[
                             desired_headers], value_input_option="USER_ENTERED")
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
        ws = spreadsheet.add_worksheet(
            title=nama_worksheet, rows=200, cols=len(NAMA_KOLOM_STANDAR))
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
            ws = spreadsheet.add_worksheet(
                title=SHEET_CONFIG_NAMA, rows=100, cols=1)
            ws.append_row(["Daftar Nama Staf"],
                          value_input_option="USER_ENTERED")
            ws.append_row(["Saya"], value_input_option="USER_ENTERED")
            maybe_auto_format_sheet(ws, force=True)
            return default_staf

        nama_list = ws.col_values(1)
        if nama_list and nama_list[0] == "Daftar Nama Staf":
            nama_list.pop(0)

        return nama_list if nama_list else default_staf
    except Exception:
        return default_staf


def hapus_staf_by_name(nama_staf):
    """Menghapus nama staf dari worksheet Config_Staf."""
    try:
        ws = spreadsheet.worksheet(SHEET_CONFIG_NAMA)
        # Cari sel yang berisi nama tersebut
        cell = ws.find(nama_staf)
        if cell:
            ws.delete_rows(cell.row)
            return True, f"Staf '{nama_staf}' berhasil dihapus."
        return False, "Nama staf tidak ditemukan di database."
    except Exception as e:
        return False, f"Gagal menghapus: {e}"


def tambah_staf_baru(nama_baru):
    try:
        try:
            ws = spreadsheet.worksheet(SHEET_CONFIG_NAMA)
        except Exception:
            ws = spreadsheet.add_worksheet(
                title=SHEET_CONFIG_NAMA, rows=100, cols=1)

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
            ws = spreadsheet.add_worksheet(
                title=SHEET_CONFIG_TEAM, rows=300, cols=len(TEAM_COLUMNS))
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
            ws = spreadsheet.add_worksheet(
                title=SHEET_CONFIG_TEAM, rows=300, cols=len(TEAM_COLUMNS))
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

        clean_filename = "".join(
            [c for c in file_obj.name if c.isalnum() or c in (".", "_")])
        clean_user_folder = "".join(
            [c for c in nama_staf if c.isalnum() or c in (" ", "_")]).replace(" ", "_")
        clean_kategori = "".join(
            [c for c in kategori if c.isalnum() or c in (" ", "_")]).replace(" ", "_")

        path = f"{FOLDER_DROPBOX}/{clean_user_folder}/{clean_kategori}/{ts}_{clean_filename}"
        dbx.files_upload(file_data, path, mode=dropbox.files.WriteMode.add)

        settings = SharedLinkSettings(
            requested_visibility=RequestedVisibility.public)
        try:
            link = dbx.sharing_create_shared_link_with_settings(
                path, settings=settings)
        except ApiError as e:
            if e.error.is_shared_link_already_exists():
                link = dbx.sharing_list_shared_links(
                    path, direct_only=True).links[0]
            else:
                return "-"

        return link.url.replace("?dl=0", "?raw=1")
    except Exception:
        return "-"


# =========================================================
# TARGET / CHECKLIST HELPERS
# =========================================================
def clean_bulk_input(text_input):
    # GUNAKAN \n (satu backslash), bukan \\n
    lines = (text_input or "").split("\n") 
    cleaned_targets = []
    for line in lines:
        # Regex ini akan menghapus angka "1.", "2.", "-", atau "*" di awal baris
        cleaned = re.sub(r"^[\d\.\-\*\s]+", "", line).strip()
        if cleaned:
            cleaned_targets.append(cleaned)
    return cleaned_targets


@st.cache_data(ttl=3600)
def load_checklist(sheet_name, columns):
    try:
        try:
            ws = spreadsheet.worksheet(sheet_name)
        except Exception:
            ws = spreadsheet.add_worksheet(
                title=sheet_name, rows=200, cols=len(columns))
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
            df["Status"] = df["Status"].apply(
                lambda x: True if str(x).upper() == "TRUE" else False)

        return df[columns].copy()
    except Exception:
        return pd.DataFrame(columns=columns)


def save_checklist(sheet_name, df, columns):
    try:
        ws = spreadsheet.worksheet(sheet_name)
        # Tidak perlu ensure_headers setiap saat jika struktur stabil
        
        # Siapkan data
        df_save = df.copy().fillna("")
        for c in columns: # Pastikan kolom lengkap
            if c not in df_save.columns: df_save[c] = ""
        
        # Konversi boolean ke string explicit
        if "Status" in df_save.columns:
            df_save["Status"] = df_save["Status"].apply(lambda x: "TRUE" if x is True or str(x).upper()=='TRUE' else "FALSE")

        # Gabungkan Header + Data
        data_body = [columns] + df_save.astype(str).values.tolist()
        
        # UPDATE CERDAS: Hanya update area yang diperlukan (A1 sampai Z_sekian)
        num_rows = len(data_body)
        num_cols = len(columns)
        range_sq = f"A1:{gspread.utils.rowcol_to_a1(num_rows, num_cols)}"
        
        ws.update(range_name=range_sq, values=data_body, value_input_option="USER_ENTERED")
        
        # Hapus sisa baris kosong di bawah (Cleanup) sesekali saja
        if ws.row_count > num_rows + 50:
             ws.resize(rows=num_rows + 10)

        # Matikan auto-format agar simpan instan (< 1 detik)
        # maybe_auto_format_sheet(ws) 
        
        return True
    except Exception as e:
        print(f"Save Error: {e}")
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
            link_bukti = upload_ke_dropbox(
                file_obj, user_folder_name, kategori=kategori_folder)

        catatan_lama = str(df.at[row_idx_pandas, "Bukti/Catatan"]
                           ) if "Bukti/Catatan" in df.columns else ""
        if catatan_lama in {"-", "nan"}:
            catatan_lama = ""

        ts_update = now_ts_str()
        actor = safe_str(user_folder_name, "-").strip() or "-"

        update_text = f"[{ts_update}] "
        if note:
            update_text += f"{note}. "
        if link_bukti and link_bukti != "-":
            update_text += f"[FOTO: {link_bukti}]"

        final_note = f"{catatan_lama}\\n{update_text}" if catatan_lama.strip(
        ) else update_text
        final_note = final_note.strip() if final_note.strip() else "-"
        final_note = final_note.strip() if final_note.strip() else "-"

        headers = ws.row_values(1)
        if "Bukti/Catatan" not in headers:
            return False, "Kolom Bukti error."

        updates = []

        # Bukti/Catatan
        col_idx_gsheet = headers.index("Bukti/Catatan") + 1
        cell_address = gspread.utils.rowcol_to_a1(
            row_idx_gsheet, col_idx_gsheet)
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
                gb.configure_column(
                    main_text_col, wrapText=True, autoHeight=True, width=400, editable=False)

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
        column_config["Status"] = st.column_config.CheckboxColumn(
            "Done?", width="small")
    if main_text_col in df_data.columns:
        column_config[main_text_col] = st.column_config.TextColumn(
            main_text_col, disabled=True, width="large")
    if "Bukti/Catatan" in df_data.columns:
        column_config["Bukti/Catatan"] = st.column_config.TextColumn(
            "Bukti/Note", width="large")
    if COL_TS_UPDATE in df_data.columns:
        column_config[COL_TS_UPDATE] = st.column_config.TextColumn(
            COL_TS_UPDATE, disabled=True, width="large")
    if COL_UPDATED_BY in df_data.columns:
        column_config[COL_UPDATED_BY] = st.column_config.TextColumn(
            COL_UPDATED_BY, disabled=True, width="medium")

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

    tab1, tab2, tab3, tab4 = st.tabs(
        ["📌 Aktivitas", "🏁 Kesimpulan", "📇 Kontak", "✅ Submit"])

    # ===== TAB 1: Aktivitas =====
    with tab1:
        kategori_aktivitas = st.radio(
            "Jenis Aktivitas",
            ["🚗 Sales (Kunjungan Lapangan)", "💻 Digital Marketing / Konten / Ads",
             "📞 Telesales / Follow Up", "🏢 Lainnya"],
            horizontal=False,
            key="m_kategori"
        )
        is_kunjungan = kategori_aktivitas.startswith("🚗")

        if "Digital Marketing" in kategori_aktivitas:
            st.text_input("Link Konten / Ads / Drive (Opsional)",
                          key="m_sosmed")

        if is_kunjungan:
            st.text_input(
                "📍 Nama Klien / Lokasi Kunjungan (Wajib)", key="m_lokasi")
        else:
            st.text_input("Jenis Tugas", value=kategori_aktivitas,
                          disabled=True, key="m_tugas")

        fotos = st.file_uploader(
            "Upload Bukti (opsional)",
            accept_multiple_files=True,
            disabled=not KONEKSI_DROPBOX_BERHASIL,
            key="m_fotos"
        )

        # 1 deskripsi saja agar ringkas (bisa detail per file via expander)
        st.text_area("Deskripsi Aktivitas (Wajib)",
                     height=120, key="m_deskripsi")

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
        st.text_input("📌 Next Plan / Pending (Reminder Besok)",
                      key="m_pending")

# ===== TAB 4: Submit =====
    with tab4:
        st.caption("Pastikan data sudah benar, lalu submit.")

        if st.button("✅ Submit Laporan", type="primary", use_container_width=True):

            # --- 1. SIAPKAN VARIABEL DATA ---
            kategori_aktivitas = st.session_state.get("m_kategori", "")
            is_kunjungan = str(kategori_aktivitas).startswith("🚗")
            lokasi_input = st.session_state.get(
                "m_lokasi", "") if is_kunjungan else kategori_aktivitas
            main_deskripsi = st.session_state.get("m_deskripsi", "")
            sosmed_link = st.session_state.get(
                "m_sosmed", "") if "Digital Marketing" in str(kategori_aktivitas) else ""
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
            my_bar = progress_placeholder.progress(
                0, text="🚀 Memulai proses...")

            try:
                # Siapkan data timestamp & string lain
                ts = now_ts_str()
                val_kesimpulan = (st.session_state.get(
                    "m_kesimpulan") or "-").strip() or "-"
                val_kendala = (st.session_state.get(
                    "m_kendala") or "-").strip() or "-"
                val_kendala_klien = (st.session_state.get(
                    "m_kendala_klien") or "-").strip() or "-"
                val_pending = (st.session_state.get(
                    "m_pending") or "-").strip() or "-"
                val_feedback = ""
                val_interest = st.session_state.get("interest_persen") or "-"
                val_nama_klien = (st.session_state.get(
                    "nama_klien_input") or "-").strip() or "-"
                val_kontak_klien = (st.session_state.get(
                    "kontak_klien_input") or "-").strip() or "-"

                rows = []
                final_lokasi = lokasi_input if is_kunjungan else kategori_aktivitas

                # --- 4. PROSES UPLOAD FOTO (LOOPING) ---
                if fotos and KONEKSI_DROPBOX_BERHASIL:
                    for i, f in enumerate(fotos):
                        # Update Persentase Progress Bar
                        # (Contoh: Foto 1 dari 3 => 33%)
                        pct = float(current_step / total_steps)
                        # Pastikan pct tidak lebih dari 1.0
                        if pct > 1.0:
                            pct = 1.0

                        my_bar.progress(
                            pct, text=f"📤 Mengupload foto ke-{i+1} dari {jml_foto}...")

                        # Eksekusi Upload (Berat)
                        url = upload_ke_dropbox(
                            f, nama_pelapor, "Laporan_Harian")

                        # Ambil deskripsi per foto jika ada
                        desc = st.session_state.get(
                            f"m_desc_{i}", "") or main_deskripsi or "-"

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
                if pct_save > 0.95:
                    pct_save = 0.95  # Biarkan sisa sedikit untuk efek selesai

                my_bar.progress(
                    pct_save, text="💾 Menyimpan data ke Database...")

                # Eksekusi Simpan (Berat)
                ok = simpan_laporan_harian_batch(rows, nama_pelapor)

                # --- 6. FINISHING ---
                # Set bar ke 100%
                my_bar.progress(1.0, text="✅ Selesai!")
                time.sleep(0.8)  # Jeda sebentar agar user lihat status 100%
                progress_placeholder.empty()  # Hapus bar agar bersih

                if ok:
                    st.success(
                        f"✅ Laporan tersimpan! Reminder: **{val_pending}**")
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
            ws = spreadsheet.add_worksheet(
                title=SHEET_CLOSING_DEAL, rows=300, cols=len(CLOSING_COLUMNS))
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
        nama_marketing = str(nama_marketing).strip(
        ) if nama_marketing is not None else ""
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
            ws = spreadsheet.add_worksheet(
                title=SHEET_CLOSING_DEAL, rows=300, cols=len(CLOSING_COLUMNS))
            ws.append_row(CLOSING_COLUMNS, value_input_option="USER_ENTERED")

        ensure_headers(ws, CLOSING_COLUMNS)

        tgl_str = tanggal_event.strftime(
            "%Y-%m-%d") if hasattr(tanggal_event, "strftime") else str(tanggal_event)

        ws.append_row([nama_group, nama_marketing, tgl_str, bidang, int(
            nilai_int)], value_input_option="USER_ENTERED")

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
            ws = spreadsheet.add_worksheet(
                title=SHEET_PEMBAYARAN, rows=500, cols=len(PAYMENT_COLUMNS))
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
            df[COL_STATUS_BAYAR] = df[COL_STATUS_BAYAR].apply(
                lambda x: True if str(x).upper() == "TRUE" else False)

        if COL_JATUH_TEMPO in df.columns:
            df[COL_JATUH_TEMPO] = pd.to_datetime(
                df[COL_JATUH_TEMPO], errors="coerce").dt.date

        for c in [COL_TS_BAYAR, COL_GROUP, COL_MARKETING, COL_TGL_EVENT, COL_JENIS_BAYAR,
                  COL_BUKTI_BAYAR, COL_CATATAN_BAYAR, COL_TS_UPDATE, COL_UPDATED_BY]:
            if c in df.columns:
                df[c] = df[c].fillna("").astype(str)

        # rapihkan log agar tampil bernomor & multiline
        if COL_TS_UPDATE in df.columns:
            df[COL_TS_UPDATE] = df[COL_TS_UPDATE].apply(
                lambda x: build_numbered_log(parse_payment_log_lines(x)))

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

        df_save[COL_STATUS_BAYAR] = df_save[COL_STATUS_BAYAR].apply(
            lambda x: "TRUE" if bool(x) else "FALSE")

        def _to_int_or_blank(x):
            if x is None or pd.isna(x):
                return ""
            val = parse_rupiah_to_int(x)
            return "" if val is None else int(val)

        df_save[COL_NOMINAL_BAYAR] = df_save[COL_NOMINAL_BAYAR].apply(
            _to_int_or_blank)

        def _fmt_date(d):
            if d is None or pd.isna(d):
                return ""
            if hasattr(d, "strftime"):
                return d.strftime("%Y-%m-%d")
            s = str(d).strip()
            return s if s and s.lower() not in {"nan", "none"} else ""

        df_save[COL_JATUH_TEMPO] = df_save[COL_JATUH_TEMPO].apply(_fmt_date)

        df_save[COL_TS_UPDATE] = df_save[COL_TS_UPDATE].apply(
            lambda x: build_numbered_log(parse_payment_log_lines(x)))
        df_save[COL_UPDATED_BY] = df_save[COL_UPDATED_BY].apply(
            lambda x: safe_str(x, "-").strip() or "-")

        df_save = df_save[PAYMENT_COLUMNS].fillna("")
        data_to_save = [df_save.columns.values.tolist()] + \
            df_save.values.tolist()

        ws.update(range_name="A1", values=data_to_save,
                  value_input_option="USER_ENTERED")
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
            oldlog = after.at[i,
                              COL_TS_UPDATE] if COL_TS_UPDATE in after.columns else ""
            after.at[i, COL_TS_UPDATE] = append_payment_ts_update(
                oldlog, ts, actor, ["Data diperbarui (fallback)"])
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
                oldlog = build_numbered_log(
                    [safe_str(row.get(COL_TS_BAYAR, ts), ts)])
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
                    changes.append(
                        f"Status Pembayaran: {_fmt_payment_val_for_log(col, oldv)} → {_fmt_payment_val_for_log(col, newv)}")
            elif col == COL_JATUH_TEMPO:
                if normalize_date(oldv) != normalize_date(newv):
                    changes.append(
                        f"Jatuh Tempo: {_fmt_payment_val_for_log(col, oldv)} → {_fmt_payment_val_for_log(col, newv)}")
            elif col == COL_NOMINAL_BAYAR:
                if parse_rupiah_to_int(oldv) != parse_rupiah_to_int(newv):
                    changes.append(
                        f"Nominal: {_fmt_payment_val_for_log(col, oldv)} → {_fmt_payment_val_for_log(col, newv)}")
            else:
                if safe_str(oldv, "").strip() != safe_str(newv, "").strip():
                    changes.append(
                        f"{col}: {_fmt_payment_val_for_log(col, oldv)} → {_fmt_payment_val_for_log(col, newv)}")

        if changes:
            oldlog = safe_str(prev.get(COL_TS_UPDATE, ""), "")
            newlog = append_payment_ts_update(oldlog, ts, actor, changes)
            after_idx.at[key, COL_TS_UPDATE] = newlog
            after_idx.at[key, COL_UPDATED_BY] = actor

    return after_idx.reset_index(drop=True)


def tambah_pembayaran_dp(nama_group, nama_marketing, tgl_event, jenis_bayar, nominal_input, total_sepakat_input, tenor, jatuh_tempo, bukti_file, catatan):
    """
    Menambah record pembayaran dengan sistem Smart Balance Tracking dan kalkulator cicilan transparan.
    """
    if not KONEKSI_GSHEET_BERHASIL: 
        return False, "Sistem Error: Koneksi Google Sheets tidak aktif."

    try:
        group = str(nama_group).strip() if nama_group else "-"
        marketing = str(nama_marketing).strip() if nama_marketing else "Unknown"
        catatan_clean = str(catatan).strip() if catatan else "-"
        
        nom_bayar = parse_rupiah_to_int(nominal_input) or 0
        total_sepakat = parse_rupiah_to_int(total_sepakat_input) or 0
        tenor_val = int(tenor) if tenor else 0
        
        if total_sepakat <= 0:
            return False, "Input Gagal: Total nilai kesepakatan harus diisi dengan benar."

        sisa_bayar = total_sepakat - nom_bayar
        
        info_cicilan = ""
        if tenor_val > 0 and sisa_bayar > 0:
            nilai_per_cicilan = sisa_bayar / tenor_val
            info_cicilan = f" | Cicilan: {format_rupiah_display(nilai_per_cicilan)} x{tenor_val} term"

        if sisa_bayar <= 0:
            status_fix = "✅ Lunas"
            if jenis_bayar == "Cash": 
                status_fix += " (Cash)"
        else:
            if jenis_bayar == "Down Payment (DP)":
                status_fix = f"⏳ DP (Sisa: {format_rupiah_display(sisa_bayar)}){info_cicilan}"
            elif jenis_bayar == "Cicilan":
                status_fix = f"💳 Cicilan (Sisa: {format_rupiah_display(sisa_bayar)}){info_cicilan}"
            else:
                status_fix = f"⚠️ Belum Lunas (Sisa: {format_rupiah_display(sisa_bayar)}){info_cicilan}"

        link_bukti = "-"
        if bukti_file and KONEKSI_DROPBOX_BERHASIL:
            link_bukti = upload_ke_dropbox(bukti_file, marketing, kategori="Bukti_Pembayaran")

        ts_in = now_ts_str()
        
        fmt_tgl_event = tgl_event.strftime("%Y-%m-%d") if hasattr(tgl_event, "strftime") else str(tgl_event)
        fmt_jatuh_tempo = jatuh_tempo.strftime("%Y-%m-%d") if hasattr(jatuh_tempo, "strftime") else str(jatuh_tempo)

        log_entry = f"[{ts_in}] Input Baru: {jenis_bayar}{info_cicilan}"

        row = [
            ts_in,
            group,
            marketing,
            fmt_tgl_event,
            total_sepakat,
            jenis_bayar,
            nom_bayar,
            tenor_val,
            sisa_bayar,
            fmt_jatuh_tempo,
            status_fix,
            link_bukti,
            catatan_clean,
            build_numbered_log([log_entry]),
            marketing
        ]

        ws = spreadsheet.worksheet(SHEET_PEMBAYARAN)
        ensure_headers(ws, PAYMENT_COLUMNS)
        
        ws.append_row(row, value_input_option="USER_ENTERED")
        
        msg_feedback = f"Pembayaran berhasil disimpan! "
        if sisa_bayar > 0:
            msg_feedback += f"Sisa tagihan: {format_rupiah_display(sisa_bayar)} dengan rincian {info_cicilan.replace(' | ', '')}."
        else:
            msg_feedback += "Status: LUNAS."

        return True, msg_feedback

    except Exception as e:
        return False, f"System Error: {str(e)}"


def build_alert_pembayaran(df: pd.DataFrame, days_due_soon: int = 3):
    """
    Sistem Alert Pintar (Hybrid): 
    Mendeteksi tagihan berdasarkan Sisa Pembayaran > 0 dan Tanggal Jatuh Tempo.
    Mempertahankan fleksibilitas parameter 'days_due_soon' dari kode lama.
    """
    # 1. Validasi awal: Jika data kosong, kembalikan dataframe kosong dengan kolom yang benar
    if df is None or df.empty:
        cols = df.columns if df is not None else PAYMENT_COLUMNS
        return pd.DataFrame(columns=cols), pd.DataFrame(columns=cols)

    today = datetime.now(tz=TZ_JKT).date()
    
    # 2. Copy data agar manipulasi tipe data tidak merusak data utama di memori
    df_alert = df.copy()

    # 3. Normalisasi Tipe Data (Penting untuk kalkulator sisa)
    # Pastikan Jatuh Tempo adalah objek date dan Sisa Pembayaran adalah angka
    if COL_JATUH_TEMPO in df_alert.columns:
        df_alert[COL_JATUH_TEMPO] = pd.to_datetime(df_alert[COL_JATUH_TEMPO], errors="coerce").dt.date
    
    if COL_SISA_BAYAR in df_alert.columns:
        df_alert[COL_SISA_BAYAR] = pd.to_numeric(df_alert[COL_SISA_BAYAR], errors='coerce').fillna(0)
    else:
        # Fallback jika kolom sisa belum ada (masa transisi), buat sisa 0 agar tidak alert
        df_alert[COL_SISA_BAYAR] = 0

    # 4. KRITERIA PINTAR: Filter hanya data yang benar-benar belum lunas (Sisa > 0)
    # Fitur lama 'COL_STATUS_BAYAR == False' otomatis terwakili oleh 'Sisa > 0'
    df_tagihan_aktif = df_alert[
        (df_alert[COL_SISA_BAYAR] > 0) & 
        (pd.notna(df_alert[COL_JATUH_TEMPO]))
    ].copy()

    # Jika tidak ada tagihan aktif (semua sudah lunas), kembalikan DF kosong
    if df_tagihan_aktif.empty:
        return pd.DataFrame(columns=df.columns), pd.DataFrame(columns=df.columns)

    # 5. PEMBAGIAN KATEGORI (Overdue vs Due Soon)
    
    # Kategori A: Overdue (Sisa > 0 DAN Tanggal sudah terlewat)
    overdue = df_tagihan_aktif[df_tagihan_aktif[COL_JATUH_TEMPO] < today].copy()
    
    # Kategori B: Due Soon (Sisa > 0 DAN Tanggal mendekati/hari ini)
    # Menggunakan parameter 'days_due_soon' agar fitur lama tetap berfungsi
    due_soon = df_tagihan_aktif[
        (df_tagihan_aktif[COL_JATUH_TEMPO] >= today) & 
        (df_tagihan_aktif[COL_JATUH_TEMPO] <= (today + timedelta(days=days_due_soon)))
    ].copy()

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

        link = upload_ke_dropbox(
            file_obj, nama_marketing or "Unknown", kategori="Bukti_Pembayaran")
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
    

# =========================================================
# [MIGRASI] CORE DATABASE & AUDIT PAYMENTS
# =========================================================
def apply_audit_payments_changes(df_before: pd.DataFrame, df_after: pd.DataFrame, actor: str):
    actor = safe_str(actor, "-").strip() or "-"
    before = df_before.copy() if df_before is not None else pd.DataFrame()
    after = df_after.copy()
    for c in [COL_TS_UPDATE, COL_UPDATED_BY]:
        if c not in after.columns: after[c] = ""
    
    if before.empty:
        ts = now_ts_str()
        for i in range(len(after)):
            after.at[i, COL_TS_UPDATE] = build_numbered_log([ts])
            after.at[i, COL_UPDATED_BY] = actor
        return after

    before_idx = before.set_index(COL_TS_BAYAR, drop=False)
    after_idx = after.set_index(COL_TS_BAYAR, drop=False)
    watched_cols = [COL_JENIS_BAYAR, COL_NOMINAL_BAYAR, COL_JATUH_TEMPO, COL_STATUS_BAYAR, COL_BUKTI_BAYAR, COL_CATATAN_BAYAR]
    ts = now_ts_str()

    for key, row in after_idx.iterrows():
        if key not in before_idx.index: continue
        prev = before_idx.loc[key]
        if isinstance(prev, pd.DataFrame): prev = prev.iloc[0]
        changes = []
        for col in watched_cols:
            if col not in after_idx.columns: continue
            oldv, newv = prev[col], row[col]
            if col == COL_STATUS_BAYAR:
                if normalize_bool(oldv) != normalize_bool(newv):
                    changes.append(f"Status: {_fmt_payment_val_for_log(col, oldv)} → {_fmt_payment_val_for_log(col, newv)}")
            elif col == COL_NOMINAL_BAYAR:
                if parse_rupiah_to_int(oldv) != parse_rupiah_to_int(newv):
                    changes.append(f"Nominal: {_fmt_payment_val_for_log(col, oldv)} → {_fmt_payment_val_for_log(col, newv)}")
            elif col == COL_JATUH_TEMPO:
                if normalize_date(oldv) != normalize_date(newv):
                    changes.append(f"Jatuh Tempo: {_fmt_payment_val_for_log(col, oldv)} → {_fmt_payment_val_for_log(col, newv)}")
            else:
                if safe_str(oldv).strip() != safe_str(newv).strip():
                    changes.append(f"{col}: {oldv} → {newv}")

        if changes:
            oldlog = safe_str(prev.get(COL_TS_UPDATE, ""), "")
            after_idx.at[key, COL_TS_UPDATE] = append_payment_ts_update(oldlog, ts, actor, changes)
            after_idx.at[key, COL_UPDATED_BY] = actor
    return after_idx.reset_index(drop=True)

@st.cache_resource(show_spinner=False)
def get_cached_assets():
    """
    Membaca semua file fisik (Disk) hanya 1x saat VPS start, 
    lalu menyimpannya di RAM dalam format Base64 & String.
    """
    assets = {
        "logo_left": _img_to_base64(LOGO_LEFT),
        "logo_right": _img_to_base64(LOGO_RIGHT),
        "logo_holding": _img_to_base64(LOGO_HOLDING),
        "hero_bg": _img_to_base64(HERO_BG),
        "global_css": """
            <style>
            /* Copy seluruh isi CSS dari fungsi inject_global_css Anda di sini */
            @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700;800&display=swap');
            :root{ --bg0:#020805; --green:#16a34a; --gold:#facc15; }
            /* ... (lanjutkan sisa CSS Anda) ... */
            </style>
        """
    }
    return assets

# Panggil aset ke variabel global (Sekali jalan, langsung masuk RAM)
RAM_ASSETS = get_cached_assets()

def inject_global_css_fast():
    """Ganti fungsi inject_global_css lama dengan ini agar instan."""
    st.markdown(RAM_ASSETS["global_css"], unsafe_allow_html=True)


def render_header():
    ts_now = datetime.now(tz=TZ_JKT).strftime("%d %B %Y %H:%M:%S")

    # MENGAMBIL DATA DARI RAM VPS (INSTAN)
    left_b64 = RAM_ASSETS["logo_left"]
    right_b64 = RAM_ASSETS["logo_right"]
    holding_b64 = RAM_ASSETS["logo_holding"]
    bg_b64 = RAM_ASSETS["hero_bg"]

    g_on = bool(KONEKSI_GSHEET_BERHASIL)
    d_on = bool(KONEKSI_DROPBOX_BERHASIL)

    def pill(label: str, on: bool):
        cls = "sx-pill on" if on else "sx-pill off"
        return f"<span class='{cls}'><span class='sx-dot'></span>{label}</span>"

    # Style background hero menggunakan data base64 dari RAM
    hero_style = (
        f"--hero-bg: url('data:image/jpeg;base64,{bg_b64}'); "
        f"--hero-bg-pos: 50% 72%; "
        f"--hero-bg-size: 140%;"
    ) if bg_b64 else "--hero-bg: none;"

    # Menyiapkan elemen HTML Logo Kiri & Kanan
    left_html = f"<img src='data:image/png;base64,{left_b64}' alt='Logo EO' />" if left_b64 else ""
    right_html = f"<img src='data:image/png;base64,{right_b64}' alt='Logo Training' />" if right_b64 else ""

    # Menyiapkan elemen HTML Logo Holding (Paling Atas)
    top_logo_html = ""
    if holding_b64:
        top_logo_html = f"""
        <div style="display: flex; justify-content: center; margin-bottom: 25px; padding-top: 10px;">
            <img src='data:image/png;base64,{holding_b64}'
                 alt='Holding Logo'
                 style="height: 100px; width: auto; object-fit: contain; filter: drop-shadow(0 5px 15px rgba(0,0,0,0.5));" />
        </div>
        """

    # Menggabungkan seluruh komponen ke dalam satu template HTML Hero
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
        {"key": "report",  "icon": "📝", "title": "Laporan Harian",
            "sub": "Input aktivitas + reminder"},
        {"key": "kpi",     "icon": "🎯", "title": "Target & KPI",
            "sub": "Checklist team & individu"},
        {"key": "closing", "icon": "🤝", "title": "Closing Deal",
            "sub": "Catat deal + export"},
        {"key": "payment", "icon": "💳", "title": "Pembayaran",
            "sub": "DP/Termin/Pelunasan + jatuh tempo"},
        {"key": "log",     "icon": "📜", "title": "Global Audit Log",
            "sub": "Riwayat perubahan data (Super Admin)"},
        {"key": "admin",   "icon": "🔐", "title": "Akses Admin",
            "sub": "Dashboard + kontrol (butuh login)"},
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

# Update: Menambahkan entry 'presensi' ke dalam Mapping
NAV_MAP = {
    "home": HOME_NAV,
    "presensi": "📅 Presensi",
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
            st.query_params["nav"] = [nav_key]
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
    # Mobile masuk Beranda, Desktop tetap ke Laporan Harian
    st.session_state["menu_nav"] = HOME_NAV if IS_MOBILE else "📝 Laporan Harian"

# Sinkronkan kalau URL ada ?nav=...
nav_from_url = _get_query_nav()
if nav_from_url in NAV_MAP:
    st.session_state["menu_nav"] = NAV_MAP[nav_from_url]

# Render header
render_header()

if st.session_state.get("logged_in"):
    prefetch_all_data() # Pastikan data siap di RAM

# MOBILE: tampilkan Beranda sebagai landing page
menu_nav = st.session_state.get(
    "menu_nav", HOME_NAV if IS_MOBILE else "📝 Laporan Harian")

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

    st.markdown("<div class='sx-section-title'>Navigation</div>",
                unsafe_allow_html=True)

    # Update: Menambahkan "📅 Presensi" di daftar menu utama sidebar
    menu_items = [
        "📅 Presensi",
        "📝 Laporan Harian",
        "🎯 Target & KPI",
        "🤝 Closing Deal",
        "💳 Pembayaran",
        "📜 Global Audit Log",
    ]

    if st.session_state.get("is_admin"):
        menu_items.append("📊 Dashboard Admin")

    # SpaceX-like nav buttons
    st.markdown("<div class='sx-nav'>", unsafe_allow_html=True)
    for i, item in enumerate(menu_items):
        active = (st.session_state.get("menu_nav") == item)
        btype = "primary" if active else "secondary"
        if st.button(item, use_container_width=True, type=btype, key=f"nav_{i}"):
            st.session_state["menu_nav"] = item
            # Sync URL query param saat menu diklik
            nav_k = [k for k, v in NAV_MAP.items() if v == item]
            if nav_k:
                try:
                    if hasattr(st, "query_params"):
                        st.query_params["nav"] = nav_k[0]
                    else:
                        st.experimental_set_query_params(nav=nav_k[0])
                except:
                    pass
            st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)
    # -----------------------------------------------------------
    # PROFIL USER (OTP LOGIN)
    # -----------------------------------------------------------
    st.divider()

    col_p1, col_p2 = st.columns([1, 3])

    with col_p1:
        # Icon default karena OTP tidak ambil foto profil Google
        st.markdown("👤")

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
        overdue_s, due_soon_s = build_alert_pembayaran(
            df_pay_sidebar, days_due_soon=3) if not df_pay_sidebar.empty else (pd.DataFrame(), pd.DataFrame())
        st.markdown("<div class='sx-section-title'>Quick Stats</div>",
                    unsafe_allow_html=True)
        st.metric("Overdue Payment", int(len(overdue_s))
                  if overdue_s is not None else 0)
        st.metric("Due ≤ 3 hari", int(len(due_soon_s))
                  if due_soon_s is not None else 0)
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
                final_df = apply_audit_checklist_changes(
                    df_team, edited_team, ["Misi"], actor)
                if save_checklist(SHEET_TARGET_TEAM, final_df, TEAM_CHECKLIST_COLUMNS):
                    st.success("Tersimpan!")
                    st.rerun()

            st.divider()

            # 2. Upload Bukti (Fitur Desktop dibawa ke HP)
            with st.expander("📂 Upload Bukti / Catatan"):
                sel_misi = st.selectbox(
                    "Pilih Misi", df_team["Misi"].unique(), key="mob_sel_misi")
                note_misi = st.text_area("Catatan", key="mob_note_misi")
                file_misi = st.file_uploader("File", key="mob_file_misi")

                if st.button("Update Bukti", use_container_width=True, key="mob_upd_team"):
                    actor = get_actor_fallback()
                    res, msg = update_evidence_row(
                        SHEET_TARGET_TEAM, sel_misi, note_misi, file_misi, actor, "Team")
                    if res:
                        st.success("Updated!")
                        st.rerun()
                    else:
                        st.error(msg)
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
            # --- TAMBAHKAN LOGIKA PROGRES DI SINI ---
            total_target = len(df_user)
            # Menghitung jumlah 'TRUE' pada kolom Status
            jumlah_selesai = df_user["Status"].sum() 
            
            # Hitung persentase
            persentase = (jumlah_selesai / total_target) if total_target > 0 else 0
            
            # Tampilkan Progress Bar yang Estetik
            st.markdown(f"### 📈 Progres Kerja: {int(persentase * 100)}%")
            st.progress(persentase)
            st.write(f"Selesai: **{jumlah_selesai}** dari **{total_target}** tugas.")
            st.divider()
            # --- END LOGIKA PROGRES ---

            edited_indiv = render_hybrid_table(df_user, f"mob_indiv_{filter_nama}", "Target")

            if st.button(f"💾 Simpan ({filter_nama})", use_container_width=True, key="mob_save_indiv"):
                df_merged = df_indiv_all.copy()
                df_merged.update(edited_indiv)
                final_df = apply_audit_checklist_changes(
                    df_indiv_all, df_merged, ["Nama", "Target"], filter_nama)
                save_checklist(SHEET_TARGET_INDIVIDU, final_df,
                               INDIV_CHECKLIST_COLUMNS)
                st.success("Tersimpan!")
                st.rerun()

            # Upload Bukti Individu
            with st.expander(f"📂 Update Bukti ({filter_nama})"):
                pilih_target = st.selectbox(
                    "Target:", df_user["Target"].tolist(), key="mob_sel_indiv")
                note_target = st.text_area("Catatan", key="mob_note_indiv")
                file_target = st.file_uploader("File", key="mob_file_indiv")
                if st.button("Update Pribadi", use_container_width=True, key="mob_upd_indiv"):
                    res, msg = update_evidence_row(
                        SHEET_TARGET_INDIVIDU, pilih_target, note_target, file_target, filter_nama, "Individu")
                    if res:
                        st.success("Updated!")
                        st.rerun()
                    else:
                        st.error(msg)
        else:
            st.info("Kosong.")

    # --- TAB 3: ADMIN (Fitur Tambah Target) ---
    with tab3:
        st.markdown("#### ➕ Tambah Target Baru")
        jenis_t = st.radio(
            "Jenis", ["Team", "Individu"], horizontal=True, key="mob_jenis_target")

        with st.form("mob_add_kpi"):
            target_text = st.text_area("Isi Target (1 per baris)", height=100)
            c1, c2 = st.columns(2)
            t_mulai = c1.date_input("Mulai", value=datetime.now())
            t_selesai = c2.date_input(
                "Selesai", value=datetime.now()+timedelta(days=30))

            nama_target = ""
            if jenis_t == "Individu":
                nama_target = st.selectbox(
                    "Staf:", get_daftar_staf_terbaru(), key="mob_add_staf_target")

            if st.form_submit_button("Tambah Target", use_container_width=True):
                targets = clean_bulk_input(target_text)
                sheet = SHEET_TARGET_TEAM if jenis_t == "Team" else SHEET_TARGET_INDIVIDU
                base = ["", str(t_mulai), str(t_selesai), "FALSE", "-"]
                if jenis_t == "Individu":
                    base = [nama_target] + base

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
            cd_marketing = st.selectbox(
                "Nama Marketing", get_daftar_staf_terbaru())
            cd_tgl = st.date_input("Tanggal Event")
            cd_bidang = st.text_input("Bidang", placeholder="F&B / Wedding")
            cd_nilai = st.text_input("Nilai (Rp)", placeholder="Contoh: 15jt")

            if st.form_submit_button("Simpan Deal", type="primary", use_container_width=True):
                res, msg = tambah_closing_deal(
                    cd_group, cd_marketing, cd_tgl, cd_bidang, cd_nilai)
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
        tot = df_cd[COL_NILAI_KONTRAK].sum(
        ) if COL_NILAI_KONTRAK in df_cd.columns else 0
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
                    df_plot[COL_NILAI_KONTRAK] = df_plot[COL_NILAI_KONTRAK].fillna(
                        0).astype(int)
                    fig = px.bar(df_plot, x=COL_MARKETING, y=COL_NILAI_KONTRAK, color=COL_BIDANG,
                                 title="Total per Marketing")
                    st.plotly_chart(fig, use_container_width=True)
                except:
                    pass
    else:
        st.info("Belum ada data.")


def render_payment_mobile():
    st.markdown("### 💳 Pembayaran (Full Mobile)")
    
    # =========================================================
    # 1. INITIALIZATION & REFRESH SYSTEM (Integrasi Kode Kedua)
    # =========================================================
    if "buffer_pay_data" not in st.session_state:
        with st.spinner("Memuat data dari server..."):
            # Load awal dari GSheet -> Masuk ke RAM (Session State)
            st.session_state["buffer_pay_data"] = load_pembayaran_dp()

    # Tombol Refresh untuk menarik data terbaru dari Google Sheets
    if st.button("🔄 Refresh Data Server", use_container_width=True):
        st.cache_data.clear()
        st.session_state["buffer_pay_data"] = load_pembayaran_dp()
        st.success("Data diperbarui dari server!")
        time.sleep(0.5)
        st.rerun()

    # =========================================================
    # 2. FORM INPUT BARU
    # =========================================================
    with st.expander("➕ Input Pembayaran Baru", expanded=False):
        with st.form("mob_form_pay"):
            p_group = st.text_input("Group (Opsional)")
            p_marketing = st.selectbox("Marketing", get_daftar_staf_terbaru())
            p_nominal = st.text_input("Nominal (Rp)", placeholder="Contoh: 15.000.000 atau 15jt")
            p_jenis = st.selectbox("Jenis", ["Down Payment (DP)", "Termin", "Pelunasan"])
            p_jatuh_tempo = st.date_input("Batas Waktu Bayar", value=datetime.now(tz=TZ_JKT).date() + timedelta(days=7))
            p_status = st.checkbox("Sudah Dibayar?")
            p_bukti = st.file_uploader("Upload Bukti Transfer", disabled=not KONEKSI_DROPBOX_BERHASIL)
            
            if st.form_submit_button("Simpan Pembayaran", type="primary", use_container_width=True):
                with st.spinner("Menyimpan data..."):
                    res, msg = tambah_pembayaran_dp(
                        p_group, p_marketing, datetime.now(tz=TZ_JKT), 
                        p_jenis, p_nominal, p_jatuh_tempo, p_status, p_bukti, "-"
                    )
                    if res:
                        st.success(msg)
                        # Update Buffer RAM agar data baru langsung muncul setelah rerun
                        st.cache_data.clear()
                        st.session_state["buffer_pay_data"] = load_pembayaran_dp()
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(msg)

    st.divider()

    # =========================================================
    # 3. LOAD DATA DARI BUFFER & SISTEM ALERT
    # =========================================================
    # Gunakan data dari RAM (Session State)
    df_pay = st.session_state["buffer_pay_data"]

    if not df_pay.empty:
        # Sistem Peringatan (Overdue & Due Soon)
        overdue, due_soon = build_alert_pembayaran(df_pay)
        if not overdue.empty:
            st.error(f"⛔ **{len(overdue)} Data Overdue!** Segera follow up pembayaran yang terlambat.")
        if not due_soon.empty:
            st.warning(f"⚠️ **{len(due_soon)} Jatuh Tempo Dekat.** Batas bayar dalam 3 hari ke depan.")

        # =========================================================
        # 4. EDITOR DATA (Audit Log & Dynamic Cleaning)
        # =========================================================
        st.markdown("#### 📋 Edit Data & Cek Status")
        st.caption("Ubah status 'Lunas' atau 'Jatuh Tempo' langsung di tabel bawah ini.")

        # Bersihkan tipe data secara dinamis sebelum masuk ke editor (Integrasi Kode Kedua)
        df_editor_source = clean_df_types_dynamically(df_pay)
        
        # Sesuai Code Pertama: Batasi kolom yang boleh diubah staf via HP
        # Kita gunakan display wrapper untuk tampilan rupiah yang cantik
        df_view = payment_df_for_display(df_editor_source)
        
        editable_cols = [COL_STATUS_BAYAR, COL_JATUH_TEMPO, COL_CATATAN_BAYAR]
        disabled_cols = [c for c in df_view.columns if c not in editable_cols]

        edited_pay_mob = st.data_editor(
            df_view,
            column_config={
                COL_STATUS_BAYAR: st.column_config.CheckboxColumn("Lunas?", width="small"),
                COL_JATUH_TEMPO: st.column_config.DateColumn("Jatuh Tempo", format="DD/MM/YYYY"),
                COL_NOMINAL_BAYAR: st.column_config.TextColumn("Nominal", disabled=True),
                COL_BUKTI_BAYAR: st.column_config.LinkColumn("Bukti"),
                COL_TS_UPDATE: st.column_config.TextColumn("Riwayat Perubahan (Log)", disabled=True)
            },
            disabled=disabled_cols,
            hide_index=True,
            use_container_width=True,
            key="smart_payment_editor_mobile_v3"
        )

        # Tombol Simpan Perubahan
        if st.button("💾 Simpan Perubahan Data", type="primary", use_container_width=True):
            with st.spinner("Memproses perubahan & mencatat audit log..."):
                actor_name = st.session_state.get("user_name", "Mobile User")
                
                # Proses deteksi perubahan
                final_df = apply_audit_payments_changes(df_pay, edited_pay_mob, actor=actor_name)
                
                if save_pembayaran_dp(final_df):
                    # Update RAM lokal agar UI langsung sinkron (Integrasi Kode Kedua)
                    st.session_state["buffer_pay_data"] = final_df
                    st.success("✅ Perubahan database berhasil disimpan!")
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("❌ Gagal menyimpan ke Database GSheet.")

        st.divider()

        # =========================================================
        # 5. FITUR UPLOAD BUKTI SUSULAN
        # =========================================================
        with st.expander("📎 Upload Bukti (Susulan)", expanded=False):
            st.caption("Gunakan ini untuk menambah/mengganti foto bukti transfer.")
            df_pay_reset = df_pay.reset_index(drop=True)
            
            options = [
                f"{i+1}. {r[COL_MARKETING]} | {r[COL_GROUP]} ({format_rupiah_display(r[COL_NOMINAL_BAYAR])})" 
                for i, r in df_pay_reset.iterrows()
            ]
            
            sel_idx = st.selectbox("Pilih Data Pembayaran:", range(len(options)), 
                                  format_func=lambda x: options[x], key="mob_sel_susulan")

            file_susulan = st.file_uploader("Pilih File Bukti Baru", key="mob_file_susulan")

            if st.button("⬆️ Update Foto Bukti", use_container_width=True):
                if file_susulan:
                    marketing_name = df_pay_reset.iloc[sel_idx][COL_MARKETING]
                    actor_now = st.session_state.get("user_name", "Mobile User")
                    
                    ok, msg = update_bukti_pembayaran_by_index(sel_idx, file_susulan, marketing_name, actor=actor_now)
                    if ok:
                        st.success("✅ Bukti berhasil di-update!")
                        # Refresh buffer setelah upload file
                        st.session_state["buffer_pay_data"] = load_pembayaran_dp()
                        st.cache_data.clear()
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(msg)
                else:
                    st.warning("Silakan pilih file terlebih dahulu.")
    else:
        st.info("Belum ada data pembayaran yang tercatat.")


def render_admin_mobile():
    st.markdown("### 🔐 Admin Dashboard (Full Mobile)")

    # 1. VERIFIKASI LOGIN ADMIN
    if not st.session_state.get("is_admin"):
        with st.container(border=True):
            st.warning("Akses Terbatas: Masukkan Password Admin")
            pwd = st.text_input("Password", type="password", key="mob_adm_pwd")
            if st.button("Masuk Ke Dashboard", use_container_width=True, type="primary"):
                if verify_admin_password(pwd):
                    st.session_state["is_admin"] = True
                    st.rerun()
                else:
                    st.error("Password salah.")
        return 

    # 2. TOMBOL LOGOUT & REFRESH
    col_nav1, col_nav2 = st.columns(2)
    if col_nav1.button("🔓 Logout Admin", use_container_width=True):
        st.session_state["is_admin"] = False
        st.rerun()
    if col_nav2.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    # 3. LOADING DATA KE RAM VPS
    staff_list = get_daftar_staf_terbaru()
    df_all = load_all_reports(staff_list)

    if not df_all.empty:
        try:
            df_all[COL_TIMESTAMP] = pd.to_datetime(
                df_all[COL_TIMESTAMP], format="%d-%m-%Y %H:%M:%S", errors="coerce")
            df_all["Tgl"] = df_all[COL_TIMESTAMP].dt.date
            df_all["Kat"] = df_all[COL_TEMPAT].apply(lambda x: "Digital/Kantor" if any(
                k in str(x) for k in ["Digital", "Ads", "Konten", "Telesales", "Marketing"]) else "Kunjungan Lapangan")
        except Exception:
            pass

    # 4. PENGATURAN TABS ADAPTIF
    is_manager = (st.session_state.get("user_role") == "manager")
    tab_labels = ["🔔 Approval"] if is_manager else []
    tab_labels.extend(["📈 Grafik", "⚡ Quick Edit", "🧲 Leads", "📜 Log", "👥 Config"])
    
    all_tabs = st.tabs(tab_labels)
    ptr = 0

    # --- TAB 1: APPROVAL (KHUSUS MANAGER) ---
    if is_manager:
        with all_tabs[ptr]:
            st.markdown("#### 🔔 Persetujuan Manager")
            pending_requests = get_pending_approvals()
            if not pending_requests:
                st.info("Tidak ada data yang menunggu persetujuan.")
            else:
                for i, req in enumerate(pending_requests):
                    with st.container(border=True):
                        st.markdown(f"**Pengaju:** {req['Requestor']}")
                        st.caption(f"Target: `{req['Target Sheet']}` | {req['Timestamp']}")
                        st.warning(f"Alasan: {req['Reason']}")
                        
                        with st.expander("Lihat Perubahan Data"):
                            try:
                                old_json = json.loads(req.get("Old Data JSON", "{}"))
                                new_json = json.loads(req.get("New Data JSON", "{}"))
                                diff_list = []
                                for col, val_new in new_json.items():
                                    val_old = old_json.get(col, "")
                                    if str(val_new).strip() != str(val_old).strip():
                                        diff_list.append({"Kolom": col, "Lama": val_old, "Baru": val_new})
                                if diff_list:
                                    st.table(pd.DataFrame(diff_list))
                                else:
                                    st.write("Re-save data (Tidak ada perubahan nilai).")
                            except:
                                st.write("Gagal memproses detail JSON.")

                        c_acc, c_rej = st.columns(2)
                        if c_acc.button("✅ ACC", key=f"mob_acc_{i}", use_container_width=True, type="primary"):
                            ok, msg = execute_approval(i, "APPROVE", st.session_state["user_name"])
                            if ok: st.success("Berhasil di-ACC"); time.sleep(1); st.rerun()
                        
                        if c_rej.button("❌ Tolak", key=f"mob_rej_{i}", use_container_width=True):
                            ok, msg = execute_approval(i, "REJECT", st.session_state["user_name"], "Ditolak via HP")
                            if ok: st.warning("Request Ditolak"); time.sleep(1); st.rerun()
        ptr += 1

    # --- TAB 2: GRAFIK & AI INSIGHT ---
    with all_tabs[ptr]:
        st.markdown("#### 📈 Analisa Produktivitas")
        if not df_all.empty:
            days = st.selectbox("Rentang Waktu:", [7, 30, 90], index=0, key="mob_days_filter")
            cutoff = datetime.now(tz=TZ_JKT).date() - timedelta(days=days)
            df_f = df_all[df_all["Tgl"] >= cutoff]
            
            st.metric("Total Laporan", len(df_f))
            report_counts = df_f[COL_NAMA].value_counts()
            st.bar_chart(report_counts)

            st.divider()
            st.markdown("#### 🤖 AI Assistant Analysis")
            with st.spinner("Menganalisa data untuk Pak Nugroho..."):
                try:
                    staf_stats_str = json.dumps(report_counts.to_dict(), indent=2)
                    full_prompt = f"""
                    [CONTEXT] Nama Pemimpin: Pak Nugroho. Total Laporan: {len(df_f)}. Statistik: {staf_stats_str}.
                    [SYSTEM] Kamu asisten cerdas Pak Nugroho. Gunakan Bahasa Indonesia santun dan berwibawa.
                    [INSTRUCTION] Bandingkan kecepatan tim dengan kompetitor secara naratif. Tekankan keunggulan data real-time.
                    Apresiasi staf yang aktif. Jangan sebut angka target teknis. JANGAN mengaku sebagai AI.
                    """
                    ai_reply = ""
                    for model_name in MODEL_FALLBACKS:
                        try:
                            if SDK == "new":
                                resp = client_ai.models.generate_content(model=model_name, contents=full_prompt)
                                ai_reply = resp.text
                            else:
                                model = genai_legacy.GenerativeModel(model_name)
                                resp = model.generate_content(full_prompt)
                                ai_reply = resp.text
                            if ai_reply: break
                        except: continue
                    if ai_reply: st.info(ai_reply)
                except Exception as e:
                    st.error("Gagal memuat AI Insight.")
        else:
            st.info("Belum ada data untuk dianalisa.")
        ptr += 1

    # --- TAB 3: QUICK EDIT (PENGGANTI SUPER EDITOR) ---
    with all_tabs[ptr]:
        st.markdown("#### ⚡ Quick Edit (Search-to-Edit)")
        st.caption("Cari data spesifik untuk diperbaiki tanpa memuat tabel besar.")
        
        map_sheets = {"Laporan": "Laporan Kegiatan Harian", "Closing": SHEET_CLOSING_DEAL, "Payment": SHEET_PEMBAYARAN}
        target_label = st.selectbox("Pilih Tabel:", list(map_sheets.keys()), key="mob_edit_target")
        target_sheet_name = map_sheets[target_label]

        try:
            ws_edit = spreadsheet.worksheet(target_sheet_name)
            df_edit = pd.DataFrame(ws_edit.get_all_records())
            search_query = st.text_input("🔍 Cari Kata Kunci (Nama/Group):", key="mob_search_val")
            
            if search_query:
                mask = df_edit.apply(lambda row: row.astype(str).str.contains(search_query, case=False).any(), axis=1)
                df_filtered = df_edit[mask]
                
                if not df_filtered.empty:
                    options = [f"Baris {idx+2}: {row.iloc[0]} | {row.iloc[1]}" for idx, row in df_filtered.iterrows()]
                    selected_row = st.selectbox("Pilih Baris yang Diedit:", options)
                    row_idx_gsheet = int(selected_row.split(":")[0].replace("Baris ", ""))
                    
                    with st.form("mob_quick_edit_form"):
                        st.info(f"Mengedit Baris {row_idx_gsheet}")
                        current_row_data = df_edit.iloc[row_idx_gsheet-2].to_dict()
                        updated_values = {}
                        for col_name, col_val in current_row_data.items():
                            updated_values[col_name] = st.text_input(f"Field: {col_name}", value=str(col_val))
                        
                        if st.form_submit_button("💾 Update Data", use_container_width=True):
                            headers = ws_edit.row_values(1)
                            new_row_list = [updated_values.get(h, "") for h in headers]
                            ws_edit.update(range_name=f"A{row_idx_gsheet}", values=[new_row_list], value_input_option="USER_ENTERED")
                            force_audit_log(st.session_state["user_name"], "⚡ QUICK EDIT", target_sheet_name, f"Edit Baris {row_idx_gsheet}", "Update via Mobile")
                            st.success("Data berhasil diperbarui!"); time.sleep(1); st.rerun()
                else:
                    st.error("Data tidak ditemukan.")
        except Exception as e:
            st.error(f"Gagal memuat editor: {e}")
        ptr += 1

    # --- TAB 4: LEADS & INTEREST ---
    with all_tabs[ptr]:
        st.markdown("#### 🧲 Leads Management")
        if not df_all.empty and COL_INTEREST in df_all.columns:
            sel_int = st.select_slider("Filter Interest:", options=["Under 50% (A)", "50-75% (B)", "75%-100%"])
            df_leads = df_all[df_all[COL_INTEREST].astype(str).str.strip() == sel_int]
            
            for _, row in df_leads.iterrows():
                with st.container(border=True):
                    st.markdown(f"👤 **{row[COL_NAMA_KLIEN]}**")
                    st.write(f"📞 {row[COL_KONTAK_KLIEN]}")
                    st.caption(f"Marketing: {row[COL_NAMA]} | Hasil: {row[COL_KESIMPULAN]}")

            if HAS_OPENPYXL and not df_leads.empty:
                xb = df_to_excel_bytes(df_leads, sheet_name="Leads")
                st.download_button("⬇️ Download Leads (Excel)", data=xb, file_name=f"leads_{sel_int}.xlsx", use_container_width=True)
        ptr += 1

    # --- TAB 5: GLOBAL AUDIT LOG ---
    with all_tabs[ptr]:
        st.markdown("#### 📜 15 Aktivitas Terakhir")
        from audit_service import load_audit_log
        df_log_raw = load_audit_log(spreadsheet)
        if not df_log_raw.empty:
            df_log = dynamic_column_mapper(df_log_raw)
            for _, row in df_log.head(15).iterrows():
                with st.container(border=True):
                    st.markdown(f"**{row.get('User','-')}** ➡ `{row.get('Status','-')}`")
                    st.caption(f"🕒 {row.get('Waktu','-')} | Data: {row.get('Target Data','-')}")
                    chat = row.get('Chat & Catatan', '')
                    if chat and chat != "-": st.info(chat)
                    with st.expander("Detail Perubahan"):
                        st.code(row.get('Detail Perubahan','-'), language="text")
        else:
            st.info("Belum ada log aktivitas.")
        ptr += 1

    # --- TAB 6: CONFIG STAFF & AKUN ---
    with all_tabs[ptr]:
        st.markdown("#### 👥 Kelola Tim")
        with st.form("mob_add_staff_form"):
            st.markdown("**Tambah Anggota Baru**")
            new_st_name = st.text_input("Nama Lengkap Staf:")
            if st.form_submit_button("➕ Tambah Staf", use_container_width=True):
                if new_st_name:
                    ok, msg = tambah_staf_baru(new_st_name)
                    if ok: st.success("Staf ditambahkan"); st.cache_data.clear(); time.sleep(1); st.rerun()
                    else: st.error(msg)

        st.divider()
        st.markdown("#### 🗑️ Hapus Akses")
        st.error("Tindakan ini permanen.")
        list_hapus = get_daftar_staf_terbaru()
        nama_hapus = st.selectbox("Pilih Staf yang Dihapus:", ["-- Pilih --"] + list_hapus)
        confirm = st.checkbox("Konfirmasi Penghapusan")
        if st.button("🔥 Hapus Staf", type="primary", use_container_width=True):
            if nama_hapus != "-- Pilih --" and confirm:
                ok, msg = hapus_staf_by_name(nama_hapus)
                if ok:
                    force_audit_log(st.session_state["user_name"], "❌ DELETE USER", "Config_Staf", f"Hapus: {nama_hapus}", "-")
                    st.success("Staf dihapus"); st.cache_data.clear(); time.sleep(1); st.rerun()
                else: st.error(msg)

    render_section_watermark()


def render_audit_mobile():
    st.markdown("### 📜 Global Audit Log (Mobile)")
    st.caption("Rekaman jejak perubahan data admin.")

    from audit_service import load_audit_log

    if st.button("🔄 Refresh", use_container_width=True, key="mob_refresh_log"):
        st.cache_data.clear()
        st.rerun()

    df_raw = load_audit_log(spreadsheet)

    if not df_raw.empty:
        # Gunakan mapper dinamis agar kolom terdeteksi otomatis
        df_log = dynamic_column_mapper(df_raw)

        # Sortir data terbaru
        try:
            df_log["Waktu"] = pd.to_datetime(df_log["Waktu"], errors="coerce")
            df_log = df_log.sort_values(by="Waktu", ascending=False)
        except:
            pass

        st.markdown("#### 🕒 10 Aktivitas Terakhir")

        for i, row in df_log.head(10).iterrows():
            with st.container(border=True):
                # Gunakan .get() agar aman jika kolom tetap tidak terdeteksi
                st.markdown(f"**{row.get('User', '-')}**")
                st.caption(
                    f"📅 {row.get('Waktu', '-')} | Status: {row.get('Status', '-')}")
                st.text(f"Data: {row.get('Target Data', '-')}")

                chat_val = row.get('Chat & Catatan', '-')
                if chat_val not in ["-", ""]:
                    st.info(f"📝 {chat_val}")

                with st.expander("Lihat Detail"):
                    st.code(row.get('Detail Perubahan', '-'), language="text")
    else:
        st.info("Belum ada data log.")

# =========================================================
# MAIN ROUTER LOGIC (REVISI TOTAL)
# =========================================================


# =========================================================
# MAIN ROUTER LOGIC: IMPLEMENTASI SELURUH FITUR
# =========================================================

# --- 1. HALAMAN PRESENSI (REAL-TIME & NO-EDIT) ---
if menu_nav == "📅 Presensi":
    st.markdown("## 📅 Presensi Kehadiran Real-Time")
    st.caption(
        "Silakan pilih nama Anda. Waktu, hari, dan tanggal akan tercatat otomatis oleh sistem (WIB).")

    with st.container(border=True):
        staff_list = get_daftar_staf_terbaru()
        pilih_nama = st.selectbox(
            "Pilih Nama Anda:", ["-- Pilih Nama --"] + staff_list, key="presensi_name_sel")

        waktu_skrg = datetime.now(TZ_JKT)
        st.info(
            f"🕒 Waktu Sistem Saat Ini: **{waktu_skrg.strftime('%A, %d %B %Y - %H:%M:%S')} WIB**")

        if st.button("✅ Kirim Presensi Sekarang", type="primary", use_container_width=True):
            if pilih_nama == "-- Pilih Nama --":
                st.error("Silakan pilih nama terlebih dahulu!")
            else:
                with st.spinner("Mencatat kehadiran..."):
                    ok, msg = catat_presensi(pilih_nama)
                    if ok:
                        st.success(msg)
                        force_audit_log(
                            actor=pilih_nama,
                            action="✅ PRESENSI",
                            target_sheet="Presensi_Kehadiran",
                            chat_msg="Absensi Masuk Real-time",
                            details_input=f"Presensi sukses pukul {waktu_skrg.strftime('%H:%M:%S')}"
                        )
                        time.sleep(2)
                        st.rerun()
                    else:
                        st.warning(msg)

    st.divider()
    st.markdown("### 📋 Kehadiran Hari Ini")
    ws_p = init_presensi_db()
    if ws_p:
        data_p = ws_p.get_all_records()
        if data_p:
            df_p = pd.DataFrame(data_p)
            tgl_hari_ini = waktu_skrg.strftime("%d")
            bln_hari_ini = waktu_skrg.strftime("%B")
            df_today = df_p[(df_p['Tanggal'].astype(str) == tgl_hari_ini) & (
                df_p['Bulan'] == bln_hari_ini)]
            if not df_today.empty:
                st.dataframe(df_today, use_container_width=True,
                             hide_index=True)
            else:
                st.info("Belum ada data kehadiran hari ini.")

# --- 2. HALAMAN LAPORAN HARIAN ---
elif menu_nav == "📝 Laporan Harian":
    if IS_MOBILE:
        render_laporan_harian_mobile()
    else:
        st.markdown("## 📝 Laporan Kegiatan Harian")
        c1, c2 = st.columns([1, 2])
        with c1:
            pelapor = st.selectbox(
                "Nama Pelapor", get_daftar_staf_terbaru(), key="pelapor_desk")
        with c2:
            pending = get_reminder_pending(pelapor)
            if pending:
                st.warning(f"🔔 Reminder Pending: {pending}")

        with st.container(border=True):
            with st.form("daily_report_desk", clear_on_submit=False):
                st.markdown("### 📌 Detail Aktivitas")
                col_kiri, col_kanan = st.columns(2)
                with col_kiri:
                    kategori = st.radio(
                        "Kategori", ["🚗 Sales Lapangan", "💻 Digital/Kantor", "📞 Telesales", "🏢 Lainnya"])
                    lokasi = st.text_input(
                        "Lokasi / Nama Klien / Jenis Tugas", placeholder="Wajib diisi...")
                    deskripsi = st.text_area("Deskripsi Detail", height=150)
                    foto = st.file_uploader(
                        "Upload Bukti", accept_multiple_files=True, disabled=not KONEKSI_DROPBOX_BERHASIL)
                with col_kanan:
                    st.markdown("### 📊 Hasil & Follow Up")
                    kesimpulan = st.text_area("Kesimpulan / Hasil", height=80)
                    kendala = st.text_area(
                        "Kendala Internal/Lapangan", height=60)
                    next_plan = st.text_input("Next Plan / Pending (Reminder)")
                    st.markdown("### 👤 Data Klien")
                    cl_nama = st.text_input("Nama Klien")
                    cl_kontak = st.text_input("No HP/WA")
                    cl_interest = st.selectbox(
                        "Interest Level", ["-", "Under 50%", "50-75%", "75-100%"])
                st.divider()
                if st.form_submit_button("✅ KIRIM LAPORAN", type="primary", use_container_width=True):
                    if not lokasi or not deskripsi:
                        st.error("Lokasi dan Deskripsi wajib diisi!")
                    else:
                        with st.spinner("Mengirim laporan..."):
                            ts = now_ts_str()
                            final_link = "-"
                            if foto and KONEKSI_DROPBOX_BERHASIL:
                                links = [upload_ke_dropbox(
                                    f, pelapor, "Laporan_Harian") for f in foto]
                                final_link = ", ".join(links)
                            row_data = [ts, pelapor, lokasi, deskripsi, final_link, "-", kesimpulan,
                                        kendala, "-", next_plan, "-", cl_interest, cl_nama, cl_kontak]
                            if simpan_laporan_harian_batch([row_data], pelapor):
                                st.success("Laporan Terkirim!")
                                st.cache_data.clear()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("Gagal simpan ke GSheet.")

# --- 3. TARGET & KPI ---
elif menu_nav == "🎯 Target & KPI":
    if IS_MOBILE:
        render_kpi_mobile()
    else:
        st.markdown("## 🎯 Manajemen Target & KPI")
        tab1, tab2, tab3 = st.tabs(
            ["🏆 Target Team", "⚡ Target Individu", "⚙️ Admin Setup"])
        with tab1:
            df_team = load_checklist(SHEET_TARGET_TEAM, TEAM_CHECKLIST_COLUMNS)
            if not df_team.empty:
                edited_team = render_hybrid_table(df_team, "team_desk", "Misi")
                if st.button("💾 Simpan Perubahan Team"):
                    final_df = apply_audit_checklist_changes(
                        df_team, edited_team, ["Misi"], get_actor_fallback())
                    save_checklist(SHEET_TARGET_TEAM, final_df,
                                   TEAM_CHECKLIST_COLUMNS)
                    st.success("Tersimpan!")
                    st.cache_data.clear()
                    st.rerun()
        with tab2:
            st.caption("Monitoring target perorangan.")
            pilih_staf = st.selectbox(
                "Pilih Nama Staf:", get_daftar_staf_terbaru())
            
            df_indiv_all = load_checklist(
                SHEET_TARGET_INDIVIDU, INDIV_CHECKLIST_COLUMNS)
            df_user = df_indiv_all[df_indiv_all["Nama"] == pilih_staf]
            
            if not df_user.empty:
                # ==========================================
                # ANCHOR: LOGIKA PROGRESS BAR (PENTING)
                # ==========================================
                total_target = len(df_user)
                # Menghitung jumlah baris yang statusnya dicentang (True)
                jumlah_selesai = df_user["Status"].sum() 
                persentase = jumlah_selesai / total_target if total_target > 0 else 0
                
                # Tampilan Visual Progres
                st.markdown(f"### 📈 Progres {pilih_staf}: {int(persentase * 100)}%")
                st.progress(persentase)
                st.write(f"✅ **{jumlah_selesai}** selesai dari **{total_target}** target.")
                st.divider()
                # ==========================================

                # Tabel editor untuk mencentang target
                edited_indiv = render_hybrid_table(
                    df_user, f"indiv_{pilih_staf}", "Target")
                
                if st.button(f"💾 Simpan Target {pilih_staf}", use_container_width=True):
                    df_merged = df_indiv_all.copy()
                    
                    # Update data lama dengan data hasil editan tabel
                    df_merged.update(edited_indiv)
                    
                    final_df = apply_audit_checklist_changes(
                        df_indiv_all, df_merged, ["Nama", "Target"], pilih_staf)
                    
                    if save_checklist(SHEET_TARGET_INDIVIDU, final_df, INDIV_CHECKLIST_COLUMNS):
                        st.success(f"Berhasil menyimpan progres {pilih_staf}!")
                        st.cache_data.clear()
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Gagal menyimpan ke database.")
            else:
                st.info(f"Belum ada target yang ditugaskan untuk {pilih_staf}.")
        with tab3:
            st.markdown("### ➕ Tambah Target Baru")
            jenis_t = st.radio(
                "Jenis Target", ["Team", "Individu"], horizontal=True)
            with st.form("add_kpi_desk"):
                target_text = st.text_area("Isi Target (1 per baris)")
                tgl_m = st.date_input("Mulai", value=datetime.now())
                tgl_s = st.date_input(
                    "Selesai", value=datetime.now()+timedelta(days=30))
                nama_t = st.selectbox(
                    "Untuk Staf:", get_daftar_staf_terbaru()) if jenis_t == "Individu" else ""
                if st.form_submit_button("Tambah Target"):
                    targets = clean_bulk_input(target_text)
                    sheet = SHEET_TARGET_TEAM if jenis_t == "Team" else SHEET_TARGET_INDIVIDU
                    base = ["", str(tgl_m), str(tgl_s), "FALSE", "-"]
                    if jenis_t == "Individu":
                        base = [nama_t] + base
                    if add_bulk_targets(sheet, base, targets):
                        st.success("Berhasil!")
                        st.cache_data.clear()
                        st.rerun()

# --- 4. CLOSING DEAL ---
elif menu_nav == "🤝 Closing Deal":
    if IS_MOBILE:
        render_closing_mobile()
    else:
        st.markdown("## 🤝 Closing Deal")
        with st.container(border=True):
            with st.form("form_closing_desk_full", clear_on_submit=True):
                c1, c2, c3 = st.columns(3)
                inp_group = c1.text_input("Nama Group (Opsional)")
                inp_marketing = c2.text_input("Nama Marketing")
                inp_tgl_event = c3.date_input(
                    "Tanggal Event", value=datetime.now(tz=TZ_JKT).date())
                inp_bidang = st.text_input("Bidang / Jenis Event")
                inp_nilai = st.text_input("Nilai Kontrak (Rupiah)")
                if st.form_submit_button("✅ Simpan Closing Deal", type="primary", use_container_width=True):
                    res, msg = tambah_closing_deal(
                        inp_group, inp_marketing, inp_tgl_event, inp_bidang, inp_nilai)
                    if res:
                        st.success(msg)
                        st.cache_data.clear()
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(msg)
        st.divider()
        df_cd = load_closing_deal()
        if not df_cd.empty:
            st.dataframe(df_cd, use_container_width=True, hide_index=True)

# --- 5. PEMBAYARAN ---
elif menu_nav == "💳 Pembayaran":
    # 1. Cek Buffer (Jika sudah login dan prefetch jalan, ini harusnya sudah ada)
    if "buffer_pay_data" not in st.session_state:
        with st.spinner("🚀 Inisialisasi Database..."):
            st.session_state["buffer_pay_data"] = load_pembayaran_dp()

    # Ambil dataframe utama langsung dari RAM (0 detik delay)
    df_pay = st.session_state["buffer_pay_data"]

    if IS_MOBILE:
        render_payment_mobile() # #sisanyatetapsama (Pastikan fungsi mobile juga membaca buffer jika ingin cepat)
    else:
        st.markdown("## 💳 Smart Payment Action Center")
        
        # Layout Header + Tombol Sync Manual
        col_title, col_sync = st.columns([5, 1])
        with col_title:
            st.caption("Manajemen pembayaran terpadu (Mode Cepat: Data berjalan di RAM).")
        with col_sync:
            # Tombol ini hanya diklik jika ingin menarik data terbaru dari inputan orang lain
            if st.button("🔄 Sync Server", help="Paksa tarik data baru dari Cloud", use_container_width=True):
                st.cache_data.clear()
                st.session_state["buffer_pay_data"] = load_pembayaran_dp() # Update RAM
                st.success("Data Sinkron!")
                time.sleep(0.5)
                st.rerun()

        # =========================================================
        # 1. SEKSI INPUT: KALKULATOR PEMBAYARAN PINTAR
        # =========================================================
        with st.container(border=True):
            st.markdown("### ➕ Input Pembayaran & Kalkulator Sisa")
            
            # [OPTIMASI CRITICAL] Menggunakan st.form agar TIDAK reload saat mengetik huruf
            with st.form("form_smart_pay_ram", clear_on_submit=True):
                c1, c2, c3 = st.columns(3)
                
                with c1:
                    # #logikasama: Menggunakan cache function untuk list staf agar cepat
                    p_marketing = st.selectbox("Nama Marketing", get_daftar_staf_terbaru())
                    p_group = st.text_input("Nama Group / Klien", placeholder="Masukkan nama entitas...")
                    p_total_sepakat = st.text_input("Total Nilai Kesepakatan (Rp)", placeholder="Contoh: 100.000.000")
                
                with c2:
                    # #sisanyatetapsama
                    p_jenis = st.selectbox("Mekanisme Pembayaran", ["Down Payment (DP)", "Cicilan", "Cash"])
                    p_nom_bayar = st.text_input("Nominal yang Dibayar Sekarang (Rp)")
                    p_tenor = st.number_input("Tenor Cicilan (Bulan)", min_value=0, step=1, help="Isi 0 jika Cash/DP")
                
                with c3:
                    # #sisanyatetapsama
                    p_tgl_event = st.date_input("Tanggal Event", value=datetime.now(tz=TZ_JKT).date())
                    p_due = st.date_input("Batas Waktu Bayar (Jatuh Tempo)", value=datetime.now(tz=TZ_JKT).date() + timedelta(days=7))
                    p_bukti = st.file_uploader("Upload Bukti Transfer (Foto/PDF)")

                p_note = st.text_area("Catatan Tambahan (Opsional)", placeholder="Keterangan bank, nomor referensi, dll.")
                
                # Tombol Submit WAJIB di dalam form
                submitted = st.form_submit_button("✅ Simpan & Hitung Sisa", type="primary", use_container_width=True)
                
                if submitted:
                    if not p_total_sepakat or not p_nom_bayar:
                        st.error("Gagal: Nilai Kesepakatan dan Nominal Bayar wajib diisi!")
                    else:
                        with st.spinner("Menyimpan ke Database..."):
                            # #logikasama: Memanggil fungsi simpan database
                            ok, msg = tambah_pembayaran_dp(
                                p_group, p_marketing, p_tgl_event, p_jenis, 
                                p_nom_bayar, p_total_sepakat, p_tenor, p_due, p_bukti, p_note
                            )
                            if ok:
                                st.success(msg)
                                st.cache_data.clear()
                                # [OPTIMASI] Update Buffer RAM agar data baru langsung muncul di tabel bawah tanpa fetch ulang
                                st.session_state["buffer_pay_data"] = load_pembayaran_dp() 
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error(msg)

        st.divider()

        # =========================================================
        # 2. SEKSI MONITORING: ALERT & DATA EDITOR (RAM BASED)
        # =========================================================
        st.markdown("### 📋 Monitoring & Riwayat Pembayaran")
        
        # [OPTIMASI] Menggunakan data dari RAM (df_pay) -> Instan
        if df_pay.empty:
            st.info("Belum ada data pembayaran yang tersimpan.")
        else:
            # --- Sistem Alert Pintar (Hitung di RAM) ---
            # #logikasama: Logika overdue/due soon
            overdue, due_soon = build_alert_pembayaran(df_pay)
            col_stat1, col_stat2 = st.columns(2)
            with col_stat1:
                st.metric("⛔ Overdue (Belum Lunas)", len(overdue))
                if not overdue.empty: st.error("Ada tagihan overdue!")
            with col_stat2:
                st.metric("⚠️ Jatuh Tempo Dekat (≤ 3 Hari)", len(due_soon))
                if not due_soon.empty: st.warning("Segera lakukan penagihan ulang.")

            st.caption("Klik dua kali pada sel untuk mengedit. Data diload dari RAM (High Speed).")

            # --- CLEANING DATA (Di RAM) ---
            # #logikasama: Membersihkan tipe data agar editor tidak error
            df_ready = clean_df_types_dynamically(df_pay)
            
            # #logikasama: Generate config kolom otomatis
            auto_config = generate_dynamic_column_config(df_ready)
            
            # #logikasama: Kunci kolom log/timestamp
            locked_keywords = ["timestamp", "updated by", "log", "pelaku", "waktu", "input"]
            disabled_list = [c for c in df_ready.columns if any(k in c.lower() for k in locked_keywords)]

            # --- RENDER DATA EDITOR ---
            # [OPTIMASI] Key editor dibuat statis ('ram_optimized') agar tidak reset saat user mengetik
            edited_pay = st.data_editor(
                df_ready,
                column_config=auto_config,
                disabled=disabled_list,
                hide_index=True,
                use_container_width=True,
                num_rows="dynamic",
                key="smart_payment_editor_ram_optimized" 
            )

            # --- SIMPAN PERUBAHAN (WRITE-THROUGH CACHE) ---
            if st.button("💾 Simpan Perubahan Riwayat", use_container_width=True):
                with st.spinner("Sinkronisasi RAM & Cloud..."):
                    current_user = st.session_state.get("user_name", "Admin Desktop")
                    
                    # #logikasama: Bandingkan data RAM vs Editor untuk Log Audit
                    final_df = apply_audit_payments_changes(df_pay, edited_pay, actor=current_user)
                    
                    if save_pembayaran_dp(final_df):
                        # [OPTIMASI SUPER] Update RAM lokal langsung!
                        # Ini membuat UI langsung berubah tanpa perlu download ulang dari GSheet (Hemat 3-5 detik)
                        st.session_state["buffer_pay_data"] = final_df 
                        
                        st.success("✅ Tersimpan! (RAM Updated)")
                        st.cache_data.clear()
                        time.sleep(0.5) # Jeda dipercepat karena tidak perlu loading berat
                        st.rerun()
                    else:
                        st.error("Gagal menyimpan ke Google Sheets.")

            st.divider()

            # =========================================================
            # 3. FITUR TAMBAHAN: UPDATE FOTO BUKTI SUSULAN
            # =========================================================
            with st.expander("📎 Update Bukti Pembayaran (Susulan)", expanded=False):
                st.info("Fitur ini langsung update ke cloud dan sinkronisasi RAM.")
                
                # [OPTIMASI] Reset index dari data RAM untuk dropdown (Cepat)
                df_pay_reset = df_pay.reset_index(drop=True)
                
                # #logikasama: Membuat list opsi dropdown
                pay_options = [
                    f"{i+1}. {r.get(COL_MARKETING,'-')} | {r.get(COL_GROUP,'-')} | Sisa: {format_rupiah_display(r.get(COL_SISA_BAYAR,0))}" 
                    for i, r in df_pay_reset.iterrows()
                ]
                
                sel_idx_upd = st.selectbox("Pilih Record Pembayaran:", range(len(pay_options)), 
                                         format_func=lambda x: pay_options[x], key="desk_sel_susulan")
                
                file_susulan = st.file_uploader("Upload File Bukti Baru", key="desk_file_susulan")
                
                if st.button("⬆️ Upload Foto Sekarang", use_container_width=True):
                    if file_susulan:
                        mkt_name = df_pay_reset.iloc[sel_idx_upd][COL_MARKETING]
                        # #logikasama: Upload ke Dropbox & Update Sheet
                        ok, msg = update_bukti_pembayaran_by_index(sel_idx_upd, file_susulan, mkt_name, actor="Admin")
                        if ok:
                            st.success("Foto bukti berhasil ditambahkan!")
                            
                            # [OPTIMASI] Force refresh RAM karena link bukti berubah di server
                            st.cache_data.clear()
                            st.session_state["buffer_pay_data"] = load_pembayaran_dp()
                            
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(msg)
                    else:
                        st.warning("Silakan pilih file terlebih dahulu.")

        render_section_watermark()

elif menu_nav == "📜 Global Audit Log":
    if IS_MOBILE:
        render_audit_mobile()
    else:
        # --- LOGIC DESKTOP ---
        st.markdown("## 📜 Global Audit Log")
        st.caption(
            "Rekaman jejak perubahan data. Transparansi data Admin & Manager.")

        # Load Data dari Service
        from audit_service import load_audit_log

        # Tombol Refresh
        if st.button("🔄 Refresh Log", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        with st.spinner("Memuat data log..."):
            df_raw = load_audit_log(spreadsheet)

        if not df_raw.empty:
            # 1. Jalankan Mapper Dinamis (Mengubah header GSheet lama/baru ke standar aplikasi)
            df_log = dynamic_column_mapper(df_raw)

            # 2. Pastikan kolom standar yang dibutuhkan UI tersedia (fallback "-" jika benar-benar tidak ketemu)
            standard_cols = ["Waktu", "User", "Status",
                             "Target Data", "Chat & Catatan", "Detail Perubahan"]
            for c in standard_cols:
                if c not in df_log.columns:
                    df_log[c] = "-"

            # 3. Urutkan Waktu (Terbaru di atas)
            try:
                df_log["Waktu"] = pd.to_datetime(
                    df_log["Waktu"], format="%d-%m-%Y %H:%M:%S", errors="coerce")
                df_log = df_log.sort_values(by="Waktu", ascending=False)
            except Exception:
                pass

            # --- FITUR FILTERING ---
            with st.expander("🔍 Filter Pencarian"):
                c1, c2 = st.columns(2)
                # Ambil list unik untuk filter
                all_users = df_log["User"].unique().tolist()
                all_sheets = df_log["Target Data"].unique().tolist()

                with c1:
                    filter_user = st.multiselect(
                        "Pilih Pelaku (User)", all_users)
                with c2:
                    filter_sheet = st.multiselect(
                        "Pilih Sheet/Data", all_sheets)

            # Terapkan Filter jika dipilih
            df_show = df_log.copy()
            if filter_user:
                df_show = df_show[df_show["User"].isin(filter_user)]
            if filter_sheet:
                df_show = df_show[df_show["Target Data"].isin(filter_sheet)]

            # --- TAMPILKAN DATA UI ---
            st.markdown(f"**Total Record:** {len(df_show)}")

            # 4. Render Dataframe (Pastikan Key Column Config sesuai hasil Mapping)
            st.dataframe(
                df_show[standard_cols],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Waktu": st.column_config.DatetimeColumn(
                        "🕒 Waktu",
                        format="D MMM YYYY, HH:mm",
                        width="small"
                    ),
                    "Target Data": st.column_config.TextColumn("Data"),
                    "Chat & Catatan": st.column_config.TextColumn("💬 Catatan / Chat", width="medium"),
                    "Detail Perubahan": st.column_config.TextColumn(
                        "📄 Detail Perubahan",
                        width="large",
                        help="Menampilkan detail perubahan data"
                    )
                }
            )

            # Download Button (Excel)
            if HAS_OPENPYXL:
                xb = df_to_excel_bytes(df_show, sheet_name="Audit_Log")
                if xb:
                    st.download_button(
                        "⬇️ Download Log (Excel)",
                        data=xb,
                        file_name="global_audit_log.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
        else:
            st.info("Belum ada riwayat perubahan data.")

        # Watermark
        render_section_watermark()

elif menu_nav == "📊 Dashboard Admin":
    if IS_MOBILE:
        render_admin_mobile()
    else:
        # --- LOGIC DESKTOP (PC/LAPTOP) ---
        st.markdown("## 📊 Dashboard Admin & Analytics")

        # 1. Verifikasi Akses Admin
        if not st.session_state.get("is_admin"):
            col_l1, col_l2, col_l3 = st.columns([1, 1, 1])
            with col_l2:
                with st.container(border=True):
                    st.markdown("### 🔐 Login Dashboard")
                    pwd_input = st.text_input(
                        "Masukkan Password Admin:", type="password", key="pwd_admin_desk")
                    if st.button("Masuk Ke Dashboard", use_container_width=True, type="primary"):
                        if verify_admin_password(pwd_input):
                            st.session_state["is_admin"] = True
                            st.rerun()
                        else:
                            st.error("Password salah. Akses ditolak.")
        else:
            # --- SETUP DATA DASHBOARD ---
            staff_list = get_daftar_staf_terbaru()
            df_all = load_all_reports(staff_list)

            # Pre-processing Kategori (Sales vs Digital)
            if not df_all.empty:
                try:
                    df_all[COL_TIMESTAMP] = pd.to_datetime(
                        df_all[COL_TIMESTAMP], format="%d-%m-%Y %H:%M:%S", errors="coerce")
                    df_all["Tanggal_Date"] = df_all[COL_TIMESTAMP].dt.date
                    df_all["Kategori_Aktivitas"] = df_all[COL_TEMPAT].apply(
                        lambda x: "Digital/Kantor" if any(k in str(x) for k in ["Digital", "Marketing", "Ads", "Konten", "Telesales"])
                        else "Kunjungan Lapangan"
                    )
                except Exception:
                    pass

            # --- DINAMIS TABS BERDASARKAN ROLE ---
            is_manager = (st.session_state.get("user_role") == "manager")

            # Susun Label Tab (Approval hanya muncul jika Manager)
            tabs_labels = []
            if is_manager:
                tabs_labels.append("🔔 APPROVAL (ACC)")

            tabs_labels.extend([
                "📈 Produktivitas",
                "🧲 Leads & Interest",
                "💬 Review & Feedback",
                "🖼️ Galeri Bukti",
                "📦 Master Data",
                "⚙️ Config Staff",
                "🗑️ Hapus Akun",
                "⚡ SUPER EDITOR"
            ])

            all_tabs = st.tabs(tabs_labels)
            tab_ptr = 0  # Pointer untuk melacak index tab

            # -----------------------------------------------------------
            # 1. TAB APPROVAL (HANYA MANAGER)
            # -----------------------------------------------------------
            if is_manager:
                with all_tabs[tab_ptr]:
                    st.markdown("### 🔔 Pusat Persetujuan Manager")
                    pending_data = get_pending_approvals()
                    if not pending_data:
                        st.info("✅ Tidak ada data yang menunggu persetujuan.")
                    else:
                        for i, req in enumerate(pending_data):
                            with st.container(border=True):
                                c_h1, c_h2 = st.columns([3, 1])
                                with c_h1:
                                    st.markdown(
                                        f"👤 **{req['Requestor']}** mengajukan perubahan pada `{req['Target Sheet']}`")
                                    st.info(f"📝 Alasan: {req['Reason']}")
                                with c_h2:
                                    st.caption(f"📅 {req['Timestamp']}")

                                # Tampilkan Perbandingan Data
                                try:
                                    old_json = json.loads(
                                        req.get("Old Data JSON", "{}"))
                                    new_json = json.loads(
                                        req.get("New Data JSON", "{}"))
                                    diff_data = []
                                    for col, val_new in new_json.items():
                                        val_old = old_json.get(col, "")
                                        if str(val_new).strip() != str(val_old).strip():
                                            diff_data.append(
                                                {"Kolom": col, "Data Lama": val_old, "Data Baru": val_new})
                                    if diff_data:
                                        st.table(pd.DataFrame(diff_data))
                                except:
                                    st.warning(
                                        "Detail perubahan tidak dapat ditampilkan.")

                                # Tombol Approve/Reject
                                b1, b2 = st.columns(2)
                                if b1.button("✅ SETUJUI SEKARANG", key=f"btn_acc_{i}", type="primary", use_container_width=True):
                                    ok, m = execute_approval(
                                        i, "APPROVE", st.session_state["user_name"])
                                    if ok:
                                        st.success("Data berhasil diupdate!")
                                        time.sleep(1)
                                        st.rerun()

                                with b2:
                                    with st.popover("❌ TOLAK REQUEST", use_container_width=True):
                                        alasan_t = st.text_area(
                                            "Berikan alasan penolakan:", key=f"t_area_{i}")
                                        if st.button("Konfirmasi Tolak", key=f"btn_rej_{i}", type="primary", use_container_width=True):
                                            execute_approval(
                                                i, "REJECT", st.session_state["user_name"], alasan_t)
                                            st.warning(
                                                "Request telah ditolak.")
                                            time.sleep(1)
                                            st.rerun()
                tab_ptr += 1


            # -----------------------------------------------------------
            # 2. TAB PRODUKTIVITAS (PLOTLY CHART + AI GEMINI)
            # -----------------------------------------------------------
            with all_tabs[tab_ptr]:
                st.markdown("### 🚀 Analisa Kinerja Tim")
                if not df_all.empty:
                    d_opt = st.selectbox(
                        "Lihat Data:", [7, 14, 30, 90], index=2, key="d_opt_prod")
                    cutoff = datetime.now(
                        tz=TZ_JKT).date() - timedelta(days=d_opt)
                    df_f = df_all[df_all["Tanggal_Date"] >= cutoff]

                    col_m1, col_m2 = st.columns(2)
                    with col_m1:
                        st.markdown("#### Total Laporan per Staf")
                        report_counts = df_f[COL_NAMA].value_counts()
                        st.bar_chart(report_counts)
                    with col_m2:
                        if HAS_PLOTLY:
                            fig = px.pie(df_f, names="Kategori_Aktivitas",
                                         title="Proporsi Jenis Aktivitas", hole=0.3)
                            st.plotly_chart(fig, use_container_width=True)

                    # --- INTEGRASI AI GEMINI UNTUK DESKTOP ---
                    st.divider()
                    st.markdown("#### 🤖 AI Management Insight")
                    with st.spinner("Asisten Pak Nugroho sedang meninjau kinerja tim..."):
                        # Penyiapan Data Non-Visual
                        staf_stats_str = json.dumps(report_counts.to_dict(), indent=2)
                        
                        # 1. Konstruksi Prompt Terstruktur (Meniru format [META], [SYSTEM], [TASK])
                        full_prompt = f"""
                        [CONTEXT_DATA]
                        Nama Pemimpin: Pak Nugroho
                        Total Laporan Masuk: {len(df_f)}
                        Data Statistik Staf: {staf_stats_str}

                        [SYSTEM_INSTRUCTION]
                        Kamu adalah asisten kepercayaan Pak Nugroho. Gunakan bahasa Indonesia yang santun, cerdas, namun tetap membumi agar mudah dipahami secara awam. 

                        PANDUAN PENULISAN:
                        1. Gunakan bahasa yang enak dibaca dan berwibawa. Jangan gunakan istilah teknis yang terlalu berat dan jangan gunakan simbol em-dash atau sejenisnya.
                        2. JANGAN pernah menyebutkan target angka kunjungan mingguan (seperti angka 48).
                        3. Gunakan Logika Perbandingan Kompetitor: Jelaskan bahwa saat sales di perusahaan lain mungkin hari ini masih sibuk urusan kantor, terjebak administrasi, atau baru menyusun rencana, tim Pak Nugroho sudah mencatatkan langkah nyata di lapangan.
                        4. Gunakan Teori Keunggulan Awal: Tekankan bahwa laporan yang masuk di awal waktu jauh lebih berharga karena memberikan Bapak data nyata tentang kondisi pasar saat ini untuk segera diambil keputusannya.
                        5. Jika jumlah laporan masih sedikit, gunakan sudut pandang Kualitas: Jelaskan bahwa tim sedang melakukan pendekatan yang mendalam ke klien kunci, sehingga interaksinya lebih bermutu dibandingkan sekadar kunjungan formalitas yang cepat.
                        6. Berikan apresiasi kepada staf yang sudah mengirim laporan (sebutkan namanya) sebagai bukti bahwa mereka lebih sigap dan tanggap dibanding rata-rata sales di luar sana.
                        7. JANGAN PERNAH mengaku sebagai AI atau Gemini. Tunjukkan rasa hormat dan semangat tinggi untuk mendukung kepemimpinan Pak Nugroho.

                        [TASK]
                        Berikan analisis kinerja tim Sales kepada Pak Nugroho secara naratif dan kreatif berdasarkan data laporan yang terkumpul hari ini.
                        """

                        # 2. Eksekusi Pemanggilan dengan Mekanisme Fallback (Anti-Gagal)
                        ai_reply = ""
                        last_error = ""
                        
                        for model_name in MODEL_FALLBACKS:
                            try:
                                if SDK == "new":
                                    resp = client_ai.models.generate_content(model=model_name, contents=full_prompt)
                                    ai_reply = resp.text
                                else:
                                    model = genai_legacy.GenerativeModel(model_name)
                                    resp = model.generate_content(full_prompt)
                                    ai_reply = resp.text
                                
                                if ai_reply: break # Jika berhasil, keluar dari perulangan model
                            except Exception as e:
                                last_error = str(e)
                                continue # Coba model berikutnya jika model ini gagal

                        # 3. Tampilkan Hasil
                        if ai_reply:
                            st.info(ai_reply)
                        else:
                            st.error(f"⚠️ Gagal mendapatkan insight setelah mencoba semua model. Error terakhir: {last_error}")

            # -----------------------------------------------------------
            # 3. TAB LEADS & INTEREST (EXPORT ENABLED)
            # -----------------------------------------------------------
            with all_tabs[tab_ptr]:
                st.markdown("### 🧲 Leads Management")
                if not df_all.empty and COL_INTEREST in df_all.columns:
                    sel_in = st.radio("Pilih Tingkat Interest:", [
                                      "Under 50% (A)", "50-75% (B)", "75%-100%"], horizontal=True)
                    df_leads = df_all[df_all[COL_INTEREST].astype(
                        str).str.strip() == sel_in]
                    st.dataframe(df_leads[[COL_TIMESTAMP, COL_NAMA, COL_NAMA_KLIEN,
                                 COL_KONTAK_KLIEN, COL_KESIMPULAN]], use_container_width=True)

                    if HAS_OPENPYXL and not df_leads.empty:
                        xb = df_to_excel_bytes(df_leads, sheet_name="Leads")
                        st.download_button(
                            "⬇️ Download Leads (Excel)", data=xb, file_name=f"leads_{sel_in}.xlsx")
            tab_ptr += 1

            # -----------------------------------------------------------
            # 4. TAB REVIEW & FEEDBACK
            # -----------------------------------------------------------
            with all_tabs[tab_ptr]:
                st.markdown("### 💬 Review & Beri Feedback")
                if not df_all.empty:
                    # Ambil 10 laporan terbaru
                    for i, r in df_all.sort_values(by=COL_TIMESTAMP, ascending=False).head(10).iterrows():
                        with st.container(border=True):
                            st.markdown(
                                f"**{r[COL_NAMA]}** | {r[COL_TIMESTAMP]} | 📍 {r[COL_TEMPAT]}")
                            st.write(f"📝 {r[COL_DESKRIPSI]}")
                            f_input = st.text_input(
                                "Kirim masukan ke staf:", key=f"f_in_{i}")
                            if st.button("Kirim Masukan", key=f"f_btn_{i}"):
                                ok, m = kirim_feedback_admin(
                                    r[COL_NAMA], str(r[COL_TIMESTAMP]), f_input)
                                if ok:
                                    st.success("Feedback terkirim!")
                                    st.rerun()
            tab_ptr += 1

            # -----------------------------------------------------------
            # 5. TAB GALERI BUKTI
            # -----------------------------------------------------------
            with all_tabs[tab_ptr]:
                st.markdown("### 🖼️ Galeri Foto Aktivitas")
                if not df_all.empty:
                    df_img = df_all[df_all[COL_LINK_FOTO].str.contains(
                        "http", na=False)].head(12)
                    c_gal = st.columns(3)
                    for idx, row in enumerate(df_img.to_dict("records")):
                        with c_gal[idx % 3]:
                            img_clean = row[COL_LINK_FOTO].replace(
                                "www.dropbox.com", "dl.dropboxusercontent.com").replace("?dl=0", "")
                            st.image(img_clean, use_container_width=True,
                                     caption=f"{row[COL_NAMA]} @ {row[COL_TEMPAT]}")
            tab_ptr += 1

            # -----------------------------------------------------------
            # 6. TAB MASTER DATA
            # -----------------------------------------------------------
            with all_tabs[tab_ptr]:
                st.markdown("### 📦 Database Utama")
                st.dataframe(df_all, use_container_width=True)
            tab_ptr += 1

            # -----------------------------------------------------------
            # 7. TAB CONFIG STAFF (TAMBAH STAF)
            # -----------------------------------------------------------
            with all_tabs[tab_ptr]:
                st.markdown("### 👥 Kelola Personel & Tim")
                with st.form("form_add_staf_new"):
                    new_st_name = st.text_input("Nama Staf Baru:")
                    if st.form_submit_button("➕ Tambahkan ke Sistem"):
                        if new_st_name:
                            ok, msg = tambah_staf_baru(new_st_name)
                            if ok:
                                st.success("Staf ditambahkan!")
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error(msg)

                st.divider()
                st.markdown("#### ⚙️ Konfigurasi Tim (Departemen)")
                df_tm = load_team_config()
                st.dataframe(df_tm, use_container_width=True)
            tab_ptr += 1

            # -----------------------------------------------------------
            # 8. TAB HAPUS AKUN (FITUR KHUSUS DARI CODE PERTAMA)
            # -----------------------------------------------------------
            with all_tabs[tab_ptr]:
                st.markdown("### 🗑️ Hapus Personel")
                st.error(
                    "⚠️ Tindakan ini permanen. Nama staf akan hilang dari daftar pelapor.")
                list_staf_del = get_daftar_staf_terbaru()
                nama_hapus = st.selectbox("Pilih nama yang akan dihapus:", [
                                          "-- Pilih --"] + list_staf_del, key="sb_del")
                confirm_del = st.checkbox(
                    "Saya mengonfirmasi penghapusan ini.")
                if st.button("🔥 HAPUS PERMANEN", type="primary", use_container_width=True):
                    if nama_hapus != "-- Pilih --" and confirm_del:
                        ok, m = hapus_staf_by_name(nama_hapus)
                        if ok:
                            force_audit_log(
                                st.session_state["user_name"], "❌ DELETE USER", "Config_Staf", f"Menghapus staf: {nama_hapus}", "-")
                            st.success(m)
                            st.cache_data.clear()
                            time.sleep(1)
                            st.rerun()
                    else:
                        st.error(
                            "Pilih nama dan centang konfirmasi terlebih dahulu.")
            tab_ptr += 1

            # -----------------------------------------------------------
            # 9. TAB SUPER EDITOR (THE MASTER KEY)
            # -----------------------------------------------------------
            with all_tabs[tab_ptr]:
                st.markdown("### ⚡ Super Admin Editor")
                st.caption(
                    "Gunakan tab ini untuk memperbaiki data lama secara masal.")
                map_s = {"Laporan": "Laporan Kegiatan Harian",
                         "Closing": SHEET_CLOSING_DEAL, "Payment": SHEET_PEMBAYARAN}
                s_target_label = st.selectbox(
                    "Pilih Tabel Data:", list(map_s.keys()))
                s_target_name = map_s[s_target_label]

                if st.button("📂 Ambil Data"):
                    try:
                        ws_edit = spreadsheet.worksheet(s_target_name)
                        st.session_state["df_editor_raw"] = pd.DataFrame(
                            ws_edit.get_all_records())
                        st.session_state["df_editor_name"] = s_target_name
                    except:
                        st.error("Tabel tidak ditemukan.")

                if "df_editor_raw" in st.session_state:
                    st.info(
                        f"Sedang mengedit: **{st.session_state['df_editor_name']}**")
                    alasan_edit = st.text_input(
                        "📝 Alasan Edit (Wajib):", key="alasan_super_desk")

                    # Data Editor
                    edited_result = st.data_editor(
                        st.session_state["df_editor_raw"], use_container_width=True, num_rows="dynamic")

                    if is_manager:
                        if st.button("💾 SIMPAN PERUBAHAN (Manager Direct)", type="primary", use_container_width=True):
                            if alasan_edit:
                                ws_final = spreadsheet.worksheet(
                                    st.session_state["df_editor_name"])
                                ws_final.clear()
                                data_push = [edited_result.columns.tolist(
                                )] + edited_result.astype(str).values.tolist()
                                ws_final.update(
                                    range_name="A1", values=data_push, value_input_option="USER_ENTERED")
                                force_audit_log(st.session_state["user_name"], "✅ SUPER UPDATE",
                                                st.session_state['df_editor_name'], alasan_edit, "Update masal")
                                st.success("Database berhasil diperbarui!")
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("Alasan harus diisi.")
                    else:
                        if st.button("📤 AJUKAN KE MANAGER (Admin Request)", type="primary", use_container_width=True):
                            if alasan_edit:
                                changes_list = compare_and_get_changes(
                                    st.session_state["df_editor_raw"], edited_result)
                                if changes_list:
                                    for c in changes_list:
                                        submit_change_request(st.session_state["df_editor_name"], c['row_idx'], edited_result.iloc[c['row_idx']],
                                                              st.session_state["df_editor_raw"].iloc[c['row_idx']], alasan_edit, st.session_state["user_name"])
                                    st.success(
                                        "Permintaan perubahan dikirim ke Manager!")
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.warning("Tidak ada data yang berubah.")
                            else:
                                st.error("Alasan harus diisi.")

        render_section_watermark()
