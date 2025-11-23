import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import gspread
from google.oauth2.service_account import Credentials
import dropbox
from dropbox.exceptions import AuthError, ApiError
from dropbox.sharing import RequestedVisibility, SharedLinkSettings
import re # Import Regex untuk pembersihan teks

# --- KONFIGURASI HALAMAN ---
st.set_page_config(
    page_title="Sales Action Center",
    page_icon="🚀",
    layout="wide"
)

# --- KONFIGURASI ---
NAMA_GOOGLE_SHEET = "Laporan Kegiatan Harian"
FOLDER_DROPBOX = "/Laporan_Kegiatan_Harian"

# Sheet Names
SHEET_CONFIG_NAMA = "Config_Staf"
SHEET_TARGET_TEAM = "Target_Team_Checklist"       
SHEET_TARGET_INDIVIDU = "Target_Individu_Checklist" 

# --- KOLOM LAPORAN HARIAN ---
COL_TIMESTAMP = "Timestamp"
COL_NAMA = "Nama"
COL_TEMPAT = "Tempat Dikunjungi"
COL_DESKRIPSI = "Deskripsi"
COL_LINK_FOTO = "Link Foto"
COL_LINK_SOSMED = "Link Sosmed"
NAMA_KOLOM_STANDAR = [COL_TIMESTAMP, COL_NAMA, COL_TEMPAT, COL_DESKRIPSI, COL_LINK_FOTO, COL_LINK_SOSMED]

# --- KONEKSI ---
KONEKSI_GSHEET_BERHASIL = False
KONEKSI_DROPBOX_BERHASIL = False
spreadsheet = None 
dbx = None 

# 1. Connect GSheet
try:
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open(NAMA_GOOGLE_SHEET)
    KONEKSI_GSHEET_BERHASIL = True
except Exception as e:
    st.error(f"GSheet Error: {e}")

# 2. Connect Dropbox
try:
    dbx = dropbox.Dropbox(st.secrets["dropbox"]["access_token"])
    dbx.users_get_current_account()
    KONEKSI_DROPBOX_BERHASIL = True
except Exception as e:
    st.error(f"Dropbox Error: {e}")

# --- FUNGSI HELPER CORE ---

@st.cache_resource(ttl=60)
def get_or_create_worksheet(nama_worksheet):
    try:
        return spreadsheet.worksheet(nama_worksheet)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=nama_worksheet, rows=1, cols=len(NAMA_KOLOM_STANDAR))
        ws.append_row(NAMA_KOLOM_STANDAR)
        return ws
    except: return None

@st.cache_data(ttl=60)
def get_daftar_staf_terbaru():
    default_staf = ["Saya"]
    try:
        try: ws = spreadsheet.worksheet(SHEET_CONFIG_NAMA)
        except: 
            ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_NAMA, rows=100, cols=1)
            ws.append_row(["Daftar Nama Staf"])
            ws.append_row(["Saya"])
            return default_staf
        
        nama_list = ws.col_values(1)
        if len(nama_list) > 0 and nama_list[0] == "Daftar Nama Staf": nama_list.pop(0)
        return nama_list if nama_list else default_staf
    except: return default_staf

def tambah_staf_baru(nama_baru):
    try:
        try: ws = spreadsheet.worksheet(SHEET_CONFIG_NAMA)
        except: ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_NAMA, rows=100, cols=1)
        if nama_baru in ws.col_values(1): return False, "Nama sudah ada!"
        ws.append_row([nama_baru])
        return True, "Berhasil tambah tim!"
    except Exception as e: return False, str(e)

# --- FUNGSI CHECKLIST TARGET (BULK INPUT SUPPORT) ---

def clean_bulk_input(text_input):
    """
    Memecah teks multi-baris menjadi list target.
    Membersihkan nomor urut (1. 2. dst) atau bullet point (- *)
    """
    lines = text_input.split('\n')
    cleaned_targets = []
    
    for line in lines:
        # Regex: Hapus angka di awal (1. atau 1 ), hapus simbol (- atau *), hapus spasi
        cleaned = re.sub(r'^[\d\.\-\*\s]+', '', line).strip()
        if cleaned:
            cleaned_targets.append(cleaned)
    return cleaned_targets

def load_checklist(sheet_name, columns):
    try:
        try: ws = spreadsheet.worksheet(sheet_name)
        except: 
            ws = spreadsheet.add_worksheet(title=sheet_name, rows=100, cols=len(columns))
            ws.append_row(columns)
            return pd.DataFrame(columns=columns)
        
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        
        # FIX: Pastikan kolom Status (Checkbox) dibaca sebagai boolean
        # Sesuaikan nama kolom dengan header baru yang kita perbaiki
        col_status = "Status" # Nama kolom baru untuk checkbox
        if col_status in df.columns:
             df[col_status] = df[col_status].apply(lambda x: True if str(x).upper() == "TRUE" else False)
             
        return df
    except: return pd.DataFrame(columns=columns)

def save_checklist(sheet_name, df):
    try:
        ws = spreadsheet.worksheet(sheet_name)
        ws.clear()
        # Update header & values
        # Convert boolean to string TRUE/FALSE for GSheets stability
        df_save = df.copy()
        col_status = "Status"
        if col_status in df_save.columns:
            df_save[col_status] = df_save[col_status].apply(lambda x: "TRUE" if x else "FALSE")
            
        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        return True
    except: return False

def add_bulk_targets(sheet_name, base_row_data, targets_list):
    """Menambah banyak baris sekaligus"""
    try:
        try: ws = spreadsheet.worksheet(sheet_name)
        except: return False
        
        rows_to_add = []
        # base_row_data berisi [..., target_placeholder, ...]
        # Kita ganti placeholder dengan text target asli
        
        for t in targets_list:
            new_row = base_row_data.copy()
            # Asumsi: Kolom Target/Misi selalu di index tertentu.
            # Team: [Misi, Mulai, Selesai, Status, Bukti] -> Index 0
            # Individu: [Nama, Target, Mulai, Selesai, Status, Bukti] -> Index 1
            
            if sheet_name == SHEET_TARGET_TEAM:
                new_row[0] = t
            elif sheet_name == SHEET_TARGET_INDIVIDU:
                new_row[1] = t
                
            rows_to_add.append(new_row)
            
        ws.append_rows(rows_to_add)
        return True
    except: return False

# --- FUNGSI UPLOAD ---

def upload_ke_dropbox(file_obj, nama_staf):
    try:
        file_data = file_obj.getvalue()
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        clean_name = "".join([c for c in file_obj.name if c.isalnum() or c in ('.','_')])
        folder = "".join([c for c in nama_staf if c.isalnum() or c in (' ','_')]).replace(' ','_')
        path = f"{FOLDER_DROPBOX}/{folder}/{ts}_{clean_name}"
        dbx.files_upload(file_data, path, mode=dropbox.files.WriteMode.add)
        settings = SharedLinkSettings(requested_visibility=RequestedVisibility.public)
        try: link = dbx.sharing_create_shared_link_with_settings(path, settings=settings)
        except ApiError as e: 
            if e.error.is_shared_link_already_exists():
                link = dbx.sharing_list_shared_links(path, direct_only=True).links[0]
            else: return "-"
        return link.url.replace("?dl=0", "?raw=1")
    except: return "-"

def simpan_laporan_harian(data_list, nama_staf):
    try:
        ws = get_or_create_worksheet(nama_staf)
        ws.append_row(data_list)
        return True
    except: return False

@st.cache_data(ttl=60)
def load_all_reports(daftar_staf):
    all_data = []
    for nama in daftar_staf:
        try:
            ws = get_or_create_worksheet(nama)
            d = ws.get_all_records()
            if d: all_data.extend(d)
        except: pass
    return pd.DataFrame(all_data) if all_data else pd.DataFrame(columns=NAMA_KOLOM_STANDAR)

# --- APLIKASI UTAMA ---

if KONEKSI_GSHEET_BERHASIL and KONEKSI_DROPBOX_BERHASIL:

    # ==========================================
    # SIDEBAR: MANAJEMEN TARGET (Action Plan)
    # ==========================================
    with st.sidebar:
        st.header("🎯 Manajemen Target")
        
        tab_team, tab_individu, tab_admin = st.tabs(["Team", "Pribadi", "Admin"])

        # --- 1. TARGET TEAM (BULANAN/GLOBAL) ---
        with tab_team:
            st.caption("Copy-Paste list target tim di bawah:")
            
            with st.form("add_team_goal", clear_on_submit=True):
                # UBAH KE TEXT AREA UNTUK BULK INPUT
                goal_team_text = st.text_area("Target/Misi Team (Satu per baris)", height=150,
                                             placeholder="1. Closing 50 Klien\n2. Rekrut 2 Sales Baru\n3. Event Pameran JCC")
                
                c1, c2 = st.columns(2)
                today = datetime.now(tz=ZoneInfo("Asia/Jakarta")).date()
                start_d = c1.date_input("Mulai", value=today, key="start_team")
                end_d = c2.date_input("Selesai", value=today + timedelta(days=30), key="end_team")
                
                if st.form_submit_button("➕ Tambah Misi Team"):
                    targets = clean_bulk_input(goal_team_text)
                    if targets:
                        # Template Row: [Misi, Tgl_Mulai, Tgl_Selesai, Status, Bukti]
                        # Status Default = FALSE (String "FALSE" atau Bool False, nanti dihandle fungsi save)
                        base_row = ["", str(start_d), str(end_d), "FALSE", "-"] 
                        
                        if add_bulk_targets(SHEET_TARGET_TEAM, base_row, targets):
                            st.success(f"Berhasil menambah {len(targets)} target!")
                            st.cache_data.clear()
                            st.rerun()
                    else:
                        st.warning("Input target kosong!")

            st.divider()
            st.caption("Checklist Progress Team:")
            
            # Load & Edit Checklist
            # NAMA KOLOM DIPERBAIKI (TIDAK ADA DUPLIKAT 'SELESAI')
            cols_team = ["Misi", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan"]
            df_team = load_checklist(SHEET_TARGET_TEAM, cols_team)
            
            if not df_team.empty:
                edited_team = st.data_editor(
                    df_team,
                    column_config={
                        "Status": st.column_config.CheckboxColumn("Done?", help="Centang jika selesai"),
                        "Misi": st.column_config.TextColumn(disabled=True),
                        "Tgl_Mulai": st.column_config.TextColumn(disabled=True),
                        "Tgl_Selesai": st.column_config.TextColumn(disabled=True),
                        "Bukti/Catatan": st.column_config.TextColumn("Bukti Link/Ket", width="medium")
                    },
                    hide_index=True,
                    key="editor_team"
                )
                if st.button("💾 Simpan Update Team"):
                    save_checklist(SHEET_TARGET_TEAM, edited_team)
                    st.success("Update Disimpan!")
                    st.cache_data.clear()

        # --- 2. TARGET INDIVIDU (MINGGUAN) ---
        with tab_individu:
            st.caption("Action Plan Pribadi (Mingguan)")
            
            NAMA_STAF = get_daftar_staf_terbaru()
            pilih_nama = st.selectbox("Siapa Anda?", NAMA_STAF, key="sidebar_user")
            
            with st.form("add_indiv_goal", clear_on_submit=True):
                # UBAH KE TEXT AREA UNTUK BULK INPUT
                goal_indiv_text = st.text_area("Target Mingguan (Satu per baris)", height=150,
                                              placeholder="Follow up 20 data\nVisit Klien A\nMeeting Internal")
                
                c1, c2 = st.columns(2)
                today = datetime.now(tz=ZoneInfo("Asia/Jakarta")).date()
                start_i = c1.date_input("Mulai", value=today, key="start_indiv")
                end_i = c2.date_input("Selesai", value=today + timedelta(days=7), key="end_indiv")
                
                if st.form_submit_button("➕ Tambah Target Saya"):
                    targets = clean_bulk_input(goal_indiv_text)
                    if targets:
                        # Template Row: [Nama, Target, Tgl_Mulai, Tgl_Selesai, Status, Bukti]
                        base_row = [pilih_nama, "", str(start_i), str(end_i), "FALSE", "-"] 
                        if add_bulk_targets(SHEET_TARGET_INDIVIDU, base_row, targets):
                            st.success(f"Berhasil menambah {len(targets)} target!")
                            st.cache_data.clear()
                            st.rerun()
                    else:
                        st.warning("Input target kosong!")
            
            st.divider()
            st.caption(f"Checklist Milik {pilih_nama}:")
            
            # NAMA KOLOM DIPERBAIKI
            cols_indiv = ["Nama", "Target", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan"]
            df_indiv_all = load_checklist(SHEET_TARGET_INDIVIDU, cols_indiv)
            
            if not df_indiv_all.empty:
                df_indiv_user = df_indiv_all[df_indiv_all["Nama"] == pilih_nama]
                
                if not df_indiv_user.empty:
                    edited_indiv = st.data_editor(
                        df_indiv_user,
                        column_config={
                            "Nama": None, 
                            "Status": st.column_config.CheckboxColumn("Done?", help="Centang jika beres"),
                            "Target": st.column_config.TextColumn(disabled=True),
                             "Bukti/Catatan": st.column_config.TextColumn("Bukti Link/Ket", width="medium")
                        },
                        hide_index=True,
                        key="editor_indiv"
                    )
                    
                    if st.button("💾 Simpan Progress Saya"):
                        df_indiv_all.update(edited_indiv)
                        save_checklist(SHEET_TARGET_INDIVIDU, df_indiv_all)
                        st.success("Progress Tersimpan!")
                        st.cache_data.clear()

        # --- 3. TAB ADMIN ---
        with tab_admin:
            with st.expander("➕ Tambah Karyawan Baru"):
                with st.form("add_staff", clear_on_submit=True):
                    new_name = st.text_input("Nama", placeholder="Riky")
                    new_role = st.text_input("Jabatan", placeholder="Sales")
                    if st.form_submit_button("Tambah"):
                        if new_name and new_role:
                            res, msg = tambah_staf_baru(f"{new_name} ({new_role})")
                            if res: 
                                st.success("Berhasil!")
                                st.cache_data.clear()
                                st.rerun()
                            else: st.error(msg)

    # ==========================================
    # MAIN PAGE: HARIAN & MONITORING
    # ==========================================
    
    st.title("🚀 Sales Action Center")
    st.caption(f"Hari ini: {datetime.now(tz=ZoneInfo('Asia/Jakarta')).strftime('%d %B %Y')}")

    # 1. INPUT HARIAN (Task List Realization)
    with st.container(border=True):
        st.subheader("📝 Task List Harian (Realisasi)")
        st.info("Laporkan apa yang Anda kerjakan **HARI INI** untuk mencapai target mingguan di Sidebar.")
        
        NAMA_STAF_MAIN = get_daftar_staf_terbaru()
        nama_pelapor = st.selectbox("Nama Pelapor", NAMA_STAF_MAIN)

        with st.form("input_harian_task", clear_on_submit=True):
            c1, c2 = st.columns(2)
            
            with c1:
                today_now = datetime.now(tz=ZoneInfo("Asia/Jakarta")).date()
                st.markdown(f"**Tanggal:** `{today_now.strftime('%d-%m-%Y')}`")
                
                sosmed_link = ""
                if "Social Media Specialist" in nama_pelapor:
                    sosmed_link = st.text_input("Link Konten (Sosmed)")
            
            with c2:
                lokasi = st.text_input("Tempat / Klien", placeholder="Misal: Meeting di PT ABC")
                fotos = st.file_uploader("Upload Bukti Foto", accept_multiple_files=True)
            
            deskripsi = st.text_area("Deskripsi Aktivitas Hari Ini", placeholder="Jelaskan detail apa yang dikerjakan...")
            
            if st.form_submit_button("✅ Submit Laporan Hari Ini"):
                if not deskripsi: st.error("Deskripsi wajib diisi!")
                else:
                    with st.spinner("Mengupload & Menyimpan..."):
                        link_foto = "\n".join([upload_ke_dropbox(f, nama_pelapor) for f in fotos]) if fotos else "-"
                        link_sosmed = sosmed_link if sosmed_link else "-" if "Social Media Specialist" in nama_pelapor else ""
                        timestamp = datetime.now(tz=ZoneInfo("Asia/Jakarta")).strftime('%d-%m-%Y %H:%M:%S')
                        
                        row_laporan = [timestamp, nama_pelapor, lokasi, deskripsi, link_foto, link_sosmed]
                        
                        if simpan_laporan_harian(row_laporan, nama_pelapor):
                            st.success("Laporan Harian Berhasil Masuk!")
                            st.cache_data.clear()

    # 2. DASHBOARD MONITORING (CHECKLIST VIEW)
    st.divider()
    st.subheader("📊 Monitoring Target")
    
    col_dash_1, col_dash_2 = st.columns(2)
    
    # LOAD DATA DENGAN NAMA KOLOM BARU
    cols_team = ["Misi", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan"]
    df_view_team = load_checklist(SHEET_TARGET_TEAM, cols_team)
    
    cols_indiv = ["Nama", "Target", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan"]
    df_view_indiv = load_checklist(SHEET_TARGET_INDIVIDU, cols_indiv)

    with col_dash_1:
        st.markdown("#### 🏆 Target Team (Bulan Ini)")
        if not df_view_team.empty:
            # Hitung progress boolean
            total_misi = len(df_view_team)
            selesai_misi = len(df_view_team[df_view_team['Status'] == True])
            if total_misi > 0:
                prog = selesai_misi/total_misi
                st.progress(prog, text=f"Progress: {int(prog*100)}%")
            
            # Tampilkan list simple
            st.dataframe(df_view_team[["Misi", "Status", "Tgl_Selesai"]], hide_index=True, use_container_width=True)
        else:
            st.info("Belum ada target team.")

    with col_dash_2:
        st.markdown("#### ⚡ Target Individu (Minggu Ini)")
        if not df_view_indiv.empty:
            st.dataframe(
                df_view_indiv[["Nama", "Target", "Status"]].sort_values(by="Nama"),
                hide_index=True, 
                use_container_width=True
            )
        else:
            st.info("Belum ada target individu.")

    # 3. RECENT ACTIVITY LOG
    st.divider()
    with st.expander("📂 Riwayat Laporan Harian (Log Aktivitas)", expanded=True):
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()
            
        df_log = load_all_reports(get_daftar_staf_terbaru())
        if not df_log.empty:
            st.dataframe(
                df_log[[COL_TIMESTAMP, COL_NAMA, COL_TEMPAT, COL_DESKRIPSI]].sort_values(by=COL_TIMESTAMP, ascending=False),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("Belum ada laporan harian.")

else:
    st.error("Gagal terhubung ke Database.")
