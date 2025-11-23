import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import gspread
from google.oauth2.service_account import Credentials
import dropbox
from dropbox.exceptions import AuthError, ApiError
from dropbox.sharing import RequestedVisibility, SharedLinkSettings
import re

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

# --- FUNGSI CHECKLIST TARGET ---

def clean_bulk_input(text_input):
    lines = text_input.split('\n')
    cleaned_targets = []
    for line in lines:
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
        
        col_status = "Status"
        if col_status in df.columns:
             df[col_status] = df[col_status].apply(lambda x: True if str(x).upper() == "TRUE" else False)
        return df
    except: return pd.DataFrame(columns=columns)

def save_checklist(sheet_name, df):
    try:
        ws = spreadsheet.worksheet(sheet_name)
        ws.clear()
        df_save = df.copy()
        col_status = "Status"
        if col_status in df_save.columns:
            df_save[col_status] = df_save[col_status].apply(lambda x: "TRUE" if x else "FALSE")
            
        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist())
        return True
    except: return False

def add_bulk_targets(sheet_name, base_row_data, targets_list):
    try:
        try: ws = spreadsheet.worksheet(sheet_name)
        except: return False
        
        rows_to_add = []
        for t in targets_list:
            new_row = base_row_data.copy()
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
    # SIDEBAR: MANAJEMEN TARGET
    # ==========================================
    with st.sidebar:
        st.header("🎯 Manajemen Target")
        
        tab_team, tab_individu, tab_admin = st.tabs(["Team", "Pribadi", "Admin"])

        # 1. TARGET TEAM
        with tab_team:
            st.caption("Input Target Team (Bulk Input)")
            with st.form("add_team_goal", clear_on_submit=True):
                goal_team_text = st.text_area("Target Team (Satu per baris)", height=100)
                c1, c2 = st.columns(2)
                today = datetime.now(tz=ZoneInfo("Asia/Jakarta")).date()
                start_d = c1.date_input("Mulai", value=today, key="start_team")
                end_d = c2.date_input("Selesai", value=today + timedelta(days=30), key="end_team")
                
                if st.form_submit_button("➕ Tambah"):
                    targets = clean_bulk_input(goal_team_text)
                    if targets:
                        base_row = ["", str(start_d), str(end_d), "FALSE", "-"] 
                        if add_bulk_targets(SHEET_TARGET_TEAM, base_row, targets):
                            st.success(f"{len(targets)} target ditambah!")
                            st.cache_data.clear()
                            st.rerun()

        # 2. TARGET INDIVIDU
        with tab_individu:
            st.caption("Input Target Pribadi (Bulk Input)")
            NAMA_STAF = get_daftar_staf_terbaru()
            pilih_nama = st.selectbox("Siapa Anda?", NAMA_STAF, key="sidebar_user")
            
            with st.form("add_indiv_goal", clear_on_submit=True):
                goal_indiv_text = st.text_area("Target Mingguan (Satu per baris)", height=100)
                c1, c2 = st.columns(2)
                today = datetime.now(tz=ZoneInfo("Asia/Jakarta")).date()
                start_i = c1.date_input("Mulai", value=today, key="start_indiv")
                end_i = c2.date_input("Selesai", value=today + timedelta(days=7), key="end_indiv")
                
                if st.form_submit_button("➕ Tambah"):
                    targets = clean_bulk_input(goal_indiv_text)
                    if targets:
                        base_row = [pilih_nama, "", str(start_i), str(end_i), "FALSE", "-"] 
                        if add_bulk_targets(SHEET_TARGET_INDIVIDU, base_row, targets):
                            st.success(f"{len(targets)} target ditambah!")
                            st.cache_data.clear()
                            st.rerun()

        # 3. ADMIN
        with tab_admin:
            with st.expander("➕ Tambah Karyawan"):
                with st.form("add_staff", clear_on_submit=True):
                    new_name = st.text_input("Nama")
                    new_role = st.text_input("Jabatan")
                    if st.form_submit_button("Tambah"):
                        if new_name and new_role:
                            res, msg = tambah_staf_baru(f"{new_name} ({new_role})")
                            if res: 
                                st.success("Berhasil!")
                                st.cache_data.clear()
                                st.rerun()

    # ==========================================
    # MAIN PAGE
    # ==========================================
    
    st.title("🚀 Sales Action Center")
    st.caption(f"Hari ini: {datetime.now(tz=ZoneInfo('Asia/Jakarta')).strftime('%d %B %Y')}")

    # --- 1. MONITORING TARGET (DIPERBAIKI) ---
    st.subheader("📊 Monitoring & Checklist Target")
    
    col_dash_1, col_dash_2 = st.columns(2)
    
    # Load Data
    cols_team = ["Misi", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan"]
    df_team = load_checklist(SHEET_TARGET_TEAM, cols_team)
    
    cols_indiv = ["Nama", "Target", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan"]
    df_indiv_all = load_checklist(SHEET_TARGET_INDIVIDU, cols_indiv)

    # --- PANEL TEAM (KIRI) ---
    with col_dash_1:
        st.markdown("#### 🏆 Target Team")
        
        if not df_team.empty:
            # Hitung Progress Bar Team
            total_team = len(df_team)
            done_team = len(df_team[df_team['Status'] == True])
            prog_team = done_team / total_team if total_team > 0 else 0
            
            st.progress(prog_team, text=f"Pencapaian Team: {int(prog_team*100)}%")
            
            # Tampilkan Editor Agar Bisa Dicetang
            edited_team = st.data_editor(
                df_team,
                column_config={
                    "Status": st.column_config.CheckboxColumn("Done?", width="small"),
                    "Misi": st.column_config.TextColumn(disabled=True),
                    "Bukti/Catatan": st.column_config.TextColumn(width="medium")
                },
                column_order=["Status", "Misi", "Bukti/Catatan"],
                hide_index=True,
                key="editor_dash_team",
                use_container_width=True
            )
            
            # Tombol Simpan Team
            if st.button("💾 Update Progress Team", use_container_width=True):
                save_checklist(SHEET_TARGET_TEAM, edited_team)
                st.toast("Progress Team Berhasil Disimpan!", icon="✅")
                st.cache_data.clear()
                st.rerun()
        else:
            st.info("Belum ada target team.")

    # --- PANEL INDIVIDU (KANAN) ---
    with col_dash_2:
        st.markdown("#### ⚡ Target Individu")
        
        # Filter Nama Agar Progress Bar Akurat Per Orang
        # Gunakan list nama dari config
        list_staff_filter = get_daftar_staf_terbaru()
        
        # Coba auto-select "Saya" atau nama pertama
        filter_nama = st.selectbox("Lihat Progress Siapa?", list_staff_filter, index=0)
        
        if not df_indiv_all.empty:
            # Filter Dataframe berdasarkan nama yg dipilih
            df_indiv_user = df_indiv_all[df_indiv_all["Nama"] == filter_nama]
            
            if not df_indiv_user.empty:
                # Hitung Progress Bar Individu
                total_indiv = len(df_indiv_user)
                done_indiv = len(df_indiv_user[df_indiv_user['Status'] == True])
                prog_indiv = done_indiv / total_indiv if total_indiv > 0 else 0
                
                # Tampilkan Progress Bar
                st.progress(prog_indiv, text=f"Progress {filter_nama}: {int(prog_indiv*100)}%")
                
                # Tampilkan Editor
                edited_indiv = st.data_editor(
                    df_indiv_user,
                    column_config={
                        "Status": st.column_config.CheckboxColumn("Done?", width="small"),
                        "Target": st.column_config.TextColumn(disabled=True),
                        "Bukti/Catatan": st.column_config.TextColumn(width="medium")
                    },
                    column_order=["Status", "Target", "Bukti/Catatan"],
                    hide_index=True,
                    key=f"editor_dash_indiv_{filter_nama}", # Key unik biar gak crash ganti nama
                    use_container_width=True
                )
                
                # Tombol Simpan Individu
                if st.button(f"💾 Update Progress {filter_nama}", use_container_width=True):
                    # Logic simpan: Update baris yang sesuai di DF Utama
                    # Kita pakai index dari edited_indiv untuk update df_indiv_all
                    df_indiv_all.update(edited_indiv)
                    save_checklist(SHEET_TARGET_INDIVIDU, df_indiv_all)
                    st.toast(f"Progress {filter_nama} Disimpan!", icon="✅")
                    st.cache_data.clear()
                    st.rerun()
            else:
                st.info(f"{filter_nama} belum memiliki target aktif.")
        else:
            st.info("Belum ada data target individu sama sekali.")

    # --- 2. INPUT HARIAN ---
    st.divider()
    with st.container(border=True):
        st.subheader("📝 Laporan Harian (Bukti Kerja)")
        
        NAMA_STAF_MAIN = get_daftar_staf_terbaru()
        nama_pelapor = st.selectbox("Nama Pelapor", NAMA_STAF_MAIN, key="pelapor_main")

        with st.form("input_harian_task", clear_on_submit=True):
            c1, c2 = st.columns(2)
            with c1:
                today_now = datetime.now(tz=ZoneInfo("Asia/Jakarta")).date()
                st.markdown(f"**Tanggal:** `{today_now.strftime('%d-%m-%Y')}`")
                sosmed_link = ""
                if "Social Media Specialist" in nama_pelapor:
                    sosmed_link = st.text_input("Link Konten (Sosmed)")
            with c2:
                lokasi = st.text_input("Tempat / Klien")
                fotos = st.file_uploader("Upload Bukti Foto", accept_multiple_files=True)
            
            deskripsi = st.text_area("Deskripsi Aktivitas")
            
            if st.form_submit_button("✅ Submit Laporan"):
                if not deskripsi: st.error("Deskripsi wajib diisi!")
                else:
                    with st.spinner("Proses..."):
                        link_foto = "\n".join([upload_ke_dropbox(f, nama_pelapor) for f in fotos]) if fotos else "-"
                        link_sosmed = sosmed_link if sosmed_link else "-" if "Social Media Specialist" in nama_pelapor else ""
                        ts = datetime.now(tz=ZoneInfo("Asia/Jakarta")).strftime('%d-%m-%Y %H:%M:%S')
                        row = [ts, nama_pelapor, lokasi, deskripsi, link_foto, link_sosmed]
                        if simpan_laporan_harian(row, nama_pelapor):
                            st.success("Tersimpan!")
                            st.cache_data.clear()

    # --- 3. LOG AKTIVITAS ---
    with st.expander("📂 Riwayat Laporan", expanded=False):
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()
        df_log = load_all_reports(get_daftar_staf_terbaru())
        if not df_log.empty:
            st.dataframe(df_log[[COL_TIMESTAMP, COL_NAMA, COL_TEMPAT, COL_DESKRIPSI]].sort_values(by=COL_TIMESTAMP, ascending=False), use_container_width=True, hide_index=True)

else:
    st.error("Gagal terhubung ke Database.")
