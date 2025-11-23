import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import gspread
from google.oauth2.service_account import Credentials
import io
import dropbox
from dropbox.exceptions import AuthError, ApiError
from dropbox.sharing import RequestedVisibility, SharedLinkSettings

# --- KONFIGURASI HALAMAN ---
st.set_page_config(
    page_title="Sales Command Center",
    page_icon="🎯",
    layout="wide"
)

# --- KONFIGURASI ---
NAMA_GOOGLE_SHEET = "Laporan Kegiatan Harian"
FOLDER_DROPBOX = "/Laporan_Kegiatan_Harian"
SHEET_CONFIG_NAMA = "Config_Staf"
SHEET_CONFIG_TARGET = "Config_Target" 
SHEET_CONFIG_TEAM = "Config_Team_Goal" # Sheet baru untuk target global team

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

try:
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open(NAMA_GOOGLE_SHEET)
    KONEKSI_GSHEET_BERHASIL = True
except Exception as e:
    st.error(f"GSheet Error: {e}")

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
        try:
            ws = spreadsheet.worksheet(SHEET_CONFIG_NAMA)
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
        # 1. Tambah ke Config Nama
        try: ws = spreadsheet.worksheet(SHEET_CONFIG_NAMA)
        except: ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_NAMA, rows=100, cols=1)
        
        if nama_baru in ws.col_values(1): return False, "Nama sudah ada!"
        ws.append_row([nama_baru])

        # 2. Tambah ke Config Target (Default 5)
        try: ws_t = spreadsheet.worksheet(SHEET_CONFIG_TARGET)
        except: 
            ws_t = spreadsheet.add_worksheet(title=SHEET_CONFIG_TARGET, rows=100, cols=2)
            ws_t.append_row(["Nama", "Target_Mingguan"])
        
        existing_t = ws_t.col_values(1)
        if nama_baru not in existing_t:
            ws_t.append_row([nama_baru, 5]) # Default target mingguan

        return True, "Berhasil tambah tim!"
    except Exception as e: return False, str(e)

# --- FUNGSI MANAGERIAL (TARGETING) ---

def get_team_target_bulanan():
    """Mengambil 1 angka target team bulanan"""
    try:
        try: ws = spreadsheet.worksheet(SHEET_CONFIG_TEAM)
        except: 
            ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_TEAM, rows=2, cols=1)
            ws.update_cell(1,1, "Target_Team_Bulanan")
            ws.update_cell(2,1, 100) # Default
            return 100
        
        val = ws.cell(2, 1).value
        return int(val) if val else 100
    except: return 100

def update_team_target_bulanan(angka_baru):
    try:
        ws = spreadsheet.worksheet(SHEET_CONFIG_TEAM)
        ws.update_cell(2, 1, angka_baru)
        return True
    except: return False

def get_individual_targets():
    """Mengambil DataFrame target mingguan per orang"""
    try:
        try: ws = spreadsheet.worksheet(SHEET_CONFIG_TARGET)
        except: 
            ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_TARGET, rows=100, cols=2)
            ws.append_row(["Nama", "Target_Mingguan"])
            return pd.DataFrame(columns=["Nama", "Target_Mingguan"])
            
        data = ws.get_all_records()
        return pd.DataFrame(data)
    except: return pd.DataFrame()

def update_individual_targets(df_edited):
    """Menyimpan editan target individu kembali ke GSheet"""
    try:
        ws = spreadsheet.worksheet(SHEET_CONFIG_TARGET)
        ws.clear()
        # Tulis ulang header & data
        ws.update([df_edited.columns.values.tolist()] + df_edited.values.tolist())
        return True
    except Exception as e: return False

# --- FUNGSI UPLOAD & SIMPAN LAPORAN ---

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

def simpan_laporan(data_list, nama_staf):
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

# --- UI UTAMA ---

if KONEKSI_GSHEET_BERHASIL and KONEKSI_DROPBOX_BERHASIL:

    # --- SIDEBAR (MANAGER AREA) ---
    with st.sidebar:
        st.title("⚙️ Manager Panel")
        
        # 1. Tambah Tim
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

        # 2. Atur Target (FITUR BARU)
        st.divider()
        st.subheader("🎯 Atur Target")
        
        # A. Target Team (Bulanan)
        current_team_target = get_team_target_bulanan()
        new_team_target = st.number_input("Target Team (Bulanan)", value=current_team_target, step=5)
        if new_team_target != current_team_target:
            update_team_target_bulanan(new_team_target)
            st.toast("Target Team Diupdate!", icon="✅")
            st.cache_data.clear()
            st.rerun()

        # B. Target Individu (Mingguan)
        st.caption("Edit Target Mingguan per Staff:")
        df_target_raw = get_individual_targets()
        
        if not df_target_raw.empty:
            # Gunakan Data Editor agar bisa edit langsung seperti Excel
            edited_df = st.data_editor(
                df_target_raw, 
                column_config={
                    "Nama": st.column_config.TextColumn(disabled=True), # Nama jangan diedit
                    "Target_Mingguan": st.column_config.NumberColumn("Target/Minggu", min_value=1, max_value=100)
                },
                hide_index=True,
                key="editor_target"
            )
            
            # Tombol Simpan Perubahan
            if st.button("💾 Simpan Perubahan Target"):
                if update_individual_targets(edited_df):
                    st.success("Target Individu Berhasil Disimpan!")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error("Gagal menyimpan ke Google Sheet.")

    # --- MAIN PAGE ---
    NAMA_STAF = get_daftar_staf_terbaru()
    st.title("🚀 Sales Command Center")
    
    # --- FORM INPUT ---
    with st.expander("📝 Input Laporan Harian", expanded=False):
        nama = st.selectbox("Pilih Nama", NAMA_STAF)
        with st.form("input_harian", clear_on_submit=True):
            c1, c2 = st.columns(2)
            today = datetime.now(tz=ZoneInfo("Asia/Jakarta")).date()
            
            with c1:
                tgl = st.date_input("Tanggal", value=today, max_value=today, min_value=today-timedelta(days=2))
                sosmed = ""
                if "Social Media Specialist" in nama:
                    sosmed = st.text_input("Link Konten")
            with c2:
                tempat = st.text_input("Klien / Tempat")
                fotos = st.file_uploader("Foto Bukti", accept_multiple_files=True)
            
            desc = st.text_area("Deskripsi Kegiatan")
            
            if st.form_submit_button("Kirim Laporan"):
                if not desc: st.error("Deskripsi wajib diisi")
                else:
                    with st.spinner("Mengirim..."):
                        links = "\n".join([upload_ke_dropbox(f, nama) for f in fotos]) if fotos else "-"
                        lsos = sosmed if sosmed else "-" if "Social Media Specialist" in nama else ""
                        row = [datetime.now(tz=ZoneInfo("Asia/Jakarta")).strftime('%d-%m-%Y %H:%M:%S'), nama, tempat, desc, links, lsos]
                        if simpan_laporan(row, nama):
                            st.success("Laporan Masuk!")
                            st.cache_data.clear()

    # --- DASHBOARD & KPI ---
    
    # Refresh logic
    col_ref, _ = st.columns([1, 4])
    if col_ref.button("🔄 Refresh Dashboard"):
        st.cache_data.clear()
        st.rerun()

    # Load All Data
    df_all = load_all_reports(NAMA_STAF)
    target_team_val = get_team_target_bulanan() # Load target team dinamis
    df_targets = get_individual_targets() # Load target individu dinamis

    if not df_all.empty:
        df_all['dt'] = pd.to_datetime(df_all[COL_TIMESTAMP], format='%d-%m-%Y %H:%M:%S', errors='coerce')
        now = datetime.now(tz=ZoneInfo("Asia/Jakarta"))
        
        # Filter Waktu
        start_month = now.replace(day=1, hour=0, minute=0, second=0)
        start_week = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0)
        
        df_month = df_all[df_all['dt'] >= pd.Timestamp(start_month.date())]
        df_week = df_all[df_all['dt'] >= pd.Timestamp(start_week.date())]

        # 1. TEAM MONITOR (BULANAN)
        st.markdown("### 🏆 Team Performance (Bulan Ini)")
        
        total_act_month = len(df_month)
        progress_team = min(total_act_month / target_team_val, 1.0)
        
        met_col1, met_col2 = st.columns([3, 1])
        with met_col1:
            st.progress(progress_team)
        with met_col2:
            st.metric("Pencapaian Team", f"{total_act_month} / {target_team_val}", f"{int(progress_team*100)}%")

        # 2. INDIVIDUAL MONITOR (MINGGUAN)
        st.markdown("### ⚡ Individual Progress (Minggu Ini)")
        
        count_week = df_week[COL_NAMA].value_counts()
        
        # Grid layout untuk card
        cols = st.columns(3)
        for idx, row in df_targets.iterrows():
            p_name = row['Nama']
            # Pastikan kolom target valid angka
            try: t_weekly = int(row['Target_Mingguan'])
            except: t_weekly = 5 
            
            act_weekly = count_week.get(p_name, 0)
            ind_prog = min(act_weekly / t_weekly, 1.0)
            
            with cols[idx % 3]:
                st.metric(
                    label=p_name,
                    value=f"{act_weekly} / {t_weekly}",
                    delta=f"{int(ind_prog*100)}%",
                    delta_color="normal" if ind_prog < 1.0 else "off"
                )
                st.progress(ind_prog)
                if ind_prog >= 1.0: st.caption("🔥 On Fire!")
                else: st.caption("⚠️ Keep Pushing")

        # 3. DATA RECENT
        st.divider()
        st.subheader("📂 History Terkini")
        st.dataframe(
            df_all[[COL_TIMESTAMP, COL_NAMA, COL_TEMPAT, COL_DESKRIPSI]].sort_values(by=COL_TIMESTAMP, ascending=False).head(10),
            use_container_width=True,
            hide_index=True
        )

    else:
        st.info("Belum ada data laporan.")

else:
    st.error("Gagal koneksi database.")
