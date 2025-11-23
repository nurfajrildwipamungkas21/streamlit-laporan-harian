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
    page_title="Aplikasi Laporan & KPI Sales",
    page_icon="🎯",
    layout="wide"
)

# --- KONFIGURASI GOOGLE API & DROPBOX ---
NAMA_GOOGLE_SHEET = "Laporan Kegiatan Harian"
FOLDER_DROPBOX = "/Laporan_Kegiatan_Harian"

# --- KONFIGURASI NAMA SHEET ---
SHEET_CONFIG_NAMA = "Config_Staf"
SHEET_CONFIG_TARGET = "Config_Target" # <-- TAB BARU UNTUK TARGET

# --- KONFIGURASI NAMA KOLOM ---
COL_TIMESTAMP = "Timestamp"
COL_NAMA = "Nama"
COL_TEMPAT = "Tempat Dikunjungi"
COL_DESKRIPSI = "Deskripsi"
COL_LINK_FOTO = "Link Foto"
COL_LINK_SOSMED = "Link Sosmed"

NAMA_KOLOM_STANDAR = [COL_TIMESTAMP, COL_NAMA, COL_TEMPAT, COL_DESKRIPSI, COL_LINK_FOTO, COL_LINK_SOSMED]

# --- Setup koneksi ---
KONEKSI_GSHEET_BERHASIL = False
KONEKSI_DROPBOX_BERHASIL = False
spreadsheet = None 
dbx = None 

# 1. Koneksi Google Sheets
try:
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open(NAMA_GOOGLE_SHEET)
    KONEKSI_GSHEET_BERHASIL = True
except Exception as e:
    st.error(f"Koneksi ke Google Sheets Gagal: {e}")

# 2. Koneksi Dropbox
try:
    DROPBOX_ACCESS_TOKEN = st.secrets["dropbox"]["access_token"]
    dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
    dbx.users_get_current_account()
    KONEKSI_DROPBOX_BERHASIL = True
except Exception as e:
    st.error(f"Koneksi ke Dropbox Gagal: {e}")

# --- FUNGSI HELPER ---

@st.cache_resource(ttl=60)
def get_or_create_worksheet(nama_worksheet):
    try:
        worksheet = spreadsheet.worksheet(nama_worksheet)
        headers = worksheet.row_values(1)
        if headers != NAMA_KOLOM_STANDAR:
            worksheet.update([NAMA_KOLOM_STANDAR]) # Update header simple
        return worksheet
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=nama_worksheet, rows=1, cols=len(NAMA_KOLOM_STANDAR))
        worksheet.append_row(NAMA_KOLOM_STANDAR)
        return worksheet
    except Exception as e:
        raise e

@st.cache_data(ttl=60)
def get_daftar_staf_terbaru():
    """Mengambil daftar nama staf untuk dropdown"""
    default_staf = ["Saya"]
    try:
        try:
            ws = spreadsheet.worksheet(SHEET_CONFIG_NAMA)
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_NAMA, rows=100, cols=2)
            ws.append_row(["Daftar Nama Staf"])
            for nama in default_staf: ws.append_row([nama])
            return default_staf

        nama_dari_sheet = ws.col_values(1)
        if len(nama_dari_sheet) > 0 and nama_dari_sheet[0] == "Daftar Nama Staf":
            nama_dari_sheet.pop(0)
        return nama_dari_sheet if nama_dari_sheet else default_staf
    except:
        return default_staf 

def tambah_staf_baru_ke_sheet(nama_baru):
    try:
        try:
            ws = spreadsheet.worksheet(SHEET_CONFIG_NAMA)
        except:
            ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_NAMA, rows=100, cols=1)
            ws.append_row(["Daftar Nama Staf"])
            
        existing = ws.col_values(1)
        if any(nama_baru.lower() == e.lower() for e in existing):
            return False, "Nama sudah ada!"
        ws.append_row([nama_baru])
        
        # --- UPDATE JUGA SHEET TARGET SECARA OTOMATIS ---
        try:
            ws_target = spreadsheet.worksheet(SHEET_CONFIG_TARGET)
        except:
            ws_target = spreadsheet.add_worksheet(title=SHEET_CONFIG_TARGET, rows=100, cols=3)
            ws_target.append_row(["Nama", "Target Mingguan", "Target Bulanan"])
        
        # Cek apakah nama sudah ada di sheet target
        existing_target = ws_target.col_values(1)
        if not any(nama_baru.lower() == e.lower() for e in existing_target):
            # Tambah default target: 0
            ws_target.append_row([nama_baru, 5, 20]) # Default 5/minggu, 20/bulan
            
        return True, f"Berhasil menambahkan '{nama_baru}'."
    except Exception as e:
        return False, f"Gagal menyimpan: {e}"

# --- FUNGSI BARU: LOAD KPI TARGET ---
@st.cache_data(ttl=60)
def load_target_kpi():
    """Mengambil data target dari sheet Config_Target"""
    try:
        try:
            ws = spreadsheet.worksheet(SHEET_CONFIG_TARGET)
        except gspread.WorksheetNotFound:
            # Buat baru jika belum ada
            ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_TARGET, rows=100, cols=3)
            ws.append_row(["Nama", "Target Mingguan", "Target Bulanan"])
            return pd.DataFrame(columns=["Nama", "Target Mingguan", "Target Bulanan"])
            
        data = ws.get_all_records()
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Error load target: {e}")
        return pd.DataFrame()

def upload_ke_dropbox(file_obj, nama_staf):
    try:
        file_data = file_obj.getvalue()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nama_file_asli = "".join([c for c in file_obj.name if c.isalnum() or c in ('.', '_', '-')])
        nama_folder_staf = "".join([c for c in nama_staf if c.isalnum() or c in (' ', '_', '-')]).replace(' ', '_')
        path = f"{FOLDER_DROPBOX}/{nama_folder_staf}/{timestamp}_{nama_file_asli}"
        
        dbx.files_upload(file_data, path, mode=dropbox.files.WriteMode.add)
        settings = SharedLinkSettings(requested_visibility=RequestedVisibility.public)
        
        try:
            link = dbx.sharing_create_shared_link_with_settings(path, settings=settings)
        except ApiError as e:
            if e.error.is_shared_link_already_exists():
                link = dbx.sharing_list_shared_links(path, direct_only=True).links[0]
            else: raise e
        return link.url.replace("?dl=0", "?raw=1")
    except Exception: return None

def simpan_ke_sheet(data_list, nama_staf):
    try:
        ws = get_or_create_worksheet(nama_staf)
        if ws:
            ws.append_row(data_list)
            return True
        return False
    except: return False

@st.cache_data(ttl=60)
def load_data(daftar_staf):
    try:
        all_data = []
        for nama in daftar_staf:
            try:
                ws = get_or_create_worksheet(nama)
                if ws:
                    d = ws.get_all_records()
                    if d: all_data.extend(d)
            except: pass
        return pd.DataFrame(all_data) if all_data else pd.DataFrame(columns=NAMA_KOLOM_STANDAR)
    except: return pd.DataFrame(columns=NAMA_KOLOM_STANDAR)

# --- APLIKASI UTAMA ---
st.title("🎯 Aplikasi Laporan & KPI Sales")

if KONEKSI_GSHEET_BERHASIL and KONEKSI_DROPBOX_BERHASIL:

    # --- SIDEBAR: Admin & Info ---
    with st.sidebar:
        st.header("⚙️ Pengaturan")
        
        with st.expander("➕ Tambah Tim Baru"):
            with st.form("form_tambah_staf", clear_on_submit=True):
                nama_karyawan = st.text_input("Nama", placeholder="Riky")
                jabatan_karyawan = st.text_input("Jabatan", placeholder="Sales")
                if st.form_submit_button("Simpan"):
                    if nama_karyawan and jabatan_karyawan:
                        nama_gabungan = f"{nama_karyawan} ({jabatan_karyawan})"
                        with st.spinner("Menyimpan..."):
                            sukses, pesan = tambah_staf_baru_ke_sheet(nama_gabungan)
                            if sukses:
                                st.success(pesan)
                                st.cache_data.clear()
                                st.rerun()
                            else: st.error(pesan)
                    else: st.warning("Isi semua data.")

        st.info("💡 **Info Lead:**\nUntuk mengubah angka target, silakan edit langsung file Google Sheet pada tab 'Config_Target'.")

    # --- LOAD DATA ---
    NAMA_STAF = get_daftar_staf_terbaru()
    
    # --- FORM INPUT ---
    st.header("📝 Input Aktivitas")
    nama = st.selectbox("Pilih Nama (Job Desc)", NAMA_STAF, key="selector")

    with st.form(key="form_kegiatan", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            today = datetime.now(tz=ZoneInfo("Asia/Jakarta")).date()
            tgl = st.date_input("Tanggal", value=today, max_value=today, min_value=today-timedelta(days=1))
            
            sosmed = ""
            if "Social Media Specialist" in nama:
                sosmed = st.text_input("Link Konten", placeholder="URL Instagram/TikTok")

        with c2:
            tempat = st.text_input("Tempat / Klien", placeholder="Nama Klien / Lokasi")
            foto_list = st.file_uploader("Foto Bukti", type=['jpg','png'], accept_multiple_files=True)

        desk = st.text_area("Deskripsi", placeholder="Hasil pertemuan / aktivitas...")
        
        if st.form_submit_button("Submit Laporan"):
            if not desk: st.error("Deskripsi wajib diisi!")
            else:
                with st.spinner("Upload & Simpan..."):
                    link_foto = "\n".join([upload_ke_dropbox(f, nama) for f in foto_list]) if foto_list else "-"
                    link_sosmed = sosmed if sosmed else "-" if "Social Media Specialist" in nama else ""
                    
                    row = [
                        datetime.now(tz=ZoneInfo("Asia/Jakarta")).strftime('%d-%m-%Y %H:%M:%S'),
                        nama, tempat, desk, link_foto, link_sosmed
                    ]
                    
                    if simpan_ke_sheet(row, nama):
                        st.success("Laporan tersimpan!")
                        st.cache_data.clear()

    # --- DASHBOARD & KPI ---
    st.divider()
    st.header("📊 Dashboard & Pencapaian Target")
    
    if st.button("🔄 Refresh Data KPI"):
        st.cache_data.clear()
        st.rerun()

    df = load_data(NAMA_STAF)
    df_target = load_target_kpi()

    if not df.empty:
        # Konversi tanggal
        df['dt'] = pd.to_datetime(df[COL_TIMESTAMP], format='%d-%m-%Y %H:%M:%S', errors='coerce')
        
        # Filter Minggu Ini & Bulan Ini
        now = datetime.now(tz=ZoneInfo("Asia/Jakarta"))
        start_week = now - timedelta(days=now.weekday()) # Senin minggu ini
        start_month = now.replace(day=1) # Tanggal 1 bulan ini
        
        # Hitung jumlah laporan per orang
        df_week = df[df['dt'] >= pd.Timestamp(start_week.date())]
        df_month = df[df['dt'] >= pd.Timestamp(start_month.date())]
        
        count_week = df_week[COL_NAMA].value_counts()
        count_month = df_month[COL_NAMA].value_counts()

        # TAMPILAN KPI CARD
        st.subheader("🏆 Leaderboard & KPI (Minggu Ini)")
        
        cols = st.columns(3)
        # Loop hanya untuk orang yang ada di Config Target
        for idx, row_target in df_target.iterrows():
            nama_staff = row_target['Nama']
            target_w = int(row_target['Target Mingguan']) if row_target['Target Mingguan'] else 0
            
            # Realisasi
            realisasi_w = count_week.get(nama_staff, 0)
            
            # Hitung Progress
            if target_w > 0:
                progress = min(realisasi_w / target_w, 1.0)
                delta = f"{realisasi_w - target_w} dari target"
            else:
                progress = 0
                delta = "Belum ada target"

            # Tampilkan di Card
            with cols[idx % 3]:
                st.metric(
                    label=nama_staff,
                    value=f"{realisasi_w} / {target_w} Laporan",
                    delta=f"{int(progress*100)}%",
                    delta_color="normal"
                )
                st.progress(progress)
                if progress >= 1.0:
                    st.caption("✅ Target Mingguan Tercapai!")
                else:
                    st.caption("⚠️ Masih perlu mengejar target.")

        # --- TABEL DETAIL ---
        st.subheader("📂 Detail Laporan")
        st.dataframe(
            df[[COL_TIMESTAMP, COL_NAMA, COL_TEMPAT, COL_DESKRIPSI]].sort_values(by=COL_TIMESTAMP, ascending=False),
            use_container_width=True,
            hide_index=True
        )

    else:
        st.info("Belum ada data laporan masuk.")

elif not KONEKSI_GSHEET_BERHASIL: st.error("Gagal koneksi Google Sheet")
elif not KONEKSI_DROPBOX_BERHASIL: st.error("Gagal koneksi Dropbox")
