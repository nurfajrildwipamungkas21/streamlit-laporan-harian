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
    page_title="Aplikasi Laporan Kegiatan Harian",
    page_icon="✅",
    layout="wide"
)

# --- KONFIGURASI GOOGLE API & DROPBOX ---
NAMA_GOOGLE_SHEET = "Laporan Kegiatan Harian"
FOLDER_DROPBOX = "/Laporan_Kegiatan_Harian"

# --- KONFIGURASI BARU: SHEET UNTUK MENYIMPAN DAFTAR NAMA ---
SHEET_CONFIG_NAMA = "Config_Staf"

# --- KONFIGURASI NAMA KOLOM (SUMBER KEBENARAN) ---
COL_TIMESTAMP = "Timestamp"
COL_NAMA = "Nama"
COL_TEMPAT = "Tempat Dikunjungi"
COL_DESKRIPSI = "Deskripsi"
COL_LINK_FOTO = "Link Foto"
COL_LINK_SOSMED = "Link Sosmed"

# Daftar standar untuk pengecekan header
NAMA_KOLOM_STANDAR = [
    COL_TIMESTAMP, 
    COL_NAMA, 
    COL_TEMPAT, 
    COL_DESKRIPSI, 
    COL_LINK_FOTO, 
    COL_LINK_SOSMED
]

# --- Setup koneksi (MENGGUNAKAN st.secrets) ---
KONEKSI_GSHEET_BERHASIL = False
KONEKSI_DROPBOX_BERHASIL = False

# Variabel global untuk koneksi
spreadsheet = None 
dbx = None 

# 1. Koneksi Google Sheets
try:
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open(NAMA_GOOGLE_SHEET)
    KONEKSI_GSHEET_BERHASIL = True
except Exception as e:
    st.error(f"Koneksi ke Google Sheets Gagal: {e}")
    st.info("PENTING: Pastikan Google Sheet sudah dibagikan ke email Service Account.")

# 2. Koneksi Dropbox
try:
    DROPBOX_ACCESS_TOKEN = st.secrets["dropbox"]["access_token"]
    dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
    dbx.users_get_current_account()
    KONEKSI_DROPBOX_BERHASIL = True
except AuthError:
    st.error("Otentikasi Dropbox gagal. Pastikan Access Token valid.")
except Exception as e:
    st.error(f"Koneksi ke Dropbox Gagal: {e}")

# --- FUNGSI HELPER ---

@st.cache_resource(ttl=60)
def get_or_create_worksheet(nama_worksheet):
    """
    Dapatkan worksheet (tab) berdasarkan nama, atau buat baru jika tidak ada.
    """
    try:
        worksheet = spreadsheet.worksheet(nama_worksheet)
        
        # Pengecekan header
        headers_di_sheet = worksheet.row_values(1)
        if headers_di_sheet != NAMA_KOLOM_STANDAR:
            cell_list = worksheet.range(1, 1, 1, len(NAMA_KOLOM_STANDAR))
            for i, header_val in enumerate(NAMA_KOLOM_STANDAR):
                cell_list[i].value = header_val
            worksheet.update_cells(cell_list)
        
        # Formatting
        worksheet.format("C:D", {"wrapStrategy": "WRAP", "verticalAlignment": "TOP"})
        worksheet.format("A:B", {"verticalAlignment": "TOP"})
        worksheet.format("E:F", {"verticalAlignment": "TOP"})
            
        return worksheet
    
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=nama_worksheet, rows=1, cols=len(NAMA_KOLOM_STANDAR))
        worksheet.append_row(NAMA_KOLOM_STANDAR)
        
        worksheet.format("C:D", {"wrapStrategy": "WRAP", "verticalAlignment": "TOP"})
        worksheet.format("A:B", {"verticalAlignment": "TOP"})
        worksheet.format("E:F", {"verticalAlignment": "TOP"})
        return worksheet
    
    except Exception as e:
        print(f"Error di get_or_create_worksheet: {e}")
        raise e

# --- FUNGSI BARU UNTUK UPDATE NAMA KARYAWAN ---

@st.cache_data(ttl=60)
def get_daftar_staf_terbaru():
    """
    Mengambil daftar nama staf dari sheet khusus 'Config_Staf'.
    Jika sheet belum ada, akan dibuatkan defaultnya.
    """
    # DEFAULT HANYA 'Saya'. Yang lain dihapus.
    default_staf = ["Saya"]
    
    try:
        # Coba buka worksheet config
        try:
            ws = spreadsheet.worksheet(SHEET_CONFIG_NAMA)
        except gspread.WorksheetNotFound:
            # Jika tidak ada, buat baru dan isi default
            ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_NAMA, rows=100, cols=2)
            ws.append_row(["Daftar Nama Staf"]) # Header
            for nama in default_staf:
                ws.append_row([nama])
            return default_staf

        # Jika sheet ada, ambil semua nilai di kolom 1
        nama_dari_sheet = ws.col_values(1)
        
        # Hapus header "Daftar Nama Staf" (baris pertama) jika ada
        if len(nama_dari_sheet) > 0 and nama_dari_sheet[0] == "Daftar Nama Staf":
            nama_dari_sheet.pop(0)
            
        # Jika kosong, kembalikan default
        if not nama_dari_sheet:
            return default_staf
            
        return nama_dari_sheet

    except Exception as e:
        print(f"Error loading staf config: {e}")
        return default_staf 

def tambah_staf_baru_ke_sheet(nama_baru):
    """
    Menambahkan nama staf baru ke sheet config
    """
    try:
        try:
            ws = spreadsheet.worksheet(SHEET_CONFIG_NAMA)
        except:
            ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_NAMA, rows=100, cols=1)
            ws.append_row(["Daftar Nama Staf"])
            
        # Cek duplikasi
        existing_names = ws.col_values(1)
        if any(nama_baru.lower() == existing.lower() for existing in existing_names):
            return False, "Nama sudah ada di daftar!"
            
        ws.append_row([nama_baru])
        return True, f"Berhasil menambahkan '{nama_baru}'."
    except Exception as e:
        return False, f"Gagal menyimpan: {e}"

# --- END FUNGSI BARU ---

def upload_ke_dropbox(file_obj, nama_staf):
    try:
        file_data = file_obj.getvalue()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nama_file_asli = "".join([c for c in file_obj.name if c.isalnum() or c in ('.', '_', '-')])
        nama_folder_staf = "".join([c for c in nama_staf if c.isalnum() or c in (' ', '_', '-')]).replace(' ', '_')
        nama_file_unik = f"{timestamp}_{nama_file_asli}"
        path_dropbox = f"{FOLDER_DROPBOX}/{nama_folder_staf}/{nama_file_unik}"

        dbx.files_upload(file_data, path_dropbox, mode=dropbox.files.WriteMode.add)
        
        settings = SharedLinkSettings(requested_visibility=RequestedVisibility.public)
        try:
            link = dbx.sharing_create_shared_link_with_settings(path_dropbox, settings=settings)
        except ApiError as e:
            if e.error.is_shared_link_already_exists():
                links = dbx.sharing_list_shared_links(path_dropbox, direct_only=True)
                if links.links:
                    link = links.links[0]
                else:
                    raise Exception("Gagal mendapatkan link Dropbox.")
            else:
                raise e
        return link.url.replace("?dl=0", "?raw=1")
    except Exception as e:
        st.error(f"Error Dropbox: {e}")
        return None

def simpan_ke_sheet(data_list, nama_staf):
    try:
        worksheet = get_or_create_worksheet(nama_staf) 
        if worksheet:
            worksheet.append_row(data_list)
            return True
        return False
    except Exception as e:
        st.error(f"Error Sheet '{nama_staf}': {e}")
        return False

@st.cache_data(ttl=60)
def load_data(daftar_staf):
    try:
        all_data = []
        for nama_staf in daftar_staf:
            try:
                worksheet = get_or_create_worksheet(nama_staf)
                if worksheet:
                    data = worksheet.get_all_records()  
                    if data:
                        all_data.extend(data) 
            except Exception as e:
                print(f"Warning load '{nama_staf}': {e}")
                pass 
        
        if not all_data:
            return pd.DataFrame(columns=NAMA_KOLOM_STANDAR)

        return pd.DataFrame(all_data)
    except Exception as e:
        print(f"Error fatal load_data: {e}")
        raise e 

# --- APLIKASI UTAMA ---
st.title("✅ Aplikasi Laporan Kegiatan Harian")
st.write("Silakan masukkan kegiatan yang telah Anda lakukan hari ini.")

if KONEKSI_GSHEET_BERHASIL and KONEKSI_DROPBOX_BERHASIL:

    # --- FITUR ADMIN: TAMBAH KARYAWAN VIA SIDEBAR ---
    with st.sidebar:
        st.header("⚙️ Pengaturan Karyawan")
        st.info("Tambah karyawan baru dengan format Nama + Jabatan.")
        
        with st.form("form_tambah_staf", clear_on_submit=True):
            # INPUT BARU: Dipecah jadi 2 kolom
            nama_karyawan = st.text_input("Nama Karyawan", placeholder="Contoh: Riky")
            jabatan_karyawan = st.text_input("Jabatan / Job Desc", placeholder="Contoh: Social Media Specialist")
            
            tombol_tambah = st.form_submit_button("Tambahkan")
            
            if tombol_tambah:
                if nama_karyawan and jabatan_karyawan:
                    # GABUNGKAN STRING DI SINI
                    nama_gabungan = f"{nama_karyawan} ({jabatan_karyawan})"
                    
                    with st.spinner("Menyimpan konfigurasi..."):
                        sukses, pesan = tambah_staf_baru_ke_sheet(nama_gabungan)
                        if sukses:
                            st.success(f"Berhasil: {nama_gabungan}")
                            # Hapus cache agar dropdown langsung update
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error(pesan)
                else:
                    st.warning("Nama dan Jabatan harus diisi semua.")

    # --- LOAD NAMA STAF DARI GOOGLE SHEET ---
    NAMA_STAF = get_daftar_staf_terbaru()

    # --- 1. FORM INPUT KEGIATAN ---
    st.header("📝 Input Kegiatan Baru")

    # Dropdown sekarang berisi data dinamis
    nama = st.selectbox(
        "Pilih Nama (Job Desc)", 
        NAMA_STAF, 
        key="nama_job_desc_selector" 
    )

    with st.form(key="form_kegiatan", clear_on_submit=True):
        col1, col2 = st.columns(2)
        
        with col1:
            today_wib = datetime.now(tz=ZoneInfo("Asia/Jakarta")).date()
            yesterday_wib = today_wib - timedelta(days=1)

            tanggal = st.date_input(
                "Tanggal Kegiatan", 
                value=today_wib, 
                min_value=yesterday_wib,
                max_value=today_wib,
                key="tanggal"
            )
            
            link_sosmed_input = "" 
            # LOGIC BARU: Cek apakah mengandung kata kunci
            if "Social Media Specialist" in nama:
                link_sosmed_input = st.text_input(
                    "Link Sosmed", 
                    placeholder="Contoh: https://www.instagram.com/p/...", 
                    key="linksosmed"
                )
        
        with col2:
            tempat_dikunjungi = st.text_input("Tempat yang Dikunjungin", placeholder="Contoh: Klien A, Kantor Cabang", key="tempat")
            
            list_foto_bukti = st.file_uploader(
                "Upload Foto Bukti (Bisa lebih dari 1)",
                type=['jpg', 'jpeg', 'png'],
                accept_multiple_files=True,
                key="foto"
            )

        deskripsi = st.text_area(
            "Deskripsi Lengkap Kegiatan",  
            placeholder="Contoh: Menghubungi 10 prospek baru dari data Pameran.",
            key="deskripsi"
        )
        
        submitted = st.form_submit_button("Submit Laporan")

    # --- 2. PROSES SUBMIT ---
    if submitted:
        if not deskripsi:
            st.error("Deskripsi kegiatan wajib diisi!")
        else:
            with st.spinner("Sedang menyimpan laporan Anda..."):
                list_link_hasil_upload = []
                
                if list_foto_bukti:
                    for foto in list_foto_bukti:
                        st.info(f"Meng-upload {foto.name}...")
                        link = upload_ke_dropbox(foto, nama)
                        if link:
                            list_link_hasil_upload.append(link)
                        else:
                            st.error(f"Gagal meng-upload foto {foto.name}. Laporan dibatalkan.")
                            st.stop()
                
                if list_link_hasil_upload:
                    link_foto_final = "\n".join(list_link_hasil_upload)
                else:
                    link_foto_final = "-"

                zona_waktu_wib = ZoneInfo("Asia/Jakarta")
                timestamp_sekarang = datetime.now(tz=zona_waktu_wib).strftime('%d-%m-%Y %H:%M:%S')

                # LOGIC BARU JUGA DI SINI
                if "Social Media Specialist" in nama:
                    link_sosmed_final = link_sosmed_input if link_sosmed_input else "-"
                else:
                    link_sosmed_final = ""

                data_row = [
                    timestamp_sekarang,
                    nama,
                    tempat_dikunjungi,
                    deskripsi,
                    link_foto_final,
                    link_sosmed_final
                ]
                
                if simpan_ke_sheet(data_row, nama): 
                    st.success(f"Laporan untuk {nama} berhasil disimpan!")
                    st.cache_data.clear()

    # --- 3. DASBOR ---
    st.header("📊 Dasbor Laporan Kegiatan")
    
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

    try:
        # Load data menggunakan daftar nama yang terbaru
        df = load_data(NAMA_STAF)
    except Exception as e:
        st.error(f"Gagal memuat data. Error: {e}")
        df = pd.DataFrame(columns=NAMA_KOLOM_STANDAR)
        
    if df.empty:
        st.info("Belum ada data laporan yang masuk.")
    else:   
        st.subheader("Filter Data")
        col_filter1, col_filter2 = st.columns(2)
        
        if COL_NAMA not in df.columns or COL_TEMPAT not in df.columns:
            st.error("Struktur kolom tidak sesuai.")
            st.stop()

        with col_filter1:
            nama_unik = df[COL_NAMA].unique()
            filter_nama = st.multiselect("Filter Nama", options=nama_unik, default=list(nama_unik))
        
        with col_filter2:
            tempat_unik = df[COL_TEMPAT].fillna("").unique()
            filter_tempat = st.multiselect("Filter Tempat", options=tempat_unik, default=list(tempat_unik))
        
        df_filtered = df.copy()

        if filter_nama:
            df_filtered = df_filtered[df_filtered[COL_NAMA].isin(filter_nama)]
        if filter_tempat:
            df_filtered = df_filtered[df_filtered[COL_TEMPAT].fillna("").isin(filter_tempat)]

        if not df_filtered.empty:
            try:
                df_filtered['sort_dt'] = pd.to_datetime(df_filtered[COL_TIMESTAMP], format='%d-%m-%Y %H:%M:%S', errors='coerce')
                df_filtered = df_filtered.sort_values(by='sort_dt', ascending=False).drop(columns=['sort_dt'])
            except:
                pass

        st.subheader("Hasil Laporan Terfilter")
        nama_unik_terfilter = df_filtered[COL_NAMA].unique()

        if not nama_unik_terfilter.any():
            st.info("Tidak ada data yang sesuai filter.")
        else:
            for nama_staf in nama_unik_terfilter:
                data_staf = df_filtered[df_filtered[COL_NAMA] == nama_staf]
                jumlah_laporan = len(data_staf)
                
                with st.expander(f"📁 {nama_staf}     ({jumlah_laporan} Laporan)", expanded=True):
                    column_config = {}
                    if COL_LINK_SOSMED in data_staf.columns:
                        column_config[COL_LINK_SOSMED] = st.column_config.LinkColumn(
                            COL_LINK_SOSMED, display_text="Buka Link"
                        )
                    
                    st.data_editor(
                        data_staf,
                        use_container_width=True,
                        column_config=column_config,
                        disabled=True,
                        key=f"editor_{nama_staf}"
                    )

elif not KONEKSI_GSHEET_BERHASIL:
    st.warning("Koneksi Google Sheets gagal.")
elif not KONEKSI_DROPBOX_BERHASIL:
    st.warning("Koneksi Dropbox gagal.")
