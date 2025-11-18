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
import streamlit_authenticator as stauth # --- PERUBAHAN AUTH ---

# --- KONFIGURASI HALAMAN ---
st.set_page_config(
    page_title="Aplikasi Laporan Kegiatan Harian",
    page_icon="✅",
    layout="wide"
)

# --- KONFIGURASI GOOGLE API & DROPBOX ---
NAMA_GOOGLE_SHEET = "Laporan Kegiatan Harian"
FOLDER_DROPBOX = "/Laporan_Kegiatan_Harian"

# --- KONFIGURASI NAMA KOLOM (SUMBER KEBENARAN) ---
COL_TIMESTAMP = "Timestamp"
COL_NAMA = "Nama"
COL_TEMPAT = "Tempat Dikunjungi"
COL_DESKRIPSI = "Deskripsi"
COL_LINK_FOTO = "Link Foto"
COL_LINK_SOSMED = "Link Sosmed" 

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
spreadsheet = None
dbx = None

# 1. Koneksi Google Sheets (Data Teks disimpan di sini)
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
    st.info("PENTING: Pastikan Google Sheet sudah dibagikan ke email Service Account (lihat di secrets.toml bagian client_email) dan diberi akses 'Editor'.")

# 2. Koneksi Dropbox (Foto disimpan di sini)
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
# (Semua fungsi helper Anda: get_or_create_worksheet, upload_ke_dropbox, 
# simpan_ke_sheet, load_data tetap SAMA, tidak perlu diubah)

@st.cache_resource(ttl=60) 
def get_or_create_worksheet(nama_worksheet):
    # ... (Kode Anda tidak berubah)
    try:
        worksheet = spreadsheet.worksheet(nama_worksheet)
        headers_di_sheet = worksheet.row_values(1)
        if headers_di_sheet != NAMA_KOLOM_STANDAR:
            cell_list = worksheet.range(1, 1, 1, len(NAMA_KOLOM_STANDAR))
            for i, header_val in enumerate(NAMA_KOLOM_STANDAR):
                cell_list[i].value = header_val
            worksheet.update_cells(cell_list)
        
        worksheet.format("C:D", {"wrapStrategy": "WRAP", "verticalAlignment": "TOP"})
        worksheet.format("A:B", {"verticalAlignment": "TOP"})
        worksheet.format("E:F", {"verticalAlignment": "TOP"})
        return worksheet
    
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=nama_worksheet, rows=1, cols=len(NAMA_KOLOM_STANDAR))
        worksheet.append_row(NAMA_KOLOM_STANDAR)
        worksheet.format("C:D", {"wrapStrategy": "WRAP", "verticalAlignment": "TOP"})
        worksK.sheet.format("A:B", {"verticalAlignment": "TOP"})
        worksheet.format("E:F", {"verticalAlignment": "TOP"})
        return worksheet
    except Exception as e:
        print(f"Error di get_or_create_worksheet: {e}")
        raise e

def upload_ke_dropbox(file_obj, nama_staf):
    # ... (Kode Anda tidak berubah)
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
                    raise Exception("Gagal mendapatkan link Dropbox yang sudah ada.")
            else:
                raise e
        return link.url.replace("?dl=0", "?raw=1")
    except Exception as e:
        st.error(f"Error tidak terduga saat upload ke Dropbox: {e}")
        return None

def simpan_ke_sheet(data_list, nama_staf):
    # ... (Kode Anda tidak berubah)
    try:
        worksheet = get_or_create_worksheet(nama_staf) 
        if worksheet:
            worksheet.append_row(data_list)
            return True
        return False
    except Exception as e:
        st.error(f"Error saat mencoba mengakses Sheet '{nama_staf}': {e}")
        return False

@st.cache_data(ttl=60)
def load_data(daftar_staf):
    # ... (Kode Anda tidak berubah)
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
                print(f"PERINGATAN: Gagal memuat data untuk '{nama_staf}'. Error: {e}")
                pass 
        if not all_data:
            return pd.DataFrame(columns=NAMA_KOLOM_STANDAR)
        return pd.DataFrame(all_data)
    except Exception as e:
        print(f"Error fatal saat load_data: {e}")
        raise e 


# --- 1. INISIASI AUTENTIKASI ---
# Ambil data dari st.secrets
credentials = st.secrets["credentials"]

authenticator = stauth.Authenticate(
    credentials,
    "laporan_harian_cookie", # Nama cookie unik
    "abcdef123456",          # Kunci rahasia (ganti dengan string acak)
    cookie_expiry_days=30    # Otorisasi berlaku 30 hari
)

# --- 2. TAMPILKAN UI LOGIN ---
# Ini akan menampilkan form login di tengah layar
authenticator.login()


# --- 3. LOGIKA KONDISIONAL SETELAH LOGIN ---

if st.session_state["authentication_status"] is False:
    st.error('Username/password yang Anda masukkan salah.')
elif st.session_state["authentication_status"] is None:
    st.warning('Silakan masukkan username dan password Anda.')

elif st.session_state["authentication_status"] is True:
    # --- PERUBAHAN AUTH: Aplikasi utama dimulai DI SINI ---
    
    # Tampilkan nama & tombol logout di sidebar
    # "name" adalah nama asli (cth: "Social Media Specialist")
    nama = st.session_state["name"] 
    with st.sidebar:
        st.title(f"Selamat Datang, {nama}!")
        authenticator.logout('Logout', 'main', key='logout_button')

    # --- JUDUL APLIKASI ---
    st.title("✅ Aplikasi Laporan Kegiatan Harian")
    st.write("Silakan masukkan kegiatan yang telah Anda lakukan hari ini.")

    # Hanya tampilkan form jika kedua koneksi berhasil
    if KONEKSI_GSHEET_BERHASIL and KONEKSI_DROPBOX_BERHASIL:

        # --- DAFTAR NAMA STAF (TETAP DIPERLUKAN UNTUK DASBOR) ---
        # Ini adalah daftar SEMUA staf yang datanya akan DITARIK oleh load_data
        NAMA_STAF = [
            "Saya",
            "Social Media Specialist",
            "Deal Maker"
        ]

        # --- 1. FORM INPUT KEGIATAN ---
        st.header("📝 Input Kegiatan Baru")

        # --- PERUBAHAN AUTH: Selectbox nama DIHAPUS ---
        # Kita sudah tahu 'nama' pengguna dari st.session_state["name"]
        # st.selectbox("Pilih Job Desc Anda", NAMA_STAF, ... ) <-- INI DIHAPUS

        with st.form(key="form_kegiatan", clear_on_submit=True):
            
            col1, col2 = st.columns(2)
            
            with col1:
                # --- Fitur Anti-Curang (Sudah Ada) ---
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
                
                # --- PERUBAHAN AUTH: Logika ini sekarang otomatis ---
                # Jika pengguna yang login adalah "Social Media Specialist",
                # maka input link sosmed akan muncul.
                if nama == "Social Media Specialist":
                    link_sosmed_input = st.text_input(
                        "Link Sosmed", 
                        placeholder="Contoh: https://www.instagram.com/p/...", 
                        key="linksosmed"
                    )
            
            with col2:
                tempat_dikunjungi = st.text_input("Tempat yang Dikunjungin", placeholder="Contoh: Klien A, Kantor Cabang", key="tempat")
                
                # --- Fitur Multi-File Upload (Sudah Ada) ---
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

        # --- 2. LOGIKA SETELAH TOMBOL SUBMIT DITEKAN ---
        if submitted:
            
            if not deskripsi:
                st.error("Deskripsi kegiatan wajib diisi!")
            else:
                with st.spinner("Sedang menyimpan laporan Anda..."):
                    
                    # --- Logika Multi-Upload (Sudah Ada) ---
                    list_link_hasil_upload = [] 
                    if list_foto_bukti:
                        for foto in list_foto_bukti:
                            st.info(f"Meng-upload {foto.name}...") 
                            # --- PERUBAHAN AUTH: 'nama' sudah otomatis benar ---
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
                    
                    # --- Logika Timestamp WIB (Sudah Ada) ---
                    zona_waktu_wib = ZoneInfo("Asia/Jakarta")
                    timestamp_sekarang = datetime.now(tz=zona_waktu_wib).strftime('%d-%m-%Y %H:%M:%S')

                    # --- PERUBAHAN AUTH: Logika ini otomatis benar ---
                    if nama == "Social Media Specialist":
                        link_sosmed_final = link_sosmed_input if link_sosmed_input else "-"
                    else:
                        link_sosmed_final = ""

                    # Siapkan data untuk Google Sheets
                    data_row = [
                        timestamp_sekarang,
                        nama, # <-- Ini adalah nama dari st.session_state["name"]
                        tempat_dikunjungi,
                        deskripsi,
                        link_foto_final,
                        link_sosmed_final
                    ]
                    
                    # --- PERUBAHAN AUTH: 'nama' sudah otomatis benar ---
                    if simpan_ke_sheet(data_row, nama): 
                        st.success(f"Laporan untuk {nama} berhasil disimpan!")
                        st.cache_data.clear()
                    # else: Pesan error sudah ditangani


        # --- 3. DASBOR (TABEL LAPORAN) ---
        st.header("📊 Dasbor Laporan Kegiatan")
        
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.rerun()

        try:
            # --- PERUBAHAN AUTH: Kita tetap load data SEMUA STAF ---
            # NAMA_STAF digunakan di sini untuk menarik semua data
            df = load_data(NAMA_STAF)
        
        except Exception as e:
            st.error(f"Gagal memuat data dari Google Sheet. Error: {e}")
            df = pd.DataFrame(columns=NAMA_KOLOM_STANDAR)
            
            
        if df.empty:
            st.info("Belum ada data laporan yang masuk atau gagal memuat data.")
        else:   
            st.subheader("Filter Data")
            col_filter1, col_filter2 = st.columns(2)
            
            if COL_NAMA not in df.columns or COL_TEMPAT not in df.columns:
                st.error(f"Struktur kolom di Google Sheet tidak sesuai. Pastikan ada kolom '{COL_NAMA}' dan '{COL_TEMPAT}'.")
                st.dataframe(df, use_container_width=True)
                st.stop()

            with col_filter1:
                nama_unik = df[COL_NAMA].unique()
                filter_nama = st.multiselect("Filter berdasarkan Nama", options=nama_unik, default=list(nama_unik))
            
            with col_filter2:
                tempat_unik = df[COL_TEMPAT].fillna("").unique()
                filter_tempat = st.multisetolect("Filter berdasarkan Tempat", options=tempat_unik, default=list(tempat_unik))
            
            df_filtered = df.copy() 

            if filter_nama:
                df_filtered = df_filtered[df_filtered[COL_NAMA].isin(filter_nama)]

            if filter_tempat:
                df_filtered = df_filtered[df_filtered[COL_TEMPAT].fillna("").isin(filter_tempat)]

            if not df_filtered.empty:
                try:
                    df_filtered['sort_dt'] = pd.to_datetime(df_filtered[COL_TIMESTAMP], format='%d-%m-%Y %H:%M:%S', errors='coerce')
                    df_filtered = df_filtered.sort_values(by='sort_dt', ascending=False).drop(columns=['sort_dt'])
                except Exception as e:
                    st.warning(f"Gagal mengurutkan data berdasarkan tanggal. Pastikan format tanggal benar. Error: {e}")

            st.subheader("Hasil Laporan Terfilter")
            nama_unik_terfilter = df_filtered[COL_NAMA].unique()

            if not nama_unik_terfilter.any():
                st.info("Tidak ada data yang sesuai dengan filter Anda.")
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


    # Tampilkan pesan jika koneksi gagal (logika ini tetap di dalam 'if login berhasil')
    elif not KONEKSI_GSHEET_BERHASIL:
        st.warning("Aplikasi tidak dapat berjalan karena koneksi Google Sheets gagal.")
    elif not KONEKSI_DROPBOX_BERHASIL:
        st.warning("Aplikasi tidak dapat menerima upload foto karena koneksi Dropbox gagal.")
