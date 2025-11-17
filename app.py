import streamlit as st
import pandas as pd
from datetime import date, datetime
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
# Folder di Dropbox tempat file akan disimpan (harus dimulai dengan /)
FOLDER_DROPBOX = "/Laporan_Kegiatan_Harian"

# --- KONFIGURASI NAMA KOLOM (SUMBER KEBENARAN) ---
# Ini akan jadi "source of truth" untuk header GSheet & filter DataFrame
COL_TIMESTAMP = "Timestamp"
COL_NAMA = "Nama"
COL_TEMPAT = "Tempat Dikunjungi"
COL_DESKRIPSI = "Deskripsi"
COL_LINK_FOTO = "Link Foto"
# Daftar standar untuk pengecekan header
NAMA_KOLOM_STANDAR = [COL_TIMESTAMP, COL_NAMA, COL_TEMPAT, COL_DESKRIPSI, COL_LINK_FOTO]


# --- Setup koneksi (MENGGUNAKAN st.secrets) ---
KONEKSI_GSHEET_BERHASIL = False
KONEKSI_DROPBOX_BERHASIL = False

# 1. Koneksi Google Sheets (Data Teks disimpan di sini)
try:
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    # Menggunakan kredensial dari [gcp_service_account]
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    
    client_gspread = gspread.authorize(creds)
    
    # Buka Sheet (didefinisikan secara global agar bisa diakses fungsi lain)
    sh = client_gspread.open(NAMA_GOOGLE_SHEET).sheet1
    
    # --- PENGECEKAN HEADER OTOMATIS (IMPROVEMENT) ---
    header_di_sheet = []
    try:
        # Coba baca baris pertama
        header_di_sheet = sh.row_values(1)
    except Exception:
        pass # Biarkan list kosong jika sheet benar-benar baru/kosong

    if not header_di_sheet:
        # SHEET KOSONG: Buat header baru secara otomatis
        sh.append_row(NAMA_KOLOM_STANDAR)
        st.toast("Sheet kosong. Header standar berhasil dibuat!")
    elif header_di_sheet != NAMA_KOLOM_STANDAR:
        # HEADER SALAH: Beri peringatan dan hentikan
        st.error(f"Struktur kolom di Google Sheet tidak cocok!")
        st.error(f"**Yang ada di Sheet:** {header_di_sheet}")
        st.error(f"**Yang diharapkan App:** {NAMA_KOLOM_STANDAR}")
        st.info("SOLUSI: Hapus baris pertama (header) di Google Sheet Anda dan refresh aplikasi ini.")
        st.stop() # Hentikan aplikasi
    
    KONEKSI_GSHEET_BERHASIL = True

except Exception as e:
    # Menampilkan error dan info penting jika gagal koneksi
    st.error(f"Koneksi ke Google Sheets Gagal: {e}")
    st.info("PENTING: Pastikan Google Sheet sudah dibagikan ke email Service Account (lihat di secrets.toml bagian client_email) dan diberi akses 'Editor'.")

# 2. Koneksi Dropbox (Foto disimpan di sini)
try:
    # Menggunakan kredensial dari [dropbox]
    DROPBOX_ACCESS_TOKEN = st.secrets["dropbox"]["access_token"]
    dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
    # Cek koneksi
    dbx.users_get_current_account()
    KONEKSI_DROPBOX_BERHASIL = True
except AuthError:
    st.error("Otentikasi Dropbox gagal. Pastikan Access Token valid.")
except Exception as e:
    st.error(f"Koneksi ke Dropbox Gagal: {e}")

# --- FUNGSI HELPER ---

def upload_ke_dropbox(file_obj):
    """Upload file ke Dropbox dan mengembalikan link langsung (raw=1) yang bisa dibagikan."""
    try:
        # Dapatkan bytes dari file
        file_data = file_obj.getvalue()
        
        # Buat nama file unik menggunakan timestamp untuk menghindari tumpang tindih
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Sanitasi nama file asli
        nama_file_asli = "".join([c for c in file_obj.name if c.isalnum() or c in ('.', '_', '-')])
        
        nama_file_unik = f"{timestamp}_{nama_file_asli}"
        path_dropbox = f"{FOLDER_DROPBOX}/{nama_file_unik}"

        # 1. Upload file
        dbx.files_upload(file_data, path_dropbox, mode=dropbox.files.WriteMode.add)
        
        # 2. Buat shared link publik
        # Setelan agar link bisa dilihat publik secara eksplisit
        settings = SharedLinkSettings(requested_visibility=RequestedVisibility.public)
        
        try:
            link = dbx.sharing_create_shared_link_with_settings(path_dropbox, settings=settings)
        except ApiError as e:
            # Menangani kasus jika link sudah ada (seharusnya jarang terjadi karena nama unik)
            if e.error.is_shared_link_already_exists():
                links = dbx.sharing_list_shared_links(path_dropbox, direct_only=True)
                if links.links:
                    link = links.links[0]
                else:
                    raise Exception("Gagal mendapatkan link Dropbox yang sudah ada.")
            else:
                raise e
        
        # 3. Dapatkan URL langsung
        # Ganti dl=0 (halaman preview) dengan raw=1 (file langsung)
        return link.url.replace("?dl=0", "?raw=1")

    except Exception as e:
        st.error(f"Error tidak terduga saat upload ke Dropbox: {e}")
        return None

def simpan_ke_sheet(data_list):
    """Menyimpan satu baris data ke Google Sheet."""
    try:
        sh.append_row(data_list)
        return True
    except Exception as e:
        st.error(f"Error menyimpan ke Sheet: {e}")
        return False

# Fungsi untuk memuat data dengan caching (meningkatkan performa)
@st.cache_data(ttl=60) # Cache data selama 60 detik
def load_data():
    try:
        # get_all_records() otomatis menggunakan baris pertama sebagai header
        data = sh.get_all_records()
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Gagal memuat data dari Google Sheet: {e}")
        return pd.DataFrame()

# --- JUDUL APLIKASI ---
st.title("✅ Aplikasi Laporan Kegiatan Harian")
st.write("Silakan masukkan kegiatan yang telah Anda lakukan hari ini.")

# Hanya tampilkan form jika kedua koneksi berhasil
if KONEKSI_GSHEET_BERHASIL and KONEKSI_DROPBOX_BERHASIL:

    # --- DAFTAR NAMA STAF ---
    NAMA_STAF = [
        "Saya",
        "Social Media Specialist",
        "Deal Maker"
    ]

    # --- 1. FORM INPUT KEGIATAN ---
    st.header("📝 Input Kegiatan Baru")

    with st.form(key="form_kegiatan", clear_on_submit=True):
        
        col1, col2 = st.columns(2)
        with col1:
            nama = st.selectbox("Pilih Nama Anda", NAMA_STAF, key="nama")
            tanggal = st.date_input("Tanggal Kegiatan", value=date.today(), key="tanggal")
        
        with col2:
            # Perhatikan: 'key' dari input ini (tempat) harus cocok dengan form
            tempat_dikunjungi = st.text_input("Tempat yang Dikunjungin", placeholder="Contoh: Klien A, Kantor Cabang", key="tempat")
            
            foto_bukti = st.file_uploader(
                "Upload Foto Bukti (Opsional)", 
                type=['jpg', 'jpeg', 'png'],
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
                
                # 1. Handle Upload Foto ke Dropbox (jika ada)
                link_foto = "-" # Default jika tidak ada foto
                if foto_bukti is not None:
                    link_foto = upload_ke_dropbox(foto_bukti)
                    if link_foto is None:
                        st.error("Gagal meng-upload foto ke Dropbox, laporan tidak disimpan.")
                        st.stop() 

                # Dapatkan timestamp saat ini untuk disimpan
                timestamp_sekarang = datetime.now().strftime('%d-%m-%Y %H:%M:%S')

                # 2. Siapkan data untuk Google Sheets
                # Urutan list INI harus sama persis dengan NAMA_KOLOM_STANDAR
                data_row = [
                    timestamp_sekarang,
                    nama,
                    tempat_dikunjungi,
                    deskripsi,
                    link_foto
                ]
                
                # 3. Simpan ke Google Sheets
                if simpan_ke_sheet(data_row):
                    st.success(f"Laporan untuk {nama} berhasil disimpan!")
                    # Hapus cache agar data terbaru muncul di dashboard
                    st.cache_data.clear()
                else:
                    st.error("Terjadi kesalahan saat menyimpan data ke Google Sheet.")


    # --- 3. DASBOR (TABEL LAPORAN) ---
    st.header("📊 Dasbor Laporan Kegiatan")
    
    # Tombol refresh manual
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    df = load_data()
        
    if df.empty:
        st.info("Belum ada data laporan yang masuk atau gagal memuat data.")
    else:            
        # Tampilkan filter
        st.subheader("Filter Data")
        col_filter1, col_filter2 = st.columns(2)
        
        # --- IMPROVEMENT: Pengecekan kolom menggunakan variabel global ---
        if COL_NAMA not in df.columns or COL_TEMPAT not in df.columns:
            st.error(f"Struktur kolom di Google Sheet tidak sesuai. Pastikan ada kolom '{COL_NAMA}' dan '{COL_TEMPAT}'.")
            st.dataframe(df, use_container_width=True)
            st.stop()

        with col_filter1:
            # Filter Nama
            nama_unik = df[COL_NAMA].unique()
            # Konversi ke list() agar default multiselect berfungsi dengan baik
            filter_nama = st.multiselect("Filter berdasarkan Nama", options=nama_unik, default=list(nama_unik))
        
        with col_filter2:
            # Filter berdasarkan 'Tempat Dikunjungi'
            tempat_unik = df[COL_TEMPAT].unique()
            filter_tempat = st.multiselect("Filter berdasarkan Tempat", options=tempat_unik, default=list(tempat_unik))
        
        # Terapkan filter
        if filter_nama and filter_tempat:
            df_filtered = df[
                df[COL_NAMA].isin(filter_nama) &
                df[COL_TEMPAT].isin(filter_tempat)
            ].copy() # Gunakan .copy() untuk menghindari SettingWithCopyWarning
        else:
            # Tampilkan tabel kosong jika salah satu filter tidak dipilih
            df_filtered = pd.DataFrame(columns=df.columns)

        # Urutkan data dari yang terbaru
        
        if not df_filtered.empty:
            try:
                # Asumsi kolom pertama adalah Timestamp (COL_TIMESTAMP)
                # Konversi kolom timestamp ke datetime untuk pengurutan yang benar
                df_filtered['sort_dt'] = pd.to_datetime(df_filtered[COL_TIMESTAMP], format='%d-%m-%Y %H:%M:%S', errors='coerce')
                # Urutkan dari yang terbaru dan hapus kolom bantu 'sort_dt'
                df_filtered = df_filtered.sort_values(by='sort_dt', ascending=False).drop(columns=['sort_dt'])
            except Exception as e:
                st.warning(f"Gagal mengurutkan data berdasarkan tanggal. Pastikan format tanggal benar. Error: {e}")

        # Tampilkan tabel data
        # Gunakan column_config untuk membuat link foto bisa diklik
        # --- IMPROVEMENT: Menggunakan variabel global ---
        if COL_LINK_FOTO in df.columns:
            st.dataframe(df_filtered, use_container_width=True, column_config={
                COL_LINK_FOTO: st.column_config.LinkColumn(COL_LINK_FOTO, display_text="Buka Foto")
            })
        else:
            st.dataframe(df_filtered, use_container_width=True)


# Tampilkan pesan jika koneksi gagal
elif not KONEKSI_GSHEET_BERHASIL:
    st.warning("Aplikasi tidak dapat berjalan karena koneksi Google Sheets gagal.")
elif not KONEKSI_DROPBOX_BERHASIL:
    st.warning("Aplikasi tidak dapat menerima upload foto karena koneksi Dropbox gagal.")
