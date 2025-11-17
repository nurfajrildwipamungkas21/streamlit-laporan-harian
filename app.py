import streamlit as st
import pandas as pd
from datetime import date
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io  # Diperlukan untuk menangani file upload

# --- KONFIGURASI HALAMAN ---
st.set_page_config(
    page_title="Aplikasi Laporan Kegiatan Harian",
    page_icon="✅",
    layout="wide"
)

# --- KONFIGSIRASI GOOGLE API ---
# Ini adalah nilai yang Anda berikan
NAMA_GOOGLE_SHEET = "Laporan Kegiatan Harian"
ID_FOLDER_DRIVE = "1aVWhj_x6TWvINldqfnpj9SP3zOjpP66w"  # ID Folder Drive Anda

# --- Setup koneksi (MENGGUNAKAN st.secrets) ---
try:
    # Menggunakan st.secrets untuk koneksi yang aman
    scopes = ['https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive']
    
    # Ambil info credentials dari Streamlit Secrets
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    
    # Otorisasi gspread (Google Sheets)
    client_gspread = gspread.authorize(creds)
    
    # Otorisasi Google Drive API
    client_drive = build('drive', 'v3', credentials=creds)
    
    # Buka Sheet
    sh = client_gspread.open(NAMA_GOOGLE_SHEET).sheet1
    
    KONEKSI_BERHASIL = True

except Exception as e:
    st.error(f"Koneksi ke Google API Gagal: {e}")
    st.error("Pastikan Anda sudah mengatur 'gcp_service_account' di Streamlit Secrets dengan benar.")
    KONEKSI_BERHASIL = False


# --- FUNGSI HELPER (JANGAN DIUBAH) ---

def upload_ke_drive(file_obj, folder_id):
    """Upload file ke Google Drive dan mengembalikan link."""
    try:
        # File object dari Streamlit perlu dibungkus io.BytesIO
        file_io = io.BytesIO(file_obj.getvalue())
        
        file_metadata = {
            'name': file_obj.name,
            'parents': [folder_id]
        }
        media = MediaIoBaseUpload(file_io, mimetype=file_obj.type)
        
        file = client_drive.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, webViewLink'
        ).execute()
        
        # Dapatkan link untuk dilihat (bukan di-download)
        return file.get('webViewLink')
    except Exception as e:
        st.error(f"Error upload ke Drive: {e}")
        return None

def simpan_ke_sheet(data_list):
    """Menyimpan satu baris data ke Google Sheet."""
    try:
        sh.append_row(data_list)
        return True
    except Exception as e:
        st.error(f"Error menyimpan ke Sheet: {e}")
        return False

# --- JUDUL APLIKASI ---
st.title("✅ Aplikasi Laporan Kegiatan Harian")
st.write("Silakan masukkan kegiatan yang telah Anda lakukan hari ini.")

# Hanya tampilkan form jika koneksi berhasil
if KONEKSI_BERHASIL:

    # --- DAFTAR NAMA STAF (SUDAH DIUBAH) ---
    NAMA_STAF = [
        "Saya",  # <-- (Pengawas) sudah dihapus
        "Social Media Specialist", 
        "Deal Maker"
    ]

    # --- KATEGORI PEKERJAAN (Sudah dihapus) ---
    # Variabel KATEGORI dihapus seluruhnya

    # --- 1. FORM INPUT KEGIATAN ---
    st.header("📝 Input Kegiatan Baru")

    with st.form(key="form_kegiatan", clear_on_submit=True):
        
        col1, col2 = st.columns(2)
        with col1:
            nama = st.selectbox("Pilih Nama Anda", NAMA_STAF, key="nama")
            tanggal = st.date_input("Tanggal Kegiatan", value=date.today(), key="tanggal")
        
        with col2:
            # --- SUDAH DIUBAH ---
            # Mengganti Kategori dengan Tempat yang Dikunjungin
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
                
                # 1. Handle Upload Foto (jika ada)
                link_foto = "-" # Default jika tidak ada foto
                if foto_bukti is not None:
                    link_foto = upload_ke_drive(foto_bukti, ID_FOLDER_DRIVE)
                    if link_foto is None:
                        st.error("Gagal meng-upload foto, laporan tidak disimpan.")
                        st.stop() 

                # 2. Siapkan data untuk Google Sheets (SUDAH DIUBAH)
                data_row = [
                    tanggal.strftime('%d-%m-%Y %H:%M:%S'), # Tambah timestamp
                    nama,
                    tempat_dikunjungi,  # <-- Menggunakan variabel baru
                    deskripsi,
                    link_foto
                ]
                
                # 3. Simpan ke Google Sheets
                if simpan_ke_sheet(data_row):
                    st.success(f"Laporan untuk {nama} berhasil disimpan!")
                else:
                    st.error("Terjadi kesalahan saat menyimpan data ke Google Sheet.")


    # --- 3. DASBOR (TABEL LAPORAN) ---
    st.header("📊 Dasbor Laporan Kegiatan")
    
    try:
        # Ambil semua data dari sheet
        data = sh.get_all_records()
        
        if not data:
            st.info("Belum ada data laporan yang masuk.")
        else:
            # Konversi ke DataFrame Pandas
            df = pd.DataFrame(data)
            
            # Tampilkan filter
            st.subheader("Filter Data")
            col_filter1, col_filter2 = st.columns(2)
            
            with col_filter1:
                # Filter Nama
                nama_unik = df['Nama'].unique()
                filter_nama = st.multiselect("Filter berdasarkan Nama", options=nama_unik, default=nama_unik)
            
            with col_filter2:
                # --- SUDAH DIUBAH ---
                # Filter berdasarkan 'Tempat Dikunjungi' (sesuai nama kolom baru di Sheet)
                tempat_unik = df['Tempat Dikunjungi'].unique()
                filter_tempat = st.multiselect("Filter berdasarkan Tempat", options=tempat_unik, default=tempat_unik)
            
            # Terapkan filter (SUDAH DIUBAH)
            df_filtered = df[
                df['Nama'].isin(filter_nama) &
                df['Tempat Dikunjungi'].isin(filter_tempat)
            ]

            # Tampilkan tabel data
            st.dataframe(df_filtered, use_container_width=True)

    except Exception as e:
        st.error(f"Gagal mengambil data dari Google Sheet: {e}")
        st.error("PASTIKAN Anda sudah mengubah nama kolom 'Kategori' menjadi 'Tempat Dikunjungi' di Google Sheet.")
