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
# Ganti dengan nilai Anda
NAMA_GOOGLE_SHEET = "Laporan Kegiatan Harian"  # NAMA GOOGLE SHEET ANDA
ID_FOLDER_DRIVE = "ID_FOLDER_GOOGLE_DRIVE_ANDA"  # GANTI DENGAN ID FOLDER DRIVE

# --- API KEY DEMO (DITANAM / HARDCODED) ---
# Ini adalah key demo yang Anda berikan
SERVICE_ACCOUNT_DEMO = {
    "type": "service_account",
    "project_id": "sales-hotel-satya-graha",
    "private_key_id": "eef8eb1414691e8c168a0474ea534615bb3a8c92",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCjwLjN70CworXm\nDwoNKtFFptVkj/+mTlFee6Kkmk099UxmWcghWBlz1HoGDiF5NOyLynv1Flrw/Wqm\nCx4X2rK3wlZrIdJCuCRjPWW39wVd46sf6sTLq0LGt0i17gYmuPwS4qo+XM058pfW\nbmvKtfWgeI7hWfQWkBAJUY/mjOHUiq5vZUJ4uS0sDLQYvDucllwX7LMBMhkeY7VL\nJlykCFVxkRZPLlQpRDP3rjN/IZgIxbt3JgHnyc2p+1SGCRWHFcgvrw0C4b+emeXN\nyPS7ttVbFkwHrQ54D3bokrWxJFNG8zEJfynUzLGooRlUOl/cBb14ze9yU0Z3Rxy4\nLWhxTxpLAgMBAAECggEACZ+BEhO7TiYMYSavxTJSS1uQiBuw6bRfMm7lkfLCE0q1\nC1a/XLub/fiV6yvYFRK3y/HsSEI2D0TKiFN4Q0Kbj2eg7c7LrRa73LEg/Hyhw3C7\nqmEri3nXBMKEnVxi3tJZeJVQF+DFlrWGQsMJG/KG1Wqq9YdJwYGvJOe2zdT9oyB4\ndtcu47Q4Fs2/GLBf96HJUaQGjsTvoV5cr8bPeokd7QZ0HNLIwo/92ag7be8NOJ6Y\nH2W6eet7/dCOm7/YJAyti8dQ/4ETnA5bUJGeiBB98l+RAfdBEUjY9W9VKiIJtEfB\nTfHF1Gj0KRZ/0MNtM2VDpqVC2CMXI4iPp9PKZlWpYQKBgQDYnF/t3tfMXdiQJd7w\n0STTzod5A05QZwbWldhU+UIKhmj/ToA8w33zFWBT/hBaRTXp1FP/suUBhvZKULRM\nv8V4y305s729QyZ6PPvA38JbvaU+rvzllb2NZbtIoeh8wjT5RewDUDNuUzz6f2Nd\nlAB/2ROj5dOzx0FyIhayjBhOyQKBgQDBh7YG55PBHqTWMY8foft2Pqu3ZBEAcU2q\nrL3LT6BJAO8G4FDWvjgGA7YSJMY658dAEPMei+JSXXotVCS2FdPQzR3J0Nd+fj1E\nMqleSdRCHwC2vow9w6ka8kVfZvBtS/gHytCg5m97TIdg3QtB0RiUnh25fJgVD6vD\ndvkYCrMGcwKBgQDLOmwGkX2GkMYO+HkjGBalYrBYKXHgnckcq5o8n2AG3/TDFLPc\nTs5sszGdnFFbYHQk3sGwtfeGZ0nMj8uZ9pMfskvbR3hNiiMfrPfHDikcSeIv3Z4J\nCWS+tSPyEXY/Fzb8aU32DdkzOYWMwNhJhAKz8McsjwUBN4F/w9vnyOlFgQKBgGa8\nCi8kI9Vy1QI3kMi0Dm842aMi5bucaiReSupwvJ/EdR7rWT6F/+uBcNe02d5PSACE\nfusKSvx6Tu9dKZfXgnfnSxblXF/z18YqNqqs+paXKqPYB06KXWzGi3kXhi4fE+3f\nBl1Dto53k64h6WXo/+l0/kaHE5yqkv3SXG0c0OhtAoGBAIrLURc/31RdJiHk/iap\naKDwVbifQ23mWF7BctVKQhJoaWu4T7/B07e1ga4giruKwuRWJ7SZw4rZ1yUbA/qG\npCVYnRgPvSJTM7xk3wssUxMmLuSxhJ8k7+7YYmaZUBypd5isoc0TX5GwBDmG2bcF\n7KS6sJVaVxQtK+zC3CuXQEWN\n-----END PRIVATE KEY-----\n",
    "client_email": "bot-laporan-harian@sales-hotel-satya-graha.iam.gserviceaccount.com",
    "client_id": "102931102151388388973",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bot-laporan-harian%40sales-hotel-satya-graha.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}
# -----------------------------------------------


# Setup koneksi
try:
    # Menggunakan API Key Tanam (Hardcoded)
    scopes = ['https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive']
    
    # Menggunakan credentials demo yang ditanam
    creds_dict = SERVICE_ACCOUNT_DEMO
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
    st.error("Koneksi gagal. Periksa API Key Tanam atau pastikan email robot sudah di-share ke Sheet/Folder.")
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

    # --- DAFTAR NAMA STAF ---
    NAMA_STAF = [
        "Saya (Pengawas)", 
        "Social Media Specialist", 
        "Deal Maker"
    ]

    # --- KATEGORI PEKERJAAN ---
    KATEGORI = {
        "Social Media Specialist": ["Konten Plan", "Desain", "Copywriting", "Upload", "Analitik", "Lainnya"],
        "Deal Maker": ["Prospek", "Follow Up", "Meeting", "Negosiasi", "Closing", "Lainnya"],
        "Saya (Pengawas)": ["Supervisi", "Meeting Internal", "Admin", "Lainnya"]
    }

    # --- 1. FORM INPUT KEGIATAN ---
    st.header("📝 Input Kegiatan Baru")

    with st.form(key="form_kegiatan", clear_on_submit=True):
        
        col1, col2 = st.columns(2)
        with col1:
            nama = st.selectbox("Pilih Nama Anda", NAMA_STAF, key="nama")
            tanggal = st.date_input("Tanggal Kegiatan", value=date.today(), key="tanggal")
        
        with col2:
            kategori_pilihan = KATEGORI.get(nama, ["Lainnya"])
            kategori = st.selectbox("Kategori Pekerjaan", kategori_pilihan, key="kategori")
            
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
                        # Jika upload gagal, hentikan proses
                        st.error("Gagal meng-upload foto, laporan tidak disimpan.")
                        st.stop() # Menghentikan eksekusi script

                # 2. Siapkan data untuk Google Sheets
                data_row = [
                    tanggal.strftime('%d-%m-%Y %H:%M:%S'), # Tambah timestamp
                    nama,
                    kategori,
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
                # Ambil nama unik dari kolom 'Nama'
                nama_unik = df['Nama'].unique()
                filter_nama = st.multiselect("Filter berdasarkan Nama", options=nama_unik, default=nama_unik)
            
            with col_filter2:
                # Ambil kategori unik dari kolom 'Kategori'
                kategori_unik = df['Kategori'].unique()
                filter_kategori = st.multiselect("Filter berdasarkan Kategori", options=kategori_unik, default=kategori_unik)
            
            # Terapkan filter
            df_filtered = df[
                df['Nama'].isin(filter_nama) &
                df['Kategori'].isin(filter_kategori)
            ]

            # Tampilkan tabel data
            st.dataframe(df_filtered, use_container_width=True)

    except Exception as e:
        st.error(f"Gagal mengambil data dari Google Sheet: {e}")
