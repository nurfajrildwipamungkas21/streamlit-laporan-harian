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
COL_LINK_SOSMED = "Link Sosmed" # --- PERUBAHAN --- Kolom baru ditambahkan

# Daftar standar untuk pengecekan header
NAMA_KOLOM_STANDAR = [
    COL_TIMESTAMP, 
    COL_NAMA, 
    COL_TEMPAT, 
    COL_DESKRIPSI, 
    COL_LINK_FOTO, 
    COL_LINK_SOSMED # --- PERUBAHAN --- Kolom baru ditambahkan
]


# --- Setup koneksi (MENGGUNAKAN st.secrets) ---
KONEKSI_GSHEET_BERHASIL = False
KONEKSI_DROPBOX_BERHASIL = False

# Variabel global untuk koneksi
spreadsheet = None # Akan menampung seluruh file Google Sheet (sebagai "lemari")
dbx = None # Akan menampung koneksi Dropbox

# 1. Koneksi Google Sheets (Data Teks disimpan di sini)
try:
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    # Menggunakan kredensial dari [gcp_service_account]
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    
    gc = gspread.authorize(creds)
    
    # Buka seluruh file Spreadsheet (bukan cuma sheet1)
    spreadsheet = gc.open(NAMA_GOOGLE_SHEET)
    
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

@st.cache_resource(ttl=60) # Cache resource agar tidak buat worksheet berulang-ulang
def get_or_create_worksheet(nama_worksheet):
    """
    Dapatkan worksheet (tab) berdasarkan nama, atau buat baru jika tidak ada.
    Ini adalah "laci" di dalam "lemari" (spreadsheet).
    """
    try:
        # Coba dapatkan worksheet (tab)
        worksheet = spreadsheet.worksheet(nama_worksheet)
        
        # --- PERUBAHAN --- Pengecekan header
        # Cek apakah header di sheet sudah sesuai dengan standar terbaru
        headers_di_sheet = worksheet.row_values(1)
        if headers_di_sheet != NAMA_KOLOM_STANDAR:
            st.toast(f"Memperbarui header untuk worksheet '{nama_worksheet}'...")
            # Menyiapkan update batch
            cell_list = worksheet.range(1, 1, 1, len(NAMA_KOLOM_STANDAR))
            for i, header_val in enumerate(NAMA_KOLOM_STANDAR):
                cell_list[i].value = header_val
            # Update header dalam satu kali panggilan API
            worksheet.update_cells(cell_list)
            
        return worksheet
    except gspread.WorksheetNotFound:
        # Jika tidak ada, buat baru
        st.toast(f"Worksheet '{nama_worksheet}' tidak ditemukan. Membuat baru...")
        worksheet = spreadsheet.add_worksheet(title=nama_worksheet, rows=1, cols=len(NAMA_KOLOM_STANDAR))
        # Otomatis buat header di worksheet baru
        worksheet.append_row(NAMA_KOLOM_STANDAR)
        return worksheet
    except Exception as e:
        st.error(f"Gagal mendapatkan/membuat worksheet '{nama_worksheet}': {e}")
        return None

def upload_ke_dropbox(file_obj, nama_staf):
    """
    Upload file ke subfolder Dropbox berdasarkan nama_staf.
    Ini adalah "folder" di dalam Dropbox.
    """
    try:
        # Dapatkan bytes dari file
        file_data = file_obj.getvalue()
        
        # Buat nama file unik menggunakan timestamp untuk menghindari tumpang tindih
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Sanitasi nama file asli
        nama_file_asli = "".join([c for c in file_obj.name if c.isalnum() or c in ('.', '_', '-')])
        
        # --- IMPROVEMENT: Buat subfolder berdasarkan nama staf ---
        nama_folder_staf = "".join([c for c in nama_staf if c.isalnum() or c in (' ', '_', '-')]).replace(' ', '_')
        
        nama_file_unik = f"{timestamp}_{nama_file_asli}"
        
        # --- IMPROVEMENT: Path baru kini menyertakan subfolder nama_folder_staf ---
        path_dropbox = f"{FOLDER_DROPBOX}/{nama_folder_staf}/{nama_file_unik}"

        # 1. Upload file
        dbx.files_upload(file_data, path_dropbox, mode=dropbox.files.WriteMode.add)
        
        # 2. Buat shared link publik
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
        
        # 3. Dapatkan URL langsung
        return link.url.replace("?dl=0", "?raw=1")

    except Exception as e:
        st.error(f"Error tidak terduga saat upload ke Dropbox: {e}")
        return None

def simpan_ke_sheet(data_list, nama_staf):
    """Menyimpan satu baris data ke worksheet (tab) yang sesuai."""
    try:
        # Dapatkan "laci" (worksheet) yang benar berdasarkan nama
        worksheet = get_or_create_worksheet(nama_staf)
        if worksheet:
            worksheet.append_row(data_list)
            return True
        return False
    except Exception as e:
        st.error(f"Error menyimpan ke Sheet: {e}")
        return False

# Fungsi untuk memuat data dengan caching (meningkatkan performa)
@st.cache_data(ttl=60) # Cache data selama 60 detik
def load_data(daftar_staf):
    """
    Memuat data dari SEMUA worksheet staf dan menggabungkannya
    agar bisa ditampilkan di dasbor.
    """
    try:
        all_data = []
        for nama_staf in daftar_staf:
            # Dapatkan "laci" (worksheet) untuk staf ini
            worksheet = get_or_create_worksheet(nama_staf)
            if worksheet:
                # get_all_records() akan membaca header dan mengambil semua data
                data = worksheet.get_all_records()  
                if data:
                    all_data.extend(data) # Gabungkan data
        
        if not all_data:
            return pd.DataFrame(columns=NAMA_KOLOM_STANDAR) # Kembalikan DF kosong jika tidak ada data

        # --- PERUBAHAN ---
        # Membuat DataFrame. Jika ada sheet lama yg belum punya kolom 'Link Sosmed',
        # pandas otomatis mengisi dgn NaN (Not a Number), yg aman.
        return pd.DataFrame(all_data)
    
    except Exception as e:
        st.error(f"Gagal memuat data dari Google Sheet: {e}")
        # --- PERUBAHAN --- Pastikan DataFrame kosong punya semua kolom
        return pd.DataFrame(columns=NAMA_KOLOM_STANDAR)

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
            # --- PERUBAHAN --- Label diubah dan input kondisional ditambahkan
            nama = st.selectbox("Pilih Job Desc Anda", NAMA_STAF, key="nama")
            tanggal = st.date_input("Tanggal Kegiatan", value=date.today(), key="tanggal")
            
            link_sosmed = "" # Inisialisasi variabel
            if nama == "Social Media Specialist":
                link_sosmed = st.text_input(
                    "Link Sosmed", 
                    placeholder="Contoh: https://www.instagram.com/p/...", 
                    key="linksosmed"
                )
            # --- AKHIR PERUBAHAN ---
        
        with col2:
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
                
                link_foto = "-" # Default jika tidak ada foto
                # 1. Handle Upload Foto ke Dropbox (jika ada)
                if foto_bukti is not None:
                    # --- IMPROVEMENT: Kirim 'nama' ke fungsi upload ---
                    link_foto = upload_ke_dropbox(foto_bukti, nama)
                    if link_foto is None:
                        st.error("Gagal meng-upload foto ke Dropbox, laporan tidak disimpan.")
                        st.stop() 

                # Dapatkan timestamp saat ini untuk disimpan
                timestamp_sekarang = datetime.now().strftime('%d-%m-%Y %H:%M:%S')

                # --- PERUBAHAN --- Siapkan link sosmed (beri default "-" jika kosong)
                link_sosmed_final = link_sosmed if link_sosmed else "-"

                # 2. Siapkan data untuk Google Sheets
                # Urutan list INI harus sama persis dengan NAMA_KOLOM_STANDAR
                data_row = [
                    timestamp_sekarang,
                    nama,
                    tempat_dikunjungi,
                    deskripsi,
                    link_foto,
                    link_sosmed_final # --- PERUBAHAN --- Tambahkan data baru
                ]
                
                # 3. Simpan ke Google Sheets
                # --- IMPROVEMENT: Kirim 'nama' ke fungsi simpan ---
                if simpan_ke_sheet(data_row, nama):
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

    # --- IMPROVEMENT: Kirim 'NAMA_STAF' ke load_data ---
    # Fungsi ini akan otomatis memuat dan menggabungkan data dari semua worksheet
    df = load_data(NAMA_STAF)
        
    if df.empty:
        st.info("Belum ada data laporan yang masuk atau gagal memuat data.")
    else:      
        # Tampilkan filter
        st.subheader("Filter Data")
        col_filter1, col_filter2 = st.columns(2)
        
        # Pengecekan kolom (Logika ini tetap sama dan valid)
        if COL_NAMA not in df.columns or COL_TEMPAT not in df.columns:
            st.error(f"Struktur kolom di Google Sheet tidak sesuai. Pastikan ada kolom '{COL_NAMA}' dan '{COL_TEMPAT}'.")
            st.dataframe(df, use_container_width=True)
            st.stop()

        with col_filter1:
            # Filter Nama (Tetap berfungsi seperti biasa)
            nama_unik = df[COL_NAMA].unique()
            filter_nama = st.multiselect("Filter berdasarkan Nama", options=nama_unik, default=list(nama_unik))
        
        with col_filter2:
            # Filter berdasarkan 'Tempat Dikunjungi' (Tetap berfungsi seperti biasa)
            # --- PERUBAHAN --- Mengisi NaN (jika ada data lama) dengan string kosong agar filter tetap jalan
            tempat_unik = df[COL_TEMPAT].fillna("").unique()
            filter_tempat = st.multiselect("Filter berdasarkan Tempat", options=tempat_unik, default=list(tempat_unik))
        
        # Terapkan filter secara dinamis
        df_filtered = df.copy() # Mulai dengan semua data

        if filter_nama:
            # Terapkan filter nama HANYA JIKA ada yang dipilih
            df_filtered = df_filtered[df_filtered[COL_NAMA].isin(filter_nama)]

        if filter_tempat:
            # Terapkan filter tempat HANYA JIKA ada yang dipilih
            # --- PERUBAHAN --- Mengisi NaN (jika ada data lama) dengan string kosong agar filter tetap jalan
            df_filtered = df_filtered[df_filtered[COL_TEMPAT].fillna("").isin(filter_tempat)]

        # Urutkan data dari yang terbaru
        if not df_filtered.empty:
            try:
                df_filtered['sort_dt'] = pd.to_datetime(df_filtered[COL_TIMESTAMP], format='%d-%m-%Y %H:%M:%S', errors='coerce')
                df_filtered = df_filtered.sort_values(by='sort_dt', ascending=False).drop(columns=['sort_dt'])
            except Exception as e:
                st.warning(f"Gagal mengurutkan data berdasarkan tanggal. Pastikan format tanggal benar. Error: {e}")

        # --- IMPROVEMENT: Tampilkan data dalam "folder" (expander) per nama ---
        st.subheader("Hasil Laporan Terfilter")

        # Dapatkan nama unik dari data yang SUDAH difilter
        nama_unik_terfilter = df_filtered[COL_NAMA].unique()

        if not nama_unik_terfilter.any():
            st.info("Tidak ada data yang sesuai dengan filter Anda.")
        else:
            # Buat expander untuk setiap nama
            for nama_staf in nama_unik_terfilter:
                
                # Ambil data HANYA untuk staf ini dari dataframe yang sudah difilter
                data_staf = df_filtered[df_filtered[COL_NAMA] == nama_staf]
                
                # Hitung jumlah laporan untuk staf ini
                jumlah_laporan = len(data_staf)
                
                # Tampilkan expander (seperti "folder")
                # 'expanded=True' berarti "folder" ini akan langsung terbuka
                with st.expander(f"📁 {nama_staf}    ({jumlah_laporan} Laporan)", expanded=True):
                    
                    # --- PERUBAHAN ---
                    # Tampilkan tabel data di dalam expander
                    # Membuat column_config dinamis untuk link
                    
                    column_config = {}
                    
                    if COL_LINK_FOTO in data_staf.columns:
                        column_config[COL_LINK_FOTO] = st.column_config.LinkColumn(
                            COL_LINK_FOTO, display_text="Buka Foto"
                        )
                    
                    if COL_LINK_SOSMED in data_staf.columns:
                        column_config[COL_LINK_SOSMED] = st.column_config.LinkColumn(
                            COL_LINK_SOSMED, display_text="Buka Link"
                        )

                    st.dataframe(
                        data_staf, 
                        use_container_width=True, 
                        column_config=column_config
                    )
                    # --- AKHIR PERUBAHAN ---


# Tampilkan pesan jika koneksi gagal
elif not KONEKSI_GSHEET_BERHASIL:
    st.warning("Aplikasi tidak dapat berjalan karena koneksi Google Sheets gagal.")
elif not KONEKSI_DROPBOX_BERHASIL:
    st.warning("Aplikasi tidak dapat menerima upload foto karena koneksi Dropbox gagal.")
