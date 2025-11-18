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
COL_LINK_SOSMED = "Link Sosmed" # Kolom baru ditambahkan

# Daftar standar untuk pengecekan header
NAMA_KOLOM_STANDAR = [
    COL_TIMESTAMP, 
    COL_NAMA, 
    COL_TEMPAT, 
    COL_DESKRIPSI, 
    COL_LINK_FOTO, 
    COL_LINK_SOSMED # Kolom baru ditambahkan
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
    Fungsi ini "BERSIH" dari panggilan UI Streamlit agar aman di-cache.
    """
    try:
        # Coba dapatkan worksheet (tab)
        worksheet = spreadsheet.worksheet(nama_worksheet)
        
        # Pengecekan header
        # Cek apakah header di sheet sudah sesuai dengan standar terbaru
        headers_di_sheet = worksheet.row_values(1)
        if headers_di_sheet != NAMA_KOLOM_STANDAR:
            # --- PERUBAHAN CACHE ---
            # st.toast(...)  <-- DIHAPUS (Tidak boleh ada UI di fungsi cache)
            
            # Menyiapkan update batch
            cell_list = worksheet.range(1, 1, 1, len(NAMA_KOLOM_STANDAR))
            for i, header_val in enumerate(NAMA_KOLOM_STANDAR):
                cell_list[i].value = header_val
            # Update header dalam satu kali panggilan API
            worksheet.update_cells(cell_list)
        
        # Atur Format Text Wrapping
        worksheet.format("C:D", {
            "wrapStrategy": "WRAP",
            "verticalAlignment": "TOP"
        })

        # Atur Format untuk sisanya (Hanya Top Align)
        # Kolom A, B, dan E, F (sesuai NAMA_KOLOM_STANDAR Anda)
        worksheet.format("A:B", {"verticalAlignment": "TOP"})
        worksheet.format("E:F", {"verticalAlignment": "TOP"})
            
        return worksheet
    
    except gspread.WorksheetNotFound:
        # Jika tidak ada, buat baru
        # --- PERUBAHAN CACHE ---
        # st.toast(...)  <-- DIHAPUS (Tidak boleh ada UI di fungsi cache)
        worksheet = spreadsheet.add_worksheet(title=nama_worksheet, rows=1, cols=len(NAMA_KOLOM_STANDAR))
        # Otomatis buat header di worksheet baru
        worksheet.append_row(NAMA_KOLOM_STANDAR)
        
        # Atur Format Text Wrapping (untuk sheet BARU)
        worksheet.format("C:D", {
            "wrapStrategy": "WRAP",
            "verticalAlignment": "TOP"
        })

        # Atur Format untuk sisanya (Hanya Top Align)
        worksheet.format("A:B", {"verticalAlignment": "TOP"})
        worksheet.format("E:F", {"verticalAlignment": "TOP"})
        
        return worksheet
    
    except Exception as e:
        # --- PERUBAHAN CACHE ---
        # st.error(...)  <-- DIHAPUS (Tidak boleh ada UI di fungsi cache)
        # Sebaliknya, lempar error ini agar fungsi pemanggil bisa menanganinya
        print(f"Error di get_or_create_worksheet: {e}") # Log ke konsol
        raise e # Lempar lagi error-nya

def upload_ke_dropbox(file_obj, nama_staf):
    """
    Upload file ke subfolder Dropbox berdasarkan nama_staf.
    (Fungsi ini aman, tidak di-cache, st.error boleh ada di sini)
    """
    try:
        # Dapatkan bytes dari file
        file_data = file_obj.getvalue()
        
        # Buat nama file unik menggunakan timestamp untuk menghindari tumpang tindih
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Sanitasi nama file asli
        nama_file_asli = "".join([c for c in file_obj.name if c.isalnum() or c in ('.', '_', '-')])
        
        # Buat subfolder berdasarkan nama staf
        nama_folder_staf = "".join([c for c in nama_staf if c.isalnum() or c in (' ', '_', '-')]).replace(' ', '_')
        
        nama_file_unik = f"{timestamp}_{nama_file_asli}"
        
        # Path baru kini menyertakan subfolder nama_folder_staf
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
    """
    Menyimpan satu baris data ke worksheet (tab) yang sesuai.
    (Fungsi ini tidak di-cache, jadi aman untuk menampilkan st.error)
    """
    try:
        # Dapatkan "laci" (worksheet) yang benar berdasarkan nama
        # --- PERUBAHAN CACHE ---
        # Ini sekarang bisa melempar error dari get_or_create_worksheet
        worksheet = get_or_create_worksheet(nama_staf) 
        if worksheet:
            worksheet.append_row(data_list)
            return True
        return False
    except Exception as e:
        # TANGKAP errornya DI SINI dan tampilkan ke UI
        st.error(f"Error saat mencoba mengakses Sheet '{nama_staf}': {e}")
        return False

# Fungsi untuk memuat data dengan caching (meningkatkan performa)
@st.cache_data(ttl=60) # Cache data selama 60 detik
def load_data(daftar_staf):
    """
    Memuat data dari SEMUA worksheet staf dan menggabungkannya
    agar bisa ditampilkan di dasbor. "BERSIH" dari panggilan UI.
    """
    try:
        all_data = []
        for nama_staf in daftar_staf:
            # --- PERUBAHAN CACHE: Tambahkan try..except di dalam loop ---
            # Agar jika satu sheet gagal, aplikasi tidak crash total
            try:
                # Dapatkan "laci" (worksheet) untuk staf ini
                worksheet = get_or_create_worksheet(nama_staf)
                if worksheet:
                    # get_all_records() akan membaca header dan mengambil semua data
                    data = worksheet.get_all_records()  
                    if data:
                        all_data.extend(data) # Gabungkan data
            except Exception as e:
                # Jika 1 sheet gagal, jangan hentikan semua.
                # Cukup log ke konsol (BUKAN UI st.error)
                print(f"PERINGATAN: Gagal memuat data untuk '{nama_staf}'. Error: {e}")
                pass # Lanjut ke staf berikutnya
        
        if not all_data:
            return pd.DataFrame(columns=NAMA_KOLOM_STANDAR) # Kembalikan DF kosong jika tidak ada data

        # Membuat DataFrame.
        return pd.DataFrame(all_data)
    
    except Exception as e:
        # --- PERUBAHAN CACHE ---
        # st.error(...)  <-- DIHAPUS (Tidak boleh ada UI di fungsi cache)
        print(f"Error fatal saat load_data: {e}") # Log ke konsol
        # Lempar error agar bisa ditangani DI LUAR fungsi cache
        raise e 

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

    # 'nama' dipindahkan ke LUAR form.
    nama = st.selectbox(
        "Pilih Job Desc Anda", 
        NAMA_STAF, 
        key="nama_job_desc_selector" 
    )

    with st.form(key="form_kegiatan", clear_on_submit=True):
        
        col1, col2 = st.columns(2)
        
        with col1:
            tanggal = st.date_input("Tanggal Kegiatan", value=date.today(), key="tanggal")
            
            # Inisialisasi variabel input
            link_sosmed_input = "" 
            
            # Input kondisional
            if nama == "Social Media Specialist":
                link_sosmed_input = st.text_input(
                    "Link Sosmed", 
                    placeholder="Contoh: https://www.instagram.com/p/...", 
                    key="linksosmed"
                )
        
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
                if foto_bukti is not None:
                    link_foto = upload_ke_dropbox(foto_bukti, nama)
                    if link_foto is None:
                        st.error("Gagal meng-upload foto ke Dropbox, laporan tidak disimpan.")
                        st.stop() 

                timestamp_sekarang = datetime.now().strftime('%d-%m-%Y %H:%M:%S')

                # Ambil nilai link_sosmed secara eksplisit
                link_sosmed_final = link_sosmed_input if link_sosmed_input else "-"

                # Siapkan data untuk Google Sheets
                data_row = [
                    timestamp_sekarang,
                    nama,
                    tempat_dikunjungi,
                    deskripsi,
                    link_foto,
                    link_sosmed_final
                ]
                
                # --- PERUBAHAN CACHE ---
                # Fungsi ini sekarang akan menampilkan st.error jika gagal
                if simpan_ke_sheet(data_row, nama): 
                    st.success(f"Laporan untuk {nama} berhasil disimpan!")
                    st.cache_data.clear() # Hapus cache data agar dasbor update
                # else: Pesan error sudah ditangani DI DALAM fungsi simpan_ke_sheet()


    # --- 3. DASBOR (TABEL LAPORAN) ---
    st.header("📊 Dasbor Laporan Kegiatan")
    
    # Tombol refresh manual
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.cache_resource.clear() # Hapus juga cache resource
        st.rerun()

    # --- PERUBAHAN CACHE: Bungkus pemanggilan load_data dengan try...except ---
    try:
        # Kirim 'NAMA_STAF' ke load_data
        df = load_data(NAMA_STAF)
    
    except Exception as e:
        # Tampilkan error ke UI DI SINI (di luar fungsi cache)
        st.error(f"Gagal memuat data dari Google Sheet. Error: {e}")
        # Buat DataFrame kosong agar sisa skrip tidak crash
        df = pd.DataFrame(columns=NAMA_KOLOM_STANDAR)
        
        
    if df.empty:
        st.info("Belum ada data laporan yang masuk atau gagal memuat data.")
    else:    
        # Tampilkan filter
        st.subheader("Filter Data")
        col_filter1, col_filter2 = st.columns(2)
        
        if COL_NAMA not in df.columns or COL_TEMPAT not in df.columns:
            st.error(f"Struktur kolom di Google Sheet tidak sesuai. Pastikan ada kolom '{COL_NAMA}' dan '{COL_TEMPAT}'.")
            st.dataframe(df, use_container_width=True)
            st.stop()

        with col_filter1:
            # Filter Nama
            nama_unik = df[COL_NAMA].unique()
            filter_nama = st.multiselect("Filter berdasarkan Nama", options=nama_unik, default=list(nama_unik))
        
        with col_filter2:
            # Filter berdasarkan 'Tempat Dikunjungi'
            tempat_unik = df[COL_TEMPAT].fillna("").unique()
            filter_tempat = st.multiselect("Filter berdasarkan Tempat", options=tempat_unik, default=list(tempat_unik))
        
        # Terapkan filter secara dinamis
        df_filtered = df.copy() # Mulai dengan semua data

        if filter_nama:
            df_filtered = df_filtered[df_filtered[COL_NAMA].isin(filter_nama)]

        if filter_tempat:
            df_filtered = df_filtered[df_filtered[COL_TEMPAT].fillna("").isin(filter_tempat)]

        # Urutkan data dari yang terbaru
        if not df_filtered.empty:
            try:
                df_filtered['sort_dt'] = pd.to_datetime(df_filtered[COL_TIMESTAMP], format='%d-%m-%Y %H:%M:%S', errors='coerce')
                df_filtered = df_filtered.sort_values(by='sort_dt', ascending=False).drop(columns=['sort_dt'])
            except Exception as e:
                st.warning(f"Gagal mengurutkan data berdasarkan tanggal. Pastikan format tanggal benar. Error: {e}")

        # Tampilkan data dalam "folder" (expander) per nama
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
                with st.expander(f"📁 {nama_staf}     ({jumlah_laporan} Laporan)", expanded=True):
                    
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

                    # --- PERUBAHAN: Ganti st.dataframe + styler dengan st.data_editor ---
                    
                    # st.data_editor secara otomatis akan wrap teks di kolom Deskripsi
                    # dan akan mematuhi column_config untuk Link Foto/Sosmed.
                    
                    st.data_editor(
                        data_staf,
                        use_container_width=True,
                        column_config=column_config,
                        disabled=True, # <-- PENTING: Membuat tabel jadi read-only
                        key=f"editor_{nama_staf}" # <-- PENTING: Key unik di dalam loop
                    )
                    # --- PERUBAHAN SELESAI ---


# Tampilkan pesan jika koneksi gagal
elif not KONEKSI_GSHEET_BERHASIL:
    st.warning("Aplikasi tidak dapat berjalan karena koneksi Google Sheets gagal.")
elif not KONEKSI_DROPBOX_BERHASIL:
    st.warning("Aplikasi tidak dapat menerima upload foto karena koneksi Dropbox gagal.")
