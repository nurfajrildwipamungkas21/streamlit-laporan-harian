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

# --- FUNGSI HELPER CORE & FORMATTING (DIPERBAIKI) ---

def auto_format_sheet(worksheet):
    """
    Fungsi Ajaib Revisi: Memaksa WRAP TEXT agar tidak terpotong.
    """
    try:
        # 1. Format Header (Tebal & Tengah)
        worksheet.format("A1:Z1", {
            "textFormat": {"bold": True},
            "horizontalAlignment": "CENTER",
            "verticalAlignment": "MIDDLE"
        })
        
        # 2. Format Body (WRAP TEXT adalah kuncinya!)
        # wrapStrategy: WRAP -> Teks turun ke bawah jika panjang
        # verticalAlignment: TOP -> Agar rapi di atas
        worksheet.format("A2:Z1000", {
            "wrapStrategy": "WRAP", 
            "verticalAlignment": "TOP"
        })

        # 3. Resize Lebar Kolom
        # Kita resize kolom A-F (index 0-5) agar pas
        worksheet.columns_auto_resize(0, 6)
        
    except Exception as e:
        print(f"Format Error: {e}")

@st.cache_resource(ttl=60)
def get_or_create_worksheet(nama_worksheet):
    try:
        return spreadsheet.worksheet(nama_worksheet)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=nama_worksheet, rows=1, cols=len(NAMA_KOLOM_STANDAR))
        ws.append_row(NAMA_KOLOM_STANDAR)
        auto_format_sheet(ws) # Format saat buat baru
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
        auto_format_sheet(ws) 
        return True, "Berhasil tambah tim!"
    except Exception as e: return False, str(e)

# --- FUNGSI UPLOAD ---

def upload_ke_dropbox(file_obj, nama_staf, kategori="Umum"):
    try:
        file_data = file_obj.getvalue()
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        clean_filename = "".join([c for c in file_obj.name if c.isalnum() or c in ('.','_')])
        clean_user_folder = "".join([c for c in nama_staf if c.isalnum() or c in (' ','_')]).replace(' ','_')
        clean_kategori = "".join([c for c in kategori if c.isalnum() or c in (' ','_')]).replace(' ','_')
        
        path = f"{FOLDER_DROPBOX}/{clean_user_folder}/{clean_kategori}/{ts}_{clean_filename}"
        
        dbx.files_upload(file_data, path, mode=dropbox.files.WriteMode.add)
        settings = SharedLinkSettings(requested_visibility=RequestedVisibility.public)
        try: link = dbx.sharing_create_shared_link_with_settings(path, settings=settings)
        except ApiError as e: 
            if e.error.is_shared_link_already_exists():
                link = dbx.sharing_list_shared_links(path, direct_only=True).links[0]
            else: return "-"
        return link.url.replace("?dl=0", "?raw=1")
    except Exception as e: 
        print(f"Upload Error: {e}")
        return "-"

# --- FUNGSI CHECKLIST ---

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
        auto_format_sheet(ws) # FORMAT ULANG SETELAH SAVE
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
        auto_format_sheet(ws) # FORMAT ULANG SETELAH NAMBAH
        return True
    except: return False

def update_evidence_row(sheet_name, target_name, note, file_obj, user_folder_name, kategori_folder):
    try:
        ws = spreadsheet.worksheet(sheet_name)
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        
        col_target_key = "Misi" if sheet_name == SHEET_TARGET_TEAM else "Target"
        matches = df.index[df[col_target_key] == target_name].tolist()
        
        if not matches:
            return False, f"Target '{target_name}' tidak ditemukan di database."
            
        row_idx_pandas = matches[0] 
        row_idx_gsheet = row_idx_pandas + 2 
        
        link_bukti = ""
        if file_obj:
            link_bukti = upload_ke_dropbox(file_obj, user_folder_name, kategori=kategori_folder)
        
        catatan_lama = df.at[row_idx_pandas, "Bukti/Catatan"]
        catatan_lama = str(catatan_lama) if catatan_lama else "-"
        if catatan_lama == "-": catatan_lama = ""
        
        update_text = ""
        if note: update_text += f"{note}. "
        if link_bukti and link_bukti != "-": update_text += f"[FOTO: {link_bukti}]"
        
        final_note = f"{catatan_lama} | {update_text}" if catatan_lama else update_text
        if not final_note: final_note = "-"
        
        headers = df.columns.tolist()
        try:
            col_idx_gsheet = headers.index("Bukti/Catatan") + 1
        except ValueError:
            return False, "Kolom Bukti/Catatan hilang."

        ws.update_cell(row_idx_gsheet, col_idx_gsheet, final_note)
        auto_format_sheet(ws) # FORMAT ULANG SETELAH UPDATE BUKTI
        
        return True, "Berhasil update bukti!"
        
    except Exception as e:
        return False, f"Error: {e}"

def simpan_laporan_harian(data_list, nama_staf):
    try:
        ws = get_or_create_worksheet(nama_staf)
        ws.append_row(data_list)
        auto_format_sheet(ws) # FORMAT ULANG LAPORAN HARIAN
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

    # --- 1. MONITORING TARGET ---
    st.subheader("📊 Monitoring & Checklist Target")
    
    col_dash_1, col_dash_2 = st.columns(2)
    
    cols_team = ["Misi", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan"]
    df_team = load_checklist(SHEET_TARGET_TEAM, cols_team)
    
    cols_indiv = ["Nama", "Target", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan"]
    df_indiv_all = load_checklist(SHEET_TARGET_INDIVIDU, cols_indiv)

    # --- PANEL TEAM (KIRI) ---
    with col_dash_1:
        st.markdown("#### 🏆 Target Team")
        
        if not df_team.empty:
            total_team = len(df_team)
            done_team = len(df_team[df_team['Status'] == True])
            prog_team = done_team / total_team if total_team > 0 else 0
            
            st.progress(prog_team, text=f"Pencapaian Team: {int(prog_team*100)}%")
            
            edited_team = st.data_editor(
                df_team,
                column_config={
                    "Status": st.column_config.CheckboxColumn("Done?", width="small"),
                    "Misi": st.column_config.TextColumn(disabled=True),
                    "Bukti/Catatan": st.column_config.TextColumn("Bukti/Note (Edit Disini)", width="medium")
                },
                column_order=["Status", "Misi", "Bukti/Catatan"],
                hide_index=True,
                key="editor_dash_team",
                use_container_width=True
            )
            
            if st.button("💾 Update Progress Team", use_container_width=True):
                save_checklist(SHEET_TARGET_TEAM, edited_team)
                st.toast("Progress Team Berhasil Disimpan!", icon="✅")
                st.cache_data.clear()
                st.rerun()
                
            with st.expander("📂 Upload Bukti Foto (Team)"):
                list_misi_team = df_team["Misi"].tolist()
                pilih_misi = st.selectbox("Pilih Misi Team:", list_misi_team)
                note_misi = st.text_input("Catatan Tambahan")
                file_misi = st.file_uploader("Upload Foto Bukti Team", key="up_team")
                
                if st.button("Kirim Bukti Team"):
                    with st.spinner("Mengupload..."):
                        pelapor_team = get_daftar_staf_terbaru()[0] 
                        sukses, msg = update_evidence_row(
                            SHEET_TARGET_TEAM, 
                            pilih_misi, 
                            note_misi, 
                            file_misi, 
                            user_folder_name=pelapor_team, 
                            kategori_folder="Target_Team"
                        )
                        if sukses:
                            st.success("Bukti Team Terupload!")
                            st.cache_data.clear()
                            st.rerun()
                        else: st.error(msg)
        else:
            st.info("Belum ada target team.")

    # --- PANEL INDIVIDU (KANAN) ---
    with col_dash_2:
        st.markdown("#### ⚡ Target Individu")
        
        list_staff_filter = get_daftar_staf_terbaru()
        filter_nama = st.selectbox("Lihat Progress Siapa?", list_staff_filter, index=0)
        
        if not df_indiv_all.empty:
            df_indiv_user = df_indiv_all[df_indiv_all["Nama"] == filter_nama]
            
            if not df_indiv_user.empty:
                total_indiv = len(df_indiv_user)
                done_indiv = len(df_indiv_user[df_indiv_user['Status'] == True])
                prog_indiv = done_indiv / total_indiv if total_indiv > 0 else 0
                
                st.progress(prog_indiv, text=f"Progress {filter_nama}: {int(prog_indiv*100)}%")
                
                edited_indiv = st.data_editor(
                    df_indiv_user,
                    column_config={
                        "Status": st.column_config.CheckboxColumn("Done?", width="small"),
                        "Target": st.column_config.TextColumn(disabled=True),
                        "Bukti/Catatan": st.column_config.TextColumn("Bukti/Note (Edit Disini)", width="medium")
                    },
                    column_order=["Status", "Target", "Bukti/Catatan"],
                    hide_index=True,
                    key=f"editor_dash_indiv_{filter_nama}",
                    use_container_width=True
                )
                
                if st.button(f"💾 Update Progress {filter_nama}", use_container_width=True):
                    df_indiv_all.update(edited_indiv)
                    save_checklist(SHEET_TARGET_INDIVIDU, df_indiv_all)
                    st.toast(f"Progress {filter_nama} Disimpan!", icon="✅")
                    st.cache_data.clear()
                    st.rerun()

                with st.expander(f"📂 Upload Bukti Foto ({filter_nama})"):
                    list_target_user = df_indiv_user["Target"].tolist()
                    pilih_target = st.selectbox("Pilih Target Mingguan:", list_target_user)
                    note_target = st.text_input("Catatan Tambahan", key="note_indiv")
                    file_target = st.file_uploader("Upload Foto Bukti Pribadi", key="up_indiv")
                    
                    if st.button("Kirim Bukti Pribadi"):
                         with st.spinner("Mengupload..."):
                            sukses, msg = update_evidence_row(
                                SHEET_TARGET_INDIVIDU, 
                                pilih_target, 
                                note_target, 
                                file_target, 
                                user_folder_name=filter_nama,
                                kategori_folder="Target_Individu"
                            )
                            if sukses:
                                st.success("Bukti Pribadi Terupload!")
                                st.cache_data.clear()
                                st.rerun()
                            else: st.error(msg)
            else:
                st.info(f"{filter_nama} belum memiliki target aktif.")
        else:
            st.info("Belum ada data target individu.")

    # --- 2. INPUT HARIAN ---
    st.divider()
    with st.container(border=True):
        st.subheader("📝 Laporan Harian (Task List)")
        
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
                        link_foto = "-"
                        if fotos:
                            links = []
                            for f in fotos:
                                url = upload_ke_dropbox(f, nama_pelapor, kategori="Laporan_Harian")
                                links.append(url)
                            link_foto = "\n".join(links)
                        
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
