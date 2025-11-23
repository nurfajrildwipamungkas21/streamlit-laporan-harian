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

# --- HYBRID LIBRARY IMPORT (FALLBACK MECHANISM) ---
# Mencoba import AgGrid untuk tampilan tabel advanced.
# Jika tidak ada, variable HAS_AGGRID menjadi False dan aplikasi memakai tabel standar.
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
    HAS_AGGRID = True
except ImportError:
    HAS_AGGRID = False

# --- KONFIGURASI HALAMAN ---
st.set_page_config(
    page_title="Sales & Marketing Action Center",
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
    if "gcp_service_account" in st.secrets:
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scopes)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open(NAMA_GOOGLE_SHEET)
        KONEKSI_GSHEET_BERHASIL = True
    else:
        st.error("GSheet Error: Kredensial (gcp_service_account) tidak ditemukan di secrets.")
except Exception as e:
    st.error(f"GSheet Error: {e}")

# 2. Connect Dropbox
try:
    if "dropbox" in st.secrets and "access_token" in st.secrets["dropbox"]:
        dbx = dropbox.Dropbox(st.secrets["dropbox"]["access_token"])
        dbx.users_get_current_account()
        KONEKSI_DROPBOX_BERHASIL = True
    else:
        pass
except AuthError:
    st.error("Dropbox Error: Token Autentikasi tidak valid.")
except Exception as e:
    st.error(f"Dropbox Error: {e}")


# --- FUNGSI HELPER CORE & SMART FORMATTING (ROBUST API) ---

def auto_format_sheet(worksheet):
    """
    Fungsi Formatting Robust (Full API Batch Update)
    """
    try:
        sheet_id = worksheet.id
        all_values = worksheet.get_all_values()
        if not all_values: return 

        headers = all_values[0]
        data_row_count = len(all_values)
        formatting_row_count = worksheet.row_count
        if data_row_count > formatting_row_count: formatting_row_count = data_row_count

        requests = []
        default_body_format = {"verticalAlignment": "TOP", "wrapStrategy": "CLIP"}

        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": formatting_row_count},
                "cell": {"userEnteredFormat": default_body_format},
                "fields": "userEnteredFormat(verticalAlignment,wrapStrategy)"
            }
        })

        for i, col_name in enumerate(headers):
            col_index = i
            cell_format_override = {}
            width = 100

            if col_name in ["Misi", "Target", "Deskripsi", "Bukti/Catatan", "Link Foto", "Link Sosmed"]:
                width = 350
                cell_format_override["wrapStrategy"] = "WRAP"
            elif col_name in ["Tgl_Mulai", "Tgl_Selesai", "Timestamp"]:
                width = 150 if col_name == "Timestamp" else 120
                cell_format_override["horizontalAlignment"] = "CENTER"
            elif col_name in ["Status", "Done?"]:
                width = 60
                cell_format_override["horizontalAlignment"] = "CENTER"
            elif col_name == "Nama":
                width = 150
                cell_format_override["wrapStrategy"] = "WRAP"

            requests.append({
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": col_index, "endIndex": col_index + 1},
                    "properties": {"pixelSize": width},
                    "fields": "pixelSize"
                }
            })

            if cell_format_override:
                requests.append({
                    "repeatCell": {
                        "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": formatting_row_count, "startColumnIndex": col_index, "endColumnIndex": col_index + 1},
                        "cell": {"userEnteredFormat": cell_format_override},
                        "fields": f"userEnteredFormat({','.join(cell_format_override.keys())})"
                    }
                })

        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {
                    "textFormat": {"bold": True},
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE",
                    "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9},
                    "wrapStrategy": "WRAP"
                }},
                "fields": "userEnteredFormat(textFormat,horizontalAlignment,verticalAlignment,backgroundColor,wrapStrategy)"
            }
        })
        
        requests.append({
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount"
            }
        })

        if data_row_count > 1: 
            requests.append({
                "autoResizeDimensions": {
                    "dimensions": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 1, "endIndex": data_row_count}
                }
            })

        if requests:
            body = {"requests": requests}
            worksheet.spreadsheet.batch_update(body)

    except Exception as e:
        print(f"Robust Format Error pada sheet '{worksheet.title}': {e}")


@st.cache_resource(ttl=60)
def get_or_create_worksheet(nama_worksheet):
    try:
        return spreadsheet.worksheet(nama_worksheet)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=nama_worksheet, rows=100, cols=len(NAMA_KOLOM_STANDAR))
        ws.append_row(NAMA_KOLOM_STANDAR, value_input_option='USER_ENTERED')
        auto_format_sheet(ws)
        return ws
    except Exception as e:
        print(f"Error get_or_create_worksheet: {e}")
        return None

@st.cache_data(ttl=60)
def get_daftar_staf_terbaru():
    default_staf = ["Saya"]
    if not KONEKSI_GSHEET_BERHASIL: return default_staf
    try:
        try: ws = spreadsheet.worksheet(SHEET_CONFIG_NAMA)
        except:
            ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_NAMA, rows=100, cols=1)
            ws.append_row(["Daftar Nama Staf"], value_input_option='USER_ENTERED')
            ws.append_row(["Saya"], value_input_option='USER_ENTERED')
            auto_format_sheet(ws)
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
        ws.append_row([nama_baru], value_input_option='USER_ENTERED')
        auto_format_sheet(ws)
        return True, "Berhasil tambah tim!"
    except Exception as e: return False, str(e)

# --- FUNGSI UPLOAD ---

def upload_ke_dropbox(file_obj, nama_staf, kategori="Umum"):
    if not KONEKSI_DROPBOX_BERHASIL or dbx is None:
        return "Koneksi Dropbox Error"
        
    try:
        file_data = file_obj.getvalue()
        ts = datetime.now(tz=ZoneInfo("Asia/Jakarta")).strftime("%Y%m%d_%H%M%S")
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
            ws.append_row(columns, value_input_option='USER_ENTERED')
            auto_format_sheet(ws)
            return pd.DataFrame(columns=columns)
        
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        
        # Pembersihan Data Awal untuk mencegah error serialisasi
        df.fillna("", inplace=True)
        
        for col in columns:
            if col not in df.columns:
                df[col] = False if col == "Status" else ""

        col_status = "Status"
        if col_status in df.columns:
             df[col_status] = df[col_status].apply(lambda x: True if str(x).upper() == "TRUE" else False)
        return df
    except Exception as e:
        print(f"Error loading checklist {sheet_name}: {e}")
        return pd.DataFrame(columns=columns)

def save_checklist(sheet_name, df):
    try:
        ws = spreadsheet.worksheet(sheet_name)
        ws.clear()

        rows_needed = len(df) + 1
        if ws.row_count < rows_needed:
             ws.resize(rows=rows_needed)

        df_save = df.copy()
        col_status = "Status"
        if col_status in df_save.columns:
            df_save[col_status] = df_save[col_status].apply(lambda x: "TRUE" if x else "FALSE")
            
        df_save = df_save.astype(str)
            
        ws.update([df_save.columns.values.tolist()] + df_save.values.tolist(), value_input_option='USER_ENTERED')
        auto_format_sheet(ws) 
        return True
    except Exception as e:
        print(f"Error saving checklist {sheet_name}: {e}")
        return False

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
            
        ws.append_rows(rows_to_add, value_input_option='USER_ENTERED')
        auto_format_sheet(ws)
        return True
    except Exception as e:
        print(f"Error adding bulk targets {sheet_name}: {e}")
        return False

def update_evidence_row(sheet_name, target_name, note, file_obj, user_folder_name, kategori_folder):
    try:
        ws = spreadsheet.worksheet(sheet_name)
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        
        col_target_key = "Misi" if sheet_name == SHEET_TARGET_TEAM else "Target"
        
        if col_target_key not in df.columns:
             return False, f"Kolom kunci '{col_target_key}' tidak ditemukan."
             
        matches = df.index[df[col_target_key] == target_name].tolist()
        
        if not matches:
            return False, f"Target '{target_name}' tidak ditemukan di database."
            
        row_idx_pandas = matches[0]
        row_idx_gsheet = row_idx_pandas + 2
        
        link_bukti = ""
        if file_obj:
            link_bukti = upload_ke_dropbox(file_obj, user_folder_name, kategori=kategori_folder)
        
        catatan_lama = df.at[row_idx_pandas, "Bukti/Catatan"]
        catatan_lama = str(catatan_lama) if catatan_lama else ""
        if catatan_lama == "-": catatan_lama = ""
        
        update_text = ""
        ts_update = datetime.now(tz=ZoneInfo("Asia/Jakarta")).strftime('%d-%m %H:%M')
        update_text += f"[{ts_update}] "

        if note: update_text += f"{note}. "
        
        if link_bukti and link_bukti != "-":
            if link_bukti == "Koneksi Dropbox Error":
                 update_text += "[FOTO: Gagal Upload (Error Dropbox)]"
            else:
                update_text += f"[FOTO: {link_bukti}]"
        
        final_note = f"{catatan_lama}\n{update_text}" if catatan_lama.strip() else update_text
        if not final_note.strip(): final_note = "-"
        
        headers = df.columns.tolist()
        try:
            col_idx_gsheet = headers.index("Bukti/Catatan") + 1
        except ValueError:
            return False, "Kolom Bukti/Catatan hilang."

        cell_address = gspread.utils.rowcol_to_a1(row_idx_gsheet, col_idx_gsheet)
        ws.update(cell_address, final_note, value_input_option='USER_ENTERED')
        auto_format_sheet(ws)
        return True, "Berhasil update bukti!"
        
    except Exception as e:
        return False, f"Error: {e}"

def simpan_laporan_harian(data_list, nama_staf):
    try:
        ws = get_or_create_worksheet(nama_staf)
        ws.append_row(data_list, value_input_option='USER_ENTERED')
        auto_format_sheet(ws)
        return True
    except Exception as e:
        print(f"Error saving daily report for {nama_staf}: {e}")
        return False

@st.cache_data(ttl=60)
def load_all_reports(daftar_staf):
    all_data = []
    for nama in daftar_staf:
        try:
            ws = get_or_create_worksheet(nama)
            if ws:
                d = ws.get_all_records()
                if d: all_data.extend(d)
        except: pass
    return pd.DataFrame(all_data) if all_data else pd.DataFrame(columns=NAMA_KOLOM_STANDAR)

# --- FUNGSI HYBRID TABLE RENDERER (CRASH-PROOF VERSION) ---
# Fungsi ini dimodifikasi untuk menangani MarshallComponentException

def render_hybrid_table(df_data, unique_key, main_text_col):
    """
    Me-render tabel dengan mode hybrid.
    Mencoba menggunakan AgGrid, jika error (MarshallComponentException/Lainnya),
    otomatis fallback ke st.data_editor biasa.
    """
    
    use_aggrid_attempt = HAS_AGGRID

    # -- MODE 1: ATTEMPT AG-GRID (PRO) --
    if use_aggrid_attempt:
        try:
            # COPY dataframe untuk memastikan data bersih dan index tereset
            # Ini seringkali memperbaiki masalah serialisasi
            df_grid = df_data.copy()
            df_grid.reset_index(drop=True, inplace=True)

            # Konfigurasi AgGrid
            gb = GridOptionsBuilder.from_dataframe(df_grid)
            
            # Config Kolom Status
            gb.configure_column("Status", editable=True, width=90)
            
            # Config Kolom Utama (Misi/Target) - Read Only tapi Wrap Text
            gb.configure_column(main_text_col, wrapText=True, autoHeight=True, width=400, editable=False)
            
            # Config Kolom Bukti - Editable & Wrap Text
            gb.configure_column("Bukti/Catatan", wrapText=True, autoHeight=True, editable=True, cellEditor="agLargeTextCellEditor", width=300)
            
            # Config Kolom Lain (Hidden/Readonly sesuai kebutuhan)
            gb.configure_default_column(editable=False) # Default tidak bisa edit
            
            gridOptions = gb.build()
            
            grid_response = AgGrid(
                df_grid,
                gridOptions=gridOptions,
                update_mode=GridUpdateMode.MODEL_CHANGED,
                fit_columns_on_grid_load=True,
                height=400,
                theme='streamlit',
                key=f"aggrid_{unique_key}"
            )
            # Return dataframenya
            return pd.DataFrame(grid_response['data'])
            
        except Exception as e:
            # JIKA AGGRID ERROR, JANGAN TAMPILKAN ERROR MERAH KE USER
            # Cukup print ke console server untuk debugging, lalu switch ke Fallback
            print(f"AgGrid Error (Fallback triggered) for {unique_key}: {e}")
            use_aggrid_attempt = False # Trigger fallback below

    # -- MODE 2: NATIVE STREAMLIT (FALLBACK) --
    # Dijalankan jika HAS_AGGRID False ATAU jika AgGrid diatas Error (Exception)
    if not use_aggrid_attempt:
        # Konfigurasi Native
        return st.data_editor(
            df_data,
            column_config={
                "Status": st.column_config.CheckboxColumn("Done?", width="small"),
                # Fallback menggunakan TextColumn dengan width large & tooltip
                main_text_col: st.column_config.TextColumn(
                    main_text_col, 
                    disabled=True, 
                    width="large",
                    help="Double click untuk membaca teks penuh"
                ),
                "Bukti/Catatan": st.column_config.TextColumn(
                    "Bukti/Note (Edit Disini)", 
                    width="medium",
                    help="Double click untuk mengedit"
                )
            },
            column_order=["Status", main_text_col, "Bukti/Catatan"],
            hide_index=True,
            key=f"editor_native_{unique_key}",
            use_container_width=True
        )

# --- APLIKASI UTAMA ---

if KONEKSI_GSHEET_BERHASIL:
    
    if not KONEKSI_DROPBOX_BERHASIL:
        st.warning("⚠️ Peringatan: Koneksi ke Dropbox gagal. Fitur upload foto tidak aktif, namun input data teks tetap berjalan.")

    # ==========================================
    # SIDEBAR: MANAJEMEN TARGET
    # ==========================================
    with st.sidebar:
        st.header("🎯 Manajemen Target")
        
        tab_team, tab_individu, tab_admin = st.tabs(["Team", "Pribadi", "Admin"])

        # 1. TARGET TEAM
        with tab_team:
            st.caption("Input Target Team (Bulk Input)")
            st.info("ℹ️ Tips: 1 Baris = 1 Target. Teks panjang akan otomatis menyesuaikan di database.")
            with st.form("add_team_goal", clear_on_submit=True):
                goal_team_text = st.text_area("Target Team (Satu per baris)", height=150)
                c1, c2 = st.columns(2)
                today = datetime.now(tz=ZoneInfo("Asia/Jakarta")).date()
                start_d = c1.date_input("Mulai", value=today, key="start_team")
                end_d = c2.date_input("Selesai", value=today + timedelta(days=30), key="end_team")
                
                if st.form_submit_button("➕ Tambah"):
                    targets = clean_bulk_input(goal_team_text)
                    if targets:
                        base_row = ["", str(start_d), str(end_d), "FALSE", "-"]
                        with st.spinner("Menyimpan dan memformat database..."):
                            if add_bulk_targets(SHEET_TARGET_TEAM, base_row, targets):
                                st.success(f"{len(targets)} target ditambah & database diformat!")
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error("Gagal menambah target.")

        # 2. TARGET INDIVIDU
        with tab_individu:
            st.caption("Input Target Pribadi (Bulk Input)")
            NAMA_STAF = get_daftar_staf_terbaru()
            pilih_nama = st.selectbox("Siapa Anda?", NAMA_STAF, key="sidebar_user")
            
            with st.form("add_indiv_goal", clear_on_submit=True):
                goal_indiv_text = st.text_area("Target Mingguan (Satu per baris)", height=150)
                c1, c2 = st.columns(2)
                today = datetime.now(tz=ZoneInfo("Asia/Jakarta")).date()
                start_i = c1.date_input("Mulai", value=today, key="start_indiv")
                end_i = c2.date_input("Selesai", value=today + timedelta(days=7), key="end_indiv")
                
                if st.form_submit_button("➕ Tambah"):
                    targets = clean_bulk_input(goal_indiv_text)
                    if targets:
                        base_row = [pilih_nama, "", str(start_i), str(end_i), "FALSE", "-"]
                        with st.spinner("Menyimpan dan memformat database..."):
                            if add_bulk_targets(SHEET_TARGET_INDIVIDU, base_row, targets):
                                st.success(f"{len(targets)} target ditambah & database diformat!")
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error("Gagal menambah target.")

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
                                st.success(msg)
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error(msg)

    # ==========================================
    # MAIN PAGE
    # ==========================================
    
    st.title("🚀 Sales & Marketing Action Center")
    st.caption(f"Update Realtime: {datetime.now(tz=ZoneInfo('Asia/Jakarta')).strftime('%d %B %Y %H:%M:%S')}")

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
            
            st.progress(prog_team, text=f"Pencapaian Team: {int(prog_team*100)}% ({done_team}/{total_team})")
            
            # IMPLEMENTASI HYBRID RENDERER UNTUK TEAM
            # Kolom teks utama: "Misi"
            edited_team = render_hybrid_table(df_team, "team_table", "Misi")
            
            if st.button("💾 Update Progress Team", use_container_width=True):
                with st.spinner("Menyimpan dan memformat ulang database..."):
                    if save_checklist(SHEET_TARGET_TEAM, edited_team):
                        st.toast("Progress Team Berhasil Disimpan & Diformat!", icon="✅")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("Gagal menyimpan progress.")
                
            with st.expander("📂 Upload Bukti/Update Catatan (Team)"):
                list_misi_team = df_team["Misi"].tolist()
                pilih_misi = st.selectbox("Pilih Misi Team:", list_misi_team)
                note_misi = st.text_area("Catatan Update Terbaru")
                file_misi = st.file_uploader("Upload Foto Bukti Team (Opsional)", key="up_team", disabled=not KONEKSI_DROPBOX_BERHASIL)
                
                if st.button("Kirim Update Team"):
                    if not note_misi and not file_misi:
                        st.warning("Harap isi catatan atau upload file.")
                    else:
                        with st.spinner("Mengupload dan memformat database..."):
                            pelapor_team = get_daftar_staf_terbaru()[0] if get_daftar_staf_terbaru() else "Admin"
                            sukses, msg = update_evidence_row(
                                SHEET_TARGET_TEAM,
                                pilih_misi,
                                note_misi,
                                file_misi,
                                user_folder_name=pelapor_team,
                                kategori_folder="Target_Team"
                            )
                            if sukses:
                                st.success("Update Team Terkirim & Database Diformat!")
                                st.cache_data.clear()
                                st.rerun()
                            else: st.error(msg)
        else:
            st.info("Belum ada target team. Tambahkan melalui sidebar.")

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
                
                st.progress(prog_indiv, text=f"Progress {filter_nama}: {int(prog_indiv*100)}% ({done_indiv}/{total_indiv})")
                
                # IMPLEMENTASI HYBRID RENDERER UNTUK INDIVIDU
                # Kolom teks utama: "Target"
                # Error sebelumnya terjadi di sini, sekarang sudah dihandle oleh try-except di dalam fungsi
                edited_indiv = render_hybrid_table(df_indiv_user, f"indiv_table_{filter_nama}", "Target")
                
                if st.button(f"💾 Update Progress {filter_nama}", use_container_width=True):
                      with st.spinner("Menyimpan dan memformat ulang database..."):
                        df_indiv_all_updated = df_indiv_all.copy()
                        df_indiv_all_updated.update(edited_indiv)
                        
                        if save_checklist(SHEET_TARGET_INDIVIDU, df_indiv_all_updated):
                            st.toast(f"Progress {filter_nama} Disimpan & Diformat!", icon="✅")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("Gagal menyimpan progress.")

                with st.expander(f"📂 Upload Bukti/Update Catatan ({filter_nama})"):
                    list_target_user = df_indiv_user["Target"].tolist()
                    pilih_target = st.selectbox("Pilih Target Mingguan:", list_target_user)
                    note_target = st.text_area("Catatan Update Terbaru", key="note_indiv")
                    file_target = st.file_uploader("Upload Foto Bukti Pribadi (Opsional)", key="up_indiv", disabled=not KONEKSI_DROPBOX_BERHASIL)
                    
                    if st.button("Kirim Update Pribadi"):
                        if not note_target and not file_target:
                             st.warning("Harap isi catatan atau upload file.")
                        else:
                            with st.spinner("Mengupload dan memformat database..."):
                                sukses, msg = update_evidence_row(
                                    SHEET_TARGET_INDIVIDU,
                                    pilih_target,
                                    note_target,
                                    file_target,
                                    user_folder_name=filter_nama,
                                    kategori_folder="Target_Individu"
                                )
                                if sukses:
                                    st.success("Update Pribadi Terkirim & Database Diformat!")
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
                fotos = st.file_uploader("Upload Bukti Foto", accept_multiple_files=True, disabled=not KONEKSI_DROPBOX_BERHASIL)
            
            deskripsi = st.text_area("Deskripsi Aktivitas")
            
            if st.form_submit_button("✅ Submit Laporan"):
                if not deskripsi: st.error("Deskripsi wajib diisi!")
                else:
                    with st.spinner("Memproses laporan dan memformat database..."):
                        link_foto = "-"
                        if fotos and KONEKSI_DROPBOX_BERHASIL:
                            links = []
                            for f in fotos:
                                url = upload_ke_dropbox(f, nama_pelapor, kategori="Laporan_Harian")
                                links.append(url)
                            link_foto = "\n".join(links) 
                        
                        link_sosmed = sosmed_link if sosmed_link else "-" if "Social Media Specialist" in nama_pelapor else ""
                        ts = datetime.now(tz=ZoneInfo("Asia/Jakarta")).strftime('%d-%m-%Y %H:%M:%S')
                        row = [str(ts), str(nama_pelapor), str(lokasi) if lokasi else "-", str(deskripsi), str(link_foto), str(link_sosmed)]
                        
                        if simpan_laporan_harian(row, nama_pelapor):
                            st.success("Laporan Tersimpan & Database Diformat!")
                            st.cache_data.clear()
                        else:
                            st.error("Gagal menyimpan laporan.")

    # --- 3. LOG AKTIVITAS ---
    with st.expander("📂 Riwayat Laporan (Log Aktivitas)", expanded=False):
        if st.button("🔄 Refresh Data Terbaru"):
            st.cache_data.clear()
            st.rerun()
        df_log = load_all_reports(get_daftar_staf_terbaru())
        if not df_log.empty:
            df_display = df_log.copy()
            try:
                df_display[COL_TIMESTAMP] = pd.to_datetime(df_display[COL_TIMESTAMP], format='%d-%m-%Y %H:%M:%S', errors='coerce')
                df_display = df_display.sort_values(by=COL_TIMESTAMP, ascending=False)
            except Exception:
                df_display = df_display.sort_values(by=COL_TIMESTAMP, ascending=False)
                
            st.dataframe(df_display, use_container_width=True, hide_index=True)
        else:
            st.info("Belum ada riwayat laporan.")

else:
    st.error("🛑 Gagal terhubung ke Google Sheets Database. Aplikasi tidak dapat berjalan.")
