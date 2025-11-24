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

# --- HYBRID LIBRARY IMPORT ---
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
    HAS_AGGRID = True
except ImportError:
    HAS_AGGRID = False

try:
    import plotly.express as px
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

# --- KONFIGURASI HALAMAN ---
st.set_page_config(
    page_title="Sales & Marketing Action Center",
    page_icon="🚀",
    layout="wide"
)

# --- HIDE STREAMLIT STYLE ---
hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)

# --- KONFIGURASI CONSTANTS ---
NAMA_GOOGLE_SHEET = "Laporan Kegiatan Harian"
FOLDER_DROPBOX = "/Laporan_Kegiatan_Harian"
SHEET_CONFIG_NAMA = "Config_Staf"
SHEET_TARGET_TEAM = "Target_Team_Checklist"
SHEET_TARGET_INDIVIDU = "Target_Individu_Checklist"

# --- KOLOM LAPORAN ---
COL_TIMESTAMP = "Timestamp"
COL_NAMA = "Nama"
COL_KOTA = "Kota/Area"           
COL_TEMPAT = "Lokasi Spesifik"     
COL_DESKRIPSI = "Deskripsi"
COL_LINK_FOTO = "Link Foto"
COL_LINK_SOSMED = "Link Sosmed"
COL_KESIMPULAN = "Kesimpulan"
COL_KENDALA = "Kendala"
COL_PENDING = "Next Plan (Pending)"

NAMA_KOLOM_STANDAR = [
    COL_TIMESTAMP, COL_NAMA, COL_KOTA, COL_TEMPAT, COL_DESKRIPSI, 
    COL_LINK_FOTO, COL_LINK_SOSMED, 
    COL_KESIMPULAN, COL_KENDALA, COL_PENDING
]

# --- KONEKSI ---
KONEKSI_GSHEET_BERHASIL = False
KONEKSI_DROPBOX_BERHASIL = False
spreadsheet = None
dbx = None

# 1. Connect GSheet
try:
    if "gcp_service_account" in st.secrets:
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds_dict = dict(st.secrets["gcp_service_account"])
        if "private_key" in creds_dict:
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
            
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open(NAMA_GOOGLE_SHEET)
        KONEKSI_GSHEET_BERHASIL = True
    else:
        st.error("GSheet Error: Kredensial tidak ditemukan.")
except Exception as e:
    st.error(f"GSheet Error: {e}")

# 2. Connect Dropbox
try:
    if "dropbox" in st.secrets and "access_token" in st.secrets["dropbox"]:
        dbx = dropbox.Dropbox(st.secrets["dropbox"]["access_token"])
        dbx.users_get_current_account()
        KONEKSI_DROPBOX_BERHASIL = True
except Exception as e:
    pass

# --- FUNGSI HELPER ---

def auto_format_sheet(worksheet):
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

            if col_name in ["Misi", "Target", "Deskripsi", "Bukti/Catatan", "Link Foto", "Link Sosmed", "Lokasi Spesifik", "Kesimpulan", "Kendala", "Next Plan (Pending)"]:
                width = 300
                cell_format_override["wrapStrategy"] = "WRAP"
            elif col_name in ["Tgl_Mulai", "Tgl_Selesai", "Timestamp"]:
                width = 150 if col_name == "Timestamp" else 120
                cell_format_override["horizontalAlignment"] = "CENTER"
            elif col_name in ["Status", "Done?"]:
                width = 60
                cell_format_override["horizontalAlignment"] = "CENTER"
            elif col_name == "Nama" or col_name == "Kota/Area":
                width = 150

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

        if requests:
            body = {"requests": requests}
            worksheet.spreadsheet.batch_update(body)
    except Exception as e:
        print(f"Format Error: {e}")

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

def upload_ke_dropbox(file_obj, nama_staf, kategori="Umum"):
    if not KONEKSI_DROPBOX_BERHASIL or dbx is None: return "Koneksi Dropbox Error"
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
    except Exception as e: return "-"

def clean_bulk_input(text_input):
    lines = text_input.split('\n')
    cleaned_targets = []
    for line in lines:
        cleaned = re.sub(r'^[\d\.\-\*\s]+', '', line).strip()
        if cleaned: cleaned_targets.append(cleaned)
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
        df.fillna("", inplace=True)
        for col in columns:
            if col not in df.columns: df[col] = False if col == "Status" else ""
        col_status = "Status"
        if col_status in df.columns:
             df[col_status] = df[col_status].apply(lambda x: True if str(x).upper() == "TRUE" else False)
        return df
    except Exception as e: return pd.DataFrame(columns=columns)

def save_checklist(sheet_name, df):
    try:
        ws = spreadsheet.worksheet(sheet_name)
        ws.clear()
        rows_needed = len(df) + 1
        if ws.row_count < rows_needed: ws.resize(rows=rows_needed)
        df_save = df.copy()
        df_save.fillna("", inplace=True)
        col_status = "Status"
        if col_status in df_save.columns:
            df_save[col_status] = df_save[col_status].apply(lambda x: "TRUE" if x else "FALSE")
        df_save = df_save.astype(str)
        data_to_save = [df_save.columns.values.tolist()] + df_save.values.tolist()
        ws.update(range_name="A1", values=data_to_save, value_input_option='USER_ENTERED')
        auto_format_sheet(ws) 
        return True
    except Exception as e: return False

def add_bulk_targets(sheet_name, base_row_data, targets_list):
    try:
        try: ws = spreadsheet.worksheet(sheet_name)
        except: return False
        rows_to_add = []
        for t in targets_list:
            new_row = base_row_data.copy()
            if sheet_name == SHEET_TARGET_TEAM: new_row[0] = t 
            elif sheet_name == SHEET_TARGET_INDIVIDU: new_row[1] = t 
            rows_to_add.append(new_row)
        ws.append_rows(rows_to_add, value_input_option='USER_ENTERED')
        auto_format_sheet(ws)
        return True
    except Exception as e: return False

def update_evidence_row(sheet_name, target_name, note, file_obj, user_folder_name, kategori_folder):
    try:
        ws = spreadsheet.worksheet(sheet_name)
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        col_target_key = "Misi" if sheet_name == SHEET_TARGET_TEAM else "Target"
        if col_target_key not in df.columns: return False, "Kolom kunci error."
        matches = df.index[df[col_target_key] == target_name].tolist()
        if not matches: return False, "Target tidak ditemukan."
        row_idx_pandas = matches[0]
        row_idx_gsheet = row_idx_pandas + 2
        link_bukti = ""
        if file_obj: link_bukti = upload_ke_dropbox(file_obj, user_folder_name, kategori=kategori_folder)
        catatan_lama = df.at[row_idx_pandas, "Bukti/Catatan"]
        catatan_lama = str(catatan_lama) if catatan_lama else ""
        if catatan_lama == "-": catatan_lama = ""
        update_text = ""
        ts_update = datetime.now(tz=ZoneInfo("Asia/Jakarta")).strftime('%d-%m %H:%M')
        update_text += f"[{ts_update}] "
        if note: update_text += f"{note}. "
        if link_bukti and link_bukti != "-": update_text += f"[FOTO: {link_bukti}]"
        final_note = f"{catatan_lama}\n{update_text}" if catatan_lama.strip() else update_text
        if not final_note.strip(): final_note = "-"
        headers = df.columns.tolist()
        try: col_idx_gsheet = headers.index("Bukti/Catatan") + 1
        except ValueError: return False, "Kolom Bukti error."
        cell_address = gspread.utils.rowcol_to_a1(row_idx_gsheet, col_idx_gsheet)
        ws.update(range_name=cell_address, values=[[final_note]], value_input_option='USER_ENTERED')
        auto_format_sheet(ws)
        return True, "Berhasil update!"
    except Exception as e: return False, f"Error: {e}"

def simpan_laporan_harian_batch(list_of_rows, nama_staf):
    try:
        ws = get_or_create_worksheet(nama_staf)
        current_header = ws.row_values(1)
        if len(current_header) != len(NAMA_KOLOM_STANDAR):
            ws.resize(cols=len(NAMA_KOLOM_STANDAR))
            ws.update(range_name="A1", values=[NAMA_KOLOM_STANDAR], value_input_option='USER_ENTERED')
            
        ws.append_rows(list_of_rows, value_input_option='USER_ENTERED')
        auto_format_sheet(ws)
        return True
    except Exception as e:
        print(f"Error saving daily report batch: {e}")
        return False

@st.cache_data(ttl=30)
def get_reminder_pending(nama_staf):
    try:
        ws = get_or_create_worksheet(nama_staf)
        if not ws: return None
        all_vals = ws.get_all_records()
        if not all_vals: return None
        last_row = all_vals[-1]
        pending_task = last_row.get(COL_PENDING, "")
        if pending_task and str(pending_task).strip() != "-" and str(pending_task).strip() != "":
            return pending_task
        return None
    except: return None

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

def render_hybrid_table(df_data, unique_key, main_text_col):
    use_aggrid_attempt = HAS_AGGRID
    if use_aggrid_attempt:
        try:
            df_grid = df_data.copy()
            df_grid.reset_index(drop=True, inplace=True)
            gb = GridOptionsBuilder.from_dataframe(df_grid)
            gb.configure_column("Status", editable=True, width=90)
            gb.configure_column(main_text_col, wrapText=True, autoHeight=True, width=400, editable=False)
            gb.configure_column("Bukti/Catatan", wrapText=True, autoHeight=True, editable=True, cellEditor="agLargeTextCellEditor", width=300)
            gb.configure_default_column(editable=False)
            gridOptions = gb.build()
            grid_response = AgGrid(df_grid, gridOptions=gridOptions, update_mode=GridUpdateMode.MODEL_CHANGED, fit_columns_on_grid_load=True, height=400, theme='streamlit', key=f"aggrid_{unique_key}")
            return pd.DataFrame(grid_response['data'])
        except Exception as e: use_aggrid_attempt = False
    if not use_aggrid_attempt:
        return st.data_editor(df_data, column_config={"Status": st.column_config.CheckboxColumn("Done?", width="small"), main_text_col: st.column_config.TextColumn(main_text_col, disabled=True, width="large"), "Bukti/Catatan": st.column_config.TextColumn("Bukti/Note", width="medium")}, hide_index=True, key=f"editor_native_{unique_key}", use_container_width=True)

# --- APLIKASI UTAMA ---
if KONEKSI_GSHEET_BERHASIL:
    if not KONEKSI_DROPBOX_BERHASIL: st.warning("⚠️ Dropbox non-aktif. Fitur foto dimatikan.")

    # ===============================================
    # SIDEBAR: SEKARANG MEMILIKI STRUKTUR YANG JELAS
    # ===============================================
    with st.sidebar:
        st.header("Navigasi Utama")
        
        # LOGIKA LOGIN ADMIN
        if "is_admin" not in st.session_state: st.session_state["is_admin"] = False

        # --- MENU UTAMA (DIPISAH SESUAI PERMINTAAN) ---
        # 1. Check-in Lokasi (Fitur Khusus Pelacakan)
        # 2. Laporan Harian (Rekap & Target)
        opsi_menu = ["📍 Check-in Lokasi", "📝 Laporan & Target"]
        
        if st.session_state["is_admin"]:
            opsi_menu.append("📊 Dashboard Admin")
        
        menu_nav = st.radio("Pilih Aktivitas:", opsi_menu)
        st.divider()

        # AKSES ADMIN
        if not st.session_state["is_admin"]:
            with st.expander("🔐 Akses Khusus Admin"):
                pwd = st.text_input("Password:", type="password", key="input_pwd")
                if st.button("Login Admin"):
                    if pwd == st.secrets.get("password_admin", "fayza123"): 
                        st.session_state["is_admin"] = True
                        st.rerun()
                    else: st.error("Password salah!")
        else:
            if st.button("🔓 Logout Admin"):
                st.session_state["is_admin"] = False
                st.rerun()

        st.divider()
        st.caption("Manajemen Target (Bulk Input)")
        tab_team, tab_individu, tab_admin = st.tabs(["Team", "Pribadi", "Admin"])

        # (TAB ADMIN SIDEBAR TETAP ADA UNTUK INPUT TARGET CEPAT)
        with tab_team:
            with st.form("add_team_goal", clear_on_submit=True):
                goal_team_text = st.text_area("Target Team", height=70)
                if st.form_submit_button("➕"):
                    targets = clean_bulk_input(goal_team_text)
                    if targets:
                        today = datetime.now().date()
                        add_bulk_targets(SHEET_TARGET_TEAM, ["", str(today), str(today+timedelta(days=30)), "FALSE", "-"], targets)
                        st.success("OK"); st.cache_data.clear()

        with tab_individu:
            pilih_nama_sb = st.selectbox("Nama", get_daftar_staf_terbaru(), key="sb_nama")
            with st.form("add_indiv_goal", clear_on_submit=True):
                goal_indiv_text = st.text_area("Target Mingguan", height=70)
                if st.form_submit_button("➕"):
                    targets = clean_bulk_input(goal_indiv_text)
                    if targets:
                        today = datetime.now().date()
                        add_bulk_targets(SHEET_TARGET_INDIVIDU, [pilih_nama_sb, "", str(today), str(today+timedelta(days=7)), "FALSE", "-"], targets)
                        st.success("OK"); st.cache_data.clear()

        with tab_admin:
            if st.session_state["is_admin"]:
                with st.form("add_staff", clear_on_submit=True):
                    new_name = st.text_input("Nama")
                    new_role = st.text_input("Jabatan")
                    if st.form_submit_button("Tambah"):
                        tambah_staf_baru(f"{new_name} ({new_role})")
                        st.success("OK"); st.rerun()
            else: st.caption("Login dulu.")

    # TITLE UTAMA
    st.title("🚀 Sales & Marketing Action Center")
    st.caption(f"Realtime: {datetime.now(tz=ZoneInfo('Asia/Jakarta')).strftime('%d %B %Y %H:%M:%S')}")

    # =======================================================
    # MENU 1: 📍 CHECK-IN LOKASI (FITUR SPESIAL TERPISAH)
    # =======================================================
    if menu_nav == "📍 Check-in Lokasi":
        st.markdown("### 📍 Live Location Tracking (Real-time)")
        st.info("Gunakan menu ini setiap kali Anda tiba di lokasi baru atau memulai aktivitas lapangan.")
        
        with st.container(border=True):
            # 1. Identitas
            c1, c2 = st.columns([1, 2])
            with c1:
                nama_pelapor = st.selectbox("Nama Pelapor", get_daftar_staf_terbaru(), key="pelapor_checkin")
            with c2:
                # 2. Input Lokasi & Divisi
                kategori_aktivitas = st.radio("Divisi/Tugas:", ["🚗 Sales", "💻 Marketing", "📞 Admin", "🏢 Lainnya"], horizontal=True)

            st.divider()
            col_loc1, col_loc2 = st.columns(2)
            with col_loc1:
                input_kota = st.text_input("Kota / Area (Grouping)", placeholder="Cth: Yogyakarta, Jaksel")
            with col_loc2:
                input_tempat = st.text_input("Lokasi Spesifik / Nama Klien", placeholder="Cth: UII, PT Maju Jaya")

            # 3. Bukti
            main_deskripsi = st.text_area("Keterangan Singkat", placeholder="Sedang melakukan apa?", height=70)
            fotos = st.file_uploader("📸 Ambil Foto (Bukti Kehadiran)", accept_multiple_files=True, disabled=not KONEKSI_DROPBOX_BERHASIL)
            
            if st.button("📍 Check-in Sekarang", type="primary", use_container_width=True):
                if not input_kota or not input_tempat:
                    st.error("⚠️ Kota dan Lokasi Spesifik wajib diisi!")
                else:
                    with st.spinner("Mengirim data lokasi..."):
                        rows = []
                        ts = datetime.now(tz=ZoneInfo("Asia/Jakarta")).strftime('%d-%m-%Y %H:%M:%S')
                        folder_kategori = input_kota.replace("/", "_").strip()
                        
                        url_foto = "-"
                        if fotos and KONEKSI_DROPBOX_BERHASIL:
                            for idx, f in enumerate(fotos):
                                temp_url = upload_ke_dropbox(f, nama_pelapor, kategori=folder_kategori)
                                if idx == 0: url_foto = temp_url 
                        
                        # Simpan Data (Bagian Refleksi dikosongkan "-")
                        rows.append([
                            ts, nama_pelapor, input_kota.upper(), input_tempat, main_deskripsi, 
                            url_foto, "-", "-", "-", "-" 
                        ])
                        
                        if simpan_laporan_harian_batch(rows, nama_pelapor):
                            st.success(f"✅ Berhasil Check-in di {input_tempat}!"); st.balloons(); st.cache_data.clear()
                        else: st.error("Gagal simpan.")

    # =======================================================
    # MENU 2: 📝 LAPORAN HARIAN (REKAP & TARGET)
    # =======================================================
    elif menu_nav == "📝 Laporan & Target":
        st.subheader("📊 Laporan & Evaluasi Harian")
        st.caption("Gunakan menu ini di akhir hari untuk update target dan refleksi.")

        col_dash_1, col_dash_2 = st.columns(2)
        df_team = load_checklist(SHEET_TARGET_TEAM, ["Misi", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan"])
        df_indiv_all = load_checklist(SHEET_TARGET_INDIVIDU, ["Nama", "Target", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan"])

        # --- CHECKLIST TARGET ---
        with col_dash_1:
            st.markdown("#### 🏆 Target Team")
            if not df_team.empty:
                done = len(df_team[df_team['Status'] == True])
                st.progress(done/len(df_team) if len(df_team)>0 else 0, text=f"{done}/{len(df_team)}")
                edited_team = render_hybrid_table(df_team, "team_table", "Misi")
                if st.button("💾 Simpan Team"):
                      if save_checklist(SHEET_TARGET_TEAM, edited_team): st.toast("Saved!"); st.cache_data.clear()
                
                with st.expander("Update Bukti (Team)"):
                    pilih_misi = st.selectbox("Misi:", df_team["Misi"].tolist())
                    note_misi = st.text_area("Note Team")
                    if st.button("Update Team Note"):
                        pelapor = get_daftar_staf_terbaru()[0]
                        sukses, msg = update_evidence_row(SHEET_TARGET_TEAM, pilih_misi, note_misi, None, pelapor, "Target_Team")
                        if sukses: st.success("Updated!"); st.cache_data.clear()

        with col_dash_2:
            st.markdown("#### ⚡ Target Pribadi")
            filter_nama = st.selectbox("Filter:", get_daftar_staf_terbaru(), index=0)
            if not df_indiv_all.empty:
                df_user = df_indiv_all[df_indiv_all["Nama"] == filter_nama]
                if not df_user.empty:
                    done = len(df_user[df_user['Status'] == True])
                    st.progress(done/len(df_user) if len(df_user)>0 else 0, text=f"{done}/{len(df_user)}")
                    edited_indiv = render_hybrid_table(df_user, f"indiv_{filter_nama}", "Target")
                    if st.button("💾 Simpan Pribadi"):
                        df_all_upd = df_indiv_all.copy(); df_all_upd.update(edited_indiv)
                        if save_checklist(SHEET_TARGET_INDIVIDU, df_all_upd): st.toast("Saved!"); st.cache_data.clear()
                    
                    with st.expander(f"Update Bukti ({filter_nama})"):
                        pilih_target = st.selectbox("Target:", df_user["Target"].tolist())
                        note_target = st.text_area("Note Pribadi", key="note_indiv")
                        if st.button("Update Note"):
                             sukses, msg = update_evidence_row(SHEET_TARGET_INDIVIDU, pilih_target, note_target, None, filter_nama, "Target_Individu")
                             if sukses: st.success("Updated!"); st.cache_data.clear()
                else: st.info("Belum ada target.")
            else: st.info("Data kosong.")

        # --- REFLEKSI HARIAN ---
        st.divider()
        with st.container(border=True):
            st.subheader("📝 Refleksi Akhir Hari")
            
            c_nama, c_reminder = st.columns([1, 2])
            with c_nama:
                nama_pelapor = st.selectbox("Nama Pelapor", get_daftar_staf_terbaru(), key="pelapor_rekap")
            with c_reminder:
                pending_msg = get_reminder_pending(nama_pelapor)
                if pending_msg: st.warning(f"🔔 Reminder: '{pending_msg}'")
                else: st.caption("✅ Tidak ada pendingan.")
            
            c_ref1, c_ref2 = st.columns(2)
            with c_ref1: input_kesimpulan = st.text_area("💡 Hasil Hari Ini")
            with c_ref2: input_kendala = st.text_area("🚧 Kendala")
            input_pending = st.text_input("📌 Next Plan (Reminder Besok)")
            
            if st.button("✅ Submit Laporan Harian"):
                 with st.spinner("Menyimpan rekap..."):
                    rows = []
                    ts = datetime.now(tz=ZoneInfo("Asia/Jakarta")).strftime('%d-%m-%Y %H:%M:%S')
                    # Simpan Data (Bagian Lokasi diisi "Daily Report")
                    rows.append([
                        ts, nama_pelapor, "REKAP HARIAN", "Kantor/Remote", "Laporan Akhir Hari", 
                        "-", "-", 
                        input_kesimpulan if input_kesimpulan else "-", 
                        input_kendala if input_kendala else "-", 
                        input_pending if input_pending else "-"
                    ])
                    if simpan_laporan_harian_batch(rows, nama_pelapor):
                        st.success("Rekap tersimpan!"); st.cache_data.clear()

        with st.expander("📂 Log Data Mentah"):
            if st.button("🔄 Refresh Log"): st.cache_data.clear(); st.rerun()
            df_log = load_all_reports(get_daftar_staf_terbaru())
            if not df_log.empty: st.dataframe(df_log, use_container_width=True, hide_index=True)

    # =======================================================
    # MENU 3: 📊 DASHBOARD ADMIN
    # =======================================================
    elif menu_nav == "📊 Dashboard Admin":
        st.header("📊 Dashboard Produktivitas")
        if st.button("🔄 Refresh Data"): st.cache_data.clear(); st.rerun()

        df_log = load_all_reports(get_daftar_staf_terbaru())
        if not df_log.empty:
            try:
                df_log[COL_TIMESTAMP] = pd.to_datetime(df_log[COL_TIMESTAMP], format='%d-%m-%Y %H:%M:%S', errors='coerce')
                df_log['Tanggal'] = df_log[COL_TIMESTAMP].dt.date
            except: df_log['Tanggal'] = datetime.now().date()
            
            keywords_digital = ["Digital", "Marketing", "Konten", "Ads", "Telesales", "Admin", "Follow"]
            def get_category(val):
                val_str = str(val)
                if any(k in val_str for k in keywords_digital): return "Digital/Internal"
                return "Kunjungan Lapangan"
            
            df_log['Kategori'] = df_log[COL_TEMPAT].apply(get_category)
            days = st.selectbox("Rentang Waktu:", [7, 14, 30], index=0)
            df_filt = df_log[df_log['Tanggal'] >= (date.today() - timedelta(days=days))]

            tab_sales, tab_marketing, tab_sebaran, tab_review = st.tabs(["🚗 Sales", "💻 Marketing", "🗺️ Peta Area", "📝 Review"])

            with tab_sales:
                df_sales = df_filt[df_filt['Kategori'] == "Kunjungan Lapangan"]
                col1, col2 = st.columns(2)
                col1.metric("Total Kunjungan", len(df_sales))
                col2.metric("Sales Aktif", df_sales[COL_NAMA].nunique())
                if not df_sales.empty:
                    st.bar_chart(df_sales[COL_NAMA].value_counts(), color="#FF4B4B")
                    st.dataframe(df_sales[COL_TEMPAT].value_counts().head(5), use_container_width=True)

            with tab_marketing:
                df_mkt = df_filt[df_filt['Kategori'] == "Digital/Internal"]
                col1, col2 = st.columns(2)
                col1.metric("Total Output", len(df_mkt))
                col2.metric("Aktif", df_mkt[COL_NAMA].nunique())
                if not df_mkt.empty:
                    st.bar_chart(df_mkt[COL_NAMA].value_counts())

            with tab_sebaran:
                st.subheader("🗺️ Visualisasi Sebaran Aktivitas")
                if not df_filt.empty and COL_KOTA in df_filt.columns and COL_TEMPAT in df_filt.columns:
                    # Filter Out Rekap Harian agar peta bersih
                    df_map = df_filt[df_filt[COL_KOTA] != "REKAP HARIAN"]
                    df_sebaran = df_map.groupby([COL_KOTA, COL_TEMPAT]).size().reset_index(name='Jumlah Kunjungan')
                    
                    if HAS_PLOTLY:
                        fig_tree = px.treemap(
                            df_sebaran, path=[COL_KOTA, COL_TEMPAT], values='Jumlah Kunjungan',
                            color=COL_KOTA, title="Hierarki Kunjungan: Area > Lokasi Spesifik"
                        )
                        st.plotly_chart(fig_tree, use_container_width=True)
                    else: st.dataframe(df_sebaran)

            with tab_review:
                df_review = df_filt.sort_values(by=COL_TIMESTAMP, ascending=False)
                if not df_review.empty:
                    for index, row in df_review.iterrows():
                        with st.container(border=True):
                            c_head1, c_head2 = st.columns([3, 1])
                            with c_head1:
                                st.markdown(f"### 👤 {row[COL_NAMA]}")
                                st.caption(f"📅 {row[COL_TIMESTAMP]}")
                            c_body, c_img = st.columns([3, 1])
                            with c_body:
                                st.markdown(f"**📍 {row.get(COL_KOTA, '-')} > {row[COL_TEMPAT]}**")
                                st.markdown(f"**📝 Desc:** {row[COL_DESKRIPSI]}")
                                st.divider()
                                col_a, col_b = st.columns(2)
                                with col_a: st.info(f"💡 {row.get(COL_KESIMPULAN, '-')}")
                                with col_b: st.error(f"📌 {row.get(COL_PENDING, '-')}")
                            with c_img:
                                if "http" in str(row[COL_LINK_FOTO]):
                                    url = row[COL_LINK_FOTO].replace("www.dropbox.com", "dl.dropboxusercontent.com").replace("?dl=0", "")
                                    st.image(url, use_container_width=True)

else: st.error("Database Error.")
