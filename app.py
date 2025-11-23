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
# 1. AgGrid (Tabel Canggih)
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
    HAS_AGGRID = True
except ImportError:
    HAS_AGGRID = False

# 2. Plotly (Grafik Canggih)
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

# --- KONFIGURASI ---
NAMA_GOOGLE_SHEET = "Laporan Kegiatan Harian"
FOLDER_DROPBOX = "/Laporan_Kegiatan_Harian"

# Sheet Names
SHEET_CONFIG_NAMA = "Config_Staf"
SHEET_TARGET_TEAM = "Target_Team_Checklist"
SHEET_TARGET_INDIVIDU = "Target_Individu_Checklist"

# --- KOLOM LAPORAN HARIAN (UPDATED WITH REFLECTION) ---
COL_TIMESTAMP = "Timestamp"
COL_NAMA = "Nama"
COL_TEMPAT = "Tempat Dikunjungi"
COL_DESKRIPSI = "Deskripsi"
COL_LINK_FOTO = "Link Foto"
COL_LINK_SOSMED = "Link Sosmed"
# Fitur Baru: Refleksi & Reminder
COL_KESIMPULAN = "Kesimpulan"
COL_KENDALA = "Kendala"
COL_PENDING = "Next Plan (Pending)"

NAMA_KOLOM_STANDAR = [
    COL_TIMESTAMP, COL_NAMA, COL_TEMPAT, COL_DESKRIPSI, 
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
    else:
        pass
except AuthError:
    st.error("Dropbox Error: Token Autentikasi tidak valid.")
except Exception as e:
    st.error(f"Dropbox Error: {e}")


# --- FUNGSI HELPER CORE & SMART FORMATTING ---

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

        # 1. Reset Body
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": formatting_row_count},
                "cell": {"userEnteredFormat": default_body_format},
                "fields": "userEnteredFormat(verticalAlignment,wrapStrategy)"
            }
        })

        # 2. Smart Column Sizing
        for i, col_name in enumerate(headers):
            col_index = i
            cell_format_override = {}
            width = 100

            if col_name in ["Misi", "Target", "Deskripsi", "Bukti/Catatan", "Link Foto", "Link Sosmed", "Tempat Dikunjungi", "Kesimpulan", "Kendala", "Next Plan (Pending)"]:
                width = 300
                cell_format_override["wrapStrategy"] = "WRAP"
            elif col_name in ["Tgl_Mulai", "Tgl_Selesai", "Timestamp"]:
                width = 150 if col_name == "Timestamp" else 120
                cell_format_override["horizontalAlignment"] = "CENTER"
            elif col_name in ["Status", "Done?"]:
                width = 60
                cell_format_override["horizontalAlignment"] = "CENTER"
            elif col_name == "Nama":
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

        # 3. Header Styling
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
        
        # 4. Freeze Header
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
        # Pastikan header update jika ada kolom baru
        current_header = ws.row_values(1)
        if len(current_header) < len(NAMA_KOLOM_STANDAR):
            ws.resize(cols=len(NAMA_KOLOM_STANDAR))
            ws.update(range_name="A1", values=[NAMA_KOLOM_STANDAR], value_input_option='USER_ENTERED')
            
        ws.append_rows(list_of_rows, value_input_option='USER_ENTERED')
        auto_format_sheet(ws)
        return True
    except Exception as e:
        print(f"Error saving daily report batch: {e}")
        return False

@st.cache_data(ttl=30) # Cache sebentar biar responsif
def get_reminder_pending(nama_staf):
    """
    Fungsi untuk mengambil 'Next Plan (Pending)' dari entri terakhir user.
    """
    try:
        ws = get_or_create_worksheet(nama_staf)
        if not ws: return None
        all_vals = ws.get_all_records()
        if not all_vals: return None
        
        # Ambil baris terakhir
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

    # --- SIDEBAR (LOGIKA LOGIN MANAGER) ---
    with st.sidebar:
        st.header("Navigasi")
        
        if "is_manager" not in st.session_state:
            st.session_state["is_manager"] = False

        opsi_menu = ["📝 Laporan & Target"]
        if st.session_state["is_manager"]:
            opsi_menu.append("📊 Dashboard Manager")
        
        menu_nav = st.radio("Pilih Menu:", opsi_menu)
        st.divider()

        if not st.session_state["is_manager"]:
            with st.expander("🔐 Akses Manager (Khusus Admin)"):
                pwd = st.text_input("Password:", type="password", key="input_pwd")
                if st.button("Login Manager"):
                    if pwd == st.secrets.get("password_manager", "admin123"): 
                        st.session_state["is_manager"] = True
                        st.rerun()
                    else:
                        st.error("Password salah!")
        else:
            if st.button("🔓 Logout Manager"):
                st.session_state["is_manager"] = False
                st.rerun()

        st.divider()
        st.header("🎯 Manajemen Target")
        
        tab_team, tab_individu, tab_admin = st.tabs(["Team", "Pribadi", "Admin"])

        with tab_team:
            st.caption("Bulk Input Target Team")
            with st.form("add_team_goal", clear_on_submit=True):
                goal_team_text = st.text_area("Target Team (Satu per baris)", height=100)
                c1, c2 = st.columns(2)
                today = datetime.now(tz=ZoneInfo("Asia/Jakarta")).date()
                start_d = c1.date_input("Mulai", value=today, key="start_team")
                end_d = c2.date_input("Selesai", value=today + timedelta(days=30), key="end_team")
                if st.form_submit_button("➕ Tambah"):
                    targets = clean_bulk_input(goal_team_text)
                    if targets:
                        if add_bulk_targets(SHEET_TARGET_TEAM, ["", str(start_d), str(end_d), "FALSE", "-"], targets):
                            st.success(f"{len(targets)} target ditambahkan!"); st.cache_data.clear(); st.rerun()
                        else: st.error("Gagal.")

        with tab_individu:
            st.caption("Bulk Input Target Pribadi")
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
                        if add_bulk_targets(SHEET_TARGET_INDIVIDU, [pilih_nama, "", str(start_i), str(end_i), "FALSE", "-"], targets):
                            st.success(f"{len(targets)} target ditambahkan!"); st.cache_data.clear(); st.rerun()
                        else: st.error("Gagal.")

        with tab_admin:
            with st.expander("➕ Tambah Karyawan"):
                with st.form("add_staff", clear_on_submit=True):
                    new_name = st.text_input("Nama")
                    new_role = st.text_input("Jabatan")
                    if st.form_submit_button("Tambah"):
                        if new_name and new_role:
                            res, msg = tambah_staf_baru(f"{new_name} ({new_role})")
                            if res: st.success(msg); st.cache_data.clear(); st.rerun()
                            else: st.error(msg)

    st.title("🚀 Sales & Marketing Action Center")
    st.caption(f"Realtime: {datetime.now(tz=ZoneInfo('Asia/Jakarta')).strftime('%d %B %Y %H:%M:%S')}")

    # --- MENU 1: OPERASIONAL ---
    if menu_nav == "📝 Laporan & Target":
        st.subheader("📊 Checklist Target (Result KPI)")
        col_dash_1, col_dash_2 = st.columns(2)
        
        df_team = load_checklist(SHEET_TARGET_TEAM, ["Misi", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan"])
        df_indiv_all = load_checklist(SHEET_TARGET_INDIVIDU, ["Nama", "Target", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan"])

        with col_dash_1:
            st.markdown("#### 🏆 Target Team")
            if not df_team.empty:
                done = len(df_team[df_team['Status'] == True])
                st.progress(done/len(df_team) if len(df_team)>0 else 0, text=f"Pencapaian: {done}/{len(df_team)}")
                edited_team = render_hybrid_table(df_team, "team_table", "Misi")
                if st.button("💾 Simpan Team", use_container_width=True):
                      if save_checklist(SHEET_TARGET_TEAM, edited_team): st.toast("Tersimpan!", icon="✅"); st.cache_data.clear(); st.rerun()
                
                with st.expander("📂 Update Bukti (Team)"):
                    pilih_misi = st.selectbox("Misi:", df_team["Misi"].tolist())
                    note_misi = st.text_area("Catatan")
                    file_misi = st.file_uploader("Bukti", key="up_team", disabled=not KONEKSI_DROPBOX_BERHASIL)
                    if st.button("Update Team"):
                        pelapor = get_daftar_staf_terbaru()[0] if get_daftar_staf_terbaru() else "Admin"
                        sukses, msg = update_evidence_row(SHEET_TARGET_TEAM, pilih_misi, note_misi, file_misi, pelapor, "Target_Team")
                        if sukses: st.success("Updated!"); st.cache_data.clear(); st.rerun()
                        else: st.error(msg)

        with col_dash_2:
            st.markdown("#### ⚡ Target Individu")
            filter_nama = st.selectbox("Filter:", get_daftar_staf_terbaru(), index=0)
            if not df_indiv_all.empty:
                df_user = df_indiv_all[df_indiv_all["Nama"] == filter_nama]
                if not df_user.empty:
                    done = len(df_user[df_user['Status'] == True])
                    st.progress(done/len(df_user) if len(df_user)>0 else 0, text=f"Progress: {done}/{len(df_user)}")
                    edited_indiv = render_hybrid_table(df_user, f"indiv_{filter_nama}", "Target")
                    if st.button(f"💾 Simpan {filter_nama}", use_container_width=True):
                        df_all_upd = df_indiv_all.copy(); df_all_upd.update(edited_indiv)
                        if save_checklist(SHEET_TARGET_INDIVIDU, df_all_upd): st.toast("Tersimpan!", icon="✅"); st.cache_data.clear(); st.rerun()
                    
                    with st.expander(f"📂 Update Bukti ({filter_nama})"):
                        pilih_target = st.selectbox("Target:", df_user["Target"].tolist())
                        note_target = st.text_area("Catatan", key="note_indiv")
                        file_target = st.file_uploader("Bukti", key="up_indiv", disabled=not KONEKSI_DROPBOX_BERHASIL)
                        if st.button("Update Pribadi"):
                             sukses, msg = update_evidence_row(SHEET_TARGET_INDIVIDU, pilih_target, note_target, file_target, filter_nama, "Target_Individu")
                             if sukses: st.success("Updated!"); st.cache_data.clear(); st.rerun()
                             else: st.error(msg)
                else: st.info("Belum ada target.")
            else: st.info("Data kosong.")

        # --- INPUT HARIAN (DYNAMIC + REFLECTION + REMINDER) ---
        st.divider()
        with st.container(border=True):
            st.subheader("📝 Input Laporan Harian (Activity)")
            
            # 1. Identitas & REMINDER CHECK
            c_nama, c_reminder = st.columns([1, 2])
            with c_nama:
                nama_pelapor = st.selectbox("Nama Pelapor", get_daftar_staf_terbaru(), key="pelapor_main")
            
            # >>> LOGIKA REMINDER DISINI <<<
            with c_reminder:
                pending_msg = get_reminder_pending(nama_pelapor)
                if pending_msg:
                    st.warning(f"🔔 **Reminder:** Kamu punya pendingan kemarin: '{pending_msg}'")
                else:
                    st.caption("Tidak ada pendingan dari laporan terakhir.")

            # 2. Kategori Aktivitas
            kategori_aktivitas = st.radio(
                "Jenis Aktivitas:", 
                ["🚗 Sales", "💻 Digital Marketing / Konten / Ads", "📞 Telesales / Follow Up", "🏢 Lainnya"],
                horizontal=True
            )

            c1, c2 = st.columns(2)
            with c1:
                today_now = datetime.now(tz=ZoneInfo("Asia/Jakarta")).date()
                st.markdown(f"**Tanggal:** `{today_now.strftime('%d-%m-%Y')}`")
                sosmed_link = ""
                if "Digital Marketing" in kategori_aktivitas:
                    sosmed_link = st.text_input("Link Konten / Ads / Drive (Wajib jika ada)")
            
            with c2:
                lokasi_input = ""
                if "Kunjungan" in kategori_aktivitas:
                    lokasi_input = st.text_input("📍 Nama Klien / Lokasi Kunjungan (Wajib)")
                else:
                    lokasi_input = st.text_input("Jenis Tugas (Otomatis)", value=kategori_aktivitas.split(' ')[1], disabled=True)
                
                fotos = st.file_uploader("Upload Bukti (Foto/Screenshot/Dokumen)", accept_multiple_files=True, disabled=not KONEKSI_DROPBOX_BERHASIL)

            # Deskripsi & Detail Foto
            deskripsi_map = {}
            main_deskripsi = ""

            if fotos:
                st.info("📸 **Detail Bukti:** Berikan keterangan spesifik untuk setiap file:")
                for i, f in enumerate(fotos):
                    with st.container(border=True):
                        col_img, col_desc = st.columns([1, 3])
                        with col_img:
                            if f.type.startswith('image'): st.image(f, width=150)
                            else: st.markdown(f"📄 **{f.name}**")
                        with col_desc:
                            deskripsi_map[f.name] = st.text_area(
                                f"Ket. File: {f.name}", 
                                height=70, 
                                key=f"desc_{i}", 
                                placeholder="Jelaskan aktivitas terkait file ini..."
                            )
            else:
                placeholder_text = "Jelaskan hasil kunjungan..." if "Kunjungan" in kategori_aktivitas else "Jelaskan konten/ads/calls yang dikerjakan..."
                main_deskripsi = st.text_area("Deskripsi Aktivitas", placeholder=placeholder_text)

            # --- BAGIAN BARU: REFLEKSI HARIAN ---
            st.divider()
            st.markdown("#### 🏁 Kesimpulan Harian")
            st.caption("Bagian ini penting agar progress besok lebih terarah.")
            
            col_ref_1, col_ref_2 = st.columns(2)
            with col_ref_1:
                input_kesimpulan = st.text_area("💡 Kesimpulan / Apa yang dicapai hari ini?", height=100, placeholder="Contoh: Klien setuju, tapi minta diskon. / Konten sudah jadi 3 feeds.")
            
            with col_ref_2:
                input_kendala = st.text_area("🚧 Kendala / Masalah?", height=100, placeholder="Contoh: Hujan deras jadi telat. / Laptop agak lemot render video.")

            input_pending = st.text_input("📌 Next Plan / Pending Item (Akan jadi Reminder Besok)", placeholder="Contoh: Follow up Bu Susi jam 10 pagi. / Revisi desain banner.")

            # BUTTON SUBMIT
            if st.button("✅ Submit Laporan", type="primary"):
                valid = True
                if "Kunjungan" in kategori_aktivitas and not lokasi_input:
                    st.error("Untuk Kunjungan, Lokasi Wajib Diisi!"); valid = False
                if not fotos and not main_deskripsi:
                    st.error("Deskripsi Wajib Diisi!"); valid = False
                
                if valid:
                    with st.spinner("Menyimpan dan memformat database..."):
                        rows = []
                        ts = datetime.now(tz=ZoneInfo("Asia/Jakarta")).strftime('%d-%m-%Y %H:%M:%S')
                        final_lokasi = lokasi_input if lokasi_input else kategori_aktivitas
                        
                        # Siapkan data tambahan (Refleksi)
                        val_kesimpulan = input_kesimpulan if input_kesimpulan else "-"
                        val_kendala = input_kendala if input_kendala else "-"
                        val_pending = input_pending if input_pending else "-"

                        if fotos and KONEKSI_DROPBOX_BERHASIL:
                            for f in fotos:
                                url = upload_ke_dropbox(f, nama_pelapor, "Laporan_Harian")
                                desc = deskripsi_map.get(f.name, "-")
                                # Append kolom baru di sini
                                rows.append([ts, nama_pelapor, final_lokasi, desc, url, sosmed_link if sosmed_link else "-", val_kesimpulan, val_kendala, val_pending])
                        else:
                            rows.append([ts, nama_pelapor, final_lokasi, main_deskripsi, "-", sosmed_link if sosmed_link else "-", val_kesimpulan, val_kendala, val_pending])
                        
                        if simpan_laporan_harian_batch(rows, nama_pelapor):
                            st.success(f"Laporan Tersimpan! Reminder besok: {val_pending}"); st.balloons(); st.cache_data.clear()
                        else: st.error("Gagal simpan.")
        
        with st.expander("📂 Log Data Mentah"):
            if st.button("🔄 Refresh"): st.cache_data.clear(); st.rerun()
            df_log = load_all_reports(get_daftar_staf_terbaru())
            if not df_log.empty: st.dataframe(df_log, use_container_width=True, hide_index=True)
            else: st.info("Kosong")

    # --- MENU 2: DASHBOARD MANAGER ---
    elif menu_nav == "📊 Dashboard Manager":
        st.header("📊 Dashboard Produktivitas")
        st.info("Dashboard ini memisahkan analisa antara Sales dan Marketing.")
        
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
            start_date = date.today() - timedelta(days=days)
            df_filt = df_log[df_log['Tanggal'] >= start_date]

            # TAB BARU: REVIEW HARIAN (CARD VIEW)
            tab_sales, tab_marketing, tab_review, tab_galeri = st.tabs(["🚗 Sales (Lapangan)", "💻 Marketing (Digital)", "📝 Review Harian", "🖼️ Galeri Bukti"])

            with tab_sales:
                df_sales = df_filt[df_filt['Kategori'] == "Kunjungan Lapangan"]
                col1, col2 = st.columns(2)
                col1.metric("Total Kunjungan", len(df_sales))
                col2.metric("Sales Aktif", df_sales[COL_NAMA].nunique())
                
                if not df_sales.empty:
                    st.subheader("Top Visiting Sales")
                    st.bar_chart(df_sales[COL_NAMA].value_counts(), color="#FF4B4B")
                    st.subheader("Lokasi Paling Sering Dikunjungi")
                    st.dataframe(df_sales[COL_TEMPAT].value_counts().head(5), use_container_width=True)
                else: st.info("Tidak ada data kunjungan lapangan.")

            with tab_marketing:
                df_mkt = df_filt[df_filt['Kategori'] == "Digital/Internal"]
                col1, col2 = st.columns(2)
                col1.metric("Total Output", len(df_mkt))
                col2.metric("Marketer Aktif", df_mkt[COL_NAMA].nunique())
                
                if not df_mkt.empty:
                    st.subheader("Produktivitas Tim Digital")
                    if HAS_PLOTLY:
                        fig = px.pie(df_mkt, names=COL_NAMA, title="Distribusi Beban Kerja Digital")
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.bar_chart(df_mkt[COL_NAMA].value_counts())
                        
                    st.subheader("Jenis Tugas Digital")
                    st.bar_chart(df_mkt[COL_TEMPAT].value_counts(), color="#00CC96")
                else: st.info("Tidak ada data aktivitas digital.")

            # --- FITUR BARU: REVIEW HARIAN CARD VIEW ---
            with tab_review:
                st.subheader("📝 Review Catatan Harian")
                st.caption("Monitoring kendala dan rencana harian tim secara detail.")
                
                # Urutkan dari yang terbaru
                df_review = df_filt.sort_values(by=COL_TIMESTAMP, ascending=False)
                
                if not df_review.empty:
                    for index, row in df_review.iterrows():
                        with st.container(border=True):
                            # Header: Nama & Waktu
                            c_head1, c_head2 = st.columns([3, 1])
                            with c_head1:
                                st.markdown(f"### 👤 {row[COL_NAMA]}")
                                st.caption(f"📅 {row[COL_TIMESTAMP]} | 🏷️ {row['Kategori']}")
                            
                            # Isi Utama
                            c_body, c_img = st.columns([3, 1])
                            with c_body:
                                st.markdown(f"**📍 Aktivitas/Lokasi:** {row[COL_TEMPAT]}")
                                st.markdown(f"**📝 Deskripsi:** {row[COL_DESKRIPSI]}")
                                
                                st.divider()
                                # Bagian Refleksi (Kolom 3 Sejajar)
                                col_a, col_b, col_c = st.columns(3)
                                with col_a: 
                                    st.info(f"💡 **Hasil/Kesimpulan:**\n\n{row.get(COL_KESIMPULAN, '-')}")
                                with col_b: 
                                    st.warning(f"🚧 **Kendala:**\n\n{row.get(COL_KENDALA, '-')}")
                                with col_c: 
                                    st.error(f"📌 **Next Plan (Reminder):**\n\n{row.get(COL_PENDING, '-')}")

                            # Sisi Kanan: Foto (Jika ada)
                            with c_img:
                                if "http" in str(row[COL_LINK_FOTO]):
                                    url_asli = row[COL_LINK_FOTO]
                                    direct_url = url_asli.replace("www.dropbox.com", "dl.dropboxusercontent.com").replace("?dl=0", "")
                                    try:
                                        st.image(direct_url, use_container_width=True)
                                        st.caption("Bukti Foto")
                                    except:
                                        st.caption("Gagal load foto")
                else:
                    st.info("Belum ada data laporan pada rentang waktu ini.")
            
            with tab_galeri:
                st.caption("Menampilkan bukti foto/dokumen terbaru")
                df_foto = df_filt[df_filt[COL_LINK_FOTO].str.contains("http", na=False, case=False)].sort_values(by=COL_TIMESTAMP, ascending=False).head(12)
                
                if not df_foto.empty:
                    data_dict = df_foto.to_dict('records')
                    cols = st.columns(4)
                    for idx, row in enumerate(data_dict):
                        with cols[idx % 4]:
                            with st.container(border=True):
                                url_asli = row[COL_LINK_FOTO] 
                                nama = row[COL_NAMA]
                                tempat = row[COL_TEMPAT]
                                direct_url = url_asli.replace("www.dropbox.com", "dl.dropboxusercontent.com").replace("?dl=0", "")
                                
                                try:
                                    st.image(direct_url, use_container_width=True)
                                    st.markdown(f"**{nama}**")
                                    st.caption(f"📍 {tempat}")
                                except:
                                    st.error("Gagal load gambar")
                                    st.link_button("Buka Link", url_asli)
                else: 
                    st.info("Belum ada bukti yang terupload.")

else: st.error("Database Error.")
