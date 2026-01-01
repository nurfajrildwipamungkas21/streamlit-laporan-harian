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

# =========================================================
# OPTIONAL LIBRARIES (FALLBACK MECHANISM)
# =========================================================
# 1) AgGrid (Tabel Canggih)
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
    HAS_AGGRID = True
except ImportError:
    HAS_AGGRID = False

# 2) Plotly (Grafik Canggih)
try:
    import plotly.express as px
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False


# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="Sales & Marketing Action Center",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =========================================================
# UI THEME / CSS (PRO LOOK)
# =========================================================
def inject_pro_css():
    st.markdown(
        """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

:root{
  --bg0: #070B14;
  --bg1: #0B1220;
  --panel: rgba(255,255,255,.04);
  --panel2: rgba(255,255,255,.06);
  --border: rgba(255,255,255,.10);
  --border2: rgba(255,255,255,.14);
  --text: rgba(255,255,255,.92);
  --muted: rgba(255,255,255,.65);
  --muted2: rgba(255,255,255,.52);
  --accent: #7C5CFC;
  --accent2:#22C55E;
  --warn: #F59E0B;
  --danger:#EF4444;
  --radius: 16px;
  --radius2: 14px;
  --shadow: 0 10px 30px rgba(0,0,0,.35);
}

html, body, [class*="css"] {
  font-family: 'Inter', system-ui, -apple-system, Segoe UI, Roboto, sans-serif !important;
  color: var(--text);
}

div[data-testid="stAppViewContainer"]{
  background:
    radial-gradient(1200px 700px at 15% 10%, rgba(124,92,252,.18), transparent 55%),
    radial-gradient(900px 600px at 85% 25%, rgba(34,197,94,.10), transparent 50%),
    linear-gradient(180deg, var(--bg0), var(--bg1) 35%, var(--bg0));
}

div[data-testid="stSidebar"]{
  background: linear-gradient(180deg, rgba(255,255,255,.03), rgba(255,255,255,.02));
  border-right: 1px solid var(--border);
}

div[data-testid="stSidebar"] .stMarkdown, 
div[data-testid="stSidebar"] label,
div[data-testid="stSidebar"] p{
  color: var(--text);
}

div[data-testid="stToolbar"] { visibility: hidden; height: 0px; }
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}

h1, h2, h3, h4{
  letter-spacing: -0.02em;
}
h1{
  font-weight: 800 !important;
}
h2{
  font-weight: 700 !important;
}
h3{
  font-weight: 700 !important;
}

small, .stCaption, div[data-testid="stCaptionContainer"]{
  color: var(--muted) !important;
}

hr{
  border: none !important;
  height: 1px !important;
  background: linear-gradient(90deg, transparent, var(--border), transparent) !important;
  margin: 14px 0 !important;
}

/* Containers with border=True: make them nicer */
div[data-testid="stVerticalBlockBorderWrapper"]{
  border: 1px solid var(--border) !important;
  background: var(--panel) !important;
  border-radius: var(--radius) !important;
  box-shadow: var(--shadow);
}

/* Alerts (info/warn/error/success) */
div[data-testid="stAlert"]{
  border-radius: var(--radius2) !important;
  border: 1px solid var(--border) !important;
}

/* Buttons */
div.stButton > button{
  border-radius: 14px !important;
  border: 1px solid var(--border) !important;
  background: rgba(255,255,255,.04) !important;
  color: var(--text) !important;
  padding: 0.60rem 0.95rem !important;
  font-weight: 650 !important;
  transition: all .15s ease-in-out !important;
}
div.stButton > button:hover{
  transform: translateY(-1px);
  border-color: var(--border2) !important;
  background: rgba(255,255,255,.06) !important;
}
button[data-testid="baseButton-primary"]{
  border: none !important;
  background: linear-gradient(135deg, rgba(124,92,252,1), rgba(88,101,242,1)) !important;
  box-shadow: 0 10px 25px rgba(124,92,252,.25) !important;
}
button[data-testid="baseButton-primary"]:hover{
  filter: brightness(1.04);
}

/* Inputs */
div[data-testid="stTextInput"] input,
div[data-testid="stTextArea"] textarea,
div[data-testid="stSelectbox"] div[role="combobox"],
div[data-testid="stMultiSelect"] div[role="combobox"]{
  border-radius: 14px !important;
  border: 1px solid var(--border) !important;
  background: rgba(255,255,255,.03) !important;
}
div[data-testid="stTextInput"] input:focus,
div[data-testid="stTextArea"] textarea:focus{
  border-color: rgba(124,92,252,.55) !important;
  box-shadow: 0 0 0 3px rgba(124,92,252,.15) !important;
}

/* Tabs */
button[data-baseweb="tab"]{
  font-weight: 650 !important;
}
div[data-baseweb="tab-list"]{
  gap: 6px !important;
}

/* Metric styling */
div[data-testid="stMetric"]{
  background: rgba(255,255,255,.035);
  border: 1px solid var(--border);
  border-radius: 16px;
  padding: 14px 14px 10px 14px;
}

/* DataFrame / Editor */
div[data-testid="stDataFrame"]{
  border-radius: 16px !important;
  overflow: hidden !important;
  border: 1px solid var(--border) !important;
}
div[data-testid="stDataEditor"]{
  border-radius: 16px !important;
  overflow: hidden !important;
  border: 1px solid var(--border) !important;
}

/* Custom classes */
.hero{
  padding: 18px 18px;
  border-radius: 20px;
  border: 1px solid var(--border);
  background:
    radial-gradient(800px 240px at 15% 20%, rgba(124,92,252,.18), transparent 55%),
    radial-gradient(700px 240px at 85% 50%, rgba(34,197,94,.10), transparent 55%),
    rgba(255,255,255,.03);
  box-shadow: var(--shadow);
}
.hero-title{
  font-size: 2.05rem;
  margin: 0 0 4px 0;
  font-weight: 850;
}
.hero-sub{
  margin: 0;
  color: var(--muted);
  font-size: 0.98rem;
}
.badges{
  display:flex;
  flex-wrap:wrap;
  gap:8px;
  justify-content:flex-end;
}
.badge{
  display:inline-flex;
  align-items:center;
  gap:8px;
  padding: 8px 10px;
  border-radius: 999px;
  border: 1px solid var(--border);
  background: rgba(255,255,255,.03);
  font-size: .82rem;
  color: var(--text);
}
.badge-dot{
  width:9px;
  height:9px;
  border-radius:999px;
  display:inline-block;
}
.section-title{
  display:flex;
  align-items:baseline;
  justify-content:space-between;
  gap: 12px;
  margin: 4px 0 10px 0;
}
.section-title h2, .section-title h3{
  margin: 0;
}
.kicker{
  color: var(--muted2);
  font-size: .88rem;
}
.pill{
  display:inline-flex;
  align-items:center;
  gap:8px;
  padding: 7px 10px;
  border-radius: 999px;
  border: 1px solid var(--border);
  background: rgba(255,255,255,.03);
  font-size: .85rem;
  color: var(--muted);
}
</style>
        """,
        unsafe_allow_html=True,
    )

inject_pro_css()


# =========================================================
# CONFIG
# =========================================================
APP_TITLE = "Sales & Marketing Action Center"
APP_TAGLINE = "Laporan harian, target KPI, dan feedback team lead — dalam satu dashboard yang rapi."

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
COL_KESIMPULAN = "Kesimpulan"
COL_KENDALA = "Kendala"
COL_PENDING = "Next Plan (Pending)"
COL_FEEDBACK = "Feedback Lead"

NAMA_KOLOM_STANDAR = [
    COL_TIMESTAMP, COL_NAMA, COL_TEMPAT, COL_DESKRIPSI,
    COL_LINK_FOTO, COL_LINK_SOSMED,
    COL_KESIMPULAN, COL_KENDALA, COL_PENDING,
    COL_FEEDBACK
]


# =========================================================
# CONNECTIONS
# =========================================================
KONEKSI_GSHEET_BERHASIL = False
KONEKSI_DROPBOX_BERHASIL = False
spreadsheet = None
dbx = None

# 1) Connect GSheet
try:
    if "gcp_service_account" in st.secrets:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds_dict = dict(st.secrets["gcp_service_account"])
        if "private_key" in creds_dict:
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open(NAMA_GOOGLE_SHEET)
        KONEKSI_GSHEET_BERHASIL = True
    else:
        st.error("GSheet Error: Kredensial tidak ditemukan di secrets.")
except Exception as e:
    st.error(f"GSheet Error: {e}")

# 2) Connect Dropbox
try:
    if "dropbox" in st.secrets and "access_token" in st.secrets["dropbox"]:
        dbx = dropbox.Dropbox(st.secrets["dropbox"]["access_token"])
        dbx.users_get_current_account()
        KONEKSI_DROPBOX_BERHASIL = True
except AuthError:
    st.error("Dropbox Error: Token Autentikasi tidak valid.")
except Exception as e:
    st.error(f"Dropbox Error: {e}")


# =========================================================
# HELPERS (CORE + FORMATTING)
# =========================================================
def now_jakarta() -> datetime:
    return datetime.now(tz=ZoneInfo("Asia/Jakarta"))

def fmt_realtime(dt: datetime) -> str:
    # Tetap aman tanpa locale OS
    bulan_id = [
        "Januari","Februari","Maret","April","Mei","Juni",
        "Juli","Agustus","September","Oktober","November","Desember"
    ]
    return f"{dt.day:02d} {bulan_id[dt.month-1]} {dt.year} {dt:%H:%M:%S}"

def badge_html(label: str, ok: bool, hint: str = "") -> str:
    if ok:
        dot = "#22C55E"
        text = f"{label}: OK"
    else:
        dot = "#F59E0B"
        text = f"{label}: OFF"

    hint_txt = f" — {hint}" if hint else ""
    return f"""
    <span class="badge">
      <span class="badge-dot" style="background:{dot}; box-shadow:0 0 0 4px {dot}22;"></span>
      <span>{text}{hint_txt}</span>
    </span>
    """

def section_header(title: str, subtitle: str = ""):
    st.markdown(
        f"""
<div class="section-title">
  <div>
    <h3>{title}</h3>
    <div class="kicker">{subtitle}</div>
  </div>
</div>
        """,
        unsafe_allow_html=True,
    )

def auto_format_sheet(worksheet):
    try:
        sheet_id = worksheet.id
        all_values = worksheet.get_all_values()
        if not all_values:
            return

        headers = all_values[0]
        data_row_count = len(all_values)
        formatting_row_count = worksheet.row_count
        if data_row_count > formatting_row_count:
            formatting_row_count = data_row_count

        requests = []
        default_body_format = {"verticalAlignment": "TOP", "wrapStrategy": "CLIP"}

        # 1) Reset Body
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": formatting_row_count},
                "cell": {"userEnteredFormat": default_body_format},
                "fields": "userEnteredFormat(verticalAlignment,wrapStrategy)"
            }
        })

        # 2) Smart Column Sizing
        for i, col_name in enumerate(headers):
            col_index = i
            cell_format_override = {}
            width = 110

            if col_name in ["Misi", "Target", "Deskripsi", "Bukti/Catatan", "Link Foto", "Link Sosmed",
                            "Tempat Dikunjungi", "Kesimpulan", "Kendala", "Next Plan (Pending)", "Feedback Lead"]:
                width = 320
                cell_format_override["wrapStrategy"] = "WRAP"
            elif col_name in ["Tgl_Mulai", "Tgl_Selesai", "Timestamp"]:
                width = 170 if col_name == "Timestamp" else 120
                cell_format_override["horizontalAlignment"] = "CENTER"
            elif col_name in ["Status", "Done?"]:
                width = 80
                cell_format_override["horizontalAlignment"] = "CENTER"
            elif col_name == "Nama":
                width = 160

            requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": col_index,
                        "endIndex": col_index + 1
                    },
                    "properties": {"pixelSize": width},
                    "fields": "pixelSize"
                }
            })

            if cell_format_override:
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "endRowIndex": formatting_row_count,
                            "startColumnIndex": col_index,
                            "endColumnIndex": col_index + 1
                        },
                        "cell": {"userEnteredFormat": cell_format_override},
                        "fields": f"userEnteredFormat({','.join(cell_format_override.keys())})"
                    }
                })

        # 3) Header Styling
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {
                    "textFormat": {"bold": True},
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE",
                    "backgroundColor": {"red": 0.12, "green": 0.14, "blue": 0.20},
                    "wrapStrategy": "WRAP"
                }},
                "fields": "userEnteredFormat(textFormat,horizontalAlignment,verticalAlignment,backgroundColor,wrapStrategy)"
            }
        })

        # 4) Freeze Header
        requests.append({
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount"
            }
        })

        if requests:
            worksheet.spreadsheet.batch_update({"requests": requests})

    except Exception as e:
        print(f"Format Error: {e}")


@st.cache_resource(ttl=60)
def get_or_create_worksheet(nama_worksheet):
    try:
        return spreadsheet.worksheet(nama_worksheet)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=nama_worksheet, rows=200, cols=len(NAMA_KOLOM_STANDAR))
        ws.append_row(NAMA_KOLOM_STANDAR, value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return ws
    except Exception:
        return None


@st.cache_data(ttl=60)
def get_daftar_staf_terbaru():
    default_staf = ["Saya"]
    if not KONEKSI_GSHEET_BERHASIL:
        return default_staf
    try:
        try:
            ws = spreadsheet.worksheet(SHEET_CONFIG_NAMA)
        except Exception:
            ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_NAMA, rows=100, cols=1)
            ws.append_row(["Daftar Nama Staf"], value_input_option="USER_ENTERED")
            ws.append_row(["Saya"], value_input_option="USER_ENTERED")
            auto_format_sheet(ws)
            return default_staf

        nama_list = ws.col_values(1)
        if len(nama_list) > 0 and nama_list[0] == "Daftar Nama Staf":
            nama_list.pop(0)
        return nama_list if nama_list else default_staf
    except Exception:
        return default_staf


def tambah_staf_baru(nama_baru):
    try:
        try:
            ws = spreadsheet.worksheet(SHEET_CONFIG_NAMA)
        except Exception:
            ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_NAMA, rows=100, cols=1)

        if nama_baru in ws.col_values(1):
            return False, "Nama sudah ada!"

        ws.append_row([nama_baru], value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return True, "Berhasil tambah tim!"
    except Exception as e:
        return False, str(e)


def upload_ke_dropbox(file_obj, nama_staf, kategori="Umum"):
    if not KONEKSI_DROPBOX_BERHASIL or dbx is None:
        return "Koneksi Dropbox Error"
    try:
        file_data = file_obj.getvalue()
        ts = now_jakarta().strftime("%Y%m%d_%H%M%S")
        clean_filename = "".join([c for c in file_obj.name if c.isalnum() or c in (".", "_")])
        clean_user_folder = "".join([c for c in nama_staf if c.isalnum() or c in (" ", "_")]).replace(" ", "_")
        clean_kategori = "".join([c for c in kategori if c.isalnum() or c in (" ", "_")]).replace(" ", "_")

        path = f"{FOLDER_DROPBOX}/{clean_user_folder}/{clean_kategori}/{ts}_{clean_filename}"
        dbx.files_upload(file_data, path, mode=dropbox.files.WriteMode.add)

        settings = SharedLinkSettings(requested_visibility=RequestedVisibility.public)
        try:
            link = dbx.sharing_create_shared_link_with_settings(path, settings=settings)
        except ApiError as e:
            if e.error.is_shared_link_already_exists():
                link = dbx.sharing_list_shared_links(path, direct_only=True).links[0]
            else:
                return "-"

        return link.url.replace("?dl=0", "?raw=1")
    except Exception:
        return "-"


def clean_bulk_input(text_input):
    lines = text_input.split("\n")
    cleaned_targets = []
    for line in lines:
        cleaned = re.sub(r"^[\d\.\-\*\s]+", "", line).strip()
        if cleaned:
            cleaned_targets.append(cleaned)
    return cleaned_targets


def load_checklist(sheet_name, columns):
    try:
        try:
            ws = spreadsheet.worksheet(sheet_name)
        except Exception:
            ws = spreadsheet.add_worksheet(title=sheet_name, rows=200, cols=len(columns))
            ws.append_row(columns, value_input_option="USER_ENTERED")
            auto_format_sheet(ws)
            return pd.DataFrame(columns=columns)

        data = ws.get_all_records()
        df = pd.DataFrame(data)
        df.fillna("", inplace=True)

        for col in columns:
            if col not in df.columns:
                df[col] = False if col == "Status" else ""

        if "Status" in df.columns:
            df["Status"] = df["Status"].apply(lambda x: True if str(x).upper() == "TRUE" else False)

        return df
    except Exception:
        return pd.DataFrame(columns=columns)


def save_checklist(sheet_name, df):
    try:
        ws = spreadsheet.worksheet(sheet_name)
        ws.clear()

        rows_needed = len(df) + 1
        if ws.row_count < rows_needed:
            ws.resize(rows=rows_needed)

        df_save = df.copy()
        df_save.fillna("", inplace=True)

        if "Status" in df_save.columns:
            df_save["Status"] = df_save["Status"].apply(lambda x: "TRUE" if x else "FALSE")

        df_save = df_save.astype(str)
        data_to_save = [df_save.columns.values.tolist()] + df_save.values.tolist()
        ws.update(range_name="A1", values=data_to_save, value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return True
    except Exception:
        return False


def add_bulk_targets(sheet_name, base_row_data, targets_list):
    try:
        try:
            ws = spreadsheet.worksheet(sheet_name)
        except Exception:
            return False

        rows_to_add = []
        for t in targets_list:
            new_row = base_row_data.copy()
            if sheet_name == SHEET_TARGET_TEAM:
                new_row[0] = t
            elif sheet_name == SHEET_TARGET_INDIVIDU:
                new_row[1] = t
            rows_to_add.append(new_row)

        ws.append_rows(rows_to_add, value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return True
    except Exception:
        return False


def update_evidence_row(sheet_name, target_name, note, file_obj, user_folder_name, kategori_folder):
    try:
        ws = spreadsheet.worksheet(sheet_name)
        data = ws.get_all_records()
        df = pd.DataFrame(data)

        col_target_key = "Misi" if sheet_name == SHEET_TARGET_TEAM else "Target"
        if col_target_key not in df.columns:
            return False, "Kolom kunci error."

        matches = df.index[df[col_target_key] == target_name].tolist()
        if not matches:
            return False, "Target tidak ditemukan."

        row_idx_pandas = matches[0]
        row_idx_gsheet = row_idx_pandas + 2  # header = row 1

        link_bukti = ""
        if file_obj:
            link_bukti = upload_ke_dropbox(file_obj, user_folder_name, kategori=kategori_folder)

        catatan_lama = df.at[row_idx_pandas, "Bukti/Catatan"]
        catatan_lama = str(catatan_lama) if catatan_lama else ""
        if catatan_lama == "-":
            catatan_lama = ""

        ts_update = now_jakarta().strftime("%d-%m %H:%M")
        update_text = f"[{ts_update}] "
        if note:
            update_text += f"{note}. "
        if link_bukti and link_bukti != "-":
            update_text += f"[FOTO: {link_bukti}]"

        final_note = f"{catatan_lama}\n{update_text}" if catatan_lama.strip() else update_text
        if not final_note.strip():
            final_note = "-"

        headers = df.columns.tolist()
        try:
            col_idx_gsheet = headers.index("Bukti/Catatan") + 1
        except ValueError:
            return False, "Kolom Bukti error."

        cell_address = gspread.utils.rowcol_to_a1(row_idx_gsheet, col_idx_gsheet)
        ws.update(range_name=cell_address, values=[[final_note]], value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return True, "Berhasil update!"
    except Exception as e:
        return False, f"Error: {e}"


def kirim_feedback_admin(nama_staf, timestamp_key, isi_feedback):
    try:
        ws = spreadsheet.worksheet(nama_staf)

        # Resize guard
        if ws.col_count < 12:
            ws.resize(cols=12)

        # Header check
        headers = ws.row_values(1)
        if COL_FEEDBACK not in headers:
            ws.update_cell(1, len(headers) + 1, COL_FEEDBACK)
            headers.append(COL_FEEDBACK)
            auto_format_sheet(ws)

        # Smart search timestamp
        all_timestamps = ws.col_values(1)

        def clean_ts(text):
            return "".join(filter(str.isdigit, str(text)))

        target_clean = clean_ts(timestamp_key)
        found_row = None

        for idx, val in enumerate(all_timestamps):
            if clean_ts(val) == target_clean:
                found_row = idx + 1
                break

        if not found_row:
            return False, f"Data tidak ditemukan untuk Timestamp: {timestamp_key}"

        col_idx = headers.index(COL_FEEDBACK) + 1
        ws.update_cell(found_row, col_idx, isi_feedback)
        return True, "Feedback terkirim!"
    except Exception as e:
        return False, f"Error: {e}"


def simpan_laporan_harian_batch(list_of_rows, nama_staf):
    try:
        ws = get_or_create_worksheet(nama_staf)
        if not ws:
            return False

        # Ensure header updated
        current_header = ws.row_values(1)
        if len(current_header) < len(NAMA_KOLOM_STANDAR):
            ws.resize(cols=len(NAMA_KOLOM_STANDAR))
            ws.update(range_name="A1", values=[NAMA_KOLOM_STANDAR], value_input_option="USER_ENTERED")

        ws.append_rows(list_of_rows, value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return True
    except Exception as e:
        print(f"Error saving daily report batch: {e}")
        return False


@st.cache_data(ttl=30)
def get_reminder_pending(nama_staf):
    try:
        ws = get_or_create_worksheet(nama_staf)
        if not ws:
            return None

        all_vals = ws.get_all_records()
        if not all_vals:
            return None

        last_row = all_vals[-1]
        pending_task = last_row.get(COL_PENDING, "")

        if pending_task and str(pending_task).strip() not in ["-", ""]:
            return pending_task
        return None
    except Exception:
        return None


@st.cache_data(ttl=60)
def load_all_reports(daftar_staf):
    all_data = []
    for nama in daftar_staf:
        try:
            ws = get_or_create_worksheet(nama)
            if ws:
                d = ws.get_all_records()
                if d:
                    all_data.extend(d)
        except Exception:
            pass
    return pd.DataFrame(all_data) if all_data else pd.DataFrame(columns=NAMA_KOLOM_STANDAR)


def render_hybrid_table(df_data, unique_key, main_text_col):
    # Prefer AgGrid if available
    use_aggrid_attempt = HAS_AGGRID
    if use_aggrid_attempt:
        try:
            df_grid = df_data.copy()
            df_grid.reset_index(drop=True, inplace=True)

            gb = GridOptionsBuilder.from_dataframe(df_grid)
            gb.configure_column("Status", editable=True, width=90)
            gb.configure_column(main_text_col, wrapText=True, autoHeight=True, width=420, editable=False)
            gb.configure_column(
                "Bukti/Catatan",
                wrapText=True,
                autoHeight=True,
                editable=True,
                cellEditor="agLargeTextCellEditor",
                width=320,
            )
            gb.configure_default_column(editable=False)
            gridOptions = gb.build()

            grid_response = AgGrid(
                df_grid,
                gridOptions=gridOptions,
                update_mode=GridUpdateMode.MODEL_CHANGED,
                fit_columns_on_grid_load=True,
                height=420,
                theme="streamlit",
                key=f"aggrid_{unique_key}",
            )
            return pd.DataFrame(grid_response["data"])
        except Exception:
            use_aggrid_attempt = False

    # Fallback: native data_editor
    return st.data_editor(
        df_data,
        column_config={
            "Status": st.column_config.CheckboxColumn("Done?", width="small"),
            main_text_col: st.column_config.TextColumn(main_text_col, disabled=True, width="large"),
            "Bukti/Catatan": st.column_config.TextColumn("Bukti/Note", width="medium"),
        },
        hide_index=True,
        key=f"editor_native_{unique_key}",
        use_container_width=True,
    )


# =========================================================
# APP START
# =========================================================
if not KONEKSI_GSHEET_BERHASIL:
    st.error("Database Error: Google Sheet tidak tersambung.")
    st.stop()

# Hero header
with st.container():
    c1, c2 = st.columns([3.2, 1.2], vertical_alignment="center")
    with c1:
        st.markdown(
            f"""
<div class="hero">
  <div class="hero-title">🚀 {APP_TITLE}</div>
  <p class="hero-sub">{APP_TAGLINE}</p>
</div>
            """,
            unsafe_allow_html=True,
        )
    with c2:
        badges = []
        badges.append(badge_html("GSheet", KONEKSI_GSHEET_BERHASIL, "Database"))
        badges.append(badge_html("Dropbox", KONEKSI_DROPBOX_BERHASIL, "Upload bukti"))
        st.markdown(f'<div class="badges">{"".join(badges)}</div>', unsafe_allow_html=True)
        st.caption(f"Realtime: {fmt_realtime(now_jakarta())}")

if not KONEKSI_DROPBOX_BERHASIL:
    st.warning("Dropbox non-aktif. Fitur upload bukti (foto/dokumen) otomatis dimatikan.")


# =========================================================
# SIDEBAR (NAV + ADMIN LOGIN + TARGET MGMT)
# =========================================================
with st.sidebar:
    st.markdown("### Navigasi")
    if "is_admin" not in st.session_state:
        st.session_state["is_admin"] = False

    opsi_menu = ["📝 Operasional (Laporan & Target)"]
    if st.session_state["is_admin"]:
        opsi_menu.append("📊 Dashboard Admin")

    menu_nav = st.radio("Pilih Menu:", opsi_menu, label_visibility="collapsed")
    st.divider()

    # Admin gate
    if not st.session_state["is_admin"]:
        with st.expander("🔐 Akses Khusus Admin", expanded=False):
            st.caption("Masuk sebagai admin untuk membuka dashboard review & feedback.")
            pwd = st.text_input("Password", type="password", key="input_pwd")
            if st.button("Login Admin", use_container_width=True):
                if pwd == st.secrets.get("password_admin", "fajril123"):
                    st.session_state["is_admin"] = True
                    st.rerun()
                else:
                    st.error("Password salah!")
    else:
        st.success("Mode Admin aktif.")
        if st.button("🔓 Logout Admin", use_container_width=True):
            st.session_state["is_admin"] = False
            st.rerun()

    st.divider()
    st.markdown("### 🎯 Manajemen Target")
    st.caption("Tambah target team & pribadi secara cepat (bulk input).")

    tab_team, tab_individu, tab_admin = st.tabs(["Team", "Pribadi", "Admin"])

    with tab_team:
        with st.form("add_team_goal", clear_on_submit=True):
            st.markdown("**Bulk Input Target Team**")
            goal_team_text = st.text_area("Target Team (1 per baris)", height=110)
            c1, c2 = st.columns(2)
            today = now_jakarta().date()
            start_d = c1.date_input("Mulai", value=today, key="start_team")
            end_d = c2.date_input("Selesai", value=today + timedelta(days=30), key="end_team")
            if st.form_submit_button("➕ Tambah Target Team", use_container_width=True):
                targets = clean_bulk_input(goal_team_text)
                if targets:
                    ok = add_bulk_targets(SHEET_TARGET_TEAM, ["", str(start_d), str(end_d), "FALSE", "-"], targets)
                    if ok:
                        st.success(f"{len(targets)} target ditambahkan!")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("Gagal menambahkan target.")

    with tab_individu:
        NAMA_STAF = get_daftar_staf_terbaru()
        pilih_nama = st.selectbox("Pilih Nama", NAMA_STAF, key="sidebar_user")

        with st.form("add_indiv_goal", clear_on_submit=True):
            st.markdown("**Bulk Input Target Pribadi**")
            goal_indiv_text = st.text_area("Target Mingguan (1 per baris)", height=110)
            c1, c2 = st.columns(2)
            today = now_jakarta().date()
            start_i = c1.date_input("Mulai", value=today, key="start_indiv")
            end_i = c2.date_input("Selesai", value=today + timedelta(days=7), key="end_indiv")
            if st.form_submit_button("➕ Tambah Target Pribadi", use_container_width=True):
                targets = clean_bulk_input(goal_indiv_text)
                if targets:
                    ok = add_bulk_targets(
                        SHEET_TARGET_INDIVIDU,
                        [pilih_nama, "", str(start_i), str(end_i), "FALSE", "-"],
                        targets
                    )
                    if ok:
                        st.success(f"{len(targets)} target ditambahkan!")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("Gagal menambahkan target.")

    with tab_admin:
        with st.expander("➕ Tambah Karyawan", expanded=False):
            with st.form("add_staff", clear_on_submit=True):
                new_name = st.text_input("Nama")
                new_role = st.text_input("Jabatan")
                if st.form_submit_button("Tambah", use_container_width=True):
                    if new_name and new_role:
                        res, msg = tambah_staf_baru(f"{new_name} ({new_role})")
                        if res:
                            st.success(msg)
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error(msg)
                    else:
                        st.warning("Nama dan jabatan wajib diisi.")

    st.divider()
    st.caption("v2 UI Pro • Streamlit")


# =========================================================
# MENU 1: OPERASIONAL (LAPORAN & TARGET)
# =========================================================
if menu_nav == "📝 Operasional (Laporan & Target)":
    section_header("📌 Checklist Target (KPI Result)", "Pantau progress target team dan individu secara realtime.")

    df_team = load_checklist(SHEET_TARGET_TEAM, ["Misi", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan"])
    df_indiv_all = load_checklist(SHEET_TARGET_INDIVIDU, ["Nama", "Target", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan"])

    colL, colR = st.columns(2, gap="large")

    # -------------------------
    # TEAM
    # -------------------------
    with colL:
        with st.container(border=True):
            st.markdown("### 🏆 Target Team")
            if df_team.empty:
                st.info("Belum ada target team.")
            else:
                done = int((df_team["Status"] == True).sum())
                total = len(df_team)
                st.progress(done / total if total else 0, text=f"Progress: {done}/{total}")

                edited_team = render_hybrid_table(df_team, "team_table", "Misi")

                btn1, btn2 = st.columns([1, 1])
                with btn1:
                    if st.button("💾 Simpan Perubahan Team", use_container_width=True):
                        if save_checklist(SHEET_TARGET_TEAM, edited_team):
                            st.toast("Tersimpan!", icon="✅")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("Gagal menyimpan.")

                with btn2:
                    if st.button("🔄 Refresh", use_container_width=True):
                        st.cache_data.clear()
                        st.rerun()

                with st.expander("📂 Update Bukti (Team)", expanded=False):
                    pilih_misi = st.selectbox("Pilih Misi", df_team["Misi"].tolist())
                    note_misi = st.text_area("Catatan singkat (opsional)", height=90)
                    file_misi = st.file_uploader("Upload Bukti (foto/dokumen)", key="up_team", disabled=not KONEKSI_DROPBOX_BERHASIL)
                    if st.button("Update Bukti Team", use_container_width=True):
                        pelapor = get_daftar_staf_terbaru()[0] if get_daftar_staf_terbaru() else "Admin"
                        sukses, msg = update_evidence_row(SHEET_TARGET_TEAM, pilih_misi, note_misi, file_misi, pelapor, "Target_Team")
                        if sukses:
                            st.success("Updated!")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error(msg)

    # -------------------------
    # INDIVIDU
    # -------------------------
    with colR:
        with st.container(border=True):
            st.markdown("### ⚡ Target Individu")
            daftar = get_daftar_staf_terbaru()
            filter_nama = st.selectbox("Filter Nama", daftar, index=0)

            if df_indiv_all.empty:
                st.info("Belum ada target individu.")
            else:
                df_user = df_indiv_all[df_indiv_all["Nama"] == filter_nama]

                if df_user.empty:
                    st.info("Belum ada target untuk user ini.")
                else:
                    done = int((df_user["Status"] == True).sum())
                    total = len(df_user)
                    st.progress(done / total if total else 0, text=f"Progress: {done}/{total}")

                    edited_indiv = render_hybrid_table(df_user, f"indiv_{filter_nama}", "Target")

                    btn1, btn2 = st.columns([1, 1])
                    with btn1:
                        if st.button(f"💾 Simpan {filter_nama}", use_container_width=True):
                            df_all_upd = df_indiv_all.copy()
                            df_all_upd.update(edited_indiv)
                            if save_checklist(SHEET_TARGET_INDIVIDU, df_all_upd):
                                st.toast("Tersimpan!", icon="✅")
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error("Gagal menyimpan.")

                    with btn2:
                        if st.button("🔄 Refresh", use_container_width=True, key="refresh_indiv"):
                            st.cache_data.clear()
                            st.rerun()

                    with st.expander(f"📂 Update Bukti ({filter_nama})", expanded=False):
                        pilih_target = st.selectbox("Pilih Target", df_user["Target"].tolist())
                        note_target = st.text_area("Catatan singkat (opsional)", height=90, key="note_indiv")
                        file_target = st.file_uploader("Upload Bukti (foto/dokumen)", key="up_indiv", disabled=not KONEKSI_DROPBOX_BERHASIL)

                        if st.button("Update Bukti Pribadi", use_container_width=True):
                            sukses, msg = update_evidence_row(
                                SHEET_TARGET_INDIVIDU, pilih_target, note_target, file_target, filter_nama, "Target_Individu"
                            )
                            if sukses:
                                st.success("Updated!")
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error(msg)

    st.divider()

    # =========================================================
    # INPUT HARIAN
    # =========================================================
    with st.container(border=True):
        section_header("📝 Input Laporan Harian", "Tulis aktivitas, bukti, refleksi, dan next plan (reminder otomatis).")

        # Identity + Reminder + Feedback preview
        colA, colB = st.columns([1.25, 2.0], gap="large")
        with colA:
            nama_pelapor = st.selectbox("Nama Pelapor", get_daftar_staf_terbaru(), key="pelapor_main")

            pending_msg = get_reminder_pending(nama_pelapor)
            if pending_msg:
                st.warning(f"🔔 Reminder dari laporan terakhir: **{pending_msg}**")
            else:
                st.caption("Tidak ada pendingan dari laporan terakhir.")

        with colB:
            # Feedback terbaru dari lead
            try:
                df_user_only = load_all_reports([nama_pelapor])
                if not df_user_only.empty and COL_FEEDBACK in df_user_only.columns:
                    df_with_feed = df_user_only[df_user_only[COL_FEEDBACK].astype(str).str.strip() != ""]
                    if not df_with_feed.empty:
                        last_feed = df_with_feed.iloc[-1]
                        st.info(
                            f"💌 **Pesan terbaru Team Lead** (Laporan: `{last_feed[COL_TIMESTAMP]}`)\n\n> {last_feed[COL_FEEDBACK]}"
                        )
                    else:
                        st.caption("Belum ada feedback baru dari lead.")
            except Exception:
                st.caption("Belum ada feedback baru dari lead.")

        st.divider()

        # Activity type
        kategori_aktivitas = st.radio(
            "Jenis Aktivitas",
            [
                "🚗 Sales (Kunjungan Lapangan)",
                "💻 Digital Marketing / Konten / Ads",
                "📞 Telesales / Follow Up",
                "🏢 Internal / Lainnya",
            ],
            horizontal=True,
        )

        is_sales = "Sales" in kategori_aktivitas
        is_digital = "Digital" in kategori_aktivitas
        is_telesales = "Telesales" in kategori_aktivitas

        c1, c2 = st.columns(2, gap="large")

        with c1:
            today_now = now_jakarta().date()
            st.markdown(f"**Tanggal:** `{today_now.strftime('%d-%m-%Y')}`")

            sosmed_link = ""
            if is_digital:
                sosmed_link = st.text_input("Link Konten/Ads/Drive (opsional tapi disarankan)")

            if is_telesales:
                sosmed_link = st.text_input("Link CRM / Catatan Follow Up (opsional)", key="crm_link")

        with c2:
            if is_sales:
                lokasi_input = st.text_input("📍 Klien / Lokasi Kunjungan (wajib)", placeholder="Contoh: PT Maju Jaya / Ruko A12 / Rumah Bu Susi")
            else:
                lokasi_input = st.text_input(
                    "🗂️ Judul Tugas / Aktivitas (ringkas)",
                    value="Digital Marketing" if is_digital else ("Follow Up" if is_telesales else "Internal"),
                    placeholder="Contoh: Setup iklan, desain konten, rekap data, meeting internal, dsb."
                )

            fotos = st.file_uploader(
                "Upload Bukti (foto/screenshot/dokumen)",
                accept_multiple_files=True,
                disabled=not KONEKSI_DROPBOX_BERHASIL,
            )

        # Deskripsi
        deskripsi_map = {}
        main_deskripsi = ""

        if fotos:
            st.info("📎 Tambahkan deskripsi singkat untuk setiap file bukti (lebih jelas saat review).")
            for i, f in enumerate(fotos):
                with st.container(border=True):
                    col_img, col_desc = st.columns([1, 3], gap="large")
                    with col_img:
                        if hasattr(f, "type") and str(f.type).startswith("image"):
                            st.image(f, width=160)
                        else:
                            st.markdown(f"📄 **{f.name}**")
                    with col_desc:
                        deskripsi_map[f.name] = st.text_area(
                            f"Deskripsi untuk: {f.name}",
                            height=80,
                            key=f"desc_{i}",
                            placeholder="Apa yang ditunjukkan file ini? hasil apa? konteksnya apa?"
                        )
        else:
            placeholder_text = (
                "Jelaskan hasil kunjungan, siapa yang ditemui, progress, dan next step..."
                if is_sales else
                "Jelaskan output kerja hari ini (apa yang dibuat/dikerjakan, hasilnya bagaimana)..."
            )
            main_deskripsi = st.text_area("Deskripsi Aktivitas (wajib)", placeholder=placeholder_text, height=120)

        st.divider()

        # Reflection
        st.markdown("#### 🏁 Refleksi Harian")
        st.caption("Biar besok lebih terarah: tulis hasil, kendala, dan next plan (jadi reminder otomatis).")

        r1, r2 = st.columns(2, gap="large")
        with r1:
            input_kesimpulan = st.text_area(
                "💡 Hasil / Kesimpulan Hari Ini",
                height=110,
                placeholder="Contoh: Klien setuju, minta penawaran revisi. / 3 konten selesai, 1 ads aktif."
            )
        with r2:
            input_kendala = st.text_area(
                "🚧 Kendala / Hambatan",
                height=110,
                placeholder="Contoh: PIC susah dihubungi. / Render video lambat."
            )

        input_pending = st.text_input(
            "📌 Next Plan / Pending (akan jadi reminder besok)",
            placeholder="Contoh: Follow up Bu Susi jam 10. / Revisi desain banner A/B."
        )

        st.divider()

        # Submit
        cbtn1, cbtn2, cbtn3 = st.columns([1.4, 1.0, 1.0], gap="large")
        with cbtn1:
            submit = st.button("✅ Submit Laporan", type="primary", use_container_width=True)
        with cbtn2:
            refresh = st.button("🔄 Refresh", use_container_width=True)
        with cbtn3:
            st.markdown('<span class="pill">Tips: Upload bukti membuat review lebih cepat ✅</span>', unsafe_allow_html=True)

        if refresh:
            st.cache_data.clear()
            st.rerun()

        if submit:
            valid = True

            if is_sales and (not lokasi_input or str(lokasi_input).strip() == ""):
                st.error("Untuk **Sales (Kunjungan Lapangan)**, kolom **Klien/Lokasi** wajib diisi.")
                valid = False

            if (not fotos) and (not main_deskripsi or str(main_deskripsi).strip() == ""):
                st.error("Deskripsi wajib diisi (atau upload bukti dengan deskripsi per file).")
                valid = False

            if valid:
                with st.spinner("Menyimpan laporan..."):
                    rows = []
                    ts = now_jakarta().strftime("%d-%m-%Y %H:%M:%S")

                    final_lokasi = lokasi_input if lokasi_input else kategori_aktivitas

                    val_kesimpulan = input_kesimpulan if input_kesimpulan else "-"
                    val_kendala = input_kendala if input_kendala else "-"
                    val_pending = input_pending if input_pending else "-"
                    val_feedback = ""  # init kosong

                    if fotos and KONEKSI_DROPBOX_BERHASIL:
                        for f in fotos:
                            url = upload_ke_dropbox(f, nama_pelapor, "Laporan_Harian")
                            desc = deskripsi_map.get(f.name, "-")
                            rows.append([
                                ts, nama_pelapor, final_lokasi, desc,
                                url, sosmed_link if sosmed_link else "-",
                                val_kesimpulan, val_kendala, val_pending, val_feedback
                            ])
                    else:
                        rows.append([
                            ts, nama_pelapor, final_lokasi, main_deskripsi,
                            "-", sosmed_link if sosmed_link else "-",
                            val_kesimpulan, val_kendala, val_pending, val_feedback
                        ])

                    if simpan_laporan_harian_batch(rows, nama_pelapor):
                        st.success(f"Laporan tersimpan. Reminder besok: **{val_pending}**")
                        st.balloons()
                        st.cache_data.clear()
                    else:
                        st.error("Gagal menyimpan laporan.")

    # Raw logs
    with st.expander("📂 Log Data Mentah (All Reports)", expanded=False):
        if st.button("🔄 Refresh Data Mentah", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        df_log = load_all_reports(get_daftar_staf_terbaru())
        if not df_log.empty:
            st.dataframe(df_log, use_container_width=True, hide_index=True)
        else:
            st.info("Data masih kosong.")


# =========================================================
# MENU 2: DASHBOARD ADMIN
# =========================================================
elif menu_nav == "📊 Dashboard Admin":
    if not st.session_state.get("is_admin", False):
        st.warning("Kamu belum login admin.")
        st.stop()

    section_header("📊 Dashboard Produktivitas", "Review laporan, pantau kendala, dan kirim feedback langsung.")

    if st.button("🔄 Refresh Data Dashboard", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    df_log = load_all_reports(get_daftar_staf_terbaru())

    if df_log.empty:
        st.info("Belum ada data laporan.")
        st.stop()

    # parse time
    try:
        df_log[COL_TIMESTAMP] = pd.to_datetime(df_log[COL_TIMESTAMP], format="%d-%m-%Y %H:%M:%S", errors="coerce")
        df_log["Tanggal"] = df_log[COL_TIMESTAMP].dt.date
    except Exception:
        df_log["Tanggal"] = date.today()

    # categorize
    keywords_digital = ["Digital", "Marketing", "Konten", "Ads", "Telesales", "Internal", "Follow", "CRM", "Admin"]
    def get_category(val):
        val_str = str(val)
        if any(k.lower() in val_str.lower() for k in keywords_digital):
            return "Digital/Internal"
        return "Kunjungan Lapangan"

    df_log["Kategori"] = df_log[COL_TEMPAT].apply(get_category)

    # range filter
    days = st.selectbox("Rentang Waktu", [7, 14, 30], index=0)
    start_date = date.today() - timedelta(days=days)
    df_filt = df_log[df_log["Tanggal"] >= start_date]

    tab_sales, tab_marketing, tab_review, tab_galeri = st.tabs(
        ["🚗 Sales (Lapangan)", "💻 Marketing (Digital)", "📝 Review & Feedback", "🖼️ Galeri Bukti"]
    )

    # -------------------------
    # SALES TAB
    # -------------------------
    with tab_sales:
        with st.container(border=True):
            df_sales = df_filt[df_filt["Kategori"] == "Kunjungan Lapangan"]
            c1, c2, c3 = st.columns(3, gap="large")
            c1.metric("Total Laporan Lapangan", len(df_sales))
            c2.metric("Sales Aktif", int(df_sales[COL_NAMA].nunique()))
            c3.metric("Hari Terpantau", days)

            st.divider()

            if df_sales.empty:
                st.info("Tidak ada data kunjungan lapangan pada rentang waktu ini.")
            else:
                left, right = st.columns(2, gap="large")
                with left:
                    st.markdown("**Top Visiting (by jumlah laporan)**")
                    st.bar_chart(df_sales[COL_NAMA].value_counts())
                with right:
                    st.markdown("**Lokasi paling sering muncul**")
                    st.dataframe(df_sales[COL_TEMPAT].value_counts().head(10), use_container_width=True)

    # -------------------------
    # MARKETING TAB
    # -------------------------
    with tab_marketing:
        with st.container(border=True):
            df_mkt = df_filt[df_filt["Kategori"] == "Digital/Internal"]
            c1, c2, c3 = st.columns(3, gap="large")
            c1.metric("Total Laporan Digital/Internal", len(df_mkt))
            c2.metric("Marketer Aktif", int(df_mkt[COL_NAMA].nunique()))
            c3.metric("Hari Terpantau", days)

            st.divider()

            if df_mkt.empty:
                st.info("Tidak ada data aktivitas digital/internal pada rentang waktu ini.")
            else:
                left, right = st.columns(2, gap="large")
                with left:
                    st.markdown("**Distribusi beban kerja**")
                    if HAS_PLOTLY:
                        fig = px.pie(df_mkt, names=COL_NAMA, title=None)
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.bar_chart(df_mkt[COL_NAMA].value_counts())
                with right:
                    st.markdown("**Jenis tugas yang sering muncul**")
                    st.bar_chart(df_mkt[COL_TEMPAT].value_counts().head(12))

    # -------------------------
    # REVIEW TAB (CARD + FEEDBACK)
    # -------------------------
    with tab_review:
        st.caption("Urut terbaru. Klik card untuk membaca detail dan kirim feedback.")

        df_review = df_filt.sort_values(by=COL_TIMESTAMP, ascending=False)

        if df_review.empty:
            st.info("Belum ada data laporan pada rentang waktu ini.")
        else:
            for _, row in df_review.iterrows():
                with st.container(border=True):
                    # header
                    h1, h2 = st.columns([3, 1], vertical_alignment="center")
                    with h1:
                        nama = row.get(COL_NAMA, "-")
                        cat = row.get("Kategori", "-")
                        st.markdown(f"### 👤 {nama}")
                        st.caption(f"🏷️ {cat}")

                    with h2:
                        ts_val = row.get(COL_TIMESTAMP, None)
                        if isinstance(ts_val, pd.Timestamp) and pd.notna(ts_val):
                            st.markdown(f'<span class="pill">🕒 {ts_val.strftime("%d-%m-%Y %H:%M")}</span>', unsafe_allow_html=True)
                        else:
                            st.markdown('<span class="pill">🕒 -</span>', unsafe_allow_html=True)

                    # body
                    left, right = st.columns([3, 1], gap="large")
                    with left:
                        st.markdown(f"**📍 Aktivitas/Lokasi:** {row.get(COL_TEMPAT, '-')}")
                        st.markdown(f"**📝 Deskripsi:** {row.get(COL_DESKRIPSI, '-')}")
                        st.divider()

                        col_a, col_b, col_c = st.columns(3, gap="large")
                        with col_a:
                            st.info(f"💡 **Hasil/Kesimpulan**\n\n{row.get(COL_KESIMPULAN, '-')}")
                        with col_b:
                            st.warning(f"🚧 **Kendala**\n\n{row.get(COL_KENDALA, '-')}")
                        with col_c:
                            st.error(f"📌 **Next Plan**\n\n{row.get(COL_PENDING, '-')}")

                        st.divider()

                        existing_feed = row.get(COL_FEEDBACK, "")
                        if pd.isna(existing_feed):
                            existing_feed = ""

                        with st.expander(f"💬 Beri Feedback untuk {nama}", expanded=False):
                            unique_key = f"feed_{nama}_{str(row.get(COL_TIMESTAMP,'-'))}"
                            input_feed = st.text_area("Tulis arahan/masukan:", value=str(existing_feed), key=unique_key, height=110)

                            if st.button("Kirim Feedback 🚀", key=f"btn_{unique_key}", use_container_width=True):
                                if input_feed.strip():
                                    # timestamp string key
                                    if isinstance(ts_val, pd.Timestamp) and pd.notna(ts_val):
                                        ts_str = ts_val.strftime("%d-%m-%Y %H:%M:%S")
                                    else:
                                        ts_str = str(row.get(COL_TIMESTAMP, ""))

                                    res, msg = kirim_feedback_admin(nama, ts_str, input_feed)
                                    if res:
                                        st.toast(f"Feedback terkirim ke {nama}!", icon="✅")
                                        st.cache_data.clear()
                                    else:
                                        st.error(msg)
                                else:
                                    st.warning("Feedback tidak boleh kosong.")

                    with right:
                        link_foto = str(row.get(COL_LINK_FOTO, ""))
                        if "http" in link_foto.lower():
                            direct_url = link_foto.replace("www.dropbox.com", "dl.dropboxusercontent.com").replace("?dl=0", "")
                            try:
                                st.image(direct_url, use_container_width=True)
                                st.caption("Bukti")
                            except Exception:
                                st.caption("Gagal memuat bukti (cek link).")
                        else:
                            st.caption("Tidak ada bukti foto/dokumen.")

    # -------------------------
    # GALLERY TAB
    # -------------------------
    with tab_galeri:
        st.caption("Menampilkan bukti terbaru (max 12).")

        df_foto = (
            df_filt[df_filt[COL_LINK_FOTO].astype(str).str.contains("http", na=False, case=False)]
            .sort_values(by=COL_TIMESTAMP, ascending=False)
            .head(12)
        )

        if df_foto.empty:
            st.info("Belum ada bukti yang terupload pada rentang waktu ini.")
        else:
            data_dict = df_foto.to_dict("records")
            cols = st.columns(4, gap="large")
            for idx, row in enumerate(data_dict):
                with cols[idx % 4]:
                    with st.container(border=True):
                        url_asli = str(row.get(COL_LINK_FOTO, ""))
                        nama = row.get(COL_NAMA, "-")
                        tempat = row.get(COL_TEMPAT, "-")
                        direct_url = url_asli.replace("www.dropbox.com", "dl.dropboxusercontent.com").replace("?dl=0", "")

                        try:
                            st.image(direct_url, use_container_width=True)
                            st.markdown(f"**{nama}**")
                            st.caption(f"📍 {tempat}")
                        except Exception:
                            st.error("Gagal load gambar")
                            st.link_button("Buka Link", url_asli)
