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
import io

# =========================================================
# OPTIONAL LIBS (Excel Export / AgGrid / Plotly)
# =========================================================
try:
    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment, PatternFill
    from openpyxl.utils.dataframe import dataframe_to_rows
    from openpyxl.utils import get_column_letter
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False

try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
    HAS_AGGRID = True
except ImportError:
    HAS_AGGRID = False

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
    layout="wide"
)

hide_st_style = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
</style>
"""
st.markdown(hide_st_style, unsafe_allow_html=True)


# =========================================================
# CONSTANTS
# =========================================================
NAMA_GOOGLE_SHEET = "Laporan Kegiatan Harian"
FOLDER_DROPBOX = "/Laporan_Kegiatan_Harian"

# Sheet Names
SHEET_CONFIG_NAMA = "Config_Staf"
SHEET_TARGET_TEAM = "Target_Team_Checklist"
SHEET_TARGET_INDIVIDU = "Target_Individu_Checklist"
SHEET_CONFIG_TEAM = "Config_Team"
SHEET_CLOSING_DEAL = "Closing_Deal"

# Kolom laporan harian
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
COL_INTEREST = "Interest (%)"
COL_NAMA_KLIEN = "Nama Klien"
COL_KONTAK_KLIEN = "No HP/WA"

NAMA_KOLOM_STANDAR = [
    COL_TIMESTAMP, COL_NAMA, COL_TEMPAT, COL_DESKRIPSI,
    COL_LINK_FOTO, COL_LINK_SOSMED,
    COL_KESIMPULAN, COL_KENDALA, COL_PENDING,
    COL_FEEDBACK,
    COL_INTEREST,
    COL_NAMA_KLIEN,
    COL_KONTAK_KLIEN
]

# Team config columns
TEAM_COL_NAMA_TEAM = "Nama Team"
TEAM_COL_POSISI = "Posisi"
TEAM_COL_ANGGOTA = "Nama Anggota"
TEAM_COLUMNS = [TEAM_COL_NAMA_TEAM, TEAM_COL_POSISI, TEAM_COL_ANGGOTA]

# Closing deal columns
COL_GROUP = "Nama Group"
COL_MARKETING = "Nama Marketing"
COL_TGL_EVENT = "Tanggal Event"
COL_BIDANG = "Bidang"
COL_NILAI_KONTRAK = "Nilai Kontrak"  # disimpan sebagai angka (int)

CLOSING_COLUMNS = [COL_GROUP, COL_MARKETING, COL_TGL_EVENT, COL_BIDANG, COL_NILAI_KONTRAK]

TZ_JKT = ZoneInfo("Asia/Jakarta")


# =========================================================
# CONNECTIONS
# =========================================================
KONEKSI_GSHEET_BERHASIL = False
KONEKSI_DROPBOX_BERHASIL = False
spreadsheet = None
dbx = None

# 1) Google Sheets
try:
    if "gcp_service_account" in st.secrets:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
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

# 2) Dropbox
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
# RUPIAH PARSER (input bebas -> int Rupiah)  ✅ FIXED
# =========================================================
def parse_rupiah_to_int(value):
    """
    Parser Rupiah yang lebih pintar.

    Contoh input yang didukung:
      - 15000000
      - 15.000.000
      - Rp 15.000.000
      - 15jt / 15,5jt / 15.5jt
      - 15 juta / 15,5 juta
      - 15.000.000,00

    Output: int rupiah, atau None kalau invalid.
    """
    if value is None:
        return None

    # jika sudah numeric
    if isinstance(value, (int, float)) and not pd.isna(value):
        try:
            return int(round(float(value)))
        except Exception:
            return None

    s = str(value).strip()
    if not s:
        return None

    s_lower = s.lower().strip()
    if s_lower in {"nan", "none", "-", "null"}:
        return None

    # hilangkan spasi + penanda mata uang
    s_lower = re.sub(r"\s+", "", s_lower)
    s_lower = s_lower.replace("idr", "").replace("rp", "")

    # deteksi satuan
    multiplier = 1
    if "miliar" in s_lower or "milyar" in s_lower:
        multiplier = 1_000_000_000
    elif "jt" in s_lower or "juta" in s_lower:
        multiplier = 1_000_000
    elif "rb" in s_lower or "ribu" in s_lower:
        multiplier = 1_000

    # buang kata satuan dari string angka
    s_num = re.sub(r"(miliar|milyar|juta|jt|ribu|rb)", "", s_lower)

    # sisakan digit + pemisah ribuan/desimal
    s_num = re.sub(r"[^0-9.,]", "", s_num)
    if not s_num:
        return None

    def to_float_locale(num_str: str) -> float:
        # kasus ada titik & koma sekaligus -> tentukan decimal dari pemisah terakhir
        if "." in num_str and "," in num_str:
            if num_str.rfind(",") > num_str.rfind("."):
                # dot ribuan, koma desimal (contoh 15.000.000,50)
                cleaned = num_str.replace(".", "").replace(",", ".")
            else:
                # koma ribuan, dot desimal (contoh 15,000,000.50)
                cleaned = num_str.replace(",", "")
            return float(cleaned)

        # hanya koma
        if "," in num_str:
            if num_str.count(",") > 1:
                # anggap koma ribuan
                return float(num_str.replace(",", ""))
            after = num_str.split(",")[1]
            # kalau 3 digit setelah koma -> kemungkinan pemisah ribuan
            if len(after) == 3:
                return float(num_str.replace(",", ""))
            # selain itu -> desimal
            return float(num_str.replace(",", "."))

        # hanya titik
        if "." in num_str:
            if num_str.count(".") > 1:
                # anggap titik ribuan
                return float(num_str.replace(".", ""))
            after = num_str.split(".")[1]
            if len(after) == 3:
                return float(num_str.replace(".", ""))
            return float(num_str)

        return float(num_str)

    try:
        base = to_float_locale(s_num)
    except Exception:
        digits = re.sub(r"\D", "", s_num)
        return int(digits) if digits else None

    if multiplier != 1:
        # heuristik: kalau user sudah ketik full rupiah besar, jangan dikali lagi
        if base >= multiplier:
            return int(round(base))
        return int(round(base * multiplier))

    return int(round(base))


def format_rupiah_display(amount) -> str:
    """Hanya untuk display di UI (bukan untuk disimpan)."""
    try:
        if amount is None or pd.isna(amount):
            return ""
        n = int(amount)
        return "Rp " + f"{n:,}".replace(",", ".")
    except Exception:
        return str(amount)


# =========================================================
# EXCEL EXPORT (rapi + bisa currency format)
# =========================================================
def df_to_excel_bytes(
    df: pd.DataFrame,
    sheet_name="Sheet1",
    col_widths=None,
    wrap_cols=None,
    right_align_cols=None,
    number_format_cols=None
):
    """
    Export dataframe ke .xlsx rapi:
    - Header bold + abu-abu, freeze header
    - Column width rapi
    - Wrap untuk kolom tertentu
    - Align right untuk kolom tertentu
    - number_format untuk kolom tertentu (mis. Rupiah)
    """
    if not HAS_OPENPYXL:
        return None

    df_export = df.copy()

    # pastikan missing jadi None (openpyxl friendly)
    df_export = df_export.where(pd.notna(df_export), None)

    output = io.BytesIO()
    wb = Workbook()
    ws = wb.active
    ws.title = (sheet_name or "Sheet1")[:31]

    for r in dataframe_to_rows(df_export, index=False, header=True):
        ws.append(r)

    header_fill = PatternFill("solid", fgColor="E6E6E6")
    header_font = Font(bold=True)

    for cell in ws[1]:
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    ws.freeze_panes = "A2"

    wrap_cols = set(wrap_cols or [])
    right_align_cols = set(right_align_cols or [])
    number_format_cols = dict(number_format_cols or {})
    col_widths = dict(col_widths or {})

    cols = list(df_export.columns)

    for i, col_name in enumerate(cols, 1):
        col_letter = get_column_letter(i)

        # width
        if col_name in col_widths:
            ws.column_dimensions[col_letter].width = col_widths[col_name]
        else:
            max_len = len(str(col_name))
            try:
                for v in df_export[col_name]:
                    v_str = "" if v is None else str(v)
                    max_len = max(max_len, len(v_str))
            except Exception:
                pass
            ws.column_dimensions[col_letter].width = min(max(10, max_len + 2), 60)

        # styles per-cell
        for cell in ws[col_letter][1:]:
            wrap = col_name in wrap_cols
            horiz = "right" if col_name in right_align_cols else "left"
            cell.alignment = Alignment(vertical="top", horizontal=horiz, wrap_text=wrap)

            if col_name in number_format_cols:
                cell.number_format = number_format_cols[col_name]

    wb.save(output)
    return output.getvalue()


# =========================================================
# GOOGLE SHEETS FORMATTING
# =========================================================
def _build_currency_number_format_rupiah():
    # Pola aman. Locale spreadsheet akan menentukan titik/koma.
    return {"type": "CURRENCY", "pattern": '"Rp" #,##0'}


def auto_format_sheet(worksheet):
    """
    Auto-format:
    - body: top align, clip default
    - header: bold, gray, center, freeze
    - smart column width + wrap kolom panjang
    - khusus COL_NILAI_KONTRAK: RIGHT + CURRENCY (Rupiah) => numeric tetap numeric, tampil Rp
    """
    try:
        sheet_id = worksheet.id
        all_values = worksheet.get_all_values()
        if not all_values:
            return

        headers = all_values[0]
        data_row_count = len(all_values)
        formatting_row_count = max(worksheet.row_count, data_row_count)

        requests = []
        default_body_format = {"verticalAlignment": "TOP", "wrapStrategy": "CLIP"}

        # 1) Reset body base style
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": formatting_row_count},
                "cell": {"userEnteredFormat": default_body_format},
                "fields": "userEnteredFormat(verticalAlignment,wrapStrategy)"
            }
        })

        # 2) Column sizing + per-column overrides
        for i, col_name in enumerate(headers):
            col_index = i
            cell_format_override = {}
            width = 100

            long_text_cols = {
                "Misi", "Target", "Deskripsi", "Bukti/Catatan", "Link Foto", "Link Sosmed",
                "Tempat Dikunjungi", "Kesimpulan", "Kendala", "Next Plan (Pending)", "Feedback Lead",
                COL_NAMA_KLIEN,
                TEAM_COL_NAMA_TEAM, TEAM_COL_POSISI, TEAM_COL_ANGGOTA,
                COL_GROUP, COL_MARKETING, COL_BIDANG
            }

            if col_name in long_text_cols:
                width = 300
                cell_format_override["wrapStrategy"] = "WRAP"
            elif col_name in {"Tgl_Mulai", "Tgl_Selesai", "Timestamp", COL_TGL_EVENT}:
                width = 150 if col_name == "Timestamp" else 120
                cell_format_override["horizontalAlignment"] = "CENTER"
            elif col_name in {"Status", "Done?"}:
                width = 60
                cell_format_override["horizontalAlignment"] = "CENTER"
            elif col_name == "Nama":
                width = 150
            elif col_name == COL_INTEREST:
                width = 140
                cell_format_override["horizontalAlignment"] = "CENTER"
            elif col_name == COL_KONTAK_KLIEN:
                width = 150
                cell_format_override["horizontalAlignment"] = "CENTER"
            elif col_name == COL_NILAI_KONTRAK:
                # ✅ Rupiah numeric format
                width = 180
                cell_format_override["horizontalAlignment"] = "RIGHT"
                cell_format_override["numberFormat"] = _build_currency_number_format_rupiah()

            # Set width
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

            # Apply per-column format
            if cell_format_override:
                fields = ",".join(cell_format_override.keys())
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
                        "fields": f"userEnteredFormat({fields})"
                    }
                })

        # 3) Header style
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

        # 4) Freeze header
        requests.append({
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount"
            }
        })

        worksheet.spreadsheet.batch_update({"requests": requests})
    except Exception as e:
        print(f"Format Error: {e}")


def ensure_headers(worksheet, desired_headers):
    """Pastikan header sesuai; jika belum, set ulang lalu auto format."""
    try:
        if worksheet.col_count < len(desired_headers):
            worksheet.resize(cols=len(desired_headers))

        headers = worksheet.row_values(1)
        need_reset = (not headers) or (len(headers) < len(desired_headers)) or any(h not in headers for h in desired_headers)
        if need_reset:
            worksheet.update(range_name="A1", values=[desired_headers], value_input_option="USER_ENTERED")
            auto_format_sheet(worksheet)
    except Exception as e:
        print(f"Ensure Header Error: {e}")


# =========================================================
# WORKSHEET GET/CREATE + STAFF LIST
# =========================================================
@st.cache_resource(ttl=60)
def get_or_create_worksheet(nama_worksheet):
    try:
        return spreadsheet.worksheet(nama_worksheet)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=nama_worksheet, rows=100, cols=len(NAMA_KOLOM_STANDAR))
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
        if nama_list and nama_list[0] == "Daftar Nama Staf":
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


# =========================================================
# TEAM CONFIG
# =========================================================
@st.cache_data(ttl=60)
def load_team_config():
    if not KONEKSI_GSHEET_BERHASIL:
        return pd.DataFrame(columns=TEAM_COLUMNS)

    try:
        try:
            ws = spreadsheet.worksheet(SHEET_CONFIG_TEAM)
        except Exception:
            ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_TEAM, rows=300, cols=len(TEAM_COLUMNS))
            ws.append_row(TEAM_COLUMNS, value_input_option="USER_ENTERED")
            auto_format_sheet(ws)
            return pd.DataFrame(columns=TEAM_COLUMNS)

        data = ws.get_all_records()
        df = pd.DataFrame(data).fillna("")
        for c in TEAM_COLUMNS:
            if c not in df.columns:
                df[c] = ""
        return df[TEAM_COLUMNS].copy()
    except Exception:
        return pd.DataFrame(columns=TEAM_COLUMNS)


def tambah_team_baru(nama_team, posisi, anggota_list):
    if not KONEKSI_GSHEET_BERHASIL:
        return False, "Koneksi GSheet belum aktif."

    try:
        nama_team = str(nama_team).strip()
        posisi = str(posisi).strip()
        anggota_list = [str(a).strip() for a in anggota_list if str(a).strip()]

        if not nama_team or not posisi or not anggota_list:
            return False, "Nama team, posisi, dan minimal 1 anggota wajib diisi."

        try:
            ws = spreadsheet.worksheet(SHEET_CONFIG_TEAM)
        except Exception:
            ws = spreadsheet.add_worksheet(title=SHEET_CONFIG_TEAM, rows=300, cols=len(TEAM_COLUMNS))
            ws.append_row(TEAM_COLUMNS, value_input_option="USER_ENTERED")
            auto_format_sheet(ws)

        existing = set()
        try:
            for r in ws.get_all_records():
                key = (
                    str(r.get(TEAM_COL_NAMA_TEAM, "")).strip(),
                    str(r.get(TEAM_COL_POSISI, "")).strip(),
                    str(r.get(TEAM_COL_ANGGOTA, "")).strip()
                )
                existing.add(key)
        except Exception:
            pass

        rows_to_add = []
        for anggota in anggota_list:
            key = (nama_team, posisi, anggota)
            if key not in existing:
                rows_to_add.append([nama_team, posisi, anggota])

        if not rows_to_add:
            return False, "Semua anggota sudah terdaftar di team tersebut."

        ws.append_rows(rows_to_add, value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return True, f"Berhasil tambah team '{nama_team}' ({len(rows_to_add)} anggota)."
    except Exception as e:
        return False, str(e)


# =========================================================
# DROPBOX UPLOAD
# =========================================================
def upload_ke_dropbox(file_obj, nama_staf, kategori="Umum"):
    if not KONEKSI_DROPBOX_BERHASIL or dbx is None:
        return "Koneksi Dropbox Error"

    try:
        file_data = file_obj.getvalue()
        ts = datetime.now(tz=TZ_JKT).strftime("%Y%m%d_%H%M%S")

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


# =========================================================
# TARGET / CHECKLIST HELPERS
# =========================================================
def clean_bulk_input(text_input):
    lines = (text_input or "").split("\n")
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
            ws = spreadsheet.add_worksheet(title=sheet_name, rows=100, cols=len(columns))
            ws.append_row(columns, value_input_option="USER_ENTERED")
            auto_format_sheet(ws)
            return pd.DataFrame(columns=columns)

        data = ws.get_all_records()
        df = pd.DataFrame(data).fillna("")

        for col in columns:
            if col not in df.columns:
                df[col] = False if col == "Status" else ""

        if "Status" in df.columns:
            df["Status"] = df["Status"].apply(lambda x: True if str(x).upper() == "TRUE" else False)

        return df[columns].copy()
    except Exception:
        return pd.DataFrame(columns=columns)


def save_checklist(sheet_name, df):
    try:
        ws = spreadsheet.worksheet(sheet_name)
        ws.clear()

        rows_needed = len(df) + 1
        if ws.row_count < rows_needed:
            ws.resize(rows=rows_needed)

        df_save = df.copy().fillna("")
        if "Status" in df_save.columns:
            df_save["Status"] = df_save["Status"].apply(lambda x: "TRUE" if bool(x) else "FALSE")

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
        df = pd.DataFrame(ws.get_all_records())

        col_target_key = "Misi" if sheet_name == SHEET_TARGET_TEAM else "Target"
        if col_target_key not in df.columns:
            return False, "Kolom kunci error."

        matches = df.index[df[col_target_key] == target_name].tolist()
        if not matches:
            return False, "Target tidak ditemukan."

        row_idx_pandas = matches[0]
        row_idx_gsheet = row_idx_pandas + 2  # + header

        link_bukti = ""
        if file_obj:
            link_bukti = upload_ke_dropbox(file_obj, user_folder_name, kategori=kategori_folder)

        catatan_lama = str(df.at[row_idx_pandas, "Bukti/Catatan"]) if "Bukti/Catatan" in df.columns else ""
        if catatan_lama == "-" or catatan_lama == "nan":
            catatan_lama = ""

        ts_update = datetime.now(tz=TZ_JKT).strftime("%d-%m %H:%M")
        update_text = f"[{ts_update}] "
        if note:
            update_text += f"{note}. "
        if link_bukti and link_bukti != "-":
            update_text += f"[FOTO: {link_bukti}]"

        final_note = f"{catatan_lama}\n{update_text}" if catatan_lama.strip() else update_text
        final_note = final_note.strip() if final_note.strip() else "-"

        headers = df.columns.tolist()
        if "Bukti/Catatan" not in headers:
            return False, "Kolom Bukti error."

        col_idx_gsheet = headers.index("Bukti/Catatan") + 1
        cell_address = gspread.utils.rowcol_to_a1(row_idx_gsheet, col_idx_gsheet)

        ws.update(range_name=cell_address, values=[[final_note]], value_input_option="USER_ENTERED")
        auto_format_sheet(ws)
        return True, "Berhasil update!"
    except Exception as e:
        return False, f"Error: {e}"


# =========================================================
# FEEDBACK + DAILY REPORT
# =========================================================
def kirim_feedback_admin(nama_staf, timestamp_key, isi_feedback):
    try:
        ws = spreadsheet.worksheet(nama_staf)

        if ws.col_count < len(NAMA_KOLOM_STANDAR):
            ws.resize(cols=len(NAMA_KOLOM_STANDAR))

        headers = ws.row_values(1)
        if COL_FEEDBACK not in headers:
            ws.update_cell(1, len(headers) + 1, COL_FEEDBACK)
            headers.append(COL_FEEDBACK)
            auto_format_sheet(ws)

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
            return False, "Data tidak ditemukan."

        col_idx = headers.index(COL_FEEDBACK) + 1
        ws.update_cell(found_row, col_idx, isi_feedback)
        return True, "Feedback terkirim!"
    except Exception as e:
        return False, f"Error: {e}"


def simpan_laporan_harian_batch(list_of_rows, nama_staf):
    try:
        ws = get_or_create_worksheet(nama_staf)
        if ws is None:
            return False

        current_header = ws.row_values(1)
        need_update = (len(current_header) < len(NAMA_KOLOM_STANDAR)) or any(c not in current_header for c in NAMA_KOLOM_STANDAR)

        if need_update:
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
        if pending_task and str(pending_task).strip() not in {"-", ""}:
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
    use_aggrid_attempt = HAS_AGGRID

    if use_aggrid_attempt:
        try:
            df_grid = df_data.copy().reset_index(drop=True)
            gb = GridOptionsBuilder.from_dataframe(df_grid)
            gb.configure_column("Status", editable=True, width=90)
            gb.configure_column(main_text_col, wrapText=True, autoHeight=True, width=400, editable=False)
            gb.configure_column("Bukti/Catatan", wrapText=True, autoHeight=True, editable=True, cellEditor="agLargeTextCellEditor", width=300)
            gb.configure_default_column(editable=False)
            gridOptions = gb.build()

            grid_response = AgGrid(
                df_grid,
                gridOptions=gridOptions,
                update_mode=GridUpdateMode.MODEL_CHANGED,
                fit_columns_on_grid_load=True,
                height=400,
                theme="streamlit",
                key=f"aggrid_{unique_key}"
            )
            return pd.DataFrame(grid_response["data"])
        except Exception:
            use_aggrid_attempt = False

    return st.data_editor(
        df_data,
        column_config={
            "Status": st.column_config.CheckboxColumn("Done?", width="small"),
            main_text_col: st.column_config.TextColumn(main_text_col, disabled=True, width="large"),
            "Bukti/Catatan": st.column_config.TextColumn("Bukti/Note", width="medium")
        },
        hide_index=True,
        key=f"editor_native_{unique_key}",
        use_container_width=True
    )


# =========================================================
# CLOSING DEAL (NUMERIC STORAGE + RUPIAH DISPLAY) ✅ FIXED
# =========================================================
@st.cache_data(ttl=60)
def load_closing_deal():
    """
    Load Closing Deal:
    - Nilai Kontrak dipaksa jadi numeric (int) untuk export SUM.
    - Kalau ada data lama yang masih "Rp 15.000.000", akan diparse ke int.
    """
    if not KONEKSI_GSHEET_BERHASIL:
        return pd.DataFrame(columns=CLOSING_COLUMNS)

    try:
        try:
            ws = spreadsheet.worksheet(SHEET_CLOSING_DEAL)
        except Exception:
            ws = spreadsheet.add_worksheet(title=SHEET_CLOSING_DEAL, rows=300, cols=len(CLOSING_COLUMNS))
            ws.append_row(CLOSING_COLUMNS, value_input_option="USER_ENTERED")
            auto_format_sheet(ws)
            return pd.DataFrame(columns=CLOSING_COLUMNS)

        ensure_headers(ws, CLOSING_COLUMNS)

        data = ws.get_all_records()
        df = pd.DataFrame(data)

        for c in CLOSING_COLUMNS:
            if c not in df.columns:
                df[c] = ""

        # ✅ pastikan Nilai Kontrak numeric (nullable int agar tidak jadi NaN float)
        if COL_NILAI_KONTRAK in df.columns:
            parsed = df[COL_NILAI_KONTRAK].apply(parse_rupiah_to_int)
            df[COL_NILAI_KONTRAK] = pd.Series(parsed, dtype="Int64")

        # rapikan kolom lain kosong jadi ""
        for c in [COL_GROUP, COL_MARKETING, COL_TGL_EVENT, COL_BIDANG]:
            if c in df.columns:
                df[c] = df[c].fillna("").astype(str)

        return df[CLOSING_COLUMNS].copy()
    except Exception:
        return pd.DataFrame(columns=CLOSING_COLUMNS)


def tambah_closing_deal(nama_group, nama_marketing, tanggal_event, bidang, nilai_kontrak_input):
    """
    ✅ PENTING:
    - Simpan Nilai Kontrak sebagai ANGKA (int) ke Google Sheet
    - Google Sheet akan menampilkan Rupiah via numberFormat (auto_format_sheet)
    """
    if not KONEKSI_GSHEET_BERHASIL:
        return False, "Koneksi GSheet belum aktif."

    try:
        nama_group = str(nama_group).strip() if nama_group is not None else ""
        nama_marketing = str(nama_marketing).strip() if nama_marketing is not None else ""
        bidang = str(bidang).strip() if bidang is not None else ""

        if not nama_group:
            nama_group = "-"

        # wajib
        if not nama_marketing or not tanggal_event or not bidang or not str(nilai_kontrak_input).strip():
            return False, "Field wajib: Nama Marketing, Tanggal Event, Bidang, dan Nilai Kontrak."

        nilai_int = parse_rupiah_to_int(nilai_kontrak_input)
        if nilai_int is None:
            return False, "Nilai Kontrak tidak valid. Contoh: 15000000 / 15.000.000 / Rp 15.000.000 / 15jt / 15,5jt"

        try:
            ws = spreadsheet.worksheet(SHEET_CLOSING_DEAL)
        except Exception:
            ws = spreadsheet.add_worksheet(title=SHEET_CLOSING_DEAL, rows=300, cols=len(CLOSING_COLUMNS))
            ws.append_row(CLOSING_COLUMNS, value_input_option="USER_ENTERED")

        ensure_headers(ws, CLOSING_COLUMNS)

        tgl_str = tanggal_event.strftime("%Y-%m-%d") if hasattr(tanggal_event, "strftime") else str(tanggal_event)

        # ✅ SIMPAN NUMERIC (bukan "Rp ...")
        ws.append_row([nama_group, nama_marketing, tgl_str, bidang, int(nilai_int)], value_input_option="USER_ENTERED")

        # format Rupiah + rapi
        auto_format_sheet(ws)

        return True, "Closing deal berhasil disimpan!"
    except Exception as e:
        return False, str(e)


# =========================================================
# APP UI
# =========================================================
if KONEKSI_GSHEET_BERHASIL:
    if not KONEKSI_DROPBOX_BERHASIL:
        st.warning("⚠️ Dropbox non-aktif. Fitur foto dimatikan.")

    # -----------------------------
    # SIDEBAR
    # -----------------------------
    with st.sidebar:
        st.header("Navigasi")

        if "is_admin" not in st.session_state:
            st.session_state["is_admin"] = False

        opsi_menu = ["📝 Laporan & Target"]
        if st.session_state["is_admin"]:
            opsi_menu.append("📊 Dashboard Admin")

        menu_nav = st.radio("Pilih Menu:", opsi_menu)
        st.divider()

        if not st.session_state["is_admin"]:
            with st.expander("🔐 Akses Khusus Admin"):
                pwd = st.text_input("Password:", type="password", key="input_pwd")
                if st.button("Login Admin"):
                    if pwd == st.secrets.get("password_admin", "fajril123"):
                        st.session_state["is_admin"] = True
                        st.rerun()
                    else:
                        st.error("Password salah!")
        else:
            if st.button("🔓 Logout Admin"):
                st.session_state["is_admin"] = False
                st.rerun()

        st.divider()
        st.header("🎯 Manajemen Target")

        tab_team, tab_individu, tab_admin = st.tabs(["Team", "Pribadi", "Admin"])

        with tab_team:
            st.caption("Bulk Input Target Team")
            with st.form("add_team_goal", clear_on_submit=True):
                goal_team_text = st.text_area("Target Team (Satu per baris)", height=100)
                c1, c2 = st.columns(2)
                today = datetime.now(tz=TZ_JKT).date()
                start_d = c1.date_input("Mulai", value=today, key="start_team")
                end_d = c2.date_input("Selesai", value=today + timedelta(days=30), key="end_team")
                if st.form_submit_button("➕ Tambah"):
                    targets = clean_bulk_input(goal_team_text)
                    if targets:
                        if add_bulk_targets(SHEET_TARGET_TEAM, ["", str(start_d), str(end_d), "FALSE", "-"], targets):
                            st.success(f"{len(targets)} target ditambahkan!")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("Gagal.")

        with tab_individu:
            st.caption("Bulk Input Target Pribadi")
            NAMA_STAF = get_daftar_staf_terbaru()
            pilih_nama = st.selectbox("Siapa Anda?", NAMA_STAF, key="sidebar_user")

            with st.form("add_indiv_goal", clear_on_submit=True):
                goal_indiv_text = st.text_area("Target Mingguan (Satu per baris)", height=100)
                c1, c2 = st.columns(2)
                today = datetime.now(tz=TZ_JKT).date()
                start_i = c1.date_input("Mulai", value=today, key="start_indiv")
                end_i = c2.date_input("Selesai", value=today + timedelta(days=7), key="end_indiv")
                if st.form_submit_button("➕ Tambah"):
                    targets = clean_bulk_input(goal_indiv_text)
                    if targets:
                        if add_bulk_targets(SHEET_TARGET_INDIVIDU, [pilih_nama, "", str(start_i), str(end_i), "FALSE", "-"], targets):
                            st.success(f"{len(targets)} target ditambahkan!")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("Gagal.")

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
                        else:
                            st.error("Nama dan jabatan wajib diisi.")

            with st.expander("👥 Tambah Team (Admin)"):
                with st.form("add_team_admin", clear_on_submit=True):
                    team_name = st.text_input("Nama Team", placeholder="Contoh: Team Sales A")
                    team_posisi = st.text_input("Posisi/Divisi", placeholder="Contoh: Sales Lapangan / Digital Marketing")
                    anggota_text = st.text_area("Nama Anggota (satu per baris)", height=120, placeholder="Contoh:\nAndi\nBudi\nSusi")
                    if st.form_submit_button("Tambah Team"):
                        anggota_list = clean_bulk_input(anggota_text)
                        res, msg = tambah_team_baru(team_name, team_posisi, anggota_list)
                        if res:
                            st.success(msg)
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error(msg)

            with st.expander("📋 Lihat Daftar Team"):
                df_team_cfg = load_team_config()
                if not df_team_cfg.empty:
                    st.dataframe(df_team_cfg, use_container_width=True, hide_index=True)
                else:
                    st.info("Belum ada team yang tersimpan.")

        # -----------------------------
        # CLOSING DEAL (Sidebar section)
        # -----------------------------
        st.divider()
        st.header("🤝 Closing Deal")

        with st.expander("➕ Input Closing Deal", expanded=False):
            with st.form("form_closing_deal", clear_on_submit=True):
                cd_group = st.text_input("Nama Group (Opsional)", placeholder="Kosongkan jika tidak ada")
                cd_marketing = st.text_input("Nama Marketing", placeholder="Contoh: Andi")
                cd_tgl = st.date_input("Tanggal Event", value=datetime.now(tz=TZ_JKT).date(), key="closing_event_date")
                cd_bidang = st.text_input("Bidang (Manual)", placeholder="Contoh: F&B / Properti / Pendidikan")

                cd_nilai = st.text_input(
                    "Nilai Kontrak (Input bebas)",
                    placeholder="Contoh: 15000000 / 15.000.000 / Rp 15.000.000 / 15jt / 15,5jt"
                )

                if st.form_submit_button("✅ Simpan Closing Deal"):
                    res, msg = tambah_closing_deal(cd_group, cd_marketing, cd_tgl, cd_bidang, cd_nilai)
                    if res:
                        st.success(msg)
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(msg)

        with st.expander("📋 Data Closing Deal", expanded=False):
            df_cd = load_closing_deal()

            if not df_cd.empty:
                # Display versi rupiah (UI), tapi data asli df_cd tetap numeric untuk export
                df_cd_display = df_cd.copy()
                df_cd_display[COL_NILAI_KONTRAK] = df_cd_display[COL_NILAI_KONTRAK].apply(
                    lambda x: "" if pd.isna(x) else format_rupiah_display(x)   # ✅ FIX: no 'nan'
                )
                st.dataframe(df_cd_display, use_container_width=True, hide_index=True)

                # ✅ Download Excel rapi + currency format
                if HAS_OPENPYXL:
                    col_widths = {
                        COL_GROUP: 25,
                        COL_MARKETING: 20,
                        COL_TGL_EVENT: 16,
                        COL_BIDANG: 25,
                        COL_NILAI_KONTRAK: 18
                    }

                    # pastikan numeric (int/None)
                    df_export = df_cd.copy()
                    df_export[COL_NILAI_KONTRAK] = df_export[COL_NILAI_KONTRAK].apply(
                        lambda x: None if pd.isna(x) else int(x)              # ✅ FIX: pd.isna
                    )

                    excel_bytes = df_to_excel_bytes(
                        df_export,
                        sheet_name="Closing_Deal",
                        col_widths=col_widths,
                        wrap_cols=[COL_GROUP, COL_BIDANG],
                        right_align_cols=[COL_NILAI_KONTRAK],
                        number_format_cols={COL_NILAI_KONTRAK: '"Rp" #,##0'}
                    )
                    st.download_button(
                        "⬇️ Download Excel Closing Deal (Rapi + Rupiah)",
                        data=excel_bytes,
                        file_name="closing_deal.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                else:
                    st.warning("openpyxl belum tersedia. Download Excel dinonaktifkan (fallback CSV).")

                # CSV fallback
                csv_cd = df_cd.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "⬇️ Download CSV Closing Deal",
                    data=csv_cd,
                    file_name="closing_deal.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            else:
                st.info("Belum ada data closing deal.")

    # =========================================================
    # MAIN PAGE
    # =========================================================
    st.title("🚀 Sales & Marketing Action Center")
    st.caption(f"Realtime: {datetime.now(tz=TZ_JKT).strftime('%d %B %Y %H:%M:%S')}")

    # -----------------------------
    # MENU: LAPORAN & TARGET
    # -----------------------------
    if menu_nav == "📝 Laporan & Target":
        st.subheader("📊 Checklist Target (Result KPI)")
        col_dash_1, col_dash_2 = st.columns(2)

        df_team = load_checklist(SHEET_TARGET_TEAM, ["Misi", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan"])
        df_indiv_all = load_checklist(SHEET_TARGET_INDIVIDU, ["Nama", "Target", "Tgl_Mulai", "Tgl_Selesai", "Status", "Bukti/Catatan"])

        with col_dash_1:
            st.markdown("#### 🏆 Target Team")
            if not df_team.empty:
                done = len(df_team[df_team["Status"] == True])
                st.progress(done / len(df_team) if len(df_team) > 0 else 0, text=f"Pencapaian: {done}/{len(df_team)}")
                edited_team = render_hybrid_table(df_team, "team_table", "Misi")

                if st.button("💾 Simpan Team", use_container_width=True):
                    if save_checklist(SHEET_TARGET_TEAM, edited_team):
                        st.toast("Tersimpan!", icon="✅")
                        st.cache_data.clear()
                        st.rerun()

                with st.expander("📂 Update Bukti (Team)"):
                    pilih_misi = st.selectbox("Misi:", df_team["Misi"].tolist())
                    note_misi = st.text_area("Catatan")
                    file_misi = st.file_uploader("Bukti", key="up_team", disabled=not KONEKSI_DROPBOX_BERHASIL)
                    if st.button("Update Team"):
                        pelapor = get_daftar_staf_terbaru()[0] if get_daftar_staf_terbaru() else "Admin"
                        sukses, msg = update_evidence_row(SHEET_TARGET_TEAM, pilih_misi, note_misi, file_misi, pelapor, "Target_Team")
                        if sukses:
                            st.success("Updated!")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error(msg)
            else:
                st.info("Belum ada target team.")

        with col_dash_2:
            st.markdown("#### ⚡ Target Individu")
            filter_nama = st.selectbox("Filter:", get_daftar_staf_terbaru(), index=0)

            if not df_indiv_all.empty:
                df_user = df_indiv_all[df_indiv_all["Nama"] == filter_nama]
                if not df_user.empty:
                    done = len(df_user[df_user["Status"] == True])
                    st.progress(done / len(df_user) if len(df_user) > 0 else 0, text=f"Progress: {done}/{len(df_user)}")
                    edited_indiv = render_hybrid_table(df_user, f"indiv_{filter_nama}", "Target")

                    if st.button(f"💾 Simpan {filter_nama}", use_container_width=True):
                        df_all_upd = df_indiv_all.copy()
                        df_all_upd.update(edited_indiv)
                        if save_checklist(SHEET_TARGET_INDIVIDU, df_all_upd):
                            st.toast("Tersimpan!", icon="✅")
                            st.cache_data.clear()
                            st.rerun()

                    with st.expander(f"📂 Update Bukti ({filter_nama})"):
                        pilih_target = st.selectbox("Target:", df_user["Target"].tolist())
                        note_target = st.text_area("Catatan", key="note_indiv")
                        file_target = st.file_uploader("Bukti", key="up_indiv", disabled=not KONEKSI_DROPBOX_BERHASIL)
                        if st.button("Update Pribadi"):
                            sukses, msg = update_evidence_row(SHEET_TARGET_INDIVIDU, pilih_target, note_target, file_target, filter_nama, "Target_Individu")
                            if sukses:
                                st.success("Updated!")
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error(msg)
                else:
                    st.info("Belum ada target.")
            else:
                st.info("Data kosong.")

        # -----------------------------
        # INPUT HARIAN
        # -----------------------------
        st.divider()
        with st.container(border=True):
            st.subheader("📝 Input Laporan Harian (Activity)")

            c_nama, c_reminder = st.columns([1, 2])
            with c_nama:
                nama_pelapor = st.selectbox("Nama Pelapor", get_daftar_staf_terbaru(), key="pelapor_main")

            # cek feedback terbaru
            try:
                df_user_only = load_all_reports([nama_pelapor])
                if not df_user_only.empty and COL_FEEDBACK in df_user_only.columns:
                    df_with_feed = df_user_only[df_user_only[COL_FEEDBACK].astype(str).str.strip() != ""]
                    if not df_with_feed.empty:
                        last_feed = df_with_feed.iloc[-1]
                        st.info(
                            f"💌 **Pesan Terbaru Team Lead (Laporan {last_feed[COL_TIMESTAMP]}):**\n\n"
                            f"\"{last_feed[COL_FEEDBACK]}\""
                        )
            except Exception:
                pass

            with c_reminder:
                pending_msg = get_reminder_pending(nama_pelapor)
                if pending_msg:
                    st.warning(f"🔔 **Reminder:** Kamu punya pendingan kemarin: '{pending_msg}'")
                else:
                    st.caption("Tidak ada pendingan dari laporan terakhir.")

            kategori_aktivitas = st.radio(
                "Jenis Aktivitas:",
                ["🚗 Sales (Kunjungan Lapangan)", "💻 Digital Marketing / Konten / Ads", "📞 Telesales / Follow Up", "🏢 Lainnya"],
                horizontal=True
            )

            is_kunjungan = kategori_aktivitas.startswith("🚗")

            c1, c2 = st.columns(2)
            with c1:
                today_now = datetime.now(tz=TZ_JKT).date()
                st.markdown(f"**Tanggal:** `{today_now.strftime('%d-%m-%Y')}`")

                sosmed_link = ""
                if "Digital Marketing" in kategori_aktivitas:
                    sosmed_link = st.text_input("Link Konten / Ads / Drive (Wajib jika ada)")

            with c2:
                if is_kunjungan:
                    lokasi_input = st.text_input("📍 Nama Klien / Lokasi Kunjungan (Wajib)")
                else:
                    lokasi_input = st.text_input("Jenis Tugas (Otomatis)", value=kategori_aktivitas.split(" ")[1], disabled=True)

                fotos = st.file_uploader(
                    "Upload Bukti (Foto/Screenshot/Dokumen)",
                    accept_multiple_files=True,
                    disabled=not KONEKSI_DROPBOX_BERHASIL
                )

            deskripsi_map = {}
            main_deskripsi = ""

            if fotos:
                st.info("📸 **Detail Bukti:** Berikan keterangan spesifik untuk setiap file:")
                for i, f in enumerate(fotos):
                    with st.container(border=True):
                        col_img, col_desc = st.columns([1, 3])
                        with col_img:
                            if f.type.startswith("image"):
                                st.image(f, width=150)
                            else:
                                st.markdown(f"📄 **{f.name}**")
                        with col_desc:
                            deskripsi_map[f.name] = st.text_area(
                                f"Ket. File: {f.name}",
                                height=70,
                                key=f"desc_{i}",
                                placeholder="Jelaskan aktivitas terkait file ini..."
                            )
            else:
                placeholder_text = "Jelaskan hasil kunjungan..." if is_kunjungan else "Jelaskan konten/ads/calls yang dikerjakan..."
                main_deskripsi = st.text_area("Deskripsi Aktivitas", placeholder=placeholder_text)

            st.divider()
            st.markdown("#### 🏁 Kesimpulan Harian")
            st.caption("Bagian ini penting agar progress besok lebih terarah.")

            col_ref_1, col_ref_2 = st.columns(2)
            with col_ref_1:
                input_kesimpulan = st.text_area(
                    "💡 Kesimpulan / Apa yang dicapai hari ini?",
                    height=100,
                    placeholder="Contoh: Klien setuju, tapi minta diskon. / Konten sudah jadi 3 feeds."
                )
            with col_ref_2:
                input_kendala = st.text_area(
                    "🚧 Kendala / Masalah?",
                    height=100,
                    placeholder="Contoh: Hujan deras jadi telat. / Laptop agak lemot render video."
                )

            input_interest = st.radio(
                "📈 Tingkat Interest (Presentase)",
                ["Under 50% (A)", "50-75% (B)", "75%-100%"],
                horizontal=True,
                key="interest_persen"
            )

            c_lead1, c_lead2 = st.columns(2)
            with c_lead1:
                input_nama_klien = st.text_input("👤 Nama Klien yang Dihubungi", placeholder="Contoh: Bu Susi / Pak Andi", key="nama_klien_input")
            with c_lead2:
                input_kontak_klien = st.text_input("📞 No HP/WA Klien", placeholder="Contoh: 08xxxxxxxxxx", key="kontak_klien_input")

            input_pending = st.text_input(
                "📌 Next Plan / Pending Item (Akan jadi Reminder Besok)",
                placeholder="Contoh: Follow up Bu Susi jam 10 pagi. / Revisi desain banner."
            )

            if st.button("✅ Submit Laporan", type="primary"):
                valid = True

                if is_kunjungan and not str(lokasi_input).strip():
                    st.error("Untuk Sales (Kunjungan), Lokasi Wajib Diisi!")
                    valid = False

                if (not fotos) and (not str(main_deskripsi).strip()):
                    st.error("Deskripsi Wajib Diisi!")
                    valid = False

                if valid:
                    with st.spinner("Menyimpan dan memformat database..."):
                        rows = []
                        ts = datetime.now(tz=TZ_JKT).strftime("%d-%m-%Y %H:%M:%S")
                        final_lokasi = lokasi_input if is_kunjungan else kategori_aktivitas

                        val_kesimpulan = input_kesimpulan.strip() if str(input_kesimpulan).strip() else "-"
                        val_kendala = input_kendala.strip() if str(input_kendala).strip() else "-"
                        val_pending = input_pending.strip() if str(input_pending).strip() else "-"
                        val_feedback = ""
                        val_interest = input_interest if input_interest else "-"
                        val_nama_klien = input_nama_klien.strip() if str(input_nama_klien).strip() else "-"
                        val_kontak_klien = input_kontak_klien.strip() if str(input_kontak_klien).strip() else "-"

                        if fotos and KONEKSI_DROPBOX_BERHASIL:
                            for f in fotos:
                                url = upload_ke_dropbox(f, nama_pelapor, "Laporan_Harian")
                                desc = deskripsi_map.get(f.name, "-")
                                rows.append([
                                    ts, nama_pelapor, final_lokasi, desc,
                                    url, sosmed_link if sosmed_link else "-",
                                    val_kesimpulan, val_kendala, val_pending,
                                    val_feedback, val_interest,
                                    val_nama_klien, val_kontak_klien
                                ])
                        else:
                            rows.append([
                                ts, nama_pelapor, final_lokasi, main_deskripsi,
                                "-", sosmed_link if sosmed_link else "-",
                                val_kesimpulan, val_kendala, val_pending,
                                val_feedback, val_interest,
                                val_nama_klien, val_kontak_klien
                            ])

                        if simpan_laporan_harian_batch(rows, nama_pelapor):
                            st.success(f"Laporan Tersimpan! Reminder besok: {val_pending}")
                            st.balloons()
                            st.cache_data.clear()
                        else:
                            st.error("Gagal simpan.")

        with st.expander("📂 Log Data Mentah"):
            if st.button("🔄 Refresh"):
                st.cache_data.clear()
                st.rerun()

            df_log = load_all_reports(get_daftar_staf_terbaru())
            if not df_log.empty:
                st.dataframe(df_log, use_container_width=True, hide_index=True)
            else:
                st.info("Kosong")

    # -----------------------------
    # MENU: DASHBOARD ADMIN
    # -----------------------------
    elif menu_nav == "📊 Dashboard Admin":
        st.header("📊 Dashboard Produktivitas")
        st.info("Dashboard ini memisahkan analisa antara Sales dan Marketing.")

        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()

        df_log = load_all_reports(get_daftar_staf_terbaru())

        if not df_log.empty:
            try:
                df_log[COL_TIMESTAMP] = pd.to_datetime(df_log[COL_TIMESTAMP], format="%d-%m-%Y %H:%M:%S", errors="coerce")
                df_log["Tanggal"] = df_log[COL_TIMESTAMP].dt.date
            except Exception:
                df_log["Tanggal"] = datetime.now(tz=TZ_JKT).date()

            keywords_digital = ["Digital", "Marketing", "Konten", "Ads", "Telesales", "Admin", "Follow"]

            def get_category(val):
                val_str = str(val)
                if any(k in val_str for k in keywords_digital):
                    return "Digital/Internal"
                return "Kunjungan Lapangan"

            df_log["Kategori"] = df_log[COL_TEMPAT].apply(get_category)

            days = st.selectbox("Rentang Waktu:", [7, 14, 30], index=0)
            start_date = date.today() - timedelta(days=days)
            df_filt = df_log[df_log["Tanggal"] >= start_date]

            tab_sales, tab_marketing, tab_review, tab_galeri = st.tabs(
                ["🚗 Sales (Lapangan)", "💻 Marketing (Digital)", "📝 Review & Feedback", "🖼️ Galeri Bukti"]
            )

            with tab_sales:
                df_sales = df_filt[df_filt["Kategori"] == "Kunjungan Lapangan"]
                col1, col2 = st.columns(2)
                col1.metric("Total Kunjungan", len(df_sales))
                col2.metric("Sales Aktif", df_sales[COL_NAMA].nunique())
                if not df_sales.empty:
                    st.subheader("Top Visiting Sales")
                    st.bar_chart(df_sales[COL_NAMA].value_counts())
                    st.subheader("Lokasi Paling Sering Dikunjungi")
                    st.dataframe(df_sales[COL_TEMPAT].value_counts().head(5), use_container_width=True)
                else:
                    st.info("Tidak ada data kunjungan lapangan.")

            with tab_marketing:
                df_mkt = df_filt[df_filt["Kategori"] == "Digital/Internal"]
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
                    st.bar_chart(df_mkt[COL_TEMPAT].value_counts())
                else:
                    st.info("Tidak ada data aktivitas digital.")

            with tab_review:
                st.subheader("📝 Review Catatan Harian & Feedback")
                st.caption("Monitoring kendala dan memberikan feedback langsung per individu.")

                with st.expander("📇 Tarik Data Nama & No HP per Tingkat Interest", expanded=True):
                    if COL_INTEREST not in df_filt.columns:
                        st.warning("Kolom Interest (%) belum ada di data.")
                    else:
                        if "filter_interest_admin" not in st.session_state:
                            st.session_state["filter_interest_admin"] = "Under 50% (A)"

                        b1, b2, b3 = st.columns(3)
                        if b1.button("Tarik Under 50% (A)", use_container_width=True):
                            st.session_state["filter_interest_admin"] = "Under 50% (A)"
                        if b2.button("Tarik 50-75% (B)", use_container_width=True):
                            st.session_state["filter_interest_admin"] = "50-75% (B)"
                        if b3.button("Tarik 75%-100%", use_container_width=True):
                            st.session_state["filter_interest_admin"] = "75%-100%"

                        selected_interest = st.session_state["filter_interest_admin"]
                        st.info(f"Filter aktif: **{selected_interest}**")

                        df_tmp = df_filt.copy()
                        for c in [COL_NAMA_KLIEN, COL_KONTAK_KLIEN, COL_INTEREST]:
                            if c not in df_tmp.columns:
                                df_tmp[c] = ""

                        df_tmp[COL_INTEREST] = df_tmp[COL_INTEREST].astype(str).fillna("").str.strip()
                        df_interest = df_tmp[df_tmp[COL_INTEREST] == selected_interest].copy()

                        cols_out = []
                        for c in [COL_TIMESTAMP, COL_NAMA, COL_NAMA_KLIEN, COL_KONTAK_KLIEN, COL_INTEREST, COL_TEMPAT, COL_DESKRIPSI]:
                            if c in df_interest.columns:
                                cols_out.append(c)

                        df_out = df_interest[cols_out].copy() if cols_out else df_interest.copy()
                        st.dataframe(df_out, use_container_width=True, hide_index=True)

                        # Download Excel rapi (optional)
                        if HAS_OPENPYXL:
                            df_export = df_out.copy()
                            if COL_TIMESTAMP in df_export.columns and pd.api.types.is_datetime64_any_dtype(df_export[COL_TIMESTAMP]):
                                df_export[COL_TIMESTAMP] = df_export[COL_TIMESTAMP].dt.strftime("%d-%m-%Y %H:%M:%S")

                            excel_bytes = df_to_excel_bytes(
                                df_export,
                                sheet_name="Data_Interest",
                                wrap_cols=[COL_DESKRIPSI, COL_TEMPAT],
                            )
                            safe_name = selected_interest.replace("%", "").replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")
                            st.download_button(
                                "⬇️ Download Excel (sesuai filter)",
                                data=excel_bytes,
                                file_name=f"data_klien_{safe_name}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True
                            )

                        # CSV fallback
                        df_export_csv = df_out.copy()
                        if COL_TIMESTAMP in df_export_csv.columns and pd.api.types.is_datetime64_any_dtype(df_export_csv[COL_TIMESTAMP]):
                            df_export_csv[COL_TIMESTAMP] = df_export_csv[COL_TIMESTAMP].dt.strftime("%d-%m-%Y %H:%M:%S")

                        csv_data = df_export_csv.to_csv(index=False).encode("utf-8")
                        safe_name = selected_interest.replace("%", "").replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")
                        st.download_button(
                            "⬇️ Download CSV (sesuai filter)",
                            data=csv_data,
                            file_name=f"data_klien_{safe_name}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )

                df_review = df_filt.sort_values(by=COL_TIMESTAMP, ascending=False)

                if not df_review.empty:
                    for _, row in df_review.iterrows():
                        with st.container(border=True):
                            c_head1, c_head2 = st.columns([3, 1])
                            with c_head1:
                                st.markdown(f"### 👤 {row.get(COL_NAMA, '-')}")
                                st.caption(f"📅 {row.get(COL_TIMESTAMP, '-')} | 🏷️ {row.get('Kategori', '-')}")

                            c_body, c_img = st.columns([3, 1])
                            with c_body:
                                st.markdown(f"**📍 Aktivitas/Lokasi:** {row.get(COL_TEMPAT, '-')}")
                                st.markdown(f"**📝 Deskripsi:** {row.get(COL_DESKRIPSI, '-')}")

                                nama_klien_val = row.get(COL_NAMA_KLIEN, "-") or "-"
                                kontak_klien_val = row.get(COL_KONTAK_KLIEN, "-") or "-"
                                interest_val = row.get(COL_INTEREST, "-") or "-"

                                st.markdown(f"**👤 Klien:** {nama_klien_val}  |  **📞 No HP/WA:** {kontak_klien_val}")
                                st.markdown(f"**📈 Interest:** {interest_val}")

                                st.divider()
                                col_a, col_b, col_c = st.columns(3)
                                with col_a:
                                    st.info(f"💡 **Hasil/Kesimpulan:**\n\n{row.get(COL_KESIMPULAN, '-')}")
                                with col_b:
                                    st.warning(f"🚧 **Kendala:**\n\n{row.get(COL_KENDALA, '-')}")
                                with col_c:
                                    st.error(f"📌 **Next Plan (Reminder):**\n\n{row.get(COL_PENDING, '-')}")

                                st.divider()
                                existing_feed = row.get(COL_FEEDBACK, "") or ""

                                with st.expander(f"💬 Beri Feedback untuk {row.get(COL_NAMA, '-')}", expanded=False):
                                    unique_key = f"feed_{row.get(COL_NAMA, '-')}_{row.get(COL_TIMESTAMP, '-')}"
                                    input_feed = st.text_area("Tulis Masukan/Arahan:", value=str(existing_feed), key=unique_key)

                                    if st.button("Kirim Feedback 🚀", key=f"btn_{unique_key}"):
                                        if input_feed:
                                            ts_val = row.get(COL_TIMESTAMP)
                                            if hasattr(ts_val, "strftime"):
                                                ts_str = ts_val.strftime("%d-%m-%Y %H:%M:%S")
                                            else:
                                                ts_str = str(ts_val)

                                            res, msg = kirim_feedback_admin(row.get(COL_NAMA, ""), ts_str, input_feed)
                                            if res:
                                                st.toast("Feedback terkirim!", icon="✅")
                                            else:
                                                st.error(msg)

                            with c_img:
                                link_foto = str(row.get(COL_LINK_FOTO, ""))
                                if "http" in link_foto:
                                    url_asli = link_foto
                                    direct_url = url_asli.replace("www.dropbox.com", "dl.dropboxusercontent.com").replace("?dl=0", "")
                                    try:
                                        st.image(direct_url, use_container_width=True)
                                        st.caption("Bukti Foto")
                                    except Exception:
                                        st.caption("Gagal load foto")
                else:
                    st.info("Belum ada data laporan pada rentang waktu ini.")

            with tab_galeri:
                st.caption("Menampilkan bukti foto/dokumen terbaru")
                if COL_LINK_FOTO in df_filt.columns:
                    df_foto = df_filt[
                        df_filt[COL_LINK_FOTO].astype(str).str.contains("http", na=False, case=False)
                    ].sort_values(by=COL_TIMESTAMP, ascending=False).head(12)
                else:
                    df_foto = pd.DataFrame()

                if not df_foto.empty:
                    data_dict = df_foto.to_dict("records")
                    cols = st.columns(4)
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
                else:
                    st.info("Belum ada bukti yang terupload.")

else:
    st.error("Database Error.")
