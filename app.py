import streamlit as st
from datetime import datetime
from zoneinfo import ZoneInfo
import re
import io
from typing import Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

import dropbox
from dropbox.sharing import RequestedVisibility, SharedLinkSettings
from dropbox.exceptions import ApiError, AuthError

import qrcode

# =========================
# CONFIG
# =========================
st.set_page_config(page_title="Absensi QR", page_icon="✅", layout="centered")

APP_CFG = st.secrets.get("app", {})
SHEET_NAME = APP_CFG.get("sheet_name", "Absensi_Karyawan")
WORKSHEET_NAME = APP_CFG.get("worksheet_name", "Log")
DROPBOX_ROOT = APP_CFG.get("dropbox_folder", "/Absensi_Selfie")
TZ_NAME = APP_CFG.get("timezone", "Asia/Jakarta")

QR_URL = APP_CFG.get("qr_url", "")
ENABLE_TOKEN = bool(APP_CFG.get("enable_token", False))
TOKEN_SECRET = str(APP_CFG.get("token", "")).strip()

# ✅ kalau True, sistem mengizinkan nomor yang sama absen berkali-kali dalam 1 hari
ALLOW_RESUBMIT_SAME_DAY = bool(APP_CFG.get("allow_resubmit_same_day", False))

COL_TIMESTAMP = "Timestamp"
COL_NAMA = "Nama"
COL_HP = "No HP/WA"
COL_POSISI = "Posisi"
COL_LINK_SELFIE = "Bukti Selfie"
COL_DBX_PATH = "Dropbox Path"

SHEET_COLUMNS = [COL_TIMESTAMP, COL_NAMA, COL_HP, COL_POSISI, COL_LINK_SELFIE, COL_DBX_PATH]


# =========================
# HELPERS
# =========================
def get_mode() -> str:
    try:
        return str(st.query_params.get("mode", "")).strip().lower()
    except Exception:
        qp = st.experimental_get_query_params()
        return (qp.get("mode", [""])[0] or "").strip().lower()


def get_token_from_url() -> str:
    try:
        return str(st.query_params.get("token", "")).strip()
    except Exception:
        qp = st.experimental_get_query_params()
        return (qp.get("token", [""])[0] or "").strip()


def sanitize_name(text: str) -> str:
    text = str(text).strip()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^A-Za-z0-9 _.-]", "", text)
    return text.strip()


def sanitize_phone(text: str) -> str:
    text = str(text).strip()
    if text.startswith("+"):
        return "+" + re.sub(r"\D", "", text[1:])
    return re.sub(r"\D", "", text)


def now_local():
    return datetime.now(tz=ZoneInfo(TZ_NAME))


def build_qr_png(url: str) -> bytes:
    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=10,
        border=2,
    )
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def make_hyperlink(url: str, label: str = "Bukti Foto") -> str:
    if not url or url == "-":
        return "-"
    safe = url.replace('"', '""')
    return f'=HYPERLINK("{safe}", "{label}")'


def auto_format_absensi_sheet(ws):
    try:
        sheet_id = ws.id
        all_values = ws.get_all_values()
        row_count = max(len(all_values), ws.row_count)

        col_widths = [170, 180, 150, 180, 140, 340]
        requests = []

        for i, w in enumerate(col_widths):
            requests.append({
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
                    "properties": {"pixelSize": w},
                    "fields": "pixelSize"
                }
            })

        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {
                    "textFormat": {"bold": True},
                    "horizontalAlignment": "CENTER",
                    "verticalAlignment": "MIDDLE",
                    "backgroundColor": {"red": 0.93, "green": 0.93, "blue": 0.93},
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

        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": row_count},
                "cell": {"userEnteredFormat": {"verticalAlignment": "MIDDLE", "wrapStrategy": "CLIP"}},
                "fields": "userEnteredFormat(verticalAlignment,wrapStrategy)"
            }
        })

        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": row_count, "startColumnIndex": 0, "endColumnIndex": 1},
                "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                "fields": "userEnteredFormat(horizontalAlignment)"
            }
        })
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": row_count, "startColumnIndex": 2, "endColumnIndex": 3},
                "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                "fields": "userEnteredFormat(horizontalAlignment)"
            }
        })

        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": row_count, "startColumnIndex": 5, "endColumnIndex": 6},
                "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}},
                "fields": "userEnteredFormat(wrapStrategy)"
            }
        })

        ws.spreadsheet.batch_update({"requests": requests})
    except Exception as e:
        print(f"Format Absensi Error: {e}")


@st.cache_resource
def connect_gsheet():
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("GSheet secrets tidak ditemukan: gcp_service_account")

    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds_dict = dict(st.secrets["gcp_service_account"])
    if "private_key" in creds_dict:
        creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open(SHEET_NAME)


def get_or_create_ws(spreadsheet):
    try:
        ws = spreadsheet.worksheet(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=5000, cols=len(SHEET_COLUMNS))
        ws.append_row(SHEET_COLUMNS, value_input_option="USER_ENTERED")
        auto_format_absensi_sheet(ws)
        return ws

    header = ws.row_values(1)
    if header != SHEET_COLUMNS:
        ws.resize(cols=max(ws.col_count, len(SHEET_COLUMNS)))
        ws.update("A1", [SHEET_COLUMNS], value_input_option="USER_ENTERED")
        auto_format_absensi_sheet(ws)

    return ws


@st.cache_resource
def connect_dropbox():
    if "dropbox" not in st.secrets or "access_token" not in st.secrets["dropbox"]:
        raise RuntimeError("Dropbox secrets tidak ditemukan: dropbox.access_token")

    dbx = dropbox.Dropbox(st.secrets["dropbox"]["access_token"])
    dbx.users_get_current_account()
    return dbx


def upload_selfie_to_dropbox(dbx, img_bytes: bytes, nama: str, ts_file: str, ext: str) -> Tuple[str, str]:
    clean_name = sanitize_name(nama).replace(" ", "_") or "Unknown"
    filename = f"{ts_file}_selfie{ext}"
    path = f"{DROPBOX_ROOT}/{clean_name}/{filename}"

    dbx.files_upload(img_bytes, path, mode=dropbox.files.WriteMode.add)

    settings = SharedLinkSettings(requested_visibility=RequestedVisibility.public)
    url = "-"
    try:
        link = dbx.sharing_create_shared_link_with_settings(path, settings=settings)
        url = link.url
    except ApiError as e:
        try:
            if e.error.is_shared_link_already_exists():
                links = dbx.sharing_list_shared_links(path, direct_only=True).links
                if links:
                    url = links[0].url
        except Exception:
            url = "-"

    url_raw = url.replace("?dl=0", "?raw=1") if url and url != "-" else "-"
    return url_raw, path


def detect_ext_and_mime(mime: str) -> str:
    mime = (mime or "").lower()
    return ".png" if "png" in mime else ".jpg"


def get_selfie_bytes(selfie_cam, selfie_upload) -> Tuple[Optional[bytes], str]:
    if selfie_cam is not None:
        mime = getattr(selfie_cam, "type", "") or ""
        return selfie_cam.getvalue(), detect_ext_and_mime(mime)

    if selfie_upload is not None:
        mime = getattr(selfie_upload, "type", "") or ""
        return selfie_upload.getvalue(), detect_ext_and_mime(mime)

    return None, ".jpg"


def already_absen_today(ws, hp_clean: str, today_ddmmyyyy: str) -> bool:
    """
    Cek apakah No HP/WA ini sudah absen di tanggal yang sama.
    Timestamp format: dd-mm-YYYY HH:MM:SS
    """
    if not hp_clean:
        return False

    try:
        # Ambil kolom A..C saja (lebih ringan daripada get_all_values)
        values = ws.get("A:C")  # termasuk header
        if not values or len(values) < 2:
            return False

        # skip header
        for row in values[1:]:
            ts = (row[0] if len(row) > 0 else "").strip()
            hp = (row[2] if len(row) > 2 else "").strip()
            if not ts or not hp:
                continue

            # cocokkan tanggal + hp
            # ts: "02-01-2026 23:40:12" => tanggal = ts[:10]
            if ts[:10] == today_ddmmyyyy and sanitize_phone(hp) == hp_clean:
                return True

        return False
    except Exception:
        # kalau gagal baca, jangan false positive; anggap belum
        return False


# =========================
# SESSION DEFAULTS
# =========================
if "open_cam" not in st.session_state:
    st.session_state.open_cam = False
if "saving" not in st.session_state:
    st.session_state.saving = False
if "submitted_once" not in st.session_state:
    st.session_state.submitted_once = False


# =========================
# UI
# =========================
mode = get_mode()

# ===== PAGE: QR / ADMIN
if mode != "absen":
    st.title("✅ QR Code Absensi")

    if not QR_URL:
        st.warning("QR URL belum diset. Isi `app.qr_url` di secrets.")
        st.code("Contoh: https://YOUR-APP.streamlit.app/?mode=absen", language="text")
        st.stop()

    if ENABLE_TOKEN and TOKEN_SECRET:
        if "token=" not in QR_URL:
            sep = "&" if "?" in QR_URL else "?"
            qr_url_effective = f"{QR_URL}{sep}token={TOKEN_SECRET}"
        else:
            qr_url_effective = QR_URL
    else:
        qr_url_effective = QR_URL

    st.caption("Cetak/Tempel QR ini. Karyawan scan → langsung ke form absen.")

    qr_png = build_qr_png(qr_url_effective)
    st.image(qr_png, caption="QR Absensi", use_container_width=True)

    c1, c2 = st.columns(2)
    with c1:
        st.link_button("🔗 Tes Link Absensi", qr_url_effective, use_container_width=True)
    with c2:
        st.download_button(
            "⬇️ Download QR",
            data=qr_png,
            file_name="qr_absensi.png",
            mime="image/png",
            use_container_width=True
        )

    with st.expander("Tips penggunaan (klik untuk buka)"):
        st.write(
            "- Pastikan URL aplikasi **HTTPS**.\n"
            "- Jika kamera bermasalah, pakai **Upload
