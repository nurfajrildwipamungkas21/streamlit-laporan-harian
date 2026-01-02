import streamlit as st
from datetime import datetime
from zoneinfo import ZoneInfo
import re
import io
import difflib
from collections import Counter
from typing import Optional, Tuple, List, Dict, Any
import csv

import gspread
from google.oauth2.service_account import Credentials

import dropbox
from dropbox.sharing import RequestedVisibility, SharedLinkSettings
from dropbox.exceptions import ApiError, AuthError

import qrcode

# Excel export (professional)
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.utils import get_column_letter

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

# Kolom utama
COL_TIMESTAMP = "Timestamp"
COL_NAMA = "Nama"
COL_HP = "No HP/WA"
COL_POSISI = "Posisi"
COL_LINK_SELFIE = "Bukti Selfie"
COL_DBX_PATH = "Dropbox Path"

# Kolom internal tambahan (untuk rekap & download yang akurat)
COL_POSISI_NORM = "Posisi (Normalized)"
COL_SELFIE_RAW = "Selfie URL Raw"

# Header sheet (kompatibel + upgrade otomatis)
SHEET_COLUMNS = [
    COL_TIMESTAMP, COL_NAMA, COL_HP, COL_POSISI, COL_LINK_SELFIE, COL_DBX_PATH,
    COL_POSISI_NORM, COL_SELFIE_RAW
]

# =========================
# HELPERS
# =========================
def get_mode() -> str:
    """Baca query param ?mode=absen/admin dgn fallback ke API lama."""
    try:
        return str(st.query_params.get("mode", "")).strip().lower()
    except Exception:
        qp = st.experimental_get_query_params()
        return (qp.get("mode", [""])[0] or "").strip().lower()

def get_token_from_url() -> str:
    """Ambil token dari query param ?token=..."""
    try:
        return str(st.query_params.get("token", "")).strip()
    except Exception:
        qp = st.experimental_get_query_params()
        return (qp.get("token", [""])[0] or "").strip()

def sanitize_name(text: str) -> str:
    """Bersihkan nama dari karakter aneh + spasi berlebih."""
    text = str(text).strip()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^A-Za-z0-9 _.-]", "", text)
    return text.strip()

def sanitize_phone(text: str) -> str:
    """Ambil hanya digit (dan + di depan bila ada)."""
    text = str(text).strip()
    if text.startswith("+"):
        return "+" + re.sub(r"\D", "", text[1:])
    return re.sub(r"\D", "", text)

def now_local() -> datetime:
    """Waktu lokal sesuai TZ_NAME."""
    return datetime.now(tz=ZoneInfo(TZ_NAME))

def build_qr_png(url: str) -> bytes:
    """Generate QR code PNG dari URL."""
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
    """Formula HYPERLINK untuk Google Sheet."""
    if not url or url == "-":
        return "-"
    safe = url.replace('"', '""')
    return f'=HYPERLINK("{safe}", "{label}")'

# ---- Normalisasi posisi (pintar tapi aman)
def _default_pos_aliases() -> Dict[str, str]:
    return {
        "spv": "supervisor",
        "sup": "supervisor",
        "super visor": "supervisor",
        "supervisor": "supervisor",
        "leader": "leader",
        "ketua": "leader",
        "admin": "admin",
        "operator": "operator",
        "ops": "operator",
        "teknisi": "teknisi",
        "technician": "teknisi",
        "driver": "driver",
        "supir": "driver",
        "satpam": "satpam",
        "security": "satpam",
        "karyawan": "karyawan",
        "pegawai": "karyawan",
        "staff": "karyawan",
        "staf": "karyawan",
        "warehouse": "gudang",
        "gudang": "gudang",
        "hse": "hse",
        "operasional": "operasional",
        "operational": "operasional",
    }

def normalize_posisi(text: str) -> str:
    t = str(text or "").strip().lower()
    if not t:
        return ""
    t = t.replace("&", " dan ")
    t = re.sub(r"[\/\-\_\.\|]+", " ", t)
    t = re.sub(r"[^a-z0-9\s]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def get_pos_aliases() -> Dict[str, str]:
    """Gabung alias default + alias custom (dari secrets)."""
    user_alias = APP_CFG.get("position_aliases", {}) or {}
    merged = _default_pos_aliases()
    for k, v in dict(user_alias).items():
        kk = normalize_posisi(str(k))
        vv = normalize_posisi(str(v))
        if kk and vv:
            merged[kk] = vv
    return merged

def canonicalize_posisi(raw_pos: str, known_canon: Optional[List[str]] = None) -> str:
    """Normalisasi + mapping ke posisi kanonik."""
    base = normalize_posisi(raw_pos)
    if not base:
        return ""

    aliases = get_pos_aliases()
    if base in aliases:
        return aliases[base]

    candidates: List[str] = []
    if known_canon:
        candidates.extend([c for c in known_canon if c])

    candidates.extend(list(set(aliases.values())))
    # Buang duplikat tapi jaga urutan
    candidates = list(dict.fromkeys(candidates))

    if candidates:
        # cutoff tinggi supaya tidak nyasar
        best = difflib.get_close_matches(base, candidates, n=1, cutoff=0.92)
        if best:
            return best[0]

    return base

def display_posisi(norm: str) -> str:
    """Posisi untuk display (kapital tiap kata)."""
    if not norm:
        return "-"
    return " ".join([w.capitalize() for w in norm.split(" ")])

def ts_to_datekey(ts: str) -> str:
    """Ambil bagian tanggal dari timestamp string (dd-mm-YYYY)."""
    s = str(ts or "").strip()
    if len(s) >= 10 and s[2:3] == "-" and s[5:6] == "-":
        return s[:10]
    try:
        dt = datetime.strptime(s, "%d-%m-%Y %H:%M:%S")
        return dt.strftime("%d-%m-%Y")
    except Exception:
        return ""

def parse_ts(ts: str) -> Optional[datetime]:
    """
    Lebih robust:
    - Mendukung "03-01-2026 00:53:20"
    - Mendukung "03-01-2026 0:53:20" (jam 1 digit)
    """
    s = str(ts or "").strip()
    if not s:
        return None

    # Normalisasi jam 1 digit -> 2 digit
    m = re.match(r"^(\d{2}-\d{2}-\d{4})\s+(\d{1,2}):(\d{2}):(\d{2})$", s)
    if m:
        date_part, hh, mm, ss = m.groups()
        hh2 = hh.zfill(2)
        s = f"{date_part} {hh2}:{mm}:{ss}"

    try:
        return datetime.strptime(s, "%d-%m-%Y %H:%M:%S")
    except Exception:
        return None

def auto_format_absensi_sheet(ws) -> None:
    """Format sheet Google agar rapi (sekali saja, dipanggil saat init/update header)."""
    try:
        sheet_id = ws.id
        all_values = ws.get_all_values()
        row_count = max(len(all_values), ws.row_count)

        # A-H
        col_widths = [170, 180, 150, 180, 140, 340, 190, 320]
        requests = []

        for i, w in enumerate(col_widths):
            requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": i,
                        "endIndex": i + 1
                    },
                    "properties": {"pixelSize": w},
                    "fields": "pixelSize"
                }
            })

        # Header style
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1
                },
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

        # Freeze header
        requests.append({
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 1}
                },
                "fields": "gridProperties.frozenRowCount"
            }
        })

        # Body default format
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": row_count
                },
                "cell": {"userEnteredFormat": {
                    "verticalAlignment": "MIDDLE",
                    "wrapStrategy": "CLIP"
                }},
                "fields": "userEnteredFormat(verticalAlignment,wrapStrategy)"
            }
        })

        # Center: Timestamp (A) & No HP (C)
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": row_count,
                    "startColumnIndex": 0,
                    "endColumnIndex": 1
                },
                "cell": {"userEnteredFormat": {
                    "horizontalAlignment": "CENTER"
                }},
                "fields": "userEnteredFormat(horizontalAlignment)"
            }
        })
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": row_count,
                    "startColumnIndex": 2,
                    "endColumnIndex": 3
                },
                "cell": {"userEnteredFormat": {
                    "horizontalAlignment": "CENTER"
                }},
                "fields": "userEnteredFormat(horizontalAlignment)"
            }
        })

        # Wrap Dropbox Path (F) & Selfie Raw (H)
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": row_count,
                    "startColumnIndex": 5,
                    "endColumnIndex": 6
                },
                "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}},
                "fields": "userEnteredFormat(wrapStrategy)"
            }
        })
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": row_count,
                    "startColumnIndex": 7,
                    "endColumnIndex": 8
                },
                "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}},
                "fields": "userEnteredFormat(wrapStrategy)"
            }
        })

        if requests:
            ws.spreadsheet.batch_update({"requests": requests})

    except Exception as e:
        print(f"Format Absensi Error: {e}")

@st.cache_resource
def connect_gsheet():
    """Koneksi ke Google Sheet utama."""
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("GSheet secrets tidak ditemukan: gcp_service_account")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_dict = dict(st.secrets["gcp_service_account"])
    if "private_key" in creds_dict:
        # private_key di secrets biasanya escape '\n'
        creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open(SHEET_NAME)

def get_or_create_ws(spreadsheet):
    """Ambil worksheet log; buat + set header bila belum ada."""
    try:
        ws = spreadsheet.worksheet(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(
            title=WORKSHEET_NAME,
            rows=5000,
            cols=len(SHEET_COLUMNS)
        )
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
    """Koneksi ke Dropbox untuk simpan selfie."""
    if "dropbox" not in st.secrets or "access_token" not in st.secrets["dropbox"]:
        raise RuntimeError("Dropbox secrets tidak ditemukan: dropbox.access_token")

    dbx = dropbox.Dropbox(st.secrets["dropbox"]["access_token"])
    dbx.users_get_current_account()
    return dbx

def upload_selfie_to_dropbox(
    dbx,
    img_bytes: bytes,
    nama: str,
    ts_file: str,
    ext: str
) -> Tuple[str, str]:
    """Upload selfie ke Dropbox, return (public_raw_url, path)."""
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
    """Tentukan ekstensi dari mime type camera/uploader."""
    mime = (mime or "").lower()
    if "png" in mime:
        return ".png"
    return ".jpg"

def get_selfie_bytes(selfie_cam, selfie_upload) -> Tuple[Optional[bytes], str]:
    """Ambil bytes selfie dari camera_input atau file_uploader."""
    if selfie_cam is not None:
        mime = getattr(selfie_cam, "type", "") or ""
        return selfie_cam.getvalue(), detect_ext_and_mime(mime)

    if selfie_upload is not None:
        mime = getattr(selfie_upload, "type", "") or ""
        return selfie_upload.getvalue(), detect_ext_and_mime(mime)

    return None, ".jpg"

def already_checked_in_today(ws, hp_clean: str, today_key: str) -> Tuple[bool, str]:
    """Cek apakah nomor HP sudah absen hari ini."""
    hp_clean = (hp_clean or "").strip()
    if not hp_clean:
        return False, ""

    ts_list = ws.col_values(1)[1:]
    hp_list = ws.col_values(3)[1:]

    for ts, hp in zip(reversed(ts_list), reversed(hp_list)):
        if ts_to_datekey(ts) != today_key:
            continue
        if sanitize_phone(hp) == hp_clean:
            return True, str(ts)
    return False, ""

@st.cache_data(ttl=30)
def get_today_data_and_rekap() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int, str]:
    """Ambil data hadir hari ini + rekap posisi dari GSheet (cache 30 detik)."""
    sh = connect_gsheet()
    ws = get_or_create_ws(sh)

    today_key = now_local().strftime("%d-%m-%Y")
    records = ws.get_all_records(default_blank="")

    # Bangun referensi posisi kanonik dari data hari ini
    known: List[str] = []
    for r in records:
        if ts_to_datekey(r.get(COL_TIMESTAMP, "")) != today_key:
            continue
        pnorm = normalize_posisi(r.get(COL_POSISI_NORM, "")) or normalize_posisi(r.get(COL_POSISI, ""))
        if pnorm:
            known.append(pnorm)
    known = list(dict.fromkeys(known))

    hadir_today: List[Dict[str, Any]] = []
    counter: Counter = Counter()

    for r in records:
        ts = r.get(COL_TIMESTAMP, "")
        if ts_to_datekey(ts) != today_key:
            continue

        nama = str(r.get(COL_NAMA, "")).strip()
        hp = str(r.get(COL_HP, "")).strip()
        posisi_raw = str(r.get(COL_POSISI, "")).strip()

        posisi_norm_saved = str(r.get(COL_POSISI_NORM, "")).strip()
        posisi_norm = normalize_posisi(posisi_norm_saved) if posisi_norm_saved else ""
        if not posisi_norm:
            posisi_norm = canonicalize_posisi(posisi_raw, known_canon=known)

        selfie_raw = str(r.get(COL_SELFIE_RAW, "")).strip()

        posisi_disp = display_posisi(posisi_norm)
        counter[posisi_disp] += 1

        hadir_today.append({
            "Timestamp": ts,
            "Nama": nama,
            "No HP/WA": hp,
            "Posisi": posisi_disp,
            "Selfie URL": selfie_raw if selfie_raw else "",
        })

    total = sum(counter.values())
    rekap_rows = [
        {"Posisi": k, "Jumlah": v}
        for k, v in sorted(counter.items(), key=lambda x: (-x[1], x[0]))
    ]
    return hadir_today, rekap_rows, total, today_key

def build_absensi_excel_bytes(hadir_today: List[Dict[str, Any]], today_key: str) -> bytes:
    """
    Export Excel (.xlsx) yang benar-benar rapi untuk HRD:
    - Header bold + background
    - Freeze header + auto filter
    - Timestamp jadi datetime (format dd-mm-yyyy hh:mm:ss)
    - No HP sebagai TEXT (anti scientific, leading zero aman)
    - Bukti foto berupa hyperlink dengan label (bukan URL panjang)
    - Lebar kolom nyaman dibaca
    """
    wb = Workbook()
    ws = wb.active
    ws.title = f"Absensi {today_key}"

    headers = ["Timestamp", "Nama", "No HP/WA", "Posisi", "Bukti Foto (Link)"]
    ws.append(headers)

    # Header style
    header_font = Font(bold=True)
    header_fill = PatternFill("solid", fgColor="EDEDED")
    header_align = Alignment(horizontal="center", vertical="center", wrap_text=True)

    for col_idx in range(1, len(headers) + 1):
        cell = ws.cell(row=1, column=col_idx)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_align

    # Freeze header
    ws.freeze_panes = "A2"

    # Column widths
    widths = {
        1: 20,  # Timestamp
        2: 24,  # Nama
        3: 18,  # No HP
        4: 18,  # Posisi
        5: 32,  # Bukti Foto
    }
    for c, w in widths.items():
        ws.column_dimensions[get_column_letter(c)].width = w

    # Alignment body
    align_center = Alignment(horizontal="center", vertical="center", wrap_text=False)
    align_left = Alignment(horizontal="left", vertical="center", wrap_text=False)

    row_num = 2
    for r in hadir_today:
        ts_str = str(r.get("Timestamp", "")).strip()
        nama = str(r.get("Nama", "")).strip()
        hp = str(r.get("No HP/WA", "")).strip()
        posisi = str(r.get("Posisi", "")).strip()
        selfie_url = str(r.get("Selfie URL", "")).strip()

        # Timestamp
        dt_obj = parse_ts(ts_str)
        c_ts = ws.cell(row=row_num, column=1, value=dt_obj if dt_obj else ts_str)
        if dt_obj:
            c_ts.number_format = "dd-mm-yyyy hh:mm:ss"
        c_ts.alignment = align_center

        # Nama
        c_nama = ws.cell(row=row_num, column=2, value=nama)
        c_nama.alignment = align_left

        # No HP sebagai TEXT
        c_hp = ws.cell(row=row_num, column=3, value=hp)
        c_hp.number_format = "@"
        c_hp.alignment = align_center

        # Posisi
        c_pos = ws.cell(row=row_num, column=4, value=posisi)
        c_pos.alignment = align_left

        # Link bukti foto (label)
        c_link = ws.cell(row=row_num, column=5, value="Bukti Foto" if selfie_url else "-")
        c_link.alignment = align_center
        if selfie_url:
            c_link.hyperlink = selfie_url
            c_link.font = Font(color="0563C1", underline="single")

        row_num += 1

    # Auto filter
    ws.auto_filter.ref = f"A1:E{max(1, row_num - 1)}"

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()

def build_absensi_csv_bytes(hadir_today: List[Dict[str, Any]], today_key: str) -> bytes:
    """
    Export CSV yang tetap ramah Excel:
    - Gunakan semicolon (;) sebagai delimiter agar Excel Indonesia mengenali kolom.
    - Nomor HP dipaksa sebagai text (=\"08...\") supaya tidak jadi scientific notation.
    - Encoding UTF-8 dengan BOM agar karakter non-ASCII aman.
    """
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=';')
    writer.writerow(["Timestamp", "Nama", "No HP/WA", "Posisi", "Selfie URL"])

    for r in hadir_today:
        ts_str = str(r.get("Timestamp", "")).strip()
        nama = str(r.get("Nama", "")).strip()
        hp = str(r.get("No HP/WA", "")).strip()
        posisi = str(r.get("Posisi", "")).strip()
        selfie_url = str(r.get("Selfie URL", "")).strip()

        # Trik agar Excel baca sebagai text, bukan angka / scientific
        hp_text = f'="{hp}"' if hp else ""
        writer.writerow([ts_str, nama, hp_text, posisi, selfie_url])

    # UTF-8 with BOM supaya Excel langsung kenali encoding
    return buf.getvalue().encode("utf-8-sig")

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
            "- Untuk HP jadul: jika kamera bermasalah, pakai opsi **Upload foto**.\n"
            "- Jika pakai token, QR mengandung `token=...`."
        )
    st.stop()

# ===== PAGE: ABSEN
st.title("🧾 Form Absensi")

if ENABLE_TOKEN and TOKEN_SECRET:
    incoming_token = get_token_from_url()
    if incoming_token != TOKEN_SECRET:
        st.error("Akses tidak valid. Silakan scan QR resmi dari kantor.")
        st.stop()

dt = now_local()
ts_display = dt.strftime("%d-%m-%Y %H:%M:%S")
ts_file = dt.strftime("%Y-%m-%d_%H-%M-%S")
today_key = dt.strftime("%d-%m-%Y")

st.caption(f"🕒 Waktu server ({TZ_NAME}): **{ts_display}**")
st.info("Jika muncul pop-up izin kamera, pilih **Allow / Izinkan**. Untuk HP tertentu, gunakan **Upload foto**.")

with st.form("form_absen", clear_on_submit=False):
    st.subheader("1) Data Karyawan")

    nama = st.text_input("Nama Lengkap", placeholder="Contoh: Andi Saputra")
    no_hp = st.text_input("No HP/WA", placeholder="Contoh: 08xxxxxxxxxx atau +628xxxxxxxxxx")
    posisi = st.text_input("Posisi / Jabatan", placeholder="Contoh: Driver / Teknisi / Supervisor")

    st.divider()
    st.subheader("2) Selfie Kehadiran")

    open_cam_now = st.checkbox("Buka kamera (disarankan jika HP mendukung)", value=st.session_state.open_cam)
    st.session_state.open_cam = open_cam_now

    selfie_cam = None
    if st.session_state.open_cam:
        selfie_cam = st.camera_input("Ambil selfie")

    st.caption("Jika kamera tidak bisa dibuka, gunakan opsi upload:")
    selfie_upload = st.file_uploader("Upload foto selfie", type=["jpg", "jpeg", "png"])

    st.divider()

    submit = st.form_submit_button(
        "✅ Submit Absensi",
        disabled=st.session_state.saving or st.session_state.submitted_once,
        use_container_width=True
    )

# ===== SUBMIT LOGIC
if submit:
    if st.session_state.submitted_once:
        st.warning("Absensi sudah tersimpan. Jika ingin absen lagi, refresh halaman.")
        st.stop()

    nama_clean = sanitize_name(nama)
    hp_clean = sanitize_phone(no_hp)
    posisi_raw = str(posisi).strip()

    img_bytes, ext = get_selfie_bytes(selfie_cam, selfie_upload)

    errors: List[str] = []
    if not nama_clean:
        errors.append("• Nama wajib diisi.")
    if not hp_clean or len(hp_clean.replace("+", "")) < 8:
        errors.append("• No HP/WA wajib diisi (minimal 8 digit).")
    if not posisi_raw:
        errors.append("• Posisi wajib diisi.")
    if img_bytes is None:
        errors.append("• Selfie wajib (kamera atau upload).")

    if errors:
        st.error("Mohon lengkapi dulu:\n\n" + "\n".join(errors))
        st.stop()

    st.session_state.saving = True
    try:
        with st.spinner("Menyimpan absensi..."):
            sh = connect_gsheet()
            ws = get_or_create_ws(sh)

            # anti double absen (biar rekap & export akurat)
            exists, last_ts = already_checked_in_today(ws, hp_clean, today_key)
            if exists:
                st.session_state.saving = False
                st.warning(
                    f"No HP/WA ini sudah absen hari ini (terakhir: {last_ts}). "
                    f"Jika itu salah, hubungi admin."
                )
                st.stop()

            # referensi canon posisi hari ini
            try:
                hadir_today_cache, _, _, _ = get_today_data_and_rekap()
                known_canon = [
                    normalize_posisi(x.get("Posisi", ""))
                    for x in hadir_today_cache
                    if x.get("Posisi")
                ]
                known_canon = [k for k in known_canon if k]
            except Exception:
                known_canon = []

            posisi_norm = canonicalize_posisi(posisi_raw, known_canon=known_canon)

            dbx = connect_dropbox()
            link_selfie_raw, dbx_path = upload_selfie_to_dropbox(
                dbx, img_bytes, nama_clean, ts_file, ext
            )

            link_cell = make_hyperlink(link_selfie_raw, "Bukti Foto")

            ws.append_row(
                [ts_display, nama_clean, hp_clean, posisi_raw, link_cell, dbx_path, posisi_norm, link_selfie_raw],
                value_input_option="USER_ENTERED"
            )

            auto_format_absensi_sheet(ws)
            get_today_data_and_rekap.clear()

        st.session_state.submitted_once = True
        st.success("Absensi berhasil tersimpan. Terima kasih ✅")
        st.balloons()

        if st.button("↩️ Isi ulang (reset form)", use_container_width=True):
            st.session_state.open_cam = False
            st.session_state.saving = False
            st.session_state.submitted_once = False
            st.rerun()

    except AuthError:
        st.error("Dropbox token tidak valid. Hubungi admin.")
    except Exception as e:
        st.error("Gagal menyimpan absensi.")
        with st.expander("Detail error (untuk admin)"):
            st.code(str(e))
    finally:
        st.session_state.saving = False

# =========================
# REKAP + DOWNLOAD (UI/UX bawah)
# =========================
st.divider()
st.subheader("📊 Rekap Kehadiran (Hari ini)")

c1, c2 = st.columns([1, 1])
with c1:
    st.caption("Rekap dihitung dari Google Sheet (aman untuk audit).")
with c2:
    if st.button("🔄 Refresh rekap", use_container_width=True):
        get_today_data_and_rekap.clear()
        st.rerun()

try:
    hadir_today, rekap_rows, total, today_key2 = get_today_data_and_rekap()

    mc1, mc2 = st.columns([1, 1])
    with mc1:
        st.metric("Total hadir", total)
    with mc2:
        st.metric("Tanggal", today_key2)

    if total == 0:
        st.info("Belum ada absensi untuk hari ini.")
    else:
        # Tampilkan rekap posisi tanpa tombol download rekap
        st.table(rekap_rows)

        with st.expander("👥 Daftar hadir hari ini (klik untuk buka)"):
            tampil_cols = ["Timestamp", "Nama", "No HP/WA", "Posisi"]
            st.dataframe(
                [{k: r.get(k, "") for k in tampil_cols} for r in hadir_today],
                use_container_width=True,
                hide_index=True
            )

        # Download laporan: hanya Excel (CSV dihapus)
        excel_bytes = build_absensi_excel_bytes(hadir_today, today_key2)

        st.download_button(
            "⬇️ Download Data Hadir (Excel .xlsx)",
            data=excel_bytes,
            file_name=f"absensi_harian_{today_key2}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

        # Catatan: CSV tidak disediakan di sini lagi sesuai permintaan

except Exception as e:
    st.warning("Rekap kehadiran belum bisa ditampilkan (cek koneksi GSheet).")
    with st.expander("Detail error (untuk admin)"):
        st.code(str(e))
