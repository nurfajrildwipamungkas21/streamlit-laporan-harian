import streamlit as st
from datetime import datetime
from zoneinfo import ZoneInfo
import re
import io
from typing import Optional, Tuple, Dict, List
from collections import defaultdict
import difflib

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

COL_TIMESTAMP = "Timestamp"
COL_NAMA = "Nama"
COL_HP = "No HP/WA"
COL_POSISI = "Posisi"
COL_LINK_SELFIE = "Bukti Selfie"     # tampil lebih professional
COL_DBX_PATH = "Dropbox Path"        # internal/admin

SHEET_COLUMNS = [COL_TIMESTAMP, COL_NAMA, COL_HP, COL_POSISI, COL_LINK_SELFIE, COL_DBX_PATH]


# =========================
# HELPERS
# =========================
def get_mode() -> str:
    # kompatibel streamlit baru & lama
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
    """Supaya kolom link rapi di GSheet/Excel."""
    if not url or url == "-":
        return "-"
    safe = url.replace('"', '""')  # escape double quote untuk formula
    return f'=HYPERLINK("{safe}", "{label}")'


def auto_format_absensi_sheet(ws):
    """Format Google Sheet Absensi agar rapi & profesional."""
    try:
        sheet_id = ws.id
        all_values = ws.get_all_values()
        row_count = max(len(all_values), ws.row_count)

        # Lebar kolom A-F
        # A Timestamp, B Nama, C No HP/WA, D Posisi, E Bukti Selfie, F Dropbox Path
        col_widths = [170, 180, 150, 180, 140, 340]

        requests = []

        # 1) Set lebar kolom
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

        # 2) Header styling (row 1)
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

        # 3) Freeze header
        requests.append({
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount"
            }
        })

        # 4) Body default format
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": row_count},
                "cell": {"userEnteredFormat": {
                    "verticalAlignment": "MIDDLE",
                    "wrapStrategy": "CLIP"
                }},
                "fields": "userEnteredFormat(verticalAlignment,wrapStrategy)"
            }
        })

        # 5) Center: Timestamp (A) & No HP (C)
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

        # 6) Wrap untuk Dropbox Path (F)
        requests.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": row_count, "startColumnIndex": 5, "endColumnIndex": 6},
                "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP"}},
                "fields": "userEnteredFormat(wrapStrategy)"
            }
        })

        if requests:
            ws.spreadsheet.batch_update({"requests": requests})

    except Exception as e:
        # jangan bikin app crash kalau format gagal
        print(f"Format Absensi Error: {e}")


@st.cache_resource
def connect_gsheet():
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("GSheet secrets tidak ditemukan: gcp_service_account")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
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


def upload_selfie_to_dropbox(
    dbx,
    img_bytes: bytes,
    nama: str,
    ts_file: str,
    ext: str
) -> Tuple[str, str]:
    """
    Return (shared_link_raw, dropbox_path)
    """
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
    if "png" in mime:
        return ".png"
    return ".jpg"


def get_selfie_bytes(selfie_cam, selfie_upload) -> Tuple[Optional[bytes], str]:
    """
    Return (bytes, ext).
    """
    if selfie_cam is not None:
        mime = getattr(selfie_cam, "type", "") or ""
        return selfie_cam.getvalue(), detect_ext_and_mime(mime)

    if selfie_upload is not None:
        mime = getattr(selfie_upload, "type", "") or ""
        return selfie_upload.getvalue(), detect_ext_and_mime(mime)

    return None, ".jpg"


# =========================
# REKAP (PINTAR) - POSISI & HADIR
# =========================
def normalize_posisi(text: str) -> str:
    """
    Normalisasi posisi agar kategori konsisten walau input manual:
    - lowercase
    - hapus simbol aneh
    - samakan separator (/ , . - _) jadi spasi
    - rapikan spasi
    """
    t = str(text or "").strip().lower()
    t = t.replace("&", " dan ")
    t = re.sub(r"[/,_\-\.]+", " ", t)
    t = re.sub(r"[^a-z0-9\s]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


# Silakan tambah alias sesuai kebutuhan kantor kamu
POSISI_ALIASES: Dict[str, str] = {
    "spv": "supervisor",
    "sup": "supervisor",
    "super visor": "supervisor",
    "supervisior": "supervisor",
    "admin": "administrasi",
    "adm": "administrasi",
    "kry": "karyawan",
    "karyawan": "karyawan",
    "staf": "staff",
    "staff": "staff",
    "teknisi": "teknisi",
    "technician": "teknisi",
    "driver": "driver",
    "drv": "driver",
    "security": "security",
    "satpam": "security",
}


def smart_canonical_posisi(raw_pos: str, known_canon: List[str]) -> str:
    """
    Buat kategori posisi yang "pintar":
    1) normalisasi
    2) alias mapping (spv -> supervisor)
    3) fuzzy match ke kategori yang sudah ada (biar typo kecil nyatu)
    """
    p = normalize_posisi(raw_pos)
    if not p:
        return ""

    # alias langsung
    if p in POSISI_ALIASES:
        p = POSISI_ALIASES[p]

    # fuzzy match ke yang sudah ada
    # threshold dibuat cukup ketat biar tidak salah gabung
    if known_canon:
        best = difflib.get_close_matches(p, known_canon, n=1, cutoff=0.88)
        if best:
            return best[0]

    return p


def display_posisi(canon: str) -> str:
    if not canon:
        return "-"
    return " ".join(w.capitalize() for w in canon.split())


def parse_date_prefix(ts: str) -> str:
    """
    Kembalikan tanggal dd-mm-YYYY dari Timestamp sheet.
    Aman walau format agak beda; kalau gagal, fallback pakai 10 char pertama.
    """
    s = str(ts or "").strip()
    if not s:
        return ""
    try:
        dt = datetime.strptime(s, "%d-%m-%Y %H:%M:%S")
        return dt.strftime("%d-%m-%Y")
    except Exception:
        return s[:10]  # fallback


@st.cache_data(ttl=30)
def get_rekap_today() -> Dict:
    """
    Rekap hari ini (berdasarkan Timestamp):
    - Total hadir (dedup by no_hp, fallback nama)
    - Jumlah per posisi (posisi pintar)
    - Daftar siapa saja yang sudah datang (per posisi dan total)
    """
    sh = connect_gsheet()
    ws = get_or_create_ws(sh)

    # Ambil A:D supaya ringan (Timestamp, Nama, NoHP, Posisi)
    rows = ws.get("A:D")
    if not rows or len(rows) < 2:
        return {
            "today": now_local().strftime("%d-%m-%Y"),
            "total": 0,
            "dup_removed": 0,
            "by_pos": [],
            "all_people": [],
        }

    header, data = rows[0], rows[1:]
    today_str = now_local().strftime("%d-%m-%Y")

    # Dedup: kunci utama No HP (lebih unik), fallback Nama.
    seen_keys = set()
    dup_removed = 0

    # posisi_canon -> list of people strings
    people_by_pos = defaultdict(list)
    all_people = []

    known_canon = []  # untuk fuzzy matching konsisten dalam 1 rekap

    for r in data:
        # Pastikan panjang minimal 4
        ts = (r[0] if len(r) > 0 else "") or ""
        nama = (r[1] if len(r) > 1 else "") or ""
        hp = (r[2] if len(r) > 2 else "") or ""
        pos = (r[3] if len(r) > 3 else "") or ""

        if parse_date_prefix(ts) != today_str:
            continue

        nama_clean = sanitize_name(nama)
        hp_clean = sanitize_phone(hp)
        key = hp_clean if hp_clean else nama_clean.lower().strip()

        if not key:
            # kalau dua-duanya kosong, skip biar tidak bikin rekap kacau
            continue

        if key in seen_keys:
            dup_removed += 1
            continue

        seen_keys.add(key)

        pos_canon = smart_canonical_posisi(pos, known_canon)
        if pos_canon and pos_canon not in known_canon:
            known_canon.append(pos_canon)

        who = nama_clean if nama_clean else (hp_clean if hp_clean else "Tanpa Nama")
        # Biar “siapa yang hadir” jelas, tampilkan nama + (hp) kalau ada
        who_display = f"{who} ({hp_clean})" if hp_clean and who else who

        all_people.append({
            "Nama": who,
            "No HP/WA": hp_clean or "-",
            "Posisi": display_posisi(pos_canon) if pos_canon else "-",
            "Timestamp": ts,
        })

        people_by_pos[pos_canon if pos_canon else "(tanpa posisi)"].append(who_display)

    # Build summary per posisi
    by_pos = []
    for canon, people in people_by_pos.items():
        by_pos.append({
            "Posisi": display_posisi(canon) if canon != "(tanpa posisi)" else "Tanpa Posisi",
            "Jumlah": len(people),
            "Yang Hadir": ", ".join(people),
        })

    # Urutkan: jumlah desc, posisi asc
    by_pos.sort(key=lambda x: (-x["Jumlah"], x["Posisi"].lower()))

    return {
        "today": today_str,
        "total": len(seen_keys),
        "dup_removed": dup_removed,
        "by_pos": by_pos,
        "all_people": all_people,
    }


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
            "- Pastikan URL aplikasi **HTTPS** (Streamlit Cloud biasanya sudah).\n"
            "- Untuk HP jadul: jika kamera bermasalah, pakai opsi **Upload foto**.\n"
            "- Jika pakai token, QR mengandung `token=...` agar tidak sembarang orang submit."
        )
    st.stop()


# ===== PAGE: ABSEN (dibuka dari scan QR)
st.title("🧾 Form Absensi")

if ENABLE_TOKEN and TOKEN_SECRET:
    incoming_token = get_token_from_url()
    if incoming_token != TOKEN_SECRET:
        st.error("Akses tidak valid. Silakan scan QR resmi dari kantor.")
        st.stop()

dt = now_local()
ts_display = dt.strftime("%d-%m-%Y %H:%M:%S")
ts_file = dt.strftime("%Y-%m-%d_%H-%M-%S")
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
    posisi_final = str(posisi).strip()

    img_bytes, ext = get_selfie_bytes(selfie_cam, selfie_upload)

    errors = []
    if not nama_clean:
        errors.append("• Nama wajib diisi.")
    if not hp_clean or len(hp_clean.replace("+", "")) < 8:
        errors.append("• No HP/WA wajib diisi (minimal 8 digit).")
    if not posisi_final:
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
            dbx = connect_dropbox()

            link_selfie, dbx_path = upload_selfie_to_dropbox(dbx, img_bytes, nama_clean, ts_file, ext)

            # ✅ buat link rapi (tidak panjang)
            link_cell = make_hyperlink(link_selfie, "Bukti Foto")

            ws.append_row(
                [ts_display, nama_clean, hp_clean, posisi_final, link_cell, dbx_path],
                value_input_option="USER_ENTERED"
            )

            # ✅ format ulang agar kalau row bertambah tetap rapi (aman dipanggil)
            auto_format_absensi_sheet(ws)

        # setelah submit, rekap perlu refresh
        get_rekap_today.clear()

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
# UI: REKAP KEHADIRAN (BAGIAN BAWAH)
# =========================
st.divider()
st.subheader("📊 Rekap Kehadiran (Hari ini)")

try:
    rekap = get_rekap_today()

    top1, top2 = st.columns([1, 1])
    with top1:
        st.metric("Total hadir", rekap["total"])
    with top2:
        if st.button("🔄 Refresh rekap", use_container_width=True):
            get_rekap_today.clear()
            st.rerun()

    st.caption(f"Tanggal: **{rekap['today']}**")

    if rekap["dup_removed"] > 0:
        st.info(f"Catatan: terdeteksi **{rekap['dup_removed']}** entri duplikat (No HP/Nama sama) dan tidak dihitung agar rekap akurat.")

    if rekap["total"] == 0:
        st.warning("Belum ada absensi untuk hari ini.")
    else:
        st.write("**Klasifikasi jumlah hadir per posisi:**")
        st.dataframe(rekap["by_pos"], use_container_width=True, hide_index=True)

        with st.expander("👥 Lihat siapa saja yang sudah datang (detail)"):
            st.dataframe(rekap["all_people"], use_container_width=True, hide_index=True)

except Exception as e:
    st.warning("Rekap kehadiran belum bisa ditampilkan (cek koneksi GSheet).")
    with st.expander("Detail error (untuk admin)"):
        st.code(str(e))
