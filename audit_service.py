# audit_service.py
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import gspread
import json

# Nama Sheet Khusus Audit
SHEET_AUDIT_NAME = "Global_Audit_Log"
TZ_JKT = ZoneInfo("Asia/Jakarta")

# ====================================================================
# KONFIGURASI NAMA KOLOM (HEADER) - BAHASA INDONESIA UNIVERSAL
# ====================================================================
# Ini adalah judul kolom yang akan muncul di Spreadsheet.
# Kita buat universal agar mudah dipahami admin.
AUDIT_COLS = [
    "Waktu & Tanggal",      # Dulu: Timestamp
    "Pelaku (User)",        # Dulu: Actor
    "Jabatan / Role",       # Dulu: Role
    "Fitur yg Digunakan",   # Dulu: Feature
    "Nama Data / Sheet",    # Dulu: Target_Sheet
    "Baris Ke-",            # Dulu: Row_Index
    "Aksi Dilakukan",       # Dulu: Action
    "Alasan Perubahan",     # Dulu: Reason
    "Rincian (Sebelum ➡ Sesudah)" # Dulu: Changes_JSON/Detail
]

def ensure_audit_sheet(spreadsheet):
    """
    Memastikan tab audit tersedia dengan Formatting Rapi.
    Menggunakan fungsi native gspread agar lebih stabil dan anti-error.
    """
    try:
        # 1. GET / CREATE SHEET
        try:
            ws = spreadsheet.worksheet(SHEET_AUDIT_NAME)
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=SHEET_AUDIT_NAME, rows=2000, cols=len(AUDIT_COLS))
            ws.append_row(AUDIT_COLS, value_input_option="USER_ENTERED")

        # 2. PASTIKAN HEADER UPDATE
        current_header = ws.row_values(1)
        if not current_header or current_header[0] != AUDIT_COLS[0]:
            # Update cell A1:I1 sekaligus
            range_header = f"A1:{chr(65 + len(AUDIT_COLS) - 1)}1" # A1:I1
            ws.update(range_name=range_header, values=[AUDIT_COLS], value_input_option="USER_ENTERED")

        # ==========================================
        # 3. FORMATTING (SAFE MODE)
        # ==========================================
        # Kita pisah-pisah agar jika satu gagal, yang lain tetap jalan.
        
        # A. Freeze Header (Baris 1)
        try:
            ws.freeze(rows=1)
        except Exception as e:
            print(f"[Warn] Freeze gagal: {e}")

        # B. Atur Lebar Kolom (Satu per satu agar aman)
        # Index kolom di gspread dimulai dari 1 (A=1, B=2, dst)
        widths = {
            1: 160, # A: Waktu
            2: 120, # B: Pelaku
            3: 130, # C: Jabatan
            4: 130, # D: Fitur
            5: 150, # E: Nama Data
            6: 80,  # F: Baris Ke
            7: 100, # G: Aksi
            8: 250, # H: Alasan
            9: 500  # I: Rincian (Paling Lebar)
        }
        try:
            # Batch update column width (Lebih efisien daripada loop satu-satu)
            body_width = {"requests": []}
            for col_idx, px in widths.items():
                body_width["requests"].append({
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": ws.id,
                            "dimension": "COLUMNS",
                            "startIndex": col_idx - 1,
                            "endIndex": col_idx
                        },
                        "properties": {"pixelSize": px},
                        "fields": "pixelSize"
                    }
                })
            ws.batch_update(body_width)
        except Exception as e:
            print(f"[Warn] Width formatting gagal: {e}")

        # C. Styling Header (Bold, Center, Warna Biru)
        try:
            ws.format("A1:I1", {
                "backgroundColor": {"red": 0.85, "green": 0.92, "blue": 0.97}, # Biru Muda
                "textFormat": {"bold": True, "fontSize": 10},
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE"
            })
        except Exception as e:
            print(f"[Warn] Header style gagal: {e}")

        # D. Styling Body (Wrap Text, Align Top)
        # A2:I artinya dari baris 2 sampai bawah
        try:
            ws.format("A2:I", {
                "wrapStrategy": "WRAP",
                "verticalAlignment": "TOP"
            })
        except Exception as e:
            print(f"[Warn] Body style gagal: {e}")

        return ws

    except Exception as e:
        print(f"CRITICAL ERROR di ensure_audit_sheet: {e}")
        # Kembalikan ws apa adanya agar aplikasi tidak crash total
        try:
            return spreadsheet.worksheet(SHEET_AUDIT_NAME)
        except:
            raise e

def format_changes_human_readable(changes_dict):
    """
    Format teks Before -> After yang rapi.
    """
    if not changes_dict:
        return "-"
    
    lines = []
    for col, vals in changes_dict.items():
        old_v = str(vals.get('old', '-')).strip()
        new_v = str(vals.get('new', '-')).strip()
        
        if not old_v: old_v = "(kosong)"
        if not new_v: new_v = "(kosong)"

        # Format: [Nama Kolom]: Lama ➡ Baru
        line = f"• {col}: {old_v} ➡ {new_v}"
        lines.append(line)
    
    return "\n".join(lines)

def log_admin_action(spreadsheet, actor, role, feature, target_sheet, row_idx, action, reason, changes_dict):
    """
    Mencatat log.
    """
    try:
        ws = ensure_audit_sheet(spreadsheet)
        
        ts = datetime.now(TZ_JKT).strftime("%d-%m-%Y %H:%M:%S") # Format Indonesia (Tgl-Bln-Thn)
        
        readable_changes = format_changes_human_readable(changes_dict)
        
        # Petakan data ke kolom Bahasa Indonesia
        row_data = [
            ts,                 # Waktu & Tanggal
            str(actor),         # Pelaku
            str(role),          # Jabatan
            str(feature),       # Fitur
            str(target_sheet),  # Nama Data
            str(row_idx),       # Baris Ke
            str(action),        # Aksi
            str(reason) if reason else "-", # Alasan
            readable_changes    # Rincian
        ]
        
        ws.append_row(row_data, value_input_option="USER_ENTERED")
        return True
    except Exception as e:
        print(f"Audit Error: {e}")
        return False

def compare_and_get_changes(df_old, df_new, key_col_index=None):
    """
    Logika perbandingan data (Tetap sama).
    """
    changes = []
    if len(df_old) != len(df_new):
        return [] 

    cols = df_old.columns
    for i in range(len(df_old)):
        row_diff = {}
        has_change = False
        for col in cols:
            val_old = df_old.iloc[i][col]
            val_new = df_new.iloc[i][col]
            
            s_old = str(val_old).strip() if pd.notna(val_old) else ""
            s_new = str(val_new).strip() if pd.notna(val_new) else ""
            
            if s_old != s_new:
                row_diff[col] = {"old": s_old, "new": s_new}
                has_change = True
        
        if has_change:
            changes.append({
                "row_idx": i,
                "diff": row_diff
            })
            
    return changes
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import gspread
import json

# Nama Sheet Khusus Audit
SHEET_AUDIT_NAME = "Global_Audit_Log"
TZ_JKT = ZoneInfo("Asia/Jakarta")

# ====================================================================
# KONFIGURASI HEADER
# ====================================================================
AUDIT_COLS = [
    "Waktu & Tanggal",      # Index 0
    "Pelaku (User)",        # Index 1
    "Jabatan / Role",       # Index 2
    "Fitur yg Digunakan",   # Index 3
    "Nama Data / Sheet",    # Index 4
    "Baris Ke-",            # Index 5
    "Aksi Dilakukan",       # Index 6
    "Alasan Perubahan",     # Index 7
    "Rincian (Sebelum ➡ Sesudah)" # Index 8
]

def format_audit_sheet_smart(ws):
    """
    Fungsi ini meniru logika 'auto_format_sheet' di app.py.
    Mengirim satu paket perintah (batch_update) untuk mengatur lebar kolom,
    warna header, dan text wrapping sekaligus agar rapi.
    """
    try:
        sheet_id = ws.id
        
        # 1. Definisi Lebar Kolom (dalam Pixel)
        # Urutan harus sesuai dengan AUDIT_COLS
        col_widths = [
            160, # Waktu & Tanggal
            150, # Pelaku
            120, # Jabatan
            130, # Fitur
            180, # Nama Data
            80,  # Baris Ke
            100, # Aksi
            250, # Alasan (Agak lebar)
            500  # Rincian (Sangat lebar)
        ]

        requests = []

        # A. Atur Lebar Kolom (Column Resizing)
        for i, width in enumerate(col_widths):
            requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": i,
                        "endIndex": i + 1
                    },
                    "properties": {"pixelSize": width},
                    "fields": "pixelSize"
                }
            })

        # B. Styling Header (Baris 1): Bold, Center, Background Biru
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.85, "green": 0.92, "blue": 0.97}, # Biru Muda
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {"bold": True, "fontSize": 10}
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"
            }
        })

        # C. Styling Body (Baris 2 ke bawah): Wrap Text untuk kolom panjang, Align Top
        # Kita set default Alignment TOP untuk semua sel
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1
                },
                "cell": {
                    "userEnteredFormat": {
                        "verticalAlignment": "TOP",
                        "wrapStrategy": "CLIP" # Default clip agar tidak berantakan
                    }
                },
                "fields": "userEnteredFormat(verticalAlignment,wrapStrategy)"
            }
        })

        # Khusus Kolom 'Alasan' (Index 7) dan 'Rincian' (Index 8) kita paksa WRAP text
        # agar kalau teks panjang dia turun ke bawah (multiline)
        for col_idx in [7, 8]: 
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "startColumnIndex": col_idx,
                        "endColumnIndex": col_idx + 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "wrapStrategy": "WRAP",
                            "verticalAlignment": "TOP"
                        }
                    },
                    "fields": "userEnteredFormat(wrapStrategy,verticalAlignment)"
                }
            })

        # D. Freeze Header (Bekukan baris 1)
        requests.append({
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 1}
                },
                "fields": "gridProperties.frozenRowCount"
            }
        })

        # Eksekusi semua perintah formatting dalam 1 kali request (Cepat & Stabil)
        ws.spreadsheet.batch_update({"requests": requests})
        
    except Exception as e:
        print(f"[Warning] Gagal formatting audit sheet: {e}")

def ensure_audit_sheet(spreadsheet):
    """
    Memastikan tab audit tersedia dan terformat rapi.
    """
    try:
        # 1. GET / CREATE SHEET
        try:
            ws = spreadsheet.worksheet(SHEET_AUDIT_NAME)
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=SHEET_AUDIT_NAME, rows=1000, cols=len(AUDIT_COLS))
            # Isi Header
            ws.update(range_name="A1", values=[AUDIT_COLS], value_input_option="USER_ENTERED")
            # Langsung format jika baru dibuat
            format_audit_sheet_smart(ws)
            return ws

        # 2. CEK APAKAH HEADER SESUAI
        # Ambil baris pertama untuk cek
        existing_headers = ws.row_values(1)
        
        # Jika sheet kosong atau header beda, kita timpa header & format ulang
        if not existing_headers or existing_headers != AUDIT_COLS:
            ws.update(range_name="A1", values=[AUDIT_COLS], value_input_option="USER_ENTERED")
            format_audit_sheet_smart(ws)
        
        # Opsional: Kita bisa panggil format setiap kali app start untuk memastikan rapi,
        # tapi untuk performa, kita panggil hanya jika diperlukan. 
        # Namun, agar "pasti rapi" sesuai request Anda, kita panggil saja:
        # (Fungsi ini ringan karena hanya kirim JSON config)
        format_audit_sheet_smart(ws)

        return ws

    except Exception as e:
        print(f"CRITICAL ERROR di ensure_audit_sheet: {e}")
        # Fallback return sheet apa adanya agar app tidak crash
        try:
            return spreadsheet.worksheet(SHEET_AUDIT_NAME)
        except:
            raise e

def format_changes_human_readable(changes_dict):
    """
    Format teks Before -> After yang rapi untuk kolom Rincian.
    """
    if not changes_dict:
        return "-"
    
    lines = []
    for col, vals in changes_dict.items():
        old_v = str(vals.get('old', '-')).strip()
        new_v = str(vals.get('new', '-')).strip()
        
        if not old_v: old_v = "(kosong)"
        if not new_v: new_v = "(kosong)"

        # Format: [Nama Kolom]: Lama ➡ Baru
        line = f"• {col}: {old_v} ➡ {new_v}"
        lines.append(line)
    
    return "\n".join(lines)

def log_admin_action(spreadsheet, actor, role, feature, target_sheet, row_idx, action, reason, changes_dict):
    """
    Mencatat log perubahan ke sheet.
    """
    try:
        ws = ensure_audit_sheet(spreadsheet)
        
        ts = datetime.now(TZ_JKT).strftime("%d-%m-%Y %H:%M:%S")
        
        readable_changes = format_changes_human_readable(changes_dict)
        
        # Petakan data
        row_data = [
            ts,             # Waktu
            str(actor),     # Pelaku
            str(role),      # Role
            str(feature),   # Fitur
            str(target_sheet), 
            str(row_idx),
            str(action),
            str(reason) if reason else "-",
            readable_changes
        ]
        
        ws.append_row(row_data, value_input_option="USER_ENTERED")
        return True
    except Exception as e:
        print(f"Audit Error: {e}")
        return False

def compare_and_get_changes(df_old, df_new, key_col_index=None):
    """
    Logika perbandingan data (Tetap sama).
    """
    changes = []
    if len(df_old) != len(df_new):
        return [] 

    cols = df_old.columns
    for i in range(len(df_old)):
        row_diff = {}
        has_change = False
        for col in cols:
            val_old = df_old.iloc[i][col]
            val_new = df_new.iloc[i][col]
            
            s_old = str(val_old).strip() if pd.notna(val_old) else ""
            s_new = str(val_new).strip() if pd.notna(val_new) else ""
            
            if s_old != s_new:
                row_diff[col] = {"old": s_old, "new": s_new}
                has_change = True
        
        if has_change:
            changes.append({
                "row_idx": i,
                "diff": row_diff
            })
            
    return changes

# --- Tambahkan ini di bagian paling bawah audit_service.py ---

def load_audit_log(spreadsheet):
    """
    Mengambil seluruh data Audit Log dari Spreadsheet untuk ditampilkan di App.
    """
    try:
        ws = ensure_audit_sheet(spreadsheet)
        data = ws.get_all_records()
        if not data:
            return pd.DataFrame(columns=AUDIT_COLS)
            
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        print(f"Error loading audit log: {e}")
        return pd.DataFrame(columns=AUDIT_COLS)
