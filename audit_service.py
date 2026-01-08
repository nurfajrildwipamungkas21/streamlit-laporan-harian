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
    Memastikan tab audit tersedia dengan Formatting Profesional (Enterprise Look):
    1. Header: Biru Elegan + Bold + Center.
    2. Body: Borders (Garis Tabel) + Align Top + Wrap Text.
    3. Column Width: 
       - Kolom A (Waktu) & I (Rincian) -> FIXED (Agar rapi).
       - Kolom B-H -> AUTO RESIZE (Menyesuaikan isi).
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
            range_header = f"A1:{chr(65 + len(AUDIT_COLS) - 1)}1"
            ws.update(range_name=range_header, values=[AUDIT_COLS], value_input_option="USER_ENTERED")

        # ==========================================
        # 3. FORMATTING (ULTIMATE BATCH)
        # ==========================================
        sheet_id = ws.id

        requests = [
            # A. Freeze Header
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                    "fields": "gridProperties.frozenRowCount"
                }
            },
            # B. Format Header (Background Biru Profesional)
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {"red": 0.22, "green": 0.46, "blue": 0.73}, # Biru Tua Professional
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE",
                            "textFormat": {"bold": True, "fontSize": 10, "foregroundColor": {"red": 1, "green": 1, "blue": 1}} # Teks Putih
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"
                }
            },
            # C. Format Body (Wrap Text, Align Top, Font Standar)
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1},
                    "cell": {
                        "userEnteredFormat": {
                            "verticalAlignment": "TOP",
                            "wrapStrategy": "WRAP",
                            "textFormat": {"fontSize": 10}
                        }
                    },
                    "fields": "userEnteredFormat(verticalAlignment,wrapStrategy,textFormat)"
                }
            },
            # D. ADD BORDERS (GARIS TABEL) - INI YANG BIKIN RAPI
            {
                "updateBorders": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0, # Dari Header sampai bawah
                        "startColumnIndex": 0,
                        "endColumnIndex": 9
                    },
                    "top": {"style": "SOLID", "width": 1, "color": {"red": 0.8, "green": 0.8, "blue": 0.8}},
                    "bottom": {"style": "SOLID", "width": 1, "color": {"red": 0.8, "green": 0.8, "blue": 0.8}},
                    "left": {"style": "SOLID", "width": 1, "color": {"red": 0.8, "green": 0.8, "blue": 0.8}},
                    "right": {"style": "SOLID", "width": 1, "color": {"red": 0.8, "green": 0.8, "blue": 0.8}},
                    "innerHorizontal": {"style": "SOLID", "width": 1, "color": {"red": 0.9, "green": 0.9, "blue": 0.9}},
                    "innerVertical": {"style": "SOLID", "width": 1, "color": {"red": 0.9, "green": 0.9, "blue": 0.9}},
                }
            },
            # E. FIXED WIDTH (Waktu & Rincian)
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1}, # Kolom A (Waktu)
                    "properties": {"pixelSize": 160}, "fields": "pixelSize"
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 8, "endIndex": 9}, # Kolom I (Rincian)
                    "properties": {"pixelSize": 500}, "fields": "pixelSize"
                }
            },
            # F. AUTO RESIZE (Kolom B - H)
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": 1, # Mulai dari Kolom B (Pelaku)
                        "endIndex": 8    # Sampai sebelum I
                    }
                }
            }
        ]

        ws.batch_update({"requests": requests})
        return ws

    except Exception as e:
        print(f"[ERROR Formatting Audit]: {e}")
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
