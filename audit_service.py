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
    Memastikan tab audit tersedia dengan Formatting Rapi (Style ala app.py).
    """
    try:
        # Coba buka sheetnya
        try:
            ws = spreadsheet.worksheet(SHEET_AUDIT_NAME)
        except gspread.WorksheetNotFound:
            # Jika belum ada, buat baru
            ws = spreadsheet.add_worksheet(title=SHEET_AUDIT_NAME, rows=2000, cols=len(AUDIT_COLS))
            # Isi Header
            ws.update(range_name="A1", values=[AUDIT_COLS], value_input_option="USER_ENTERED")

        # ==========================================
        # LOGIC FORMATTING OTOMATIS (PERBAIKAN)
        # ==========================================
        
        # 1. Pastikan Header Sesuai (Jika header lama, timpa baru)
        current_header = ws.row_values(1)
        if not current_header or current_header[0] != AUDIT_COLS[0]:
            ws.update(range_name="A1", values=[AUDIT_COLS], value_input_option="USER_ENTERED")

        # 2. Atur Lebar Kolom (Pixel)
        ws.batch_update({
            "requests": [
                # Atur Lebar Kolom
                {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1}, "properties": {"pixelSize": 150}, "fields": "pixelSize"}}, # Waktu
                {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2}, "properties": {"pixelSize": 120}, "fields": "pixelSize"}}, # Pelaku
                {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 2, "endIndex": 4}, "properties": {"pixelSize": 130}, "fields": "pixelSize"}}, # Role & Fitur
                {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 4, "endIndex": 5}, "properties": {"pixelSize": 150}, "fields": "pixelSize"}}, # Nama Data
                {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 5, "endIndex": 6}, "properties": {"pixelSize": 80},  "fields": "pixelSize"}}, # Baris Ke
                {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 6, "endIndex": 7}, "properties": {"pixelSize": 100}, "fields": "pixelSize"}}, # Aksi
                {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 7, "endIndex": 8}, "properties": {"pixelSize": 200}, "fields": "pixelSize"}}, # Alasan
                {"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 8, "endIndex": 9}, "properties": {"pixelSize": 450}, "fields": "pixelSize"}}, # Rincian (Lebar)
                
                # Freeze Row 1
                {"updateSheetProperties": {"properties": {"sheetId": ws.id, "gridProperties": {"frozenRowCount": 1}}, "fields": "gridProperties.frozenRowCount"}},
                
                # Format Header (Bold, Center, Background Biru Muda)
                {"repeatCell": {
                    "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {"red": 0.9, "green": 0.94, "blue": 0.98}, # Biru Muda Soft
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE",
                            "textFormat": {"bold": True, "fontSize": 10}
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"
                }},

                # Format Body (Rata Atas, Wrap Text) - PENTING BIAR RAPI
                {"repeatCell": {
                    "range": {"sheetId": ws.id, "startRowIndex": 1},
                    "cell": {
                        "userEnteredFormat": {
                            "verticalAlignment": "TOP",   # Teks selalu mulai dari atas sel
                            "wrapStrategy": "WRAP"        # Teks panjang akan turun ke bawah (tidak kepotong)
                        }
                    },
                    "fields": "userEnteredFormat(verticalAlignment,wrapStrategy)"
                }}
            ]
        })
        
        return ws

    except Exception as e:
        # Jika error formatting, return ws apa adanya agar sistem tidak crash
        print(f"Warning (Audit Format): {e}")
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
