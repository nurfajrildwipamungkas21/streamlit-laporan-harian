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
    Memastikan tab audit tersedia dengan Header Bahasa Indonesia.
    """
    try:
        ws = spreadsheet.worksheet(SHEET_AUDIT_NAME)
        
        # [OPSIONAL] Cek apakah header masih lama? Jika iya, update header saja.
        # Ini biar Anda tidak perlu hapus sheet manual jika hanya ganti nama kolom.
        current_header = ws.row_values(1)
        if not current_header or current_header[0] == "Timestamp": 
            # Timpa header lama dengan Bahasa Indonesia
            ws.update(range_name="A1", values=[AUDIT_COLS], value_input_option="USER_ENTERED")
            ws.format("A1:I1", {"textFormat": {"bold": True}})
            
        return ws
    except gspread.WorksheetNotFound:
        try:
            ws = spreadsheet.add_worksheet(title=SHEET_AUDIT_NAME, rows=2000, cols=len(AUDIT_COLS))
            ws.update(range_name="A1", values=[AUDIT_COLS], value_input_option="USER_ENTERED")
            ws.format("A1:I1", {"textFormat": {"bold": True}})
            ws.freeze(rows=1)
            
            # Atur lebar kolom agar enak dibaca (dalam pixel)
            ws.set_column_width(1, 160) # Waktu
            ws.set_column_width(2, 120) # Pelaku
            ws.set_column_width(5, 150) # Nama Data
            ws.set_column_width(8, 200) # Alasan
            ws.set_column_width(9, 450) # Rincian (Paling Lebar)
            return ws
        except Exception as e:
            raise Exception(f"Gagal membuat sheet Audit otomatis: {e}")

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
