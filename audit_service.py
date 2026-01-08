# audit_service.py
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import gspread
import json

# Nama Sheet Khusus Audit
SHEET_AUDIT_NAME = "Global_Audit_Log"
TZ_JKT = ZoneInfo("Asia/Jakarta")

# Format Kolom Audit (Saya ubah nama kolom terakhir biar lebih relevan)
AUDIT_COLS = [
    "Timestamp", "Actor", "Role", "Feature", 
    "Target_Sheet", "Row_Index", "Action", 
    "Reason", "Detail_Perubahan" 
]

def ensure_audit_sheet(spreadsheet):
    """
    Memastikan tab audit tersedia. 
    Jika tidak ada, BUAT BARU SEKARANG JUGA.
    """
    try:
        ws = spreadsheet.worksheet(SHEET_AUDIT_NAME)
        return ws
    except gspread.WorksheetNotFound:
        try:
            ws = spreadsheet.add_worksheet(title=SHEET_AUDIT_NAME, rows=2000, cols=len(AUDIT_COLS))
            ws.update(range_name="A1", values=[AUDIT_COLS], value_input_option="USER_ENTERED")
            ws.format("A1:I1", {"textFormat": {"bold": True}})
            ws.freeze(rows=1)
            # Atur lebar kolom Detail Perubahan (Kolom I / ke-9) agar lega
            ws.set_column_width(8, 250) # Kolom Reason
            ws.set_column_width(9, 400) # Kolom Detail
            return ws
        except Exception as e:
            raise Exception(f"Gagal membuat sheet Audit otomatis: {e}")

def format_changes_human_readable(changes_dict):
    """
    Mengubah dictionary aneh menjadi teks rapi.
    Contoh Input: {"Nama": {"old": "A", "new": "B"}}
    Output: "• Nama: A ➡ B"
    """
    if not changes_dict:
        return "-"
    
    lines = []
    for col, vals in changes_dict.items():
        # Ambil nilai lama dan baru, jika kosong/None ganti strip
        old_v = str(vals.get('old', '-')).strip()
        new_v = str(vals.get('new', '-')).strip()
        
        # Jika kosong stringnya
        if not old_v: old_v = "(kosong)"
        if not new_v: new_v = "(kosong)"

        # Format rapi: [Nama Kolom]: Lama ➡ Baru
        line = f"• {col}: {old_v} ➡ {new_v}"
        lines.append(line)
    
    # Gabung dengan Enter (Line Break) agar berbaris ke bawah di Excel/GSheet
    return "\n".join(lines)

def log_admin_action(spreadsheet, actor, role, feature, target_sheet, row_idx, action, reason, changes_dict):
    """
    Mencatat log ke Google Sheet dengan format teks yang mudah dibaca.
    """
    try:
        ws = ensure_audit_sheet(spreadsheet)
        
        ts = datetime.now(TZ_JKT).strftime("%Y-%m-%d %H:%M:%S")
        
        # --- PERUBAHAN UTAMA DISINI ---
        # Tidak lagi pakai json.dumps, tapi pakai formatter baru
        readable_changes = format_changes_human_readable(changes_dict)
        # ------------------------------
        
        row_data = [
            ts, str(actor), str(role), str(feature),
            str(target_sheet), str(row_idx), str(action),
            str(reason) if reason else "-", 
            readable_changes # Masukkan teks rapi ini
        ]
        
        ws.append_row(row_data, value_input_option="USER_ENTERED")
        return True
    except Exception as e:
        print(f"Audit Error: {e}")
        return False

def compare_and_get_changes(df_old, df_new, key_col_index=None):
    """
    Membandingkan 2 DataFrame dan mendeteksi perubahan.
    Mengembalikan list of updates.
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
            
            # Normalisasi string biar aman
            s_old = str(val_old).strip() if pd.notna(val_old) else ""
            s_new = str(val_new).strip() if pd.notna(val_new) else ""
            
            # Bandingkan
            if s_old != s_new:
                row_diff[col] = {"old": s_old, "new": s_new}
                has_change = True
        
        if has_change:
            changes.append({
                "row_idx": i,
                "diff": row_diff
            })
            
    return changes
