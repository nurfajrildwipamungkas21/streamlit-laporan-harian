# audit_service.py
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import gspread
import json

# Nama Sheet Khusus Audit
SHEET_AUDIT_NAME = "Global_Audit_Log"
TZ_JKT = ZoneInfo("Asia/Jakarta")

# Format Kolom Audit
AUDIT_COLS = [
    "Timestamp", "Actor", "Role", "Feature", 
    "Target_Sheet", "Row_Index", "Action", 
    "Reason", "Changes_JSON"
]

def ensure_audit_sheet(spreadsheet):
    """Memastikan tab audit tersedia."""
    try:
        ws = spreadsheet.worksheet(SHEET_AUDIT_NAME)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=SHEET_AUDIT_NAME, rows=1000, cols=len(AUDIT_COLS))
        ws.append_row(AUDIT_COLS)
    return ws

def log_admin_action(spreadsheet, actor, role, feature, target_sheet, row_idx, action, reason, changes_dict):
    """
    Mencatat log ke Google Sheet.
    changes_dict format: {"ColumnName": {"old": "val_old", "new": "val_new"}}
    """
    try:
        ws = ensure_audit_sheet(spreadsheet)
        
        ts = datetime.now(TZ_JKT).strftime("%Y-%m-%d %H:%M:%S")
        changes_json = json.dumps(changes_dict, ensure_ascii=False)
        
        row_data = [
            ts, str(actor), str(role), str(feature),
            str(target_sheet), str(row_idx), str(action),
            str(reason) if reason else "-", changes_json
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
    # Pastikan index sama
    if len(df_old) != len(df_new):
        return [] # Row count changed, complex scenario skipped for now

    cols = df_old.columns
    for i in range(len(df_old)):
        row_diff = {}
        has_change = False
        for col in cols:
            val_old = df_old.iloc[i][col]
            val_new = df_new.iloc[i][col]
            
            # Normalisasi untuk perbandingan (str vs int, dsb)
            if str(val_old).strip() != str(val_new).strip():
                row_diff[col] = {"old": str(val_old), "new": str(val_new)}
                has_change = True
        
        if has_change:
            changes.append({
                "row_idx": i, # 0-based index from dataframe
                "diff": row_diff
            })
            
    return changes
