import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import gspread
import json

SHEET_AUDIT_NAME = "Global_Audit_Log"
TZ_JKT = ZoneInfo("Asia/Jakarta")

AUDIT_COLS = [
    "Waktu & Tanggal",
    "Pelaku (User)",
    "Jabatan / Role",
    "Fitur yg Digunakan",
    "Nama Data / Sheet",
    "Baris Ke-",
    "Aksi Dilakukan",
    "Alasan Perubahan",
    "Rincian (Sebelum ➡ Sesudah)"
]

def format_audit_sheet_smart(ws):
    try:
        sheet_id = ws.id
        col_widths = [
            160, # Waktu
            150, # Pelaku
            120, # Jabatan
            130, # Fitur
            180, # Nama Data
            80,  # Baris Ke
            100, # Aksi
            250, # Alasan
            500  # Rincian
        ]

        requests = []

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

        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.85, "green": 0.92, "blue": 0.97},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {"bold": True, "fontSize": 10}
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,horizontalAlignment,verticalAlignment,textFormat)"
            }
        })

        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1
                },
                "cell": {
                    "userEnteredFormat": {
                        "verticalAlignment": "TOP",
                        "wrapStrategy": "CLIP"
                    }
                },
                "fields": "userEnteredFormat(verticalAlignment,wrapStrategy)"
            }
        })

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

        requests.append({
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 1}
                },
                "fields": "gridProperties.frozenRowCount"
            }
        })

        ws.spreadsheet.batch_update({"requests": requests})
        
    except Exception as e:
        print(f"[Warning] Gagal formatting audit sheet: {e}")

def ensure_audit_sheet(spreadsheet):
    try:
        try:
            ws = spreadsheet.worksheet(SHEET_AUDIT_NAME)
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=SHEET_AUDIT_NAME, rows=2000, cols=len(AUDIT_COLS))
            ws.update(range_name="A1", values=[AUDIT_COLS], value_input_option="USER_ENTERED")
            format_audit_sheet_smart(ws)
            return ws

        existing_headers = ws.row_values(1)
        
        if not existing_headers or existing_headers != AUDIT_COLS:
            ws.update(range_name="A1", values=[AUDIT_COLS], value_input_option="USER_ENTERED")
            format_audit_sheet_smart(ws)
        
        return ws

    except Exception as e:
        print(f"CRITICAL ERROR di ensure_audit_sheet: {e}")
        try:
            return spreadsheet.worksheet(SHEET_AUDIT_NAME)
        except:
            raise e

def format_changes_human_readable(changes_dict):
    if not changes_dict:
        return "-"
    
    lines = []
    for col, vals in changes_dict.items():
        old_v = str(vals.get('old', '-')).strip()
        new_v = str(vals.get('new', '-')).strip()
        
        if not old_v: old_v = "(kosong)"
        if not new_v: new_v = "(kosong)"

        line = f"• {col}: {old_v} ➡ {new_v}"
        lines.append(line)
    
    return "\n".join(lines)

def log_admin_action(spreadsheet, actor, role, feature, target_sheet, row_idx, action, reason, changes_dict):
    try:
        ws = ensure_audit_sheet(spreadsheet)
        
        ts = datetime.now(TZ_JKT).strftime("%d-%m-%Y %H:%M:%S")
        
        readable_changes = format_changes_human_readable(changes_dict)
        
        row_data = [
            ts,
            str(actor),
            str(role),
            str(feature),
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

def load_audit_log(spreadsheet):
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
