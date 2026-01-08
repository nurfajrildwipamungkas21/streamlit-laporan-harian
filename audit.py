# audit.py
import sqlite3, json
from datetime import datetime, timezone

DB_PATH = "app.db"

def conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_audit():
    with conn() as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_utc TEXT NOT NULL,
            actor TEXT NOT NULL,          -- email/username
            role TEXT NOT NULL,           -- admin/user
            feature TEXT NOT NULL,        -- "Pembayaran", "Closing Deal", dll
            entity TEXT NOT NULL,         -- nama tabel/sheet
            record_id TEXT NOT NULL,      -- id row/uuid
            action TEXT NOT NULL,         -- INSERT/UPDATE/DELETE
            reason TEXT,                  -- alasan admin edit (disarankan wajib)
            before_json TEXT,
            after_json TEXT,
            diff_json TEXT
        )
        """)
        con.commit()

def _diff(before: dict, after: dict):
    diff = {}
    keys = set(before.keys()) | set(after.keys())
    for k in keys:
        b, a = before.get(k), after.get(k)
        if b != a:
            diff[k] = {"before": b, "after": a}
    return diff

def log_change(*, actor, role, feature, entity, record_id, action, before=None, after=None, reason=None):
    ts = datetime.now(timezone.utc).isoformat()
    before = before or {}
    after = after or {}
    diff = _diff(before, after) if action == "UPDATE" else {"before": before, "after": after}

    with conn() as con:
        con.execute("""
        INSERT INTO audit_log
        (ts_utc, actor, role, feature, entity, record_id, action, reason, before_json, after_json, diff_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            ts, str(actor), str(role), str(feature), str(entity), str(record_id), str(action), reason,
            json.dumps(before, ensure_ascii=False),
            json.dumps(after, ensure_ascii=False),
            json.dumps(diff, ensure_ascii=False),
        ))
        con.commit()
