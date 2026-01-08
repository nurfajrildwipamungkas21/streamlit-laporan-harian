# data_gateway.py
import sqlite3
from audit import DB_PATH, log_change

def conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def get_one(table: str, record_id: str):
    with conn() as con:
        cur = con.execute(f"SELECT * FROM {table} WHERE id = ?", (record_id,))
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))

def update_record(*, table: str, record_id: str, new_data: dict,
                  actor: str, role: str, feature: str, reason: str | None):
    before = get_one(table, record_id)
    if before is None:
        raise ValueError(f"Record tidak ditemukan: {table} id={record_id}")

    keys = list(new_data.keys())
    set_clause = ", ".join([f"{k} = ?" for k in keys])
    values = [new_data[k] for k in keys] + [record_id]

    with conn() as con:
        con.execute(f"UPDATE {table} SET {set_clause} WHERE id = ?", values)
        con.commit()

    after = get_one(table, record_id)

    log_change(
        actor=actor,
        role=role,
        feature=feature,
        entity=table,
        record_id=record_id,
        action="UPDATE",
        before=before,
        after=after,
        reason=reason
    )
