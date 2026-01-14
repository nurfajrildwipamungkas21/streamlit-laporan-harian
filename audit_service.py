# =========================================================
# [BARU] SYSTEM: RAM STATE MANAGER (VPS OPTIMIZED)
# =========================================================
# Fungsi-fungsi ini menggantikan akses langsung ke GSheet
# agar aplikasi tidak perlu download ulang setiap kali diklik.

def init_ram_storage():
    """Menyiapkan struktur memori di RAM Session State."""
    if "RAM_DB" not in st.session_state:
        st.session_state["RAM_DB"] = {
            "loaded": False,          # Status apakah data sudah ditarik dari cloud
            "payment": None,          # Menyimpan Tabel Pembayaran
            "closing": None,          # Menyimpan Tabel Closing
            "staff": [],              # Menyimpan List Staff
            "kpi_team": None,         # Menyimpan Target Team
            "kpi_indiv": None         # Menyimpan Target Individu
        }

def get_ram_data(key):
    """Ambil data dari RAM (Kecepatan Instan)."""
    init_ram_storage()
    return st.session_state["RAM_DB"].get(key, None)

def update_ram_data(key, dataframe):
    """Update data di RAM agar UI berubah seketika."""
    init_ram_storage()
    st.session_state["RAM_DB"][key] = dataframe.copy()

def append_ram_data(key, new_row_dict):
    """Menambahkan baris baru ke data RAM."""
    current_df = get_ram_data(key)
    if current_df is not None:
        new_df = pd.DataFrame([new_row_dict])
        # Gabungkan dan simpan kembali ke RAM
        updated_df = pd.concat([current_df, new_df], ignore_index=True)
        st.session_state["RAM_DB"][key] = updated_df

def manual_hard_refresh():
    """Tombol Darurat: Hapus RAM dan paksa download ulang dari Cloud."""
    if "RAM_DB" in st.session_state:
        del st.session_state["RAM_DB"]
    st.cache_data.clear()
    st.rerun()
