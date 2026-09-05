import os
import shutil
from datetime import datetime, timedelta
import pytz

# ================= CONFIG =================
BASE_PATHS = [
    "advance/data",
    "daily/data"
]

IST = pytz.timezone("Asia/Kolkata")
NOW_IST = datetime.now(IST)

# 📅 Shard Cleanup Range (Existing Feature)
START_DATE = (NOW_IST - timedelta(days=5)).strftime("%Y%m%d")
END_DATE   = (NOW_IST - timedelta(days=1)).strftime("%Y%m%d")

# 📅 45+ Days Full Folder Deletion (New Feature)
CUTOFF_DATE = (NOW_IST - timedelta(days=45)).date()

FILES_TO_DELETE = [
    *(f"detailed{i}.json" for i in range(1, 10)),
    *(f"movie_summary{i}.json" for i in range(1, 10)),
]

# ================= HELPERS =================
def daterange(start, end):
    cur = datetime.strptime(start, "%Y%m%d")
    end = datetime.strptime(end, "%Y%m%d")
    while cur <= end:
        yield cur.strftime("%Y%m%d")
        cur += timedelta(days=1)

def is_valid_date_folder(folder_name: str) -> bool:
    if len(folder_name) != 8 or not folder_name.isdigit():
        return False
    try:
        datetime.strptime(folder_name, "%Y%m%d")
        return True
    except ValueError:
        return False

# ================= CLEANUP =================
deleted_files = 0
deleted_dirs = 0
deleted_old_folders = 0

print(f"🗓 Shard cleanup from {START_DATE} → {END_DATE} (IST)")
print(f"🧹 Full folder removal older than: {CUTOFF_DATE} (IST)")
print("======================================\n")

for base in BASE_PATHS:
    if not os.path.exists(base):
        continue

    # ---------------------------------------
    # 1️⃣ EXISTING FEATURE: SHARD CLEANUP
    # ---------------------------------------
    for date in daterange(START_DATE, END_DATE):
        folder = os.path.join(base, date)
        if not os.path.isdir(folder):
            continue

        # delete file shards
        for fname in FILES_TO_DELETE:
            path = os.path.join(folder, fname)
            if os.path.exists(path):
                os.remove(path)
                deleted_files += 1
                print(f"🗑 Deleted file: {path}")

        # delete logs directory
        logs_dir = os.path.join(folder, "logs")
        if os.path.isdir(logs_dir):
            shutil.rmtree(logs_dir)
            deleted_dirs += 1
            print(f"🗑 Deleted directory: {logs_dir}")

    # ---------------------------------------
    # 2️⃣ NEW FEATURE: DELETE 45+ DAY FOLDERS
    # ---------------------------------------
    for folder_name in os.listdir(base):
        folder_path = os.path.join(base, folder_name)

        if not os.path.isdir(folder_path):
            continue

        if not is_valid_date_folder(folder_name):
            continue

        folder_date = datetime.strptime(folder_name, "%Y%m%d").date()

        if folder_date < CUTOFF_DATE:
            try:
                shutil.rmtree(folder_path)
                deleted_old_folders += 1
                print(f"🔥 Deleted old folder (45+ days): {folder_path}")
            except Exception as e:
                print(f"❌ Error deleting {folder_path}: {e}")

# ================= SUMMARY =================
print("\n======================================")
print(f" Files removed (shards): {deleted_files}")
print(f" Log folders removed: {deleted_dirs}")
print(f" 45+ day folders removed: {deleted_old_folders}")
print(" Cleanup complete.")
