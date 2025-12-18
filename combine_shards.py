import json
import os
from datetime import datetime, timedelta
import pytz

# ---------------- IST DATE + 1 ----------------
IST = pytz.timezone("Asia/Kolkata")
date_ist_plus_1 = (datetime.now(IST) + timedelta(days=1)).strftime("%Y%m%d")

BASE_DIR = f"advance/data/{date_ist_plus_1}"
OUTPUT_FILE = os.path.join(BASE_DIR, "finaldetailed.json")

print(f"📁 Using directory: {BASE_DIR}")

# ---------------- HELPERS ----------------
def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"⚠️ Missing: {path}")
        return []
    except json.JSONDecodeError:
        print(f"❌ Invalid JSON: {path}")
        return []


def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ---------------- COMBINE DETAILED LISTS ----------------
final_detailed = []

for i in range(1, 9):
    file_path = os.path.join(BASE_DIR, f"detailed{i}.json")
    data = load_json(file_path)

    if isinstance(data, list):
        final_detailed.extend(data)
        print(f"✅ Added {len(data)} records from detailed{i}.json")
    else:
        print(f"⚠️ Skipped detailed{i}.json (not a list)")

print(f"🎯 Total combined records: {len(final_detailed)}")

save_json(OUTPUT_FILE, final_detailed)

print(f"🎉 finaldetailed.json created successfully")
