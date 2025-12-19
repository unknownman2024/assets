import json
import os
import sys
import threading
import random
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import cloudscraper

# -------- Selenium --------
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# =====================================================
# CONFIG
# =====================================================
NUM_WORKERS = 1                  # do NOT increase
SHARD_ID = 1
DUMP_EVERY = 25

API_TIMEOUT = 12
RETRY_SLEEP = (0.8, 1.5)
SELENIUM_SLEEP = (2.0, 3.5)

IST = timezone(timedelta(hours=5, minutes=30))
DATE_CODE = (datetime.now(IST) + timedelta(days=1)).strftime("%Y%m%d")

BASE_DIR = os.path.join("advance", "data", DATE_CODE)
os.makedirs(BASE_DIR, exist_ok=True)

SUMMARY_FILE  = f"{BASE_DIR}/movie_summary{SHARD_ID}.json"
DETAILED_FILE = f"{BASE_DIR}/detailed{SHARD_ID}.json"

lock = threading.Lock()
thread_local = threading.local()

all_data = {}
empty_venues = set()
fetch_count = 0

# =====================================================
# HEADERS
# =====================================================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118 Safari/537.36",
]

def random_ip():
    return ".".join(str(random.randint(10, 240)) for _ in range(4))

def headers():
    ip = random_ip()
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://in.bookmyshow.com",
        "Referer": "https://in.bookmyshow.com/",
        "X-Forwarded-For": ip,
        "Client-IP": ip,
    }

# =====================================================
# SCRAPERS
# =====================================================
def get_scraper():
    if hasattr(thread_local, "scraper"):
        return thread_local.scraper
    s = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True}
    )
    thread_local.scraper = s
    return s

def reset_identity():
    if hasattr(thread_local, "scraper"):
        del thread_local.scraper
    if hasattr(thread_local, "driver"):
        try:
            thread_local.driver.quit()
        except Exception:
            pass
        del thread_local.driver

def fetch_cloud(url):
    r = get_scraper().get(url, headers=headers(), timeout=API_TIMEOUT)
    if not r.text.strip().startswith("{"):
        raise ValueError("HTML response")
    return r.json()

# ---------------- Selenium API call ----------------
def get_driver():
    if hasattr(thread_local, "driver"):
        return thread_local.driver

    o = Options()
    o.add_argument("--headless=new")
    o.add_argument("--no-sandbox")
    o.add_argument("--disable-dev-shm-usage")
    o.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")

    d = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=o,
    )
    thread_local.driver = d
    return d

def fetch_via_selenium_api(venue_code):
    """
    Calls the SAME API but inside a real browser session.
    This bypasses BMS poisoning.
    """
    api_url = (
        "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
        f"?venueCode={venue_code}&dateCode={DATE_CODE}"
    )
    d = get_driver()
    d.set_page_load_timeout(30)
    d.get(api_url)
    body = d.page_source.strip()
    if not body.startswith("{"):
        return {}
    return json.loads(body)

# =====================================================
# PARSE RESPONSE (COMMON)
# =====================================================
def parse_payload(data, venue_code):
    sd = data.get("ShowDetails", [])
    if not sd:
        return {}

    venue_info = sd[0].get("Venues", {})
    venue_name = venue_info.get("VenueName", "")
    venue_add  = venue_info.get("VenueAdd", "")
    chain      = venue_info.get("VenueCompName", "Unknown")

    out = defaultdict(list)
    valid = 0

    for ev in sd[0].get("Event", []):
        title = ev.get("EventTitle", "Unknown")

        for ch in ev.get("ChildEvents", []):
            dim  = ch.get("EventDimension", "").strip()
            lang = ch.get("EventLanguage", "").strip()
            suffix = " | ".join(x for x in (dim, lang) if x)
            movie = f"{title} [{suffix}]" if suffix else title

            for sh in ch.get("ShowTimes", []):
                show_date = sh.get("ShowDateCode") or (sh.get("ShowDateTime", "")[:8])
                if show_date != DATE_CODE:
                    continue

                total = sold = avail = gross = 0
                for cat in sh.get("Categories", []):
                    seats = int(cat.get("MaxSeats", 0))
                    free  = int(cat.get("SeatsAvail", 0))
                    price = float(cat.get("CurPrice", 0))
                    total += seats
                    avail += free
                    sold  += seats - free
                    gross += (seats - free) * price

                valid += 1
                out[movie].append({
                    "venue": venue_name,
                    "address": venue_add,
                    "chain": chain,
                    "time": sh.get("ShowTime"),
                    "session_id": sh.get("SessionId"),
                    "audi": sh.get("Attributes", ""),
                    "total": total,
                    "available": avail,
                    "sold": sold,
                    "gross": round(gross, 2),
                })

    if valid:
        print(f"✅ [FETCHED] {venue_code} | shows={valid}")
    else:
        print(f"⚠️ [EMPTY] {venue_code}")

    return out

# =====================================================
# FETCH METHODS
# =====================================================
def fetch_api(venue_code):
    url = (
        "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
        f"?venueCode={venue_code}&dateCode={DATE_CODE}"
    )
    data = fetch_cloud(url)
    return parse_payload(data, venue_code)

# =====================================================
# AGGREGATION
# =====================================================
def aggregate(all_data, venues_meta):
    summary = {}
    detailed = []

    for vcode, movies in all_data.items():
        meta = venues_meta.get(vcode, {})
        city  = meta.get("City", "Unknown")
        state = meta.get("State", "Unknown")

        for movie, shows in movies.items():
            m = summary.setdefault(movie, {
                "shows": 0, "gross": 0, "sold": 0, "totalSeats": 0,
                "venues": set(), "cities": set(),
                "fastfilling": 0, "housefull": 0
            })

            m["venues"].add(vcode)
            m["cities"].add(city)

            for s in shows:
                sold = s["sold"]
                total = s["total"]
                occ = (sold / total * 100) if total else 0

                m["shows"] += 1
                m["gross"] += s["gross"]
                m["sold"] += sold
                m["totalSeats"] += total

                if occ >= 98:
                    m["housefull"] += 1
                elif occ >= 50:
                    m["fastfilling"] += 1

                detailed.append({
                    "movie": movie,
                    "city": city,
                    "state": state,
                    "venue": s["venue"],
                    "address": s["address"],
                    "time": s["time"],
                    "audi": s["audi"],
                    "session_id": s["session_id"],
                    "totalSeats": total,
                    "available": s["available"],
                    "sold": sold,
                    "gross": s["gross"],
                    "occupancy": round(occ, 2),
                    "source": "BMS",
                    "date": DATE_CODE
                })

    final = {
        k: {
            "shows": v["shows"],
            "gross": round(v["gross"], 2),
            "sold": v["sold"],
            "totalSeats": v["totalSeats"],
            "venues": len(v["venues"]),
            "cities": len(v["cities"]),
            "fastfilling": v["fastfilling"],
            "housefull": v["housefull"],
            "occupancy": round(v["sold"] / v["totalSeats"] * 100, 2) if v["totalSeats"] else 0
        }
        for k, v in summary.items()
    }

    return final, detailed

def dump_memory(venues):
    summary, detailed = aggregate(all_data, venues)
    with open(SUMMARY_FILE, "w") as f:
        json.dump(summary, f, indent=2)
    with open(DETAILED_FILE, "w") as f:
        json.dump(detailed, f, indent=2)

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    with open("venues1.json") as f:
        venues = json.load(f)

    print("🚀 PASS 1 — API")

    for vcode in venues.keys():
        try:
            data = fetch_api(vcode)
            all_data[vcode] = data
            if not data:
                empty_venues.add(vcode)
        except Exception:
            empty_venues.add(vcode)

    print(f"\n🔁 PASS 2 — API retry ({len(empty_venues)})\n")

    for vcode in list(empty_venues):
        time.sleep(random.uniform(*RETRY_SLEEP))
        reset_identity()
        try:
            data = fetch_api(vcode)
            if data:
                all_data[vcode] = data
                empty_venues.remove(vcode)
        except Exception:
            pass

    print(f"\n🧠 PASS 3 — SELENIUM VERIFY ({len(empty_venues)})\n")

    for idx, vcode in enumerate(list(empty_venues), start=1):
        print(f"[SEL {idx}/{len(empty_venues)}] {vcode}")
        time.sleep(random.uniform(*SELENIUM_SLEEP))
        reset_identity()
        try:
            data = fetch_via_selenium_api(vcode)
            parsed = parse_payload(data, vcode)
            if parsed:
                all_data[vcode] = parsed
                empty_venues.remove(vcode)
                print(f"✅ [RECOVERED via Selenium] {vcode}")
        except Exception:
            pass

    dump_memory(venues)

    print("\n🎯 FINAL STATUS")
    print(f"Total venues   : {len(venues)}")
    print(f"Recovered data : {len(all_data) - len(empty_venues)}")
    print(f"Still empty    : {len(empty_venues)}")
    print("✅ DONE — 100% RECOVERY MODE COMPLETE")
