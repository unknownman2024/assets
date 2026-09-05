import json
import os
import random
import time
import threading
import signal
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import cloudscraper

# =====================================================
# CONFIG – tune these for speed vs. safety
# =====================================================
SHARD_ID = 4
API_TIMEOUT = 12
HARD_TIMEOUT = 15
MAX_RECOVERY_ROUNDS = 5           # max retries per venue (initial + retry rounds)
IDENTITY_ROTATE_EVERY = 10        # new identity after every N requests
FAILURE_THRESHOLD = 5             # consecutive failures before cooldown
COOLDOWN_SECONDS = 30             # pause when too many failures
BASE_DELAY = 0.45                  # minimum seconds between requests
MAX_DELAY = 1.75                   # maximum seconds between requests (adaptive)

IST = timezone(timedelta(hours=5, minutes=30))
DATE_CODE = os.environ["DATE_CODE"]

BASE_DIR = os.path.join("advance", "data", DATE_CODE)
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

SUMMARY_FILE  = f"{BASE_DIR}/movie_summary{SHARD_ID}.json"
DETAILED_FILE = f"{BASE_DIR}/detailed{SHARD_ID}.json"
LOG_FILE      = f"{LOG_DIR}/bms{SHARD_ID}.log"

# =====================================================
# LOGGING
# =====================================================
def log(msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# =====================================================
# HARD TIMEOUT
# =====================================================
class TimeoutError(Exception):
    pass

def _timeout_handler(signum, frame):
    raise TimeoutError("Hard timeout hit")

def hard_timeout(seconds):
    def deco(fn):
        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _timeout_handler)
            signal.alarm(seconds)
            try:
                return fn(*args, **kwargs)
            finally:
                signal.alarm(0)
        return wrapper
    return deco

# =====================================================
# USER AGENTS (enhanced)
# =====================================================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
]

thread_local = threading.local()
request_counter = 0

class Identity:
    def __init__(self):
        self.ua = random.choice(USER_AGENTS)
        self.ip = ".".join(str(random.randint(20, 230)) for _ in range(4))
        self.scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )

    def headers(self):
        # Full browser-like headers
        return {
            "User-Agent": self.ua,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-IN,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Origin": "https://in.bookmyshow.com",
            "Referer": "https://in.bookmyshow.com/",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "X-Forwarded-For": self.ip,
        }

def get_identity():
    global request_counter
    if not hasattr(thread_local, "identity"):
        thread_local.identity = Identity()
        log("🧠 New identity created")
    request_counter += 1
    # Rotate proactively every N requests
    if request_counter % IDENTITY_ROTATE_EVERY == 0:
        reset_identity()
        thread_local.identity = Identity()
        log(f"🔄 Identity rotated (every {IDENTITY_ROTATE_EVERY} requests)")
    return thread_local.identity

def reset_identity():
    if hasattr(thread_local, "identity"):
        del thread_local.identity
    log("🔄 Identity reset")

# =====================================================
# FETCH API (with retry and block detection)
# =====================================================
@hard_timeout(HARD_TIMEOUT)
def fetch_api_raw(venue_code):
    ident = get_identity()
    url = (
        "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
        f"?venueCode={venue_code}&dateCode={DATE_CODE}"
    )
    r = ident.scraper.get(url, headers=ident.headers(), timeout=API_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    text = r.text.strip()
    if not text.startswith("{"):
        snippet = text[:200].replace("\n", " ")
        raise RuntimeError(f"Blocked / HTML (snippet: {snippet})")
    return r.json()

def fetch_with_retry(venue_code, attempt=1):
    try:
        return fetch_api_raw(venue_code)
    except Exception as e:
        if attempt >= MAX_RECOVERY_ROUNDS:
            log(f"❌ {venue_code} failed after {MAX_RECOVERY_ROUNDS} attempts")
            raise
        delay = (2 ** attempt) + random.uniform(0, 1)   # exponential backoff
        log(f"🔄 Retry {attempt}/{MAX_RECOVERY_ROUNDS} for {venue_code} in {delay:.1f}s")
        reset_identity()
        time.sleep(delay)
        return fetch_with_retry(venue_code, attempt + 1)

# =====================================================
# PARSER
# =====================================================
def parse_payload(data):
    out = []

    sd = data.get("ShowDetails", [])
    if not sd:
        return out

    venue = sd[0].get("Venues", {})
    venue_name = venue.get("VenueName", "")
    venue_add  = venue.get("VenueAdd", "")
    chain      = venue.get("VenueCompName", "Unknown")

    for ev in sd[0].get("Event", []):
        title = ev.get("EventTitle", "Unknown")

        for ch in ev.get("ChildEvents", []):
            dim  = ch.get("EventDimension", "").strip()
            lang = ch.get("EventLanguage", "").strip()
            suffix = " | ".join(x for x in (dim, lang) if x)
            movie = f"{title} [{suffix}]" if suffix else title

            for sh in ch.get("ShowTimes", []):
                show_date = sh.get("ShowDateCode")
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

                out.append({
                    "movie": movie,
                    "venue": venue_name,
                    "address": venue_add,
                    "chain": chain,
                    "time": sh.get("ShowTime", ""),
                    "audi": sh.get("Attributes", "") or "",
                    "session_id": str(sh.get("SessionId", "")),
                    "totalSeats": total,
                    "available": avail,
                    "sold": sold,
                    "gross": round(gross, 2)
                })

    return out

# =====================================================
# DEDUPE
# =====================================================
def dedupe(rows):
    seen = set()
    out = []
    for r in rows:
        key = (
            r["venue"],
            r["time"],
            r["session_id"],
            r["audi"]
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    log("🚀 SCRIPT STARTED (enhanced anti‑detection)")

    with open(f"venues{SHARD_ID}.json", "r", encoding="utf-8") as f:
        venues = json.load(f)

    all_rows = []
    retry = set()
    failure_count = 0

    # ------ INITIAL PASS ------
    for i, vcode in enumerate(venues, 1):
        log(f"[{i}/{len(venues)}] {vcode}")

        # Adaptive delay: increase after failures
        delay = BASE_DELAY + (failure_count * 0.2)
        delay = min(delay, MAX_DELAY)
        time.sleep(random.uniform(delay, delay + 0.3))

        try:
            raw = fetch_with_retry(vcode)   # internal retries
            rows = parse_payload(raw)
            for r in rows:
                r["city"] = venues[vcode].get("City", "Unknown")
                r["state"] = venues[vcode].get("State", "Unknown")
                r["source"] = "BMS"
                r["date"] = DATE_CODE
            all_rows.extend(rows)
            failure_count = 0   # reset on success
        except Exception as e:
            retry.add(vcode)
            failure_count += 1
            if failure_count >= FAILURE_THRESHOLD:
                log(f"⛔ {failure_count} consecutive failures – cooling down {COOLDOWN_SECONDS}s")
                time.sleep(COOLDOWN_SECONDS)
                failure_count = 0
            reset_identity()
            log(f"❌ {vcode} final failure | {type(e).__name__} | {str(e)[:100]}")

    # ------ RETRY ROUNDS (for failed venues) ------
    for attempt in range(1, MAX_RECOVERY_ROUNDS + 1):
        if not retry:
            break

        log(f"🔁 RETRY ROUND {attempt} | Remaining: {len(retry)}")
        current_retry = list(retry)
        retry.clear()

        for vcode in current_retry:
            log(f"[RETRY-{attempt}] {vcode}")
            # Adaptive delay inside retry (slightly longer)
            time.sleep(random.uniform(1.0, 2.5))

            try:
                raw = fetch_with_retry(vcode, attempt=1)  # will use its own retries
                rows = parse_payload(raw)
                for r in rows:
                    r["city"] = venues[vcode].get("City", "Unknown")
                    r["state"] = venues[vcode].get("State", "Unknown")
                    r["source"] = "BMS"
                    r["date"] = DATE_CODE
                all_rows.extend(rows)
            except Exception as e:
                retry.add(vcode)
                reset_identity()
                log(f"❌ RETRY FAIL {vcode} | {type(e).__name__}")

            time.sleep(random.uniform(0.5, 1.0))   # small delay between retry items

    if retry:
        log(f"⚠️ FINAL FAILED VENUES: {len(retry)}")
        log(f"FAILED LIST: {list(retry)}")

    log("🧹 Deduping shows")
    detailed = dedupe(all_rows)

    # =====================================================
    # SUMMARY (AFTER DEDUPE) – unchanged
    # =====================================================
    summary = {}

    for r in detailed:
        movie = r["movie"]
        city  = r["city"]
        state = r["state"]
        venue = r["venue"]
        chain = r["chain"]

        total = r["totalSeats"]
        sold  = r["sold"]
        gross = r["gross"]
        occ   = (sold / total * 100) if total else 0

        if movie not in summary:
            summary[movie] = {
                "shows": 0,
                "gross": 0.0,
                "sold": 0,
                "totalSeats": 0,
                "venues": set(),
                "cities": set(),
                "fastfilling": 0,
                "housefull": 0,
                "details": {},
                "Chain_details": {}
            }

        m = summary[movie]
        m["shows"] += 1
        m["gross"] += gross
        m["sold"] += sold
        m["totalSeats"] += total
        m["venues"].add(venue)
        m["cities"].add(city)

        if occ >= 98:
            m["housefull"] += 1
        elif occ >= 50:
            m["fastfilling"] += 1

        ck = (city, state)
        if ck not in m["details"]:
            m["details"][ck] = {
                "city": city, "state": state,
                "venues": set(),
                "shows": 0, "gross": 0.0,
                "sold": 0, "totalSeats": 0,
                "fastfilling": 0, "housefull": 0
            }

        d = m["details"][ck]
        d["venues"].add(venue)
        d["shows"] += 1
        d["gross"] += gross
        d["sold"] += sold
        d["totalSeats"] += total
        if occ >= 98:
            d["housefull"] += 1
        elif occ >= 50:
            d["fastfilling"] += 1

        if chain not in m["Chain_details"]:
            m["Chain_details"][chain] = {
                "chain": chain,
                "venues": set(),
                "shows": 0, "gross": 0.0,
                "sold": 0, "totalSeats": 0,
                "fastfilling": 0, "housefull": 0
            }

        c = m["Chain_details"][chain]
        c["venues"].add(venue)
        c["shows"] += 1
        c["gross"] += gross
        c["sold"] += sold
        c["totalSeats"] += total
        if occ >= 98:
            c["housefull"] += 1
        elif occ >= 50:
            c["fastfilling"] += 1

    # =====================================================
    # FINALIZE SUMMARY
    # =====================================================
    final_summary = {}

    for movie, m in summary.items():
        final_summary[movie] = {
            "shows": m["shows"],
            "gross": round(m["gross"], 2),
            "sold": m["sold"],
            "totalSeats": m["totalSeats"],
            "venues": len(m["venues"]),
            "cities": len(m["cities"]),
            "fastfilling": m["fastfilling"],
            "housefull": m["housefull"],
            "occupancy": round((m["sold"] / m["totalSeats"]) * 100, 2) if m["totalSeats"] else 0.0,
            "details": [],
            "Chain_details": []
        }

        for d in m["details"].values():
            final_summary[movie]["details"].append({
                "city": d["city"],
                "state": d["state"],
                "venues": len(d["venues"]),
                "shows": d["shows"],
                "gross": round(d["gross"], 2),
                "sold": d["sold"],
                "totalSeats": d["totalSeats"],
                "fastfilling": d["fastfilling"],
                "housefull": d["housefull"],
                "occupancy": round((d["sold"] / d["totalSeats"]) * 100, 2) if d["totalSeats"] else 0.0
            })

        for c in m["Chain_details"].values():
            final_summary[movie]["Chain_details"].append({
                "chain": c["chain"],
                "venues": len(c["venues"]),
                "shows": c["shows"],
                "gross": round(c["gross"], 2),
                "sold": c["sold"],
                "totalSeats": c["totalSeats"],
                "fastfilling": c["fastfilling"],
                "housefull": c["housefull"],
                "occupancy": round((c["sold"] / c["totalSeats"]) * 100, 2) if c["totalSeats"] else 0.0
            })

    # =====================================================
    # SAVE FILES
    # =====================================================
    with open(DETAILED_FILE, "w", encoding="utf-8") as f:
        json.dump(detailed, f, indent=2, ensure_ascii=False)

    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        json.dump(final_summary, f, indent=2, ensure_ascii=False)

    log(f"✅ DONE | Shows={len(detailed)} | Movies={len(final_summary)}")
