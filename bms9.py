import json
import os
import hashlib
from datetime import datetime, timedelta
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
import pytz

# ============================================================
# CONFIG
# ============================================================
SHARD_ID = 9
SOURCE_BASE_URL = "https://districtdata2026.pages.dev/advance"
DISTRICT_VENUES_FILE = "districtvenues.json"
REQUEST_TIMEOUT = 30
CUTOFF_MINUTES = 20000
IST = pytz.timezone("Asia/Kolkata")

# ============================================================
# DATE / PATHS
# ============================================================
NOW_IST = datetime.now(IST) + timedelta(days=1)
DATE_CODE = NOW_IST.strftime("%Y%m%d")
DATE_DISTRICT = NOW_IST.strftime("%Y-%m-%d")
BASE_DIR = os.path.join("advance", "data", DATE_CODE)
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
DETAILED_FILE = os.path.join(BASE_DIR, f"detailed{SHARD_ID}.json")
SUMMARY_FILE = os.path.join(BASE_DIR, f"movie_summary{SHARD_ID}.json")
LOG_FILE = os.path.join(LOG_DIR, f"districtadvance{SHARD_ID}.log")

# ============================================================
# LOGGING
# ============================================================
def log(message):
    timestamp = datetime.now(IST).strftime("%H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

# ============================================================
# JSON
# ============================================================
def load_json_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# ============================================================
# VENUE ID
# ============================================================
# districtvenues.json is now the ONLY venue source.
# Matching is strictly:
# districtvenues.json id → source Detailed.json row[2]
# No venue-name matching.
# ============================================================
def get_venue_id(venue):
    if not isinstance(venue, dict):
        return None
    value = (venue.get("id") if venue.get("id") is not None
             else venue.get("venueId") if venue.get("venueId") is not None
             else venue.get("venue_id") if venue.get("venue_id") is not None
             else venue.get("cinema_id") if venue.get("cinema_id") is not None
             else venue.get("cinemaId") if venue.get("cinemaId") is not None
             else None)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return str(value).strip()

# ============================================================
# LOAD SELECTED VENUES
# ============================================================
# NO alldistrictvenues.json, NO master lookup, NO name matching.
# The selected venue ID is the source of truth.
# ============================================================
def load_selected_venues():
    district_venues = load_json_file(DISTRICT_VENUES_FILE)
    if not isinstance(district_venues, list):
        raise ValueError("districtvenues.json must contain an array")
    selected_by_id = {}
    missing_ids = 0
    duplicate_ids = 0
    for venue in district_venues:
        if not isinstance(venue, dict):
            continue
        venue_id = get_venue_id(venue)
        if venue_id is None:
            missing_ids += 1
            continue
        key = str(venue_id)
        if key in selected_by_id:
            duplicate_ids += 1
        selected_by_id[key] = venue
    log(f"📍 Selected venue records: {len(district_venues)}")
    log(f"🎯 Selected venue IDs: {len(selected_by_id)}")
    if missing_ids:
        log(f"⚠️ Selected venues without ID: {missing_ids}")
    if duplicate_ids:
        log(f"⚠️ Duplicate venue IDs: {duplicate_ids}")
    return selected_by_id

# ============================================================
# STATE / CHAIN FORMATTING
# ============================================================
def format_state(value):
    if not value:
        return "Unknown"
    value = str(value)
    if value.isupper():
        return value
    return " ".join(word if word.isupper() else word.capitalize() for word in value.replace("-", " ").split())

def format_chain(value):
    if not value:
        return "Unknown"
    value = str(value)
    if value.isupper():
        return value
    return " ".join(word if word.isupper() else word.capitalize() for word in value.replace("-", " ").split())

# ============================================================
# MOVIE
# ============================================================
def normalize_movie_name(value):
    if value is None:
        return ""
    return " ".join(str(value).split()).strip()

def build_movie_key(movie_name, language):
    movie_name = normalize_movie_name(movie_name)
    language = str(language or "").strip() or "Unknown"
    return f"{movie_name} [2D | {language}]"

# ============================================================
# SESSION ID
# ============================================================
# Venue identity is now the REAL venue/cinema ID.
# ============================================================
def generate_session_id(movie, venue_id, time, audi):
    raw = "|".join([str(movie), str(venue_id), str(time), str(audi)])
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
    return f"DISTRICT_{digest}"

# ============================================================
# MINUTES LEFT
# ============================================================
def calculate_minutes_left(show_time):
    try:
        now = datetime.now(IST)
        t = datetime.strptime(show_time, "%I:%M %p")
        show_dt = now.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
        if show_dt < (now - timedelta(hours=6)):
            show_dt += timedelta(days=1)
        return (show_dt - now).total_seconds() / 60
    except Exception:
        return 9999

# ============================================================
# FETCH SOURCE
# ============================================================
def fetch_source():
    url = f"{SOURCE_BASE_URL}/{DATE_DISTRICT}_Detailed.json"
    log("📡 Fetching Detailed JSON:")
    log(f"   {url}")
    request = Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; BMS9/1.0)"})
    try:
        with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
            raw = response.read()
        data = json.loads(raw.decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("Source JSON is not an object")
        if "dicts" not in data:
            raise ValueError("Missing dicts")
        if "movies" not in data:
            raise ValueError("Missing movies")
        log("✅ Source loaded")
        log(f"   Source date: {data.get('date')}")
        log(f"   Last updated: {data.get('lastUpdated')}")
        log(f"   Movie keys: {len(data.get('movies', {}))}")
        return data
    except HTTPError as e:
        if e.code == 404:
            log("❌ Source file not found (404) – will create empty outputs")
        else:
            log(f"❌ HTTP {e.code} – will create empty outputs")
        return None
    except URLError as e:
        log(f"❌ URL error: {e.reason} – will create empty outputs")
        return None
    except Exception as e:
        log(f"❌ Source error: {type(e).__name__}: {e} – will create empty outputs")
        return None

# ============================================================
# REVERSE DICTIONARY
# ============================================================
def reverse_dictionary(dictionary):
    if not isinstance(dictionary, dict):
        return {}
    result = {}
    for key, value in dictionary.items():
        try:
            numeric_value = int(value)
        except (TypeError, ValueError):
            continue
        result[numeric_value] = key
    return result

# ============================================================
# BUILD REVERSE DICTS
# ============================================================
def build_reverse_dicts(source):
    dicts = source.get("dicts", {})
    return {
        "cities": reverse_dictionary(dicts.get("cities", {})),
        "states": reverse_dictionary(dicts.get("states", {})),
        "venues": reverse_dictionary(dicts.get("venues", {})),
        "chains": reverse_dictionary(dicts.get("chains", {})),
        "showtimes": reverse_dictionary(dicts.get("showtimes", {})),
        "audis": reverse_dictionary(dicts.get("audis", {}))
    }

# ============================================================
# DECOMPRESS SHOW
# ============================================================
# EXACT SOURCE ARRAY:
# [0] cityId, [1] stateId, [2] venueId (REAL CINEMA ID), [3] chainId,
# [4] timeId, [5] audiId, [6] totalSeats, [7] available, [8] sold,
# [9] gross * 100, [10] occupancy * 100, [11] minsLeft
# ============================================================
def decompress_show(row, reverse):
    if not isinstance(row, list) or len(row) < 12:
        return None
    try:
        city_id, state_id, venue_id, chain_id, time_id, audi_id = row[0], row[1], row[2], row[3], row[4], row[5]
        total = int(row[6] or 0)
        available = int(row[7] or 0)
        sold = int(row[8] or 0)
        gross_cents = int(row[9] or 0)
        occupancy_raw = int(row[10] or 0)
        mins_left = float(row[11] or 0)
        return {
            "city": reverse["cities"].get(city_id, "Unknown"),
            "state": reverse["states"].get(state_id, "Unknown"),
            "venue": reverse["venues"].get(venue_id, "Unknown"),
            "venue_id": venue_id,
            "chain": reverse["chains"].get(chain_id, "Unknown"),
            "time": reverse["showtimes"].get(time_id, ""),
            "audi": reverse["audis"].get(audi_id, ""),
            "totalSeats": total,
            "available": available,
            "sold": sold,
            "gross": gross_cents / 100,
            "occupancy": occupancy_raw / 100,
            "minsLeft": mins_left
        }
    except Exception:
        return None

# ============================================================
# PARSE SOURCE
# ============================================================
# THIS IS THE CORE CHANGE.
# No source venue name lookup, no master venue lookup, no name normalization.
# Match: source row[2] == districtvenues.json id
# ============================================================
def parse_source(source, selected_venues, reverse):
    detailed = []
    movies = source.get("movies", {})
    total_movie_keys = len(movies)
    matched_rows = 0
    ignored_rows = 0
    missing_source_ids = set()
    for raw_movie_key, rows in movies.items():
        if not isinstance(rows, list):
            continue
        # Source movie key: "Movie | Language" (no format handling)
        if "|" in raw_movie_key:
            parts = [p.strip() for p in raw_movie_key.split("|")]
            movie_name = parts[0] if parts else raw_movie_key
            language = parts[-1] if len(parts) > 1 else "Unknown"
        else:
            movie_name = raw_movie_key.strip()
            language = "Unknown"
        movie_key = build_movie_key(movie_name, language)
        for compressed in rows:
            show = decompress_show(compressed, reverse)
            if not show:
                continue
            source_venue_id = show["venue_id"]
            selected_venue = selected_venues.get(str(source_venue_id))
            if selected_venue is None:
                ignored_rows += 1
                missing_source_ids.add(str(source_venue_id))
                continue
            matched_rows += 1
            # Venue data comes from districtvenues.json
            city = selected_venue.get("city") or "Unknown"
            state = format_state(selected_venue.get("state"))
            venue = str(selected_venue.get("name", show.get("venue", "Unknown Venue")) or "Unknown Venue").strip()
            address = str(selected_venue.get("address", "") or "")
            chain = format_chain(selected_venue.get("chainKey"))
            time = str(show.get("time", "") or "").strip()
            audi = str(show.get("audi", "") or "")
            mins_left = calculate_minutes_left(time)
            if mins_left > CUTOFF_MINUTES:
                continue
            total = int(show.get("totalSeats", 0) or 0)
            available = int(show.get("available", 0) or 0)
            sold = total - available
            if sold < 0:
                sold = 0
            gross = float(show.get("gross", 0) or 0)
            session_id = generate_session_id(movie_key, source_venue_id, time, audi)
            detailed.append({
                "movie": movie_key,
                "city": city,
                "state": state,
                "venue": venue,
                "venue_id": source_venue_id,
                "address": address,
                "time": time,
                "audi": audi,
                "session_id": session_id,
                "totalSeats": total,
                "available": available,
                "sold": sold,
                "gross": round(gross, 2),
                "minsLeft": round(mins_left, 1),
                "source": "District",
                "date": DATE_CODE,
                "chain": chain
            })
    log(f"🎬 Source movie keys: {total_movie_keys}")
    log(f"🎟️ Selected-venue show rows: {matched_rows}")
    log(f"🚫 Non-selected show rows ignored: {ignored_rows}")
    if missing_source_ids:
        log(f"ℹ️ Non-selected venue IDs encountered: {len(missing_source_ids)}")
        sample = list(missing_source_ids)[:20]
        for venue_id in sample:
            log(f"   ⏭️ Source venue ID not selected: {venue_id}")
        if len(missing_source_ids) > 20:
            log(f"   ... and {len(missing_source_ids) - 20} more")
    return detailed

# ============================================================
# SHOW KEY
# ============================================================
# Venue identity is ID-based. No venue-name matching.
# ============================================================
def show_key(row):
    return (str(row.get("venue_id", "")), row.get("time"), row.get("session_id"), row.get("audi"))

# ============================================================
# LOAD OLD DETAILED
# ============================================================
def load_old_detailed():
    if not os.path.exists(DETAILED_FILE):
        return []
    try:
        with open(DETAILED_FILE, "r", encoding="utf-8") as f:
            old = json.load(f)
        if isinstance(old, list):
            return old
    except Exception as e:
        log(f"⚠️ Could not load old detailed file: {e}")
    return []

# ============================================================
# MERGE WITHOUT DELETING
# ============================================================
def merge_without_deleting(fresh):
    old_rows = load_old_detailed()
    old_map = {show_key(row): row for row in old_rows if isinstance(row, dict)}
    new_map = {}
    # Fresh shows
    for row in fresh:
        key = show_key(row)
        if key in old_map:
            old_map[key].update({
                "totalSeats": row["totalSeats"],
                "available": row["available"],
                "sold": row["sold"],
                "gross": row["gross"],
                "minsLeft": row["minsLeft"]
            })
            old_map[key]["venue_id"] = row.get("venue_id")
            new_map[key] = old_map[key]
        else:
            new_map[key] = row
    # Keep old missing shows
    for key, row in old_map.items():
        if key not in new_map:
            new_map[key] = row
    merged = list(new_map.values())
    log(f"📦 Previous shows: {len(old_rows)}")
    log(f"🆕 Fresh shows: {len(fresh)}")
    log(f"📦 Final shows: {len(merged)}")
    return merged

# ============================================================
# BUILD SUMMARY
# ============================================================
def build_summary(detailed):
    summary = {}
    for row in detailed:
        movie = row.get("movie", "Unknown")
        city = row.get("city", "Unknown")
        venue_id = row.get("venue_id")
        total = int(row.get("totalSeats", 0) or 0)
        sold = int(row.get("sold", 0) or 0)
        gross = float(row.get("gross", 0) or 0)
        occupancy = (sold / total * 100) if total else 0
        if movie not in summary:
            summary[movie] = {
                "shows": 0,
                "gross": 0.0,
                "sold": 0,
                "totalSeats": 0,
                "venues": set(),
                "cities": set(),
                "fastfilling": 0,
                "housefull": 0
            }
        m = summary[movie]
        m["shows"] += 1
        m["gross"] += gross
        m["sold"] += sold
        m["totalSeats"] += total
        if venue_id is not None:
            m["venues"].add(str(venue_id))
        m["cities"].add(city)
        if occupancy >= 98:
            m["housefull"] += 1
        elif occupancy >= 50:
            m["fastfilling"] += 1
    return {
        movie: {
            "shows": m["shows"],
            "gross": round(m["gross"], 2),
            "sold": m["sold"],
            "totalSeats": m["totalSeats"],
            "venues": len(m["venues"]),
            "cities": len(m["cities"]),
            "fastfilling": m["fastfilling"],
            "housefull": m["housefull"],
            "occupancy": round((m["sold"] / m["totalSeats"] * 100) if m["totalSeats"] else 0.0, 2)
        }
        for movie, m in summary.items()
    }

# ============================================================
# SAVE JSON
# ============================================================
def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# ============================================================
# MAIN
# ============================================================
def main():
    log("🚀 DISTRICT → BMS9 CONVERTER STARTED")
    log(f"📅 Date: {DATE_DISTRICT}")
    log("🚫 Worker logic: DISABLED")
    log("🔑 Venue matching: ID ONLY")
    log("🚫 alldistrictvenues.json: NOT USED")
    log("🚫 Venue-name matching: NOT USED")

    selected_venues = load_selected_venues()

    source = fetch_source()
    if source is None:
        log("⚠️ No source data – keeping existing output files unchanged")
        if os.path.exists(DETAILED_FILE):
            log(f"📦 Existing detailed file preserved: {DETAILED_FILE}")
        else:
            save_json(DETAILED_FILE, [])
            log(f"📄 Created empty detailed file: {DETAILED_FILE}")
        if os.path.exists(SUMMARY_FILE):
            log(f"📦 Existing summary file preserved: {SUMMARY_FILE}")
        else:
            save_json(SUMMARY_FILE, {})
            log(f"📄 Created empty summary file: {SUMMARY_FILE}")
        log("✅ DONE | Existing data preserved")
        return

    reverse = build_reverse_dicts(source)
    fresh = parse_source(source, selected_venues, reverse)
    detailed = merge_without_deleting(fresh)
    summary = build_summary(detailed)
    save_json(DETAILED_FILE, detailed)
    save_json(SUMMARY_FILE, summary)
    log(f"✅ DONE | Shows={len(detailed)} | Movies={len(summary)}")
    log(f"📄 Detailed: {DETAILED_FILE}")
    log(f"📄 Summary: {SUMMARY_FILE}")

# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    main()
