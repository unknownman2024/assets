#!/usr/bin/env python3
"""
marketstrip.py

Generates market strip JSON (data/marketstrip.json) from daily aggregated data.
Compares current and previous day's show grosses up to a dynamically rounded cutoff time.
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from zoneinfo import ZoneInfo

# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------
DAILY_DATA_DIR = Path("daily/data")
OUTPUT_FILE = Path("data/marketstrip.json")
IST = ZoneInfo("Asia/Kolkata")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %Z",
)
logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def get_ist_today() -> datetime.date:
    """Return today's date in IST."""
    return datetime.now(IST).date()


def round_to_nearest_hour(time_str: str) -> str:
    """
    Parse a time string like '19:08 IST' or '18:29 IST' and round to the nearest hour.
    Returns cutoff time as "HH:00" (24-hour format).
    """
    # Extract HH:MM from the string (ignores date and timezone)
    match = re.search(r"(\d{1,2}):(\d{2})", time_str)
    if not match:
        raise ValueError(f"Could not parse time from: {time_str}")
    h, m = int(match.group(1)), int(match.group(2))
    # Round to nearest hour (minute >= 30 -> round up)
    if m >= 30:
        h = (h + 1) % 24
    return f"{h:02d}:00"


def parse_time_to_minutes(time_str: str) -> int:
    """
    Convert a time string like '04:30 PM' to minutes since midnight.
    """
    time_str = time_str.strip()
    dt = datetime.strptime(time_str, "%I:%M %p")
    return dt.hour * 60 + dt.minute


def parse_movie_label(raw: str) -> Tuple[str, str]:
    """
    Parse "Movie Name [FORMAT | LANGUAGE]" into (movie_name, language).
    Returns (movie_name.strip(), language.strip()).
    """
    match = re.match(r"^(.*?)\s*\[.*?\|\s*([^\]]+)\]$", raw)
    if not match:
        logger.warning(f"Could not parse movie label: '{raw}', using raw as movie name")
        return raw.strip(), ""
    movie = match.group(1).strip()
    lang = match.group(2).strip()
    return movie, lang


def format_indian_currency(amount: int) -> str:
    """
    Format amount in Indian numbering style with suffix: Cr, L, K, or plain.
    Examples:
        32300000 -> ₹3.23Cr
        1233000  -> ₹12.33L
        5800     -> ₹5.8K
        821      -> ₹821
    """
    if amount >= 10_000_000:  # 1 Cr
        val = amount / 10_000_000
        return f"₹{val:.2f}Cr".rstrip('0').rstrip('.') if val % 1 != 0 else f"₹{val:.0f}Cr"
    elif amount >= 100_000:  # 1 Lakh
        val = amount / 100_000
        return f"₹{val:.2f}L".rstrip('0').rstrip('.') if val % 1 != 0 else f"₹{val:.0f}L"
    elif amount >= 1000:  # 1 Thousand
        val = amount / 1000
        return f"₹{val:.1f}K".rstrip('0').rstrip('.') if val % 1 != 0 else f"₹{val:.0f}K"
    else:
        return f"₹{amount}"


# ----------------------------------------------------------------------
# Data loading & aggregation
# ----------------------------------------------------------------------
def load_daily_data(date_obj: datetime.date) -> Optional[Dict[str, Any]]:
    """
    Load finaldetailed.json for a given date.
    Returns dict with 'last_updated' and 'data' list, or None if not found.
    """
    date_str = date_obj.strftime("%Y%m%d")
    file_path = DAILY_DATA_DIR / date_str / "finaldetailed.json"
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        return None
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return None


def aggregate_movies(
    data: Dict[str, Any], cutoff_minutes: int
) -> Tuple[Dict[str, Tuple[str, int, int]], int]:
    """
    Aggregate shows from a day's data, filtering by time <= cutoff_minutes.
    Returns:
        - dict: key = "MovieName Language" -> (label, total_gross, show_count)
        - int: number of records after filtering
    """
    if not data or "data" not in data:
        return {}, 0

    agg: Dict[str, Tuple[str, int, int]] = {}
    filtered_count = 0

    for record in data["data"]:
        time_str = record.get("time", "")
        if not time_str:
            continue
        try:
            show_minutes = parse_time_to_minutes(time_str)
        except Exception:
            logger.warning(f"Skipping record with invalid time: {time_str}")
            continue

        if show_minutes > cutoff_minutes:
            continue

        filtered_count += 1
        movie_raw = record.get("movie", "")
        gross = record.get("gross", 0)

        movie, lang = parse_movie_label(movie_raw)
        key = f"{movie} {lang}".strip()
        label = key

        if key in agg:
            old_label, old_gross, old_count = agg[key]
            agg[key] = (label, old_gross + gross, old_count + 1)
        else:
            agg[key] = (label, gross, 1)

    return agg, filtered_count


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    start_time = time.time()
    logger.info("Starting market strip generation")

    today = get_ist_today()
    yesterday = today - timedelta(days=1)

    logger.info(f"Current IST date: {today.isoformat()}")

    # Load current day
    current_data = load_daily_data(today)
    if not current_data:
        logger.error("Current day data missing. Aborting.")
        return 1

    # Determine cutoff from current day's last_updated
    last_updated = current_data.get("last_updated", "")
    if not last_updated:
        logger.error("Current data missing 'last_updated' field.")
        return 1

    cutoff_str = round_to_nearest_hour(last_updated)  # e.g. "19:00"
    # Convert to minutes since midnight
    hh, mm = map(int, cutoff_str.split(":"))
    cutoff_minutes = hh * 60 + mm

    logger.info(f"Cutoff time (rounded): {cutoff_str}")

    # Aggregate current day
    current_agg, current_filtered = aggregate_movies(current_data, cutoff_minutes)
    logger.info(f"Current day: loaded {len(current_data.get('data', []))} records, "
                f"kept {current_filtered} after cutoff")

    # Load previous day
    prev_data = load_daily_data(yesterday)
    if prev_data:
        prev_agg, prev_filtered = aggregate_movies(prev_data, cutoff_minutes)
        logger.info(f"Previous day: loaded {len(prev_data.get('data', []))} records, "
                    f"kept {prev_filtered} after cutoff")
    else:
        prev_agg = {}
        logger.info("Previous day data not found. Using empty set.")

    # Rank current day by gross descending, take top 30
    sorted_current = sorted(
        current_agg.items(),
        key=lambda item: item[1][1],  # gross
        reverse=True
    )[:30]

    items = []
    for key, (label, curr_gross, curr_shows) in sorted_current:
        # Previous gross
        prev_gross = prev_agg.get(key, (None, 0, 0))[1]

        # Percentage change
        if prev_gross == 0 and curr_gross > 0:
            pct_change = 100
            trend = "up"
        elif prev_gross == 0 and curr_gross == 0:
            pct_change = 0
            trend = "flat"
        else:
            pct_change = ((curr_gross - prev_gross) / prev_gross) * 100
            if pct_change > 0:
                trend = "up"
            elif pct_change < 0:
                trend = "down"
            else:
                trend = "flat"

        # Round to integer
        pct_int = int(round(pct_change))

        # Format gross and shows
        gross_formatted = format_indian_currency(curr_gross)
        shows_formatted = f"{curr_shows:,}"

        items.append({
            "label": label,
            "value": f"{pct_int:+}%".replace("+", "") if pct_int != 0 else "0%",
            "trend": trend,
            "gross": gross_formatted,
            "shows": shows_formatted,
        })

    # Write output
    output = {"items": items}
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    elapsed = time.time() - start_time
    logger.info(f"Market strip saved to {OUTPUT_FILE} ({len(items)} items)")
    logger.info(f"Total execution time: {elapsed:.2f}s")
    return 0


if __name__ == "__main__":
    exit(main())
