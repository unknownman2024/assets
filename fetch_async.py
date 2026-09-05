import os
import json
import asyncio
import aiohttp
import uvloop
from datetime import datetime, timedelta

uvloop.install()

API_URL = "https://sacapi.text2024mail.workers.dev/?date="

START_DATE = datetime(2023, 4, 1)
END_DATE   = datetime(2025, 12, 18)

OUTPUT_DIR = "daily/olddata"
os.makedirs(OUTPUT_DIR, exist_ok=True)

CONCURRENCY = 50
RETRIES = 3

async def fetch(session, date_str):
    url = API_URL + date_str
    for attempt in range(RETRIES):
        try:
            async with session.get(url, timeout=15) as r:
                if r.status == 200:
                    return await r.json()
                else:
                    print(f"[{date_str}] HTTP {r.status}, retry...")
        except Exception as e:
            print(f"[{date_str}] Error: {e}")
        await asyncio.sleep(0.3 * (attempt + 1))
    return None

async def worker(date_str, session, sem):
    out_path = f"{OUTPUT_DIR}/{date_str}_summary.json"

    if os.path.exists(out_path):
        print(f"[skip] {date_str}")
        return

    async with sem:
        data = await fetch(session, date_str)
        if data is not None:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"[saved] {date_str}")
        else:
            print(f"[fail] {date_str}")

async def main():
    sem = asyncio.Semaphore(CONCURRENCY)

    date_list = []
    cur = START_DATE
    while cur <= END_DATE:
        date_list.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        tasks = [asyncio.create_task(worker(d, session, sem)) for d in date_list]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
