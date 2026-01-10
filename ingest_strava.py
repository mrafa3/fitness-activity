import os
import time
import requests
import duckdb
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")

DB_PATH = "data/fitness.duckdb"

def get_access_token():
    resp = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": REFRESH_TOKEN,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def fetch_activities(access_token, page=1, per_page=200):
    resp = requests.get(
        "https://www.strava.com/api/v3/athlete/activities",
        headers={"Authorization": f"Bearer {access_token}"},
        params={"page": page, "per_page": per_page},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()

def main():
    os.makedirs("data", exist_ok=True)

    print("Refreshing access token...")
    access_token = get_access_token()

    print("Connecting to DuckDB...")
    con = duckdb.connect(DB_PATH)

    con.execute("""
        CREATE TABLE IF NOT EXISTS strava_activities (
            strava_id BIGINT PRIMARY KEY,
            name TEXT,
            type TEXT,
            start_date TIMESTAMP,
            elapsed_time_sec INTEGER,
            moving_time_sec INTEGER
        )
    """)

    page = 1
    total = 0

    print("Fetching activities from Strava...")

    while True:
        activities = fetch_activities(access_token, page=page)

        if not activities:
            break

        print(f"Page {page}: {len(activities)} activities")

        for a in activities:
            con.execute(
                """
                INSERT OR REPLACE INTO strava_activities VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    a["id"],
                    a.get("name"),
                    a.get("type"),
                    a.get("start_date"),
                    a.get("elapsed_time"),
                    a.get("moving_time"),
                ],
            )
            total += 1

        page += 1
        time.sleep(0.2)  # be nice to Strava

    print(f"Done. Upserted {total} activities.")

if __name__ == "__main__":
    main()