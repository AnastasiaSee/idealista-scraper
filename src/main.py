import requests
import pandas as pd
import os
import time
import json
import subprocess
import base64
from dotenv import load_dotenv
import config

load_dotenv()

CLIENT_ID = os.getenv("IDEALISTA_CLIENT_ID")
CLIENT_SECRET = os.getenv("IDEALISTA_CLIENT_SECRET")


# -----------------------------
# TOKEN (via curl)
# -----------------------------
def get_access_token():

    credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"
    encoded = base64.b64encode(credentials.encode()).decode()

    result = subprocess.run(
        [
            "curl",
            "-X", "POST",
            "https://api.idealista.com/oauth/token",
            "-H", f"Authorization: Basic {encoded}",
            "-d", "grant_type=client_credentials"
        ],
        capture_output=True,
        text=True
    )

    data = json.loads(result.stdout)

    token = data["access_token"]

    print("✅ Token received")

    return token


# -----------------------------
# FETCH DATA
# -----------------------------
def fetch_properties():

    token = get_access_token()

    url = f"https://api.idealista.com/3.5/{config.COUNTRY}/search"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
    }

    page = 1
    all_properties = []

    while True:

        payload = {
            "operation": config.OPERATION,
            "propertyType": config.PROPERTY_TYPE,
            "center": config.CENTER,
            "distance": config.DISTANCE,
            "maxItems": config.MAX_ITEMS,
            "numPage": page,
            "language": "es"
        }

        print(f"📄 Requesting page {page}")

        response = requests.post(url, headers=headers, data=payload)

        print("STATUS:", response.status_code)

        if response.status_code != 200:
            print("❌ API error")
            print(response.text)
            break

        data = response.json()

        properties = data.get("elementList", [])

        if not properties:
            break

        all_properties.extend(properties)

        total_pages = data.get("totalPages", 1)

        print(f"Collected {len(all_properties)} properties")

        if page >= total_pages:
            break

        page += 1

        time.sleep(1)

    return all_properties


# -----------------------------
# SAVE
# -----------------------------
def save(properties):

    if not properties:
        print("⚠️ No properties collected")
        return

    df = pd.json_normalize(properties)

    os.makedirs("data", exist_ok=True)

    file = "data/idealista_data.csv"

    df.to_csv(file, index=False)

    print(f"💾 Saved {len(df)} properties")

def save(properties):

    if not properties:
        print("⚠️ No properties collected")
        return

    new_data = pd.json_normalize(properties)

    os.makedirs("data", exist_ok=True)
    file = "projects/idealista-scraper/data/idealista_data.csv"

    if os.path.exists(file):
        old_data = pd.read_csv(file)
        df = pd.concat([old_data, new_data], ignore_index=True)
        df = df.drop_duplicates(subset="propertyCode")  # ключ Idealista
    else:
        df = new_data

    df.to_csv(file, index=False)

    print(f"💾 Total saved: {len(df)} properties")

# -----------------------------
# MAIN
# -----------------------------
def main():

    properties = fetch_properties()

    save(properties)


if __name__ == "__main__":
    main()