import os
import pandas as pd
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from prefect import flow, task
from prefect_gcp import GcpCredentials
import pandas_gbq

# === ENVIRONMENT SETUP ===
@task
def debug_env():
    print("ACCESS_TOKEN:", os.getenv("ACCESS_TOKEN"))

def get_env_variable(key: str, required: bool = True) -> str:
    value = os.getenv(key)
    if required and not value:
        print(f"[WARNING] Environment variable '{key}' is missing.")
        return ""
    return value

# Gunakan getenv agar tidak error saat dijalankan di agent Prefect yang tidak punya .env
ACCESS_TOKEN = get_env_variable("ACCESS_TOKEN")
REFRESH_TOKEN = get_env_variable("REFRESH_TOKEN")
CLIENT_ID = get_env_variable("CLIENT_ID")
PROJECT_ID = get_env_variable("PROJECT_ID")
DATASET_ID = get_env_variable("DATASET_ID")
tz = ZoneInfo("Asia/Jakarta")

# GCP credentials
CREDENTIALS = GcpCredentials.load("gcp-credentials").get_credentials_from_service_account()

# === HELPER ===
def refresh_access_token(client_id: str, refresh_token: str) -> str:
    url = "https://myanimelist.net/v1/oauth2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    res = requests.post(url, data=data, headers=headers)
    if res.status_code == 200:
        print("🔁 Token refreshed successfully.")
        return res.json().get("access_token")
    else:
        raise Exception(f"Failed to refresh token: {res.text}")

def fetch_anime_ranking(ranking_type: str, access_token: str) -> list[dict]:
    url = (
        f"https://api.myanimelist.net/v2/anime/ranking?"
        f"ranking_type={ranking_type}&limit=50&fields="
        "id,title,mean,rank,popularity,num_list_users,num_scoring_users," 
        "status,start_date,end_date,num_episodes,genres,studios"
    )
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 401:
        raise PermissionError("Expired token")
    response.raise_for_status()
    return response.json().get("data", [])

# === TASK DEFINITIONS ===
@task(name="extract-anime-ranking", tags=["extract"], log_prints=True)
def extract_anime_data(ranking_types: list[str]) -> list[dict]:
    global ACCESS_TOKEN
    combined_records = []
    ranking_date = datetime.now(tz).date()

    for ranking_type in ranking_types:
        try:
            data = fetch_anime_ranking(ranking_type, ACCESS_TOKEN)
        except PermissionError:
            # Token expired, refresh
            ACCESS_TOKEN = refresh_access_token(CLIENT_ID, REFRESH_TOKEN)
            data = fetch_anime_ranking(ranking_type, ACCESS_TOKEN)
        except Exception as e:
            print(f"❌ Gagal ambil data '{ranking_type}' - {e}")
            continue

        for item in data:
            anime = item["node"]
            genre_names = [g["name"] for g in anime.get("genres", []) if g.get("name")]
            studio_names = [s["name"] for s in anime.get("studios", []) if s.get("name")]

            combined_records.append({
                "id": anime.get("id"),
                "title": anime.get("title"),
                "mean": anime.get("mean"),
                "rank": anime.get("rank"),
                "popularity": anime.get("popularity"),
                "num_list_users": anime.get("num_list_users"),
                "num_scoring_users": anime.get("num_scoring_users"),
                "status": anime.get("status"),
                "start_date": anime.get("start_date"),
                "end_date": anime.get("end_date"),
                "num_episodes": anime.get("num_episodes"),
                "genres": ", ".join(genre_names) if genre_names else None,
                "studios": ", ".join(studio_names) if studio_names else None,
                "ranking_type": ranking_type,
                "ranking_date": ranking_date,
            })
    return combined_records

@task(name="transform-anime-data", tags=["transform"], log_prints=True)
def transform_to_dataframe(data: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(data)

@task(name="load-anime-to-bigquery", tags=["load"], log_prints=True)
def load_to_bigquery(df: pd.DataFrame, table_name: str, if_exists: str = "append"):
    pandas_gbq.to_gbq(
        dataframe=df,
        destination_table=f"{DATASET_ID}.{table_name}",
        project_id=PROJECT_ID,
        credentials=CREDENTIALS,
        if_exists=if_exists
    )

# === MAIN FLOW ===
@flow(name="mal-etl-mainflow", flow_run_name="mal-etl-run-{datetime}", log_prints=True)
def mal_etl_mainflow():
    ranking_types = ["all", "airing", "upcoming"]
    raw_data = extract_anime_data(ranking_types=ranking_types)
    df = transform_to_dataframe(raw_data)
    load_to_bigquery(df, table_name="mal_anime_ranking")

if __name__ == "__main__":
    mal_etl_mainflow()