import os
import pandas as pd
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from prefect import flow, task
from prefect_gcp import GcpCredentials
import pandas_gbq

# === ENVIRONMENT SETUP ===
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
tz = ZoneInfo("Asia/Jakarta")

# Load GCP Credentials (from Prefect Block)
CREDENTIALS = GcpCredentials.load("gcp-credentials").get_credentials_from_service_account_info()

# === TASK DEFINITIONS ===
@task(name="extract-anime-ranking", tags=["extract"], log_prints=True)
def extract_anime_data(ranking_types: list[str], limit: int = 50) -> list[dict]:
    combined_records = []
    ranking_date = datetime.now(tz).date()

    for ranking_type in ranking_types:
        url = (
            f"https://api.myanimelist.net/v2/anime/ranking?"
            f"ranking_type={ranking_type}&limit={limit}&fields="
            "id,title,mean,rank,popularity,num_list_users,num_scoring_users,"
            "status,start_date,end_date,num_episodes,genres,studios"
        )
        headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json().get("data", [])
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
                    "ranking_date": ranking_date
                })
        else:
            print(f"Gagal ambil data kategori '{ranking_type}' - status: {response.status_code}")
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
def mal_etl_mainflow(datetime: str = datetime.now(tz).strftime('%Y%m%d-%H%M%S')):
    ranking_types = ["all", "airing", "upcoming"]
    raw_data = extract_anime_data(ranking_types=ranking_types)
    df = transform_to_dataframe(raw_data)
    load_to_bigquery(df, table_name="mal_anime_ranking")

# === SCRIPT ENTRYPOINT ===
if __name__ == "__main__":
    mal_etl_mainflow()
