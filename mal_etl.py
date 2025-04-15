import os
import pandas as pd
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from prefect import flow, task
from prefect_gcp import GcpCredentials
import pandas_gbq

tz = ZoneInfo("Asia/Jakarta")

# === ENVIRONMENT ===
def get_env_variable(key: str, required: bool = True) -> str:
    value = os.getenv(key)
    if required and not value:
        print(f"[WARNING] Environment variable '{key}' is missing.")
        return ""
    return value

@task(name="load-env", log_prints=True)
def load_env():
    return {
        "ACCESS_TOKEN": get_env_variable("ACCESS_TOKEN"),
        "REFRESH_TOKEN": get_env_variable("REFRESH_TOKEN"),
        "CLIENT_ID": get_env_variable("CLIENT_ID"),
        "PROJECT_ID": get_env_variable("PROJECT_ID"),
        "DATASET_ID": get_env_variable("DATASET_ID"),
    }

@task(name="load-gcp-credentials", log_prints=True)
def load_credentials():
    return GcpCredentials.load("gcp-credentials").get_credentials_from_service_account()

# === API HELPERS ===
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

# === TASKS ===
@task(name="transform-anime-data", log_prints=True)
def transform_to_dataframe(data: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(data)

    if df.empty:
        return df

    # Tipe data & parsing tanggal
    df["id"] = df["id"].astype("Int64")
    df["mean"] = df["mean"].astype("float64")
    df["rank"] = df["rank"].astype("Int64")
    df["popularity"] = df["popularity"].astype("Int64")
    df["num_list_users"] = df["num_list_users"].astype("Int64")
    df["num_scoring_users"] = df["num_scoring_users"].astype("Int64")
    df["num_episodes"] = df["num_episodes"].astype("Int64")
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")
    df["ranking_date"] = pd.to_datetime(df["ranking_date"])

    # 🔹 Tambahan transformasi
    df["start_year"] = df["start_date"].dt.year
    df["score_bucket"] = pd.cut(
        df["mean"],
        bins=[0, 6, 7, 8, 9, 10],
        labels=["<6", "6-6.9", "7-7.9", "8-8.9", "9+"],
        include_lowest=True
    )

    # 🔹 Normalisasi teks
    df["studios"] = df["studios"].str.lower().str.strip()
    df["genres"] = df["genres"].str.lower().str.strip()

    # 🔹 Pisah genres jadi list
    df["genre_list"] = df["genres"].str.split(",\s*")

    # 🔹 Explode genre jadi baris
    df = df.explode("genre_list")
    df = df.rename(columns={"genre_list": "genre"})

    return df

@task(name="load-anime-to-bigquery", log_prints=True)
def load_to_bigquery(df: pd.DataFrame, table_name: str, project_id: str, dataset_id: str, credentials, if_exists: str = "append"):
    print("DataFrame shape:", df.shape)
    print("DataFrame preview:\n", df.head())
    pandas_gbq.to_gbq(
        dataframe=df,
        destination_table=f"{dataset_id}.{table_name}",
        project_id=project_id,
        credentials=credentials,
        if_exists=if_exists
    )

@task(name="load-anime-to-bigquery", log_prints=True)
def load_to_bigquery(df: pd.DataFrame, table_name: str, project_id: str, dataset_id: str, credentials, if_exists: str = "append"):
    pandas_gbq.to_gbq(
        dataframe=df,
        destination_table=f"{dataset_id}.{table_name}",
        project_id=project_id,
        credentials=credentials,
        if_exists=if_exists
    )

# === MAIN FLOW ===
@flow(
    name="mal-etl-mainflow",
    flow_run_name=f"mal-etl-run-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
    log_prints=True
)
def mal_etl_mainflow():
    env = load_env()
    credentials = load_credentials()

    access_token = env["ACCESS_TOKEN"]
    refresh_token = env["REFRESH_TOKEN"]
    client_id = env["CLIENT_ID"]
    project_id = env["PROJECT_ID"]
    dataset_id = env["DATASET_ID"]

    ranking_types = ["all", "airing", "upcoming"]
    raw_data = extract_anime_data(ranking_types, access_token, refresh_token, client_id)
    df = transform_to_dataframe(raw_data)
    load_to_bigquery(df, table_name="mal_anime_ranking", project_id=project_id, dataset_id=dataset_id, credentials=credentials)

if __name__ == "__main__":
    mal_etl_mainflow()