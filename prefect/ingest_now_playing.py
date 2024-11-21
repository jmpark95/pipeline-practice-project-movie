import os
import requests
import pandas as pd
from dotenv import load_dotenv
from prefect import flow, task
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse, bigquery_load_file

@task
def fetch_now_playing_from_tmdb(page: str) -> requests.Response:
    load_dotenv()
    api_token = os.getenv("API_TOKEN")
    api_key = os.getenv("API_KEY")

    return requests.get(
        f"https://api.themoviedb.org/3/movie/now_playing?language=en-US&page={page}",
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {api_token}"
            },
    )

@task
def api_to_csv():
    now_playing = fetch_now_playing_from_tmdb(1).json()

    # Might need further pages in the future
    for page in range(2, 3):
        further_pages = fetch_now_playing_from_tmdb(page).json()
        now_playing["results"].extend(further_pages["results"])

    os.makedirs("./prefect/data", exist_ok=True)
    df = pd.DataFrame(now_playing["results"])
    df.to_csv("./prefect/data/nowplaying.csv")


@flow
def load_csv_to_bigquery():
    api_to_csv()

    gcp_credentials = GcpCredentials.load("gcp-creds")
    client = gcp_credentials.get_bigquery_client()
    client.create_dataset("movie", exists_ok=True)

    with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
        operation = '''
            CREATE TABLE IF NOT EXISTS movie.now_playing (
                int64_field_0 INTEGER,
                adult BOOLEAN,
                backdrop_path STRING,
                genre_ids STRING,
                id INTEGER,
                original_language STRING,
                original_title STRING,
                overview STRING,
                popularity FLOAT64,
                poster_path STRING,
                release_date DATE,
                title STRING,
                video BOOLEAN,
                vote_average FLOAT64,
                vote_count INTEGER
            );

            IF EXISTS (SELECT * FROM movie.now_playing)
            THEN
            TRUNCATE TABLE movie.now_playing;
            END IF;
        '''
        warehouse.execute(operation)

    result = bigquery_load_file(
        dataset="movie",
        table="now_playing",
        path="./prefect/data/nowplaying.csv",
        gcp_credentials=gcp_credentials
    )
    return result

if __name__ == "__main__":
    load_csv_to_bigquery()