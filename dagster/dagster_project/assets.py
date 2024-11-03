from dagster import asset
from dagster_project.resources import TMDBResource
import pandas as pd
import os

@asset
def now_playing_csv(tmdb_api: TMDBResource) -> None:
    now_playing = tmdb_api.get_now_playing(1).json()

    # Might need further pages in the future
    for page in range(2, 3):
        further_pages = tmdb_api.get_now_playing(page).json()
        now_playing["results"].extend(further_pages["results"])

    os.makedirs("data", exist_ok=True)
    df = pd.DataFrame(now_playing["results"])
    df.to_csv("data/nowplaying.csv")