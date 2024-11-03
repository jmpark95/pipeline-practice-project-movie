from dagster import Definitions, load_assets_from_modules, EnvVar
from dagster_project import assets
from dagster_project.resources import TMDBResource

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "tmdb_api": TMDBResource(api_key=EnvVar("API_KEY"), api_token=EnvVar("API_TOKEN"))
    },
)
