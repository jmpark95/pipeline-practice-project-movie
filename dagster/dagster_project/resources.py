from dagster import ConfigurableResource
import requests
from requests import Response

class TMDBResource(ConfigurableResource):
    api_key: str
    api_token: str

    def get_now_playing(self, page: str) -> Response:
        return requests.get(
            f"https://api.themoviedb.org/3/movie/now_playing?language=en-US&page={page}",
            headers = {
                "accept": "application/json",
                "Authorization": f"Bearer {self.api_token}"
            },
        )