import requests

from kacky_records_api.record_aggregators.nadeo.authentication import (
    AuthenticationHandler,
)


class NadeoServices:
    def __init__(self, user: str, pwd: str, accounttype: str):
        self._auth_handler = AuthenticationHandler(user, pwd, str)

    def _request_executor(self, url):
        self._auth_handler.services_auth_status()
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"nadeo_v1 t={self._auth_handler.token['NadeoLiveServices']}",
        }
        result = requests.get(url, headers=headers)
        return result.json()

    def get_leaders_for_map(self, mapuid):
        zone = None

        leaders = self._request_executor(
            f"https://live-services.trackmania.nadeo.live/api/token/leaderboard/group/Personal_Best/map/{mapuid}/top"
        )

        for top in leaders["tops"]:
            if top["zoneName"] == "World":
                zone = top["top"]
                break

        for rec in zone:
            if rec["position"] == 1:
                return {
                    "mapuid": leaders["mapUid"],
                    "accountId": zone["accountId"],
                    "score": zone["score"],
                }
        return {}
