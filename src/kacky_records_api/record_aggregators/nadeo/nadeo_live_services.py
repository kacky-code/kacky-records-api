import requests

from kacky_records_api.record_aggregators.nadeo.authentication import (
    AuthenticationHandler,
)


class NadeoLiveServices:
    def __init__(self, user: str, pwd: str, accounttype: str, user_agent: str):
        self._auth_handler = AuthenticationHandler(user, pwd, accounttype, user_agent)

    def _request_executor(self, url):
        self._auth_handler.services_auth_status()
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": self._auth_handler.useragent,
            "Authorization": f"nadeo_v1 t={self._auth_handler.tokens['NadeoLiveServices'].access_token}",
        }
        result = requests.get(url, headers=headers)
        return result.json()

    def get_leaders_for_map(self, mapuid):
        url = f"https://live-services.trackmania.nadeo.live/api/token/leaderboard/group/Personal_Best/map/{mapuid}/top"
        leaders = self._request_executor(url)
        return leaders

    def get_worldrecord_for_map(self, mapuid):
        leaders = self.get_leaders_for_map(mapuid)
        zone = None
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

    def get_club_activities(
        self, club_id: int, get_inactive: bool = False, length: int = 64
    ):
        if not (
            isinstance(club_id, int)
            and isinstance(get_inactive, bool)
            and isinstance(length, int)
            and length > 1
        ):
            raise ValueError("Invalid parameter!")

        url = (
            f"https://live-services.trackmania.nadeo.live/api/token/club/"
            f"{club_id}/activity?offset=0&length={length}&active={0 if get_inactive else 1}"
        )
        activities = self._request_executor(url)
        return activities

    def get_club_campaigns(
        self, club_id: int, get_inactive: bool = False, length: int = 64
    ):
        activities = self.get_club_activities(club_id, get_inactive, length)

        campaigns = []

        for activity in activities["activityList"]:
            if activity["activityType"] == "campaign":
                campaigns.append(activity)
        return campaigns

    def get_club_rooms(
        self, club_id: int, get_inactive: bool = False, length: int = 64
    ):
        activities = self.get_club_activities(club_id, get_inactive, length)

        rooms = []

        for activity in activities["activityList"]:
            if activity["activityType"] == "room":
                rooms.append(activity)
        return rooms

    def get_club_skin_uploads(
        self, club_id: int, get_inactive: bool = False, length: int = 64
    ):
        activities = self.get_club_activities(club_id, get_inactive, length)

        skins = []

        for activity in activities["activityList"]:
            if activity["activityType"] == "skin-upload":
                skins.append(activity)
        return skins

    def get_club_map_uploads(
        self, club_id: int, get_inactive: bool = False, length: int = 64
    ):
        activities = self.get_club_activities(club_id, get_inactive, length)

        maps = []

        for activity in activities["activityList"]:
            if activity["activityType"] == "map-upload":
                maps.append(activity)
        return maps

    def get_campaign(self, club_id: int, campaign_id: int):
        url = f"https://live-services.trackmania.nadeo.live/api/token/club/{club_id}/campaign/{campaign_id}"
        campaign_info = self._request_executor(url)
        return campaign_info
