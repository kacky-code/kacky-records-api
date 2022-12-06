import base64
import datetime
import json

import requests
import yaml


class NadeoAPI:
    def __init__(self, user: str, pwd: str):
        self._credentials = (user, pwd)
        self._tokens = {
            "services": {"access": None, "refresh": None, "decode": None},
            "live": {"access": None, "refresh": None, "decode": None},
            "club": {"access": None, "refresh": None, "decode": None},
        }
        self._authenticate("all")

    def _authenticate(self, service: str, reauth: bool = False):
        if service == "NadeoServices" or service == "all":
            (
                self._tokens["services"]["access"],
                self._tokens["services"]["refresh"],
            ) = self._get_nadeo_auth_token("NadeoServices", reauth).values()
            # decode the payload (element[1] is payload, element[2] is signature
            self._tokens["services"]["decode"] = json.loads(
                base64.urlsafe_b64decode(
                    self._tokens["services"]["access"].split(".")[1]
                    + "="
                    * (4 - len(self._tokens["services"]["access"].split(".")[1]) % 4)
                )
            )

        if service == "NadeoLiveServices" or service == "all":
            (
                self._tokens["live"]["access"],
                self._tokens["live"]["refresh"],
            ) = self._get_nadeo_auth_token("NadeoLiveServices", reauth).values()
            # decode the payload (element[1] is payload, element[2] is signature
            self._tokens["live"]["decode"] = json.loads(
                base64.urlsafe_b64decode(
                    self._tokens["live"]["access"].split(".")[1]
                    + "=" * (4 - len(self._tokens["live"]["access"].split(".")[1]) % 4)
                )
            )

        if service == "NadeoClubServices" or service == "all":
            (
                self._tokens["club"]["access"],
                self._tokens["club"]["refresh"],
            ) = self._get_nadeo_auth_token("NadeoClubServices", reauth).values()
            # decode the payload (element[1] is payload, element[2] is signature
            self._tokens["club"]["decode"] = json.loads(
                base64.urlsafe_b64decode(
                    self._tokens["club"]["access"].split(".")[1]
                    + "=" * (4 - len(self._tokens["club"]["access"].split(".")[1]) % 4)
                )
            )

    def _check_execute_reauth(self):
        now = datetime.datetime.now().timestamp()
        if (
            self._tokens["services"]["decode"]["rat"]
            < now
            < self._tokens["services"]["decode"]["exp"]
        ):
            self._authenticate("NadeoServices", reauth=True)

        if (
            self._tokens["live"]["decode"]["rat"]
            < now
            < self._tokens["live"]["decode"]["exp"]
        ):
            self._authenticate("NadeoLiveServices", reauth=True)

        if (
            self._tokens["club"]["decode"]["rat"]
            < now
            < self._tokens["club"]["decode"]["exp"]
        ):
            self._authenticate("NadeoClubServices", reauth=True)

    def _get_ubi_auth_token(self):
        session = requests.Session()
        session.auth = self._credentials

        # TODO: take User-Agent from config
        headers = {
            "Ubi-AppId": "86263886-327a-4328-ac69-527f0d20a237",
            "Content-Type": "application/json",
            "User-Agent": "Kacky WR Tracker / cork@dingens.me",
        }

        r = session.post(
            "https://public-ubiservices.ubi.com/v3/profiles/sessions", headers=headers
        )

        if r.status_code != 200:
            raise ValueError("Authentication to Ubisoft Services failed!")
        return r.json()["ticket"]

    def _get_nadeo_auth_token(self, service, reauth):
        if not reauth:
            protocol = "ubi_v1"
            url = (
                "https://prod.trackmania.core.nadeo.online/"
                "v2/authentication/token/ubiservices"
            )
            token = self._get_ubi_auth_token()
        else:
            protocol = "nadeo_v1"
            url = (
                "https://prod.trackmania.core.nadeo.online/"
                "v2/authentication/token/refresh"
            )
            if service == "NadeoServices":
                token = self._tokens["services"]["refresh"]
            elif service == "NadeoLiveServices":
                token = self._tokens["live"]["refresh"]
            elif service == "NadeoClubServices":
                token = self._tokens["club"]["refresh"]
            else:
                # illegal value for `service`
                raise ValueError("Bad service name!")

        body = f'{{"audience":"{service}"}}'
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"{protocol} t={token}",
        }

        r = requests.post(url, data=body, headers=headers)
        if r.status_code != 200:
            raise ValueError("Authentication to Nadeo*Services failed!")
        return r.json()

    def test(self, nadeo_token):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"nadeo_v1 t={nadeo_token}",
        }

        r = requests.get(
            "https://prod.trackmania.core.nadeo.online/accounts/"
            "07ff9d3a-849e-496e-aca9-6ac41ed23e75",
            headers=headers,
        )
        r

    def _get_leaders_for_map(self, mapuid):
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"nadeo_v1 t={self._tokens['live']['access']}",
        }

        r = requests.get(
            "https://live-services.trackmania.nadeo.live/api/token/leaderboard/"
            f"group/Personal_Best/map/{mapuid}/top",
            headers=headers,
        )
        return r.json()

    def get_leaders_for_map(self, mapuid):
        leaders = self._get_leaders_for_map(mapuid)
        zone = None
        for t in leaders["tops"]:
            if t["zoneName"] == "World":
                zone = t["top"]
                break

        for rec in zone:
            if rec["position"] == 1:
                return {
                    "mapuid": leaders["mapUid"],
                    "accountId": zone["accountId"],
                    "score": zone["score"],
                }
        return {}

    def get_map_info(self, mapuid):
        # https://live-services.trackmania.nadeo.live/api/token/map/xI5EN2vK86qweSUY965uI0GJ5wc
        # "xI5EN2vK86qweSUY965uI0GJ5wc" => map_uid
        pass


if __name__ == "__main__":
    with open("../../../secrets.yaml", "r") as s:
        secrets = yaml.load(s, yaml.FullLoader)
    mapid = "PD70uHpLr7oq9lWoheRVxugESy4"
    try:
        if secrets["credentials_type"] == "account":
            nad = NadeoAPI(secrets["ubisoft_account"], secrets["ubisoft_passwd"])
    except KeyError as ke:
        raise KeyError(
            "secrets file is configured wrong! Please check for fields "
            "'ubisoft_account' and 'ubisoft_passwd'."
        ) from ke

    res = nad.get_leaders_for_map(mapid)
    print(res)
    from kacky_records_api.record_aggregators.kacky_reloaded_db import (
        KackyReloaded_KackyRecords,
    )

    kr = KackyReloaded_KackyRecords(secrets)
    maps = kr.get_maps()
    wrs = []
    for i, m in enumerate(maps):
        print(i)
        wrs.append(nad.get_leaders_for_map(m[0]))
    print(wrs)
    1
    # res = test(services_atoken)
    # print(res)

# curl -H "Authorization: nadeo_v1
# t=eyJhbGciOiJIUzI1NiIsImVudiI6InRyYWNrbWFuaWEtcHJvZCIsInZlciI6IjEifQ.eyJqdGkiOiIzNjQ2ZDI4NC01YWI1LTExZWQtYWNhMC0wMjQyYWMxMTAwMDMiLCJpc3MiOiJOYWRlb1NlcnZpY2VzIiwiaWF0IjoxNjY3Mzk2OTg2LCJyYXQiOjE2NjczOTg3ODYsImV4cCI6MTY2NzQwMDU4NiwiYXVkIjoiTmFkZW9MaXZlU2VydmljZXMiLCJ1c2ciOiJDbGllbnQiLCJzaWQiOiIzMmE2M2JlMi01YWI1LTExZWQtOWU5NS0wMjQyYWMxMTAwMDUiLCJzdWIiOiI5OTNkNmUzNS03ZTE0LTRkMWMtYTZiZS01MzA5MzhiNGU0ZmMiLCJhdW4iOiJjb3Jrc2NyZXctZ2VyIiwicnRrIjpmYWxzZSwicGNlIjpmYWxzZX0.95eLuK4fMlShu0rS4JVUhyfp0sI_c9TTxujIlJBEqZ0"
# -H "Accept: application/json" -H "Content-Type: application/json" https://live-services.trackmania.nadeo.live/api/token/leaderboard/group/Personal_Best/map/PD70uHpLr7oq9lWoheRVxugESy4/top  # noqa E501
# curl -H "Authorization: nadeo_v1
# t=eyJhbGciOiJIUzI1NiIsImVudiI6InRyYWNrbWFuaWEtcHJvZCIsInZlciI6IjEifQ.eyJqdGkiOiI4M2I3NzlmOC01YWNjLTExZWQtYjhmZi0wMjQyYWMxMTAwMDMiLCJpc3MiOiJOYWRlb1NlcnZpY2VzIiwiaWF0IjoxNjY3NDA2OTk0LCJyYXQiOjE2Njc0MDg3OTQsImV4cCI6MTY2NzQxMDU5NCwiYXVkIjoiTmFkZW9TZXJ2aWNlcyIsInVzZyI6IkNsaWVudCIsInNpZCI6IjgzYjc3N2QyLTVhY2MtMTFlZC1hMTRhLTAyNDJhYzExMDAwMyIsInN1YiI6Ijk5M2Q2ZTM1LTdlMTQtNGQxYy1hNmJlLTUzMDkzOGI0ZTRmYyIsImF1biI6ImNvcmtzY3Jldy1nZXIiLCJydGsiOmZhbHNlLCJwY2UiOmZhbHNlLCJ1YmlzZXJ2aWNlc191aWQiOiI2MzM1MjQzMy0xMGNjLTQwOTQtOWJkZi1lODczNjI5NTc5ODQifQ.fJffepiOMH58Anfhjo5Hhd-o_3uhJZQv9NhLnQAIvaI"
# -H "Accept: application/json" -H "Content-Type: application/json" https://live-services.trackmania.nadeo.live/api/token/leaderboard/group/Personal_Best/map/PD70uHpLr7oq9lWoheRVxugESy4/top  # noqa E501
