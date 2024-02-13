from typing import Tuple, Union

import requests

from kacky_records_api.record_aggregators.nadeo.authentication import (
    AuthenticationHandler,
)


class UbiServices:
    def __init__(self, user: str, pwd: str, accounttype: str, user_agent: str):
        self._auth_handler = AuthenticationHandler(user, pwd, accounttype, user_agent)

    def _request_executor(self, url):
        self._auth_handler.ubisoft_auth_status()
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": self._auth_handler.useragent,
            "Authorization": f"ubi_v1 t={self._auth_handler._ubi_auth['ticket']}",
            "Ubi-AppId": "86263886-327a-4328-ac69-527f0d20a237",
            "Ubi-SessionId": self._auth_handler._ubi_auth["sessionId"],
        }
        result = requests.get(url, headers=headers)
        return result.json()

    def get_profile(self, player_uids: Union[str, Tuple[str]]):
        url = "https://public-ubiservices.ubi.com/v3/profiles?profileIds="
        if isinstance(player_uids, tuple):
            url += +",".join(player_uids)
        else:
            url += player_uids
        return self._request_executor(url)
